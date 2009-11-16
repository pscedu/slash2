/* $Id$ */

#include <sys/time.h>
#include <sys/types.h>

#include <stdlib.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "pfl/cdefs.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "buffer.h"
#include "cli_bmap.h"
#include "mount_slash.h"
#include "bmpc.h"
#include "slashrpc.h"
#include "slconfig.h"

static struct timespec bmapFlushDefMaxAge = {0, 1000000000L};
__static struct timespec bmapFlushDefSleep = {0, 100000000L};

struct psc_listcache bmapFlushQ;
static struct pscrpc_nbreqset *pndgReqs;
static struct dynarray pndgReqSets=DYNARRAY_INIT;

static atomic_t outstandingRpcCnt;
static atomic_t completedRpcCnt;
static int shutdown=0;

#define MAX_OUTSTANDING_RPCS 64
#define MIN_COALESCE_RPC_SZ  LNET_MTU /* Try for big RPC's */

extern int slCacheBlkSz;

__static void
bmap_flush_reap_rpcs(void)
{
	int i;
	struct pscrpc_request_set *set;

	psc_info("outstandingRpcCnt=%d (before)",
		 atomic_read(&outstandingRpcCnt));

	for (i=0; i < dynarray_len(&pndgReqSets); i++) {
		set = dynarray_getpos(&pndgReqSets, i);
		if (!pscrpc_set_finalize(set, shutdown, 0)) {
			dynarray_remove(&pndgReqSets, set);
			i--;
		}
	}
	shutdown ? (int)nbrequest_flush(pndgReqs) :
		(int)nbrequest_reap(pndgReqs);

	psc_info("outstandingRpcCnt=%d (after)",
		 atomic_read(&outstandingRpcCnt));

	if (shutdown) {
		psc_assert(!dynarray_len(&pndgReqSets));
		psc_assert(!atomic_read(&pndgReqs->nb_outstanding));
		psc_assert(!atomic_read(&outstandingRpcCnt));
	}
}

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a)
{
	struct timespec ts;

	if (a->biorq_flags & BIORQ_FORCE_EXPIRE)
		return (1);

	clock_gettime(CLOCK_REALTIME, &ts);

	if ((a->biorq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) < ts.tv_sec)
		return (1);

	else if ((a->biorq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) >
		 ts.tv_sec)
		return (0);

	if ((a->biorq_start.tv_nsec + bmapFlushDefMaxAge.tv_nsec) <=
	    ts.tv_nsec)
		return (1);

	return (0);
}

__static size_t
bmap_flush_coalesce_size(const struct dynarray *biorqs)
{
	struct bmpc_ioreq *r;
	size_t size;

	r = dynarray_getpos(biorqs, dynarray_len(biorqs) - 1);
	size = r->biorq_off + r->biorq_len;

	r = dynarray_getpos(biorqs, 0);
	size -= r->biorq_off;

	psc_info("array %p has size=%zu array len=%d",
		 biorqs, size, dynarray_len(biorqs));

	return (size);
}

__static int
bmap_flush_rpc_cb(struct pscrpc_request *req,
		  __unusedx struct pscrpc_async_args *args)
{
	atomic_dec(&outstandingRpcCnt);
	DEBUG_REQ(PLL_INFO, req, "done (outstandingRpcCnt=%d)",
		  atomic_read(&outstandingRpcCnt));

	return (0);
}


__static struct pscrpc_request *
bmap_flush_create_rpc(struct bmapc_memb *b, struct iovec *iovs,
		      size_t size, off_t soff, int niovs)
{
	struct pscrpc_import *imp;
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *req;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int rc;

	atomic_inc(&outstandingRpcCnt);

	imp = msl_bmap_to_import(b, 1);

	rc = RSX_NEWREQ(imp, SRIC_VERSION, SRMT_WRITE, req, mq, mp);
	if (rc)
		psc_fatalx("RSX_NEWREQ() bad time to fail :( rc=%d", -rc);

	rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
			    iovs, niovs);
	if (rc)
		psc_fatalx("rsx_bulkclient() failed with %d", rc);

	req->rq_interpret_reply = bmap_flush_rpc_cb;
	req->rq_compl_cntr = &completedRpcCnt;

	mq->offset = soff;
	mq->size = size;
	mq->op = SRMIO_WR;

	DEBUG_REQ(PLL_INFO, req, "off=%u sz=%u op=%u", mq->offset,
		  mq->size, mq->op);

	memcpy(&mq->sbdb, &bmap_2_msbd(b)->msbd_bdb, sizeof(mq->sbdb));

	return (req);
}

__static void
bmap_flush_inflight_ref(struct bmpc_ioreq *r)
{
	int i;
	struct bmap_pagecache_entry *bmpce;

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	r->biorq_flags |= BIORQ_INFL;
	DEBUG_BIORQ(PLL_INFO, r, "set inflight");
	freelock(&r->biorq_lock);

	for (i=0; i < dynarray_len(&r->biorq_pages); i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i);
		spinlock(&bmpce->bmpce_lock);
		bmpce->bmpce_flags |= BMPCE_INFL;
		freelock(&bmpce->bmpce_lock);
		DEBUG_BIORQ(PLL_INFO, r, "set inflight");
	}
}


__static void
bmap_flush_send_rpcs(struct dynarray *biorqs, struct iovec *iovs,
		     int niovs)
{
	struct pscrpc_request *req;
	struct pscrpc_import *imp;
	struct bmpc_ioreq *r;
	struct bmapc_memb *b;
	off_t soff;
	size_t size;
	int i;

	r = dynarray_getpos(biorqs, 0);
	imp = msl_bmap_to_import(r->biorq_bmap, 1);
	psc_assert(imp);

	b = r->biorq_bmap;
	soff = r->biorq_off;

	for (i=0; i < dynarray_len(biorqs); i++) {
		/* All biorqs should have the same import, otherwise
		 *   there is a major problem.
		 */
		r = dynarray_getpos(biorqs, i);
		psc_assert(imp == msl_bmap_to_import(r->biorq_bmap, 0));
		psc_assert(b == r->biorq_bmap);
		bmap_flush_inflight_ref(r);
	}

	DEBUG_BIORQ(PLL_INFO, r, "biorq array cb arg (%p)", biorqs);

	if ((size = bmap_flush_coalesce_size(biorqs)) <= LNET_MTU) {
		/* Single rpc case.  Set the appropriate cb handler
		 *   and attach to the nb request set.
		 */
		req = bmap_flush_create_rpc(b, iovs, size, soff, niovs);
		/* Set the per-req cp arg for the nbreqset cb handler.
		 *   biorqs MUST be freed by the cb.
		 */
		req->rq_async_args.pointer_arg[0] = biorqs;
		nbreqset_add(pndgReqs, req);

	} else {
		/* Deal with a multiple rpc operation
		 */
		struct pscrpc_request_set *set;
		struct iovec *tiov;
		int n, j;

#define launch_rpc							\
		{							\
			req = bmap_flush_create_rpc(b, tiov, size, soff, n); \
			pscrpc_set_add_new_req(set, req);		\
			if (pscrpc_push_req(req)) {			\
				DEBUG_REQ(PLL_ERROR, req,		\
					  "pscrpc_push_req() failed");	\
				psc_fatalx("no failover yet");		\
			}						\
			soff += size;					\
		}

		size = 0;
		set = pscrpc_prep_set();
		set->set_interpret = msl_io_rpcset_cb;
		/* biorqs MUST be freed by the cb.
		 */
		set->set_arg = biorqs;

		for (j=0, n=0, size=0, tiov=iovs; j < niovs; j++) {
			if ((size + iovs[j].iov_len) == LNET_MTU) {
				n++;
				size += iovs[j].iov_len;
				launch_rpc;
				tiov = NULL;
				size = n = 0;

			} else if ((size + iovs[j].iov_len) > LNET_MTU) {
				psc_assert(n > 0);
				launch_rpc;
				size = iovs[j].iov_len;
				tiov = &iovs[j];
				n = 1;

			} else {
				if (!tiov)
					tiov = &iovs[j];
				size += iovs[j].iov_len;
				n++;
			}
		}
		/* Launch any small lingerers.
		 */
		if (tiov) {
			psc_assert(n);
			launch_rpc;
		}

		dynarray_add(&pndgReqSets, set);
	}
}

__static int
bmap_flush_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq *a = *(const struct bmpc_ioreq **)x;
	const struct bmpc_ioreq *b = *(const struct bmpc_ioreq **)y;

	//DEBUG_BIORQ(PLL_TRACE, a, "compare..");
	//DEBUG_BIORQ(PLL_TRACE, b, "..compare");

	if (a->biorq_off < b->biorq_off)
		return (-1);

	else if	(a->biorq_off > b->biorq_off)
		return (1);

	else {
		/* Larger requests with the same start offset should have
		 *   ordering priority.
		 */
		if (a->biorq_len > b->biorq_len)
			return (-1);

		else if (a->biorq_len < b->biorq_len)
			return (1);
	}
	return (0);
}

__static int
bmap_flush_coalesce_map(const struct dynarray *biorqs, struct iovec **iovset)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache_entry *bmpce;
	struct iovec *iovs=NULL;
	int i, j, niovs=0, first_iov;
	uint32_t reqsz=bmap_flush_coalesce_size(biorqs);
	off_t off=0;

	psc_assert(!*iovset);
	psc_assert(dynarray_len(biorqs) > 0);
	/* Prime the pump with initial values from the first biorq.
	 */
	r = dynarray_getpos(biorqs, 0);
	off = r->biorq_off;

	for (i=0; i < dynarray_len(biorqs); i++, first_iov=1) {
		r = dynarray_getpos(biorqs, i);

		DEBUG_BIORQ(PLL_INFO, r, "r rreqsz=%u off=%zu", reqsz, off);
		psc_assert(dynarray_len(&r->biorq_pages));

		if (biorq_voff_get(r) <= off) {
			/* No need to map this one, its data has been
			 *   accounted for but first ensure that all of the
			 *   pages have been scheduled for IO.
			 * XXX single-threaded, bmap_flush is single threaded
			 *   which will prevent any bmpce from being scheduled
			 *   twice.  Therefore, a bmpce skipped in this loop
			 *   must have BMPCE_IOSCHED set.
			 */
			for (j=0; j < dynarray_len(&r->biorq_pages); j++) {
				bmpce = dynarray_getpos(&r->biorq_pages, j);
				spinlock(&bmpce->bmpce_lock);
				psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
				freelock(&bmpce->bmpce_lock);
			}
			DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (skip)", i);
			continue;
		}
		DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (use)", i);
		psc_assert(reqsz);
		/* Now iterate through the biorq's iov set, where the
		 *   actual buffers are stored.
		 */
		for (j=0, first_iov=1; j < dynarray_len(&r->biorq_pages);
		     j++) {
			bmpce = dynarray_getpos(&r->biorq_pages, j);
			spinlock(&bmpce->bmpce_lock);

			if ((bmpce->bmpce_off <= r->biorq_off) && j)
				abort();

			if ((bmpce->bmpce_off < off) && !first_iov) {
				/* Similar case to the 'continue' stmt above,
				 *   this bmpce overlaps a previously
				 *   scheduled biorq.
				 */
				DEBUG_BMPCE(PLL_INFO, bmpce, "skip");
				psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
				freelock(&bmpce->bmpce_lock);
				continue;
			}

			bmpce->bmpce_flags |= BMPCE_IOSCHED;
			DEBUG_BMPCE(PLL_INFO, bmpce, "scheduled");
			/* Issue sanity checks on the bmpce.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
			       (first_iov ? (off & ~BMPC_BUFMASK) : off));

			freelock(&bmpce->bmpce_lock);
			/* Add a new iov!
			 */
			*iovset = iovs = PSC_REALLOC(iovs,
				     (sizeof(struct iovec) * (niovs + 1)));
			/* Set the base pointer past the overlapping
			 *   area if this is the first mapping.
			 */
			iovs[niovs].iov_base = bmpce->bmpce_base +
				(first_iov ? (off - bmpce->bmpce_off) : 0);

			iovs[niovs].iov_len = MIN(reqsz,
			  (first_iov ? bmpce->bmpce_off + BMPC_BUFSZ - off :
			   BMPC_BUFSZ));

			reqsz -= iovs[niovs].iov_len;
			off += iovs[niovs].iov_len;
			first_iov = 0;
			niovs++;

			psc_info("biorq=%p bmpce=%p base=%p len=%zu "
				 "niov=%d reqsz=%u (new)",
				 r, bmpce, iovs[niovs].iov_base,
				 iovs[niovs].iov_len, niovs, reqsz);
		}
	}
	psc_assert(!reqsz);
	return (niovs);
}

__static struct dynarray *
bmap_flush_trycoalesce(const struct dynarray *biorqs, int *offset)
{
	int i, off, expired=0;
	struct bmpc_ioreq *r=NULL, *t;
	struct dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(dynarray_len(biorqs) > *offset);

	for (off=0; (off + *offset) < dynarray_len(biorqs); off++) {
		t = dynarray_getpos(biorqs, off + *offset);

		psc_assert((t->biorq_flags & BIORQ_SCHED) &&
			   !(t->biorq_flags & BIORQ_INFL));

		if (r)
			/* Assert 'lowest to highest' ordering.
			 */
			psc_assert(t->biorq_off >= r->biorq_off);
		/* If any member is expired then we'll push everything out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(t);

		DEBUG_BIORQ(PLL_TRACE, t, "biorq #%d (expired=%d)",
			      off, expired);
		/* The next request, 't', can be added to the coalesce
		 *   group either because 'r' is not yet set (meaning
		 *   the group is empty) or because 't' overlaps or
		 *   extends 'r'.
		 */
		if (!r || t->biorq_off <= biorq_voff_get(r)) {
			dynarray_add(&b, t);
			if (!r || biorq_voff_get(t) > biorq_voff_get(r))
				/* If 'r' is not yet set or 't' is a larger
				 *   extent then set 'r' to 't'.
				 */
				r = t;
		} else {
			if ((bmap_flush_coalesce_size(&b) >=
			     MIN_COALESCE_RPC_SZ) || expired)
				goto make_coalesce;
			else {
				/* Start over.
				 */
				dynarray_reset(&b);
				dynarray_add(&b, t);
				r = t;
			}
		}
	}
	if (expired) {
	make_coalesce:
		a = PSCALLOC(sizeof(*a));
		for (i=0; i < dynarray_len(&b); i++) {
			t = dynarray_getpos(&b, i);
			dynarray_add(a, dynarray_getpos(&b, i));
		}
	}

	*offset += off;
	dynarray_free(&b);

	return (a);
}

void
bmap_flush(void)
{
	struct bmapc_memb *b;
	struct bmap_cli_info *msbd;
	struct bmap_pagecache *bmpc;
	struct dynarray a=DYNARRAY_INIT, bmaps=DYNARRAY_INIT, *biorqs;
	struct bmpc_ioreq *r;
	struct iovec *iovs=NULL;
	int i=0, niovs;

	while ((atomic_read(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS) &&
	       (msbd = lc_getnb(&bmapFlushQ))) {

		b = msbd->msbd_bmap;
		bmpc = bmap_2_msbmpc(b);
		/* Bmap lock only needed to test the dirty bit.
		 */
		BMAP_LOCK(b);
		DEBUG_BMAP(PLL_INFO, b, "try flush (outstandingRpcCnt=%d)",
			   atomic_read(&outstandingRpcCnt));
		/* Take the page cache lock too so that the bmap's
		 *   dirty state may be sanity checked.
		 */
		BMPC_LOCK(bmpc);

		if (b->bcm_mode & BMAP_DIRTY) {
			psc_assert(bmpc_queued_writes(bmpc));
			dynarray_add(&bmaps, msbd);
		} else {
			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			psc_assert(!bmpc_queued_writes(bmpc));
			freelock(&bmpc->bmpc_lock);
			BMAP_ULOCK(b);
			continue;
		}
		BMAP_ULOCK(b);

		dynarray_reset(&a);

		PLL_FOREACH(r, &bmpc->bmpc_pndg) {
			spinlock(&r->biorq_lock);
			if (r->biorq_flags & BIORQ_INFL) {
				psc_assert(r->biorq_flags & BIORQ_SCHED);
				freelock(&r->biorq_lock);
				continue;

			} else if (r->biorq_flags & BIORQ_READ) {
				freelock(&r->biorq_lock);
				continue;
			}

			r->biorq_flags |= BIORQ_SCHED;
			freelock(&r->biorq_lock);

			DEBUG_BIORQ(PLL_TRACE, r, "try flush");
			dynarray_add(&a, r);
		}
		BMPC_ULOCK(bmpc);

		if (!dynarray_len(&a)) {
			dynarray_free(&a);
			break;
		}
		/* Sort the items by their offsets.
		 */
		qsort(a.da_items, a.da_pos, sizeof(void *),
		      bmap_flush_biorq_cmp);

#if 1
		for (i=0; i < dynarray_len(&a); i++) {
			r = dynarray_getpos(&a, i);
			DEBUG_BIORQ(PLL_TRACE, r, "sorted?");
		}
#endif

		i=0;
		while (i < dynarray_len(&a) &&
		       (biorqs = bmap_flush_trycoalesce(&a, &i))) {
			/* Note: 'biorqs' must be freed!!
			 */
			niovs = bmap_flush_coalesce_map(biorqs, &iovs);
			psc_assert(niovs);
			/* Have a set of iov's now.  Let's create an rpc
			 *   or rpc set and send it out.
			 */
			bmap_flush_send_rpcs(biorqs, iovs, niovs);
			PSCFREE(iovs);
		}
	}

	for (i=0; i < dynarray_len(&bmaps); i++) {
		msbd = dynarray_getpos(&bmaps, i);
		b = msbd->msbd_bmap;
		bmpc = bmap_2_msbmpc(b);

		BMAP_LOCK(b);
		spinlock(&bmpc->bmpc_lock);
		if (bmpc_queued_writes(bmpc)) {
			psc_assert(b->bcm_mode & BMAP_DIRTY);
			BMAP_ULOCK(b);
			DEBUG_BMAP(PLL_INFO, b, "restore to dirty list");
			lc_addtail(&bmapFlushQ, msbd);

		} else {
			psc_assert(!(b->bcm_mode & BMAP_DIRTY));
			freelock(&bmpc->bmpc_lock);
			BMAP_ULOCK(b);

			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			continue;
		}
		freelock(&bmpc->bmpc_lock);
	}
	dynarray_free(&bmaps);
}

void *
msbmapflushthr_main(__unusedx void *arg)
{
	while (1) {
		if (atomic_read(&completedRpcCnt))
			bmap_flush_reap_rpcs();

		if (atomic_read(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS &&
		    lc_sz(&bmapFlushQ))
			bmap_flush();

		if (atomic_read(&completedRpcCnt))
			bmap_flush_reap_rpcs();

		if (shutdown) {
			bmap_flush_reap_rpcs();
			break;
		}
		/* This sleep should be dynamic, determined by the time of the
		 *   oldest pending oftrq.
		 */
		/*
		if (!atomic_read(&outstandingRpcCnt)) {
			LIST_CACHE_LOCK(&bmapFlushQ);
			psc_waitq_timedwait(&bmapFlushQ.lc_wq_empty,
				    &bmapFlushQ.lc_lock, &bmapFlushDefSleep);
		} else
		*/
		usleep(100000);
	}
	return (NULL);
}

void
msbmapflushthr_spawn(void)
{
	pndgReqs = nbreqset_init(NULL, msl_io_rpc_cb);
	atomic_set(&outstandingRpcCnt, 0);
	atomic_set(&completedRpcCnt, 0);

	lc_reginit(&bmapFlushQ, struct bmap_cli_info,
	    msbd_lentry, "bmapflush");

	pscthr_init(MSTHRT_BMAPFLSH, 0, msbmapflushthr_main,
	    NULL, 0, "msbflushthr");
}
