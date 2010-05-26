/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/time.h>
#include <sys/types.h>

#include <stdlib.h>

#include "pfl/cdefs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "bmpc.h"
#include "buffer.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"

__static struct timespec	 bmapFlushDefMaxAge = { 0, 1000000L };
__static struct timespec	 bmapFlushDefSleep = { 0, 100000000L };

struct psc_listcache		 bmapFlushQ;
struct psc_listcache		 bmapTimeoutQ;

__static struct pscrpc_nbreqset	*pndgReqs;
__static struct psc_dynarray	 pndgReqSets = DYNARRAY_INIT;

__static atomic_t		 outstandingRpcCnt;
__static atomic_t		 completedRpcCnt;
__static int			 shutdown;
__static struct psc_waitq	 rpcCompletion;

#define MAX_OUTSTANDING_RPCS	128
#define MIN_COALESCE_RPC_SZ	LNET_MTU /* Try for big RPC's */

__static psc_spinlock_t pndgReqLock = LOCK_INITIALIZER;

#define pndgReqsLock()		spinlock(&pndgReqLock)
#define pndgReqsUlock()		freelock(&pndgReqLock)

__static void
bmap_flush_reap_rpcs(void)
{
	struct pscrpc_request_set *set;
	int i;

	psc_trace("outstandingRpcCnt=%d (before) completedRpcCnt=%d",
	    atomic_read(&outstandingRpcCnt), atomic_read(&completedRpcCnt));

	/* Only this thread may pull from pndgReqSets dynarray,
	 *   therefore it can never shrink except by way of this
	 *   routine.
	 */
	for (i=0; i < psc_dynarray_len(&pndgReqSets); i++) {
		pndgReqsLock();
		set = psc_dynarray_getpos(&pndgReqSets, i);
		psc_assert(set);
		pndgReqsUlock();

		/* XXX handle the return code from pscrpc_set_finalize
		 *   properly.
		 */
		if (!pscrpc_set_finalize(set, shutdown, 0)) {
			pndgReqsLock();
			psc_dynarray_remove(&pndgReqSets, set);
			pndgReqsUlock();
			i--;
		}
	}
	if (shutdown)
		pscrpc_nbreqset_flush(pndgReqs);
	else
		pscrpc_nbreqset_reap(pndgReqs);

	psc_trace("outstandingRpcCnt=%d (after)",
		 atomic_read(&outstandingRpcCnt));

	if (shutdown) {
		psc_assert(!psc_dynarray_len(&pndgReqSets));
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
bmap_flush_coalesce_size(const struct psc_dynarray *biorqs)
{
	struct bmpc_ioreq *r;
	size_t size;

	r = psc_dynarray_getpos(biorqs, psc_dynarray_len(biorqs) - 1);
	size = r->biorq_off + r->biorq_len;

	r = psc_dynarray_getpos(biorqs, 0);
	size -= r->biorq_off;

	psc_info("array %p has size=%zu array len=%d",
		 biorqs, size, psc_dynarray_len(biorqs));

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

	rc = SL_RSX_NEWREQ(imp, SRIC_VERSION, SRMT_WRITE, req, mq, mp);
	if (rc)
		psc_fatalx("SL_RSX_NEWREQ() bad time to fail :( rc=%d", -rc);

	rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
			    iovs, niovs);
	if (rc)
		psc_fatalx("rsx_bulkclient() failed with %d", rc);

	req->rq_interpret_reply = bmap_flush_rpc_cb;
	req->rq_compl_cntr = &completedRpcCnt;
	req->rq_waitq = &rpcCompletion;

	mq->offset = soff;
	mq->size = size;
	mq->op = SRMIOP_WR;

	DEBUG_REQ(PLL_INFO, req, "off=%u sz=%u op=%u", mq->offset,
		  mq->size, mq->op);

	memcpy(&mq->sbd, &bmap_2_msbd(b)->msbd_sbd, sizeof(mq->sbd));
	return (req);
}

__static void
bmap_flush_inflight_set(struct bmpc_ioreq *r)
{
	struct bmap_pagecache *bmpc;

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	r->biorq_flags |= BIORQ_INFL;
	DEBUG_BIORQ(PLL_INFO, r, "set inflight");
	freelock(&r->biorq_lock);

	bmpc = bmap_2_bmpc(r->biorq_bmap);
	BMPC_LOCK(bmpc);
	/* Limit the amount of scanning done by this
	 *   thread.  Move pending biorqs out of the way.
	 */
	pll_remove(&bmpc->bmpc_new_biorqs, r);
	pll_addtail(&bmpc->bmpc_pndg_biorqs, r);
	BMPC_ULOCK(bmpc);
}


__static int
bmap_flush_send_rpcs(struct psc_dynarray *biorqs, struct iovec *iovs,
		     int niovs)
{
	struct pscrpc_request *req;
	struct pscrpc_import *imp;
	struct bmpc_ioreq *r;
	struct bmapc_memb *b;
	off_t soff;
	size_t size;
	int i, nrpcs=0;

	r = psc_dynarray_getpos(biorqs, 0);
	imp = msl_bmap_to_import(r->biorq_bmap, 1);
	psc_assert(imp);

	b = r->biorq_bmap;
	soff = r->biorq_off;

	for (i=0; i < psc_dynarray_len(biorqs); i++) {
		/* All biorqs should have the same import, otherwise
		 *   there is a major problem.
		 */
		r = psc_dynarray_getpos(biorqs, i);
		psc_assert(imp == msl_bmap_to_import(r->biorq_bmap, 0));
		psc_assert(b == r->biorq_bmap);
		bmap_flush_inflight_set(r);
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
		pscrpc_nbreqset_add(pndgReqs, req);
		nrpcs++;
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
			nrpcs++;					\
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
		pndgReqsLock();
		psc_dynarray_add(&pndgReqSets, set);
		pndgReqsUlock();
	}
	return (nrpcs);
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
bmap_flush_coalesce_map(const struct psc_dynarray *biorqs,
			struct iovec **iovset)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache_entry *bmpce;
	struct iovec *iovs=NULL;
	int i, j, niovs=0, first_iov;
	uint32_t tot_reqsz=bmap_flush_coalesce_size(biorqs), reqsz;
	off_t off=0;

	psc_trace("ENTRY: biorqs=%p tot_reqsz=%u", biorqs, tot_reqsz);

	psc_assert(!*iovset);
	psc_assert(psc_dynarray_len(biorqs) > 0);

	for (i=0; i < psc_dynarray_len(biorqs); i++, first_iov=1) {
		r = psc_dynarray_getpos(biorqs, i);
		off = r->biorq_off;
		reqsz = r->biorq_len;

		DEBUG_BIORQ(PLL_INFO, r, "r tot_reqsz=%u off=%"PSCPRIdOFF,
			    tot_reqsz, off);
		psc_assert(psc_dynarray_len(&r->biorq_pages));

		if (biorq_voff_get(r) <= off) {
			/* No need to map this one, its data has been
			 *   accounted for but first ensure that all of the
			 *   pages have been scheduled for IO.
			 * XXX single-threaded, bmap_flush is single threaded
			 *   which will prevent any bmpce from being scheduled
			 *   twice.  Therefore, a bmpce skipped in this loop
			 *   must have BMPCE_IOSCHED set.
			 */
			for (j=0; j < psc_dynarray_len(&r->biorq_pages); j++) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages, j);
				BMPCE_LOCK(bmpce);
				psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
				BMPCE_ULOCK(bmpce);
			}
			DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (skip)", i);
			continue;
		}
		DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (use)", i);
		psc_assert(tot_reqsz);
		/* Now iterate through the biorq's iov set, where the
		 *   actual buffers are stored.  Note that this dynarray
		 *   is sorted.
		 */
		for (j=0, first_iov=1; j < psc_dynarray_len(&r->biorq_pages);
		     j++) {
			psc_assert(reqsz);

			bmpce = psc_dynarray_getpos(&r->biorq_pages, j);
			BMPCE_LOCK(bmpce);

			if ((bmpce->bmpce_off <= r->biorq_off) && j)
				abort();

			if ((bmpce->bmpce_off < off) && !first_iov) {
				/* Similar case to the 'continue' stmt above,
				 *   this bmpce overlaps a previously
				 *   scheduled biorq.
				 */
				DEBUG_BMPCE(PLL_INFO, bmpce, "skip");
				psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
				BMPCE_ULOCK(bmpce);

				reqsz -= BMPC_BUFSZ;
				continue;
			}
#if 0
			bmpce->bmpce_flags |= BMPCE_IOSCHED;
#endif
			DEBUG_BMPCE(PLL_INFO, bmpce,
				    "scheduling, first_iov=%d", first_iov);
			bmpce_inflight_inc_locked(bmpce);
			/* Issue sanity checks on the bmpce.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
			       (first_iov ? (off & ~BMPC_BUFMASK) : off));

			BMPCE_ULOCK(bmpce);
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

			off += iovs[niovs].iov_len;
			reqsz -= iovs[niovs].iov_len;
			tot_reqsz -= iovs[niovs].iov_len;

			if (first_iov)
				first_iov = 0;

			psc_info("biorq=%p bmpce=%p base=%p len=%zu "
				 "niov=%d reqsz=%u tot_reqsz=%u(new)",
				 r, bmpce, iovs[niovs].iov_base,
				 iovs[niovs].iov_len, niovs, reqsz, tot_reqsz);
			niovs++;
		}
	}
	psc_assert(!tot_reqsz);
	return (niovs);
}

__static int
bmap_flush_biorq_rbwdone(const struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int rc=0;

	bmpce = (r->biorq_flags & BIORQ_RBWFP) ?
		psc_dynarray_getpos(&r->biorq_pages, 0) :
		psc_dynarray_getpos(&r->biorq_pages,
				    psc_dynarray_len(&r->biorq_pages)-1);

	BMPCE_LOCK(bmpce);
	if (bmpce->bmpce_flags & BMPCE_DATARDY)
		rc = 1;
	BMPCE_ULOCK(bmpce);

	return (rc);
}

__static int
bmap_flush_bmpce_check_sched_locked(const struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int rc=0, i;

	for (i=0; i < psc_dynarray_len(&r->biorq_pages); i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);
		if (bmpce->bmpce_flags & BMPCE_IOSCHED) {
			DEBUG_BMPCE(PLL_ERROR, bmpce, "already sched");
			rc = 1;
		} else
			DEBUG_BMPCE(PLL_INFO, bmpce, "not sched");
		BMPCE_ULOCK(bmpce);

		if (rc)
			break;
	}

	return (rc);
}

__static struct psc_dynarray *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *offset)
{
	int i, off, expired=0;
	struct bmpc_ioreq *r=NULL, *t;
	struct psc_dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(psc_dynarray_len(biorqs) > *offset);

	for (off=0; (off + *offset) < psc_dynarray_len(biorqs); off++) {
		t = psc_dynarray_getpos(biorqs, off + *offset);

		psc_assert((t->biorq_flags & BIORQ_SCHED) &&
			   !(t->biorq_flags & BIORQ_INFL));

		if (r)
			/* Assert 'lowest to highest' ordering.
			 */
			psc_assert(t->biorq_off >= r->biorq_off);
		else
			r = t;

		/* If any member is expired then we'll push everything out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(t);

		DEBUG_BIORQ(PLL_NOTIFY, t, "biorq #%d (expired=%d)",
			      off, expired);
		/* The next request, 't', can be added to the coalesce
		 *   group either because 'r' is not yet set (meaning
		 *   the group is empty) or because 't' overlaps or
		 *   extends 'r'.
		 */
		if (t->biorq_off <= biorq_voff_get(r)) {
			psc_dynarray_add(&b, t);
			if (biorq_voff_get(t) > biorq_voff_get(r))
				/* If 'r' is not yet set or 't' is a larger
				 *   extent then set 'r' to 't'.
				 */
				r = t;
		} else {
			if ((bmap_flush_coalesce_size(&b) >=
			     MIN_COALESCE_RPC_SZ) || expired)
				goto make_coalesce;
			else {
				/* This biorq is not contiguous with
				 *   the previous. Start over but first
				 *   deschedule the biorq's which are being
				 *   held back.
				 */
				for (i=0; i < psc_dynarray_len(&b); i++) {
					t = psc_dynarray_getpos(&b, i);
					spinlock(&t->biorq_lock);
					DEBUG_BIORQ(PLL_INFO, t,
						    "descheduling");
					t->biorq_flags &= ~BIORQ_SCHED;
					freelock(&t->biorq_lock);
				}
				psc_dynarray_reset(&b);
				psc_dynarray_add(&b, t);
				r = t;
			}
		}
	}

	if (expired) {
 make_coalesce:
		a = PSCALLOC(sizeof(*a));
		for (i=0; i < psc_dynarray_len(&b); i++) {
			t = psc_dynarray_getpos(&b, i);
			psc_dynarray_add(a, psc_dynarray_getpos(&b, i));
		}

	} else
		/* Clean up any lingering biorq's.
		 */
		for (i=0; i < psc_dynarray_len(&b); i++) {
			t = psc_dynarray_getpos(&b, i);
			DEBUG_BIORQ(PLL_INFO, t, "descheduling");
			t->biorq_flags &= ~BIORQ_SCHED;
		}

	*offset += off;
	psc_dynarray_free(&b);

	return (a);
}

void
bmap_flush(void)
{
	struct bmapc_memb *b;
	struct bmap_cli_info *msbd;
	struct bmap_pagecache *bmpc;
	struct psc_dynarray a=DYNARRAY_INIT, bmaps=DYNARRAY_INIT, *biorqs;
	struct bmpc_ioreq *r, *tmp;
	struct iovec *iovs=NULL;
	int i=0, niovs, nrpcs;

	nrpcs = MAX_OUTSTANDING_RPCS - atomic_read(&outstandingRpcCnt);

	if (nrpcs <= 0) {
		psc_trace("nrpcs=%d", nrpcs);
		return;
	}

	while (nrpcs > 0) {
		msbd = lc_getnb(&bmapFlushQ);
		if (!msbd)
			break;

		b = msbd->msbd_bmap;
		bmpc = bmap_2_bmpc(b);
		/* Bmap lock only needed to test the dirty bit.
		 */
		BMAP_LOCK(b);
		DEBUG_BMAP(PLL_INFO, b, "try flush (outstandingRpcCnt=%d)",
			   atomic_read(&outstandingRpcCnt));

		psc_assert(b->bcm_mode & BMAP_CLI_FLUSHPROC);
		/* Take the page cache lock too so that the bmap's
		 *   dirty state may be sanity checked.
		 */
		BMPC_LOCK(bmpc);
		if (b->bcm_mode & BMAP_DIRTY) {
			psc_assert(bmpc_queued_writes(bmpc));
			psc_dynarray_add(&bmaps, msbd);

		} else {
			psc_assert(!bmpc_queued_writes(bmpc));
			b->bcm_mode &= ~BMAP_CLI_FLUSHPROC;
			bcm_wake_locked(b);
			BMPC_ULOCK(bmpc);

			if (!bmpc_queued_ios(bmpc)) {
				/* No remaining reads or writes.
				 */
				psc_assert(!(b->bcm_mode & BMAP_REAPABLE));
				b->bcm_mode |= BMAP_REAPABLE;
				lc_addtail(&bmapTimeoutQ, bmap_2_msbd(b));
				DEBUG_BMAP(PLL_INFO, b, "added to bmapTimeoutQ");
			}
			BMAP_ULOCK(b);
			continue;
		}
		BMAP_ULOCK(b);

		psc_dynarray_reset(&a);

		PLL_FOREACH_SAFE(r, tmp, &bmpc->bmpc_new_biorqs) {
			spinlock(&r->biorq_lock);

			DEBUG_BIORQ(PLL_NOTIFY, r, "consider for flush");

			psc_assert(!(r->biorq_flags & BIORQ_READ));
			psc_assert(!(r->biorq_flags & BIORQ_DESTROY));

			if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
				freelock(&r->biorq_lock);
				continue;

			} else if (r->biorq_flags & BIORQ_SCHED) {
				DEBUG_BIORQ(PLL_WARN, r, "already sched");
				freelock(&r->biorq_lock);
				continue;

			} else if ((r->biorq_flags & BIORQ_RBWFP) ||
				   (r->biorq_flags & BIORQ_RBWLP)) {
				/* Wait for RBW I/O to complete before
				 *  pushing out any pages.
				 */
				if (!bmap_flush_biorq_rbwdone(r)) {
					freelock(&r->biorq_lock);
					continue;
				}
			}
			/* Don't assert !BIORQ_INFL until ensuring that
			 *   we can actually work on this biorq.  A RBW
			 *   process may be working on it.
			 */
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			r->biorq_flags |= BIORQ_SCHED;
			freelock(&r->biorq_lock);

			DEBUG_BIORQ(PLL_NOTIFY, r, "try flush");
			psc_dynarray_add(&a, r);
		}
		BMPC_ULOCK(bmpc);
		/* Didn't find any work on this bmap.
		 */
		if (!psc_dynarray_len(&a))
			continue;
		/* Sort the items by their offsets.
		 */
		psc_dynarray_sort(&a, qsort, bmap_flush_biorq_cmp);
#if 0
		for (i=0; i < psc_dynarray_len(&a); i++) {
			r = psc_dynarray_getpos(&a, i);
			DEBUG_BIORQ(PLL_NOTIFY, r, "sorted?");
		}
#endif
		i=0;
		while (i < psc_dynarray_len(&a) &&
		       (biorqs = bmap_flush_trycoalesce(&a, &i))) {
			/* Note: 'biorqs' must be freed!!
			 */
			niovs = bmap_flush_coalesce_map(biorqs, &iovs);
			psc_assert(niovs);
			/* Have a set of iov's now.  Let's create an rpc
			 *   or rpc set and send it out.
			 */
			nrpcs -= bmap_flush_send_rpcs(biorqs, iovs, niovs);
			PSCFREE(iovs);
		}
	}

	for (i=0; i < psc_dynarray_len(&bmaps); i++) {
		msbd = psc_dynarray_getpos(&bmaps, i);
		b = msbd->msbd_bmap;
		bmpc = bmap_2_bmpc(b);

		BMAP_LOCK(b);
		BMPC_LOCK(bmpc);
		/* BMAP_CLI_FLUSHPROC must be present, only the section
		 *   below may remove it.  BMAP_CLI_FLUSHPROC and
		 *   BMAP_REAPABLE are mutually exclusive.
		 */
		psc_assert(b->bcm_mode & BMAP_CLI_FLUSHPROC);
		psc_assert(!(b->bcm_mode & BMAP_REAPABLE));

		if (bmpc_queued_writes(bmpc)) {
			psc_assert(b->bcm_mode & BMAP_DIRTY);
			DEBUG_BMAP(PLL_INFO, b, "restore to dirty list");
			lc_addtail(&bmapFlushQ, msbd);

		} else {
			psc_assert(!(b->bcm_mode & BMAP_DIRTY));
			b->bcm_mode &= ~BMAP_CLI_FLUSHPROC;

			if (!bmpc_queued_ios(bmpc)) {
				psc_assert(!atomic_read(&bmpc->bmpc_pndgwr));
				b->bcm_mode |= BMAP_REAPABLE;
				lc_addtail(&bmapTimeoutQ, bmap_2_msbd(b));
			}

			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			bcm_wake_locked(b);
		}
		BMPC_ULOCK(bmpc);
		BMAP_ULOCK(b);
	}
	psc_dynarray_free(&bmaps);
	psc_dynarray_free(&a);
}

static __inline void
bmap_2_bid(const struct bmapc_memb *b, struct srm_bmap_id *bid)
{
	bid->fg.fg_fid = fcmh_2_fid(b->bcm_fcmh);
	bid->fg.fg_gen = fcmh_2_gen(b->bcm_fcmh);
	bid->seq = bmap_2_msbd(b)->msbd_sbd.sbd_seq;
	bid->key = bmap_2_msbd(b)->msbd_sbd.sbd_key;
	bid->bmapno = b->bcm_bmapno;
}

static void
ms_bmap_release(struct sl_resm *resm)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct resm_cli_info *rmci;
	int i, rc;

	csvc = (resm == slc_rmc_resm) ?
		slc_getmcsvc(resm) : slc_geticsvc(resm);
	if (csvc == NULL) {
		if (resm->resm_csvc)
			rc = resm->resm_csvc->csvc_lasterrno;
		else
			rc = -ENOTCONN;
		goto out;
	}

	rmci = resm2rmci(resm);
	psc_assert(rmci->rmci_bmaprls.nbmaps);

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(mq, &rmci->rmci_bmaprls, sizeof(*mq));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;

 out:
	for (i = 0; i < rmci->rmci_bmaprls.nbmaps; i++)
		psc_notify("fid "FIDFMT" bmap=%u key=%"PRId64
		    " seq=%"PRId64" rc=%d",
		    FIDFMTARGS(&rmci->rmci_bmaprls.bmaps[i].fg),
		    rmci->rmci_bmaprls.bmaps[i].bmapno,
		    rmci->rmci_bmaprls.bmaps[i].key,
		    rmci->rmci_bmaprls.bmaps[i].seq,
		    mp->bidrc[i]);
	rmci->rmci_bmaprls.nbmaps = 0;

	if (rc) {
		/* At this point the bmaps have already been purged from
		 *   our cache.  If the mds rls request fails then the
		 *   mds should time them out on his own.  In any case,
		 *   the client must reacquire leases to perform further
		 *   I/O on any bmap in this set.
		 */
		psc_errorx("bmap release RPC failed rc=%d", rc);
	}
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
msbmaprlsthr_main(__unusedx struct psc_thread *thr)
{
	struct bmapc_memb *b;
	struct bmap_cli_info *msbd;
	struct timespec ctime, wtime = {0, 0};
	struct psc_waitq waitq = PSC_WAITQ_INIT;
	struct psc_dynarray a = DYNARRAY_INIT;
	struct sl_resm *resm;
	struct resm_cli_info *rmci;

	int i;

	// just put the resm's in the dynarray. when pushing out the bid's
	//   assume an ion unless resm == slc_rmc_resm

	do {
		psc_info("msbmaprlsthr_main() top of loop");

		lc_sort(&bmapTimeoutQ, qsort, bmap_cli_timeo_cmp);
		clock_gettime(CLOCK_REALTIME, &ctime);

		wtime.tv_sec = BMAP_CLI_TIMEO_INC;

		while ((msbd = lc_getnb(&bmapTimeoutQ))) {
			b = msbd->msbd_bmap;
			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

			BMAP_LOCK(b);
			DEBUG_BMAP(PLL_INFO, b,
			   "timeoq try reap (nbmaps=%zd) etime(%ld:%ld)",
			   lc_sz(&bmapTimeoutQ), msbd->msbd_etime.tv_sec,
			   msbd->msbd_etime.tv_nsec);

			if (bmpc_queued_ios(&msbd->msbd_bmpc)) {
				b->bcm_mode &= ~BMAP_REAPABLE;
				DEBUG_BMAP(PLL_INFO, b,
					   "descheduling from timeoq");
				BMAP_ULOCK(b);
				continue;
			}

			if (timespeccmp(&ctime, &msbd->msbd_etime, <)) {
				/* Nothing past this point has expired.
				 */
				lc_addstack(&bmapTimeoutQ, msbd);
				BMAP_ULOCK(b);
				/* Set the wait time to etime - ctime.
				 */
				timespecsub(&msbd->msbd_etime, &ctime, &wtime);
				break;
			}

			/* Maintain the lock, bmap_op_done_type() will take
			 *   it anyway.
			 */
			if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				lc_addqueue(&bmapTimeoutQ, msbd);
				BMAP_ULOCK(b);

			} else {
				psc_assert(psc_atomic32_read(&b->bcm_opcnt)
					   == 1);
				/* Note that only this thread calls
				 *   ms_bmap_release() so no reentrancy
				 *   exist unless another rls thr is
				 *   introduced.
				 */
				psc_assert(!bmpc_queued_ios(&msbd->msbd_bmpc));

				if (b->bcm_mode & BMAP_WR) {
					/* Setup a msg to an ION.
					 */
					psc_assert(bmap_2_ion(b) !=
						   LNET_NID_ANY);

					resm = libsl_nid2resm(bmap_2_ion(b));
					rmci = resm2rmci(resm);
				} else {
					resm = slc_rmc_resm;
					rmci = resm2rmci(slc_rmc_resm);
				}
				bmap_2_bid(b, &rmci->rmci_bmaprls.bmaps[rmci->rmci_bmaprls.nbmaps]);
				rmci->rmci_bmaprls.nbmaps++;

				/* The bmap should be going away now, this
				 *    will call BMAP_URLOCK().
				 */
				bmap_op_done_type(b, BMAP_OPCNT_REAPER);

				if (rmci->rmci_bmaprls.nbmaps ==
				    MAX_BMAP_RELEASE) {
					ms_bmap_release(resm);
					if (psc_dynarray_exists(&a, resm))
						psc_dynarray_remove(&a, resm);
				} else
					psc_dynarray_add_ifdne(&a, resm);
			}
		}
		psc_info("msbmaprlsthr_main() out of loop (arraysz=%d)",
			 psc_dynarray_len(&a));

		/* Send out partially filled release request.
		 */
		DYNARRAY_FOREACH(resm, i, &a)
			ms_bmap_release(resm);

		psc_dynarray_reset(&a);

		if (!pscthr_run())
			break;

		if (!wtime.tv_sec && !wtime.tv_nsec)
			wtime.tv_sec = 1;
		psc_waitq_waitrel(&waitq, NULL, &wtime);

		wtime.tv_sec = wtime.tv_nsec = 0;

	} while (1);
	psc_dynarray_free(&a);
}

void
msbmapflushthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		if (atomic_read(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS)
			bmap_flush();
		usleep(2048);
	}
}

void
msbmapflushthrrpc_main(__unusedx struct psc_thread *thr)
{
	struct timespec ts = {0, 100000};
	int rc;

	while (pscthr_run()) {
		rc = psc_waitq_waitrel(&rpcCompletion, NULL, &ts);
		psc_trace("rc=%d", rc);
		bmap_flush_reap_rpcs();
	}
}

void
msbmapflushthr_spawn(void)
{
	pndgReqs = pscrpc_nbreqset_init(NULL, msl_io_rpc_cb);
	atomic_set(&outstandingRpcCnt, 0);
	atomic_set(&completedRpcCnt, 0);
	psc_waitq_init(&rpcCompletion);

	lc_reginit(&bmapFlushQ, struct bmap_cli_info,
	    msbd_lentry, "bmapFlushQ");

	lc_reginit(&bmapTimeoutQ, struct bmap_cli_info,
	    msbd_lentry, "bmapTimeoutQ");

	pscthr_init(MSTHRT_BMAPFLSH, 0, msbmapflushthr_main,
	    NULL, 0, "msbflushthr");

	pscthr_init(MSTHRT_BMAPFLSHRPC, 0, msbmapflushthrrpc_main,
	    NULL, 0, "msbflushthrrpc");

	pscthr_init(MSTHRT_BMAPFLSHRLS, 0, msbmaprlsthr_main,
	    NULL, 0, "msbrlsthr");
}
