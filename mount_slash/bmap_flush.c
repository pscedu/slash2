/* $Id$ */

#include <sys/time.h>
#include <sys/types.h>

#include <stdlib.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "buffer.h"
#include "cli_bmap.h"
#include "mount_slash.h"
#include "offtree.h"
#include "slashrpc.h"
#include "slconfig.h"

static struct timespec bmapFlushDefMaxAge = {0, 1000000000L};
static struct timespec bmapFlushDefSleep = {0, 100000000L};

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
bmap_flush_oftrq_expired(const struct offtree_req *a)
{
	struct timespec ts;

	clock_gettime(CLOCK_REALTIME, &ts);

	if ((a->oftrq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) < ts.tv_sec)
		return (1);

	else if ((a->oftrq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) > 
		 ts.tv_sec)
		return (0);

	if ((a->oftrq_start.tv_nsec + bmapFlushDefMaxAge.tv_nsec) <=
	    ts.tv_nsec)
		return (1);
	
	return (0);
}

__static size_t 
bmap_flush_coalesce_size(const struct dynarray *oftrqs)
{
	struct offtree_req *r;
	size_t size;

	r = dynarray_getpos(oftrqs, dynarray_len(oftrqs) - 1);
	size = r->oftrq_off + oftrq_size_get(r);

	r = dynarray_getpos(oftrqs, 0);
	size -= r->oftrq_off;

	psc_info("array %p has size=%zu", oftrqs, size);
	
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
	memcpy(&mq->sbdb, &bmap_2_msbd(b)->msbd_bdb, sizeof(mq->sbdb));

	return (req);
}

__static void
bmap_flush_inflight_ref(const struct offtree_req *r)
{
	struct offtree_iov *v;
	int i;

	DEBUG_OFFTREQ(PLL_INFO, r, "set inc");

	for (i=0; i < dynarray_len(r->oftrq_darray); i++) {
		v = dynarray_getpos(r->oftrq_darray, i);
		DEBUG_OFFTIOV(PLL_INFO, v, "set inc");
		slb_inflight_cb(v, SL_INFLIGHT_INC);
	}
}


__static void
bmap_flush_send_rpcs(struct dynarray *oftrqs, struct iovec *iovs, 
		     int niovs)
{
	struct pscrpc_request *req;
	struct pscrpc_import *imp;
	struct offtree_req *r;
	struct bmapc_memb *b;
	off_t soff;
	size_t size;
	int i;
	
	r = dynarray_getpos(oftrqs, 0);
	imp = msl_bmap_to_import((struct bmapc_memb *)r->oftrq_bmap, 1);
	psc_assert(imp);

	b = r->oftrq_bmap;
	soff = r->oftrq_off;

	for (i=0; i < dynarray_len(oftrqs); i++) {
		/* All oftrqs should have the same import, otherwise
		 *   there is a major problem.
		 */
		r = dynarray_getpos(oftrqs, i);
		psc_assert(imp == msl_bmap_to_import(r->oftrq_bmap, 0));
		psc_assert(b == r->oftrq_bmap);
		bmap_flush_inflight_ref(r);
	}

	DEBUG_OFFTREQ(PLL_INFO, r, "oftrq array cb arg (%p)", oftrqs);

	if ((size = bmap_flush_coalesce_size(oftrqs)) <= LNET_MTU) {
		/* Single rpc case.  Set the appropriate cb handler
		 *   and attach to the nb request set.
		 */
		req = bmap_flush_create_rpc(b, iovs, size, soff, niovs);
		/* Set the per-req cp arg for the nbreqset cb handler.
		 *   oftrqs MUST be freed by the cb.
		 */
		req->rq_async_args.pointer_arg[0] = oftrqs;
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
		/* oftrqs MUST be freed by the cb.
		 */
		set->set_arg = oftrqs;		
		
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
				n = 0;

			} else {
				if (!tiov)
					tiov = &iovs[j];
				size += iovs[j].iov_len;
				n++;
			}
		}
		/* Launch any small, lingerers.
		 */
		if (tiov) {
			psc_assert(n);
			launch_rpc;
		}

		dynarray_add(&pndgReqSets, set);
	}
}

__static int 
bmap_flush_oftrq_cmp(const void *x, const void *y)
{
	const struct offtree_req *a = *(const struct offtree_req **)x;
	const struct offtree_req *b = *(const struct offtree_req **)y;

	//DEBUG_OFFTREQ(PLL_TRACE, a, "compare..");
	//DEBUG_OFFTREQ(PLL_TRACE, b, "..compare");

	if (a->oftrq_off < b->oftrq_off)
		return (-1);

	else if	(a->oftrq_off > b->oftrq_off)
		return (1);

	else {
		/* Larger requests with the same start offset
		 *  should have ordering priority.
		 */
		if (a->oftrq_nblks > b->oftrq_nblks)
			return (-1);

		else if (a->oftrq_nblks < b->oftrq_nblks)
			return (1);
	}		
	return (0);
}

__static int
bmap_flush_coalesce_map(const struct dynarray *oftrqs, struct iovec **iovset)
{
	struct offtree_req *r, *t;
	struct offtree_iov *v;
	struct offtree_memb *m;
	struct iovec *iovs=NULL;
	off_t off;
	int i, j, niovs=0, skip;

	psc_assert(!*iovset);
	psc_assert(dynarray_len(oftrqs) > 0);
	
	/* Prime the pump with initial values from the first oftrq.
	 */
	r = dynarray_getpos(oftrqs, 0);
	off = r->oftrq_off;
	
	for (i=0; i < dynarray_len(oftrqs); i++, r=t) {
		t = dynarray_getpos(oftrqs, i);
		
		DEBUG_OFFTREQ(PLL_INFO, t, "pos=%d off=%zu", i, off);
		
		if ((off - t->oftrq_off) % slCacheBlkSz)
			DEBUG_OFFTREQ(PLL_FATAL, t, "invalid off=%zu", off);

		psc_assert(dynarray_len(t->oftrq_darray));
		
		if (oftrq_voff_get(t) <= off)
			continue;
		/* Now iterate through the oftrq's iov set, where the
		 *   actual buffers are stored.  
		 */ 
		for (j=0, skip=1; j < dynarray_len(t->oftrq_darray); j++) {
			v = dynarray_getpos(t->oftrq_darray, j);

			if ((OFT_IOV2E_VOFF_(v) <= off)) {
				if (skip)
					continue;
				else
					/* Only iov's at the beginning of 
					 *   the set may skipped or 
					 *   partially used.
					 */
					abort();
			}			
			/* Add a new iov!
			 */
			*iovset = iovs = PSC_REALLOC(iovs, 
				     (sizeof(struct iovec) * (niovs + 1)));
			/* Set the base pointer past the overlapping 
			 *   area.
			 */
			iovs[niovs].iov_base = v->oftiov_base + 
				(off - v->oftiov_off);
			
			iovs[niovs].iov_len = OFT_IOVSZ(v) - 
				(off - v->oftiov_off);
			
			psc_info("oftrq=%p oftiov=%p base=%p len=%zu niov=%d",
				 t, v, iovs[niovs].iov_base, 
				 iovs[niovs].iov_len, niovs);
			
			if (skip)
				 skip = 0;

			niovs++;
			/* Signify that the ending offset has been extended.
			 */			
			OFFTIOV_LOCK(v);
			m = v->oftiov_memb;
			off = OFT_IOV2E_VOFF_(v);
			v->oftiov_flags |= (OFTIOV_PUSHING | OFTIOV_PUSHPNDG);
			if (v->oftiov_memb != m)
				abort();
			OFFTIOV_ULOCK(v);

			DEBUG_OFFTIOV(PLL_INFO, v, "pos=%d off=%zu", j, off);
			//slb_inflight_cb(v, SL_INFLIGHT_INC);
		}
 	}
	return (niovs);
}

__static struct dynarray * 
bmap_flush_trycoalesce(const struct dynarray *oftrqs, int *offset)
{
	int i, off, expired=0;
	struct offtree_req *r=NULL, *t;
	struct dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(dynarray_len(oftrqs) > *offset);

	for (off=0; (off + *offset) < dynarray_len(oftrqs); off++) {
		t = dynarray_getpos(oftrqs, off + *offset);
		psc_assert(!(t->oftrq_flags & OFTREQ_INFLIGHT));

		if (r)
			psc_assert(t->oftrq_off >= r->oftrq_off);
		/* If any member is expired then we'll push everything out.
		 */
		if (!expired)
			expired = bmap_flush_oftrq_expired(t);

		DEBUG_OFFTREQ(PLL_TRACE, t, "oftrq #%d (expired=%d)", 
			      off, expired);

		if (!r || t->oftrq_off <= oftrq_voff_get(r)) {
			dynarray_add(&b, t);
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
			t->oftrq_flags |= OFTREQ_INFLIGHT;
			DEBUG_OFFTREQ(PLL_TRACE, t, "oftrq #%d (inflight)", i);
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
	struct msbmap_data *msbd;
	struct dynarray a=DYNARRAY_INIT, bmaps=DYNARRAY_INIT, *oftrqs;
	struct offtree_req *r;
	struct psclist_head *h;
	struct iovec *iovs=NULL;
	int i=0, niovs;

	/* Send
	 */
	while ((atomic_read(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS) && 
	       (msbd = lc_getnb(&bmapFlushQ))) {

		b = msbd->msbd_bmap;
		BMAP_LOCK(b);
		DEBUG_BMAP(PLL_INFO, b, "try flush (outstandingRpcCnt=%d)", 
			   atomic_read(&outstandingRpcCnt));
		
		h = &msbd->msbd_oftrqs;
		
		if (b->bcm_mode & BMAP_DIRTY) {
			psc_assert(!psclist_empty(h));
			dynarray_add(&bmaps, msbd);
		} else {
			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			psc_assert(psclist_empty(h));
			BMAP_ULOCK(b);
			continue;
		}
		/* Ok, have something to do.
		 */
		dynarray_reset(&a);
	
		psclist_for_each_entry(r, h, oftrq_lentry) {
			if (r->oftrq_flags & OFTREQ_INFLIGHT)
				continue;
			DEBUG_OFFTREQ(PLL_TRACE, r, "try flush");
			dynarray_add(&a, r);
		}
	
		BMAP_ULOCK(b);

		if (!dynarray_len(&a)) {
			dynarray_free(&a);
			break;
		}
		/* Sort the items by their offsets.
		 */
		qsort(a.da_items, a.da_pos, sizeof(void *),
		      bmap_flush_oftrq_cmp);
	
#if 0
		for (i=0; i < dynarray_len(&a); i++) {
			r = dynarray_getpos(&a, i);
			DEBUG_OFFTREQ(PLL_TRACE, r, "sorted?");
		}
#endif

		i=0;
		while (i < dynarray_len(&a) && 
		       (oftrqs = bmap_flush_trycoalesce(&a, &i))) {
			/* Note: 'oftrqs' must be freed!!
			 */
			niovs = bmap_flush_coalesce_map(oftrqs, &iovs);
			psc_assert(niovs);
			/* Have a set of iov's now.  Let's create an rpc
			 *   or rpc set and send it out.
			 */
			bmap_flush_send_rpcs(oftrqs, iovs, niovs);
			PSCFREE(iovs);
		}	
	}
	
	for (i=0; i < dynarray_len(&bmaps); i++) {		
		msbd = dynarray_getpos(&bmaps, i);
		h = &msbd->msbd_oftrqs;
		b = msbd->msbd_bmap;
		BMAP_LOCK(b);

		if (!psclist_empty(h)) {
			psc_assert(b->bcm_mode & BMAP_DIRTY);
			BMAP_ULOCK(b);
                        DEBUG_BMAP(PLL_INFO, b, "restore to dirty list");
                        lc_addtail(&bmapFlushQ, msbd);

		} else {
			psc_assert(!(b->bcm_mode & BMAP_DIRTY));
			BMAP_ULOCK(b);

			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			continue;
		}
	}
	dynarray_free(&bmaps);
}

void *
bmap_flush_thr(__unusedx void *arg)
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
bmap_flush_init(void)
{
	pndgReqs = nbreqset_init(NULL, msl_io_rpc_cb);
	atomic_set(&outstandingRpcCnt, 0);
	atomic_set(&completedRpcCnt, 0);	

	lc_reginit(&bmapFlushQ, struct msbmap_data, msbd_lentry, 
		   "bmap_flush_queue");

	pscthr_init(MSTHRT_BMAPFLSH, 0, bmap_flush_thr, NULL, 0,
		    "bmap_flush");
}
