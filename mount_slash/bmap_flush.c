/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/fcntl.h"
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

struct timespec			 bmapFlushDefMaxAge = { 0,   1000000L };	/* one millisecond */
struct timespec			 bmapFlushWaitTime  = { 0, 100000000L };	/* 100 milliseconds */

struct psc_listcache		 bmapFlushQ;
struct psc_listcache		 bmapReadAheadQ;
struct psc_listcache		 bmapTimeoutQ;
struct pscrpc_completion	 rpcComp;

__static struct pscrpc_nbreqset	*pndgReqs;
__static struct psc_listcache	 pndgReqSets;
__static atomic_t		 outstandingRpcCnt;

#define MAX_OUTSTANDING_RPCS	128
#define MIN_COALESCE_RPC_SZ	LNET_MTU /* Try for big RPC's */

struct psc_waitq		bmapflushwaitq = PSC_WAITQ_INIT;
psc_spinlock_t			bmapflushwaitqlock = SPINLOCK_INIT;

void bmap_flush_inflight_unset(struct bmpc_ioreq *);

__static void
bmap_flush_reap_rpcs(void)
{
	struct pscrpc_request_set *set;	

	psclog_debug("outstandingRpcCnt=%d (before) rpcComp.rqcomp_compcnt=%d",
	    atomic_read(&outstandingRpcCnt), atomic_read(&rpcComp.rqcomp_compcnt));

	/* Only this thread may pull from pndgReqSets listcache
	 *   therefore it can never shrink except by way of this
	 *   routine.  
	 * Note:  set failures are handled by set_interpret()
	 * 
	 * XXX this looks problematic.. I think that pscrpc_set_finalize()
	 *   should not block here at all, and hence, the 'set' should
	 *   not be removed from the LC.   A pscrpc_completion could be 
	 *   put in place here.  Note that pscrpc_nbreqset_reap() also sleeps
	 *   for a second.
	 */
	while ((set = lc_getnb(&pndgReqSets)))
		pscrpc_set_finalize(set, 0, 1);

	pscrpc_nbreqset_reap(pndgReqs);

	psclog_debug("outstandingRpcCnt=%d (after)",
		 atomic_read(&outstandingRpcCnt));
}

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a)
{
	struct timespec ts;

	if (a->biorq_flags & BIORQ_FORCE_EXPIRE)
		return (1);

	PFL_GETTIMESPEC(&ts);

	if (a->biorq_issue.tv_sec < ts.tv_sec)
		return (1);

	else if (a->biorq_issue.tv_sec > ts.tv_sec)
		return (0);

	if (a->biorq_issue.tv_nsec <= ts.tv_nsec)
		return (1);

	return (0);
}

int
msl_fd_offline_retry(struct msl_fhent *mfh)
{
	struct psc_thread *thr;
	int *cnt;

	thr = pscthr_get();
	if (thr->pscthr_type == MSTHRT_FS)
		cnt = &msfsthr(thr)->mft_failcnt;
	else
		cnt = &msbmflthr(thr)->mbft_failcnt;
	psc_assert(*cnt);
	if (mfh->mfh_oflags & O_NONBLOCK)
		return (0);
	if (++*cnt >= 10)
		return (0);
	return (1);
}

int
_msl_offline_retry(struct bmpc_ioreq *r, int ignore_expire)
{
	//XXX bmap_flush_biorq_expired() is not used properly here.
	if (!ignore_expire && bmap_flush_biorq_expired(r))
		return (0);
	return (msl_fd_offline_retry(r->biorq_fhent));
}

/**
 * bmap_flush_coalesce_size - This function determines the size of the
 *	region covered by an array of requests.  Note that these
 *	requests can overlap in various ways.  But they have already
 *	been ordered based on their offsets.
 */
__static size_t
bmap_flush_coalesce_size(const struct psc_dynarray *biorqs)
{
	struct bmpc_ioreq *r, *s, *e;
	size_t size;
	off_t off=0;
	int i=0;

	e = s = NULL; /* gcc */

	if (!psc_dynarray_len(biorqs))
		return (0);

	DYNARRAY_FOREACH(r, i, biorqs) {
		off = r->biorq_off;

		if (!i)
			s = e = r;
		else {
			/* Biorq offsets may not decrease.
			 */
			psc_assert(r->biorq_off >= off);
			/* Holes are not allowed.
			 */
			psc_assert(r->biorq_off <= biorq_voff_get(e));

			if (biorq_voff_get(r) > biorq_voff_get(e))
				e = r;
		}
	}

	size = (e->biorq_off - s->biorq_off) + e->biorq_len;

	psc_info("array %p has size=%zu array len=%d",
		 biorqs, size, psc_dynarray_len(biorqs));

	return (size);
}

__static int
bmap_flush_rpc_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[0];
	int rc;

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	
	atomic_dec(&outstandingRpcCnt);
	DEBUG_REQ(rq->rq_err ? PLL_ERROR : PLL_INFO, rq, 
		  "done (outstandingRpcCnt=%d)",
		  atomic_read(&outstandingRpcCnt));

	if (atomic_read(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS / 2)
		psc_waitq_wakeall(&bmapflushwaitq);

	sl_csvc_decref(csvc);
	args->pointer_arg[0] = NULL;

	return (rc);
}

__static struct pscrpc_request *
bmap_flush_create_rpc(void *set, struct bmpc_ioreq *r,
    struct bmapc_memb *b, struct iovec *iovs, size_t size, off_t soff,
    int niovs, struct psc_dynarray *biorqs)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int rc;

 retry:
	csvc = msl_bmap_to_csvc(b, 1);
	if (csvc == NULL)
		goto error;

	rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
	if (rc)
		goto error;

	rc = rsx_bulkclient(rq, BULK_GET_SOURCE, SRIC_BULK_PORTAL, iovs,
	    niovs);
	if (rc)
		goto error;

	atomic_inc(&outstandingRpcCnt);

	rq->rq_interpret_reply = bmap_flush_rpc_cb;
	rq->rq_async_args.pointer_arg[0] = csvc;
	rq->rq_comp = &rpcComp;

	mq->offset = soff;
	mq->size = size;
	mq->op = SRMIOP_WR;

	DEBUG_REQ(PLL_INFO, rq, "off=%u sz=%u op=%u",
	    mq->offset, mq->size, mq->op);

	memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd, sizeof(mq->sbd));
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	if (set == pndgReqs) {
		/* biorqs will be freed by the nbreqset callback. */
		rq->rq_async_args.pointer_arg[1] = biorqs;
		if (pscrpc_nbreqset_add(set, rq))
			goto error;
	} else {
		pscrpc_set_add_new_req(set, rq);
		rc = pscrpc_push_req(rq);
		if (rc) {
			DEBUG_REQ(PLL_ERROR, rq, "send failure: %s",
			    strerror(rc));
			pscrpc_set_remove_req(set, rq);
			goto error;
		}
	}

	return (rq);

 error:
	if (rq) {
		pscrpc_req_finished_locked(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}
	if (msl_offline_retry(r))
		goto retry;
	return (NULL);
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
	/* Limit the amount of scanning done by this
	 *   thread.  Move pending biorqs out of the way.
	 */
	BMPC_LOCK(bmpc);
	pll_remove(&bmpc->bmpc_new_biorqs, r);
	pll_addtail(&bmpc->bmpc_pndg_biorqs, r);
	BMPC_ULOCK(bmpc);
}

void
bmap_flush_inflight_unset(struct bmpc_ioreq *r)
{
	struct bmap_pagecache *bmpc;

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);
	r->biorq_flags &= ~BIORQ_INFL;
	DEBUG_BIORQ(PLL_WARN, r, "unset inflight XXX is my lease still valid?");
	freelock(&r->biorq_lock);

	bmpc = bmap_2_bmpc(r->biorq_bmap);
	BMPC_LOCK(bmpc);
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);
	pll_addtail(&bmpc->bmpc_new_biorqs, r);
	BMPC_ULOCK(bmpc);
}

__static void
bmap_flush_send_rpcs(struct psc_dynarray *biorqs, struct iovec *iovs,
    int niovs)
{
	struct slashrpc_cservice *csvc, *tcsvc;
	struct pscrpc_request_set *set = NULL;
	struct pscrpc_request *rq;
	struct bmpc_ioreq *r;
	struct bmapc_memb *b;
	int rc, i, nrpcs = 0;
	size_t size;
	off_t soff;

 retry:
	r = psc_dynarray_getpos(biorqs, 0);
	csvc = msl_bmap_to_csvc(r->biorq_bmap, 1);
	if (csvc == NULL)
		goto csvc_error;

	b = r->biorq_bmap;
	soff = r->biorq_off;

	DYNARRAY_FOREACH(r, i, biorqs) {
		/* All biorqs should have the same import, otherwise
		 *   there is a major problem.
		 */
		tcsvc = msl_bmap_to_csvc(r->biorq_bmap, 1);
		psc_assert(csvc == tcsvc);
		sl_csvc_decref(tcsvc);

		psc_assert(b == r->biorq_bmap);
		bmap_flush_inflight_set(r);
	}

	sl_csvc_decref(csvc);

	r = psc_dynarray_getpos(biorqs, 0);

	DEBUG_BIORQ(PLL_INFO, r, "biorq array cb arg (%p)", biorqs);

	size = bmap_flush_coalesce_size(biorqs);
	if (size <= LNET_MTU && niovs <= PSCRPC_MAX_BRW_PAGES) {
		/* Single RPC case.  Set the appropriate cb handler
		 *   and attach to the non-blocking request set.
		 */
		rq = bmap_flush_create_rpc(pndgReqs, r, b, iovs, size,
		    soff, niovs, biorqs);
		if (rq == NULL)
			goto error;
		nrpcs++;
	} else {
		/* Deal with a multiple RPC operation */
		struct iovec *tiov;
		int n, j;

		size = 0;
		set = pscrpc_prep_set();
		set->set_interpret = msl_io_rpcset_cb;
		/* biorqs must be freed by the cb. */
		set->set_arg = biorqs;

#define LAUNCH_RPC()							\
	do {								\
		rq = bmap_flush_create_rpc(set, r, b, tiov, size,	\
		    soff, n, NULL);					\
		if (rq == NULL)						\
			goto error;					\
		soff += size;						\
		nrpcs++;						\
	} while (0)

		for (j=0, n=0, size=0, tiov=iovs; j < niovs; j++) {
			if ((size + iovs[j].iov_len) == LNET_MTU) {
				n++;
				size += iovs[j].iov_len;
				LAUNCH_RPC();
				tiov = NULL;
				size = n = 0;

			} else if (((size + iovs[j].iov_len) > LNET_MTU) ||
				   (n == PSCRPC_MAX_BRW_PAGES)) {
				psc_assert(n > 0);
				LAUNCH_RPC();
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
			LAUNCH_RPC();
		}
		lc_addtail(&pndgReqSets, set);
	}
	return;

 error:
	if (set) {
		spinlock(&set->set_lock);
		if (psc_listhd_empty(&r->biorq_rqset->set_requests)) {
			pscrpc_set_destroy(set);
			r->biorq_rqset = NULL;
		} else
			freelock(&set->set_lock);
	}
	DYNARRAY_FOREACH(r, i, biorqs)
		bmap_flush_inflight_unset(r);

 csvc_error:
	r = psc_dynarray_getpos(biorqs, 0);

	/*
	 * If bmap is still leased, try to reconnect if such behavior is
	 * requested.
	 */
	rc = msl_offline_retry(r);
	if (rc)
		goto retry;

	/*
	 * We may have not reconnected due to bmap expiration.  In this
	 * case, if the application still requests reconnect behavior,
	 * we should renew the bmap lease.
	 */
	rc = msl_offline_retry_ignexpire(r);
	if (rc) {
		/* async refetch lease */
		//move work to end of flushq
		return;
	}

	r->biorq_fhent->mfh_flush_rc = EIO;
}

__static int
bmap_flush_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq * const *pa = x, *a = *pa;
	const struct bmpc_ioreq * const *pb = y, *b = *pb;

	//DEBUG_BIORQ(PLL_TRACE, a, "compare..");
	//DEBUG_BIORQ(PLL_TRACE, b, "..compare");

	if (a->biorq_off == b->biorq_off)
		/* Larger requests with the same start offset should have
		 *   ordering priority.
		 */
		return (CMP(b->biorq_len, a->biorq_len));
	return (CMP(a->biorq_off, b->biorq_off));
}

/**
 * bmap_flush_coalesce_map - Scan the given list of bio requests and
 *	construct I/O vectors out of them.  One I/O vector is limited to
 *	one page.
 */
__static int
bmap_flush_coalesce_map(const struct psc_dynarray *biorqs,
			struct iovec **iovset)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache_entry *bmpce;
	struct iovec *iovs=NULL;
	int i, j, niovs=0, first_iov;
	uint32_t tot_reqsz=bmap_flush_coalesce_size(biorqs), reqsz;
	off_t off = 0; /* gcc */

	psclog_trace("ENTRY: biorqs=%p tot_reqsz=%u", biorqs, tot_reqsz);

	psc_assert(!*iovset);
	psc_assert(psc_dynarray_len(biorqs) > 0);

	for (i=0; i < psc_dynarray_len(biorqs); i++) {
		r = psc_dynarray_getpos(biorqs, i);

		if (!i)
			off = r->biorq_off;

		DEBUG_BIORQ(PLL_INFO, r, "r tot_reqsz=%u off=%"PSCPRIdOFFT,
		    tot_reqsz, off);
		psc_assert(psc_dynarray_len(&r->biorq_pages));

		if (biorq_voff_get(r) <= off) {
			/* No need to map this one, its data has been
			 *   accounted for but first ensure that all of the
			 *   pages have been scheduled for IO.
			 */
			for (j=0; j < psc_dynarray_len(&r->biorq_pages); j++) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages, j);
				psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
			}
			DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (skip)", i);
			continue;
		}
		DEBUG_BIORQ(PLL_INFO, r, "t pos=%d (use)", i);
		reqsz = r->biorq_len;
		psc_assert(tot_reqsz);
		/* Now iterate through the biorq's iov set, where the
		 *   actual buffers are stored.  Note that this dynarray
		 *   is sorted.
		 */
		for (j = 0, first_iov = 1;
		    j < psc_dynarray_len(&r->biorq_pages); j++) {
			psc_assert(reqsz);

			bmpce = psc_dynarray_getpos(&r->biorq_pages, j);
			BMPCE_LOCK(bmpce);
			/*
			 * We might round down the offset of an I/O request
			 * to the start offset of the previous page.
			 */
			if ((bmpce->bmpce_off <= r->biorq_off) && j)
				abort();

			/*
			 * We might straddle the end offset of the
			 * previously scheduled I/O request.
			 */
			if (off - bmpce->bmpce_off >= BMPC_BUFSZ) {
				/* Similar case to the 'continue' stmt above,
				 *   this bmpce overlaps a previously
				 *   scheduled biorq.
				 */
				DEBUG_BMPCE(PLL_INFO, bmpce, "skip");
				psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
				BMPCE_ULOCK(bmpce);
				psc_assert(first_iov == 1);

				if (j == 0)
					reqsz -= BMPC_BUFSZ -
					    (r->biorq_off - bmpce->bmpce_off);
				else
					reqsz -= BMPC_BUFSZ;
				continue;
			}
			DEBUG_BMPCE(PLL_INFO, bmpce,
			    "scheduling, first_iov=%d", first_iov);

			/* Issue sanity checks on the bmpce.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
			    (first_iov ? (r->biorq_off & ~BMPC_BUFMASK) : off));

			BMPCE_ULOCK(bmpce);
			/* Add a new iov!
			 */
			*iovset = iovs = PSC_REALLOC(iovs,
			    sizeof(struct iovec) * (niovs + 1));

			/* Set the base pointer past the overlapping
			 *   area if this is the first mapping.
			 */
			iovs[niovs].iov_base = bmpce->bmpce_base +
			    (first_iov ? (off - bmpce->bmpce_off) : 0);

			iovs[niovs].iov_len = MIN(reqsz,
			    (first_iov ? BMPC_BUFSZ - (off - bmpce->bmpce_off) :
			     BMPC_BUFSZ));

			off += iovs[niovs].iov_len;
			reqsz -= iovs[niovs].iov_len;
			tot_reqsz -= iovs[niovs].iov_len;

			if (first_iov)
				first_iov = 0;

			psclog_info("biorq=%p bmpce=%p base=%p "
			    "len=%zu niov=%d reqsz=%u tot_reqsz=%u(new)",
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
	int rc = 0;

	bmpce = (r->biorq_flags & BIORQ_RBWFP) ?
	    psc_dynarray_getpos(&r->biorq_pages, 0) :
	    psc_dynarray_getpos(&r->biorq_pages,
		psc_dynarray_len(&r->biorq_pages)- 1);

	BMPCE_LOCK(bmpce);
	if (bmpce->bmpce_flags & BMPCE_DATARDY)
		rc = 1;
	BMPCE_ULOCK(bmpce);

	return (rc);
}

static inline int
bmap_flushready(const struct psc_dynarray *biorqs)
{
	int ready=0;

	psc_assert(psc_dynarray_len(biorqs) <= PSCRPC_MAX_BRW_PAGES);

	if ((bmap_flush_coalesce_size(biorqs) >= MIN_COALESCE_RPC_SZ) ||
	    psc_dynarray_len(biorqs) == PSCRPC_MAX_BRW_PAGES)
		ready = 1;

	return (ready);
}

/**
 * bmap_flushable - Check if we can flush the given bmpc (either an I/O
 *	request has expired or we have accumulated a big enough I/O).
 *	This function must be non-blocking.
 */
__static int
bmap_flushable(struct bmapc_memb *b)
{
	uint32_t off;
	int count, contig, flush;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *start, *end, *tmp;

	off = 0;
	count = 0; /* gcc */
	flush = 0;
	start = end = NULL;
	bmpc = bmap_2_bmpc(b);
	PLL_LOCK(&bmpc->bmpc_new_biorqs);
	PLL_FOREACH_SAFE(r, tmp, &bmpc->bmpc_new_biorqs) {

		spinlock(&r->biorq_lock);
		DEBUG_BIORQ(PLL_NOTICE, r, "consider for flush");

		psc_assert(r->biorq_off >= off);
		off = r->biorq_off;

		psc_assert(!(r->biorq_flags & BIORQ_READ));
		psc_assert(!(r->biorq_flags & BIORQ_DESTROY));

		if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
			DEBUG_BIORQ(PLL_INFO, r, "data not ready");
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

		if (bmap_flush_biorq_expired(r)) {
			flush = 1;
			freelock(&r->biorq_lock);
			break;
		}
		freelock(&r->biorq_lock);

		if (start == NULL) {
			count = 1;
			contig = 1;
			start = end = r;
		} else {
			contig = 0;
			if (r->biorq_off <= biorq_voff_get(end)) {
				contig = 1;
				count++;
				if (biorq_voff_get(r) > biorq_voff_get(end))
					end = r;
			}
		}
		/*
		 * XXX if the current request is contained completely
		 * within a previous request, should we count them
		 * separately?
		 */
		if (count == PSCRPC_MAX_BRW_PAGES) {
			flush = 1;
			break;
		}
		if (end->biorq_off - start->biorq_off + end->biorq_len >=
		    MIN_COALESCE_RPC_SZ) {
			flush = 1;
			break;
		}
		/*
		 * Not contiguous, start a new region.
		 */
		if (!contig) {
			count = 1;
			start = end = r;
		}
	}
	PLL_ULOCK(&bmpc->bmpc_new_biorqs);
	return (flush);
}

/**
 * bmap_flush_trycoalesce - Scan the given array of i/o requests for
 *	candidates to flush.  We *only* flush when (1) a request has
 *	aged out or (2) we can construct a large enough I/O.
 */
__static struct psc_dynarray *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *index)
{
	int i, idx, large=0, expired=0;
	struct bmpc_ioreq *r=NULL, *t;
	struct psc_dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(psc_dynarray_len(biorqs) > *index);

	for (idx=0; (idx + *index) < psc_dynarray_len(biorqs); idx++) {
		t = psc_dynarray_getpos(biorqs, idx + *index);

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

		DEBUG_BIORQ(PLL_NOTICE, t, "biorq #%d (expired=%d) nfrags=%d",
			    idx, expired, psc_dynarray_len(&b));

		/* The next request, 't', can be added to the coalesce
		 *   group either because 'r' is not yet set (meaning
		 *   the group is empty) or because 't' overlaps or
		 *   extends 'r'.
		 */
		if (t->biorq_off <= biorq_voff_get(r)) {
			psc_dynarray_add(&b, t);
			if (biorq_voff_get(t) > biorq_voff_get(r))
				/* If 't' is a larger extent then set
				 *   'r' to 't'.
				 */
				r = t;
		} else {
			/* This biorq is not contiguous with the previous.
			 *    If the current set is expired send it out now.
			 *    Otherwise, deschedule the current set and
			 *    resume activity with 't' as the base.
			 */
			if (expired)
				break;
			else {
				r = t;

				DYNARRAY_FOREACH(t, i, &b) {
					spinlock(&t->biorq_lock);
					DEBUG_BIORQ(PLL_INFO, t,
						    "descheduling");
					t->biorq_flags &= ~BIORQ_SCHED;
					freelock(&t->biorq_lock);
				}
				psc_dynarray_free(&b);
				psc_dynarray_add(&b, r);
			}
		}
		if (bmap_flushready(&b)) {
			idx++;
			large = 1;
			break;
		}
	}

	if (large || expired) {
		a = psc_alloc(sizeof(*a), 0);
		psc_dynarray_ensurelen(a, psc_dynarray_len(&b));
		for (i=0; i < psc_dynarray_len(&b); i++) {
			t = psc_dynarray_getpos(&b, i);
			psc_dynarray_add(a, psc_dynarray_getpos(&b, i));
		}

	} else {
		/* Clean up any lingering biorq's.
		 */
		for (i=0; i < psc_dynarray_len(&b); i++) {
			t = psc_dynarray_getpos(&b, i);
			spinlock(&t->biorq_lock);
			DEBUG_BIORQ(PLL_INFO, t, "descheduling");
			t->biorq_flags &= ~BIORQ_SCHED;
			freelock(&t->biorq_lock);
		}
	}

	*index += idx;
	psc_dynarray_free(&b);

	return (a);
}

/**
 * bmap_flush - Send out SRMT_WRITE RPCs to the I/O server.
 */
void
bmap_flush(void)
{
	struct psc_dynarray reqs = DYNARRAY_INIT_NOLOG,
	    *biorqs, bmaps = DYNARRAY_INIT_NOLOG;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *tmp;
	struct bmapc_memb *b, *tmpb;
	struct iovec *iovs = NULL;
	int i, j, niovs, nrpcs;

	i = 0;
	LIST_CACHE_LOCK(&bmapFlushQ);
	LIST_CACHE_FOREACH_SAFE(b, tmpb, &bmapFlushQ) {
		BMAP_LOCK(b);
		DEBUG_BMAP(PLL_INFO, b,
		    "checking for flush (outstandingRpcCnt=%d)",
		    atomic_read(&outstandingRpcCnt));
		psc_assert(b->bcm_flags & BMAP_CLI_FLUSHPROC);

		bmpc = bmap_2_bmpc(b);
		BMPC_LOCK(bmpc);
		if (!(b->bcm_flags & BMAP_DIRTY)) {
			psc_assert(!bmpc_queued_writes(bmpc));

			if (!bmpc_queued_ios(bmpc)) {
				/* No remaining reads or writes.  BMAP_TIMEOQ
				 *   and BMAP_CLI_FLUSHPROC are mutually
				 *   exclusive.
				 */
				psc_assert(!(b->bcm_flags & BMAP_TIMEOQ));
				lc_remove(&bmapFlushQ, b);
				b->bcm_flags |= BMAP_TIMEOQ;
				b->bcm_flags &= ~BMAP_CLI_FLUSHPROC;
				lc_addtail(&bmapTimeoutQ, b);
				DEBUG_BMAP(PLL_INFO, b,
				   "added to bmapTimeoutQ");
			}
			BMPC_ULOCK(bmpc);
			bcm_wake_locked(b);
			BMAP_ULOCK(b);
			continue;
		}
		BMPC_ULOCK(bmpc);
		if (bmap_flushable(b)) {
			i++;
			psc_dynarray_add(&bmaps, b);
			if (i >= MAX_OUTSTANDING_RPCS) {
				BMAP_ULOCK(b);
				break;
			}
		}
		BMAP_ULOCK(b);
	}
	LIST_CACHE_ULOCK(&bmapFlushQ);

	/*
	 * Note that new requests can sneak in between the two loops.
	 */
	for (i = 0; i < psc_dynarray_len(&bmaps); i++) {

		b = psc_dynarray_getpos(&bmaps, i);
		bmpc = bmap_2_bmpc(b);

		DEBUG_BMAP(PLL_INFO, b, "try flush (outstandingRpcCnt=%d)",
			   atomic_read(&outstandingRpcCnt));

		PLL_LOCK(&bmpc->bmpc_new_biorqs);
		PLL_FOREACH_SAFE(r, tmp, &bmpc->bmpc_new_biorqs) {
			spinlock(&r->biorq_lock);

			if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
				DEBUG_BIORQ(PLL_INFO, r, "data not ready");
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

			DEBUG_BIORQ(PLL_NOTICE, r, "try flush");
			psc_dynarray_add(&reqs, r);
		}
		PLL_ULOCK(&bmpc->bmpc_new_biorqs);

		j = 0;
		while (j < psc_dynarray_len(&reqs) &&
		    (biorqs = bmap_flush_trycoalesce(&reqs, &j))) {
			/* Note: 'biorqs' must be freed!!
			 */
			niovs = bmap_flush_coalesce_map(biorqs, &iovs);
			psc_assert(niovs);

			/* Have a set of iov's now.  Let's create an RPC
			 *   or RPC set and send it out.
			 */
			bmap_flush_send_rpcs(biorqs, iovs, niovs);
			PSCFREE(iovs);
		}
		psc_dynarray_reset(&reqs);

		nrpcs = MAX_OUTSTANDING_RPCS - atomic_read(&outstandingRpcCnt);
		if (nrpcs < 0) {
			psclog_notice("stall flush (nrpcs=%d, outstandingRpcCnt=%d)",
			    nrpcs, atomic_read(&outstandingRpcCnt));
			break;
			//psc_waitq_waitrel(&bmapflushwaitq, NULL, &bmapFlushWaitTime);
		}
	}

	psc_dynarray_free(&reqs);
	psc_dynarray_free(&bmaps);
}

static __inline void
bmap_2_bid(const struct bmapc_memb *b, struct srm_bmap_id *bid)
{
	const struct bmap_cli_info *bci = bmap_2_bci_const(b);

	bid->fid = fcmh_2_fid(b->bcm_fcmh);
	bid->seq = bci->bci_sbd.sbd_seq;
	bid->key = bci->bci_sbd.sbd_key;
	bid->bmapno = b->bcm_bmapno;
}

static void
msl_bmap_release(struct sl_resm *resm)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct resm_cli_info *rmci;
	uint32_t i;
	int rc;

	csvc = (resm == slc_rmc_resm) ?
	    slc_getmcsvc(resm) : slc_geticsvc(resm);
	if (csvc == NULL) {
		if (resm->resm_csvc)
			rc = resm->resm_csvc->csvc_lasterrno; /* XXX race */
		else
			rc = -ENOTCONN;
		goto out;
	}

	rmci = resm2rmci(resm);
	psc_assert(rmci->rmci_bmaprls.nbmaps);

	rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(mq, &rmci->rmci_bmaprls, sizeof(*mq));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;

	for (i = 0; i < rmci->rmci_bmaprls.nbmaps; i++)
		psclog_notice("fid="SLPRI_FID" bmap=%u key=%"PRId64
		    " seq=%"PRId64" rc=%d",
		    rmci->rmci_bmaprls.bmaps[i].fid,
		    rmci->rmci_bmaprls.bmaps[i].bmapno,
		    rmci->rmci_bmaprls.bmaps[i].key,
		    rmci->rmci_bmaprls.bmaps[i].seq,
		    mp ? mp->bidrc[i] : rc); /* mp could be NULL if !rc */
	rmci->rmci_bmaprls.nbmaps = 0;
 out:
	if (rc) {
		/* At this point the bmaps have already been purged from
		 *   our cache.  If the mds rls request fails then the
		 *   mds should time them out on his own.  In any case,
		 *   the client must reacquire leases to perform further
		 *   I/O on any bmap in this set.
		 */
		psc_errorx("bmap_release failed res=%s:nid=%s (rc=%d)", 
		    resm->resm_res->res_name, libcfs_nid2str(resm->resm_nid), 
		    rc);
	}
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
}

void
msbmaprlsthr_main(__unusedx struct psc_thread *thr)
{
	struct timespec ctime, wtime = { 0, 0 };
	struct psc_waitq waitq = PSC_WAITQ_INIT;
	struct psc_dynarray rels = DYNARRAY_INIT, skips = DYNARRAY_INIT;
	struct resm_cli_info *rmci;
	struct bmap_cli_info *bci;
	struct bmapc_memb *b;
	struct sl_resm *resm;
	int i, sortbypass = 0, sawnew=0;

#define SORT_BYPASS_ITERS		32
#define ITEMS_TRY_AFTER_UNEXPIRED	MAX_BMAP_RELEASE

	// just put the resm's in the dynarray. when pushing out the bid's
	//   assume an ion unless resm == slc_rmc_resm

	for (;;) {
		psclog_trace("msbmaprlsthr_main() top of loop");

		if (!sortbypass) {
			lc_sort(&bmapTimeoutQ, qsort, bmap_cli_timeo_cmp);
			sortbypass = SORT_BYPASS_ITERS;
		} else
			sortbypass--;

		PFL_GETTIMESPEC(&ctime);

		wtime.tv_sec = BMAP_CLI_TIMEO_INC;

		while ((b = lc_getnb(&bmapTimeoutQ))) {
			bci = bmap_2_bci(b);
			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

			BMAP_LOCK(b);
			DEBUG_BMAP(PLL_INFO, b, "timeoq try reap "
			    "(nbmaps=%zd) etime("PSCPRI_TIMESPEC")",
			    lc_sz(&bmapTimeoutQ),
			    PSCPRI_TIMESPEC_ARGS(&bci->bci_etime));

			if (!(b->bcm_flags & BMAP_TIMEOQ)) {
				DEBUG_BMAP(PLL_WARN, b,
				    "race detect, skip");
				BMAP_ULOCK(b);
				continue;
			}

			if (bmpc_queued_ios(&bci->bci_bmpc)) {
				b->bcm_flags &= ~BMAP_TIMEOQ;
				DEBUG_BMAP(PLL_NOTICE, b,
				    "descheduling from timeoq");
				BMAP_ULOCK(b);
				continue;
			}

			if (timespeccmp(&ctime, &bci->bci_etime, <)) {
				if (!sawnew)
					/*
					 * Set the wait time to
					 * (etime - ctime)
					 */
					timespecsub(&bci->bci_etime,
					    &ctime, &wtime);

				DEBUG_BMAP(PLL_DEBUG, b, "sawnew=%d", sawnew);
				psc_dynarray_add(&skips, b);
				BMAP_ULOCK(b);

				sawnew++;
				if (sawnew == ITEMS_TRY_AFTER_UNEXPIRED)
					break;
				else
					continue;
			}

			/* Maintain the lock, bmap_op_done_type() will take
			 *   it anyway.
			 */
			if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				/* Put me back on the end of the queue.
				 */
				DEBUG_BMAP(PLL_NOTICE, b, "skip due to ref");
				psc_dynarray_add(&skips, b);
				BMAP_ULOCK(b);

			} else {
				psc_assert(psc_atomic32_read(
				    &b->bcm_opcnt) == 1);
				/* Note that only this thread calls
				 *   msl_bmap_release() so no reentrancy
				 *   exist unless another rls thr is
				 *   introduced.
				 */
				psc_assert(!bmpc_queued_ios(&bci->bci_bmpc));

				if (b->bcm_flags & BMAP_WR) {
					/* Setup a msg to an ION.
					 */
					psc_assert(bmap_2_ion(b) !=
					    LNET_NID_ANY);

					resm = libsl_nid2resm(bmap_2_ion(b));
					rmci = resm2rmci(resm);

					psc_assert(bmap_2_ion(b) == resm->resm_nid);

					DEBUG_BMAP(PLL_INFO, b, "nid=%"PRIx64,
					   bmap_2_ion(b));
				} else {
					resm = slc_rmc_resm;
					rmci = resm2rmci(slc_rmc_resm);
				}

				psc_assert(rmci->rmci_bmaprls.nbmaps <
				    MAX_BMAP_RELEASE);

				bmap_2_bid(b, &rmci->rmci_bmaprls.bmaps[
				    rmci->rmci_bmaprls.nbmaps]);
				rmci->rmci_bmaprls.nbmaps++;

				/* The bmap should be going away now, this
				 *    will call BMAP_URLOCK().
				 */
				DEBUG_BMAP(PLL_NOTICE, b, "release");

				bmap_op_done_type(b, BMAP_OPCNT_REAPER);

				if (rmci->rmci_bmaprls.nbmaps ==
				    MAX_BMAP_RELEASE) {
					msl_bmap_release(resm);
					if (psc_dynarray_exists(&rels, resm))
						psc_dynarray_remove(&rels, resm);
				} else
					psc_dynarray_add_ifdne(&rels, resm);
			}
		}
		psclog_trace("out of find work loop (arraysz=%d) "
		    "wtime="PSCPRI_TIMESPEC,
		    psc_dynarray_len(&rels),
		    PSCPRI_TIMESPEC_ARGS(&wtime));

		/* Send out partially filled release request.
		 */
		DYNARRAY_FOREACH(resm, i, &rels)
			msl_bmap_release(resm);

		psc_dynarray_reset(&rels);

		DYNARRAY_FOREACH_REVERSE(b, i, &skips) {
			BMAP_LOCK(b);
			if (!(b->bcm_flags & BMAP_CLI_FLUSHPROC)) {
				psc_assert(!(b->bcm_flags & BMAP_DIRTY));
				psc_assert(b->bcm_flags & BMAP_TIMEOQ);
				if (!lc_conjoint(&bmapTimeoutQ, b))
					lc_addstack(&bmapTimeoutQ, b);
			}
			BMAP_ULOCK(b);
		}

		psc_dynarray_reset(&skips);

		if (!pscthr_run())
			break;

		if (!wtime.tv_sec && !wtime.tv_nsec)
			wtime.tv_sec = 1;

		if (!wtime.tv_sec)
			wtime.tv_nsec = MAX(wtime.tv_nsec, 100000000);

		psc_waitq_waitrel(&waitq, NULL, &wtime);

		wtime.tv_sec = wtime.tv_nsec = 0;
		sawnew = 0;
	}
	psc_dynarray_free(&rels);
	psc_dynarray_free(&skips);
}

void
msbmapflushthr_main(__unusedx struct psc_thread *thr)
{
	struct timespec ts0, ts1;

	while (pscthr_run()) {
		msbmflthr(pscthr_get())->mbft_failcnt = 1;
		PFL_GETTIMESPEC(&ts0);
		bmap_flush();
		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &ts1);		
		psc_info("bmap_flush "PSCPRI_TIMESPEC, 
		    PSCPRI_TIMESPEC_ARGS(&ts1));
		PFL_GETTIMESPEC(&ts0);
		psc_waitq_waitrel(&bmapflushwaitq, NULL, &bmapFlushWaitTime);
		PFL_GETTIMESPEC(&ts1);
		psc_info("post wakeup "PSCPRI_TIMESPEC, 
		    PSCPRI_TIMESPEC_ARGS(&ts1));
	}
}

void
msbmapflushthrrpc_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		pscrpc_completion_wait(&rpcComp);
		bmap_flush_reap_rpcs();
		/* At the moment, RA requests share the same
		 *    completion as bmap reaps.  Therefore,
		 *    this thread will deal with ra_nbreqset.
		 */
		pscrpc_nbreqset_reap(ra_nbreqset);
	}
}

void
msbmaprathr_main(__unusedx struct psc_thread *thr)
{
	struct bmap_pagecache_entry *bmpce;

	while (pscthr_run()) {
		bmpce = lc_getwait(&bmapReadAheadQ);
		BMPCE_LOCK(bmpce);
		msl_bmpce_getbuf(bmpce);
		psc_assert(bmpce->bmpce_base);
		psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
		bmpce->bmpce_flags &= ~BMPCE_INIT;
		bmpce->bmpce_flags |= BMPCE_READPNDG;
		BMPCE_ULOCK(bmpce);

		msl_reada_rpc_launch(bmpce);
	}
}

void
msbmapflushthr_spawn(void)
{
	struct msbmfl_thread *mbft;
	struct psc_thread *thr;
	int i;

	pndgReqs = pscrpc_nbreqset_init(NULL, msl_io_rpc_cb);
	pscrpc_completion_init(&rpcComp);
	atomic_set(&outstandingRpcCnt, 0);
	//psc_atomic32_set(&bmapflushforceexpired, 0);
	psc_waitq_init(&bmapflushwaitq);

	lc_reginit(&bmapFlushQ, struct bmapc_memb,
	    bcm_lentry, "bmapflush");

	lc_reginit(&bmapTimeoutQ, struct bmapc_memb,
	    bcm_lentry, "bmaptimeout");

	lc_reginit(&bmapReadAheadQ, struct bmap_pagecache_entry,
	    bmpce_ralentry, "bmapreadahead");

	lc_reginit(&pndgReqSets, struct pscrpc_request_set,
	    set_lentry, "bmappndgflushsets");

	for (i = 0; i < NUM_BMAP_FLUSH_THREADS; i++) {
		thr = pscthr_init(MSTHRT_BMAPFLSH, 0,
		    msbmapflushthr_main, NULL,
		    sizeof(struct msbmfl_thread), "msbflushthr%d", i);
		mbft = msbmflthr(thr);
		psc_multiwait_init(&mbft->mbft_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}

	pscthr_init(MSTHRT_BMAPFLSHRPC, 0, msbmapflushthrrpc_main,
	    NULL, 0, "msbflushrpcthr");

	thr = pscthr_init(MSTHRT_BMAPFLSHRLS, 0, msbmaprlsthr_main,
	    NULL, sizeof(struct msbmflrls_thread), "msbrlsthr");
	psc_multiwait_init(&msbmflrlsthr(thr)->mbfrlst_mw, "%s",
		   thr->pscthr_name);
	pscthr_setready(thr);

	for (i=0; i < 4; i++) {
		thr = pscthr_init(MSTHRT_BMAPREADAHEAD, 0, msbmaprathr_main,
			  NULL, sizeof(struct msbmflra_thread), "msbrathr%d", i);
		psc_multiwait_init(&msbmfrathr(thr)->mbfra_mw, "%s",
			   thr->pscthr_name);
		pscthr_setready(thr);
	}
}
