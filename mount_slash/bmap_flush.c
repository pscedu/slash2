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

struct timespec			 bmapFlushWaitSecs = { 1, 0L };
struct timespec			 bmapFlushDefMaxAge = { 0, 10000000L };	/* 10 milliseconds */
struct timespec			 bmapFlushWaitTime;
struct psc_listcache		 bmapFlushQ;
struct psc_listcache		 bmapReadAheadQ;
struct psc_listcache		 bmapTimeoutQ;
struct pscrpc_completion	 rpcComp;

struct pscrpc_nbreqset		*pndgBmaplsReqs;	/* bmap lease */
__static struct pscrpc_nbreqset	*pndgBmapRlsReqs;	/* bmap release */
__static struct pscrpc_nbreqset	*pndgWrtReqs;
__static struct psc_listcache	 pndgWrtReqSets;
__static atomic_t		 outstandingRpcCnt;

#define MAX_OUTSTANDING_RPCS	16
#define MIN_COALESCE_RPC_SZ	LNET_MTU /* Try for big RPC's */

struct psc_waitq		bmapFlushWaitq = PSC_WAITQ_INIT;
psc_spinlock_t			bmapFlushLock = SPINLOCK_INIT;
int				bmapFlushTimeoFlags = 0;

__static void
bmap_flush_reap_rpcs(void)
{
	struct pscrpc_request_set *set;
	struct psc_lockedlist hold =
		PLL_INIT(&hold, struct pscrpc_request_set, set_lentry);

	psclogs_debug(SLSS_BMAP, "outstandingRpcCnt=%d (before) "
	       "rpcComp.rqcomp_compcnt=%d", atomic_read(&outstandingRpcCnt),
	       atomic_read(&rpcComp.rqcomp_compcnt));

	/* Only this thread may pull from pndgWrtReqSets listcache
	 *   therefore it can never shrink except by way of this
	 *   routine.
	 * Note:  set failures are handled by set_interpret()
	 *
	 * XXX this looks problematic.. I think that pscrpc_set_finalize()
	 *   should not block here at all, and hence, the 'set' should
	 *   not be removed from the LC.  A pscrpc_completion could be
	 *   put in place here.  Note that pscrpc_nbreqset_reap() also
	 *   sleeps for a second.
	 */
	while ((set = lc_getnb(&pndgWrtReqSets)))
		if (pscrpc_set_finalize(set, 0, 1))
			pll_add(&hold, set);

	pscrpc_nbreqset_reap(pndgWrtReqs);

	while ((set = pll_get(&hold)))
		lc_add(&pndgWrtReqSets, set);

	psclogs_debug(SLSS_BMAP, "outstandingRpcCnt=%d (after) "
	       "rpcComp.rqcomp_compcnt=%d", atomic_read(&outstandingRpcCnt),
	       atomic_read(&rpcComp.rqcomp_compcnt));
}

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a, struct timespec *t)
{
	struct timespec ts;

	PFL_GETTIMESPEC(&ts);

	if ((a->biorq_flags & BIORQ_RESCHED &&
	     timespeccmp(&ts, &a->biorq_resched, <)) ||
	    (a->biorq_flags & BIORQ_FORCE_EXPIRE)    ||
	    (a->biorq_issue.tv_sec < ts.tv_sec       ||
	     (a->biorq_issue.tv_sec == ts.tv_sec &&
	      a->biorq_issue.tv_nsec <= ts.tv_nsec)))
	    return (1);

	if (t)
		*t = (a->biorq_flags & BIORQ_RESCHED) ?
			a->biorq_resched : a->biorq_issue;

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
	if (!ignore_expire && bmap_flush_biorq_expired(r, NULL))
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

	psclog_info("array %p has size=%zu array len=%d",
		 biorqs, size, psc_dynarray_len(biorqs));

	return (size);
}

void
_bmap_flushq_wake(const struct pfl_callerinfo *pci, int mode,
	 struct timespec *t)
{
	int wake = 0, tmp;

	spinlock(&bmapFlushLock);

	switch (mode) {
	case BMAPFLSH_EXPIRE:
		if (!(bmapFlushTimeoFlags & BMAPFLSH_RPCWAIT)) {
			bmapFlushTimeoFlags |= BMAPFLSH_EXPIRE;
			wake = 1;
		}
		break;

	case BMAPFLSH_RPCWAIT:
		if (bmapFlushTimeoFlags & BMAPFLSH_RPCWAIT) {
			bmapFlushTimeoFlags &= ~BMAPFLSH_RPCWAIT;
			wake = 1;
		}
		break;

	case BMAPFLSH_TIMEOA:
		psc_assert(t);
		if (!(bmapFlushTimeoFlags & BMAPFLSH_RPCWAIT) &&
		    (timespeccmp(t, &bmapFlushWaitTime, <))) {
			//bmapFlushTimeoFlags |= BMAPFLSH_TIMEOA;

			psclogs_info(SLSS_BMAP, "oldalarm=("PSCPRI_TIMESPEC") "
			     "newalarm=("PSCPRI_TIMESPEC")",
			     PSCPRI_TIMESPEC_ARGS(&bmapFlushWaitTime),
			     PSCPRI_TIMESPEC_ARGS(t));

			bmapFlushWaitTime = *t;
			wake = 1;
		}
		break;

	default:
		abort();
	}
	tmp = bmapFlushTimeoFlags;
	if (wake)
		psc_waitq_wakeall(&bmapFlushWaitq);

	freelock(&bmapFlushLock);

	psclog_info("mode=%x wake=%d flags=%x outstandingRpcCnt=%d",
	     mode, wake, tmp, atomic_read(&outstandingRpcCnt));
}

#define bmap_flush_rpccnt_dec() _bmap_flush_rpccnt_dec(PFL_CALLERINFOSS(SLSS_BMAP))

__static void
_bmap_flush_rpccnt_dec(const struct pfl_callerinfo *pci)
{
	if (atomic_dec_return(&outstandingRpcCnt) < MAX_OUTSTANDING_RPCS)
		_bmap_flushq_wake(pci, BMAPFLSH_RPCWAIT, NULL);

	psc_assert(atomic_read(&outstandingRpcCnt) >= 0);
}

__static int
bmap_flush_rpc_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);

	bmap_flush_rpccnt_dec();

	DEBUG_REQ(rq->rq_err ? PLL_ERROR : PLL_INFO, rq, "done rc=%d", rc);

	sl_csvc_decref(csvc);
	args->pointer_arg[MSL_CBARG_CSVC] = NULL;

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
	int rc, secs = 0;

 retry:
	rc = msl_bmap_lease_tryext(r->biorq_bmap, &secs, 0);
	if (rc)
		goto error;

	psc_assert(secs > 0);

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

	rq->rq_timeout = secs / 2;

	rq->rq_interpret_reply = bmap_flush_rpc_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_comp = &rpcComp;

	mq->offset = soff;
	mq->size = size;
	mq->op = SRMIOP_WR;
	//XXX mq->wseqno = GETSEQNO;

	DEBUG_REQ(PLL_INFO, rq, "off=%u sz=%u op=%u set=%p",
	    mq->offset, mq->size, mq->op, set);

	memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd, sizeof(mq->sbd));
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	if (set == pndgWrtReqs) {
		/* biorqs will be freed by the nbreqset callback. */
		rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQS] = biorqs;
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
	struct timespec t, s = { 0, 10000000L };
	struct bmap_pagecache *bmpc;
	int old = 0;

	PFL_GETTIMESPEC(&t);

	BIORQ_LOCK(r);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	timespecadd(&s, &r->biorq_issue, &s);

	r->biorq_flags |= BIORQ_INFL;
	if (!(r->biorq_flags & BIORQ_RESCHED)) {
		if (timespeccmp(&s, &t, <)) {
			old = 1;
			timespecsub(&t, &r->biorq_issue, &t);
		} else
			timespecsub(&r->biorq_issue, &t, &t);
	}
	BIORQ_ULOCK(r);
	DEBUG_BIORQ(old ? PLL_INFO : PLL_DEBUG, r, "set inflight %s("
	    PSCPRI_TIMESPEC")", old ? "expired: -" : "",
	    PSCPRI_TIMESPEC_ARGS(&t));

	bmpc = bmap_2_bmpc(r->biorq_bmap);
	/* Limit the amount of scanning done by this
	 *   thread.  Move pending biorqs out of the way.
	 */
	BMPC_LOCK(bmpc);
	pll_remove(&bmpc->bmpc_new_biorqs, r);
	pll_addtail(&bmpc->bmpc_pndg_biorqs, r);
	BMPC_ULOCK(bmpc);
}

#define bmap_flush_desched(r)						\
	_bmap_flush_desched(PFL_CALLERINFOSS(SLSS_BMAP), (r))

__static int
_bmap_flush_desched(const struct pfl_callerinfo *pci,
    struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int secs, rc, i;

	BIORQ_LOCK(r);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	r->biorq_flags &= ~BIORQ_SCHED;
	/* Don't spin in bmap_flush()
	 */
	r->biorq_flags |= BIORQ_RESCHED;
	PFL_GETTIMESPEC(&r->biorq_resched);
	r->biorq_resched.tv_sec += 1;
	BIORQ_ULOCK(r);

	rc = msl_bmap_lease_tryext(r->biorq_bmap, &secs, 0);

	DEBUG_BIORQ(rc ? PLL_ERROR : PLL_NOTIFY, r,
	    "unset sched lease is %s (rc=%d) (secs_rem=%d)",
	    rc ? "OK" : "INVALID", rc, secs);

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		bmpce->bmpce_flags &= ~BMPCE_INFLIGHT;
		BMPCE_ULOCK(bmpce);
	}
	return (rc);
}

/**
 * bmap_flush_resched - called in error contexts where
 *    the biorq must be rescheduled.
 */
int
_bmap_flush_resched(const struct pfl_callerinfo *pci,
    struct bmpc_ioreq *r)
{
	struct bmap_pagecache *bmpc;
	int rc;

	BIORQ_LOCK(r);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);
	r->biorq_flags &= ~BIORQ_INFL;
	BIORQ_ULOCK(r);

	rc = _bmap_flush_desched(pci, r);

	bmpc = bmap_2_bmpc(r->biorq_bmap);
	BMPC_LOCK(bmpc);
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);
	pll_add_sorted(&bmpc->bmpc_new_biorqs, r, bmpc_biorq_cmp);
	BMPC_ULOCK(bmpc);

	return (rc);
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
	int rc = 0, i;
	size_t size;
	off_t soff;

	// retry:
	r = psc_dynarray_getpos(biorqs, 0);
	csvc = msl_bmap_to_csvc(r->biorq_bmap, 1);
	if (csvc == NULL)
		goto error;

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
	size = bmap_flush_coalesce_size(biorqs);

	DEBUG_BIORQ(PLL_INFO, r, "biorq array cb arg (%p) size=%zu niovs=%d",
		    biorqs, size, niovs);

	if (size <= LNET_MTU && niovs <= PSCRPC_MAX_BRW_PAGES) {
		/* Single RPC case.  Set the appropriate cb handler
		 *   and attach to the non-blocking request set.
		 */
		rq = bmap_flush_create_rpc(pndgWrtReqs, r, b, iovs, size,
		    soff, niovs, biorqs);
		if (rq == NULL)
			goto error;
	} else {
		/* Deal with a multiple RPC operation */
		struct iovec *tiov;
		int n, j;

		size = 0;
		set = pscrpc_prep_set();
		set->set_interpret = msl_write_rpcset_cb;
		/* biorqs must be freed by the cb. */
		set->set_arg = biorqs;

#define LAUNCH_RPC()							\
	do {								\
		rq = bmap_flush_create_rpc(set, r, b, tiov, size,	\
		    soff, n, NULL);					\
		if (rq == NULL)						\
			goto error;					\
		soff += size;						\
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
		lc_addtail(&pndgWrtReqSets, set);
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
		rc = csvc ? bmap_flush_resched(r) : bmap_flush_desched(r);

	if (rc) {
		/* Failed to flush this bmap's dirty pages.
		 */
		DYNARRAY_FOREACH(r, i, biorqs)
			DEBUG_BIORQ(PLL_ERROR, r, "could not flush");
		DEBUG_BMAP(PLL_ERROR, r->biorq_bmap, "could not flush");

		spinlock(&r->biorq_fhent->mfh_lock);
		r->biorq_fhent->mfh_flush_rc = EIO;
		psc_waitq_wakeall(&msl_fhent_flush_waitq);
		freelock(&r->biorq_fhent->mfh_lock);
	}
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
			DYNARRAY_FOREACH(bmpce, j, &r->biorq_pages)
				psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
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
			 * We might round down the offset of an I/O
			 * request to the start offset of the previous
			 * page.
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
				psc_assert(bmpce->bmpce_flags & BMPCE_INFLIGHT);
				BMPCE_ULOCK(bmpce);
				psc_assert(first_iov == 1);

				if (j == 0)
					reqsz -= BMPC_BUFSZ -
					    (r->biorq_off - bmpce->bmpce_off);
				else
					reqsz -= BMPC_BUFSZ;
				continue;
			} else
				bmpce->bmpce_flags |= BMPCE_INFLIGHT;

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
	int ready = 0;

	if (bmap_flush_coalesce_size(biorqs) >= MIN_COALESCE_RPC_SZ)
		ready = 1;

	return (ready);
}

/**
 * bmap_flushable - Check if we can flush the given bmpc (either an I/O
 *	request has expired or we have accumulated a big enough I/O).
 *	This function must be non-blocking.
 */
__static int
bmap_flushable(struct bmapc_memb *b, struct timespec *t)
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
	BMPC_LOCK(bmpc);
	PLL_FOREACH_SAFE(r, tmp, &bmpc->bmpc_new_biorqs) {
		BIORQ_LOCK(r);
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

		if (bmap_flush_biorq_expired(r, t)) {
			flush = 1;
			BIORQ_ULOCK(r);
			break;
		}
		BIORQ_ULOCK(r);

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
		 * If the current request is contained completely
		 * within a previous request, should we count them
		 * separately? -- No, because the bmpce pages are
		 * mapped in the rpc bulk, not anything in the
		 * biorq.
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
	BMPC_ULOCK(bmpc);
	return (flush);
}

/**
 * bmap_flush_trycoalesce - Scan the given array of I/O requests for
 *	candidates to flush.  We *only* flush when (1) a request has
 *	aged out or (2) we can construct a large enough I/O.
 */
__static struct psc_dynarray *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *indexp)
{
	int i, idx, large=0, expired=0;
	struct bmpc_ioreq *r=NULL, *t;
	struct psc_dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(psc_dynarray_len(biorqs) > *indexp);

	for (idx=0; (idx + *indexp) < psc_dynarray_len(biorqs); idx++) {
		t = psc_dynarray_getpos(biorqs, idx + *indexp);

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
			expired = bmap_flush_biorq_expired(t, NULL);

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
			/*
			 * This biorq is not contiguous with the
			 * previous.  If the current set is expired send
			 * it out now.  Otherwise, deschedule the
			 * current set and resume activity with 't' as
			 * the base.
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
		if (!large)
			if (bmap_flushready(&b))
				large = 1;
	}

	if (large || expired) {
		a = PSCALLOC(sizeof(*a));
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

	*indexp += idx;
	psc_dynarray_free(&b);

	return (a);
}

int
msl_bmap_release_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	uint32_t i;
	int rc = 0;

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));

	if (!mp)
		rc = -1;

	for (i = 0; i < mq->nbmaps; i++) {
		psclog((rc || mp->rc) ? PLL_ERROR : PLL_INFO,
		       "fid="SLPRI_FID" bmap=%u key=%"PRId64" seq=%"PRId64
		       " rc=%d", mq->sbd[i].sbd_fg.fg_fid, mq->sbd[i].sbd_bmapno,
		       mq->sbd[i].sbd_key, mq->sbd[i].sbd_seq, (mp) ? mp->rc : rc);
	}

	return (rc | ((mp) ? mp->rc : 0));
}

static void
msl_bmap_release(struct sl_resm *resm)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct resm_cli_info *rmci;
	int rc;

	rmci = resm2rmci(resm);

	csvc = (resm == slc_rmc_resm) ?
	    slc_getmcsvc(resm) : slc_geticsvc(resm);
	if (csvc == NULL) {
		/* Per bug 136.  If the csvc is not available then nuke
		 *   any pending bmap releases.  For now, this op is
		 *   single threaded so resetting nbmaps here should not
		 *   be racy.
		 */
		rmci->rmci_bmaprls.nbmaps = 0;
		if (resm->resm_csvc)
			rc = resm->resm_csvc->csvc_lasterrno; /* XXX race */
		else
			rc = -ENOTCONN;
		goto out;
	}

	psc_assert(rmci->rmci_bmaprls.nbmaps);

	rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(mq, &rmci->rmci_bmaprls, sizeof(*mq));
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	rc = pscrpc_nbreqset_add(pndgBmapRlsReqs, rq);

	rmci->rmci_bmaprls.nbmaps = 0;
 out:
	if (rc) {
		/* At this point the bmaps have already been purged from
		 *   our cache.  If the mds rls request fails then the
		 *   mds should time them out on his own.  In any case,
		 *   the client must reacquire leases to perform further
		 *   I/O on any bmap in this set.
		 */
		psclog_errorx("bmap_release failed res=%s (rc=%d)",
		    resm->resm_name, rc);

		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);
	}
}

void
msbmaprlsthr_main(__unusedx struct psc_thread *thr)
{
	struct timespec crtime, nexttimeo = { 0, 0 };
	struct psc_waitq waitq = PSC_WAITQ_INIT;
	struct psc_dynarray rels = DYNARRAY_INIT;
	struct resm_cli_info *rmci;
	struct bmap_cli_info *bci, *wrapdetect;
	struct bmapc_memb *b;
	struct sl_resm *resm;
	int i, sortbypass = 0, sawnew, rc;

#define SORT_BYPASS_ITERS		32
#define ITEMS_TRY_AFTER_UNEXPIRED	MAX_BMAP_RELEASE

	// just put the resm's in the dynarray. when pushing out the bid's
	//   assume an ion unless resm == slc_rmc_resm

	for (sawnew = 0;; sawnew = 0) {
		if (!sortbypass) {
			lc_sort(&bmapTimeoutQ, qsort, bmap_cli_timeo_cmp);
			sortbypass = SORT_BYPASS_ITERS;
		} else
			sortbypass--;

		PFL_GETTIMESPEC(&crtime);
		nexttimeo = crtime;
		nexttimeo.tv_sec += BMAP_CLI_TIMEO_INC;

		wrapdetect = NULL;
		while ((bci = lc_getnb(&bmapTimeoutQ))) {
			b = bci_2_bmap(bci);
			if (bci == wrapdetect) {
				lc_addstack(&bmapTimeoutQ, bci);
				break;

			} else if (!wrapdetect) {
				wrapdetect = bci;
				nexttimeo = bci->bci_etime;
			}

			BMAP_LOCK(b);
			DEBUG_BMAP(PLL_INFO, b, "timeoq try reap"
			   " (nbmaps=%zd) etime("PSCPRI_TIMESPEC")",
			   lc_sz(&bmapTimeoutQ),
			   PSCPRI_TIMESPEC_ARGS(&bci->bci_etime));

			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);
			psc_assert(b->bcm_flags & BMAP_TIMEOQ);

			if (timespeccmp(&nexttimeo, &bci->bci_etime, >))
				nexttimeo = bci->bci_etime;

			if (bmpc_queued_ios(&bci->bci_bmpc)) {
				BMAP_ULOCK(b);

				rc = msl_bmap_lease_tryext(b, NULL, 0);
				/* msl_bmap_lease_tryext() adjusted etime.
				 */
				if (timespeccmp(&nexttimeo, &bci->bci_etime, >))
					nexttimeo = bci->bci_etime;

				lc_addtail(&bmapTimeoutQ, bci);
				continue;

			} else if (timespeccmp(&crtime, &bci->bci_etime, <)) {
				BMAP_ULOCK(b);

				DEBUG_BMAP(PLL_INFO, b, "sawnew=%d", sawnew);
				lc_addtail(&bmapTimeoutQ, bci);

				sawnew++;
				if (sawnew == ITEMS_TRY_AFTER_UNEXPIRED)
					break;
				else
					continue;

			} else if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				/* Put me back on the end of the queue.
				 */
				BMAP_ULOCK(b);
				DEBUG_BMAP(PLL_NOTICE, b, "skip due to ref");
				lc_addtail(&bmapTimeoutQ, bci);
				continue;
			}

			psc_assert(psc_atomic32_read(&b->bcm_opcnt) == 1);
			/* Note that only this thread calls
			 *   msl_bmap_release() so no reentrancy
			 *   exist unless another rls thr is
			 *   introduced.
			 */
			psc_assert(!bmpc_queued_ios(&bci->bci_bmpc));

			if (b->bcm_flags & BMAP_WR) {
				/* Setup a msg to an ION.
				 */
				psc_assert(bmap_2_ios(b) !=
					   IOS_ID_ANY);

				resm = libsl_ios2resm(bmap_2_ios(b));
				rmci = resm2rmci(resm);

				DEBUG_BMAP(PLL_INFO, b, "res(%s)",
				   resm->resm_res->res_name);
			} else {
				resm = slc_rmc_resm;
				rmci = resm2rmci(slc_rmc_resm);
			}

			psc_assert(rmci->rmci_bmaprls.nbmaps <
				   MAX_BMAP_RELEASE);

			memcpy(&rmci->rmci_bmaprls.sbd[
				 rmci->rmci_bmaprls.nbmaps],
			       &bci->bci_sbd, sizeof(bci->bci_sbd));
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

			if (bci == wrapdetect)
				wrapdetect = NULL;

			PFL_GETTIMESPEC(&crtime);
		}
		/* Send out partially filled release request.
		 */
		DYNARRAY_FOREACH(resm, i, &rels)
			msl_bmap_release(resm);

		psc_dynarray_reset(&rels);
		psc_waitq_waitabs(&waitq, NULL, &nexttimeo);

		if (!pscthr_run())
			break;

		timespecsub(&nexttimeo, &crtime, &nexttimeo);
		psclogs_debug(SLSS_BMAP, "waited for ("PSCPRI_TIMESPEC")"
		       " lc_sz=%zd", PSCPRI_TIMESPEC_ARGS(&nexttimeo),
		       lc_sz(&bmapTimeoutQ));
	}
	psc_dynarray_free(&rels);
}

static void
bmap_flush_outstanding_rpcwait(void)
{
	/* XXX this should really be held in the import / resm on a
	 *   per sliod basis using multiwait instead of a single global
	 *   value
	 */
	spinlock(&bmapFlushLock);
	while (((MAX_OUTSTANDING_RPCS - atomic_read(&outstandingRpcCnt)) <= 0) ||
	       bmapFlushTimeoFlags & BMAPFLSH_RPCWAIT) {
		bmapFlushTimeoFlags |= BMAPFLSH_RPCWAIT;
		/* RPC completion will wake us up.
		 */
		psc_waitq_waitrel(&bmapFlushWaitq, &bmapFlushLock,
			  &bmapFlushWaitSecs);
		spinlock(&bmapFlushLock);
	}
	freelock(&bmapFlushLock);
}

static int
bmpces_inflight_locked(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int i;

	LOCK_ENSURE(&r->biorq_lock);

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		if (bmpce->bmpce_flags & BMPCE_INFLIGHT) {
			DEBUG_BMPCE(PLL_NOTIFY, bmpce, "inflight already");
			BMPCE_ULOCK(bmpce);
			return (1);
		} else
			BMPCE_ULOCK(bmpce);
	}
	return (0);
}

/**
 * bmap_flush - Send out SRMT_WRITE RPCs to the I/O server.
 */
__static void
bmap_flush(struct timespec *nexttimeo)
{
	struct psc_dynarray *biorqs, reqs = DYNARRAY_INIT_NOLOG,
		bmaps = DYNARRAY_INIT_NOLOG;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *tmp;
	struct bmapc_memb *b, *tmpb;
	struct iovec *iovs = NULL;
	int i, j, niovs, rc;
	struct timespec t;

	PFL_GETTIMESPEC(nexttimeo);
	timespecadd(nexttimeo, &bmapFlushWaitSecs, nexttimeo);
	t = *nexttimeo;

	LIST_CACHE_LOCK(&bmapFlushQ);
	LIST_CACHE_FOREACH_SAFE(b, tmpb, &bmapFlushQ) {
		DEBUG_BMAP(PLL_INFO, b, "flushable? (outstandingRpcCnt=%d)",
		    atomic_read(&outstandingRpcCnt));

		BMAP_LOCK(b);
		psc_assert(b->bcm_flags & BMAP_CLI_FLUSHPROC);

		if (!(b->bcm_flags & BMAP_DIRTY)) {
			bmpc = bmap_2_bmpc(b);
			BMPC_LOCK(bmpc);
			psc_assert(!bmpc_queued_writes(bmpc));
			if (!bmpc_queued_ios(bmpc)) {
				lc_remove(&bmapFlushQ, b);
				b->bcm_flags &= ~BMAP_CLI_FLUSHPROC;
			}
			BMPC_ULOCK(bmpc);
			bcm_wake_locked(b);
		} else {
			rc = msl_bmap_lease_tryext(b, NULL, 0);

			if (bmap_flushable(b, &t))
				psc_dynarray_add(&bmaps, b);
			else
				if (timespeccmp(nexttimeo, &t, >))
					*nexttimeo = t;
		}
		BMAP_ULOCK(b);

		if ((psc_dynarray_len(&bmaps) +
		     atomic_read(&outstandingRpcCnt)) >= MAX_OUTSTANDING_RPCS)
			break;
	}
	LIST_CACHE_ULOCK(&bmapFlushQ);

	for (i = 0; i < psc_dynarray_len(&bmaps); i++) {
		b = psc_dynarray_getpos(&bmaps, i);
		bmpc = bmap_2_bmpc(b);

		DEBUG_BMAP(PLL_INFO, b, "try flush (outstandingRpcCnt=%d)",
			   atomic_read(&outstandingRpcCnt));

		BMPC_LOCK(bmpc);
		PLL_FOREACH_SAFE(r, tmp, &bmpc->bmpc_new_biorqs) {
			BIORQ_LOCK(r);

			if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
				DEBUG_BIORQ(PLL_INFO, r, "data not ready");
				BIORQ_ULOCK(r);
				continue;

			} else if (r->biorq_flags & BIORQ_SCHED) {
				DEBUG_BIORQ(PLL_WARN, r, "already sched");
				BIORQ_ULOCK(r);
				continue;

			} else if ((r->biorq_flags & BIORQ_RBWFP) ||
				   (r->biorq_flags & BIORQ_RBWLP)) {
				/* Wait for RBW I/O to complete before
				 *  pushing out any pages.
				 */
				if (!bmap_flush_biorq_rbwdone(r)) {
					BIORQ_ULOCK(r);
					continue;
				}

			} else if (bmpces_inflight_locked(r)) {
				/* Check for the BMPCE_INFLIGHT bit which is
				 *  used to prevent out-of-order writes
				 *  from being sent to the I/O server.
				 *  That situation is possible since large
				 *  RPCs could take 32x longer to ship.
				 *
				 *  BMPCE_INFLIGHT is not set until the bulk
				 *  is created.
				 *
				 * XXX Write me for directio situations also.
				 */
				BIORQ_ULOCK(r);
				continue;
			}
			/* Don't assert !BIORQ_INFL until ensuring that
			 *   we can actually work on this biorq.  A RBW
			 *   process may be working on it.
			 */
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			r->biorq_flags |= BIORQ_SCHED;
			BIORQ_ULOCK(r);

			DEBUG_BIORQ(PLL_DEBUG, r, "flushable");
			psc_dynarray_add(&reqs, r);
		}
		BMPC_ULOCK(bmpc);

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

			bmap_flush_outstanding_rpcwait();
		}
		psc_dynarray_reset(&reqs);
	}

	psc_dynarray_free(&reqs);
	psc_dynarray_free(&bmaps);
}

void
msbmapflushthr_main(__unusedx struct psc_thread *thr)
{
	struct timespec flush, rpcwait, waitq, tmp, tmp1;
	int rc, neg;

	while (pscthr_run()) {
		msbmflthr(pscthr_get())->mbft_failcnt = 1;

		PFL_GETTIMESPEC(&tmp);
		bmap_flush(&bmapFlushWaitTime);
		PFL_GETTIMESPEC(&flush);
		tmp1 = flush;
		timespecsub(&flush, &tmp, &flush);

		bmap_flush_outstanding_rpcwait();
		PFL_GETTIMESPEC(&rpcwait);
		tmp = rpcwait;
		timespecsub(&rpcwait, &tmp1, &rpcwait);

		spinlock(&bmapFlushLock);
		tmp1 = bmapFlushWaitTime;
		do {
			rc = psc_waitq_waitabs(&bmapFlushWaitq, &bmapFlushLock,
			       &bmapFlushWaitTime);
			spinlock(&bmapFlushLock);
		} while (!rc && bmapFlushTimeoFlags & BMAPFLSH_EXPIRE);
		bmapFlushTimeoFlags = 0;
		freelock(&bmapFlushLock);

		PFL_GETTIMESPEC(&waitq);
		tmp = waitq;
		timespecsub(&waitq, &tmp, &waitq);

		if (timespeccmp(&tmp1, &tmp, >)) {
			timespecsub(&tmp1, &tmp, &tmp1);
			tmp = tmp1;
			neg = 0;
		} else {
			timespecsub(&tmp, &tmp1, &tmp);
			neg = 1;
		}

		psclogs_debug(SLSS_BMAP, "flush ("PSCPRI_TIMESPEC"), "
		    "rpcwait ("PSCPRI_TIMESPEC"), "
		    "waitq ("PSCPRI_TIMESPEC"), bmapFlushTimeoFlags=%d "
		    "bmapFlushWaitTime(%s"PSCPRI_TIMESPEC") rc=%d",
		    PSCPRI_TIMESPEC_ARGS(&flush),
		    PSCPRI_TIMESPEC_ARGS(&rpcwait),
		    PSCPRI_TIMESPEC_ARGS(&waitq),
		    bmapFlushTimeoFlags,
		    neg ? "-" : "", PSCPRI_TIMESPEC_ARGS(&tmp), rc);
	}
}

void
msbmapflushrpcthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		//XXX this completion wait is blocking too long when we
		// have a sliod failure
		pscrpc_completion_waitrel_s(&rpcComp, 1);
		bmap_flush_reap_rpcs();
		pscrpc_nbreqset_reap(pndgReadaReqs);
		pscrpc_nbreqset_reap(pndgBmaplsReqs);
		pscrpc_nbreqset_reap(pndgBmapRlsReqs);
	}
}

void
msbmaprathr_main(__unusedx struct psc_thread *thr)
{
#define MAX_BMPCES_PER_RPC 32
	struct msl_fhent *mfh, *lmfh = NULL;
	struct bmap_pagecache_entry *bmpces[MAX_BMPCES_PER_RPC], *tmp, *bmpce;
	int nbmpces, i;

	while (pscthr_run()) {
		nbmpces = 0;
		mfh = lc_getwait(&bmapReadAheadQ);
		if (mfh == lmfh)
			usleep(400);

		spinlock(&mfh->mfh_lock);
		psc_assert(mfh->mfh_flags & MSL_FHENT_RASCHED);
		PLL_FOREACH_SAFE(bmpce, tmp, &mfh->mfh_ra_bmpces) {
			/* Check for sequentiality.  Note that since bmpce
			 *   offsets are intra-bmap, we must check that the
			 *   bmap (bmpce_owner) is the same too.
			 */
			if (nbmpces &&
			    ((bmpce->bmpce_owner !=
			      bmpces[nbmpces-1]->bmpce_owner) ||
			     (bmpce->bmpce_off !=
			      bmpces[nbmpces-1]->bmpce_off + BMPC_BUFSZ)))
				break;

			pll_remove(&mfh->mfh_ra_bmpces, bmpce);
			bmpces[nbmpces++] = bmpce;

			if (nbmpces == mfh->mfh_ra.mra_nseq ||
			    nbmpces == MAX_BMPCES_PER_RPC)
				break;
		}

		if (pll_empty(&mfh->mfh_ra_bmpces)) {
			mfh->mfh_flags &= ~MSL_FHENT_RASCHED;
			if (mfh->mfh_flags & MSL_FHENT_CLOSING)
				psc_waitq_wakeall(&mfh->mfh_fcmh->fcmh_waitq);
		} else
			lc_addtail(&bmapReadAheadQ, mfh);
		freelock(&mfh->mfh_lock);

		for (i=0; i < nbmpces; i++) {
			/* XXX If read / wr refs are 0 then bmpce_getbuf()
			 *    should be called in a non-blocking fashion.
			 */
			bmpce = bmpces[i];
			if (i)
				psc_assert(bmpce->bmpce_owner ==
				   bmpces[i-1]->bmpce_owner);

			BMPCE_LOCK(bmpce);
			bmpce_getbuf(bmpce);
			psc_assert(bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
			bmpce->bmpce_flags &= ~BMPCE_INIT;
			bmpce->bmpce_flags |= BMPCE_READPNDG;
			BMPCE_ULOCK(bmpce);
		}

		msl_reada_rpc_launch(bmpces, nbmpces);
		lmfh = mfh;
	}
}

void
msbmapflushthr_spawn(void)
{
	struct msbmfl_thread *mbft;
	struct psc_thread *thr;
	int i;

	pndgBmapRlsReqs = pscrpc_nbreqset_init(NULL, msl_bmap_release_cb);
	pndgBmaplsReqs = pscrpc_nbreqset_init(NULL, NULL);
	pndgWrtReqs = pscrpc_nbreqset_init(NULL, msl_write_rpc_cb);

	pscrpc_completion_init(&rpcComp);
	atomic_set(&outstandingRpcCnt, 0);
	psc_waitq_init(&bmapFlushWaitq);

	lc_reginit(&bmapFlushQ, struct bmapc_memb,
	    bcm_lentry, "bmapflush");

	lc_reginit(&bmapTimeoutQ, struct bmap_cli_info,
	    bci_lentry, "bmaptimeout");

	lc_reginit(&bmapReadAheadQ, struct msl_fhent,
	    mfh_lentry, "bmapreadahead");

	lc_reginit(&pndgWrtReqSets, struct pscrpc_request_set,
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

	thr = pscthr_init(MSTHRT_BMAPFLSHRPC, 0, msbmapflushrpcthr_main,
	  NULL, sizeof(struct msbmflrpc_thread), "msbflushrpcthr");
	psc_multiwait_init(&msbmflrpc(thr)->mbflrpc_mw, "%s",
			   thr->pscthr_name);
	pscthr_setready(thr);

	thr = pscthr_init(MSTHRT_BMAPFLSHRLS, 0, msbmaprlsthr_main,
	    NULL, sizeof(struct msbmflrls_thread), "msbrlsthr");
	psc_multiwait_init(&msbmflrlsthr(thr)->mbfrlst_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

	for (i=0; i < 4; i++) {
		thr = pscthr_init(MSTHRT_BMAPREADAHEAD, 0,
		    msbmaprathr_main, NULL, sizeof(struct
		    msbmflra_thread), "msbrathr%d", i);
		psc_multiwait_init(&msbmfrathr(thr)->mbfra_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}
