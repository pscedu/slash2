/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

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
psc_atomic32_t			 offline_nretries = PSC_ATOMIC32_INIT(256);

#define MAX_OUTSTANDING_RPCS	40
#define MIN_COALESCE_RPC_SZ	LNET_MTU /* Try for big RPC's */
#define NUM_READAHEAD_THREADS   4

struct psc_waitq		bmapFlushWaitq = PSC_WAITQ_INIT;
psc_spinlock_t			bmapFlushLock = SPINLOCK_INIT;
int				bmapFlushTimeoFlags = 0;

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a, struct timespec *t)
{
	struct timespec ts;

	PFL_GETTIMESPEC(&ts);

	if ((a->biorq_flags & BIORQ_FORCE_EXPIRE)     ||
	    (a->biorq_expire.tv_sec < ts.tv_sec       ||
	     (a->biorq_expire.tv_sec == ts.tv_sec &&
	      a->biorq_expire.tv_nsec <= ts.tv_nsec)))
		return (1);

	if (t)
		*t = a->biorq_expire;

	return (0);
}

int
msl_fd_offline_retry(struct msl_fhent *mfh)
{
	struct psc_thread *thr;
	int *cnt = NULL;

	thr = pscthr_get();
	if (thr->pscthr_type == MSTHRT_FS)
		cnt = &msfsthr(thr)->mft_failcnt;
	else if (thr->pscthr_type == MSTHRT_BMAPFLSH)
		cnt = &msbmflthr(thr)->mbft_failcnt;
	else
		psc_assert("invalid thread type");
	psc_assert(*cnt);

	DEBUG_FCMH(PLL_WARN, mfh->mfh_fcmh, "nretries=%d, maxretries=%d "
	    "(non-blocking=%d)", *cnt, psc_atomic32_read(&offline_nretries), 
	    (mfh->mfh_oflags & O_NONBLOCK));

	if (mfh->mfh_oflags & O_NONBLOCK)
		return (0);
	if (++*cnt >= psc_atomic32_read(&offline_nretries))
		return (0);
	return (1);
}

/**
 * _msl_offline_retry - Determine whether an RPC should be retried.
 * @r: bmap I/O request.
 */
int
_msl_offline_retry(const struct pfl_callerinfo *pci, struct bmpc_ioreq *r)
{
	int retry;

	retry = msl_fd_offline_retry(r->biorq_fhent);

	DEBUG_BIORQ(PLL_WARN, r, "retry=%d", retry);

	return (retry);
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

#define bmap_flush_rpccnt_dec()					\
	_bmap_flush_rpccnt_dec(PFL_CALLERINFOSS(SLSS_BMAP))

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
bmap_flush_create_rpc(struct bmpc_write_coalescer *bwc,
    struct slashrpc_cservice *csvc, struct bmapc_memb *b)
{
	struct pscrpc_request *rq = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int rc;

	sl_csvc_lock(csvc);
	sl_csvc_incref(csvc);
	sl_csvc_unlock(csvc);

	rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
	if (rc)
		goto error;

	rc = rsx_bulkclient(rq, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
	    bwc->bwc_iovs, bwc->bwc_niovs);
	if (rc)
		goto error;

	rq->rq_timeout = (msl_bmap_lease_secs_remaining(b) / 2);
	if (rq->rq_timeout < 0) {
		DEBUG_REQ(PLL_ERROR, rq, "off=%u sz=%u op=%u",
			  mq->offset, mq->size, mq->op);
		goto error;
	}

	rq->rq_interpret_reply = bmap_flush_rpc_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	pscrpc_completion_set(rq,  &rpcComp);

	atomic_inc(&outstandingRpcCnt);

	mq->offset = bwc->bwc_soff;
	mq->size = bwc->bwc_size;
	mq->op = SRMIOP_WR;
	//XXX mq->wseqno = GETSEQNO;

	DEBUG_REQ(PLL_INFO, rq, "off=%u sz=%u",
	    mq->offset, mq->size);

	memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd, sizeof(mq->sbd));
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	/* biorqs will be freed by the nbreqset callback. */
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQS] = bwc;
	if (pscrpc_nbreqset_add(pndgWrtReqs, rq))
		goto error;

	return (rq);

 error:
	sl_csvc_decref(csvc);
	if (rq) {
		pscrpc_req_finished_locked(rq);
		rq = NULL;
	}
	return (NULL);
}

__static void
bmap_flush_inflight_set(struct bmpc_ioreq *r)
{
	struct timespec t;
	struct bmap_pagecache *bmpc;
	int old = 0;

	PFL_GETTIMESPEC(&t);

	BIORQ_LOCK(r);
	psc_assert(r->biorq_flags & BIORQ_SCHED);

	r->biorq_last_sliod = bmap_2_ios(r->biorq_bmap);
	r->biorq_flags |= BIORQ_INFL;

	if (timespeccmp(&r->biorq_expire, &t, <)) {
		old = 1;
		timespecsub(&t, &r->biorq_expire, &t);
	} else
		timespecsub(&r->biorq_expire, &t, &t);

	BIORQ_ULOCK(r);
	DEBUG_BIORQ(old ? PLL_NOTIFY : PLL_INFO, r, "set inflight %s("
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

__static int
biorq_destroy_failed(struct bmpc_ioreq *r)
{
	int destroy = 0;

	BIORQ_LOCK(r);
	if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
		BIORQ_ULOCK(r);

		return (-EBUSY);
	}

	if (r->biorq_flags & (BIORQ_EXPIREDLEASE | BIORQ_MAXRETRIES))
		destroy = 1;
	BIORQ_ULOCK(r);

	if (destroy) {
		DEBUG_BIORQ(PLL_WARN, r,
		    "lease expired or maxretries - destroying");
		msl_bmpces_fail(r);
		msl_biorq_destroy(r);
	}

	return (destroy);
}

/**
 * _bmap_flush_desched - unschedules a biorq, sets the RESCHED bit,
 *    and bumps the resched timer.  Called when a writeback RPC failed
 *    to get off of the ground OR via RPC cb context on failure.
 * Notes:  _bmap_flush_desched strictly asserts the biorq is not on the 'wire'.
 */
__static void
bmap_flush_desched(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(r->biorq_bmap);
	int i, delta;

	BMPC_RLOCK(bmpc);
	BIORQ_RLOCK(r);
	/* biorq [rd]esched semantics must be strictly enforced.
	 */
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(!(r->biorq_flags & (BIORQ_INFL | BIORQ_RESCHED)));
	psc_assert(pll_conjoint(&bmpc->bmpc_new_biorqs, r));

	if (r->biorq_last_sliod == bmap_2_ios(r->biorq_bmap) ||
	    r->biorq_last_sliod == IOS_ID_ANY)
		r->biorq_retries++;
	else
		r->biorq_retries = 1;

	r->biorq_flags &= ~BIORQ_SCHED;
	r->biorq_flags |= BIORQ_RESCHED;

	/*
	 * Back off to allow the I/O server to recover or become less busy.
	 * Also clear the force expire flag to avoid a spin within ourselves
	 * in the bmap flush loop.
	 *
	 * In theory, we could place them on a different queue based on its
	 * target sliod and woken them up with the connection is re-established
	 * with that sliod.  But that logic is too complicated to get right.
	 */
	r->biorq_flags &= ~BIORQ_FORCE_EXPIRE;
	PFL_GETTIMESPEC(&r->biorq_expire);

	/*
	 * Retry last more than 11 hours, but don't make it too long between
	 * retries.
	 *
	 * XXX These magic numbers should be made into tunables.
	 */
	if (r->biorq_retries < 32)
		delta = 20;
	else if (r->biorq_retries < 64)
		delta = (r->biorq_retries - 32) * 20 + 20;
	else
		delta = 32 * 20;

	r->biorq_expire.tv_sec += delta;

	BIORQ_ULOCK(r);
	BMPC_ULOCK(bmpc);

	DEBUG_BIORQ(PLL_WARN, r, "unset sched lease bmap_2_ios (%u)",
		    bmap_2_ios(r->biorq_bmap));

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		bmpce->bmpce_flags &= ~BMPCE_INFLIGHT;
		BMPCE_ULOCK(bmpce);
	}

	if (r->biorq_retries >= SL_MAX_BMAP_FLUSH_RETRIES) {
		/* Cleanup errored I/O requests.
		 */
		r->biorq_flags |= (BIORQ_MAXRETRIES | BIORQ_FLUSHABORT);
		biorq_destroy_failed(r);
	}

}

/**
 * bmap_flush_resched - called in error contexts where the biorq must be
 *    rescheduled.  Typically this is from a write RPC cb.
 */
void
bmap_flush_resched(struct bmpc_ioreq *r)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(r->biorq_bmap);

	psc_assert(r->biorq_flags & BIORQ_WRITE);

	BMPC_LOCK(bmpc);
	BIORQ_LOCK(r);

	DEBUG_BIORQ(PLL_NOTIFY, r, "resched");

	if (r->biorq_flags & BIORQ_RESCHED) {
		BMPC_ULOCK(bmpc);
		DEBUG_BIORQ(PLL_WARN, r, "already rescheduled");
		BIORQ_ULOCK(r);
		return;
	}
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);
	r->biorq_flags &= ~BIORQ_INFL;

	pll_remove(&bmpc->bmpc_pndg_biorqs, r);
	pll_add_sorted(&bmpc->bmpc_new_biorqs, r, bmpc_biorq_cmp);
	/* bmap_flush_desched drops BIORQ_LOCK and BMPC_LOCK
	 */
	bmap_flush_desched(r);
	msl_bmap_lease_tryreassign(r->biorq_bmap);
}

__static void
bmap_flush_send_rpcs(struct bmpc_write_coalescer *bwc)
{
	struct slashrpc_cservice *csvc;
	struct pscrpc_request *rq;
	struct bmpc_ioreq *r;
	struct bmapc_memb *b;

	r = pll_peekhead(&bwc->bwc_pll);

	csvc = msl_bmap_to_csvc(r->biorq_bmap, 1);
	if (csvc == NULL)
		goto error;

	b = r->biorq_bmap;
	psc_assert(bwc->bwc_soff == r->biorq_off);

	PLL_FOREACH(r, &bwc->bwc_pll) {
		psc_assert(b == r->biorq_bmap);
		bmap_flush_inflight_set(r);
	}

	psclog_info("bwc cb arg (%p) size=%zu nbiorqs=%d",
	    bwc, bwc->bwc_size, pll_nitems(&bwc->bwc_pll));

	psc_assert(bwc->bwc_niovs <= PSCRPC_MAX_BRW_PAGES);

	rq = bmap_flush_create_rpc(bwc, csvc, b);
	if (rq == NULL)
		goto error;

	sl_csvc_decref(csvc);
	return;

 error:
	while ((r = pll_get(&bwc->bwc_pll)))
		csvc ? bmap_flush_resched(r) : bmap_flush_desched(r);

	if (csvc)
		sl_csvc_decref(csvc);

	bwc_release(bwc);
}

__static int
bmap_flush_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq * const *pa = x, *a = *pa;
	const struct bmpc_ioreq * const *pb = y, *b = *pb;

	if (a->biorq_off == b->biorq_off)
		/* Larger requests with the same start offset should have
		 *   ordering priority.
		 */
		return (CMP(b->biorq_len, a->biorq_len));

	return (CMP(a->biorq_off, b->biorq_off));
}

/**
 * bmap_flush_coalesce_size - This function determines the size of the
 *	region covered by an array of requests.  Note that these
 *	requests can overlap in various ways.  But they have already
 *	been ordered based on their offsets.
 */
__static void
bmap_flush_coalesce_prep(struct bmpc_write_coalescer *bwc)
{
	struct bmpc_ioreq *r, *e = NULL;
	struct bmap_pagecache_entry *bmpce;
	uint32_t reqsz, tlen;
	off_t off, loff;
	int i;

	psc_assert(!bwc->bwc_nbmpces);

	PLL_FOREACH(r, &bwc->bwc_pll) {
		if (!e)
			e = r;
		else {
			/* Biorq offsets may not decrease and holes are not
			 * allowed.
			 */
			psc_assert(r->biorq_off >= loff);
			psc_assert(r->biorq_off <= biorq_voff_get(e));
			if (biorq_voff_get(r) > biorq_voff_get(e))
				e = r;

		}

		loff = off = r->biorq_off;
		reqsz = r->biorq_len;

		DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
			DEBUG_BMPCE(PLL_INFO, bmpce,
			    "adding if DNE nbmpces=%d (i=%d) "
			    "(off=%"PSCPRIdOFFT")", bwc->bwc_nbmpces, i, off);

			bmpce_usecheck(bmpce, BIORQ_WRITE, !i ?
			       (r->biorq_off & ~BMPC_BUFMASK) : off);

			tlen = MIN(reqsz, !i ? BMPC_BUFSZ -
				    (off - bmpce->bmpce_off) : BMPC_BUFSZ);

			off += tlen;
			reqsz -= tlen;

			if (!bwc->bwc_nbmpces) {
				bwc->bwc_bmpces[bwc->bwc_nbmpces++] = bmpce;
				DEBUG_BMPCE(PLL_INFO, bmpce, "added");
			} else {
				if (bwc->bwc_bmpces[bwc->bwc_nbmpces-1]->bmpce_off
				    >= bmpce->bmpce_off)
					continue;
				else {
					psc_assert((bmpce->bmpce_off -
					    BMPC_BUFSZ) == bwc->bwc_bmpces[
					   bwc->bwc_nbmpces-1]->bmpce_off);
					bwc->bwc_bmpces[bwc->bwc_nbmpces++] =
						bmpce;
					DEBUG_BMPCE(PLL_INFO, bmpce, "added");
				}
			}
		}
		psc_assert(!reqsz);
	}
	r = pll_peekhead(&bwc->bwc_pll);

	psc_assert(bwc->bwc_size ==
	   (e->biorq_off - r->biorq_off) + e->biorq_len);
}

/**
 * bmap_flush_coalesce_map - Scan the given list of bio requests and
 *	construct I/O vectors out of them.  One I/O vector is limited to
 *	one page.
 */
__static void
bmap_flush_coalesce_map(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmpc_ioreq *r;
	uint32_t tot_reqsz;
	int i;

	tot_reqsz = bwc->bwc_size;

	bmap_flush_coalesce_prep(bwc);

	psclog_info("tot_reqsz=%u nitems=%d nbmpces=%d", tot_reqsz,
		     pll_nitems(&bwc->bwc_pll), bwc->bwc_nbmpces);

	psc_assert(!bwc->bwc_niovs);

	r = pll_peekhead(&bwc->bwc_pll);
	psc_assert(bwc->bwc_soff == r->biorq_off);

	for (i = 0, bmpce = bwc->bwc_bmpces[0]; i < bwc->bwc_nbmpces;
	     i++, bmpce = bwc->bwc_bmpces[i]) {
		BMPCE_LOCK(bmpce);
		bmpce->bmpce_flags |= BMPCE_INFLIGHT;
		DEBUG_BMPCE(PLL_INFO, bmpce, "inflight set (niovs=%d)",
			    bwc->bwc_niovs);
		BMPCE_ULOCK(bmpce);

		bwc->bwc_iovs[i].iov_base = bmpce->bmpce_base +
			(!i ? (r->biorq_off - bmpce->bmpce_off) : 0);

		bwc->bwc_iovs[i].iov_len = MIN(tot_reqsz,
		    (!i ? BMPC_BUFSZ - (r->biorq_off - bmpce->bmpce_off) :
		     BMPC_BUFSZ));

		tot_reqsz -= bwc->bwc_iovs[i].iov_len;
		bwc->bwc_niovs++;
	}

	psc_assert(bwc->bwc_niovs <= 256);
	psc_assert(!tot_reqsz);
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

		if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
			DEBUG_BIORQ(PLL_INFO, r, "data not ready");
			BIORQ_ULOCK(r);
			continue;

		} else if (r->biorq_flags & BIORQ_SCHED) {
			DEBUG_BIORQ(PLL_WARN, r, "already sched");
			BIORQ_ULOCK(r);
			continue;

		} else if (r->biorq_flags & BIORQ_DESTROY) {
			DEBUG_BIORQ(PLL_WARN, r, "skip BIORQ_DESTROY");
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

static void
bwc_desched(struct bmpc_write_coalescer *bwc)
{
	struct bmpc_ioreq *t;

	while ((t = pll_get(&bwc->bwc_pll))) {
		BIORQ_LOCK(t);
		t->biorq_flags &= ~BIORQ_SCHED;
		BIORQ_ULOCK(t);
	}
	bwc->bwc_soff = bwc->bwc_size = 0;
}

/**
 * bmap_flush_trycoalesce - Scan the given array of I/O requests for
 *	candidates to flush.  We *only* flush when (1) a request has
 *	aged out or (2) we can construct a large enough I/O.
 */
__static struct bmpc_write_coalescer *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *indexp)
{
	int idx, large = 0, mergeable = 0, expired = 0;
	int32_t sz = 0;
	struct bmpc_ioreq *t, *e=NULL;
	struct bmpc_write_coalescer *bwc;

	psc_assert(psc_dynarray_len(biorqs) > *indexp);

	bwc = psc_pool_get(bwcPoolMgr);

	for (idx=0; (idx + *indexp) < psc_dynarray_len(biorqs); idx++) {
		t = psc_dynarray_getpos(biorqs, idx + *indexp);

		psc_assert((t->biorq_flags & BIORQ_SCHED) &&
			   !(t->biorq_flags & BIORQ_INFL));
		if (idx)
			/* Assert 'lowest to highest' ordering.
			 */
			psc_assert(t->biorq_off >= e->biorq_off);
		else {
			e = t;
			bwc->bwc_size = e->biorq_len;
			bwc->bwc_soff = e->biorq_off;
			pll_addtail(&bwc->bwc_pll, t);
		}

		/* If any member is expired then we'll push everything out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(t, NULL);

		mergeable = expired || !(t->biorq_flags & BIORQ_RESCHED);

		DEBUG_BIORQ(PLL_NOTICE, t, "biorq #%d (expired=%d)",
			    idx, expired);

		if (!idx)
			continue;

		/* The next request, 't', can be added to the coalesce
		 *   group because 't' overlaps or extends 'e'.
		 */
		if (mergeable && t->biorq_off <= biorq_voff_get(e)) {
			sz = biorq_voff_get(t) - biorq_voff_get(e);
			if (sz > 0) {
				if (sz + bwc->bwc_size > MIN_COALESCE_RPC_SZ) {
					/* Adding this biorq will push us over
					 *   the limit.
					 */
					large = 1;
					break;
				} else {
					e = t;
					bwc->bwc_size += sz;
				}
			}
			pll_addtail(&bwc->bwc_pll, t);

		} else if (expired) {
			/* Biorq is not contiguous with the previous.
			 * If the current set is expired send it out now.
			 */
			break;

		} else {
			/* Otherwise, deschedule the current set and resume
			 * activity with 't' as the base.
			 */
			e = t;
			bwc_desched(bwc);
			pll_add(&bwc->bwc_pll, e);
			bwc->bwc_size = e->biorq_len;
			bwc->bwc_soff = e->biorq_off;
		}
	}

	if (!(large || expired)) {
		/* Clean up any lingering biorq's.
		 */
		bwc_desched(bwc);
		bwc_release(bwc);
		bwc = NULL;
	}

	*indexp += idx;

	return (bwc);
}

int
msl_bmap_release_cb(struct pscrpc_request *rq,
		    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
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
		       " rc=%d", mq->sbd[i].sbd_fg.fg_fid,
		       mq->sbd[i].sbd_bmapno, mq->sbd[i].sbd_key,
		       mq->sbd[i].sbd_seq, (mp) ? mp->rc : rc);
	}

	sl_csvc_decref(csvc);
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
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	rc = pscrpc_nbreqset_add(pndgBmapRlsReqs, rq);

	rmci->rmci_bmaprls.nbmaps = 0;
 out:
	if (rc) {
		/* At this point the bmaps have already been purged from
		 *   our cache.  If the MDS RLS request fails then the
		 *   MDS should time them out on his own.  In any case,
		 *   the client must reacquire leases to perform further
		 *   I/O on any bmap in this set.
		 */
		psclog_errorx("failed res=%s (rc=%d)",
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
	struct timespec crtime, nto = { 0, 0 };
	struct psc_dynarray rels = DYNARRAY_INIT;
	struct psc_waitq waitq = PSC_WAITQ_INIT;
	struct bmap_cli_info *bci, *wrapdetect;
	struct resm_cli_info *rmci;
	struct bmapc_memb *b;
	struct sl_resm *resm;
	int i, sortbypass = 0, sawnew;

#define SORT_BYPASS_ITERS		32
#define ITEMS_TRY_AFTER_UNEXPIRED	MAX_BMAP_RELEASE

	/*
	 * XXX: just put the resm's in the dynarray.  When pushing out
	 * the bid's, assume an ion unless resm == slc_rmc_resm.
	 */
	for (;;) {
		sawnew = 0;
		if (!sortbypass) {
			lc_sort(&bmapTimeoutQ, qsort, bmap_cli_timeo_cmp);
			sortbypass = SORT_BYPASS_ITERS;
		} else
			sortbypass--;

		PFL_GETTIMESPEC(&crtime);
		nto = crtime;
		nto.tv_sec += BMAP_CLI_TIMEO_INC;

		wrapdetect = NULL;
		while ((bci = lc_getnb(&bmapTimeoutQ))) {
			b = bci_2_bmap(bci);
			if (bci == wrapdetect) {
				lc_addstack(&bmapTimeoutQ, bci);
				break;

			} else if (!wrapdetect) {
				wrapdetect = bci;
				/* Don't set expire in the past.
				 */
				if (timespeccmp(&crtime, &bci->bci_etime, <))
					nto = bci->bci_etime;
			}

			BMAP_LOCK(b);
			DEBUG_BMAP(PLL_DEBUG, b, "timeoq try reap"
			   " (nbmaps=%zd) etime("PSCPRI_TIMESPEC")",
			   lc_sz(&bmapTimeoutQ),
			   PSCPRI_TIMESPEC_ARGS(&bci->bci_etime));

			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);
			psc_assert(b->bcm_flags & BMAP_TIMEOQ);

			if (!(b->bcm_flags & BMAP_CLI_LEASEEXPIRED) &&
			    timespeccmp(&crtime, &bci->bci_etime, <))
				/* Don't spin on expired bmaps while they
				 *    unwind timedout biorqs.
				 */
				nto = bci->bci_etime;

			if (bmpc_queued_ios(&bci->bci_bmpc)) {
				int rc;

				BMAP_ULOCK(b);
				rc = msl_bmap_lease_tryext(b, NULL, 0);
				/* msl_bmap_lease_tryext() adjusted etime.
				 */
				if ((!rc || rc == -EAGAIN) &&
				    timespeccmp(&crtime, &bci->bci_etime, <) &&
				    timespeccmp(&nto, &bci->bci_etime, >))
					nto = bci->bci_etime;

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
				int expired = 0;

				if (timespeccmp(&crtime, &bci->bci_xtime, >))
					expired = 1;

				BMAP_ULOCK(b);
				DEBUG_BMAP(expired ? PLL_WARN : PLL_NOTICE, b, 
				   "skip due to ref (expired=%d)", expired);
				/* Put me back on the end of the queue.
				 */

				lc_addtail(&bmapTimeoutQ, bci);
				continue;
			}

			psc_assert(psc_atomic32_read(&b->bcm_opcnt) == 1);
			/* Note that only this thread calls
			 *   msl_bmap_release() so no reentrancy
			 *   issues can exist unless another rls thr is
			 *   introduced.
			 */
			psc_assert(!bmpc_queued_ios(&bci->bci_bmpc));

			if (b->bcm_flags & BMAP_WR) {
				/* Setup a msg to an ION.
				 */
				psc_assert(bmap_2_ios(b) != IOS_ID_ANY);

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

			if (rmci->rmci_bmaprls.nbmaps == MAX_BMAP_RELEASE) {
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
		psc_waitq_waitabs(&waitq, NULL, &nto);

		if (!pscthr_run())
			break;

		timespecsub(&nto, &crtime, &nto);
		psclogs_debug(SLSS_BMAP, "waited for ("PSCPRI_TIMESPEC")"
		    " lc_sz=%zd", PSCPRI_TIMESPEC_ARGS(&nto),
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
 * msbmflwthr_main - lease watcher thread.
 */
__static void
msbmflwthr_main(__unusedx struct psc_thread *thr)
{
	struct psc_dynarray bmaps = DYNARRAY_INIT_NOLOG;
	struct bmapc_memb *b, *tmpb;
	struct timespec ts;
	int i, rc, secs;

	while (pscthr_run()) {
		PFL_GETTIMESPEC(&ts);
		LIST_CACHE_LOCK(&bmapFlushQ);
		LIST_CACHE_FOREACH_SAFE(b, tmpb, &bmapFlushQ) {
			BMAP_LOCK(b);
			DEBUG_BMAP(PLL_INFO, b, "");
			if (!(b->bcm_flags & BMAP_CLOSING) &&
			    ((!(b->bcm_flags &
				(BMAP_CLI_LEASEEXPIRED|BMAP_CLI_REASSIGNREQ)) &&
			      (((bmap_2_bci(b)->bci_xtime.tv_sec - ts.tv_sec) <
				BMAP_CLI_EXTREQSECS))) ||
			     timespeccmp(&ts, &bmap_2_bci(b)->bci_etime, >=))) {
				    psc_dynarray_add(&bmaps, b);
			}
			BMAP_ULOCK(b);
		}
		LIST_CACHE_ULOCK(&bmapFlushQ);

		DYNARRAY_FOREACH(b, i, &bmaps) {
			rc = msl_bmap_lease_tryext(b, &secs, 0);
			DEBUG_BMAP((rc && rc != -EAGAIN) ?
			   PLL_ERROR : PLL_INFO, b,
			   "rc=%d secs=%d",  rc, secs);
		}
		psc_dynarray_reset(&bmaps);
		usleep(200000);
	}
}

/**
 * bmap_flush - Send out SRMT_WRITE RPCs to the I/O server.
 */
__static void
bmap_flush(struct timespec *nto)
{
	struct psc_dynarray reqs = DYNARRAY_INIT_NOLOG,
	    bmaps = DYNARRAY_INIT_NOLOG;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *tmp;
	struct bmapc_memb *b, *tmpb;
	struct bmpc_write_coalescer *bwc;
	struct timespec t = {0, 0};
	int i, j;

	nto->tv_sec = nto->tv_nsec = 0;

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
			BMAP_ULOCK(b);

		} else if (b->bcm_flags & BMAP_CLI_REASSIGNREQ) {
			BMAP_ULOCK(b);

		} else if (b->bcm_flags & BMAP_CLI_LEASEEXPIRED) {
			bmpc = bmap_2_bmpc(b);
			BMAP_ULOCK(b);

			while ((r = pll_peekhead(&bmpc->bmpc_new_biorqs)))
				psc_assert(biorq_destroy_failed(r));

		} else {
			if (bmap_flushable(b, &t))
				psc_dynarray_add(&bmaps, b);
			else
				if ((!nto->tv_nsec && !nto->tv_sec) ||
				    (timespeccmp(nto, &t, >)))
					*nto = t;
			BMAP_ULOCK(b);
		}

		if ((psc_dynarray_len(&bmaps) +
		     atomic_read(&outstandingRpcCnt)) >=
		    MAX_OUTSTANDING_RPCS)
			break;
	}

	PFL_GETTIMESPEC(&t);
	if ((!nto->tv_sec && !nto->tv_nsec) ||
	    timespeccmp(nto, &t, <)) {
		timespecadd(&t, &bmapFlushWaitSecs, nto);
	}

	LIST_CACHE_ULOCK(&bmapFlushQ);

	for (i = 0; i < psc_dynarray_len(&bmaps); i++) {
		b = psc_dynarray_getpos(&bmaps, i);
		bmpc = bmap_2_bmpc(b);

		/* Try to catch recently expired bmaps before they are
		 *   processed by the write back flush mechanism.
		 */
		BMAP_LOCK(b);
		if (b->bcm_flags & BMAP_CLI_LEASEEXPIRED) {
			BMAP_ULOCK(b);
			while ((r = pll_peekhead(&bmpc->bmpc_new_biorqs)))
				psc_assert(biorq_destroy_failed(r));
			continue;
		}
		BMAP_ULOCK(b);

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
				 */
				BIORQ_ULOCK(r);
				continue;
			}
			/* Don't assert !BIORQ_INFL until ensuring that
			 *   we can actually work on this biorq.  A RBW
			 *   process may be working on it.
			 */
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			r->biorq_flags &= ~BIORQ_RESCHED;
			r->biorq_flags |= BIORQ_SCHED;
			BIORQ_ULOCK(r);

			DEBUG_BIORQ(PLL_DEBUG, r, "flushable");
			psc_dynarray_add(&reqs, r);
		}
		BMPC_ULOCK(bmpc);

		j = 0;
		while (j < psc_dynarray_len(&reqs) &&
		    (bwc = bmap_flush_trycoalesce(&reqs, &j))) {
			bmap_flush_coalesce_map(bwc);
			bmap_flush_send_rpcs(bwc);
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
	struct timespec flush, rpcwait, waitq, tmp1, tmp2;
	int rc, neg;

	while (pscthr_run()) {
		msbmflthr(pscthr_get())->mbft_failcnt = 1;

		PFL_GETTIMESPEC(&tmp1);
		bmap_flush_outstanding_rpcwait();
		PFL_GETTIMESPEC(&tmp2);
		timespecsub(&tmp2, &tmp1, &rpcwait);

		PFL_GETTIMESPEC(&tmp1);
		bmap_flush(&bmapFlushWaitTime);
		PFL_GETTIMESPEC(&tmp2);
		timespecsub(&tmp2, &tmp1, &flush);

		tmp1 = bmapFlushWaitTime;

		spinlock(&bmapFlushLock);
		do {
			rc = psc_waitq_waitabs(&bmapFlushWaitq, &bmapFlushLock,
			       &bmapFlushWaitTime);
			spinlock(&bmapFlushLock);
		} while (!rc && !(bmapFlushTimeoFlags & BMAPFLSH_EXPIRE));
		bmapFlushTimeoFlags = 0;
		freelock(&bmapFlushLock);

		PFL_GETTIMESPEC(&tmp2);
		if (timespeccmp(&tmp2, &tmp1, >=)) {
			timespecsub(&tmp2, &tmp1, &waitq);
			neg = 0;
		} else {
			timespecsub(&tmp1, &tmp2, &waitq);
			neg = 1;
		}

		psclogs_debug(SLSS_BMAP, "flush ("PSCPRI_TIMESPEC"), "
		    "rpcwait ("PSCPRI_TIMESPEC"), "
		    "bmapFlushTimeoFlags=%d, "
		    "waitq (%s"PSCPRI_TIMESPEC") rc=%d",
		    PSCPRI_TIMESPEC_ARGS(&flush),
		    PSCPRI_TIMESPEC_ARGS(&rpcwait),
		    bmapFlushTimeoFlags,
		    neg ? "-" : "", PSCPRI_TIMESPEC_ARGS(&waitq), rc);
	}
}

void
msbmapflushrpcthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		pscrpc_completion_waitrel_s(&rpcComp, 1);
		pscrpc_nbreqset_reap(pndgWrtReqs);
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

	thr = pscthr_init(MSTHRT_BMAPLSWATCHER, 0, msbmflwthr_main,
	  NULL, sizeof(struct msbmflwatcher_thread), "msbmflwthr");
	psc_multiwait_init(&msbmflwthr(thr)->mbfwa_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

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

	for (i=0; i < NUM_READAHEAD_THREADS; i++) {
		thr = pscthr_init(MSTHRT_BMAPREADAHEAD, 0,
		    msbmaprathr_main, NULL, sizeof(struct
		    msbmflra_thread), "msbrathr%d", i);
		psc_multiwait_init(&msbmfrathr(thr)->mbfra_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}
