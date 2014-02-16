/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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
#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/log.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "pgcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"

struct timespec			 bmapFlushWaitSecs = { 1, 0L };
struct timespec			 bmapFlushDefMaxAge = { 0, 10000000L };	/* 10 milliseconds */
struct psc_listcache		 bmapFlushQ;
struct psc_listcache		 bmapReadAheadQ;
struct psc_listcache		 bmapTimeoutQ;
struct psc_compl		 rpcComp = PSC_COMPL_INIT;

struct pscrpc_nbreqset		*pndgBmaplsReqs;	/* bmap lease */
struct pscrpc_nbreqset		*pndgBmapRlsReqs;	/* bmap release */
__static struct pscrpc_nbreqset	*pndgWrtReqs;
psc_atomic32_t			 max_nretries = PSC_ATOMIC32_INIT(256);

#define MAX_OUTSTANDING_RPCS	40
#define MIN_COALESCE_RPC_SZ	LNET_MTU
#define NUM_READAHEAD_THREADS	4

struct psc_waitq		bmapFlushWaitq = PSC_WAITQ_INIT;
psc_spinlock_t			bmapFlushLock = SPINLOCK_INIT;
int				bmapFlushTimeoFlags = 0;

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a, struct timespec *t)
{
	struct timespec ts;

	PFL_GETTIMESPEC(&ts);

	if ((a->biorq_flags & BIORQ_FORCE_EXPIRE) ||
	    (a->biorq_expire.tv_sec < ts.tv_sec   ||
	     (a->biorq_expire.tv_sec == ts.tv_sec &&
	      a->biorq_expire.tv_nsec <= ts.tv_nsec)))
		return (1);

	if (t)
		*t = a->biorq_expire;

	return (0);
}

void
bmap_free_all_locked(struct fidc_membh *f)
{
	struct bmap *a, *b;
	struct bmap_cli_info *bci;

	FCMH_LOCK_ENSURE(f);

	for (a = SPLAY_MIN(bmap_cache, &f->fcmh_bmaptree); a; a = b) {
		b = SPLAY_NEXT(bmap_cache, &f->fcmh_bmaptree, a);
		DEBUG_BMAP(PLL_INFO, a, "mark bmap free");
		/*
		 * The MDS truncates the SLASH2 metafile on a full
		 * truncate.  We need to throw away leases and request a
		 * new lease later, so that the MDS has a chance to
		 * update its metadate file on-disk.  Otherwise, we can
		 * use an existing lease to write the file and can not
		 * update the metadata file even if the bmap is still
		 * cached at the MDS because the generation # has been
		 * bumped for the full truncate.
		 *
		 * Finally, a read comes in, we request a read bmap.  At
		 * this point, all bmaps of the file have been freed at
		 * both MDS and client. And the MDS can not find a
		 * replica for a bmap in the metafile.
		 */
		BMAP_LOCK(a);
		bci = bmap_2_bci(a);
		PFL_GETTIMESPEC(&bci->bci_etime);
		a->bcm_flags |= BMAP_TOFREE;
		BMAP_ULOCK(a);
	}
	bmap_flushq_wake(BMAPFLSH_TRUNCATE);
}

int
msl_fd_should_retry(struct msl_fhent *mfh, int rc)
{
	int retry = 1;

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "nretries=%d, maxretries=%d "
	    "(non-blocking=%d)", mfh->mfh_retries,
	    psc_atomic32_read(&max_nretries),
	    (mfh->mfh_oflags & O_NONBLOCK));

	/* test for retryable error codes */
	if (rc != -ENOTCONN && rc != -PFLERR_KEYEXPIRED)
		retry = 0;
	else if (mfh->mfh_oflags & O_NONBLOCK)
		retry = 0;
	else if (++mfh->mfh_retries >= psc_atomic32_read(&max_nretries))
		retry = 0;

	if (retry) {
		if (mfh->mfh_retries < 10)
			usleep(1000);
		else
			usleep(1000000);
		OPSTAT_INCR(SLC_OPST_OFFLINE_RETRY);
	} else
		OPSTAT_INCR(SLC_OPST_OFFLINE_NO_RETRY);

	return (retry);
}

void
_bmap_flushq_wake(const struct pfl_callerinfo *pci, int reason)
{
	int wake = 0;

	spinlock(&bmapFlushLock);
	if (bmapFlushTimeoFlags & BMAPFLSH_RPCWAIT) {
		wake = 1;
		psc_waitq_wakeall(&bmapFlushWaitq);
	}

	freelock(&bmapFlushLock);

	psclog_diag("wakeup flusher: reason=%x wake=%d", reason, wake);
}

__static int
bmap_flush_rpc_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmpc_write_coalescer *bwc =
	    args->pointer_arg[MSL_CBARG_BIORQS];
	struct sl_resm *m = args->pointer_arg[MSL_CBARG_RESM];
	struct resm_cli_info *rmci = resm2rmci(m);
	struct bmpc_ioreq *r;
	int rc;

	psc_atomic32_dec(&rmci->rmci_infl_rpcs);

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	psclog_diag("Reply to write RPC from %d: %d",
	    m->resm_res_id, psc_atomic32_read(&rmci->rmci_infl_rpcs));

	OPSTAT_INCR(SLC_OPST_SRMT_WRITE_CALLBACK);

	while ((r = pll_get(&bwc->bwc_pll))) {
		if (rc) {
			bmap_flush_resched(r, rc);
		} else {
			BIORQ_CLEARATTR(r, BIORQ_INFL | BIORQ_SCHED);
			msl_biorq_destroy(r);
		}
	}

	bwc_release(bwc);
	sl_csvc_decref(csvc);
	return (0);
}

__static int
bmap_flush_create_rpc(struct bmpc_write_coalescer *bwc,
    struct slashrpc_cservice *csvc, struct bmapc_memb *b)
{
	struct pscrpc_request *rq = NULL;
	struct sl_resm *m;
	struct resm_cli_info *rmci;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int rc;

	m = libsl_ios2resm(bmap_2_ios(b));
	rmci = resm2rmci(m);

	CSVC_LOCK(csvc);
	sl_csvc_incref(csvc);
	CSVC_ULOCK(csvc);

	rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
	if (rc)
		goto error;

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
	    bwc->bwc_iovs, bwc->bwc_niovs);
	if (rc)
		goto error;

	rq->rq_timeout = msl_bmap_lease_secs_remaining(b) / 2;

	(void)psc_fault_here_rc(SLC_FAULT_REQUEST_TIMEOUT, &rq->rq_timeout, -1);

	if (rq->rq_timeout < 0) {
		DEBUG_REQ(PLL_ERROR, rq, "off=%u sz=%u op=%u",
			  mq->offset, mq->size, mq->op);
		goto error;
	}

	rq->rq_interpret_reply = bmap_flush_rpc_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
	pscrpc_req_setcompl(rq, &rpcComp);

	mq->offset = bwc->bwc_soff;
	mq->size = bwc->bwc_size;
	mq->op = SRMIOP_WR;

	DEBUG_REQ(PLL_INFO, rq, "off=%u sz=%u",
	    mq->offset, mq->size);

	memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd, sizeof(mq->sbd));
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	/* Do we need this inc/dec combo for biorq reference? */
	psc_atomic32_inc(&rmci->rmci_infl_rpcs);
	psclog_info("Send write RPC to %d: %d",
	    m->resm_res_id, psc_atomic32_read(&rmci->rmci_infl_rpcs));

	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQS] = bwc;
	if (pscrpc_nbreqset_add(pndgWrtReqs, rq)) {
		psc_atomic32_dec(&rmci->rmci_infl_rpcs);
		goto error;
	}

	OPSTAT_INCR(SLC_OPST_SRMT_WRITE);
	return (0);

 error:
	sl_csvc_decref(csvc);
	if (rq)
		pscrpc_req_finished_locked(rq);
	return (rc);
}

__static void
bmap_flush_inflight_set(struct bmpc_ioreq *r)
{
	struct timespec t;
	int old = 0;
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(r->biorq_bmap);

	PFL_GETTIMESPEC(&t);

	BMAP_LOCK(r->biorq_bmap);
	BIORQ_LOCK(r);
	psc_assert(r->biorq_flags & BIORQ_SCHED);

	r->biorq_last_sliod = bmap_2_ios(r->biorq_bmap);
	r->biorq_flags |= BIORQ_INFL;

	if (timespeccmp(&r->biorq_expire, &t, <)) {
		old = 1;
		timespecsub(&t, &r->biorq_expire, &t);
	} else
		timespecsub(&r->biorq_expire, &t, &t);

	DEBUG_BIORQ(old ? PLL_DIAG : PLL_DEBUG, r, "set inflight %s "
	    "("PSCPRI_TIMESPEC")", old ? "expired: -" : "",
	    PSCPRI_TIMESPEC_ARGS(&t));

	/*
	 * Limit the amount of scanning done by this thread.  Move
	 * pending biorqs out of the way.
	 */
	r->biorq_flags &= ~BIORQ_SPLAY;
	PSC_SPLAY_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs, r);
	BIORQ_ULOCK(r);
	BMAP_ULOCK(r->biorq_bmap);
}

/**
 * bmap_flush_resched - Called in error contexts where the biorq must be
 *    rescheduled by putting it back to the new request queue.  Typically
 *    this is from a write RPC cb.
 */
void
bmap_flush_resched(struct bmpc_ioreq *r, int rc)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(r->biorq_bmap);
	int delta;

	DEBUG_BIORQ(PLL_INFO, r, "resched, rc = %d", rc);

	BMAP_LOCK(r->biorq_bmap);
	BIORQ_LOCK(r);
	r->biorq_flags &= ~(BIORQ_INFL | BIORQ_SCHED);

	if (r->biorq_retries >= SL_MAX_BMAPFLSH_RETRIES) {
		BIORQ_ULOCK(r);
		BMAP_ULOCK(r->biorq_bmap);
		msl_bmpces_fail(r, rc);
		msl_biorq_destroy(r);
		return;
	}
	OPSTAT_INCR(SLC_OPST_BMAP_FLUSH_RESCHED);

	if (r->biorq_last_sliod == bmap_2_ios(r->biorq_bmap) ||
	    r->biorq_last_sliod == IOS_ID_ANY)
		r->biorq_retries++;
	else
		r->biorq_retries = 1;

	/*
	 * Back off to allow the I/O server to recover or become less
	 * busy.  Also clear the force expire flag to avoid a spin
	 * within ourselves in the bmap flush loop.
	 *
	 * In theory, we could place them on a different queue based on
	 * its target sliod and woken them up with the connection is
	 * re-established with that sliod.  But that logic is too
	 * complicated to get right.
	 */
	r->biorq_flags &= ~BIORQ_FORCE_EXPIRE;
	PFL_GETTIMESPEC(&r->biorq_expire);

	/*
	 * Retry last more than 11 hours, but don't make it too long
	 * between retries.
	 *
	 * XXX These magic numbers should be made into tunables.
	 *
	 * Note that PSCRPC_OBD_TIMEOUT = 60.
	 */
	if (r->biorq_retries < 32)
		delta = 20;
	else if (r->biorq_retries < 64)
		delta = (r->biorq_retries - 32) * 20 + 20;
	else
		delta = 32 * 20;

	r->biorq_expire.tv_sec += delta;

	if (!(r->biorq_flags & BIORQ_SPLAY)) {
		r->biorq_flags |= BIORQ_SPLAY;
		PSC_SPLAY_XINSERT(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs, r);
	}

	BIORQ_ULOCK(r);
	BMAP_ULOCK(r->biorq_bmap);
	/*
	 * If we were able to connect to an IOS, but the RPC fails
	 * somehow, try to use a different IOS if possible.
	 */
	msl_bmap_lease_tryreassign(r->biorq_bmap);
}

__static void
bmap_flush_send_rpcs(struct bmpc_write_coalescer *bwc)
{
	struct slashrpc_cservice *csvc;
	struct bmpc_ioreq *r;
	struct bmapc_memb *b;
	int rc;

	r = pll_peekhead(&bwc->bwc_pll);

	csvc = msl_bmap_to_csvc(r->biorq_bmap, 1);
	if (csvc == NULL) {
		rc = -ENOTCONN;
		goto error;
	}

	b = r->biorq_bmap;
	psc_assert(bwc->bwc_soff == r->biorq_off);

	PLL_FOREACH(r, &bwc->bwc_pll) {
		psc_assert(b == r->biorq_bmap);
		bmap_flush_inflight_set(r);
	}

	psclog_info("bwc cb arg (%p) size=%zu nbiorqs=%d",
	    bwc, bwc->bwc_size, pll_nitems(&bwc->bwc_pll));

	rc = bmap_flush_create_rpc(bwc, csvc, b);
	if (rc)
		goto error;

	sl_csvc_decref(csvc);
	return;

 error:
	while ((r = pll_get(&bwc->bwc_pll)))
		bmap_flush_resched(r, rc);

	if (csvc)
		sl_csvc_decref(csvc);

	bwc_release(bwc);
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

			bmpce_usecheck(bmpce, BIORQ_WRITE,
			    !i ? (r->biorq_off & ~BMPC_BUFMASK) : off);

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

		bwc->bwc_iovs[i].iov_base = bmpce->bmpce_base +
		    (!i ? (r->biorq_off - bmpce->bmpce_off) : 0);

		bwc->bwc_iovs[i].iov_len = MIN(tot_reqsz,
		    (!i ? BMPC_BUFSZ - (r->biorq_off - bmpce->bmpce_off) :
		     BMPC_BUFSZ));

		tot_reqsz -= bwc->bwc_iovs[i].iov_len;
		bwc->bwc_niovs++;
		OPSTAT_INCR(SLC_OPST_WRITE_COALESCE);
	}
	if (bwc->bwc_niovs > OPSTAT_CURR(SLC_OPST_WRITE_COALESCE_MAX))
		OPSTAT_ASSIGN(SLC_OPST_WRITE_COALESCE_MAX, bwc->bwc_niovs);

	psc_assert(bwc->bwc_niovs <= BMPC_COALESCE_MAX_IOV);
	psc_assert(!tot_reqsz);
}

/**
 * bmap_flushable - Check if we can flush the given bmpc (either an I/O
 *	request has expired or we have accumulated a big enough I/O).
 *	This function must be non-blocking.
 */
__static int
bmap_flushable(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *tmp;
	int flush = 0;

	bmpc = bmap_2_bmpc(b);

	for (r = SPLAY_MIN(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs); r;
	    r = tmp) {
		tmp = SPLAY_NEXT(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs, r);

		BIORQ_LOCK(r);

		DEBUG_BIORQ(PLL_DIAG, r, "consider for flush");

		psc_assert(r->biorq_flags & BIORQ_FLUSHRDY);

		if (r->biorq_flags & BIORQ_SCHED) {
			DEBUG_BIORQ(PLL_WARN, r, "already sched");
			BIORQ_ULOCK(r);
			continue;
		}
		BIORQ_ULOCK(r);
		flush = 1;
		break;
	}
	return (flush);
}

static void
bwc_desched(struct bmpc_write_coalescer *bwc)
{
	struct bmpc_ioreq *r;

	while ((r = pll_get(&bwc->bwc_pll))) {
		BIORQ_LOCK(r);
		r->biorq_flags &= ~BIORQ_SCHED;
		BIORQ_ULOCK(r);
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
	int idx, large = 0, expired = 0;
	struct bmpc_write_coalescer *bwc;
	struct bmpc_ioreq *t, *e = NULL;
	int32_t sz = 0;

	psc_assert(psc_dynarray_len(biorqs) > *indexp);

	bwc = psc_pool_get(bwc_pool);

	for (idx = 0; idx + *indexp < psc_dynarray_len(biorqs);
	    idx++, e = t) {
		t = psc_dynarray_getpos(biorqs, idx + *indexp);

		psc_assert((t->biorq_flags & BIORQ_SCHED) &&
			  !(t->biorq_flags & BIORQ_INFL));

		/*
		 * If any member is expired then we'll push everything
		 * out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(t, NULL);

		DEBUG_BIORQ(PLL_DIAG, t, "biorq #%d (expired=%d)", idx,
		    expired);

		if (idx)
			/* Assert 'lowest to highest' ordering. */
			psc_assert(t->biorq_off >= e->biorq_off);
		else {
			bwc->bwc_size = t->biorq_len;
			bwc->bwc_soff = t->biorq_off;
			pll_addtail(&bwc->bwc_pll, t);
			continue;
		}

		/*
		 * The next request, 't', can be added to the coalesce
		 * group because 't' overlaps or extends 'e'.
		 */
		if (t->biorq_off <= biorq_voff_get(e)) {
			sz = biorq_voff_get(t) - biorq_voff_get(e);
			if (sz > 0) {
				if (sz + bwc->bwc_size >
				    MIN_COALESCE_RPC_SZ) {
					/*
					 * Adding this biorq will push
					 * us over the limit.
					 */
					large = 1;
					break;
				} else {
					bwc->bwc_size += sz;
				}
			}
			pll_addtail(&bwc->bwc_pll, t);

			/* keep the old e if we didn't extend */
			if (sz < 0)
				t = e;

		} else if (expired) {
			/*
			 * Biorq is not contiguous with the previous.
			 * If the current set is expired send it out
			 * now.
			 */
			break;

		} else {
			/*
			 * Otherwise, deschedule the current set and
			 * resume activity with 't' as the base.
			 */
			bwc_desched(bwc);
			bwc->bwc_size = t->biorq_len;
			bwc->bwc_soff = t->biorq_off;
			pll_add(&bwc->bwc_pll, t);
		}
	}

	if (!(large || expired)) {
		/* Clean up any lingering biorq's. */
		bwc_desched(bwc);
		bwc_release(bwc);
		bwc = NULL;
	}

	*indexp += idx;

	return (bwc);
}

__static void
bmap_flush_outstanding_rpcwait(struct sl_resm *m)
{
	struct resm_cli_info *rmci;

	rmci = resm2rmci(m);
	/*
	 * XXX this should really be held in the import/resm on a per
	 * sliod basis using multiwait instead of a single global value.
	 */
	spinlock(&bmapFlushLock);
	while (atomic_read(&rmci->rmci_infl_rpcs) >= MAX_OUTSTANDING_RPCS) {
		bmapFlushTimeoFlags |= BMAPFLSH_RPCWAIT;
		/* RPC completion will wake us up. */
		OPSTAT_INCR(SLC_OPST_BMAP_FLUSH_RPCWAIT);
		psc_waitq_waitrel(&bmapFlushWaitq, &bmapFlushLock,
		    &bmapFlushWaitSecs);
		spinlock(&bmapFlushLock);
	}
	bmapFlushTimeoFlags &= ~BMAPFLSH_RPCWAIT;
	freelock(&bmapFlushLock);
}

/**
 * msbmflwthr_main - Lease watcher thread.
 */
__static void
msbmflwthr_main(struct psc_thread *thr)
{
	struct psc_dynarray bmaps = DYNARRAY_INIT_NOLOG;
	struct bmapc_memb *b, *tmpb;
	struct timespec ts;
	int i, rc;

	while (pscthr_run(thr)) {

		lc_peekheadwait(&bmapFlushQ);

		OPSTAT_INCR(SLC_OPST_LEASE_REFRESH);
		/*
		 * A bmap can be on both bmapFlushQ and bmapTimeoutQ.
		 * It is taken off the bmapFlushQ after all its biorqs
		 * are flushed if any.
		 */
		LIST_CACHE_LOCK(&bmapFlushQ);
		LIST_CACHE_FOREACH_SAFE(b, tmpb, &bmapFlushQ) {
			if (!BMAP_TRYLOCK(b))
				continue;
			DEBUG_BMAP(PLL_INFO, b, "");
			if ((b->bcm_flags & BMAP_TOFREE) ||
			    (b->bcm_flags & BMAP_CLI_LEASEFAILED) ||
			    (b->bcm_flags & BMAP_CLI_REASSIGNREQ)) {
				BMAP_ULOCK(b);
				continue;
			}
			PFL_GETTIMESPEC(&ts);
			if (bmap_2_bci(b)->bci_etime.tv_sec - ts.tv_sec <
				BMAP_CLI_EXTREQSECS) {
				psc_dynarray_add(&bmaps, b);
			}
			BMAP_ULOCK(b);
		}
		LIST_CACHE_ULOCK(&bmapFlushQ);

		DYNARRAY_FOREACH(b, i, &bmaps) {
			/*
			 * XXX: If BMAP_TOFREE is set after the above
			 * loop but before this one.  The bmap reaper
			 * logic will assert on the bmap reference count
			 * not being zero.  And this has been seen
			 * although with a different patch.
			 */
			rc = msl_bmap_lease_tryext(b, 0);
		}
		if (!psc_dynarray_len(&bmaps))
			usleep(1000000);
		else
			psc_dynarray_reset(&bmaps);
	}
}

/**
 * bmap_flush - Send out SRMT_WRITE RPCs to the I/O server.
 */
__static void
bmap_flush(void)
{
	struct psc_dynarray reqs = DYNARRAY_INIT_NOLOG,
	    bmaps = DYNARRAY_INIT_NOLOG;
	struct bmpc_write_coalescer *bwc;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r, *tmp;
	struct bmapc_memb *b, *tmpb;
	struct sl_resm *m = NULL;
	int i, j;

	LIST_CACHE_LOCK(&bmapFlushQ);
	LIST_CACHE_FOREACH_SAFE(b, tmpb, &bmapFlushQ) {

		DEBUG_BMAP(PLL_DIAG, b, "flushable?");

		if (!BMAP_TRYLOCK(b))
			continue;

		psc_assert(b->bcm_flags & BMAP_FLUSHQ);

		if (b->bcm_flags & BMAP_CLI_REASSIGNREQ) {
			BMAP_ULOCK(b);
			continue;
		}

		if (bmap_flushable(b) ||
		    (b->bcm_flags & BMAP_TOFREE) ||
		    (b->bcm_flags & BMAP_CLI_LEASEFAILED))
			psc_dynarray_add(&bmaps, b);

		BMAP_ULOCK(b);

		if (psc_dynarray_len(&bmaps))
			break;
	}

	LIST_CACHE_ULOCK(&bmapFlushQ);

	for (i = 0; i < psc_dynarray_len(&bmaps); i++) {
		b = psc_dynarray_getpos(&bmaps, i);
		bmpc = bmap_2_bmpc(b);

		/*
		 * Try to catch recently expired bmaps before they are
		 * processed by the write back flush mechanism.
		 */
		BMAP_LOCK(b);
		if (b->bcm_flags & (BMAP_TOFREE | BMAP_CLI_LEASEFAILED)) {
			bmpc_biorqs_destroy(b, bmap_2_bci(b)->bci_error);
			continue;
		}

		m = libsl_ios2resm(bmap_2_ios(b));
		DEBUG_BMAP(PLL_DIAG, b, "try flush");

		for (r = SPLAY_MIN(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs); r; r = tmp) {
			tmp = SPLAY_NEXT(bmpc_biorq_tree,
			    &bmpc->bmpc_new_biorqs, r);

			BIORQ_LOCK(r);

			if (r->biorq_flags & BIORQ_SCHED) {
				DEBUG_BIORQ(PLL_WARN, r, "already sched");
				BIORQ_ULOCK(r);
				continue;
			}

			/*
			 * Don't assert !BIORQ_INFL until ensuring that
			 * we can actually work on this biorq.  A RBW
			 * process may be working on it.
			 */
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			r->biorq_flags |= BIORQ_SCHED;
			BIORQ_ULOCK(r);

			DEBUG_BIORQ(PLL_DEBUG, r, "flushable");
			psc_dynarray_add(&reqs, r);
		}
		BMAP_ULOCK(b);

		j = 0;
		while (j < psc_dynarray_len(&reqs) &&
		    (bwc = bmap_flush_trycoalesce(&reqs, &j))) {
			bmap_flush_coalesce_map(bwc);
			bmap_flush_outstanding_rpcwait(m);
			bmap_flush_send_rpcs(bwc);
		}
		psc_dynarray_reset(&reqs);
	}

	psc_dynarray_free(&reqs);
	psc_dynarray_free(&bmaps);
}

void
msbmapflushthr_main(struct psc_thread *thr)
{
	struct timespec work, wait, tmp1, tmp2;

	while (pscthr_run(thr)) {
		msbmflthr(pscthr_get())->mbft_failcnt = 1;

		lc_peekheadwait(&bmapFlushQ);

		OPSTAT_INCR(SLC_OPST_BMAP_FLUSH);

		PFL_GETTIMESPEC(&tmp1);
		bmap_flush();
		PFL_GETTIMESPEC(&tmp2);
		timespecsub(&tmp2, &tmp1, &work);

		PFL_GETTIMESPEC(&tmp1);

		spinlock(&bmapFlushLock);
		bmapFlushTimeoFlags |= BMAPFLSH_RPCWAIT;
		psc_waitq_waitrel(&bmapFlushWaitq,
		    &bmapFlushLock, &bmapFlushWaitSecs);
		spinlock(&bmapFlushLock);
		bmapFlushTimeoFlags &= ~BMAPFLSH_RPCWAIT;
		freelock(&bmapFlushLock);

		PFL_GETTIMESPEC(&tmp2);
		timespecsub(&tmp2, &tmp1, &wait);

		psclogs_debug(SLSS_BMAP, "work time ("PSCPRI_TIMESPEC"),"
		    "wait time ("PSCPRI_TIMESPEC")",
		    PSCPRI_TIMESPEC_ARGS(&work),
		    PSCPRI_TIMESPEC_ARGS(&wait));
	}
}

void
msbmapflushrpcthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		psc_compl_waitrel_s(&rpcComp, 1);
		pscrpc_nbreqset_reap(pndgWrtReqs);
		pscrpc_nbreqset_reap(pndgReadaReqs);
		pscrpc_nbreqset_reap(pndgBmaplsReqs);
		pscrpc_nbreqset_reap(pndgBmapRlsReqs);
	}
}

void
msbmaprathr_main(struct psc_thread *thr)
{
#define MAX_BMPCES_PER_RPC 32
	struct bmap_pagecache_entry *bmpces[MAX_BMPCES_PER_RPC], *tmp, *bmpce;
	struct msl_fhent *mfh;
	int nbmpces;

	while (pscthr_run(thr)) {

		OPSTAT_INCR(SLC_OPST_READ_AHEAD);
		nbmpces = 0;
		mfh = lc_getwait(&bmapReadAheadQ);

		spinlock(&mfh->mfh_lock);
		psc_assert(mfh->mfh_flags & MSL_FHENT_RASCHED);
		PLL_FOREACH_SAFE(bmpce, tmp, &mfh->mfh_ra_bmpces) {
			/*
			 * Check for sequentiality.  Note that since
			 * bmpce offsets are intra-bmap, we must check
			 * that the bmap (bmpce_owner) is the same too.
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
			mfh_decref(mfh);
		} else {
			lc_addtail(&bmapReadAheadQ, mfh);
			freelock(&mfh->mfh_lock);
		}

		msl_reada_rpc_launch(bmpces, nbmpces);
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
	pndgWrtReqs = pscrpc_nbreqset_init(NULL, NULL);

	psc_waitq_init(&bmapFlushWaitq);

	lc_reginit(&bmapFlushQ, struct bmapc_memb,
	    bcm_lentry, "bmapflush");

	lc_reginit(&bmapTimeoutQ, struct bmap_cli_info,
	    bci_lentry, "bmaptimeout");

	lc_reginit(&bmapReadAheadQ, struct msl_fhent,
	    mfh_lentry, "bmapreadahead");

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
		    msbmaprathr_main, NULL,
		    sizeof(struct msbmflra_thread), "msbrathr%d", i);
		psc_multiwait_init(&msbmfrathr(thr)->mbfra_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}
