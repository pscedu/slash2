/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Logic for bmap flushing: sending RPCs of dirty data modified by
 * clients to the IO systems they are bound to.  Write coalescing
 * (bwc) occurs to reduce RPC overhead.
 */

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <sys/time.h>
#include <sys/types.h>

#include <stdlib.h>

#include "pfl/cdefs.h"
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fault.h"
#include "pfl/fcntl.h"
#include "pfl/fsmod.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "pgcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"

struct timespec			 msl_bflush_timeout = { 1, 0L };
struct timespec			 msl_bflush_maxage = { 0, 10000000L };	/* 10 milliseconds */
struct psc_listcache		 msl_bmapflushq;
struct psc_listcache		 msl_bmaptimeoutq;

int				 msl_max_nretries = 256;

#define MIN_COALESCE_RPC_SZ	LNET_MTU

struct psc_waitq		 slc_bflush_waitq = PSC_WAITQ_INIT;
psc_spinlock_t			 slc_bflush_lock = SPINLOCK_INIT;
int				 slc_bflush_tmout_flags;

psc_atomic32_t			 slc_write_coalesce_max;

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a)
{
	struct timespec ts;

	if (a->biorq_flags & BIORQ_EXPIRE)
		return (1);

	PFL_GETTIMESPEC(&ts);
	/* XXX timespeccmp(&a->biorq_expire, &ts, <) */
	if ((a->biorq_expire.tv_sec < ts.tv_sec ||
	    (a->biorq_expire.tv_sec == ts.tv_sec &&
	     a->biorq_expire.tv_nsec <= ts.tv_nsec)))
		return (1);

	return (0);
}

/*
 * Manually expire all bmaps attached to a file.  The RPC flusher will
 * soon notice and flush any pending data.
 */
void
bmap_free_all_locked(struct fidc_membh *f)
{
	struct bmap_cli_info *bci;
	struct bmap *b;

	FCMH_LOCK_ENSURE(f);

	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
		DEBUG_BMAP(PLL_DIAG, b, "mark bmap free");

		/*
		 * The MDS truncates the SLASH2 metadata file on a full
		 * truncate.  We need to throw away leases and request a
		 * new lease later, so that the MDS has a chance to
		 * update its metadata file on-disk.  Otherwise, we can
		 * use an existing lease to write the file and can not
		 * update the metadata file even if the bmap is still
		 * cached at the MDS because the generation # has been
		 * bumped for the full truncate.
		 *
		 * Finally, a read comes in, we request a read bmap.  At
		 * this point, all bmaps of the file have been freed at
		 * both MDS and client.  And the MDS cannot find a
		 * replica for a bmap in the metafile.
		 */
		BMAP_LOCK(b);
		bci = bmap_2_bci(b);
		PFL_GETTIMESPEC(&bci->bci_etime);
		BMAP_ULOCK(b);
	}
	psc_waitq_wakeall(&msl_bmaptimeoutq.plc_wq_empty);
}

/*
 * Determine if an I/O operation should be retried after successive
 * RPC/communication failures.
 *
 * We want to check:
 *	- administration/control/configuration policy (e.g. "5
 *	  retries").
 *	- user process/environment/file descriptor policy
 *	- user process interrupt
 *
 * XXX this should likely be merged with slc_rmc_retry_pfr().
 * XXX mfh_retries access and modification is racy here, e.g. if the
 *	process has multiple threads or forks.
 */
int
msl_fd_should_retry(struct msl_fhent *mfh, struct pscfs_req *pfr,
    int rc)
{
	int retry = 1;

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "nretries=%d, maxretries=%d (non-blocking=%d)",
	    mfh->mfh_retries, msl_max_nretries,
	    mfh->mfh_oflags & O_NONBLOCK);

	/* test for retryable error codes */
	switch (rc) {
	case -ENOTCONN:
	case -ETIMEDOUT:
	case -PFLERR_KEYEXPIRED:
	case -PFLERR_TIMEDOUT:
	case -SLERR_ION_OFFLINE:
//	case -ECONNABORTED:
//	case -ECONNREFUSED:
//	case -ECONNRESET:
//	case -EHOSTDOWN:
//	case -EHOSTUNREACH:
//	case -EIO:
//	case -ENETDOWN:
//	case -ENETRESET:
//	case -ENETUNREACH:
#ifdef ENONET
//	case -ENONET:
#endif
		break;
	default:
		retry = 0;
		break;
	}
	// XXX can this flag be changed dynamically?
	// fcntl(2)
	if (mfh->mfh_oflags & O_NONBLOCK)
		retry = 0;
	else if (++mfh->mfh_retries >= msl_max_nretries)
		retry = 0;

	if (retry) {
		if (mfh->mfh_retries < 10)
			usleep(1000);
		else
			usleep(1000000);
		OPSTAT_INCR("msl.offline-retry");
	} else
		OPSTAT_INCR("msl.offline-no-retry");

	if (pfr->pfr_interrupted)
		retry = 0;

	return (retry);
}

/*
 * Pin (mark read-only) all pages attached to a bmap write coalescer.
 */
void
bwc_pin_pages(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *pg;
	int i;

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		pg = bwc->bwc_bmpces[i];
		BMPCE_LOCK(pg);
		while (pg->bmpce_flags & BMPCEF_PINNED) {
			BMPCE_WAIT(pg);
			BMPCE_LOCK(pg);
		}
		pg->bmpce_flags |= BMPCEF_PINNED;
		BMPCE_ULOCK(pg);
	}
}

/*
 * Unpin (release) all pages attached to a bmap write coalescer.
 */
void
bwc_unpin_pages(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *pg;
	int i;

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		pg = bwc->bwc_bmpces[i];
		BMPCE_LOCK(pg);
		pg->bmpce_flags &= ~BMPCEF_PINNED;
		BMPCE_WAKE(pg);
		BMPCE_ULOCK(pg);
	}
}

void
_bmap_flushq_wake(const struct pfl_callerinfo *pci, int reason)
{
	int wake = 0;

	if (slc_bflush_tmout_flags & BMAPFLSH_RPCWAIT) {
		wake = 1;
		if (reason == BMAPFLSH_EXPIRE)
			psc_waitq_wakeall(&slc_bflush_waitq);
		else
			psc_waitq_wakeone(&slc_bflush_waitq);
	}

	psclog_diag("wakeup flusher: reason=%x wake=%d", reason, wake);
}

/*
 * Callback run after a WRITE is recieved by an IOS.
 */
__static int
msl_ric_bflush_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmpc_write_coalescer *bwc =
	    args->pointer_arg[MSL_CBARG_BIORQS];
	struct sl_resm *m = args->pointer_arg[MSL_CBARG_RESM];
	struct resprof_cli_info *rpci = res2rpci(m->resm_res);
	struct bmpc_ioreq *r;
	int i, rc;

	RPCI_LOCK(rpci);
	rpci->rpci_infl_rpcs--;
	RPCI_WAKE(rpci);
	RPCI_ULOCK(rpci);

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	psclog_diag("callback to write RPC bwc=%p ios=%d infl=%d rc=%d",
	    bwc, m->resm_res_id, rpci->rpci_infl_rpcs, rc);

	bwc_unpin_pages(bwc);

	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		if (rc) {
			bmap_flush_resched(r, rc);
		} else {
			msl_biorq_release(r);
		}
	}

	msl_update_iocounters(slc_iorpc_iostats, SL_WRITE,
	    bwc->bwc_size);

	bwc_release(bwc);
	sl_csvc_decref(csvc);

	return (0);
}

__static int
bmap_flush_create_rpc(struct bmpc_write_coalescer *bwc,
    struct slashrpc_cservice *csvc, struct bmap *b)
{
	struct pscrpc_request *rq = NULL;
	struct resprof_cli_info *rpci;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	int rc;

	m = libsl_ios2resm(bmap_2_ios(b));
	rpci = res2rpci(m->resm_res);

	msl_resm_throttle_wait(m);

	rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
	if (rc)
		goto out;

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
	    bwc->bwc_iovs, bwc->bwc_niovs);
	if (rc)
		goto out;

#if 0
	/*
	 * Instead of timeout ourselves, the IOS will return 
	 * -PFLERR_KEYEXPIRED and we should retry.
	 */
	rq->rq_timeout = msl_bmap_lease_secs_remaining(b);
#endif

	(void)pfl_fault_here_rc("slash2/request_timeout",
	    &rq->rq_timeout, -1);

	if (rq->rq_timeout < 0) {
		rc = -EAGAIN;
		DEBUG_REQ(PLL_ERROR, rq,
		    "negative timeout: off=%u sz=%u op=%u",
		    mq->offset, mq->size, mq->op);
		OPSTAT_INCR("msl.flush-rpc-expire");
		goto out;
	}

	mq->offset = bwc->bwc_soff;
	mq->size = bwc->bwc_size;
	mq->op = SRMIOP_WR;

	if (b->bcm_flags & BMAPF_BENCH)
		mq->flags |= SRM_IOF_BENCH;

	memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd, sizeof(mq->sbd));

	DEBUG_REQ(PLL_DIAG, rq, "sending WRITE RPC to iosid=%#x "
	    "fid="SLPRI_FG" off=%u sz=%u ios=%u infl=%d",
	    m->resm_res_id, SLPRI_FG_ARGS(&mq->sbd.sbd_fg), mq->offset,
	    mq->size, bmap_2_ios(b), rpci->rpci_infl_rpcs);

	bwc_pin_pages(bwc);

	rq->rq_interpret_reply = msl_ric_bflush_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQS] = bwc;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		bwc_unpin_pages(bwc);
		goto out;
	}

	return (0);

 out:
	RPCI_LOCK(rpci);
	rpci->rpci_infl_rpcs--;
	RPCI_WAKE(rpci);
	RPCI_ULOCK(rpci);

	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

/*
 * Called in error contexts where the biorq must be rescheduled by
 * putting it back to the new request queue.  Typically this is from a
 * write RPC cb.
 */
void
bmap_flush_resched(struct bmpc_ioreq *r, int rc)
{
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc;
	struct bmap_cli_info *bci;
	int delta;

	DEBUG_BIORQ(PLL_DIAG, r, "resched rc=%d", rc);

	BMAP_LOCK(b);
	if (rc == -PFLERR_KEYEXPIRED) {
		OPSTAT_INCR("msl.bmap-flush-expired");
		b->bcm_flags |= BMAPF_LEASEEXPIRED;
	}

	BIORQ_LOCK(r);

	if (rc == -ENOSPC || r->biorq_retries >=
	    SL_MAX_BMAPFLSH_RETRIES) {
		BIORQ_ULOCK(r);

		bci = bmap_2_bci(r->biorq_bmap);
		if (rc && !bci->bci_flush_rc)
			bci->bci_flush_rc = rc;
		BMAP_ULOCK(r->biorq_bmap);

		msl_bmpces_fail(r, rc);
		msl_biorq_release(r);
		return;
	}

	if (!(r->biorq_flags & BIORQ_ONTREE)) {
		bmpc = bmap_2_bmpc(b);
		PSC_RB_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs,
		    r);
		pll_addtail(&bmpc->bmpc_new_biorqs_exp, r);
		r->biorq_flags |= BIORQ_ONTREE;
	}
	OPSTAT_INCR("msl.bmap-flush-resched");

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

	BIORQ_ULOCK(r);
	BMAP_ULOCK(b);
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
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r;
	struct bmap *b;
	int i, rc;

	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);

	rc = msl_bmap_to_csvc(r->biorq_bmap, 1, NULL, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	b = r->biorq_bmap;
	psc_assert(bwc->bwc_soff == r->biorq_off);

	BMAP_LOCK(b);
	bmpc = bmap_2_bmpc(b);
	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		psc_assert(b == r->biorq_bmap);
		/*
		 * No need to lock because we have already replied to
		 * the user space.  Furthermore, we flush each biorq in
		 * one RPC.  So the callback handler won't race with us.
		 */
		r->biorq_last_sliod = bmap_2_ios(b);
		r->biorq_flags &= ~BIORQ_ONTREE;
		PSC_RB_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs,
		    r);
		pll_remove(&bmpc->bmpc_new_biorqs_exp, r);
	}
	BMAP_ULOCK(b);

	psclog_diag("bwc cb arg (%p) size=%zu nbiorqs=%d",
	    bwc, bwc->bwc_size, psc_dynarray_len(&bwc->bwc_biorqs));

	rc = bmap_flush_create_rpc(bwc, csvc, b);
	if (!rc)
		return;

 out:
	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs)
		bmap_flush_resched(r, rc);

	if (csvc)
		sl_csvc_decref(csvc);

	bwc_release(bwc);
}

/*
 * This function determines the size of the region covered by an array
 * of requests.  Note that these requests can overlap in various ways.
 * But they have already been ordered based on their offsets.
 */
__static void
bmap_flush_coalesce_prep(struct bmpc_write_coalescer *bwc)
{
	struct bmpc_ioreq *r, *end = NULL;
	struct bmap_pagecache_entry *bmpce;
	uint32_t reqsz, tlen;
	off_t off, loff;
	int i, j;

	psc_assert(!bwc->bwc_nbmpces);

	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		if (!end)
			end = r;
		else {
			/*
			 * biorq offsets may not decrease and holes are
			 * not allowed.
			 */
			psc_assert(r->biorq_off >= loff);
			psc_assert(r->biorq_off <= biorq_voff_get(end));
			if (biorq_voff_get(r) > biorq_voff_get(end))
				end = r;
		}

		loff = off = r->biorq_off;
		reqsz = r->biorq_len;

		DYNARRAY_FOREACH(bmpce, j, &r->biorq_pages) {
			DEBUG_BMPCE(PLL_DIAG, bmpce,
			    "adding if DNE nbmpces=%d (i=%d) "
			    "(off=%"PSCPRIdOFFT")", bwc->bwc_nbmpces, j,
			    off);

			bmpce_usecheck(bmpce, BIORQ_WRITE,
			    !j ? (r->biorq_off & ~BMPC_BUFMASK) : off);

			tlen = MIN(reqsz, !j ? BMPC_BUFSZ -
			    (off - bmpce->bmpce_off) : BMPC_BUFSZ);

			off += tlen;
			reqsz -= tlen;

			if (!bwc->bwc_nbmpces) {
				bwc->bwc_bmpces[bwc->bwc_nbmpces++] =
				    bmpce;
				DEBUG_BMPCE(PLL_DIAG, bmpce, "added");
				continue;
			}
			if (bwc->bwc_bmpces[bwc->bwc_nbmpces - 1]->
			    bmpce_off >= bmpce->bmpce_off)
				continue;

			psc_assert(bmpce->bmpce_off - BMPC_BUFSZ ==
			    bwc->bwc_bmpces[bwc->bwc_nbmpces - 1]->bmpce_off);
			bwc->bwc_bmpces[bwc->bwc_nbmpces++] = bmpce;
			DEBUG_BMPCE(PLL_DIAG, bmpce, "added");
		}
		psc_assert(!reqsz);
	}
	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);

	psc_assert(bwc->bwc_size ==
	    (end->biorq_off - r->biorq_off) + end->biorq_len);
}

/*
 * Scan the given list of bio requests and construct I/O vectors out of
 * them.  One iovec is limited to one page.
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

	psclog_diag("tot_reqsz=%u nitems=%d nbmpces=%d", tot_reqsz,
	    psc_dynarray_len(&bwc->bwc_biorqs), bwc->bwc_nbmpces);

	psc_assert(!bwc->bwc_niovs);

	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);
	psc_assert(bwc->bwc_soff == r->biorq_off);

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		bmpce = bwc->bwc_bmpces[i];

		bwc->bwc_iovs[i].iov_base = bmpce->bmpce_base +
		    (i ? 0 : r->biorq_off - bmpce->bmpce_off);

		bwc->bwc_iovs[i].iov_len = MIN(tot_reqsz,
		    i ? BMPC_BUFSZ :
		    BMPC_BUFSZ - (r->biorq_off - bmpce->bmpce_off));

		tot_reqsz -= bwc->bwc_iovs[i].iov_len;
		bwc->bwc_niovs++;
		OPSTAT_INCR("msl.write-coalesce");
	}
	psc_atomic32_setmax(&slc_write_coalesce_max, bwc->bwc_niovs);

	psc_assert(bwc->bwc_niovs <= BMPC_COALESCE_MAX_IOV);
	psc_assert(!tot_reqsz);
}

/*
 * Check if we can flush the given bmp now.
 */
__static int
bmap_flushable(struct bmap *b)
{
	int flush;
	struct timespec ts;
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(b);
	flush = !RB_EMPTY(&bmpc->bmpc_new_biorqs);

	if (flush) {
		PFL_GETTIMESPEC(&ts);
		if ((bmap_2_bci(b)->bci_etime.tv_sec < ts.tv_sec) ||
		    (bmap_2_bci(b)->bci_etime.tv_sec - ts.tv_sec < BMAP_CLI_EXTREQSECS) ||
		    (b->bcm_flags & BMAPF_LEASEEXPIRED)) {
			OPSTAT_INCR("msl.flush-skip-expire");
			flush = 0;
		}
	}

	return (flush);
}

static void
bwc_desched(struct bmpc_write_coalescer *bwc)
{
	psc_dynarray_reset(&bwc->bwc_biorqs);
	bwc->bwc_soff = bwc->bwc_size = 0;
}

/*
 * Scan the given array of I/O requests for candidates to flush.  We
 * only flush when (1) a request has aged out or (2) we can construct a
 * large enough I/O.
 */
__static struct bmpc_write_coalescer *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *indexp)
{
	int idx, large = 0, expired = 0;
	struct bmpc_write_coalescer *bwc;
	struct bmpc_ioreq *curr, *last = NULL;
	int32_t sz = 0;

	psc_assert(psc_dynarray_len(biorqs) > *indexp);

	bwc = psc_pool_get(bwc_pool);

	for (idx = 0; idx + *indexp < psc_dynarray_len(biorqs);
	    idx++, last = curr) {
		curr = psc_dynarray_getpos(biorqs, idx + *indexp);

		/*
		 * If any member is expired then we'll push everything
		 * out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(curr);

		DEBUG_BIORQ(PLL_DIAG, curr, "biorq #%d (expired=%d)",
		    idx, expired);

		if (idx)
			/* Assert 'lowest to highest' ordering. */
			psc_assert(curr->biorq_off >= last->biorq_off);
		else {
			bwc->bwc_size = curr->biorq_len;
			bwc->bwc_soff = curr->biorq_off;
			psc_dynarray_add(&bwc->bwc_biorqs, curr);
			continue;
		}

		/*
		 * The next request, 'curr', can be added to the
		 * coalesce group because 'curr' overlaps or extends
		 * 'last'.
		 */
		if (curr->biorq_off <= biorq_voff_get(last)) {
			sz = biorq_voff_get(curr) - biorq_voff_get(last);
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
				OPSTAT_INCR("msl.bmap-flush-coalesce-contig");
			}
			/*
			 * All subsequent requests that do not extend
			 * our range should be collapsed here.
			 */
			psc_dynarray_add(&bwc->bwc_biorqs, curr);

			/* keep the old last if we didn't extend */
			if (sz < 0)
				curr = last;

		} else if (expired) {
			/*
			 * Biorq is not contiguous with the previous.
			 * If the current set is expired send it out
			 * now.
			 */
			OPSTAT_INCR("msl.bmap-flush-coalesce-expire");
			break;

		} else {
			/*
			 * Otherwise, deschedule the current set and
			 * resume activity with 't' as the base.
			 */
			bwc_desched(bwc);
			bwc->bwc_size = curr->biorq_len;
			bwc->bwc_soff = curr->biorq_off;
			psc_dynarray_add(&bwc->bwc_biorqs, curr);
			OPSTAT_INCR("msl.bmap-flush-coalesce-restart");
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

void
msl_resm_throttle_wake(struct sl_resm *m)
{
	struct resprof_cli_info *rpci;

	rpci = res2rpci(m->resm_res);
	RPCI_LOCK(rpci);
	rpci->rpci_infl_rpcs--;
	RPCI_WAKE(rpci);
	RPCI_ULOCK(rpci);
}

int
_msl_resm_throttle(struct sl_resm *m, int block)
{
	struct timespec ts0, ts1, tsd;
	struct resprof_cli_info *rpci;
	int account = 0;

	rpci = res2rpci(m->resm_res);
	/*
	 * XXX use resm multiwait?
	 */
	RPCI_LOCK(rpci);
	if (!block && rpci->rpci_infl_rpcs >=
	    msl_ios_max_inflight_rpcs) {
		RPCI_ULOCK(rpci);
		return (-EAGAIN);
	}

	while (rpci->rpci_infl_rpcs >= msl_ios_max_inflight_rpcs) {
		if (!account) {
			PFL_GETTIMESPEC(&ts0);
			account = 1;
		}
		RPCI_WAIT(rpci);
		RPCI_LOCK(rpci);
	}
	rpci->rpci_infl_rpcs++;
	if (rpci->rpci_infl_rpcs > rpci->rpci_max_infl_rpcs)
		rpci->rpci_max_infl_rpcs = rpci->rpci_infl_rpcs;
	RPCI_ULOCK(rpci);
	if (account) {
		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("msl.flush-wait-usecs",
		    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);
	}
	return (0);
}

/*
 * Lease watcher thread: issues "lease extension" RPCs for bmaps when
 * deemed appropriate.
 */
__static void
msbwatchthr_main(struct psc_thread *thr)
{
	struct psc_dynarray bmaps = DYNARRAY_INIT;
	struct bmap *b, *tmpb;
	struct timespec ts;
	int i;

	while (pscthr_run(thr)) {
		/*
		 * A bmap can be on both msl_bmapflushq and
		 * msl_bmaptimeoutq.  It is taken off the msl_bmapflushq
		 * after all its biorqs are flushed if any.
		 */
		LIST_CACHE_LOCK(&msl_bmapflushq);
		if (lc_peekheadwait(&msl_bmapflushq) == NULL) {
			LIST_CACHE_ULOCK(&msl_bmapflushq);
			break;
		}
		LIST_CACHE_FOREACH_SAFE(b, tmpb, &msl_bmapflushq) {
			if (!BMAP_TRYLOCK(b))
				continue;
			DEBUG_BMAP(PLL_DEBUG, b, "begin");
			if ((b->bcm_flags & BMAPF_TOFREE) ||
			    (b->bcm_flags & BMAPF_LEASEFAILED) ||
			    (b->bcm_flags & BMAPF_REASSIGNREQ)) {
				BMAP_ULOCK(b);
				continue;
			}
			PFL_GETTIMESPEC(&ts);
			if ((bmap_2_bci(b)->bci_etime.tv_sec < ts.tv_sec) ||
			    (bmap_2_bci(b)->bci_etime.tv_sec - ts.tv_sec <
				BMAP_CLI_EXTREQSECS))
				psc_dynarray_add(&bmaps, b);
			BMAP_ULOCK(b);
		}
		LIST_CACHE_ULOCK(&msl_bmapflushq);

		if (!psc_dynarray_len(&bmaps)) {
			usleep(1000);
			continue;
		}

		OPSTAT_INCR("msl.lease-refresh");

		DYNARRAY_FOREACH(b, i, &bmaps) {
			/*
			 * XXX: If BMAPF_TOFREE is set after the above
			 * loop but before this one.  The bmap reaper
			 * logic will assert on the bmap reference count
			 * not being zero.  And this has been seen
			 * although with a different patch.
			 */
			BMAP_LOCK(b);
			msl_bmap_lease_tryext(b, 0);
		}
		psc_dynarray_reset(&bmaps);
	}
	psc_dynarray_free(&bmaps);
}

/*
 * Send out SRMT_WRITE RPCs to the I/O server.
 */
__static int
bmap_flush(void)
{
	struct psc_dynarray reqs = DYNARRAY_INIT,
	    bmaps = DYNARRAY_INIT;
	struct bmpc_write_coalescer *bwc;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r;
	struct bmap *b, *tmpb;
	int i, j, didwork = 0;

	LIST_CACHE_LOCK(&msl_bmapflushq);
	LIST_CACHE_FOREACH_SAFE(b, tmpb, &msl_bmapflushq) {

		DEBUG_BMAP(PLL_DIAG, b, "flushable?");

		if (!BMAP_TRYLOCK(b))
			continue;

		psc_assert(b->bcm_flags & BMAPF_FLUSHQ);

		if ((b->bcm_flags & BMAPF_SCHED) ||
		    (b->bcm_flags & BMAPF_REASSIGNREQ)) {
			BMAP_ULOCK(b);
			continue;
		}

		if (bmap_flushable(b) ||
		   (b->bcm_flags & BMAPF_TOFREE) ||
		   (b->bcm_flags & BMAPF_LEASEFAILED)) {
			b->bcm_flags |= BMAPF_SCHED;
			psc_dynarray_add(&bmaps, b);
			bmap_op_start_type(b, BMAP_OPCNT_FLUSH);
		}
		BMAP_ULOCK(b);

		if (psc_dynarray_len(&bmaps) >= msl_ios_max_inflight_rpcs)
			break;
	}
	LIST_CACHE_ULOCK(&msl_bmapflushq);

	for (i = 0; i < psc_dynarray_len(&bmaps); i++) {
		b = psc_dynarray_getpos(&bmaps, i);
		bmpc = bmap_2_bmpc(b);

		/*
		 * Try to catch recently expired bmaps before they are
		 * processed by the write back flush mechanism.
		 */
		BMAP_LOCK(b);
		if (b->bcm_flags & (BMAPF_TOFREE | BMAPF_LEASEFAILED)) {
			b->bcm_flags &= ~BMAPF_SCHED;
			bmpc_biorqs_destroy_locked(b,
			    bmap_2_bci(b)->bci_error);
			goto next;
		}

		DEBUG_BMAP(PLL_DIAG, b, "try flush");

		RB_FOREACH(r, bmpc_biorq_tree, &bmpc->bmpc_new_biorqs) {
			DEBUG_BIORQ(PLL_DEBUG, r, "flushable");
			psc_dynarray_add(&reqs, r);
		}
		BMAP_ULOCK(b);

		j = 0;
		while (j < psc_dynarray_len(&reqs) &&
		    (bwc = bmap_flush_trycoalesce(&reqs, &j))) {
			didwork = 1;
			bmap_flush_coalesce_map(bwc);
			bmap_flush_send_rpcs(bwc);
		}
		psc_dynarray_reset(&reqs);

 next:
		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAPF_SCHED;
		bmap_op_done_type(b, BMAP_OPCNT_FLUSH);
	}

	psc_dynarray_free(&reqs);
	psc_dynarray_free(&bmaps);

	return (didwork);
}

void
msflushthr_main(struct psc_thread *thr)
{
	struct timespec work, delta, tmp1, tmp2;
	struct msflush_thread *mflt;

	mflt = msflushthr(thr);
	while (pscthr_run(thr)) {
		/*
		 * Reset deep counter incremented during successive RPC
		 * failure used to track when to give up (hard retry
		 * count).
		 */
		mflt->mflt_failcnt = 1;

		/* wait until some work appears */
		if (lc_peekheadwait(&msl_bmapflushq) == NULL)
			break;

		OPSTAT_INCR("msl.bmap-flush");

		PFL_GETTIMESPEC(&tmp1);
		while (bmap_flush())
			;
		PFL_GETTIMESPEC(&tmp2);

		timespecsub(&tmp2, &tmp1, &work);

		spinlock(&slc_bflush_lock);
		slc_bflush_tmout_flags |= BMAPFLSH_RPCWAIT;
		psc_waitq_waitrel_ts(&slc_bflush_waitq,
		    &slc_bflush_lock, &msl_bflush_timeout);
		spinlock(&slc_bflush_lock);
		slc_bflush_tmout_flags &= ~BMAPFLSH_RPCWAIT;
		freelock(&slc_bflush_lock);

		PFL_GETTIMESPEC(&tmp1);
		timespecsub(&tmp1, &tmp2, &delta);

		psclogs_debug(SLSS_BMAP, "work time ("PSCPRI_TIMESPEC"),"
		    "wait time ("PSCPRI_TIMESPEC")",
		    PSCPRI_TIMESPEC_ARGS(&work),
		    PSCPRI_TIMESPEC_ARGS(&delta));
	}
}

void
msbenchthr_main(__unusedx struct psc_thread *thr)
{
#if 0
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r;

	pscthr_setpause(thr, 1);
	while (pscthr_run(thr)) {
		e = psc_pool_get(bmpce_pool);
		r = bmpc_biorq_new(q, b, buf, rqnum, off, len, op); {
		msl_pages_schedflush(r);
	}
#endif
}

void
msbmapthr_spawn(void)
{
	struct msflush_thread *mflt;
	struct psc_thread *thr;
	int i;

	psc_waitq_init(&slc_bflush_waitq);

	lc_reginit(&msl_bmapflushq, struct bmap,
	    bcm_lentry, "bmapflushq");

	lc_reginit(&msl_bmaptimeoutq, struct bmap_cli_info,
	    bci_lentry, "bmaptimeout");

	for (i = 0; i < NUM_BMAP_FLUSH_THREADS; i++) {
		thr = pscthr_init(MSTHRT_FLUSH, msflushthr_main, NULL,
		    sizeof(struct msflush_thread), "msflushthr%d", i);
		mflt = msflushthr(thr);
		pfl_multiwait_init(&mflt->mflt_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}

	thr = pscthr_init(MSTHRT_BWATCH, msbwatchthr_main, NULL,
	    sizeof(struct msbwatch_thread), "msbwatchthr");
	pfl_multiwait_init(&msbwatchthr(thr)->mbwt_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

	thr = pscthr_init(MSTHRT_BRELEASE, msbreleasethr_main, NULL,
	    sizeof(struct msbrelease_thread), "msbreleasethr");
	pfl_multiwait_init(&msbreleasethr(thr)->mbrt_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

//	pscthr_init(MSTHRT_BENCH, msbenchthr_main, NULL, 0,
//	    "msbenchthr");
}
