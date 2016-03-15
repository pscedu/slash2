/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include <time.h>

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fault.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/tree.h"

#include "bmap_iod.h"
#include "fid.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

struct psc_poolmaster		 bmap_crcupd_poolmaster;
struct psc_poolmgr		*bmap_crcupd_pool;
psc_atomic32_t			 sli_ninfl_bcrcupd;

struct psc_listcache		 bcr_ready;
struct timespec			 sli_bcr_pause = { 0, 200000L };
struct psc_waitq		 sli_slvr_waitq = PSC_WAITQ_INIT;

int
sli_rmi_bcrcupd_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[1];
	struct psc_dynarray *a = args->pointer_arg[0];
	struct srm_bmap_crcwrt_rep *mp;
	struct srm_bmap_crcwrt_req *mq;
	struct bmap_iod_info *bii;
	struct bcrcupd *bcr;
	int rc, i;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		bii = bcr->bcr_bii;

		DEBUG_BCR(rc || !mp || mp->rc ?
		    PLL_ERROR : PLL_DIAG, bcr, "rq_status=%d rc=%d%s",
		    rq->rq_status, rc, mp ? "" : " (unknown, no buf)");

		BII_LOCK(bii);
		psc_assert(bii_2_bmap(bii)->bcm_flags &
		    BMAPF_CRUD_INFLIGHT);
		bii_2_bmap(bii)->bcm_flags &= ~BMAPF_CRUD_INFLIGHT;

		if (rc) {
			BII_ULOCK(bii);
			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");
			OPSTAT_INCR("crc-update-cb-failure");
			continue;
		}
		bcr_ready_remove(bcr);
	}

	/*
	 * If there were errors, log them but obviously the MDS will
	 * make the master choice about what our residency validity is.
	 */
	for (i = 0; i < (int)mq->ncrc_updates; i++)
		if (mp && mp->crcup_rc[i])
			psclog_errorx("MDS rejected our CRC update; "
			    "rc=%d", mp->crcup_rc[i]);

	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);

	psc_atomic32_dec(&sli_ninfl_bcrcupd);

	return (1);
}

/*
 * Send an RPC containing CRC updates for slivers to the MDS.  To avoid
 * potential woes caused by out-of-order deliveries, we allow at most
 * only one inflight CRC update RPC at any time.  Note that this does
 * not prevent us from having multiple threads to do the CRC
 * calculation.
 */
__static int
slvr_worker_crcup_genrq(const struct psc_dynarray *bcrs)
{
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct iovec *iovs = NULL;
	struct bcrcupd *bcr;
	size_t len;
	uint32_t i;
	int rc;

	if (pfl_fault_here_rc("sliod/crcup_fail", &rc, EHOSTDOWN))
		return (rc);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		return (rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	rq->rq_interpret_reply = sli_rmi_bcrcupd_cb;
	rq->rq_async_args.pointer_arg[0] = (void *)bcrs;
	rq->rq_async_args.pointer_arg[1] = csvc;

	len = mq->ncrc_updates * sizeof(struct srt_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		rc = bcr_update_inodeinfo(bcr);
		if (rc) {
			/*
			 * XXX hack because the bcr code is awfully
			 * structured.
			 */
			bcr->bcr_crcup.fg.fg_fid = FID_ANY;
		}

		DEBUG_BCR(PLL_DIAG, bcr, "bcrs pos=%d fsz=%"PRId64, i,
		    bcr->bcr_crcup.fsize);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = sizeof(struct srt_bmap_crcup) +
		    (mq->ncrcs_per_update[i] *
		     sizeof(struct srt_bmap_crcwire));
	}
	psc_assert(len <= LNET_MTU);

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc)
		PFL_GOTOERR(out, rc);

	psc_atomic32_inc(&sli_ninfl_bcrcupd);

  out:
	PSCFREE(iovs);

	if (rc) {
		if (rq)
			pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
	return (rc);
}

void
slicrudthr_main(struct psc_thread *thr)
{
	struct psc_dynarray *bcrs;
	struct timespec now, diff;
	struct bcrcupd *bcr;
	int i, rc;

	bcrs = PSCALLOC(sizeof(*bcrs));

	while (pscthr_run(thr)) {
#define MAX_INFL_BCRCUPD 128
		/* XXX per-MDS */
		while (psc_atomic32_read(&sli_ninfl_bcrcupd) >
		    MAX_INFL_BCRCUPD)
			usleep(3000);

		LIST_CACHE_LOCK(&bcr_ready);
		PFL_GETTIMESPEC(&now);
		LIST_CACHE_FOREACH(bcr, &bcr_ready) {
			psc_assert(bcr->bcr_crcup.nups > 0);

			if (!BII_TRYLOCK(bcr->bcr_bii))
				continue;

			/*
			 * Leave scheduled bcr's on the list so that in
			 * case of a failure, ordering will be
			 * maintained.
			 */
			if (bcr_2_bmap(bcr)->bcm_flags &
			    BMAPF_CRUD_INFLIGHT) {
				BII_ULOCK(bcr->bcr_bii);
				continue;
			}

			timespecsub(&now, &bcr->bcr_age, &diff);
			if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS ||
			    diff.tv_sec >= BCR_BATCH_AGE) {
				psc_dynarray_add(bcrs, bcr);
				bcr->bcr_bii->bii_bcr = NULL;
				bcr_2_bmap(bcr)->bcm_flags |=
				    BMAPF_CRUD_INFLIGHT;
			}

			BII_ULOCK(bcr->bcr_bii);

			DEBUG_BCR(PLL_DIAG, bcr,
			    "scheduled nbcrs=%d total_bcrs=%d",
			    psc_dynarray_len(bcrs),
			    lc_nitems(&bcr_ready));

			if (psc_dynarray_len(bcrs) ==
			    MAX_BMAP_NCRC_UPDATES)
				break;
		}
		LIST_CACHE_ULOCK(&bcr_ready);

		if (!psc_dynarray_len(bcrs)) {
			usleep(100);
			continue;
		}

		OPSTAT_INCR("crc-update-push");

		/*
		 * If we fail to send an RPC, we must leave the
		 * reference in the tree for future attempt(s).
		 * Otherwise, the callback function (i.e.
		 * sli_rmi_bcrcupd_cb) should remove them from the tree.
		 */
		rc = slvr_worker_crcup_genrq(bcrs);
		if (rc) {
			DYNARRAY_FOREACH(bcr, i, bcrs) {
				BII_LOCK(bcr->bcr_bii);
				bcr_2_bmap(bcr)->bcm_flags &=
				    ~BMAPF_CRUD_INFLIGHT;
				//bcr->bcr_bii->bii_bcr = NULL;
				BII_ULOCK(bcr->bcr_bii);

				/*
				 * There is a newer FID generation in
				 * existence than our packaged update is
				 * for, so this can be safely discarded.
				 */
				if (rc == ESTALE ||
				    rc == EBADF) {
					bcr_ready_remove(bcr);
				}
			}
			psc_dynarray_reset(bcrs);
		} else {
			bcrs = PSCALLOC(sizeof(*bcrs));
		}
	}
}

/*
 * Attempt to setup a bmap CRC update RPC for dirty part(s) of a sliver.
 */
__static void
slislvrthr_proc(struct slvr *s)
{
	struct bmap_iod_info *bii;
	struct bcrcupd *bcr;
	struct bmap *b;
	uint64_t crc;

	/*
	 * Take a reference now because we might free the sliver later.
	 */
	bii = slvr_2_bii(s);
	b = bii_2_bmap(bii);
	bmap_op_start_type(b, BMAP_OPCNT_BCRSCHED);

	SLVR_LOCK(s);
	DEBUG_SLVR(PLL_DEBUG, s, "got sliver");

	psc_assert(!(s->slvr_flags & SLVRF_LRU));
	psc_assert(s->slvr_flags & SLVRF_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVRF_DATARDY);

	/*
	 * OK, we've got a sliver to work on.  From this point until we
	 * set to inflight, the slvr_lentry should be disjointed.
	 */

	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s, &crc));

	/* Be paranoid, ensure the sliver is not queued anywhere. */
	psc_assert(psclist_disjoint(&s->slvr_lentry));

	/* Put the slvr back to the LRU so it may have its slab reaped. */

	if ((s->slvr_flags & SLVRF_DATAERR) && !s->slvr_refcnt) {
		OPSTAT_INCR("slvr-crc-remove2");
		SLVR_ULOCK(s);
		/*
		 * We are protected by SLVRF_FAULTING and we are on the
		 * CRC list.
		 */
		bmap_op_done_type(b, BMAP_OPCNT_BCRSCHED);
		slvr_remove(s);
		return;
	}

	s->slvr_flags |= SLVRF_LRU;
	s->slvr_flags &= ~(SLVRF_CRCDIRTY | SLVRF_FAULTING);
	OPSTAT_INCR("slvr-crc-requeue");
	lc_addtail(&sli_lruslvrs, s);
	SLVR_WAKEUP(s);
	SLVR_ULOCK(s);

	BII_LOCK(bii);
	bcr = bii->bii_bcr;

	if (bcr) {
		uint32_t i;
		int found;

		psc_assert(bcr->bcr_crcup.bno == b->bcm_bmapno);
		psc_assert(bcr->bcr_crcup.fg.fg_fid ==
		    b->bcm_fcmh->fcmh_fg.fg_fid);
		psc_assert(bcr->bcr_crcup.nups < MAX_BMAP_INODE_PAIRS);

		/*
		 * If we already have a slot for our slvr_num then reuse
		 * it.
		 */
		for (i = 0, found = 0; i < bcr->bcr_crcup.nups; i++) {
			if (bcr->bcr_crcup.crcs[i].slot == s->slvr_num) {
				found = 1;
				break;
			}
		}

		bcr->bcr_crcup.crcs[i].crc = crc;
		if (!found) {
			bcr->bcr_crcup.nups++;
			bcr->bcr_crcup.crcs[i].slot = s->slvr_num;
		}

		DEBUG_BCR(PLL_DIAG, bcr, "add to existing bcr slot=%d "
		    "nups=%d", i, bcr->bcr_crcup.nups);

		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
			bcr->bcr_bii->bii_bcr = NULL;
	} else {
		bii->bii_bcr = bcr = psc_pool_get(bmap_crcupd_pool);
		memset(bcr, 0, bmap_crcupd_pool->ppm_entsize);

		INIT_PSC_LISTENTRY(&bcr->bcr_lentry);
		COPYFG(&bcr->bcr_crcup.fg, &b->bcm_fcmh->fcmh_fg);

		bcr->bcr_bii = bii;
		bcr->bcr_crcup.bno = b->bcm_bmapno;
		bcr->bcr_crcup.crcs[0].crc = crc;
		bcr->bcr_crcup.crcs[0].slot = s->slvr_num;
		bcr->bcr_crcup.nups = 1;

		bcr_ready_add(bcr);
		PFL_GETTIMESPEC(&bcr->bcr_age);
	}

	bmap_op_done_type(b, BMAP_OPCNT_BCRSCHED);
}

#define	SLI_SYNC_AHEAD_BATCH	10

void
sli_sync_ahead(void)
{
	int i, skip;
	struct fidc_membh *f;
	struct fcmh_iod_info *fii;
	struct psc_dynarray a = DYNARRAY_INIT;

	psc_dynarray_ensurelen(&a, SLI_SYNC_AHEAD_BATCH);

 restart:

	skip = 0;
	LIST_CACHE_LOCK(&sli_fcmh_dirty);
	LIST_CACHE_FOREACH(fii, &sli_fcmh_dirty) {
		f = fii_2_fcmh(fii);

		if (!FCMH_TRYLOCK(f)) {
			skip++;
			continue;
		}
		if (f->fcmh_flags & FCMH_IOD_SYNCFILE) {
			FCMH_ULOCK(f);
			continue;
		}
		fii->fii_nwrite = 0;
		f->fcmh_flags |= FCMH_IOD_SYNCFILE;
		fcmh_op_start_type(f, FCMH_OPCNT_SYNC_AHEAD);
		FCMH_ULOCK(f);
		psc_dynarray_add(&a, f);
	}
	LIST_CACHE_ULOCK(&sli_fcmh_dirty);

	DYNARRAY_FOREACH(f, i, &a) {

		fsync(fcmh_2_fd(f));
		OPSTAT_INCR("sync-ahead");

		DEBUG_FCMH(PLL_DIAG, f, "sync ahead");

		FCMH_LOCK(f);
		fii = fcmh_2_fii(f);
		if (fii->fii_nwrite < sli_sync_max_writes / 2) {
			OPSTAT_INCR("sync-ahead-remove");
			lc_remove(&sli_fcmh_dirty, fii);
			f->fcmh_flags &= ~FCMH_IOD_DIRTYFILE;
		}
		f->fcmh_flags &= ~FCMH_IOD_SYNCFILE;
		fcmh_op_done_type(f, FCMH_OPCNT_SYNC_AHEAD);
	}
	if (skip) {
		sleep(5);
		psc_dynarray_reset(&a);
		goto restart;
	}
	psc_dynarray_free(&a);
}

void
slisyncthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		lc_peekheadwait(&sli_fcmh_dirty);
		sli_sync_ahead();
	}
}

/*
 * Guts of the sliver bmap CRC update RPC generator thread.  RPCs are
 * constructed from slivers on the queue after the minimal expiration is
 * reached and shipped off to the MDS.
 */
void
slislvrthr_main(struct psc_thread *thr)
{
	static struct pfl_mutex mtx = PSC_MUTEX_INIT;

	struct psc_dynarray ss = DYNARRAY_INIT_NOLOG;
	struct timespec expire;
	struct slvr *s, *dummy;
	int i;

	while (pscthr_run(thr)) {
		psc_mutex_lock(&mtx);

		LIST_CACHE_LOCK(&sli_crcqslvrs);
		lc_peekheadwait(&sli_crcqslvrs);

		PFL_GETTIMESPEC(&expire);
		expire.tv_sec -= CRC_QUEUE_AGE;

		LIST_CACHE_FOREACH_SAFE(s, dummy, &sli_crcqslvrs) {
			if (!SLVR_TRYLOCK(s))
				continue;
			/*
			 * A sliver can be referenced while being
			 * CRC'ed.  However, we want to wait for all
			 * references to go away before doing CRC on it.
			 */
			if (s->slvr_refcnt ||
			    s->slvr_flags & SLVRF_FREEING) {
				SLVR_ULOCK(s);
				continue;
			}
			if (s->slvr_flags & SLVRF_DATAERR) {
				s->slvr_flags |= SLVRF_FREEING;
				OPSTAT_INCR("slvr-crc-remove1");
				SLVR_ULOCK(s);
				slvr_remove(s);
				continue;
			}
			if (timespeccmp(&expire, &s->slvr_ts, >)) {
				s->slvr_flags |= SLVRF_FAULTING;
				lc_remove(&sli_crcqslvrs, s);
				psc_dynarray_add(&ss, s);
				/* XXX can't we break here ? */
			}
			SLVR_ULOCK(s);
			if (psc_dynarray_len(&ss) >=
			    MAX_BMAP_NCRC_UPDATES)
				break;
		}
		LIST_CACHE_ULOCK(&sli_crcqslvrs);

		psc_mutex_unlock(&mtx);

		/*
		 * If we didn't do any work, induce sleep to avoid
		 * spinning.
		 */
		if (!psc_dynarray_len(&ss)) {
			usleep(1000);
			continue;
		}

		DYNARRAY_FOREACH(s, i, &ss)
			slislvrthr_proc(s);

		psc_dynarray_reset(&ss);
	}
	psc_dynarray_free(&ss);
}

void
slvr_worker_init(void)
{
	int i;

	lc_reginit(&bcr_ready, struct bcrcupd, bcr_lentry, "bcr_ready");

	_psc_poolmaster_init(&bmap_crcupd_poolmaster,
	    sizeof(struct bcrcupd) +
	    sizeof(struct srt_bmap_crcwire) * MAX_BMAP_INODE_PAIRS,
	    offsetof(struct bcrcupd, bcr_lentry), PPMF_AUTO, 64,
	    64, 0, NULL, NULL, NULL, NULL, "bcrcupd");
	bmap_crcupd_pool = psc_poolmaster_getmgr(&bmap_crcupd_poolmaster);

	for (i = 0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, slislvrthr_main, NULL, 0,
		    "slislvrthr%d", i);

	for (i = 0; i < NSLVRSYNC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_SYNC, slisyncthr_main, NULL, 0,
		    "slisyncthr%d", i);

	pscthr_init(SLITHRT_CRUD, slicrudthr_main, NULL, 0,
	    "slicrudthr");
}
