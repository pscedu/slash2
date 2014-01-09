/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include <time.h>

#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/tree.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/lock.h"
#include "pfl/log.h"

#include "bmap_iod.h"
#include "fid.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

int slvr_nbreqset_cb(struct pscrpc_request *, struct pscrpc_async_args *);

struct psc_poolmaster		 bmap_crcupd_poolmaster;
struct psc_poolmgr		*bmap_crcupd_pool;

struct psc_listcache		 bcr_ready;
struct psc_listcache		 bcr_hold;
struct pscrpc_nbreqset		*sl_nbrqset;
struct timespec			 sli_bcr_pause = { 0, 200000L };
struct psc_waitq		 sli_slvr_waitq = PSC_WAITQ_INIT;

/**
 * slvr_worker_crcup_genrq - Send an RPC containing CRC updates for
 *	slivers to the metadata server.  To avoid potential woes caused
 *	by out-of-order deliveries, we should allow at most one inflight
 *	CRC update RPC at any time.  Note that this does not prevent us
 *	from having multiple threads to do the CRC calculation.
 */
__static int
slvr_worker_crcup_genrq(const struct psc_dynarray *bcrs)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct pscrpc_request *rq;
	struct bcrcupd *bcr;
	struct iovec *iovs = NULL;
	size_t len;
	uint32_t i;
	int rc;

	if (psc_fault_here_rc(SLI_FAULT_CRCUP_FAIL, &rc, EHOSTDOWN))
		return (rc);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		return (rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc)
		goto out;

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	rq->rq_interpret_reply = slvr_nbreqset_cb;
	rq->rq_async_args.pointer_arg[0] = (void *)bcrs;
	rq->rq_async_args.pointer_arg[1] = csvc;

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		rc = iod_inode_getinfo(&bcr->bcr_crcup.fg,
		    &bcr->bcr_crcup.fsize, &bcr->bcr_crcup.nblks,
		    &bcr->bcr_crcup.utimgen);

		if (rc)
			goto out;

		DEBUG_BCR(PLL_DIAG, bcr, "bcrs pos=%d fsz=%"PRId64, i,
		    bcr->bcr_crcup.fsize);

		bcr_xid_check(bcr);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = sizeof(struct srm_bmap_crcup) +
		    (mq->ncrcs_per_update[i] *
		     sizeof(struct srt_bmap_crcwire));
	}
	psc_assert(len <= LNET_MTU);

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);

	if (rc)
		goto out;

	rc = SL_NBRQSET_ADD(csvc, rq);

  out:
	PSCFREE(iovs);

	if (rc)
		sl_csvc_decref(csvc);
	else
		OPSTAT_INCR(SLI_OPST_CRC_UPDATE);
	return (rc);
}

__static void
slvr_worker_push_crcups(void)
{
	static atomic_t busy = ATOMIC_INIT(0);
	struct bcrcupd *bcr, *tmp;
	struct psc_dynarray *bcrs;
	struct timespec now, diff;
	int i, rc;

	if (atomic_xchg(&busy, 1))
		return;

	pscrpc_nbreqset_reap(sl_nbrqset);

	/*
	 * Check if an earlier CRC update RPC, if any, has finished.  If
	 * one is still inflight, we won't be able to initiate a new one.
	 */
	bcrs = PSCALLOC(sizeof(struct psc_dynarray));

	/*
	 * Leave scheduled bcr's on the list so that in case of a
	 * failure, ordering will be maintained.
	 */
	LIST_CACHE_LOCK(&bcr_ready);
	LIST_CACHE_FOREACH(bcr, &bcr_ready) {
		psc_assert(bcr->bcr_crcup.nups > 0);

		if (!BII_TRYLOCK(bcr->bcr_bii))
			continue;

		if (bcr->bcr_flags & BCR_SCHEDULED) {
			BII_ULOCK(bcr->bcr_bii);
			continue;
		}

		if (bcr_2_bmap(bcr)->bcm_flags & BMAP_IOD_INFLIGHT)
			DEBUG_BCR(PLL_FATAL, bcr, "tried to schedule "
			    "multiple bcr's xid=%"PRIu64,
			    bcr->bcr_bii->bii_bcr_xid_last);

		bcr_2_bmap(bcr)->bcm_flags |= BMAP_IOD_INFLIGHT;

		psc_dynarray_add(bcrs, bcr);
		bcr->bcr_flags |= BCR_SCHEDULED;

		BII_ULOCK(bcr->bcr_bii);

		DEBUG_BCR(PLL_INFO, bcr, "scheduled nbcrs=%d total_bcrs=%d",
		    psc_dynarray_len(bcrs),
		    lc_nitems(&bcr_ready) + lc_nitems(&bcr_hold));

		if (psc_dynarray_len(bcrs) == MAX_BMAP_NCRC_UPDATES)
			break;
	}
	LIST_CACHE_ULOCK(&bcr_ready);

	/* Now scan for old bcr's hanging about. */
	LIST_CACHE_LOCK(&bcr_hold);
	PFL_GETTIMESPEC(&now);
	LIST_CACHE_FOREACH_SAFE(bcr, tmp, &bcr_hold) {
		/* Use trylock here to avoid deadlock. */
		if (!BII_TRYLOCK(bcr->bcr_bii))
			continue;

		timespecsub(&now, &bcr->bcr_age, &diff);
		if (diff.tv_sec < BCR_MAX_AGE) {
			BII_ULOCK(bcr->bcr_bii);
			continue;
		}

		bcr_hold_2_ready(bcr);
		DEBUG_BCR(PLL_INFO, bcr, "old, moved to ready");
		BII_ULOCK(bcr->bcr_bii);
	}
	LIST_CACHE_ULOCK(&bcr_hold);

	if (!psc_dynarray_len(bcrs))
		PSCFREE(bcrs);
	else {
		rc = slvr_worker_crcup_genrq(bcrs);
		/*
		 * If we fail to send an RPC, we must leave the
		 * reference in the tree for future attempt(s).
		 * Otherwise, the callback function (i.e.
		 * slvr_nbreqset_cb()) should remove them from the tree.
		 */
		if (rc) {
			for (i = 0; i < psc_dynarray_len(bcrs); i++) {
				bcr = psc_dynarray_getpos(bcrs, i);

				BII_LOCK(bcr->bcr_bii);
				bcr->bcr_flags &= ~BCR_SCHEDULED;
				bcr_2_bmap(bcr)->bcm_flags &=
				    ~BMAP_IOD_INFLIGHT;
				BII_ULOCK(bcr->bcr_bii);

				DEBUG_BCR(PLL_INFO, bcr,
				    "unsetting BCR_SCHEDULED");
			}
			psc_dynarray_free(bcrs);
			PSCFREE(bcrs);
		}
	}

	atomic_set(&busy, 0);
}

int
slvr_nbreqset_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct srm_bmap_crcwrt_rep *mp;
	struct srm_bmap_crcwrt_req *mq;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info *bii;
	struct psc_dynarray *a;
	struct bcrcupd *bcr;
	int i;

	OPSTAT_INCR(SLI_OPST_CRC_UPDATE_CB);

	a = args->pointer_arg[0];
	csvc = args->pointer_arg[1];
	psc_assert(a);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		bii = bcr->bcr_bii;

		DEBUG_BCR(rq->rq_status || !mp || mp->rc ?
		    PLL_ERROR : PLL_INFO, bcr, "rq_status=%d rc=%d%s",
		    rq->rq_status, mp ? mp->rc : -4096,
		    mp ? "" : " (unknown, no buf)");

		psc_assert(bii_2_bmap(bii)->bcm_flags &
		    (BMAP_IOD_INFLIGHT | BMAP_IOD_BCRSCHED));

		if (rq->rq_status) {
			/*
			 * Unset the inflight bit on the bii since
			 * bcr_xid_last_bump() will not be called.
			 */
			BII_LOCK(bii);
			bcr->bcr_flags &= ~BCR_SCHEDULED;
			bii_2_bmap(bii)->bcm_flags &= ~BMAP_IOD_INFLIGHT;
			bcr_xid_check(bcr);
			BII_ULOCK(bii);

			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");
			OPSTAT_INCR(SLI_OPST_CRC_UPDATE_CB_FAILURE);
		} else
			/*
			 * bcr will be freed, so no need to clear
			 * BCR_SCHEDULED in this case, but we do clear
			 * BMAP_IOD_INFLIGHT.
			 */
			bcr_finalize(bcr);
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

	return (1);
}

/**
 * slislvrthr_proc - Attempt to setup a bmap CRC update RPC for dirty
 *	part(s) of a sliver.
 */
__static void
slislvrthr_proc(struct slvr *s)
{
	struct bmap_iod_info *bii;
	struct bcrcupd *bcr;
	struct bmap *b;
	uint16_t slvr_num;
	uint64_t crc;

	SLVR_LOCK(s);
	DEBUG_SLVR(PLL_DEBUG, s, "got sliver");

	psc_assert(!(s->slvr_flags & SLVR_LRU));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	/*
	 * Try our best to determine whether we should hold off the
	 * CRC operation strivonly CRC slivers which have no pending
	 * writes.  This section directly below may race with
	 * slvr_wio_done().
	 */
	if (s->slvr_pndgwrts > 0) {
		s->slvr_flags |= SLVR_LRU;
		lc_addqueue(&lruSlvrs, s);

		DEBUG_SLVR(PLL_DIAG, s, "descheduled due to pndg writes");

		SLVR_ULOCK(s);
		return;
	}

	/*
	 * OK, we've got a sliver to work on.  From this point until we
	 * set to inflight, the slvr_lentry should be disjointed.
	 */

	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));

	/* Be paranoid, ensure the sliver is not queued anywhere. */
	psc_assert(psclist_disjoint(&s->slvr_lentry));

	/* Copy the accumulator to the tmp variable. */
	crc = s->slvr_crc;
	PSC_CRC64_FIN(&crc);

	/* Put the slvr back to the LRU so it may have its slab reaped. */
	bii = slvr_2_bii(s);
	b = bii_2_bmap(bii);
	psc_atomic32_dec(&bii->bii_crcdrty_slvrs);
	s->slvr_dirty_cnt--;

	slvr_num = s->slvr_num;

	DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU (ndirty=%u)",
	    psc_atomic32_read(&bii->bii_crcdrty_slvrs));

	s->slvr_flags |= SLVR_LRU;
	lc_addqueue(&lruSlvrs, s);
	slvr_lru_tryunpin_locked(s);

	bmap_op_start_type(b, BMAP_OPCNT_BCRSCHED);
	SLVR_ULOCK(s);

	LIST_CACHE_LOCK(&bcr_hold);
	BII_LOCK(bii);
	bcr = bii->bii_bcr;

	if (bcr) {
		uint32_t i;
		int found;

		psc_assert(bcr->bcr_crcup.blkno == b->bcm_bmapno);
		psc_assert(bcr->bcr_crcup.fg.fg_fid ==
		    b->bcm_fcmh->fcmh_fg.fg_fid);
		psc_assert(bcr->bcr_crcup.nups < MAX_BMAP_INODE_PAIRS);
		psc_assert(b->bcm_flags & BMAP_IOD_BCRSCHED);

		/*
		 * If we already have a slot for our slvr_num then reuse
		 * it.
		 */
		for (i = 0, found = 0; i < bcr->bcr_crcup.nups; i++) {
			if (bcr->bcr_crcup.crcs[i].slot == slvr_num) {
				found = 1;
				break;
			}
		}

		bcr->bcr_crcup.crcs[i].crc = crc;
		if (!found) {
			bcr->bcr_crcup.nups++;
			bcr->bcr_crcup.crcs[i].slot = slvr_num;
		}

		DEBUG_BCR(PLL_DIAG, bcr, "add to existing bcr slot=%d "
		    "nups=%d", i, bcr->bcr_crcup.nups);

		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS) {
			if (pll_nitems(&bii->bii_bklog_bcrs))
				/*
				 * This is a backlogged bcr, cap it and
				 * move on.
				 */
				bcr->bcr_bii->bii_bcr = NULL;
			else
				/* The bcr is full; push it out now. */
				bcr_hold_2_ready(bcr);
		}
	} else {
		bmap_op_start_type(b, BMAP_OPCNT_BCRSCHED);

		bii->bii_bcr = bcr = psc_pool_get(bmap_crcupd_pool);
		memset(bcr, 0, bmap_crcupd_pool->ppm_entsize);

		INIT_PSC_LISTENTRY(&bcr->bcr_lentry);
		COPYFG(&bcr->bcr_crcup.fg, &b->bcm_fcmh->fcmh_fg);

		bcr->bcr_bii = bii;
		bcr->bcr_xid = bii->bii_bcr_xid++;
		bcr->bcr_crcup.blkno = b->bcm_bmapno;
		bcr->bcr_crcup.crcs[0].crc = crc;
		bcr->bcr_crcup.crcs[0].slot = slvr_num;
		bcr->bcr_crcup.nups = 1;

		psclog_info("new bcr@%p xid=%"PRIu64" bii@%p xid=%"PRIu64" bmap=%p",
		    bcr, bcr->bcr_xid, bii, bii->bii_bcr_xid, b);
		DEBUG_BCR(PLL_DIAG, bcr,
		    "newly added (bcr_bklog=%d) (sched=%d)",
		    pll_nitems(&bii->bii_bklog_bcrs),
		    !!(b->bcm_flags & BMAP_IOD_BCRSCHED));

		if (b->bcm_flags & BMAP_IOD_BCRSCHED) {
			/*
			 * The bklog may be empty but a pending bcr may
			 * be present on the ready list.
			 */
			OPSTAT_INCR(SLI_OPST_CRC_UPDATE_BACKLOG);
			pll_addtail(&bii->bii_bklog_bcrs, bcr);
		} else {
			b->bcm_flags |= BMAP_IOD_BCRSCHED;
			bcr_hold_add(bcr);
		}
		PFL_GETTIMESPEC(&bcr->bcr_age);
	}

	BII_ULOCK(bii);
	LIST_CACHE_ULOCK(&bcr_hold);

	bmap_op_done_type(b, BMAP_OPCNT_BCRSCHED);

	slvr_worker_push_crcups();
}

/**
 * slislvrthr_main - Guts of the sliver bmap CRC update RPC generator
 *	thread.  RPCs are constructed from slivers on the queue after
 *	the minimal expiration is reached and shipped off to the MDS.
 */
void
slislvrthr_main(struct psc_thread *thr)
{
	int i;
	struct timespec expire;
	struct slvr *s, *dummy;
	struct psc_dynarray ss = DYNARRAY_INIT_NOLOG;

	while (pscthr_run(thr)) {

		LIST_CACHE_LOCK(&crcqSlvrs);

		PFL_GETTIMESPEC(&expire);
		expire.tv_sec -= BCR_MIN_AGE;

		LIST_CACHE_FOREACH_SAFE(s, dummy, &crcqSlvrs) {
			if (!SLVR_TRYLOCK(s))
				continue;
			if (timespeccmp(&expire, &s->slvr_ts, >)) {
				lc_remove(&crcqSlvrs, s);
				psc_dynarray_add(&ss, s);
			}
			SLVR_ULOCK(s);
			if (psc_dynarray_len(&ss) >= MAX_BMAP_NCRC_UPDATES)
				break;
		}
		LIST_CACHE_ULOCK(&crcqSlvrs);

		DYNARRAY_FOREACH(s, i, &ss) {
			slislvrthr_proc(s);
		}
		slvr_worker_push_crcups();

		PFL_GETTIMESPEC(&expire);
		expire.tv_sec += BCR_MIN_AGE;
		psc_waitq_timedwait(&sli_slvr_waitq, NULL, &expire);

		psc_dynarray_reset(&ss);
	}
	psc_dynarray_free(&ss);
}

void
slvr_worker_init(void)
{
	int i;

	lc_reginit(&bcr_ready, struct bcrcupd, bcr_lentry, "bcr_ready");
	lc_reginit(&bcr_hold, struct bcrcupd, bcr_lentry, "bcr_hold");
	bcr_hold.plc_flags |= PLLF_EXTLOCK;
	bcr_hold.plc_lockp = &bcr_ready.plc_lock;

	_psc_poolmaster_init(&bmap_crcupd_poolmaster,
	    sizeof(struct bcrcupd) +
	    sizeof(struct srt_bmap_crcwire) * MAX_BMAP_INODE_PAIRS,
	    offsetof(struct bcrcupd, bcr_lentry), PPMF_AUTO, 64,
	    64, 0, NULL, NULL, NULL, NULL, "bcrcupd");
	bmap_crcupd_pool = psc_poolmaster_getmgr(&bmap_crcupd_poolmaster);

	for (i = 0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, 0, slislvrthr_main, NULL,
		    0, "slislvrthr%d", i);
}
