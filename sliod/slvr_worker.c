/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/fault.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

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
struct timespec			 slvrCrcDelay = { 0, 50000000L }; /* 50 milliseconds */
struct psc_waitq		 slvrWaitq = PSC_WAITQ_INIT;

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
	struct iovec *iovs;
	size_t len;
	uint32_t i;
	int rc;

	if (psc_fault_here_rc(SLI_FAULT_CRCUP_FAIL, &rc, EHOSTDOWN))
		return (rc);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		return (rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return (rc);
	}

	PSC_CRC64_INIT(&mq->crc);

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

		/* Bail for now if we can't stat() our file objects. */
		psc_assert(!rc);

		DEBUG_BCR(PLL_DIAG, bcr, "bcrs pos=%d fsz=%"PRId64, i,
		    bcr->bcr_crcup.fsize);

		bcr_xid_check(bcr);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = sizeof(struct srm_bmap_crcup) +
		    (mq->ncrcs_per_update[i] *
		     sizeof(struct srt_bmap_crcwire));

		psc_crc64_add(&mq->crc, iovs[i].iov_base, iovs[i].iov_len);
	}
	psc_assert(len <= LNET_MTU);

	PSC_CRC64_FIN(&mq->crc);

	rc = rsx_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL, iovs,
	    mq->ncrc_updates);
	PSCFREE(iovs);

	psc_assert(rc == 0);

	OPSTAT_INCR(SLI_OPST_CRC_UPDATE);

	rc = SL_NBRQSET_ADD(csvc, rq);
	psc_assert(rc == 0);
	return (rc);
}

__static void
slvr_worker_push_crcups(void)
{
	static atomic_t busy = ATOMIC_INIT(0);
	struct bcrcupd *bcr, *tmp;
	struct psc_dynarray *bcrs;
	struct timespec now;
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
	 * failure ordering will be maintained.
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

	PFL_GETTIMESPEC(&now);
	/* Now scan for old bcr's hanging about. */
	LIST_CACHE_LOCK(&bcr_hold);
	LIST_CACHE_FOREACH_SAFE(bcr, tmp, &bcr_hold) {
		/* Use trylock here to avoid deadlock. */
		if (!BII_TRYLOCK(bcr->bcr_bii))
			continue;

		if (now.tv_sec <
		    (bcr->bcr_age.tv_sec + BCR_MAX_AGE)) {
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
		 * reference in the tree for future attempt.  Otherwise,
		 * the callback function (i.e. slvr_nbreqset_cb())
		 * should remove them from the tree.
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
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info *bii;
	struct psc_dynarray *a;
	struct bcrcupd *bcr;
	int i;

	OPSTAT_INCR(SLI_OPST_CRC_UPDATE_CB);

	a = args->pointer_arg[0];
	csvc = args->pointer_arg[1];
	psc_assert(a);

	sli_rpc_mds_unpack_bminseq(rq, PSCRPC_MSG_REPLY);

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		bii = bcr->bcr_bii;

		DEBUG_BCR(((rq->rq_status || !mp || mp->rc) ?
		    PLL_ERROR : PLL_INFO), bcr, "rq_status=%d rc=%d%s",
		    rq->rq_status, mp ? mp->rc : -4096,
		    mp ? "" : " (unknown, no buf)");

		psc_assert(bii_2_bmap(bii)->bcm_flags &
		    (BMAP_IOD_INFLIGHT|BMAP_IOD_BCRSCHED));

		if (rq->rq_status) {
			/*
			 * Unset the inflight bit on the bii since
			 * bcr_xid_last_bump() will not be called.
			 */
			BII_LOCK(bii);
			bcr->bcr_flags &= ~BCR_SCHEDULED;
			BMAP_CLEARATTR(bii_2_bmap(bii), BMAP_IOD_INFLIGHT);
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
	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);

	return (1);
}

__static void
slvr_worker_int(void)
{
	struct bcrcupd *bcr = NULL;
	struct timespec ts, slvr_ts;
	struct bmap_iod_info *bii;
	struct bmapc_memb *b;
	struct slvr_ref	*s;
	uint16_t slvr_num;
	uint64_t crc;

#define SLVR_MIN_FREE 8

 start:
	PFL_GETTIMESPEC(&ts);
	s = NULL;
	if ((psc_pool_nfree(sl_bufs_pool) > SLVR_MIN_FREE) ||
	    lc_nitems(&lruSlvrs) > SLVR_MIN_FREE) {
		LIST_CACHE_LOCK(&crcqSlvrs);

		if ((s = lc_peekhead(&crcqSlvrs))) {
			DEBUG_SLVR(PLL_INFO, s, "peek");
			slvr_ts = s->slvr_ts;
			timespecadd(&slvr_ts, &slvrCrcDelay, &slvr_ts);

			if (timespeccmp(&ts, &slvr_ts, >))
				lc_remove(&crcqSlvrs, s);
			else {
				ts = slvr_ts;
				s = NULL;
			}
		} else
			ts.tv_sec += BCR_MAX_AGE;

		LIST_CACHE_ULOCK(&crcqSlvrs);

		if (!s) {
			int n;

			if ((n = atomic_read(&slvrWaitq.wq_nwaiters))) {
				struct timespec tmp = { 0, 200000L };

				tmp.tv_nsec *= n;
				timespecadd(&ts, &tmp, &ts);
			}

			psc_waitq_timedwait(&slvrWaitq, NULL, &ts);
			slvr_worker_push_crcups();
			goto start;

		} else {
			slvr_ts = s->slvr_ts;
			timespecadd(&slvr_ts, &slvrCrcDelay, &slvr_ts);

			if (timespeccmp(&ts, &slvr_ts, <)) {
				/*
				 * The case where a new write updated
				 * the ts between list removal and now.
				 */
				lc_addtail(&crcqSlvrs, s);
				goto start;
			}
		}

		DEBUG_SLVR(PLL_INFO, s, "crc0");

	} else {
		ts.tv_sec += BCR_MAX_AGE;
		s = lc_gettimed(&crcqSlvrs, &ts);

		if (!s) {
			slvr_worker_push_crcups();
			return;
		}

		DEBUG_SLVR(PLL_INFO, s, "crc1");
	}

	SLVR_LOCK(s);
	DEBUG_SLVR(PLL_INFO, s, "slvr_worker");

	/* Sliver assertions:
	 *  CRCING - only one slvr_worker thread may handle a sliver.
	 *  !LRU - ensure that slvr is in the right state.
	 *  CRCDIRTY - must have work to do.
	 *  PINNED - slab must not move from beneath us because the
	 *           contents must be crc'd.
	 */
	psc_assert(!(s->slvr_flags & (SLVR_CRCING | SLVR_LRU)));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	/*
	 * Try our best to determine whether or we should hold off the
	 * CRC operation strivonly crc slivers which have no pending
	 * writes.  This section directly below may race with
	 * slvr_wio_done().
	 */
	if (s->slvr_pndgwrts > 0) {
		s->slvr_flags |= SLVR_LRU;
		lc_addqueue(&lruSlvrs, s);

		DEBUG_SLVR(PLL_INFO, s, "descheduled due to pndg writes");

		SLVR_ULOCK(s);
		goto start;
	}

	/*
	 * OK, we've got a sliver to work on.  From this point until we
	 * set to inflight, the slvr_lentry should be disjointed.
	 */
	s->slvr_flags |= SLVR_CRCING;

	/* 'completed writes' signifier can be reset, the upcoming
	 *    instantiation of slvr_do_crc() will cover those writes.
	 */
	s->slvr_compwrts = 0;
	s->slvr_ncrc++;
	SLVR_ULOCK(s);

	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));

	/* Be paranoid, ensure the sliver is not queued anywhere. */
	SLVR_LOCK(s);
	psc_assert(psclist_disjoint(&s->slvr_lentry));

	if ((s->slvr_flags & SLVR_CRCDIRTY || s->slvr_compwrts) &&
	    !s->slvr_pndgwrts) {
		/* The slvr will be CRC'd again due to pending write. */
		SLVR_ULOCK(s);
		DEBUG_SLVR(PLL_INFO, s, "replace onto crcSlvrs");
		lc_addtail(&crcqSlvrs, s);
		s->slvr_flags &= ~SLVR_CRCING;
		goto start;
	}

	/* Copy the accumulator to the tmp variable. */
	crc = s->slvr_crc;
	PSC_CRC64_FIN(&crc);

	/* Put the slvr back to the LRU so it may have its slab reaped. */
	bii = slvr_2_bii(s);
	b = bii_2_bmap(bii);
	psc_atomic32_dec(&bii->bii_crcdrty_slvrs);
	s->slvr_dirty_cnt--;

	slvr_num = s->slvr_num;

	SLVR_ULOCK(s);

	LIST_CACHE_LOCK(&bcr_hold);
	BII_LOCK(bii);
	bcr = bii->bii_bcr;

	if (bcr) {
		uint32_t i, found;

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
			BMAP_SETATTR(b, BMAP_IOD_BCRSCHED);
			bcr_hold_add(bcr);
		}
		PFL_GETTIMESPEC(&bcr->bcr_age);
	}

	BII_ULOCK(bii);
	LIST_CACHE_ULOCK(&bcr_hold);

	SLVR_LOCK(s);

	s->slvr_flags &= ~SLVR_CRCING;
	DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU (ndirty=%u)",
	    psc_atomic32_read(&bii->bii_crcdrty_slvrs));

	s->slvr_flags |= SLVR_LRU;
	slvr_lru_tryunpin_locked(s);
	lc_addqueue(&lruSlvrs, s);

	SLVR_ULOCK(s);

	slvr_worker_push_crcups();
}

void
slislvrthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run())
		slvr_worker_int();
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
