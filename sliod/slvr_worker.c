/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "bmap_iod.h"
#include "fid.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

static struct biod_infl_crcs	 binflCrcs;
struct pscrpc_nbreqset		*slvrNbReqSet;

/*
 * Send an RPC containing CRC updates for slivers to the metadata server.
 *   To avoid potential woes caused by out-of-order deliveries, we should
 *   allow at most one inflight CRC update RPC at any time.  Note that this
 *   does not prevent us from having multiple threads to do the CRC
 *   calculation.
 */
__static int
slvr_worker_crcup_genrq(const struct psc_dynarray *bcrs)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct biod_crcup_ref *bcr;
	struct pscrpc_request *rq;
	struct iovec *iovs;
	size_t len;
	uint32_t i;
	int rc;

	rc = sli_rmi_getimp(&csvc);
	if (rc)
		return (rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return (rc);
	}

	PSC_CRC64_INIT(&mq->crc);

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	rq->rq_async_args.pointer_arg[0] = (void *)bcrs;
	rq->rq_async_args.pointer_arg[1] = csvc;

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		rc = iod_inode_getinfo(&bcr->bcr_crcup.fg,
		    &bcr->bcr_crcup.fsize, &bcr->bcr_crcup.nblks,
		    &bcr->bcr_crcup.utimgen);
		/* Bail for now if we can't stat() our file objects.
		 */
		psc_assert(!rc);

		DEBUG_BCR(PLL_NOTIFY, bcr, "bcrs pos=%d fsz=%"PRId64, i,
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

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	psc_assert(pscrpc_nbreqset_add(slvrNbReqSet, rq) == 0);

	return (rc);
}

__static void
slvr_worker_push_crcups(void)
{
	int			 i;
	int			 rc;
	struct timespec		 now;
	struct biod_crcup_ref	*bcr, *tmp;
	struct psc_dynarray	*bcrs;
	static atomic_t busy = ATOMIC_INIT(0);

	if (atomic_xchg(&busy, 1))
		return;

	pscrpc_nbreqset_reap(slvrNbReqSet);
	/*
	 * Check if an earlier CRC update RPC, if any, has finished.  If one
	 * is still inflight, we won't be able to initiate a new one.
	 */
	spinlock(&binflCrcs.binfcrcs_lock);
	bcrs = PSCALLOC(sizeof(struct psc_dynarray));
	/* Leave scheduled bcr's on the list so that in case of a failure
	 *   ordering will be maintained.
	 */
	PLL_FOREACH(bcr, &binflCrcs.binfcrcs_ready) {
		psc_assert(bcr->bcr_crcup.nups > 0);

		if (bcr->bcr_flags & BCR_SCHEDULED)
			continue;

		if (BIOD_TRYLOCK(bcr->bcr_biodi)) {
			if (bcr_2_bmap(bcr)->bcm_flags & BMAP_IOD_INFLIGHT) {
				DEBUG_BCR(PLL_FATAL, bcr, "tried to schedule "
					  "multiple bcr's xid=%"PRIu64,
					  bcr->bcr_biodi->biod_bcr_xid_last);
			} else {
				bcr_2_bmap(bcr)->bcm_flags |= BMAP_IOD_INFLIGHT;
				BIOD_ULOCK(bcr->bcr_biodi);
			}
		} else
			/* Don't deadlock trying for the biodi lock.
			 */
			continue;

		psc_dynarray_add(bcrs, bcr);
		bcr->bcr_flags |= BCR_SCHEDULED;

		DEBUG_BCR(PLL_INFO, bcr, "scheduled nbcrs=%d total_bcrs=%d",
			  psc_dynarray_len(bcrs),
			  atomic_read(&binflCrcs.binfcrcs_nbcrs));

		if (psc_dynarray_len(bcrs) == MAX_BMAP_NCRC_UPDATES)
			break;
	}

	PFL_GETTIMESPEC(&now);
	/* Now scan for old bcr's hanging about.
	 */
	PLL_FOREACH_SAFE(bcr, tmp, &binflCrcs.binfcrcs_hold) {
		/* Use trylock avoid deadlock.
		 */
		if (!BIOD_TRYLOCK(bcr->bcr_biodi))
			continue;

		else if (now.tv_sec <
			 (bcr->bcr_age.tv_sec + BIOD_CRCUP_MAX_AGE)) {
			BIOD_ULOCK(bcr->bcr_biodi);
			continue;
		}

		bcr_hold_2_ready(&binflCrcs, bcr);
		DEBUG_BCR(PLL_INFO, bcr, "old, moved to ready");
		BIOD_ULOCK(bcr->bcr_biodi);

	}
	freelock(&binflCrcs.binfcrcs_lock);

	if (!psc_dynarray_len(bcrs))
		PSCFREE(bcrs);
	else {
		rc = slvr_worker_crcup_genrq(bcrs);
		/*
		 * If we fail to send an RPC, we must leave the reference
		 *   in the tree for future attempt.  Otherwise, the callback
		 *   function (i.e., slvr_nbreqset_cb()) should remove them
		 *   from the tree.
		 */
		if (rc) {
			spinlock(&binflCrcs.binfcrcs_lock);
			for (i = 0; i < psc_dynarray_len(bcrs); i++) {
				bcr = psc_dynarray_getpos(bcrs, i);
				bcr->bcr_flags &= ~(BCR_SCHEDULED);
				DEBUG_BCR(PLL_INFO, bcr,
					  "unsetting BCR_SCHEDULED");
			}
			freelock(&binflCrcs.binfcrcs_lock);
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
	int			 i;
	struct psc_dynarray	*a;
	struct srm_bmap_crcwrt_rep *mp;
	struct biod_crcup_ref	*bcr;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info    *biod;

	a = args->pointer_arg[0];
	csvc = args->pointer_arg[1];
	psc_assert(a);

	sli_rpc_mds_unpack_bminseq(rq, PSCRPC_MSG_REPLY);

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		biod = bcr->bcr_biodi;

		DEBUG_BCR(((rq->rq_status || !mp || mp->rc) ?
			   PLL_ERROR : PLL_INFO),
			  bcr, "rq_status=%d rc=%d%s", rq->rq_status,
			  mp ? mp->rc : -4096, mp ? "" : " (unknown, no buf)");

		psc_assert(bii_2_bmap(biod)->bcm_flags &
		    (BMAP_IOD_INFLIGHT|BMAP_IOD_BCRSCHED));

		if (rq->rq_status) {
			spinlock(&binflCrcs.binfcrcs_lock);
			bcr->bcr_flags &= ~BCR_SCHEDULED;
			freelock(&binflCrcs.binfcrcs_lock);
			/* Unset the inflight bit on the biodi since
			 *   bcr_xid_last_bump() will not be called.
			 */
			BIOD_LOCK(biod);
			BMAP_CLEARATTR(bii_2_bmap(biod), BMAP_IOD_INFLIGHT);
			bcr_xid_check(bcr);
			BIOD_ULOCK(biod);

			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");

		} else
			bcr_finalize(&binflCrcs, bcr);

	}
	psc_dynarray_free(a);
	PSCFREE(a);

	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);
	return (1);
}

__static void
slvr_worker_int(void)
{
	struct slvr_ref		*s;
	struct timespec		 now;
	struct biod_crcup_ref	*bcr=NULL;
	uint64_t		 crc;

 start:
	PFL_GETTIMESPEC(&now);
	now.tv_sec += BIOD_CRCUP_MAX_AGE;
	if (!(s = lc_gettimed(&crcqSlvrs, &now))) {
		/* Nothing available, try to push any existing
		 *  crc updates.
		 */
		slvr_worker_push_crcups();
		return;
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
	psc_assert(!(s->slvr_flags & SLVR_CRCING));
	psc_assert(!(s->slvr_flags & SLVR_LRU));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	/* Try our best to determine whether or we should hold off
	 *   the crc operation, strive to only crc slivers which have
	 *   no pending writes.  This section directly below may race
	 *   with slvr_wio_done().
	 */
	if (s->slvr_pndgwrts > 0) {
		s->slvr_flags |= SLVR_LRU;
		lc_addqueue(&lruSlvrs, s);

		DEBUG_SLVR(PLL_INFO, s, "descheduled due to pndg writes");

		SLVR_ULOCK(s);
		goto start;
	}
	/* Ok, we've got a sliver to work on.
	 *   From this point until we set to inflight, the slvr_lentry
	 *   should be disjointed.
	 */
	s->slvr_flags |= SLVR_CRCING;
	/* 'completed writes' signifier can be reset, the upcoming
	 *    instantiation of slvr_do_crc() will cover those writes.
	 */
	s->slvr_compwrts = 0;
	SLVR_ULOCK(s);
	//XXX perhaps when we go to a lighter checksum we can hold the
	// the lock for the duration?
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));

	/* Lock the biodi first so we don't deadlock with slvr_lookup.
	 */
	BIOD_LOCK(slvr_2_biod(s));
	SLVR_LOCK(s);
	/* Be paraniod, ensure the sliver is not queued anywhere.
	 */
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(s->slvr_flags & SLVR_CRCING);
	s->slvr_flags &= ~SLVR_CRCING;
	/* Copy the accumulator to the tmp variable.
	 */
	crc = s->slvr_crc;
	PSC_CRC64_FIN(&crc);

	if ((s->slvr_flags & SLVR_CRCDIRTY || s->slvr_compwrts) &&
	    !s->slvr_pndgwrts) {
		DEBUG_SLVR(PLL_INFO, s, "replace onto crcSlvrs");
		lc_addqueue(&crcqSlvrs, s);

	} else {
		/* Put the slvr back to the LRU so it may have its slab
		 *   reaped.
		 */
		//XXX lock sliver
		slvr_2_biod(s)->biod_crcdrty_slvrs--;
		DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU (ndirty=%u)",
			   slvr_2_biod(s)->biod_crcdrty_slvrs);
		s->slvr_flags |= SLVR_LRU;
		(int)slvr_lru_tryunpin_locked(s);
		lc_addqueue(&lruSlvrs, s);
	}
	/* Note that we're covered by the slvr_lock, which is actually
	 *   a lock on the whole biodi.
	 */
	bcr = slvr_2_biod(s)->biod_bcr;
	if (bcr) {
		uint32_t i, found;

		psc_assert(bcr->bcr_crcup.blkno == slvr_2_bmap(s)->bcm_bmapno);
		psc_assert(bcr->bcr_crcup.fg.fg_fid ==
			   slvr_2_bmap(s)->bcm_fcmh->fcmh_fg.fg_fid);
		psc_assert(bcr->bcr_crcup.nups < MAX_BMAP_INODE_PAIRS);
		psc_assert(slvr_2_bmap(s)->bcm_flags & BMAP_IOD_BCRSCHED);

		/* If we already have a slot for our slvr_num then
		 *   reuse it.
		 */
		for (i=0, found=0; i < bcr->bcr_crcup.nups; i++) {
			if (bcr->bcr_crcup.crcs[i].slot == s->slvr_num) {
				found = 1;
				break;
			}
		}

		bcr->bcr_crcup.crcs[i].crc = crc;
		bcr->bcr_crcup.crcs[i].slot = s->slvr_num;
		if (!found)
			bcr->bcr_crcup.nups++;

		DEBUG_BCR(PLL_NOTIFY, bcr, "add to existing bcr slot=%d "
			  "nups=%d", i, bcr->bcr_crcup.nups);

		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS) {
			if (pll_nitems(&slvr_2_biod(s)->biod_bklog_bcrs))
				/* This is a backlogged bcr, cap it and
				 *   move on.
				 */
				bcr->bcr_biodi->biod_bcr = NULL;
			else
				/* The bcr is full, push it out now.
				 */
				bcr_hold_2_ready(&binflCrcs, bcr);
		}
	} else {
		bmap_op_start_type(slvr_2_bmap(s), BMAP_OPCNT_BCRSCHED);

		/* Freed by bcr_ready_remove() */
		slvr_2_biod(s)->biod_bcr = bcr =
			PSCALLOC(sizeof(struct biod_crcup_ref) +
				 (sizeof(struct srt_bmap_crcwire) *
				  MAX_BMAP_INODE_PAIRS));

		INIT_PSC_LISTENTRY(&bcr->bcr_lentry);

		bcr->bcr_biodi = slvr_2_biod(s);
		bcr->bcr_xid = slvr_2_biod(s)->biod_bcr_xid;
		slvr_2_biod(s)->biod_bcr_xid++;

		COPYFG(&bcr->bcr_crcup.fg,
		    &slvr_2_bmap(s)->bcm_fcmh->fcmh_fg);

		bcr->bcr_crcup.blkno = slvr_2_bmap(s)->bcm_bmapno;
		bcr->bcr_crcup.crcs[0].crc = crc;
		bcr->bcr_crcup.crcs[0].slot = s->slvr_num;
		bcr->bcr_crcup.nups = 1;

		DEBUG_BCR(PLL_NOTIFY, bcr,
			  "newly added (bcr_bklog=%d) (sched=%d)",
			  pll_nitems(&slvr_2_biod(s)->biod_bklog_bcrs),
			  (slvr_2_bmap(s)->bcm_flags & BMAP_IOD_BCRSCHED));

		if (slvr_2_bmap(s)->bcm_flags & BMAP_IOD_BCRSCHED)
			/* The bklog may be empty but a pending bcr may be
			 *    present on the ready list.
			 */
			pll_addtail(&slvr_2_biod(s)->biod_bklog_bcrs, bcr);
		else {
			BMAP_SETATTR(slvr_2_bmap(s), BMAP_IOD_BCRSCHED);
			bcr_hold_add(&binflCrcs, bcr);
		}
	}
	/* Either set the initial age of a new sliver or extend the age
	 *   of an existing one. Note that if it gets full, it will be
	 *   sent out immediately regardless of its age.
	 */
	PFL_GETTIMESPEC(&bcr->bcr_age);
	/* The sliver may go away now.
	 */
	BIOD_ULOCK(slvr_2_biod(s));
	SLVR_ULOCK(s);
	slvr_worker_push_crcups();
}

void
slvr_worker(__unusedx struct psc_thread *thr)
{
	while (pscthr_run())
		slvr_worker_int();
}

void
slvr_worker_init(void)
{
	int i;

	INIT_SPINLOCK(&binflCrcs.binfcrcs_lock);
	pll_init(&binflCrcs.binfcrcs_ready, struct biod_crcup_ref,
		 bcr_lentry, &binflCrcs.binfcrcs_lock);
	pll_init(&binflCrcs.binfcrcs_hold, struct biod_crcup_ref,
		 bcr_lentry, &binflCrcs.binfcrcs_lock);

	slvrNbReqSet = pscrpc_nbreqset_init(NULL, slvr_nbreqset_cb);
	if (!slvrNbReqSet)
		psc_fatalx("nbreqset_init() failed");

	for (i = 0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, 0, slvr_worker, NULL, 0,
			    "slislvrthr%d", i);
}
