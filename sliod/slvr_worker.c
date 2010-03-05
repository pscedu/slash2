/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

struct biod_infl_crcs	 binflCrcs;
struct pscrpc_nbreqset	*slvrNbReqSet;

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
	struct biod_crcup_ref *bcr;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *req;
	struct pscrpc_bulk_desc *desc;
	struct iovec *iovs;
	size_t len;
	int rc;
	uint32_t i;

	rc = RSX_NEWREQ(sli_rmi_getimp(), SRMI_VERSION,
			SRMT_BMAPCRCWRT, req, mq, mp);
	if (rc) {
		return rc;
	}

	PSC_CRC64_INIT(&mq->crc);

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	req->rq_async_args.pointer_arg[0] = (void *)bcrs;

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		DEBUG_BCR(PLL_NOTIFY, bcr, "bcrs pos=%d", i);

		bcr_xid_check(bcr);

		bcr->bcr_crcup.fsize =
		    iod_inode_getsize(&bcr->bcr_crcup.fg);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = ((mq->ncrcs_per_update[i] *
					   sizeof(struct srm_bmap_crcwire)) +
					   sizeof(struct srm_bmap_crcup));

		psc_crc64_add(&mq->crc, iovs[i].iov_base, iovs[i].iov_len);
	}
	psc_assert(len <= LNET_MTU);

	PSC_CRC64_FIN(&mq->crc);

	rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
			     iovs, mq->ncrc_updates);
	PSCFREE(iovs);

	pscrpc_nbreqset_add(slvrNbReqSet, req);

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

		if (trylock(&bcr->bcr_biodi->biod_lock)) {
			if (bcr->bcr_biodi->biod_inflight) {
				DEBUG_BCR(PLL_INFO, bcr, "waiting for xid=%"PRIu64,
					  bcr->bcr_biodi->biod_bcr_xid_last);
				freelock(&bcr->bcr_biodi->biod_lock);
				continue;
			} else {
				bcr->bcr_biodi->biod_inflight = 1;
				freelock(&bcr->bcr_biodi->biod_lock);
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

	clock_gettime(CLOCK_REALTIME, &now);
	/* Now scan for old bcr's hanging about.
	 */
	PLL_FOREACH_SAFE(bcr, tmp, &binflCrcs.binfcrcs_hold) {
		/* Use trylock avoid deadlock.
		 */
		if (!trylock(&bcr->bcr_biodi->biod_lock))
			continue;

		else if (now.tv_sec <
			 (bcr->bcr_age.tv_sec + BIOD_CRCUP_MAX_AGE)) {
			freelock(&bcr->bcr_biodi->biod_lock);
			continue;
		}

		bcr_hold_2_ready(&binflCrcs, bcr);
		DEBUG_BCR(PLL_INFO, bcr, "old, moved to ready");

		freelock(&bcr->bcr_biodi->biod_lock);
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
slvr_nbreqset_cb(__unusedx struct pscrpc_request *req,
		 struct pscrpc_async_args *args)
{
	int			 i, err;
	struct psc_dynarray	*a;
	struct srm_generic_rep	*mp;
	struct biod_crcup_ref	*bcr;

	err = 0;
	a = args->pointer_arg[0];
	psc_assert(a);

	mp = psc_msg_buf(req->rq_repmsg, 0, sizeof(*mp));
	if (req->rq_status || mp->rc)
		err = 1;

	for (i=0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);

		DEBUG_BCR(PLL_INFO, bcr, "err=%d", err);
		if (err) {
			spinlock(&binflCrcs.binfcrcs_lock);
			bcr->bcr_flags &= ~BCR_SCHEDULED;
			freelock(&binflCrcs.binfcrcs_lock);
			/* Unset the inflight bit on the biodi.
			 */
			spinlock(&bcr->bcr_biodi->biod_lock);
			bcr_xid_check(bcr);
			bcr->bcr_biodi->biod_inflight = 0;
			freelock(&bcr->bcr_biodi->biod_lock);

			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");
		} else
			bcr_ready_remove(&binflCrcs, bcr);
	}
	psc_dynarray_free(a);
	PSCFREE(a);

	return (0);
}


__static void
slvr_worker_int(void)
{
	struct slvr_ref		*s;
	struct timespec		 now;
	struct biod_crcup_ref	*bcr=NULL;

 start:
	clock_gettime(CLOCK_REALTIME, &now);
	now.tv_sec += BIOD_CRCUP_MAX_AGE;
	if (!(s = lc_gettimed(&crcqSlvrs, &now))) {
		/* Nothing available, try to push any existing
		 *  crc updates.
		 */
		slvr_worker_push_crcups();
		return;
	}

	SLVR_LOCK(s);
	s->slvr_flags |= SLVR_CRCING;

	DEBUG_SLVR(PLL_INFO, s, "slvr_worker");
	/* Sliver assertions:
	 *  !LRU - ensure that slvr is in the right state.
	 *  CRCDIRTY - must have work to do.
	 *  PINNED - slab must not move from beneath us because the
	 *           contents must be crc'd.
	 */
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
	SLVR_ULOCK(s);
	//XXX perhaps when we go to a lighter checksum we can hold the
	// the lock for the duration
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));
	/* Note that this lock covers the slvr lock too.
	 */
	spinlock(&slvr_2_biod(s)->biod_lock);
	/* biodi_wire() will only be present if this bmap is in read
	 *   mode.
	 */
	if (slvr_2_biodi_wire(s)) {
		psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_DATA);
		psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_CRC);
	}
	/* Be paraniod, ensure the sliver is not queued anywhere.
	 */
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(s->slvr_flags & SLVR_CRCING);
	s->slvr_flags &= ~SLVR_CRCING;

	if (s->slvr_flags & SLVR_CRCDIRTY) {
		DEBUG_SLVR(PLL_INFO, s, "replace onto crcSlvrs");
		lc_addqueue(&crcqSlvrs, s);

	} else {
		/* Put the slvr back to the LRU so it may have its slab
		 *   reaped.
		 */
		DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU");
		s->slvr_flags |= SLVR_LRU;
		(int)slvr_lru_tryunpin_locked(s);
		lc_addqueue(&lruSlvrs, s);
	}
	/* Note that we're covered by the slvr_lock, which is actually
	 *   a lock on the whole biodi.
	 */
	bcr = slvr_2_biod(s)->biod_bcr;
	if (bcr) {
		psc_assert(bcr->bcr_crcup.blkno == slvr_2_bmap(s)->bcm_blkno);

		psc_assert(SAMEFG(&bcr->bcr_crcup.fg,
		    &slvr_2_bmap(s)->bcm_fcmh->fcmh_fg));

		psc_assert(bcr->bcr_crcup.nups < MAX_BMAP_NCRC_UPDATES);

		bcr->bcr_crcup.crcs[bcr->bcr_crcup.nups].crc = s->slvr_crc;
		bcr->bcr_crcup.crcs[bcr->bcr_crcup.nups].slot = s->slvr_num;
		bcr->bcr_crcup.nups++;

		DEBUG_BCR(PLL_NOTIFY, bcr, "add to existing bcr nups=%d",
			  bcr->bcr_crcup.nups);

		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
			bcr_hold_2_ready(&binflCrcs, bcr);
		else
			/* Put this guy at the end of the list.
			 */
			bcr_hold_requeue(&binflCrcs, bcr);

	} else {
		/* XXX not freed? */
		slvr_2_biod(s)->biod_bcr = bcr =
			PSCALLOC(sizeof(struct biod_crcup_ref) +
				 (sizeof(struct srm_bmap_crcwire) *
				  MAX_BMAP_INODE_PAIRS));

		bcr->bcr_biodi = slvr_2_biod(s);
		bcr->bcr_xid = slvr_2_biod(s)->biod_bcr_xid;
		slvr_2_biod(s)->biod_bcr_xid++;

		COPYFG(&bcr->bcr_crcup.fg,
		    &slvr_2_bmap(s)->bcm_fcmh->fcmh_fg);

		bcr->bcr_crcup.blkno = slvr_2_bmap(s)->bcm_blkno;
		bcr->bcr_crcup.crcs[0].crc = s->slvr_crc;
		bcr->bcr_crcup.crcs[0].slot = s->slvr_num;
		bcr->bcr_crcup.nups = 1;

		DEBUG_BCR(PLL_NOTIFY, bcr, "newly added");
		bcr_hold_add(&binflCrcs, bcr);
	}
	/*
	 * Either set the initial age of a new sliver or extend the age
	 *   of an existing one. Note that if it gets full, it will be
	 *   sent out immediately regardless of its age.
	 */
	clock_gettime(CLOCK_REALTIME, &bcr->bcr_age);
	/* The sliver may go away now.
	 */
	freelock(&slvr_2_biod(s)->biod_lock);
	slvr_worker_push_crcups();
}

__dead void *
slvr_worker(__unusedx void *arg)
{
	for (;;) {
		slvr_worker_int();
		sched_yield();
	}
}

void
slvr_worker_init(void)
{
	int i;

	LOCK_INIT(&binflCrcs.binfcrcs_lock);
	pll_init(&binflCrcs.binfcrcs_ready, struct biod_crcup_ref,
		 bcr_lentry, &binflCrcs.binfcrcs_lock);
	pll_init(&binflCrcs.binfcrcs_hold, struct biod_crcup_ref,
		 bcr_lentry, &binflCrcs.binfcrcs_lock);

	slvrNbReqSet = pscrpc_nbreqset_init(NULL, slvr_nbreqset_cb);
	if (!slvrNbReqSet)
		psc_fatalx("nbreqset_init() failed");

	for (i=0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, 0, slvr_worker, NULL, 0,
			    "slislvrthr%d", i);
}
