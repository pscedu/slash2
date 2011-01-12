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

#include <sys/time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "bmap_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

struct bmap_iod_minseq	 bimSeq;
static struct timespec	 bim_timeo = { BIM_MINAGE, 0 };

struct psc_listcache	 bmapReapQ;
struct psc_listcache	 bmapRlsQ;

void
bim_init(void)
{
	INIT_SPINLOCK(&bimSeq.bim_lock);
	psc_waitq_init(&bimSeq.bim_waitq);
	bimSeq.bim_minseq = 0;
}

void
bim_updateseq(uint64_t seq)
{
	spinlock(&bimSeq.bim_lock);
	if (bimSeq.bim_minseq == BMAPSEQ_ANY ||
	    (seq > bimSeq.bim_minseq && seq != BMAPSEQ_ANY)) {
		bimSeq.bim_minseq = seq;
		PFL_GETTIMESPEC(&bimSeq.bim_age);
	} else
		seq = BMAPSEQ_ANY;
	freelock(&bimSeq.bim_lock);

	if (seq == BMAPSEQ_ANY)
		psc_warnx("seq %"PRId64" is invalid (bim_minseq=%"PRId64")",
			  seq, bimSeq.bim_minseq);
}

uint64_t
bim_getcurseq(void)
{
	struct slashrpc_cservice *csvc;
	struct timespec ctime;
	uint64_t seq;

 retry:
	PFL_GETTIMESPEC(&ctime);
	timespecsub(&ctime, &bim_timeo, &ctime);

	spinlock(&bimSeq.bim_lock);
	if (bimSeq.bim_flags & BIM_RETRIEVE_SEQ) {
		psc_waitq_wait(&bimSeq.bim_waitq, &bimSeq.bim_lock);
		goto retry;
	}

	if (timespeccmp(&ctime, &bimSeq.bim_age, >) ||
	    bimSeq.bim_minseq == BMAPSEQ_ANY) {
		struct pscrpc_request *req;
		struct srm_getbmapminseq_req *mq;
		struct srm_generic_rep *mp;
		int rc;

		bimSeq.bim_flags |= BIM_RETRIEVE_SEQ;
		freelock(&bimSeq.bim_lock);

		rc = sli_rmi_getimp(&csvc);
		if (rc)
			psc_fatalx("mds");
		rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMI_VERSION,
		    SRMT_GETBMAPMINSEQ, req, mq, mp);
		if (rc)
			psc_fatalx("mds");
		rc = SL_RSX_WAITREP(req, mp);
		if (rc)
			psc_fatalx("mds");

		sl_csvc_decref(csvc);

		bim_updateseq(mp->data);

		spinlock(&bimSeq.bim_lock);
		bimSeq.bim_flags &= ~BIM_RETRIEVE_SEQ;
		psc_waitq_wakeall(&bimSeq.bim_waitq);
		if (rc)
			goto retry;
	}

	seq = bimSeq.bim_minseq;
	freelock(&bimSeq.bim_lock);

	return (seq);
}

void
bcr_hold_2_ready(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	int locked;

	BIOD_LOCK_ENSURE(bcr->bcr_biodi);

	locked = reqlock(&inf->binfcrcs_lock);
	pll_remove(&inf->binfcrcs_hold, bcr);
	pll_addtail(&inf->binfcrcs_ready, bcr);
	ureqlock(&inf->binfcrcs_lock, locked);

	bcr->bcr_biodi->biod_bcr = NULL;
}


void
bcr_hold_add(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	psc_assert(psclist_disjoint(&bcr->bcr_lentry));
	pll_addtail(&inf->binfcrcs_hold, bcr);
	atomic_inc(&inf->binfcrcs_nbcrs);
}

void
bcr_ready_add(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	psc_assert(psclist_disjoint(&bcr->bcr_lentry));
	pll_addtail(&inf->binfcrcs_ready, bcr);
	atomic_inc(&inf->binfcrcs_nbcrs);
}

void
bcr_hold_requeue(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	int locked;

	locked = reqlock(&inf->binfcrcs_lock);
	pll_remove(&inf->binfcrcs_hold, bcr);
	pll_addtail(&inf->binfcrcs_hold, bcr);
	ureqlock(&inf->binfcrcs_lock, locked);
}

void
bcr_xid_check(struct biod_crcup_ref *bcr)
{
	int locked;

	locked = BIOD_RLOCK(bcr->bcr_biodi);
	psc_assert(bcr->bcr_xid < bcr->bcr_biodi->biod_bcr_xid);
	psc_assert(bcr->bcr_xid == bcr->bcr_biodi->biod_bcr_xid_last);
	/* bcr_xid_check() must be called prior to bumping xid_last.
	 */
	psc_assert(bcr->bcr_biodi->biod_bcr_xid >
		   bcr->bcr_biodi->biod_bcr_xid_last);

	BIOD_URLOCK(bcr->bcr_biodi, locked);
}

static void
bcr_xid_last_bump(struct biod_crcup_ref *bcr)
{
	bcr_xid_check(bcr);
	bcr->bcr_biodi->biod_bcr_xid_last++;
	bcr_2_bmap(bcr)->bcm_flags &= ~BMAP_IOD_INFLIGHT;
}

void
bcr_ready_remove(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	spinlock(&inf->binfcrcs_lock);
	pll_remove(&inf->binfcrcs_ready, bcr);
	atomic_dec(&inf->binfcrcs_nbcrs);
	freelock(&inf->binfcrcs_lock);

	BIOD_LOCK(bcr->bcr_biodi);
	psc_assert(bcr->bcr_flags & BCR_SCHEDULED);
	bcr_xid_last_bump(bcr);
	BIOD_ULOCK(bcr->bcr_biodi);

	bmap_op_done_type(bcr_2_bmap(bcr), BMAP_OPCNT_BCRSCHED);
	PSCFREE(bcr);
}

void
bcr_finalize(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	struct bmap_iod_info *biod = bcr->bcr_biodi;

	DEBUG_BCR(PLL_INFO, bcr, "finalize");

	BIOD_LOCK(biod);
	psc_assert(bii_2_bmap(biod)->bcm_flags & BMAP_IOD_BCRSCHED);
	/* biod->biod_bcr_xid_last is bumped in bcr_ready_remove().
	 *    bcr_ready_remove() may release the bmap so it must be
	 *    issued at the end of this call.
	 */
	if (biod->biod_bcr_xid == biod->biod_bcr_xid_last + 1) {
		/* No outstanding bcr's.
		 */
		psc_assert(pll_empty(&biod->biod_bklog_bcrs));
		psc_assert(!biod->biod_bcr);
		bii_2_bmap(biod)->bcm_flags &= ~BMAP_IOD_BCRSCHED;

		DEBUG_BMAP(PLL_INFO, bii_2_bmap(biod),
		    "descheduling drtyslvrs=%u",
		    biod->biod_crcdrty_slvrs);

		biod_rlssched_locked(biod);
		BIOD_ULOCK(biod);
	} else {
		struct biod_crcup_ref *tmp;

		tmp = pll_gethead(&biod->biod_bklog_bcrs);
		if (tmp) {
			DEBUG_BCR(PLL_INFO, tmp, "backlogged bcr, nblklog=%d",
				  pll_nitems(&biod->biod_bklog_bcrs));

			if (pll_empty(&biod->biod_bklog_bcrs)) {
				/*
				 * I am the only one on the backlog list of
				 * the bmap.  A NULL biod_bcr is ok as long
				 * as the bcr has been filled.
				 */
				psc_assert(biod->biod_bcr == tmp ||
					   !biod->biod_bcr);
				if (tmp->bcr_crcup.nups ==
				    MAX_BMAP_INODE_PAIRS) {
					biod->biod_bcr = NULL;
					bcr_ready_add(inf, tmp);
				} else
					bcr_hold_add(inf, tmp);

			} else {
				/* Only the tail of the bklog may be the
				 *    active bcr.
				 */
				psc_assert(biod->biod_bcr != tmp);
				bcr_ready_add(inf, tmp);
			}
		}
		BIOD_ULOCK(biod);
	}
	bcr_ready_remove(inf, bcr);
}

#if 0
int
bcr_cmp(const void *x, const void *y)
{
	const struct biod_crcup_ref *a = x, *b = y;

	return (CMP(a->bcr_xid, b->bcr_xid));
}
#endif

static __inline void
bmap_2_bid_sliod(const struct bmapc_memb *b, struct srm_bmap_id *bid)
{
	const struct bmap_iod_info *bmdsi = (const void *)(b + 1);

	bid->fid = fcmh_2_fid(b->bcm_fcmh);
	bid->bmapno = b->bcm_bmapno;
	bid->seq = bmdsi->biod_rls_seqkey[0];
	bid->key = bmdsi->biod_rls_seqkey[1];
	bid->cli_nid = bmdsi->biod_rls_cnp.nid;
	bid->cli_pid = bmdsi->biod_rls_cnp.pid;
}

void
biod_rlssched_locked(struct bmap_iod_info *biod)
{
	BIOD_LOCK_ENSURE(biod);

	if (bii_2_bmap(biod)->bcm_flags & BMAP_IOD_RLSSCHED)
		/* Don't test for list membership, the bmaprlsthr may
		 *   have already removed the biod in preparation for
		 *   release.
		 */
		psc_assert(bii_2_bmap(biod)->bcm_flags & BMAP_IOD_RLSSEQ);

	else {
		psc_assert(psclist_disjoint(&biod->biod_lentry));

		if (!biod->biod_crcdrty_slvrs &&
		    (bii_2_flags(biod) & BMAP_IOD_RLSSEQ) &&
		    (biod->biod_bcr_xid == biod->biod_bcr_xid_last)) {
			bmap_op_start_type(bii_2_bmap(biod),
			    BMAP_OPCNT_RLSSCHED);
			BMAP_SETATTR(bii_2_bmap(biod), BMAP_IOD_RLSSCHED);
			lc_addtail(&bmapRlsQ, biod);
		}
	}
}

void
sliod_bmaprlsthr_main(__unusedx struct psc_thread *thr)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct srm_bmap_release_req *brr, *mq;
	struct srm_bmap_release_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info *biod;
	struct bmapc_memb *b;
	int i, rc;

	brr = PSCALLOC(sizeof(struct srm_bmap_release_req));

	while (pscthr_run()) {
		i = 0;

		biod = lc_getwait(&bmapRlsQ);
		if (lc_sz(&bmapRlsQ) < MAX_BMAP_RELEASE)
			/* Try to coalesce, wait for others.
			 *   yes, this is a bit ugly.
			 */
			sleep(SLIOD_BMAP_RLS_WAIT_SECS);

		do {
			b = bii_2_bmap(biod);
			/* Account for the rls ref.
			 */
			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

			DEBUG_BMAP(PLL_INFO, b, "ndrty=%u rlsseq=%"PRId64
			    " rlskey=%"PRId64" xid=%"PRIu64" xid_last=%"PRIu64,
			    biod->biod_crcdrty_slvrs, biod->biod_rls_seqkey[0],
			    biod->biod_rls_seqkey[1], biod->biod_bcr_xid,
			    biod->biod_bcr_xid_last);

			BIOD_LOCK(biod);
			psc_assert(bii_2_flags(biod) & BMAP_IOD_RLSSEQ);
			psc_assert(bii_2_flags(biod) & BMAP_IOD_RLSSCHED);

			if (biod->biod_crcdrty_slvrs ||
			    (biod->biod_bcr_xid != biod->biod_bcr_xid_last)) {
				/* Temporarily remove unreapable biod's
				 */
				psc_dynarray_add(&a, biod);
				BIOD_ULOCK(biod);
				continue;
			}

			BMAP_CLEARATTR(bii_2_bmap(biod), 
			       BMAP_IOD_RLSSEQ | BMAP_IOD_RLSSCHED);

			bmap_2_bid_sliod(b, &brr->bmaps[i++]);
			BIOD_ULOCK(biod);

			bmap_op_done_type(b, BMAP_OPCNT_RLSSCHED);

		} while ((i < MAX_BMAP_RELEASE) &&
			 (biod = lc_getnb(&bmapRlsQ)));

		if (!i)
			goto end;

		brr->nbmaps = i;

		/* The system can tolerate the loss of these messages so
		 *   errors here should not be considered fatal.
		 */
		rc = sli_rmi_getimp(&csvc);
		if (rc) {
			psc_errorx("Failed to get MDS import");
			continue;
		}

		rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
				   SRMT_RELEASEBMAP, rq, mq, mp);
		if (rc) {
			psc_errorx("Failed to generate new RPC req");
			sl_csvc_decref(csvc);
			continue;
		}

		memcpy(mq, brr, sizeof(*mq));
		rc = SL_RSX_WAITREP(rq, mp);
		if (rc)
			psc_errorx("RELEASEBMAP req failed");

		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
 end:
		/* put any unreapable biods back to the list */
		DYNARRAY_FOREACH(biod, i, &a)
			lc_addtail(&bmapRlsQ, biod);

		if (psc_dynarray_len(&a))
			sleep(SLIOD_BMAP_RLS_WAIT_SECS);

		psc_dynarray_free(&a);
	}
}

void
sliod_bmaprlsthr_spawn(void)
{
	lc_reginit(&bmapRlsQ, struct bmap_iod_info,
		   biod_lentry, "bmapRlsQ");

	pscthr_init(SLITHRT_BMAPRLS, 0, sliod_bmaprlsthr_main,
		    NULL, 0, "slibmaprlsthr");
}

void
iod_bmap_init(struct bmapc_memb *b)
{
	struct bmap_iod_info *biod;

	biod = bmap_2_bii(b);
	biod->biod_bcr_xid = biod->biod_bcr_xid_last = 0;
	INIT_PSC_LISTENTRY(&biod->biod_lentry);
	SPLAY_INIT(&biod->biod_slvrs);
	pll_init(&biod->biod_bklog_bcrs, struct biod_crcup_ref,
	    bcr_lentry, NULL);

	PFL_GETTIMESPEC(&biod->biod_age);
	/* XXX At some point we'll want to let bmaps hang around in the
	 *   cache to prevent extra reads and crc table fetches.
	 */
	//bmap_op_start_type(b, BMAP_OPCNT_REAPER);
	//lc_addtail(b, &bmapReapQ);
}

void
iod_bmap_finalcleanup(struct bmapc_memb *b)
{
	struct bmap_iod_info *biod;

	biod = bmap_2_bii(b);
	psc_assert(SPLAY_EMPTY(&biod->biod_slvrs));
	psc_assert(psclist_disjoint(&biod->biod_lentry));
}

/**
 * iod_bmap_retrieve - Load the relevant bmap information from the metadata
 *   server.  In the case of the ION the bmap sections of interest are the
 *   CRC table and the CRC states bitmap.  For now we only load this
 *   information on read.
 * @b: bmap to load.
 * @rw: the bmap access mode.
 * Return zero on success or errno code on failure (likely an RPC problem).
 */
int
iod_bmap_retrieve(struct bmapc_memb *b, enum rw rw)
{
	struct pscrpc_request *rq = NULL;
	struct srm_getbmap_full_req *mq;
	struct srm_getbmap_full_rep *mp;
	struct slashrpc_cservice *csvc;
	int rc;

	if (rw != SL_READ)
		return (0);

	rc = sli_rmi_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_GETBMAPCRCS, rq, mq, mp);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "could not create request (%d)", rc);
		goto out;
	}

	mq->rw = rw;
	mq->bmapno = b->bcm_bmapno;
	memcpy(&mq->fg, &b->bcm_fcmh->fcmh_fg, sizeof(mq->fg));
	//memcpy(&mq->sbdb, sbdb, sizeof(*sbdb));

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "req failed (%d)", rc);
		goto out;
	}

	BMAP_LOCK(b); /* equivalent to BIOD_LOCK() */
	memcpy(bmap_2_ondisk(b), &mp->bod, sizeof(mp->bod));

	/* Need to copy any of our slvr CRCs into the table. */
	if (!SPLAY_EMPTY(bmap_2_biodi_slvrs(b))) {
		struct slvr_ref *s;

		SPLAY_FOREACH(s, biod_slvrtree, bmap_2_biodi_slvrs(b)) {
			/* Only replace the CRC if DATARDY is true (meaning that
			 *   all init operations have be done) and that the
			 *   CRC is clean (meaning that the CRC reflects the slab
			 *   contents.
			 */
			if (!(s->slvr_flags & SLVR_DATARDY))
				continue;

			slvr_2_crc(s) = s->slvr_crc;
			slvr_2_crcbits(s) |= BMAP_SLVR_DATA;

			if (s->slvr_flags & SLVR_CRCDIRTY)
				slvr_2_crcbits(s) |= BMAP_SLVR_CRC;
			else
				slvr_2_crcbits(s) |= BMAP_SLVR_CRCDIRTY;
		}
	}
	BMAP_ULOCK(b);
 out:
	/* Unblock threads no matter what.
	 *  XXX need some way to denote that a CRCGET RPC failed?
	 */
	if (rc)
		DEBUG_BMAP(PLL_ERROR, b, "rc=%d", rc);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags(&flags, &seq);
	PFL_PRFLAG(BMAP_IOD_INFLIGHT, &flags, &seq);
	PFL_PRFLAG(BMAP_IOD_RLSSEQ, &flags, &seq);
	PFL_PRFLAG(BMAP_IOD_BCRSCHED, &flags, &seq);
	PFL_PRFLAG(BMAP_IOD_RLSSCHED, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif

struct bmap_ops bmap_ops = {
	iod_bmap_init,
	iod_bmap_retrieve,
	iod_bmap_retrieve,
	iod_bmap_finalcleanup
};
