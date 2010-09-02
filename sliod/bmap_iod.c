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

struct bmap_iod_minseq	 bimSeq;
static struct timespec	 bim_timeo = { BIM_MINAGE, 0 };

struct psc_listcache	 bmapReapQ;
struct psc_listcache	 bmapRlsQ;

void
bim_init(void)
{
	LOCK_INIT(&bimSeq.bim_lock);
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
		clock_gettime(CLOCK_REALTIME, &bimSeq.bim_age);
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
	clock_gettime(CLOCK_REALTIME, &ctime);
	timespecsub(&ctime, &bim_timeo, &ctime);

	spinlock(&bimSeq.bim_lock);
	if (bimSeq.bim_flags & BIM_RETRIEVE_SEQ) {
		psc_waitq_wait(&bimSeq.bim_waitq, &bimSeq.bim_lock);
		goto retry;
	}

	if (timespeccmp(&ctime, &bimSeq.bim_age, >) ||
	    bimSeq.bim_minseq == BMAPSEQ_ANY) {
		struct pscrpc_request *req;
		struct srm_bmap_minseq_get *mq;
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

	LOCK_ENSURE(&bcr->bcr_biodi->biod_lock);

	locked = reqlock(&inf->binfcrcs_lock);
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
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
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	pll_remove(&inf->binfcrcs_hold, bcr);
	pll_addtail(&inf->binfcrcs_hold, bcr);
	ureqlock(&inf->binfcrcs_lock, locked);
}

void
bcr_xid_check(struct biod_crcup_ref *bcr)
{
	int locked;

	locked = reqlock(&bcr->bcr_biodi->biod_lock);
	psc_assert(bcr->bcr_xid < bcr->bcr_biodi->biod_bcr_xid);
	psc_assert(bcr->bcr_xid == bcr->bcr_biodi->biod_bcr_xid_last);
	/* bcr_xid_check() must be called prior to bumping xid_last.
	 */
	psc_assert(bcr->bcr_biodi->biod_bcr_xid >
		   bcr->bcr_biodi->biod_bcr_xid_last);

	ureqlock(&bcr->bcr_biodi->biod_lock, locked);
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
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	pll_remove(&inf->binfcrcs_ready, bcr);
	atomic_dec(&inf->binfcrcs_nbcrs);
	freelock(&inf->binfcrcs_lock);

	spinlock(&bcr->bcr_biodi->biod_lock);
	psc_assert(bcr->bcr_flags & BCR_SCHEDULED);
	bcr_xid_last_bump(bcr);
	freelock(&bcr->bcr_biodi->biod_lock);

	bmap_op_done_type(bcr_2_bmap(bcr), BMAP_OPCNT_BCRSCHED);
	PSCFREE(bcr);
}

void
bcr_finalize(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	struct bmap_iod_info *biod = bcr->bcr_biodi;

	DEBUG_BCR(PLL_INFO, bcr, "finalize");

	spinlock(&biod->biod_lock);
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
		freelock(&biod->biod_lock);
	} else {
		struct biod_crcup_ref *tmp;

		tmp = pll_gethd(&biod->biod_bklog_bcrs);
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
		freelock(&biod->biod_lock);
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
	LOCK_ENSURE(&biod->biod_lock);

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
			bii_2_flags(biod) |= BMAP_IOD_RLSSCHED;
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

			spinlock(&biod->biod_lock);
			psc_assert(bii_2_flags(biod) & BMAP_IOD_RLSSEQ);
			psc_assert(bii_2_flags(biod) & BMAP_IOD_RLSSCHED);

			if (biod->biod_crcdrty_slvrs ||
			    (biod->biod_bcr_xid != biod->biod_bcr_xid_last)) {
				/* Temporarily remove unreapable biod's
				 */
				psc_dynarray_add(&a, biod);
				freelock(&biod->biod_lock);
				continue;
			}

			bii_2_flags(biod) &= ~(BMAP_IOD_RLSSEQ | BMAP_IOD_RLSSCHED);

			bmap_2_bid_sliod(b, &brr->bmaps[i++]);
			freelock(&biod->biod_lock);

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

		psc_dynarray_reset(&a);
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
