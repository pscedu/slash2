/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/time.h>

#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/ctlsvr.h"
#include "pfl/lock.h"
#include "pfl/log.h"

#include "bmap_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

struct bmap_iod_minseq	 bimSeq;
static struct timespec	 bim_timeo = { BIM_MINAGE, 0 };

struct psc_listcache	 bmapRlsQ;

struct psc_poolmaster    bmap_rls_poolmaster;
struct psc_poolmgr	*bmap_rls_pool;

void
bim_init(void)
{
	INIT_SPINLOCK(&bimSeq.bim_lock);
	psc_waitq_init(&bimSeq.bim_waitq);
	bimSeq.bim_minseq = 0;
}

int
bim_updateseq(uint64_t seq)
{
	int invalid = 0;

	spinlock(&bimSeq.bim_lock);

	if (seq == BMAPSEQ_ANY) {
		invalid = 1;
		goto done;
	}
	if (seq == bimSeq.bim_minseq)
		goto done;

	if (seq > bimSeq.bim_minseq ||
	    bimSeq.bim_minseq == BMAPSEQ_ANY) {
		bimSeq.bim_minseq = seq;
		OPSTAT_ASSIGN(SLI_OPST_MIN_SEQNO, seq);
		psclog_info("update min seq to %"PRId64, seq);
		PFL_GETTIMESPEC(&bimSeq.bim_age);
		goto done;
	}

	if (seq >= bimSeq.bim_minseq - BMAP_SEQLOG_FACTOR) {
		/*
		 * This allows newly-acquired leases to be accepted
		 * after an MDS restart.  Otherwise, the client would
		 * have to keep trying with a new lease for a while
		 * depending on the size of the gap.
		 *
		 * To deal out-of-order RPCs, we may need to number our
		 * RPCs.  It is probably not worth the effort in our use
		 * cases.
		 */
		psclog_warnx("seq reduced from %"PRId64" to %"PRId64,
		    bimSeq.bim_minseq, seq);
		bimSeq.bim_minseq = seq;
		OPSTAT_INCR(SLI_OPST_SEQNO_REDUCE);
		goto done;
	}

	/*
	 * This should never happen.  Complain and ask our caller to
	 * retry again.
	 */
	invalid = 1;
	psclog_warnx("Seqno %"PRId64" is invalid "
	    "(bim_minseq=%"PRId64")",
	    seq, bimSeq.bim_minseq);
	OPSTAT_INCR(SLI_OPST_SEQNO_INVALID);

 done:
	freelock(&bimSeq.bim_lock);

	return (invalid);
}

uint64_t
bim_getcurseq(void)
{
	uint64_t seqno = BMAPSEQ_ANY;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getbmapminseq_req *mq;
	struct srm_getbmapminseq_rep *mp;
	struct timespec crtime;
	int rc;

	OPSTAT_INCR(SLI_OPST_GET_CUR_SEQ);

 retry:
	spinlock(&bimSeq.bim_lock);
	while (bimSeq.bim_flags & BIM_RETRIEVE_SEQ) {
		psc_waitq_wait(&bimSeq.bim_waitq, &bimSeq.bim_lock);
		spinlock(&bimSeq.bim_lock);
	}

	PFL_GETTIMESPEC(&crtime);
	timespecsub(&crtime, &bim_timeo, &crtime);

	if (timespeccmp(&crtime, &bimSeq.bim_age, >) ||
	    bimSeq.bim_minseq == BMAPSEQ_ANY) {

		bimSeq.bim_flags |= BIM_RETRIEVE_SEQ;
		freelock(&bimSeq.bim_lock);

		OPSTAT_INCR(SLI_OPST_GET_CUR_SEQ_RPC);
		rc = sli_rmi_getcsvc(&csvc);
		if (rc)
			goto out;

		rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAPMINSEQ, rq, mq,
		    mp);
		if (rc)
			goto out;

		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc)
			goto out;

		seqno = mp->seqno;

		rc = bim_updateseq(seqno);
 out:
		if (rq) {
			pscrpc_req_finished(rq);
			rq = NULL;
		}
		if (csvc) {
			sl_csvc_decref(csvc);
			csvc = NULL;
		}

		spinlock(&bimSeq.bim_lock);
		bimSeq.bim_flags &= ~BIM_RETRIEVE_SEQ;
		psc_waitq_wakeall(&bimSeq.bim_waitq);
		if (rc) {
			psclog_warnx("failed to get bmap seqno rc=%d",
			    rc);
			freelock(&bimSeq.bim_lock);
			sleep(1);
			goto retry;
		}
	}

	seqno = bimSeq.bim_minseq;
	freelock(&bimSeq.bim_lock);

	return (seqno);
}

void
bcr_ready_add(struct bcrcupd *bcr)
{
	BII_LOCK_ENSURE(bcr->bcr_bii);
	lc_addtail(&bcr_ready, bcr);
}

/**
 * bcr_ready_remove - We are done with this batch of CRC updates.   Drop
 *	its reference to the bmap and free the CRC update structure.
 */
__static void
bcr_ready_remove(struct bcrcupd *bcr)
{
	lc_remove(&bcr_ready, bcr);

	psc_assert(bcr->bcr_flags & BCR_SCHEDULED);

	bcr->bcr_bii->bii_bcr_xid_last++;

	BII_ULOCK(bcr->bcr_bii);

	bmap_op_done_type(bcr_2_bmap(bcr), BMAP_OPCNT_BCRSCHED);
	psc_pool_return(bmap_crcupd_pool, bcr);
}

void
bcr_finalize(struct bcrcupd *bcr)
{
	struct bmap_iod_info *bii = bcr->bcr_bii;
	struct bmapc_memb *b = bii_2_bmap(bii);

	DEBUG_BCR(PLL_INFO, bcr, "finalize");

	LIST_CACHE_LOCK(&bcr_ready);
	BII_LOCK(bii);
	psc_assert(b->bcm_flags & BMAP_IOD_BCRSCHED);
	psc_assert(b->bcm_flags & BMAP_IOD_INFLIGHT);
	b->bcm_flags &= ~BMAP_IOD_INFLIGHT;

	bcr_ready_remove(bcr);
	LIST_CACHE_ULOCK(&bcr_ready);
}

void
slibmaprlsthr_main(struct psc_thread *thr)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct srm_bmap_release_req *brr, *mq;
	struct srm_bmap_release_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info *bii, *tmp;
	struct bmap_iod_rls *brls;
	struct bmapc_memb *b;
	int nrls, rc, i;

	psc_dynarray_ensurelen(&a, MAX_BMAP_RELEASE);
	brr = PSCALLOC(sizeof(struct srm_bmap_release_req));

	while (pscthr_run(thr)) {

		nrls = 0;
		LIST_CACHE_LOCK(&bmapRlsQ);
		LIST_CACHE_FOREACH_SAFE(bii, tmp, &bmapRlsQ) {

			b = bii_2_bmap(bii);
			if (!BMAP_TRYLOCK(b))
				continue;

			if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				DEBUG_BMAP(PLL_DIAG, b, "skip due to refcnt");
				BMAP_ULOCK(b);
				continue;
			}
			i = pll_nitems(&bii->bii_rls);
			DEBUG_BMAP(PLL_INFO, b, "returning %d bmap leases", i);
			while ((brls = pll_get(&bii->bii_rls))) {
				memcpy(&brr->sbd[nrls++],
				    &brls->bir_sbd, sizeof(struct
				    srt_bmapdesc));
				psc_pool_return(bmap_rls_pool, brls);
				if (nrls >= MAX_BMAP_RELEASE)
					break;
			}
			if (!pll_nitems(&bii->bii_rls)) {
				lc_remove(&bmapRlsQ, bii);
				psc_dynarray_add(&a, b);
			} else
				BMAP_ULOCK(b);

			if (nrls >= MAX_BMAP_RELEASE)
				break;
		}
		LIST_CACHE_ULOCK(&bmapRlsQ);

		DYNARRAY_FOREACH(b, i, &a)
			bmap_op_done_type(b, BMAP_OPCNT_REAPER);
		
		psc_dynarray_reset(&a);

		if (!nrls) {
			sleep(SLIOD_BMAP_RLS_WAIT_SECS);
			continue;
		}

		OPSTAT_INCR(SLI_OPST_SRMT_RELEASE);

		brr->nbmaps = nrls;
		/*
		 * The system can tolerate the loss of these messages so
		 * errors here should not be considered fatal.
		 */
		rc = sli_rmi_getcsvc(&csvc);
		if (rc) {
			psclog_errorx("failed to get MDS import rc=%d",
			    rc);
			continue;
		}

		rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
		if (rc) {
			psclog_errorx("failed to generate new req "
			    "rc=%d", rc);
			sl_csvc_decref(csvc);
			continue;
		}

		memcpy(mq, brr, sizeof(*mq));
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc)
			psclog_errorx("RELEASEBMAP req failed rc=%d",
			    rc);

		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
	psc_dynarray_free(&a);
}

void
slibmaprlsthr_spawn(void)
{
	lc_reginit(&bmapRlsQ, struct bmap_iod_info, bii_lentry,
	    "bmaprlsq");

	pscthr_init(SLITHRT_BMAPRLS, 0, slibmaprlsthr_main, NULL, 0,
	    "slibmaprlsthr");
}

void
iod_bmap_init(struct bmapc_memb *b)
{
	struct bmap_iod_info *bii;

	bii = bmap_2_bii(b);

	memset(bii, 0, sizeof(*bii));
	INIT_PSC_LISTENTRY(&bii->bii_lentry);
	SPLAY_INIT(&bii->bii_slvrs);
	pll_init(&bii->bii_bklog_bcrs, struct bcrcupd, bcr_lentry,
	    NULL);

	pll_init(&bii->bii_rls, struct bmap_iod_rls, bir_lentry, NULL);

	psc_atomic32_set(&bii->bii_crcdirty_slvrs, 0);

	/*
	 * XXX At some point we'll want to let bmaps hang around in the
	 * cache to prevent extra reads and CRC table fetches.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_REAPER);
	lc_addtail(&bmapRlsQ, bii);
}

void
iod_bmap_finalcleanup(struct bmapc_memb *b)
{
	struct bmap_iod_info *bii;

	bii = bmap_2_bii(b);
	psc_assert(pll_empty(&bii->bii_rls));
	psc_assert(SPLAY_EMPTY(&bii->bii_slvrs));
	psc_assert(psclist_disjoint(&bii->bii_lentry));
	psc_assert(!psc_atomic32_read(&bii->bii_crcdirty_slvrs));
}

/**
 * iod_bmap_retrieve - Load the relevant bmap information from the
 *	metadata server.  In the case of the ION the bmap sections of
 *	interest are the CRC table and the CRC states bitmap.  For now
 *	we only load this information on read.
 * @b: bmap to load.
 * @rw: the bmap access mode.
 * Return zero on success or errno code on failure (likely an RPC
 *	problem).
 */
int
iod_bmap_retrieve(struct bmapc_memb *b, enum rw rw, __unusedx int flags)
{
	struct pscrpc_request *rq = NULL;
	struct srm_getbmap_full_req *mq;
	struct srm_getbmap_full_rep *mp;
	struct slashrpc_cservice *csvc;
	int rc, i;
	struct bmap_iod_info *bii = bmap_2_bii(b);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAPCRCS, rq, mq, mp);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b,
		    "could not create request rc=%d", rc);
		goto out;
	}

	mq->rw = rw;
	mq->bmapno = b->bcm_bmapno;
	memcpy(&mq->fg, &b->bcm_fcmh->fcmh_fg, sizeof(mq->fg));

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "req failed (%d)", rc);
		goto out;
	}

	BMAP_LOCK(b); /* equivalent to BII_LOCK() */

	for (i = 0; i < SLASH_SLVRS_PER_BMAP; i++) {
		bii->bii_crcstates[i] = mp->crcstates[i];
		bii->bii_crcs[i] = mp->crcs[i];
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

	_dump_bmap_flags_common(&flags, &seq);
	PFL_PRFLAG(BMAP_IOD_INFLIGHT, &flags, &seq);
	PFL_PRFLAG(BMAP_IOD_BCRSCHED, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif

struct bmap_ops sl_bmap_ops = {
	NULL,				/* bmo_free() */
	iod_bmap_init,			/* bmo_init_privatef() */
	iod_bmap_retrieve,		/* bmo_retrievef() */
	NULL,				/* bmo_mode_chngf() */
	iod_bmap_finalcleanup		/* bmo_final_cleanupf() */
};
