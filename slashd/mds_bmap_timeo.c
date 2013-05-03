/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_ds/list.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"
#include "psc_util/waitq.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "journal_mds.h"
#include "mdslog.h"

struct bmap_timeo_table	 mdsBmapTimeoTbl;
struct psc_poolmaster	 bmapMdsLeasePoolMaster;
struct psc_poolmgr	*bmapMdsLeasePool;

void
mds_bmap_timeotbl_init(void)
{
	INIT_SPINLOCK(&mdsBmapTimeoTbl.btt_lock);

	pll_init(&mdsBmapTimeoTbl.btt_leases, struct bmap_mds_lease,
	    bml_timeo_lentry, &mdsBmapTimeoTbl.btt_lock);
}

static void
mds_bmap_journal_bmapseq(struct slmds_jent_bmapseq *sjbsq)
{
	struct slmds_jent_bmapseq *buf;

	buf = pjournal_get_buf(mdsJournal,
	    sizeof(struct slmds_jent_bmapseq));

	*buf = *sjbsq;

	mds_reserve_slot(1);
	pjournal_add_entry(mdsJournal, 0, MDS_LOG_BMAP_SEQ, 0, buf,
	    sizeof(struct slmds_jent_bmapseq));
	mds_unreserve_slot(1);

	pjournal_put_buf(mdsJournal, buf);
}

void
mds_bmap_setcurseq(uint64_t maxseq, uint64_t minseq)
{
	mdsBmapTimeoTbl.btt_maxseq = maxseq;
	mdsBmapTimeoTbl.btt_minseq = minseq;
}

int
mds_bmap_getcurseq(uint64_t *maxseq, uint64_t *minseq)
{
	int locked;

	locked = reqlock(&mdsBmapTimeoTbl.btt_lock);

	if (maxseq)
		*maxseq = mdsBmapTimeoTbl.btt_maxseq;
	if (minseq)
		*minseq = mdsBmapTimeoTbl.btt_minseq;

	ureqlock(&mdsBmapTimeoTbl.btt_lock, locked);

	return (0);
}

uint64_t
mds_bmap_timeotbl_getnextseq(void)
{
	struct slmds_jent_bmapseq sjbsq;
	int locked;

	locked = reqlock(&mdsBmapTimeoTbl.btt_lock);

	mdsBmapTimeoTbl.btt_maxseq++;
	if (mdsBmapTimeoTbl.btt_maxseq == BMAPSEQ_ANY)
		mdsBmapTimeoTbl.btt_maxseq = 0;

	sjbsq.sjbsq_high_wm = mdsBmapTimeoTbl.btt_maxseq;
	sjbsq.sjbsq_low_wm = mdsBmapTimeoTbl.btt_minseq;

	ureqlock(&mdsBmapTimeoTbl.btt_lock, locked);

	if (!(sjbsq.sjbsq_high_wm % BMAP_SEQLOG_FACTOR))
		mds_bmap_journal_bmapseq(&sjbsq);

	return (sjbsq.sjbsq_high_wm);
}

/**
 * mds_bmap_timeotbl_mdsi
 * Returns bmapseqno.
 */
uint64_t
mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *bml, int flags)
{
	uint64_t seq=0;

	if (flags & BTE_DEL) {
		bml->bml_flags &= ~BML_TIMEOQ;
		pll_remove(&mdsBmapTimeoTbl.btt_leases, bml);
		return (BMAPSEQ_ANY);
	}

	if (flags & BTE_REATTACH) {
		/* BTE_REATTACH is only called from startup context.
		 */
		spinlock(&mdsBmapTimeoTbl.btt_lock);
		if (mdsBmapTimeoTbl.btt_maxseq < bml->bml_seq)
			/* A lease has been found in odtable whose
			 *   issuance was after that of the last
			 *   HWM journal entry.  (HWM's are journaled
			 *   every BMAP_SEQLOG_FACTOR times.)
			 */
			seq = mdsBmapTimeoTbl.btt_maxseq = bml->bml_seq;

		else if (mdsBmapTimeoTbl.btt_minseq > bml->bml_seq)
			/* This lease has already expired.
			 */
			seq = BMAPSEQ_ANY;
		else
			seq = bml->bml_seq;
		freelock(&mdsBmapTimeoTbl.btt_lock);

	} else {
		seq = mds_bmap_timeotbl_getnextseq();
	}

	BML_LOCK(bml);
	if (bml->bml_flags & BML_TIMEOQ) {
		pll_remove(&mdsBmapTimeoTbl.btt_leases, bml);
		pll_addtail(&mdsBmapTimeoTbl.btt_leases, bml);
	} else {
		bml->bml_flags |= BML_TIMEOQ;
		pll_addtail(&mdsBmapTimeoTbl.btt_leases, bml);
	}
	BML_ULOCK(bml);

	return (seq);
}

void
slmbmaptimeothr_begin(__unusedx struct psc_thread *thr)
{
	struct slmds_jent_bmapseq sjbsq;
	struct bmap_mds_lease *bml;
	int rc, nsecs = 0;

	while (pscthr_run()) {
		spinlock(&mdsBmapTimeoTbl.btt_lock);
		bml = pll_peekhead(&mdsBmapTimeoTbl.btt_leases);
		if (!bml) {
			freelock(&mdsBmapTimeoTbl.btt_lock);
			nsecs = BMAP_TIMEO_MAX;
			goto sleep;
		}

		if (!BML_TRYLOCK(bml)) {
			freelock(&mdsBmapTimeoTbl.btt_lock);
			nsecs = 1;
			goto sleep;
		}
		if (bml->bml_refcnt) {
			BML_ULOCK(bml);
			freelock(&mdsBmapTimeoTbl.btt_lock);
			nsecs = 1;
			goto sleep;
		}

		if (!(bml->bml_flags & BML_FREEING)) {
			nsecs = bml->bml_expire - time(NULL);
			if (nsecs > 0) {
				BML_ULOCK(bml);
				freelock(&mdsBmapTimeoTbl.btt_lock);
				goto sleep;
			}
			bml->bml_flags |= BML_FREEING;
		}

		DEBUG_BMAP(PLL_INFO, bml_2_bmap(bml),
		   "nsecs=%d bml=%p fl=%d seq=%"
		   PRId64, nsecs, bml, bml->bml_flags, bml->bml_seq);

		sjbsq.sjbsq_high_wm = mdsBmapTimeoTbl.btt_maxseq;
		if (bml->bml_seq < mdsBmapTimeoTbl.btt_minseq) {
			psclog_notify("bml %p seq (%"PRIx64") is < "
			    "mdsBmapTimeoTbl.btt_minseq (%"PRIx64")",
			    bml, bml->bml_seq,
			    mdsBmapTimeoTbl.btt_minseq);

			sjbsq.sjbsq_low_wm =
			    mdsBmapTimeoTbl.btt_minseq;
		} else
			sjbsq.sjbsq_low_wm =
			    mdsBmapTimeoTbl.btt_minseq = bml->bml_seq;

		BML_ULOCK(bml);
		freelock(&mdsBmapTimeoTbl.btt_lock);
		/* Journal the new low watermark. */
		mds_bmap_journal_bmapseq(&sjbsq);

		rc = mds_bmap_bml_release(bml);
		if (rc) {
			DEBUG_BMAP(PLL_WARN, bml_2_bmap(bml),
			   "rc=%d bml=%p fl=%d seq=%"PRId64,
			   rc, bml, bml->bml_flags, bml->bml_seq);
			nsecs = 1;
		} else
			nsecs = 0;
 sleep:
		psclog_debug("nsecs=%d", nsecs);

		if (nsecs > 0)
			sleep((uint32_t)nsecs);
	}
}

void
slmbmaptimeothr_spawn(void)
{
	pscthr_init(SLMTHRT_BMAPTIMEO, 0, slmbmaptimeothr_begin,
	    NULL, 0, "slmbmaptimeothr");
}
