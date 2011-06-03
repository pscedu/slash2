/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2011, Pittsburgh Supercomputing Center (PSC).
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

#define mds_bmap_timeotbl_curslot					\
	((time(NULL) % BMAP_TIMEO_MAX) / BMAP_TIMEO_TBL_QUANT)

#define mds_bmap_timeotbl_nextwakeup					\
	(BMAP_TIMEO_TBL_QUANT - ((time(NULL) % BMAP_TIMEO_MAX) %	\
				 BMAP_TIMEO_TBL_QUANT))

void
mds_bmap_timeotbl_init(void)
{
	int i;
	struct bmap_timeo_entry *e;

	INIT_SPINLOCK(&mdsBmapTimeoTbl.btt_lock);

	mdsBmapTimeoTbl.btt_nentries = BMAP_TIMEO_TBL_SZ;
	mdsBmapTimeoTbl.btt_entries =
		PSCALLOC(sizeof(struct bmap_timeo_entry) * BMAP_TIMEO_TBL_SZ);

	for (i=0; i < BMAP_TIMEO_TBL_SZ; i++) {
		e = &mdsBmapTimeoTbl.btt_entries[i];
		e->bte_maxseq = BMAPSEQ_ANY;
		INIT_PSCLIST_HEAD(&e->bte_bmaps);
		//mdsBmapTimeoTbl[i].bte_maxseq = BMAPSEQ_ANY;
	}
	mdsBmapTimeoTbl.btt_ready = 1;
}

static void
mds_bmap_journal_bmapseq(struct slmds_jent_bmapseq *sjbsq)
{
	struct slmds_jent_bmapseq *buf;

	buf = pjournal_get_buf(mdsJournal,
		sizeof(struct slmds_jent_bmapseq));

	*buf = *sjbsq;

	mds_reserve_slot();
	pjournal_add_entry(mdsJournal, 0, MDS_LOG_BMAP_SEQ,
		0, buf, sizeof(struct slmds_jent_bmapseq));
	mds_unreserve_slot();

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

	if (!mdsBmapTimeoTbl.btt_ready)
		return (-EAGAIN);

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
	struct bmap_timeo_entry *e;

	spinlock(&mdsBmapTimeoTbl.btt_lock);

	if (flags & BTE_DEL) {
		psclist_del(&bml->bml_timeo_lentry,
		    psc_lentry_hd(&bml->bml_timeo_lentry));
		bml->bml_flags &= ~BML_TIMEOQ;
		freelock(&mdsBmapTimeoTbl.btt_lock);
		return (BMAPSEQ_ANY);
	}

	/* Currently all leases are placed in the last slot regardless
	 *   of their start time.  This is the case for BTE_REATTACH.
	 */
	psclog_dbg("timeoslot=%"PSCPRI_TIMET, mds_bmap_timeotbl_curslot);
	e = &mdsBmapTimeoTbl.btt_entries[mds_bmap_timeotbl_curslot];

	if (flags & BTE_REATTACH) {
		/* BTE_REATTACH is only called from startup context.
		 */
		//		psc_assert(mdsBmapTimeoTbl.btt_minseq ==
		//		   mdsBmapTimeoTbl.btt_maxseq);

		if (mdsBmapTimeoTbl.btt_maxseq < bml->bml_seq)
			mdsBmapTimeoTbl.btt_minseq =
				mdsBmapTimeoTbl.btt_maxseq = bml->bml_seq;

		if (bml->bml_seq > e->bte_maxseq ||
		    e->bte_maxseq == BMAPSEQ_ANY)
			seq = e->bte_maxseq = bml->bml_seq;

	} else {
		seq = e->bte_maxseq = mds_bmap_timeotbl_getnextseq();
	}

	if (bml->bml_flags & BML_UPGRADE)
		psclist_del(&bml->bml_timeo_lentry,
		    psc_lentry_hd(&bml->bml_timeo_lentry));

	bml->bml_flags |= BML_TIMEOQ;
	psclist_add_tail(&bml->bml_timeo_lentry, &e->bte_bmaps);

	freelock(&mdsBmapTimeoTbl.btt_lock);
	return (seq);
}

void
slmbmaptimeothr_begin(__unusedx struct psc_thread *thr)
{
	int timeoslot, i;
	struct bmap_timeo_entry *e;
	struct bmap_mds_lease *bml;
	struct psc_dynarray a = DYNARRAY_INIT;
	struct slmds_jent_bmapseq sjbsq;

	while (pscthr_run()) {
		spinlock(&mdsBmapTimeoTbl.btt_lock);
		/* The oldest slot is always curslot + 1.
		 */
		timeoslot = mds_bmap_timeotbl_curslot + 1;
		if (timeoslot == BMAP_TIMEO_TBL_SZ)
			timeoslot = 0;

		e = &mdsBmapTimeoTbl.btt_entries[timeoslot];
		/* Skip empty slot to avoid journaling for nothing */
		if (e->bte_maxseq == BMAPSEQ_ANY || psc_listhd_empty(&e->bte_bmaps)) {
			freelock(&mdsBmapTimeoTbl.btt_lock);
			goto sleep;
		}

		psclist_for_each_entry(bml, &e->bte_bmaps, bml_timeo_lentry) {
			/* Don't race with slrmi threads who may be freeing
			 * the lease from an rpc context.
			 * mdsBmapTimeoTbl.btt_lock must be acquired before
			 * pulling the bml from this list.
			 */
			spinlock(&bml->bml_lock);
			if (!(bml->bml_flags & BML_FREEING)) {
				bml->bml_flags |= BML_FREEING;
				psc_dynarray_add(&a, bml);
			}
			freelock(&bml->bml_lock);
		}

		memset(&sjbsq, 0, sizeof(sjbsq));
		sjbsq.sjbsq_high_wm = mdsBmapTimeoTbl.btt_maxseq;
		sjbsq.sjbsq_low_wm = mdsBmapTimeoTbl.btt_minseq =
			e->bte_maxseq;

		freelock(&mdsBmapTimeoTbl.btt_lock);
		/* Journal the new low watermark.
		 */
		mds_bmap_journal_bmapseq(&sjbsq);

		for (i = 0; i < psc_dynarray_len(&a); i++) {
			bml = psc_dynarray_getpos(&a, i);
			psc_assert(bml->bml_seq <= e->bte_maxseq);
			if (mds_bmap_bml_release(bml))
				abort();
		}
		psc_dynarray_reset(&a);
 sleep:
		psclog_dbg("timeoslot=%d sleeptime=%"PSCPRI_TIMET,
			timeoslot, mds_bmap_timeotbl_nextwakeup);

		sleep(mds_bmap_timeotbl_nextwakeup);
	}
	psc_dynarray_free(&a);
}

void
slmbmaptimeothr_spawn(void)
{
	pscthr_init(SLMTHRT_BMAPTIMEO, 0, slmbmaptimeothr_begin,
	    NULL, 0, "slmbmaptimeothr");
}
