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
#include "psc_ds/list.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/waitq.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "mdslog.h"
#include "sljournal.h"

struct bmap_timeo_table	 mdsBmapTimeoTbl;
struct psc_poolmaster	 bmapMdsLeasePoolMaster;
struct psc_poolmgr	*bmapMdsLeasePool;

#define mds_bmap_timeotbl_curslot				\
	((time(NULL) / BMAP_TIMEO_TBL_SZ) % BMAP_TIMEO_TBL_SZ)

#define mds_bmap_timeotbl_nextwakeup				\
	(BMAP_TIMEO_TBL_SZ - ((time(NULL) % BMAP_TIMEO_MAX) %	\
			      BMAP_TIMEO_TBL_SZ))

void
mds_bmap_timeotbl_init(void)
{
	int i;
	struct bmap_timeo_entry *e;

	LOCK_INIT(&mdsBmapTimeoTbl.btt_lock);
	/* XXX hack for now.. this value should come from the system log
	 *    at startup
	 */	
	mdsBmapTimeoTbl.btt_minseq = mdsBmapTimeoTbl.btt_maxseq = 0;
	mdsBmapTimeoTbl.btt_nentries = BMAP_TIMEO_TBL_SZ;
	mdsBmapTimeoTbl.btt_entries =
		PSCALLOC(sizeof(struct bmap_timeo_entry) * BMAP_TIMEO_TBL_SZ);

	for (i=0; i < BMAP_TIMEO_TBL_SZ; i++) {
		e = &mdsBmapTimeoTbl.btt_entries[i];
		e->bte_maxseq = BMAPSEQ_ANY;
		INIT_PSCLIST_HEAD(&e->bte_bmaps);
		//mdsBmapTimeoTbl[i].bte_maxseq = BMAPSEQ_ANY;
	}
}

static void
mds_bmap_journal_bmapseq(struct slmds_jent_bmapseq *sjbsq)
{
	struct slmds_jent_bmapseq *sjbsqlog;

	sjbsqlog = (struct slmds_jent_bmapseq *)
	    pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_bmapseq *));
	
	*sjbsqlog = *sjbsq;
	pjournal_add_entry_distill(mdsJournal, 0, MDS_LOG_BMAP_SEQ,
	    (void *)sjbsqlog, sizeof(struct slmds_jent_bmapseq));

	pjournal_put_buf(mdsJournal, (void *)sjbsqlog);
}

void
mds_bmap_getcurseq(uint64_t *maxseq, uint64_t *minseq)
{
	int locked;

	locked = reqlock(&mdsBmapTimeoTbl.btt_lock);

	if (maxseq)
		*maxseq = mdsBmapTimeoTbl.btt_maxseq;
	if (minseq)
		*minseq = mdsBmapTimeoTbl.btt_minseq;

	ureqlock(&mdsBmapTimeoTbl.btt_lock, locked);
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

uint64_t
mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *bml, int flags)
{
	uint64_t seq=0;
	struct bmap_timeo_entry *e;

	psc_assert(bml->bml_flags & BML_TIMEOQ);

	spinlock(&mdsBmapTimeoTbl.btt_lock);

	if (flags & BTE_DEL) {
		psc_assert(psclist_conjoint(&bml->bml_timeo_lentry));
		psclist_del(&bml->bml_timeo_lentry);
		freelock(&mdsBmapTimeoTbl.btt_lock);
		return (BMAPSEQ_ANY);
	}

	psc_trace("timeoslot=%"PSCPRIuTIMET, mds_bmap_timeotbl_curslot);

	e = &mdsBmapTimeoTbl.btt_entries[mds_bmap_timeotbl_curslot];

	if (flags & BTE_REATTACH) {
		if (bml->bml_seq > e->bte_maxseq || 
		    e->bte_maxseq == BMAPSEQ_ANY)
			seq = e->bte_maxseq = bml->bml_seq;
		else
			seq = e->bte_maxseq;
	} else
		seq = e->bte_maxseq = mds_bmap_timeotbl_getnextseq();

	psclist_xadd_tail(&bml->bml_timeo_lentry, &e->bte_bmaps);
       
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
		if (e->bte_maxseq == BMAPSEQ_ANY) {
			freelock(&mdsBmapTimeoTbl.btt_lock);
			goto sleep;
		}

		psclist_for_each_entry(bml, &e->bte_bmaps, bml_timeo_lentry)
			psc_dynarray_add(&a, bml);

		sjbsq.sjbsq_high_wm = mdsBmapTimeoTbl.btt_maxseq;
		sjbsq.sjbsq_low_wm = mdsBmapTimeoTbl.btt_minseq =
			e->bte_maxseq;

		freelock(&mdsBmapTimeoTbl.btt_lock);
		/* Journal the new low watermark.
		 */
		mds_bmap_journal_bmapseq(&sjbsq);

		for (i=0; i < psc_dynarray_len(&a); i++) {
			bml = psc_dynarray_getpos(&a, i);
			psc_assert(bml->bml_seq <= e->bte_maxseq);
			if (mds_bmap_bml_release(bml))
				abort();
		}
		psc_dynarray_reset(&a);
 sleep:
		psc_trace("timeoslot=%d sleeptime=%"PSCPRIuTIMET,
			  timeoslot, mds_bmap_timeotbl_nextwakeup);

		sleep(mds_bmap_timeotbl_nextwakeup);
	}
}


void
slmbmaptimeothr_spawn(void)
{
	pscthr_init(SLMTHRT_BMAPTIMEO, 0, slmbmaptimeothr_begin,
	    NULL, 0, "slmbmaptimeothr");
}
