/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/alloc.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/list.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/pool.h"
#include "pfl/waitq.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "journal_mds.h"

struct bmap_timeo_table	 mdsBmapTimeoTbl;

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

	buf = pjournal_get_buf(slm_journal,
	    sizeof(struct slmds_jent_bmapseq));

	*buf = *sjbsq;

	mds_reserve_slot(1);
	pjournal_add_entry(slm_journal, 0, MDS_LOG_BMAP_SEQ, 0, buf,
	    sizeof(struct slmds_jent_bmapseq));
	mds_unreserve_slot(1);

	pjournal_put_buf(slm_journal, buf);
}

void
mds_bmap_setcurseq(uint64_t maxseq, uint64_t minseq)
{
	mdsBmapTimeoTbl.btt_maxseq = maxseq;
	mdsBmapTimeoTbl.btt_minseq = minseq;
	OPSTAT_ASSIGN(SLM_OPST_MAX_SEQNO, maxseq);
	OPSTAT_ASSIGN(SLM_OPST_MIN_SEQNO, minseq);
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

void
mds_bmap_timeotbl_journal_seqno(void)
{
	static int log = 0;
	struct slmds_jent_bmapseq sjbsq;

	sjbsq.sjbsq_high_wm = mdsBmapTimeoTbl.btt_maxseq;
	sjbsq.sjbsq_low_wm = mdsBmapTimeoTbl.btt_minseq;

	log++;
	if (!(log % BMAP_SEQLOG_FACTOR))
		mds_bmap_journal_bmapseq(&sjbsq);
}

uint64_t
mds_bmap_timeotbl_getnextseq(void)
{
	int locked;
	uint64_t hwm;

	locked = reqlock(&mdsBmapTimeoTbl.btt_lock);

	mdsBmapTimeoTbl.btt_maxseq++;
	if (mdsBmapTimeoTbl.btt_maxseq == BMAPSEQ_ANY)
		mdsBmapTimeoTbl.btt_maxseq = 0;

	hwm = mdsBmapTimeoTbl.btt_maxseq;
	OPSTAT_ASSIGN(SLM_OPST_MAX_SEQNO, mdsBmapTimeoTbl.btt_maxseq);
	mds_bmap_timeotbl_journal_seqno();

	ureqlock(&mdsBmapTimeoTbl.btt_lock, locked);

	return (hwm);
}

void
mds_bmap_timeotbl_remove(struct bmap_mds_lease *bml)
{
	struct bmap_mds_lease *tmp;
	int update = 0;

	spinlock(&mdsBmapTimeoTbl.btt_lock);
	if (pll_peekhead(&mdsBmapTimeoTbl.btt_leases) == bml)
		update = 1;
	pll_remove(&mdsBmapTimeoTbl.btt_leases, bml);
	if (update) {
		tmp = pll_peekhead(&mdsBmapTimeoTbl.btt_leases);
		if (tmp)
			mdsBmapTimeoTbl.btt_minseq = tmp->bml_seq;
		else
			mdsBmapTimeoTbl.btt_minseq =
			    mdsBmapTimeoTbl.btt_maxseq;
		OPSTAT_ASSIGN(SLM_OPST_MIN_SEQNO,
		    mdsBmapTimeoTbl.btt_minseq);
		mds_bmap_timeotbl_journal_seqno();
	}
	freelock(&mdsBmapTimeoTbl.btt_lock);
}

/**
 * mds_bmap_timeotbl_mdsi -
 * Returns bmapseqno.
 */
uint64_t
mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *bml, int flags)
{
	uint64_t seq = 0;

	if (flags & BTE_DEL) {
		bml->bml_flags &= ~BML_TIMEOQ;
		mds_bmap_timeotbl_remove(bml);
		return (BMAPSEQ_ANY);
	}

	if (flags & BTE_REATTACH) {
		/* BTE_REATTACH is only called from startup context. */
		spinlock(&mdsBmapTimeoTbl.btt_lock);
		if (mdsBmapTimeoTbl.btt_maxseq < bml->bml_seq)
			/*
			 * A lease has been found in odtable whose
			 * issuance was after that of the last HWM
			 * journal entry.  (HWM's are journaled every
			 * BMAP_SEQLOG_FACTOR times.)
			 */
			seq = mdsBmapTimeoTbl.btt_maxseq = bml->bml_seq;

		else if (mdsBmapTimeoTbl.btt_minseq > bml->bml_seq)
			/* This lease has already expired. */
			seq = BMAPSEQ_ANY;
		else
			seq = bml->bml_seq;
		freelock(&mdsBmapTimeoTbl.btt_lock);

	} else {
		seq = mds_bmap_timeotbl_getnextseq();
	}

	BML_LOCK(bml);
	if (bml->bml_flags & BML_TIMEOQ) {
		mds_bmap_timeotbl_remove(bml);
		pll_addtail(&mdsBmapTimeoTbl.btt_leases, bml);
	} else {
		bml->bml_flags |= BML_TIMEOQ;
		pll_addtail(&mdsBmapTimeoTbl.btt_leases, bml);
	}
	BML_ULOCK(bml);

	return (seq);
}

void
slmbmaptimeothr_begin(struct psc_thread *thr)
{
	struct bmap_mds_lease *bml;
	int rc, nsecs = 0;

	while (pscthr_run(thr)) {
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

		BML_ULOCK(bml);
		freelock(&mdsBmapTimeoTbl.btt_lock);

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
	OPSTAT_INCR(SLM_OPST_MIN_SEQNO);
	OPSTAT_INCR(SLM_OPST_MAX_SEQNO);
	pscthr_init(SLMTHRT_BMAPTIMEO, 0, slmbmaptimeothr_begin,
	    NULL, 0, "slmbmaptimeothr");
}
