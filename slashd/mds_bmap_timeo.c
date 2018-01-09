/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2010-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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

struct bmap_timeo_table	 slm_bmap_leases;

void
mds_bmap_timeotbl_init(void)
{
	INIT_SPINLOCK(&slm_bmap_leases.btt_lock);

	pll_init(&slm_bmap_leases.btt_leases, struct bmap_mds_lease,
	    bml_timeo_lentry, &slm_bmap_leases.btt_lock);
}

static uint32_t
mds_bmap_journal_bmapseq(struct slmds_jent_bmapseq *sjbsq)
{
	uint32_t slot;
	struct slmds_jent_bmapseq *buf;

	buf = pjournal_get_buf(slm_journal,
	    sizeof(struct slmds_jent_bmapseq));

	*buf = *sjbsq;

	mds_reserve_slot(1);
	slot = pjournal_add_entry(slm_journal, 0, MDS_LOG_BMAP_SEQ, 0, buf,
	    sizeof(struct slmds_jent_bmapseq));
	mds_unreserve_slot(1);

	pjournal_put_buf(slm_journal, buf);
	return slot;
}

void
mds_bmap_setcurseq(uint64_t maxseq, uint64_t minseq)
{
	slm_bmap_leases.btt_maxseq = maxseq;
	slm_bmap_leases.btt_minseq = minseq;
}

void
mds_bmap_getcurseq(uint64_t *maxseq, uint64_t *minseq)
{
	/*
 	 * Reading these two 64-bit sequence number is atomic.
	 * So don't take the slm_bmap_leases.btt_lock spinlock.
	 * It has caused a deadlock involving multiple parties.
 	 */
	if (minseq)
		*minseq = slm_bmap_leases.btt_minseq;
	if (maxseq)
		*maxseq = slm_bmap_leases.btt_maxseq;

	psclog_debug("retrieve: low watermark = %"PRIu64", "
	    "high watermark = %"PRIu64, 
	    minseq ? (*minseq) : BMAPSEQ_ANY, 
	    maxseq ? (*maxseq) : BMAPSEQ_ANY);
}

void
mds_bmap_timeotbl_journal_seqno(void)
{
	uint32_t slot;
	static int log = 0;
	struct slmds_jent_bmapseq sjbsq;

	sjbsq.sjbsq_low_wm = slm_bmap_leases.btt_minseq;
	sjbsq.sjbsq_high_wm = slm_bmap_leases.btt_maxseq;

	log++;
	if (log % BMAP_SEQLOG_FACTOR)
		return;

	slot = mds_bmap_journal_bmapseq(&sjbsq);
	(void)slot;
	psclog_debug("journal: slot = %u, "
	    "low watermark = %"PRIu64", "
	    "high watermark = %"PRIu64, 
	    slot, sjbsq.sjbsq_low_wm, sjbsq.sjbsq_high_wm);
}

uint64_t
mds_bmap_timeotbl_getnextseq(void)
{
	uint64_t hwm;

	/*
	 * Skip a zero sequence number because the client does not like
	 * it.  More work is needed when an IOS decides if a smaller
	 * sequence number is actually ahead of a larger one after a
	 * wrap around happens.
	 */
	slm_bmap_leases.btt_maxseq++;
	if (slm_bmap_leases.btt_maxseq == BMAPSEQ_ANY) {
		OPSTAT_INCR("seqno-wrap");
		slm_bmap_leases.btt_maxseq = 1;
	} else if (slm_bmap_leases.btt_maxseq == 0) {
		/*
 		 * This should never happen, but we have been burned.
 		 */
		OPSTAT_INCR("seqno-skip");
		slm_bmap_leases.btt_maxseq = 1;
	}

	hwm = slm_bmap_leases.btt_maxseq;
	mds_bmap_timeotbl_journal_seqno();

	return (hwm);
}

void
mds_bmap_timeotbl_remove(struct bmap_mds_lease *bml)
{
	struct bmap_mds_lease *tmp;
	int update = 0;

	if (pll_peekhead(&slm_bmap_leases.btt_leases) == bml)
		update = 1;
	pll_remove(&slm_bmap_leases.btt_leases, bml);
	if (!update)
		return;

	tmp = pll_peekhead(&slm_bmap_leases.btt_leases);
	if (!tmp) {
		slm_bmap_leases.btt_minseq = slm_bmap_leases.btt_maxseq;
		mds_bmap_timeotbl_journal_seqno();
		return;
	}
	/*
 	 * During log replay,  expired bmap leases are tagged with
 	 * BMAPSEQ_ANY. Ignore them.
 	 */
	if (tmp->bml_seq != BMAPSEQ_ANY) {
		slm_bmap_leases.btt_minseq = tmp->bml_seq;
		mds_bmap_timeotbl_journal_seqno();
	}
}

/*
 * Obtain the current bmap seqno.
 */
uint64_t
mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *bml, int flags)
{
	uint64_t seq = 0;

	spinlock(&slm_bmap_leases.btt_lock);
	if (flags & BTE_DEL) {
		bml->bml_flags &= ~BML_TIMEOQ;
		mds_bmap_timeotbl_remove(bml);
		freelock(&slm_bmap_leases.btt_lock);
		return (BMAPSEQ_ANY);
	}

	if (flags & BTE_REATTACH) {
		/* BTE_REATTACH is only called from startup context. */
		if (slm_bmap_leases.btt_maxseq < bml->bml_seq)
			/*
			 * A lease has been found in odtable whose
			 * issuance was after that of the last HWM
			 * journal entry.  (HWM's are journaled every
			 * BMAP_SEQLOG_FACTOR times.)
			 */
			seq = slm_bmap_leases.btt_maxseq = bml->bml_seq;

		else if (slm_bmap_leases.btt_minseq > bml->bml_seq)
			/* This lease has already expired. */
			seq = BMAPSEQ_ANY;
		else
			seq = bml->bml_seq;

	} else {
		/*
 		 * Don't update bml_seq in the BTE_REATTACH case.
 		 * Otherwise, it will be out-of-sync with what is
 		 * stored in the odtable and triggers asserts down
 		 * the road.
 		 */
		seq = mds_bmap_timeotbl_getnextseq();
		bml->bml_seq = seq;
	}

	if (bml->bml_flags & BML_TIMEOQ) {
		OPSTAT_INCR("bml-move");
		mds_bmap_timeotbl_remove(bml);
		pll_addtail(&slm_bmap_leases.btt_leases, bml);
	} else {
		bml->bml_flags |= BML_TIMEOQ;
		pll_addtail(&slm_bmap_leases.btt_leases, bml);
	}
	freelock(&slm_bmap_leases.btt_lock);

	return (seq);
}

void
slmbmaptimeothr_begin(struct psc_thread *thr)
{
	char wait[16];
	struct bmap *b;
	struct bmap_mds_lease *bml;
	int rc, nsecs = 0;

	while (pscthr_run(thr)) {
		spinlock(&slm_bmap_leases.btt_lock);
		bml = pll_peekhead(&slm_bmap_leases.btt_leases);
		if (!bml) {
			freelock(&slm_bmap_leases.btt_lock);
			nsecs = BMAP_TIMEO_MAX;
			goto out;
		}
		b = bml_2_bmap(bml);

		if (!BMAP_TRYLOCK(b)) {
			freelock(&slm_bmap_leases.btt_lock);
			nsecs = 1;
			goto out;
		}
		if (bml->bml_refcnt) {
			BMAP_ULOCK(b);
			freelock(&slm_bmap_leases.btt_lock);
			nsecs = 1;
			goto out;
		}
		if (bml->bml_flags & BML_FREEING) {
			BMAP_ULOCK(b);
			freelock(&slm_bmap_leases.btt_lock);
			nsecs = 1;
			goto out;
		}
		nsecs = bml->bml_expire - time(NULL);
		if (nsecs > 0) {
			BMAP_ULOCK(b);
			freelock(&slm_bmap_leases.btt_lock);
			goto out;
		}

		bml->bml_refcnt++;
		bml->bml_flags |= BML_FREEING;
		BMAP_ULOCK(b);

		freelock(&slm_bmap_leases.btt_lock);

		BMAP_LOCK(b);
		rc = mds_bmap_bml_release(bml);
		if (rc) {
			DEBUG_BMAP(PLL_WARN, bml_2_bmap(bml),
			    "rc=%d bml=%p fl=%d seq=%"PRId64,
			    rc, bml, bml->bml_flags, bml->bml_seq);
			nsecs = 1;
		} else
			nsecs = 0;
 out:
		psclog_debug("nsecs=%d", nsecs);

		if (nsecs > 0) {
			snprintf(wait, 16, "sleep %d", nsecs);
			thr->pscthr_waitq = wait;
			sleep((uint32_t)nsecs);
			thr->pscthr_waitq = NULL;
		}
	}
}

void
slmbmaptimeothr_spawn(void)
{
	pscthr_init(SLMTHRT_BMAPTIMEO, slmbmaptimeothr_begin, 0,
	    "slmbmaptimeothr");
}
