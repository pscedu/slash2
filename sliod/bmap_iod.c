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

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "bmap_iod.h"
#include "rpc_iod.h"

struct bmap_iod_minseq bimSeq;
static struct timespec bim_timeo = {BIM_MINAGE, 0};

void
bim_init(void)
{
	LOCK_INIT(&bimSeq.bim_lock);
	psc_waitq_init(&bimSeq.bim_waitq);
	bimSeq.bim_minseq = BMAPSEQ_ANY;
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
	uint64_t seq;
	struct timespec ctime;

 retry:
	clock_gettime(CLOCK_REALTIME, &ctime);
	timespecsub(&ctime, &bim_timeo, &ctime);

	spinlock(&bimSeq.bim_lock);
	if (bimSeq.bim_flags & BIM_RETRIEVE_SEQ || 
	    bimSeq.bim_minseq == BMAPSEQ_ANY) {
		psc_waitq_wait(&bimSeq.bim_waitq, &bimSeq.bim_lock);
		goto retry;
	}

	if (timespeccmp(&ctime, &bimSeq.bim_age, >)) {
		struct pscrpc_request *req;
		struct srm_bmap_minseq_get *mq;
		struct srm_generic_rep *mp;
		int rc;
	      
		bimSeq.bim_flags |= BIM_RETRIEVE_SEQ;		
		freelock(&bimSeq.bim_lock);
		
		rc = RSX_NEWREQ(sli_rmi_getimp(), SRMI_VERSION,
				SRMT_GETBMAPMINSEQ, req, mq, mp);
	      
		rc = RSX_WAITREP(req, mp);
		if (!rc)
			bim_updateseq(&mp->data);

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
	psc_assert(bcr->bcr_biodi->biod_bcr == bcr);

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
	ureqlock(&bcr->bcr_biodi->biod_lock, locked);
}

void
bcr_xid_last_bump(struct biod_crcup_ref *bcr)
{
	int locked;

	locked = reqlock(&bcr->bcr_biodi->biod_lock);
	bcr_xid_check(bcr);
	bcr->bcr_biodi->biod_bcr_xid_last++;
	bcr->bcr_biodi->biod_inflight = 0;
	ureqlock(&bcr->bcr_biodi->biod_lock, locked);
}

void
bcr_ready_remove(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	spinlock(&inf->binfcrcs_lock);
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	psc_assert(bcr->bcr_flags & BCR_SCHEDULED);
	pll_remove(&inf->binfcrcs_hold, bcr);
	freelock(&inf->binfcrcs_lock);

	atomic_dec(&inf->binfcrcs_nbcrs);
	bcr_xid_last_bump(bcr);
	PSCFREE(bcr);
}

#if 0
int
bcr_cmp(const void *x, const void *y)
{
	const struct biod_crcup_ref *a = x, *b = y;

	return (CMP(a->bcr_xid, b->bcr_xid));
}
#endif
