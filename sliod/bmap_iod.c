/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
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

#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"

#include "bmap_iod.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

#define NOTIFY_FSYNC_TIMEOUT	10

struct bmap_iod_minseq	 sli_bminseq;
const struct timespec	 sli_bmap_release_idle = { 0, 1000 * 1000L};

struct psc_listcache	 sli_bmap_releaseq;		/* bmaps to release */
struct psc_listcache	 sli_bmaplease_releaseq;	/* bmap leases to release */

struct psc_poolmaster    bmap_rls_poolmaster;
struct psc_poolmgr	*bmap_rls_pool;

void
bim_init(void)
{
	INIT_SPINLOCK(&sli_bminseq.bim_lock);
	psc_waitq_init(&sli_bminseq.bim_waitq, "bim");
	sli_bminseq.bim_minseq = 0;
}

int
bim_updateseq(uint64_t seq)
{
	int invalid = 0;

	spinlock(&sli_bminseq.bim_lock);

	if (seq == BMAPSEQ_ANY) {
		invalid = 1;
		OPSTAT_INCR("seqno-invalid");
		goto out;
	}

	if (seq >= sli_bminseq.bim_minseq ||
	    sli_bminseq.bim_minseq == BMAPSEQ_ANY) {
		sli_bminseq.bim_minseq = seq;
		psclog_info("update min seq to %"PRId64, seq);
		PFL_GETTIMESPEC(&sli_bminseq.bim_age);
		OPSTAT_INCR("seqno-advance");
		goto out;
	}

	if (seq >= sli_bminseq.bim_minseq - BMAP_SEQLOG_FACTOR) {
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
		    sli_bminseq.bim_minseq, seq);
		sli_bminseq.bim_minseq = seq;
		OPSTAT_INCR("seqno-reduce");
		goto out;
	}

	/*
	 * This should never happen.  Complain and ask our caller to
	 * retry again. 04/02/2017: This can actually happen if MDS 
	 * is recreated while the IOS is still up. As a result, we
	 * will stuck in this loop and the client will keep getting
	 * ETIMEOUT.
	 *
	 * 07/19/2017: All thread stuck in this loop and the client
	 * cannot connect to the sliod. So we must accept whatever
	 * the MDS gives us.
	 */
	psclog_warnx("seqno %"PRId64" wraps around (bim_minseq=%"PRId64")",
	    seq, sli_bminseq.bim_minseq);
	sli_bminseq.bim_minseq = seq;
	OPSTAT_INCR("seqno-restart");

 out:
	freelock(&sli_bminseq.bim_lock);

	return (invalid);
}

uint64_t
bim_getcurseq(void)
{
	uint64_t seqno;

	spinlock(&sli_bminseq.bim_lock);
	while (sli_bminseq.bim_flags & BIM_RETRIEVE_SEQ) {
		OPSTAT_INCR("bmapseqno-wait");
		psc_waitq_wait(&sli_bminseq.bim_waitq,
		    &sli_bminseq.bim_lock);
		spinlock(&sli_bminseq.bim_lock);
	}

	seqno = sli_bminseq.bim_minseq;
	freelock(&sli_bminseq.bim_lock);
	return (seqno);
}

void
sliseqnothr_main(struct psc_thread *thr)
{
	int rc;
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getbmapminseq_req *mq;
	struct srm_getbmapminseq_rep *mp;
        struct psc_waitq dummy = PSC_WAITQ_INIT("seqno");
        struct timespec ts; 

	while (pscthr_run(thr)) {

		PFL_GETTIMESPEC(&ts);
		ts.tv_sec += BIM_MINAGE;
		psc_waitq_waitabs(&dummy, NULL, &ts);

		spinlock(&sli_bminseq.bim_lock);
		sli_bminseq.bim_flags |= BIM_RETRIEVE_SEQ;
		freelock(&sli_bminseq.bim_lock);
		OPSTAT_INCR("bmapseqno-req");

 retry:
		rc = sli_rmi_getcsvc(&csvc);
		if (rc)
			goto out;

		rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAPMINSEQ, rq, mq,
		    mp);
		if (rc)
			goto out;

		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (!rc)
			rc = bim_updateseq(mp->seqno);
 out:
		if (rq) {
			pscrpc_req_finished(rq);
			rq = NULL;
		}
		if (csvc) {
			sl_csvc_decref(csvc);
			csvc = NULL;
		}

		if (rc) {
			psclog_warnx("failed to get bmap seqno rc=%d", rc);
			sleep(1);
			goto retry;
		}
		spinlock(&sli_bminseq.bim_lock);
		sli_bminseq.bim_flags &= ~BIM_RETRIEVE_SEQ;
		psc_waitq_wakeall(&sli_bminseq.bim_waitq);
		freelock(&sli_bminseq.bim_lock);
	}
}

/*
 * XXX  We probably need a way to make sure that all data have been
 * actually written from cache to persistent storage before
 * synchronizing.
 */
void
sli_bmap_sync(struct bmap *b)
{
	int rc, error, do_sync = 0;
	struct timespec ts0, ts1, delta;
	struct fidc_membh *f;

	f = b->bcm_fcmh;

	FCMH_LOCK(f);
	if (f->fcmh_flags & FCMH_IOD_BACKFILE)
		do_sync = 1;
	FCMH_ULOCK(f);

	if (!do_sync)
		return;

	PFL_GETTIMESPEC(&ts0);

#ifdef HAVE_SYNC_FILE_RANGE
	rc = sync_file_range(fcmh_2_fd(f), b->bcm_bmapno *
	    SLASH_BMAP_SIZE, SLASH_BMAP_SIZE, SYNC_FILE_RANGE_WRITE |
	    SYNC_FILE_RANGE_WAIT_AFTER);
	OPSTAT_INCR("sync-file-range");
#else
	rc = fsync(fcmh_2_fd(f));
	OPSTAT_INCR("fsync");
#endif
	if (rc == -1)
		error = errno;

	PFL_GETTIMESPEC(&ts1);
	timespecsub(&ts1, &ts0, &delta);
	OPSTAT_ADD("rlsbmap-sync-usecs",
	    delta.tv_sec * 1000000 + delta.tv_nsec / 1000);

	if (delta.tv_sec > NOTIFY_FSYNC_TIMEOUT) {
		if (delta.tv_sec > 6 * NOTIFY_FSYNC_TIMEOUT)
			OPSTAT_INCR("sync-slooow");
		else if (delta.tv_sec > 3 * NOTIFY_FSYNC_TIMEOUT)
			OPSTAT_INCR("sync-sloow");
		else
			OPSTAT_INCR("sync-slow");
		DEBUG_FCMH(PLL_WARN, f,
		    "long sync %"PSCPRI_TIMET, delta.tv_sec);
	}
	if (rc) {
		OPSTAT_INCR("sync-fail");
		DEBUG_FCMH(PLL_ERROR, f,
		    "sync failure rc=%d fd=%d errno=%d",
		    rc, fcmh_2_fd(f), error);
	}
}

static void
sli_bml_process_release(struct bmap_iod_rls *brls)
{
	struct bmap_iod_rls *tmpbrls;
	struct fidc_membh *f = NULL;
	struct bmap_iod_info *bii;
	struct srt_bmapdesc *sbd;
	struct bmap *b = NULL;
	int rc;

	sbd = &brls->bir_sbd;
	rc = sli_fcmh_get(&sbd->sbd_fg, &f);
	if (rc) {
		OPSTAT_INCR("bmap-release-fail");
		psclog(rc == ESTALE ? PLL_DIAG : PLL_ERROR,
		    "load fcmh failed; fid="SLPRI_FG" rc=%d",
		    SLPRI_FG_ARGS(&sbd->sbd_fg), rc);
		goto out;
	}

	rc = bmap_getf(f, sbd->sbd_bmapno, SL_WRITE, BMAPGETF_CREATE |
	    BMAPGETF_NORETRIEVE, &b);
	if (rc) {
		psclog_errorx("failed to load bmap %u",
		    sbd->sbd_bmapno);
		goto out;
	}

	bii = bmap_2_bii(b);
	PLL_FOREACH(tmpbrls, &bii->bii_rls) {
		if (!memcmp(&tmpbrls->bir_sbd, sbd, sizeof(*sbd))) {
			OPSTAT_INCR("bmap-release-duplicate");
			goto out;
		}
	}
	DEBUG_BMAP(PLL_DIAG, b, "brls=%p seq=%"PRId64,
	    brls, sbd->sbd_seq);

	if (pll_empty(&bii->bii_rls)) {
		bmap_op_start_type(b, BMAP_OPCNT_RELEASER);
		psc_assert(!(b->bcm_flags & BMAPF_RELEASEQ));
		b->bcm_flags |= BMAPF_RELEASEQ;
		/* 
		 * XXX rename sli_bmaplease_releaseq versus sli_bmap_releaseq.
		 */
		lc_addtail(&sli_bmap_releaseq, bii);
	}

	pll_add(&bii->bii_rls, brls);
	brls = NULL;

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	if (brls)
		psc_pool_return(bmap_rls_pool, brls);
}

void
slibmapleaseprocthr_main(struct psc_thread *thr)
{
	struct bmap_iod_rls *r;

	while (pscthr_run(thr)) {
		r = lc_getwait(&sli_bmaplease_releaseq);
		if (r)
			sli_bml_process_release(r);
	}
}

void
slibmaprlsthr_main(struct psc_thread *thr)
{
	struct psc_dynarray to_sync = DYNARRAY_INIT;
	struct srm_bmap_release_req brr;
	struct bmap_iod_info *bii, *tmp;
	struct bmap_iod_rls *brls;
	struct timespec ts;
	struct bmap *b;
	int nrls, i;

	psc_dynarray_ensurelen(&to_sync, MAX_BMAP_RELEASE);

	while (pscthr_run(thr)) {
		nrls = 0;
		LIST_CACHE_LOCK(&sli_bmap_releaseq);
		lc_peekheadwait(&sli_bmap_releaseq);
		LIST_CACHE_FOREACH_SAFE(bii, tmp, &sli_bmap_releaseq) {
			b = bii_2_bmap(bii);

			DEBUG_BMAP(PLL_DEBUG, b,
			    "considering for release");

			/* deadlock and busy bmap avoidance */
			if (!BMAP_TRYLOCK(b))
				continue;

			psc_assert(b->bcm_flags & BMAPF_RELEASEQ);
			psc_assert(!(b->bcm_flags & BMAPF_TOFREE));

			/*
			 * XXX this logic can be rewritten to avoid
			 * grabbing the pll lock so much.  Specifically,
			 * assert that there are items on bii_rls, and 
			 * we already know if bii_rls will be empty.
			 */
			if (pll_nitems(&bii->bii_rls)) {
				psc_dynarray_add(&to_sync, b);
				bmap_op_start_type(b,
				    BMAP_OPCNT_RELEASER);
			}
			while ((brls = pll_get(&bii->bii_rls))) {
				memcpy(&brr.sbd[nrls++], &brls->bir_sbd,
				    sizeof(struct srt_bmapdesc));
				psc_pool_return(bmap_rls_pool, brls);

				if (nrls >= MAX_BMAP_RELEASE)
					break;
			}
			if (pll_nitems(&bii->bii_rls))
				BMAP_ULOCK(b);
			else {
				b->bcm_flags &= ~BMAPF_RELEASEQ;
				lc_remove(&sli_bmap_releaseq, bii);
				bmap_op_done_type(b,
				    BMAP_OPCNT_RELEASER);
			}

			if (nrls >= MAX_BMAP_RELEASE)
				break;
		}
		if (!psc_dynarray_len(&to_sync)) {
			PFL_GETTIMESPEC(&ts);
			timespecadd(&ts, &sli_bmap_release_idle, &ts);
			lc_peekheadtimed(&sli_bmap_releaseq, &ts);
		}
		LIST_CACHE_ULOCK(&sli_bmap_releaseq);

		DYNARRAY_FOREACH(b, i, &to_sync) {
			/*
			 * Sync to backing file system here to guarantee
			 * that buffers are flushed to disk before the
			 * telling the MDS to release its odtable entry
			 * for this bmap.
			 */
			sli_bmap_sync(b);
			bmap_op_done_type(b, BMAP_OPCNT_RELEASER);
		}

		if (!nrls)
			continue;

		psc_dynarray_reset(&to_sync);

		DEBUG_BMAP(PLL_DIAG, b, "returning %d bmap leases",
		    nrls);

		brr.nbmaps = nrls;
		/*
		 * The system can tolerate the loss of these messages so
		 * errors here should not be fatal.
		 */
		sli_rmi_issue_bmap_release(&brr);
	}
	psc_dynarray_free(&to_sync);
}

void
slibmaprlsthr_spawn(void)
{
	int i;

	lc_reginit(&sli_bmap_releaseq, struct bmap_iod_info, bii_lentry,
	    "breleaseq");
	lc_reginit(&sli_bmaplease_releaseq, struct bmap_iod_rls,
	    bir_lentry, "bmlreleaseq");

	for (i = 0; i < NBMAPRLS_THRS; i++)
		pscthr_init(SLITHRT_BMAPRLS, slibmaprlsthr_main,
		    0, "slibmaprlsthr%d", i);

	pscthr_init(SLITHRT_BMAPLEASE_PROC, slibmapleaseprocthr_main,
	    0, "slibmapleaseprocthr");
}

void
iod_bmap_init(struct bmap *b)
{
	struct bmap_iod_info *bii;

	bii = bmap_2_bii(b);

	memset(bii, 0, sizeof(*bii));
	INIT_PSC_LISTENTRY(&bii->bii_lentry);
	SPLAY_INIT(&bii->bii_slvrs);

	pll_init(&bii->bii_rls, struct bmap_iod_rls, bir_lentry, NULL);
}

void
iod_bmap_finalcleanup(struct bmap *b)
{
	struct bmap_iod_info *bii;

	bii = bmap_2_bii(b);

	psc_assert(pll_empty(&bii->bii_rls));
	psc_assert(SPLAY_EMPTY(&bii->bii_slvrs));
	psc_assert(psclist_disjoint(&bii->bii_lentry));
}

/*
 * Load the relevant bmap information from the metadata server.  In the
 * case of the ION the bmap sections of interest are the CRC table and
 * the CRC states bitmap.  For now we only load this information on read.
 *
 * @b: bmap to load.
 *
 * Return zero on success or errno code on failure (likely an RPC
 * problem).
 */
int
iod_bmap_retrieve(struct bmap *b, __unusedx int flags)
{
	struct pscrpc_request *rq = NULL;
	struct srm_getbmap_full_req *mq;
	struct srm_getbmap_full_rep *mp;
	struct slrpc_cservice *csvc;
	int rc, i;
	struct bmap_iod_info *bii = bmap_2_bii(b);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		goto out;

	/* handled by slm_rmi_handle_bmap_getcrcs() */
	rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAPCRCS, rq, mq, mp);
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b,
		    "could not create request rc=%d", rc);
		goto out;
	}

	mq->rw = b->bcm_flags & BMAPF_RD ? SL_READ : SL_WRITE;
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
	/*
	 * Unblock threads no matter what.
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
	PFL_PRFLAG(BMAPF_CRUD_INFLIGHT, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif

struct bmap_ops sl_bmap_ops = {
	iod_bmap_init,			/* bmo_init_privatef() */
	iod_bmap_retrieve,		/* bmo_retrievef() */
	NULL,				/* bmo_mode_chngf() */
	iod_bmap_finalcleanup		/* bmo_final_cleanupf() */
};
