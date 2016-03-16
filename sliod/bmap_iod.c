/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
static struct timespec	 bim_timeo = { BIM_MINAGE, 0 };

struct psc_listcache	 sli_bmap_releaseq;

struct psc_lockedlist	 sli_bii_rls;

struct psc_poolmaster    bmap_rls_poolmaster;
struct psc_poolmgr	*bmap_rls_pool;

psc_spinlock_t		 sli_release_bmap_lock = SPINLOCK_INIT;
struct psc_waitq	 sli_release_bmap_waitq = PSC_WAITQ_INIT;

void
bim_init(void)
{
	INIT_SPINLOCK(&sli_bminseq.bim_lock);
	psc_waitq_init(&sli_bminseq.bim_waitq);
	sli_bminseq.bim_minseq = 0;
}

int
bim_updateseq(uint64_t seq)
{
	int invalid = 0;

	spinlock(&sli_bminseq.bim_lock);

	if (seq == BMAPSEQ_ANY)
		goto out1;

	if (seq >= sli_bminseq.bim_minseq ||
	    sli_bminseq.bim_minseq == BMAPSEQ_ANY) {
		sli_bminseq.bim_minseq = seq;
		psclog_info("update min seq to %"PRId64, seq);
		PFL_GETTIMESPEC(&sli_bminseq.bim_age);
		OPSTAT_INCR("seqno-update");
		goto out2;
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
		goto out2;
	}

 out1:
	/*
	 * This should never happen.  Complain and ask our caller to
	 * retry again.
	 */
	invalid = 1;
	psclog_warnx("Seqno %"PRId64" is invalid "
	    "(bim_minseq=%"PRId64")",
	    seq, sli_bminseq.bim_minseq);
	OPSTAT_INCR("seqno-invalid");

 out2:
	freelock(&sli_bminseq.bim_lock);

	return (invalid);
}

uint64_t
bim_getcurseq(void)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_getbmapminseq_req *mq;
	struct srm_getbmapminseq_rep *mp;
	struct timespec crtime;
	uint64_t seqno;
	int rc;

	OPSTAT_INCR("bim-getcurseq");

 retry:
	spinlock(&sli_bminseq.bim_lock);
	while (sli_bminseq.bim_flags & BIM_RETRIEVE_SEQ) {
		psc_waitq_wait(&sli_bminseq.bim_waitq,
		    &sli_bminseq.bim_lock);
		spinlock(&sli_bminseq.bim_lock);
	}

	PFL_GETTIMESPEC(&crtime);
	timespecsub(&crtime, &bim_timeo, &crtime);

	if (timespeccmp(&crtime, &sli_bminseq.bim_age, >) ||
	    sli_bminseq.bim_minseq == BMAPSEQ_ANY) {

		sli_bminseq.bim_flags |= BIM_RETRIEVE_SEQ;
		freelock(&sli_bminseq.bim_lock);

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

		spinlock(&sli_bminseq.bim_lock);
		sli_bminseq.bim_flags &= ~BIM_RETRIEVE_SEQ;
		psc_waitq_wakeall(&sli_bminseq.bim_waitq);
		if (rc) {
			freelock(&sli_bminseq.bim_lock);
			psclog_warnx("failed to get bmap seqno rc=%d",
			    rc);
			sleep(1);
			goto retry;
		}
	}

	seqno = sli_bminseq.bim_minseq;
	freelock(&sli_bminseq.bim_lock);

	return (seqno);
}

void
bcr_ready_add(struct bcrcupd *bcr)
{
	DEBUG_BCR(PLL_DIAG, bcr, "bcr add");

	BII_LOCK_ENSURE(bcr->bcr_bii);
	lc_addtail(&bcr_ready, bcr);
	bmap_op_start_type(bcr_2_bmap(bcr), BMAP_OPCNT_BCRSCHED);
}

void
bcr_ready_remove(struct bcrcupd *bcr)
{
	DEBUG_BCR(PLL_DIAG, bcr, "bcr remove");

	lc_remove(&bcr_ready, bcr);

	bmap_op_done_type(bcr_2_bmap(bcr), BMAP_OPCNT_BCRSCHED);
	psc_pool_return(bmap_crcupd_pool, bcr);
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
		DEBUG_FCMH(PLL_NOTICE, f,
		    "long sync %ld", delta.tv_sec);
	}
	if (rc) {
		OPSTAT_INCR("sync-fail");
		DEBUG_FCMH(PLL_ERROR, f,
		    "sync failure rc=%d fd=%d errno=%d",
		    rc, fcmh_2_fd(f), error);
	}
}

void
slibmaprlsthr_process_releases(struct psc_dynarray *a)
{
	int i, rc, new;
	struct bmap_iod_rls *brls, *tmpbrls;
	struct bmap_iod_info *bii;
	struct srt_bmapdesc *sbd;
	struct fidc_membh *f;
	struct bmap *b;

	i = 0;
	while ((brls = pll_get(&sli_bii_rls))) {
		psc_dynarray_add(a, brls);
		i++;
		if (i == MAX_BMAP_RELEASE)
			break;
	}
	DYNARRAY_FOREACH(brls, i, a) {
		sbd = &brls->bir_sbd;
		rc = sli_fcmh_peek(&sbd->sbd_fg, &f);
		if (rc) {
			OPSTAT_INCR("rlsbmap-fail");
			psclog(rc == ENOENT || rc == ESTALE ?
			    PLL_DIAG : PLL_ERROR,
			    "load fcmh failed; fid="SLPRI_FG" rc=%d",
			    SLPRI_FG_ARGS(&sbd->sbd_fg), rc);
			psc_pool_return(bmap_rls_pool, brls);
			continue;
		}

		rc = bmap_getf(f, sbd->sbd_bmapno, SL_WRITE,
		    BMAPGETF_CREATE | BMAPGETF_NORETRIEVE, &b);
		if (rc) {
			psclog_errorx("failed to load bmap %u",
			    sbd->sbd_bmapno);
			fcmh_op_done(f);
			psc_pool_return(bmap_rls_pool, brls);
			continue;
		}

		new = 1;
		bii = bmap_2_bii(b);
		PLL_FOREACH(tmpbrls, &bii->bii_rls) {
			if (!memcmp(&tmpbrls->bir_sbd, sbd, sizeof(*sbd))) {
				OPSTAT_INCR("bmap-duplicate");
				new = 0;
				break;
			}
		}
		if (new) {
			DEBUG_BMAP(PLL_DIAG, b, "brls=%p "
			    "seq=%"PRId64" key=%"PRId64,
			    brls, sbd->sbd_seq, sbd->sbd_key);

			pll_add(&bii->bii_rls, brls);
		} else
			psc_pool_return(bmap_rls_pool, brls);

		/*
		 * Sync to backing file system here to guarantee
		 * that buffers are flushed to disk before the
		 * telling the MDS to release its odtable entry
		 * for this bmap.
		 */
		sli_bmap_sync(b);

		bmap_op_done(b);
		fcmh_op_done(f);
	}
	psc_dynarray_reset(a);
}

int
sli_rmi_brelease_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	int rc;
	struct slashrpc_cservice *csvc = args->pointer_arg[SLI_CBARG_CSVC];

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_release_rep,
	    rc);

	sl_csvc_decref(csvc);
	return (0);
}

void
slibmaprlsthr_main(struct psc_thread *thr)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct srm_bmap_release_req brr, *mq;
	struct srm_bmap_release_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct bmap_iod_info *bii, *tmp;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_rls *brls;
	struct bmap *b;
	int nrls, rc, i, skip;

	psc_dynarray_ensurelen(&a, MAX_BMAP_RELEASE);

	while (pscthr_run(thr)) {

		slibmaprlsthr_process_releases(&a);

		skip = nrls = 0;
		LIST_CACHE_LOCK(&sli_bmap_releaseq);
		lc_peekheadwait(&sli_bmap_releaseq);
		LIST_CACHE_FOREACH_SAFE(bii, tmp, &sli_bmap_releaseq) {

			b = bii_2_bmap(bii);
			if (!BMAP_TRYLOCK(b)) {
				skip = 1;
				continue;
			}
			if (b->bcm_flags &BMAPF_TOFREE) {
				DEBUG_BMAP(PLL_DIAG, b,
				    "skip due to freeing");
				BMAP_ULOCK(b);
				continue;
			}
			if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				DEBUG_BMAP(PLL_DIAG, b,
				    "skip due to refcnt");
				BMAP_ULOCK(b);
				skip = 1;
				continue;
			}
			i = pll_nitems(&bii->bii_rls);
			DEBUG_BMAP(PLL_DIAG, b,
			    "returning %d bmap leases", i);
			while ((brls = pll_get(&bii->bii_rls))) {
				memcpy(&brr.sbd[nrls++],
				    &brls->bir_sbd,
				    sizeof(struct srt_bmapdesc));
				psc_pool_return(bmap_rls_pool, brls);
				if (nrls >= MAX_BMAP_RELEASE)
					break;
			}
			if (!pll_nitems(&bii->bii_rls)) {
				b->bcm_flags |= BMAPF_TOFREE;
				psc_dynarray_add(&a, b);
			}
			BMAP_ULOCK(b);

			if (nrls >= MAX_BMAP_RELEASE)
				break;
		}
		LIST_CACHE_ULOCK(&sli_bmap_releaseq);

		DYNARRAY_FOREACH(b, i, &a) {
			bii = bmap_2_bii(b);
			lc_remove(&sli_bmap_releaseq, bii);
			bmap_op_done_type(b, BMAP_OPCNT_REAPER);
		}

		psc_dynarray_reset(&a);

		if (!nrls) {
			if (skip) {
				sleep(1);
				continue;
			}
			spinlock(&sli_release_bmap_lock);
			psc_waitq_wait(&sli_release_bmap_waitq,
			    &sli_release_bmap_lock);
			freelock(&sli_release_bmap_lock);
			continue;
		}
			
		OPSTAT_INCR("bmap-release");

		brr.nbmaps = nrls;
		/*
		 * The system can tolerate the loss of these messages so
		 * errors here should not be fatal.
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

		memcpy(mq, &brr, sizeof(*mq));

		rq->rq_interpret_reply = sli_rmi_brelease_cb;
		rq->rq_async_args.pointer_arg[SLI_CBARG_CSVC] = csvc;

		rc = SL_NBRQSET_ADD(csvc, rq);
		if (rc) {
			psclog_errorx("RELEASEBMAP failed rc=%d", rc);
			pscrpc_req_finished(rq);
			sl_csvc_decref(csvc);
		}
	}
	psc_dynarray_free(&a);
}

void
slibmaprlsthr_spawn(void)
{
	int i;

	psc_waitq_init(&sli_release_bmap_waitq);

	lc_reginit(&sli_bmap_releaseq, struct bmap_iod_info, bii_lentry,
	    "breleaseq");

	pll_init(&sli_bii_rls, struct bmap_iod_rls, bir_lentry, NULL);

	for (i = 0; i < NBMAPRLS_THRS; i++) {
		pscthr_init(SLITHRT_BMAPRLS, slibmaprlsthr_main, NULL, 0,
		    "slibmaprlsthr%d", i);
	}
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

	/*
	 * XXX At some point we'll want to let bmaps hang around in the
	 * cache to prevent extra reads and CRC table fetches.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_REAPER);
	lc_addtail(&sli_bmap_releaseq, bii);
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
	NULL,				/* bmo_reapf() */
	iod_bmap_init,			/* bmo_init_privatef() */
	iod_bmap_retrieve,		/* bmo_retrievef() */
	NULL,				/* bmo_mode_chngf() */
	iod_bmap_finalcleanup		/* bmo_final_cleanupf() */
};
