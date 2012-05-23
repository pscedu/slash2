/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Update scheduler for site resources: this interface provides the
 * mechanism for managing updates such as truncation/deletion garbage
 * collection and replication activity to peer resources.
 */

#define PSC_SUBSYS SLMSS_UPSCH
#include "subsys_mds.h"

#include <sys/param.h>

#include <dirent.h>
#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/rpclog.h"
#include "psc_util/multiwait.h"
#include "psc_util/pthrutil.h"
#include "psc_util/random.h"
#include "psc_util/thread.h"

#include "bmap_mds.h"
#include "mdsio.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"
#include "slutil.h"
#include "up_sched_res.h"

struct upschedtree	 upsched_tree = SPLAY_INITIALIZER(&upsched_tree);
struct psc_poolmgr	*upsched_pool;
struct psc_lockedlist	 upsched_listhd =
    PLL_INIT(&upsched_listhd, struct up_sched_work_item,
	uswi_lentry);

/**
 * slmupschedthr_removeq - A file that required updates to be sent to
 *	I/O systems for a given site appears to be finished.  Do a quick
 *	run through the file ensuring there is truely nothing left to do
 *	and remove it from our processing queue if so.
 *
 * XXX this needs to remove any IOS' from ino_repls that are empty in
 *	all bmaps from the inode.
 */
void
slmupschedthr_removeq(struct up_sched_work_item *wk)
{
	int locked, uswi_gen, rc, retifset[NBREPLST];
	struct slmupsched_thread *smut;
	struct site_mds_info *smi;
	struct psc_thread *thr;
	struct bmapc_memb *b;
	struct sl_site *site;
	sl_bmapno_t n;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site2smi(site);

	locked = reqlock(&smi->smi_lock);
	psc_dynarray_remove(&smi->smi_upq, wk);
	ureqlock(&smi->smi_lock, locked);

	psc_mutex_reqlock(&wk->uswi_mutex);
	USWI_DECREF(wk, USWI_REFT_SITEUPQ);

	if (wk->uswi_flags & USWIF_DIE) {
		/* someone is already waiting for this to go away */
		uswi_unref(wk);
		return;
	}

	while (wk->uswi_flags & USWIF_BUSY) {
		psc_assert(psc_atomic32_read(&wk->uswi_refcnt) > 1);
		psc_multiwaitcond_wait(&wk->uswi_mwcond, &wk->uswi_mutex);
		psc_mutex_lock(&wk->uswi_mutex);
	}

 rescan:
	/*
	 * If someone bumps the generation while we're processing, we'll
	 * know there is work to do and that the uswi shouldn't go away.
	 */
	uswi_gen = wk->uswi_gen;
	wk->uswi_flags |= USWIF_BUSY;
	psc_mutex_unlock(&wk->uswi_mutex);

	/* Scan for any OLD states. */
	brepls_init(retifset, 1);
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;
	retifset[BREPLST_GARBAGE] = 0;

	/* Scan bmaps to see if the inode should disappear. */
	for (n = 0;; n++) {
		if (bmap_getf(wk->uswi_fcmh, n, SL_WRITE,
		    BMAPGETF_LOAD | BMAPGETF_NOAUTOINST, &b))
			break;

		rc = mds_repl_bmap_walk_all(b, NULL, retifset,
		    REPL_WALKF_SCIRCUIT);
		bmap_op_done(b);
		if (rc) {
			struct sl_resource *r;
			sl_replica_t ios;

			/* requeue XXX do it while bmap wr-locked */
			r = psc_dynarray_getpos(
			    &smut->smut_site->site_resources, 0);
			ios.bs_id = r->res_id;
			uswi_enqueue_sites(wk, &ios, 1);
			uswi_unref(wk);
			return;
		}
	}

	/*
	 * All states are INACTIVE/ACTIVE;
	 * remove it and its persistent link.
	 */
	UPSCHED_MGR_LOCK();
	psc_mutex_lock(&wk->uswi_mutex);
	if (wk->uswi_gen != uswi_gen) {
		UPSCHED_MGR_ULOCK();
		goto rescan;
	}
	if (!uswi_unref(wk))
		UPSCHED_MGR_ULOCK();
}

/**
 * uswi_trykill - Try to release a resource update scheduler work item.
 * This should be invoked by uswi_unref() only when there are no
 * remaining references to the structure (i.e. one for LOOKUP and one
 * for TREE membership of the structure) when it has been decided that
 * the uswi no longer needs to stay in memory (i.e. there is no more
 * work to be done by the update scheduler engine).
 * @wk: the little guy to try to release.
 */
__static int
uswi_trykill(struct up_sched_work_item *wk)
{
	char fn[FID_MAX_PATH];
	int rc;

	psc_mutex_ensure_locked(&wk->uswi_mutex);

	if (wk->uswi_flags & USWIF_DIE)
		return (0);
	wk->uswi_flags |= USWIF_DIE;
	wk->uswi_flags &= ~USWIF_BUSY;		/* XXX ensure we set this */

	USWI_ULOCK(wk);
	UPSCHED_MGR_RLOCK();
	USWI_LOCK(wk);
	if (psc_atomic32_read(&wk->uswi_refcnt) != 2) {
		UPSCHED_MGR_ULOCK();
		wk->uswi_flags &= ~USWIF_DIE;
		DEBUG_USWI(PLL_DEBUG, wk, "abort trykill");
		return (0);
	}
	USWI_DECREF(wk, USWI_REFT_LOOKUP);
	USWI_ULOCK(wk);
	UPSCHED_MGR_ULOCK();

	rc = snprintf(fn, sizeof(fn), SLPRI_FID, USWI_FID(wk));
	if (rc == -1)
		rc = errno;
	else if (rc >= (int)sizeof(fn))
		rc = ENAMETOOLONG;
	else
		rc = mdsio_unlink(mds_upschdir_inum, NULL, fn,
		    &rootcreds, NULL, NULL);
	if (rc)
		psclog_error("trying to remove upsch link: %s",
		    slstrerror(rc));

	UPSCHED_MGR_LOCK();
	USWI_LOCK(wk);

	while (psc_atomic32_read(&wk->uswi_refcnt) > 1) {
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
		UPSCHED_MGR_ULOCK();
		psc_multiwaitcond_wait(&wk->uswi_mwcond,
		    &wk->uswi_mutex);
		UPSCHED_MGR_LOCK();
		psc_mutex_lock(&wk->uswi_mutex);
	}

	PSC_SPLAY_XREMOVE(upschedtree, &upsched_tree, wk);
	pll_remove(&upsched_listhd, wk);
	UPSCHED_MGR_ULOCK();

	USWI_DECREF(wk, USWI_REFT_TREE);

	if (wk->uswi_fcmh)
		fcmh_op_done_type(wk->uswi_fcmh,
		    FCMH_OPCNT_LOOKUP_FIDC);
	USWI_ULOCK(wk);
	psc_mutex_destroy(&wk->uswi_mutex);
	psc_pool_return(upsched_pool, wk);
	return (1);
}

int
slmupschedthr_tryrepldst(struct up_sched_work_item *wk,
    struct bmapc_memb *b, int off, struct sl_resm *src_resm,
    struct sl_resource *dst_res, int j)
{
	int undo_write = 0, tract[NBREPLST], retifset[NBREPLST], rc = 0;
	struct resm_mds_info *src_rmmi, *dst_rmmi;
	struct pscrpc_request *rq = NULL;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct slashrpc_cservice *csvc;
	struct slmupsched_thread *smut;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;
	sl_bmapno_t lastbno;
	int64_t amt = 0;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site2smi(site);

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);

	dst_rmmi = resm2rmmi(dst_resm);
	src_rmmi = resm2rmmi(src_resm);

	csvc = slm_geticsvcf(dst_resm, CSVCF_NONBLOCK | CSVCF_NORECON);

	/*
	 * At this point, add this connection to our multiwait.
	 *
	 * If we schedule work here, we don't go into this multiwait;
	 * we'll try another iteration of more scheduling.
	 *
	 * If a wakeup event comes while we are timing our own
	 * establishment attempt below, we will wake up immediately when
	 * we multiwait; otherwise, the connection will wake us when it
	 * becomes available.
	 */
	if (!psc_multiwait_hascond(&smi->smi_mw,
	    &dst_resm->resm_csvc->csvc_mwc))
		psc_multiwait_addcond(&smi->smi_mw,
		    &dst_resm->resm_csvc->csvc_mwc);

	if (csvc == NULL)
		PFL_GOTOERR(fail, rc = SLERR_ION_OFFLINE);

	amt = mds_repl_nodes_adjbusy(src_resm, dst_resm,
	    slm_bmap_calc_repltraffic(b));
	if (amt == 0) {
		struct slashrpc_cservice *src_csvc;

		src_csvc = slm_geticsvcf(src_resm, CSVCF_NONBLOCK | CSVCF_NORECON); 
		if (src_csvc)
			sl_csvc_decref(src_csvc);

		/* add "src to become unbusy" condition to multiwait */
		if (!psc_multiwait_hascond(&smi->smi_mw,
		    &src_resm->resm_csvc->csvc_mwc))
			psc_multiwait_addcond(&smi->smi_mw,
			    &src_resm->resm_csvc->csvc_mwc);
		PFL_GOTOERR(fail, rc);
	}

	/* Issue replication work request */
	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(fail, rc);
	mq->src_resid = src_resm->resm_res_id;
	mq->len = SLASH_BMAP_SIZE;
	lastbno = fcmh_nvalidbmaps(wk->uswi_fcmh);
	if (lastbno > 0)
		lastbno--;
	if (b->bcm_bmapno == lastbno) {
		uint64_t sz;

		sz = fcmh_getsize(wk->uswi_fcmh);
		if (sz > b->bcm_bmapno * (uint64_t)SLASH_BMAP_SIZE)
			sz -= b->bcm_bmapno * SLASH_BMAP_SIZE;
		if (sz == 0)
			PFL_GOTOERR(fail, rc = ENODATA);

		mq->len = MIN(sz, SLASH_BMAP_SIZE);
	}
	mq->fg = *USWI_FG(wk);
	mq->bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &mq->bgen);

	/*
	 * Mark as SCHED now in case the reply RPC comes in after we
	 * finish here.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	/*
	 * If it was still QUEUED, which means we marked it SCHED, then
	 * proceed; otherwise, bail: perhaps the user dequeued the
	 * replication request or something.
	 */
	if (rc == BREPLST_REPL_QUEUED) {
		rc = mds_bmap_write_logrepls(b);
		undo_write = 1;
		/*
		 * It is OK if repl sched is resent across reboots
		 * idempotently.
		 */
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc || (mp && mp->rc)) {
			DEBUG_REQ(PLL_ERROR, rq, "dst_resm=%s src_resm=%s rc=%d mp->rc=%d (RPC)",
			    dst_resm->resm_name, src_resm->resm_name, rc, mp ? mp->rc : 0);

			DEBUG_BMAP(PLL_ERROR, b,
			    "dst_resm=%s src_resm=%s rc=%d mp->rc=%d (RPC)",
			    dst_resm->resm_name, src_resm->resm_name, rc, mp ? mp->rc : 0);

			if (rc == 0 && mp)
				rc = mp->rc;
		}

	} else {
		DEBUG_BMAP(PLL_ERROR, b,
		    "dst_resm=%s src_resm=%s rc=%d (mds_repl_bmap_apply)",
		    dst_resm->resm_name, src_resm->resm_name, rc);
		rc = ENODEV;
	}
	if (rc == 0) {
		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
		uswi_unref(wk);
		bmap_op_done(b);
		return (1);
	}

	/* handle error return failure & undo */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	mds_repl_bmap_apply(b, tract, NULL, off);
	if (undo_write)
		mds_bmap_write_logrepls(b);

 fail:
	if (amt)
		mds_repl_nodes_adjbusy(src_resm, dst_resm, -amt);
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc) {
		DEBUG_USWI(rc == SLERR_ION_OFFLINE ? PLL_INFO : PLL_WARN,
		    wk, "replication arrangement failed (dst=%s, src=%s) rc=%d",
		    dst_resm->resm_name, src_resm->resm_name, rc);
		DEBUG_BMAP(rc == SLERR_ION_OFFLINE ? PLL_INFO : PLL_WARN,
		    b, "replication arrangement failed (dst=%s, src=%s) rc=%d",
		    dst_resm->resm_name, src_resm->resm_name, rc);
	}
	return (0);
}

int
slmupschedthr_tryptrunc(struct up_sched_work_item *wk,
    struct bmapc_memb *b, int off, struct sl_resource *dst_res,
    int idx)
{
	int undo_write = 0, tract[NBREPLST], retifset[NBREPLST], rc;
	struct slmupsched_thread *smut;
	struct slashrpc_cservice *csvc;
	struct resm_mds_info *dst_rmmi;
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct pscrpc_request *rq;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site2smi(site) ;

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, idx);
	dst_rmmi = resm2rmmi(dst_resm);

	brepls_init(retifset, 0);
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;

	/*
	 * Another ION is already handling the ptrunc CRC recomputation;
	 * go do something else.
	 */
	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		return (-1);

	csvc = slm_geticsvcf(dst_resm, CSVCF_NONBLOCK | CSVCF_NORECON);
	if (csvc == NULL) {
		if (!psc_multiwait_hascond(&smi->smi_mw,
		    &dst_resm->resm_csvc->csvc_mwc))
			psc_multiwait_addcond(&smi->smi_mw,
			    &dst_resm->resm_csvc->csvc_mwc);
		return (0);
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		goto fail;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = b->bcm_bmapno;
	mq->offset = fcmh_2_fsz(wk->uswi_fcmh) % SLASH_BMAP_SIZE;

	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;
	mds_repl_bmap_apply(b, tract, NULL, off);
	rc = mds_bmap_write_logrepls(b);
	undo_write = 1;

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);

	if (rc == 0) {
		uswi_unref(wk);
		sl_csvc_decref(csvc);
		bmap_op_done(b);
		return (1);
	}

	/* handle error return failure & undo */
	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;
	mds_repl_bmap_apply(b, tract, NULL, off);
	if (undo_write)
		mds_bmap_write_logrepls(b);

 fail:
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc)
		psclog_warnx("partial truncation failed rc=%d", rc);
	return (0);
}

int
slmupschedthr_trygarbage(struct up_sched_work_item *wk,
    struct bmapc_memb *b, int off, struct sl_resource *dst_res, int j)
{
	int undo_write = 0, tract[NBREPLST], retifset[NBREPLST], rc = 0;
	struct slashrpc_cservice *csvc;
	struct slmupsched_thread *smut;
	struct resm_mds_info *dst_rmmi;
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct pscrpc_request *rq;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site2smi(site);

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);
	dst_rmmi = resm2rmmi(dst_resm);

	csvc = slm_geticsvcf(dst_resm, CSVCF_NONBLOCK | CSVCF_NORECON);

	/*
	 * At this point, add this connection to our multiwait.
	 * If a wakeup event comes while we are timing our own
	 * establishment attempt below, we will wake up immediately
	 * when we multiwait; otherwise, we the connection will
	 * wake us when it becomes available.
	 */
	if (!psc_multiwait_hascond(&smi->smi_mw,
	    &dst_resm->resm_csvc->csvc_mwc))
		psc_multiwait_addcond(&smi->smi_mw,
		    &dst_resm->resm_csvc->csvc_mwc);

	if (csvc == NULL)
		goto fail;

	/* Issue garbage reclaim request */
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		goto fail;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = b->bcm_bmapno;
	mq->bgen = bmap_2_bgen(b);

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE] = BREPLST_GARBAGE_SCHED;
	tract[BREPLST_INVALID] = BREPLST_GARBAGE_SCHED;

	brepls_init_idx(retifset);

	/*
	 * Mark it as SCHED here in case the RPC finishes really
	 * quickly...
	 */
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	if (rc == BREPLST_GARBAGE ||
	    rc == BREPLST_INVALID) {
		rc = mds_bmap_write_logrepls(b);
		undo_write = 1;
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	}
	pscrpc_req_finished(rq);
	if (rc == 0) {
		uswi_unref(wk);
		sl_csvc_decref(csvc);
		bmap_op_done(b);
		return (1);
	}

	/* handle error return failure & undo */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;
	mds_repl_bmap_apply(b, tract, NULL, off);
	if (undo_write)
		mds_bmap_write_logrepls(b);

 fail:
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc)
		psclog_warnx("garbage reclamation failed rc=%d", rc);
	return (0);
}

void
slmupschedthr_main(struct psc_thread *thr)
{
	int ngar, uswi_gen, iosidx, off, rc, has_work, val, tryarchival;
	struct rnd_iterator src_resm_i, dst_resm_i, bmap_i;
	struct rnd_iterator wk_i, src_res_i, dst_res_i;
	struct sl_resource *src_res, *dst_res;
	struct slmupsched_thread *smut;
	struct slashrpc_cservice *csvc;
	struct up_sched_work_item *wk;
	struct site_mds_info *smi;
	struct bmapc_memb *b, *bn;
	struct sl_resm *src_resm;
	struct sl_site *site;
	struct fidc_membh *f;
	sl_bmapno_t bno;
	void *dummy;

	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site2smi(site);
	while (pscthr_run()) {
		if (0)
 restart:
			sched_yield();

		/* select or wait for a repl rq */
		reqlock(&smi->smi_lock);

		psc_multiwait_reset(&smi->smi_mw);
		if (psc_multiwait_addcond(&smi->smi_mw,
		    &smi->smi_mwcond) == -1)
			psc_fatal("psc_multiwait_addcond");
		psc_multiwait_entercritsect(&smi->smi_mw);

		if (psc_dynarray_len(&smi->smi_upq) == 0) {
			freelock(&smi->smi_lock);
			psc_multiwait(&smi->smi_mw, &dummy);
			continue;
		}

		FOREACH_RND(&wk_i, psc_dynarray_len(&smi->smi_upq)) {
			reqlock(&smi->smi_lock);
			if (wk_i.ri_rnd_idx >=
			    psc_dynarray_len(&smi->smi_upq))
				goto restart;

			wk = psc_dynarray_getpos(&smi->smi_upq,
			    wk_i.ri_rnd_idx);
			USWI_INCREF(wk, USWI_REFT_LOOKUP);
			freelock(&smi->smi_lock);

			rc = uswi_access_lock(wk);
			if (rc == 0) {
				/* repl must be going away, drop it */
				slmupschedthr_removeq(wk);
				goto restart;
			}

			f = wk->uswi_fcmh;

#if 0
			rc = mds_inox_ensure_loaded(USWI_INOH(wk));
			if (rc) {
				psclog_warnx("couldn't load inoh repl table: %s",
				    slstrerror(rc));
				slmupschedthr_removeq(wk);
				goto restart;
			}
#endif

			has_work = 0;

			psc_mutex_lock(&wk->uswi_mutex);
			uswi_gen = wk->uswi_gen;
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_mutex_unlock(&wk->uswi_mutex);

			/* find a res in our site this uswi is destined for */
			iosidx = -1;
			FOREACH_RND(&dst_res_i,
			    psc_dynarray_len(&site->site_resources)) {
				dst_res = psc_dynarray_getpos(
				    &site->site_resources,
				    dst_res_i.ri_rnd_idx);
				iosidx = mds_repl_ios_lookup(
				    USWI_INOH(wk), dst_res->res_id);
				if (iosidx < 0)
					continue;
				off = SL_BITS_PER_REPLICA * iosidx;

				FCMH_LOCK(f);
				if (f->fcmh_flags & FCMH_IN_PTRUNC) {
 try_ptrunc:
					has_work = 1;

					/*
					 * Rig "random" index at
					 * ptrunc'd bmap to schedule CRC
					 * recalc.
					 */
					bmap_i.ri_n = fcmh_nvalidbmaps(f);
					bmap_i.ri_iter = bmap_i.ri_n - 1;
					bmap_i.ri_rnd_idx = howmany(
					    fcmh_2_fsz(f),
					    SLASH_BMAP_SIZE) - 1;
//					uswi_gen = wk->uswi_gen;
					FCMH_ULOCK(f);

					/*
					 * Indicate error so the random
					 * iterator gets reset if we
					 * can't schedule it.
					 */
					rc = 1;
					PFL_GOTOERR(handle_bmap, 1);
				}
				FCMH_ULOCK(f);

				/*
				 * Select random bmap then scan
				 * sequentially to check for work.
				 */
				FOREACH_RND(&bmap_i,
				    fcmh_nvalidbmaps(f)) {
					if (uswi_gen != wk->uswi_gen)
						PFL_GOTOERR(skipfile, 1);
					rc = 0;
 handle_bmap:
					if (mds_bmap_load(f,
					    bmap_i.ri_rnd_idx, &b))
						continue;

					BMAPOD_MODIFY_START(b);
					val = SL_REPL_GET_BMAP_IOS_STAT(
					    b->bcm_repls, off);
					switch (val) {
					case BREPLST_REPL_QUEUED:
						has_work = 1;
						/*
						 * There is still a
						 * lease out; we'll wait
						 * for it to be
						 * relinquished.
						 *
						 * XXX: make sure lease
						 * drops wake us up.
						 */
						if (bmap_2_bmi(b)->bmdsi_wr_ion)
							break;

						for (tryarchival = 0;
						    tryarchival < 2;
						    tryarchival++) {

						/* got a bmap; now look for a repl source */
						FOREACH_RND(&src_res_i, USWI_NREPLS(wk)) {
							if (uswi_gen != wk->uswi_gen) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
								PFL_GOTOERR(skipfile, 1);
							}
							src_res = libsl_id2res(
							    USWI_GETREPL(wk,
							    src_res_i.ri_rnd_idx).bs_id);

							/* skip ourself and old/inactive replicas */
							if (src_res_i.ri_rnd_idx == iosidx ||
							    SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls,
							    SL_BITS_PER_REPLICA *
							    src_res_i.ri_rnd_idx) != BREPLST_VALID)
								continue;

							if (tryarchival ^
							    (src_res->res_type == SLREST_ARCHIVAL_FS))
								continue;

							BMAPOD_MODIFY_DONE(b);

							/*
							 * Search source nodes for an
							 * idle, online connection.
							 */
							FOREACH_RND(&src_resm_i,
							    psc_dynarray_len(&src_res->res_members)) {
								src_resm = psc_dynarray_getpos(
								    &src_res->res_members,
								    src_resm_i.ri_rnd_idx);
								csvc = slm_geticsvc_nb(src_resm, NULL);
								if (csvc == NULL) {
									if (!psc_multiwait_hascond(&smi->smi_mw,
									    &src_resm->resm_csvc->csvc_mwc))
										if (psc_multiwait_addcond(&smi->smi_mw,
										    &src_resm->resm_csvc->csvc_mwc))
											psc_fatal("multiwait_addcond");
									continue;
								}
								sl_csvc_decref(csvc);

								/* scan destination resms */
								FOREACH_RND(&dst_resm_i,
								    psc_dynarray_len(&dst_res->res_members))
									if (slmupschedthr_tryrepldst(wk,
									    b, off, src_resm, dst_res,
									    dst_resm_i.ri_rnd_idx))
										PFL_GOTOERR(restart, 1);
							}
							BMAPOD_MODIFY_START(b);
						}
						}
						break;
					case BREPLST_TRUNCPNDG:
						has_work = 1;
						BMAPOD_MODIFY_DONE(b);
						FOREACH_RND(&dst_resm_i,
						    psc_dynarray_len(&dst_res->res_members)) {
							rc = slmupschedthr_tryptrunc(wk,
							    b, off, dst_res,
							    dst_resm_i.ri_rnd_idx);
							if (rc < 0)
								break;
							if (rc > 0)
								PFL_GOTOERR(restart, 1);
						}
						BMAPOD_MODIFY_START(b);
						break;
					case BREPLST_GARBAGE:
						ngar = 0;
						/*
						 * We found garbage.  We
						 * must scan from EOF
						 * toward the beginning of
						 * the file since we
						 * can't reclaim garbage
						 * in the middle of a
						 * file.
						 */
						bno = fcmh_nallbmaps(f);
						if (bno)
							bno--;
						if (b->bcm_bmapno != bno) {
							BMAPOD_MODIFY_DONE(b);
							bmap_op_done(b);

							if (f->fcmh_flags & FCMH_IN_PTRUNC)
								PFL_GOTOERR(try_ptrunc, 1);
							if (uswi_gen != wk->uswi_gen)
								PFL_GOTOERR(skipfile, 1);
							if (mds_bmap_load(f,
							    bno, &b))
								continue;
							BMAPOD_MODIFY_START(b);
						}
						val = BREPLST_INVALID;
						for (bn = b, b = NULL;; ) {
							val = SL_REPL_GET_BMAP_IOS_STAT(
							    bn->bcm_repls, off);
							if (val == BREPLST_GARBAGE)
								ngar++;
							if ((val != BREPLST_GARBAGE &&
							    val != BREPLST_INVALID) ||
							    bno == 0)
								break;

							if (b) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
							}
							b = bn;
							bn = NULL;

							if (f->fcmh_flags & FCMH_IN_PTRUNC) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
								PFL_GOTOERR(try_ptrunc, 1);
							}
							if (uswi_gen != wk->uswi_gen) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
								PFL_GOTOERR(skipfile, 1);
							}

							bno--;
							if (mds_bmap_load(f,
							    bno, &bn))
								continue;
							BMAPOD_MODIFY_START(bn);
						}

						if (bn == NULL) {
							bn = b;
							b = NULL;
						}

						if (val == BREPLST_GARBAGE_SCHED ||
						    ngar == 0) {
							if ((int)bno < bmap_i.ri_n)
								bmap_i.ri_n = bno;
							if (b) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
							}
							b = bn;
							break;
						}
						if (b == NULL &&
						    val != BREPLST_GARBAGE) {
							b = bn;
							break;
						}
						has_work = 1;

						if (val == BREPLST_GARBAGE ||
						    val == BREPLST_INVALID) {
							psc_assert(bno == 0);
							if (b) {
								BMAPOD_MODIFY_DONE(b);
								bmap_op_done(b);
							}
							b = bn;
						} else {
							BMAPOD_MODIFY_DONE(bn);
							bmap_op_done(bn);
						}

						BMAPOD_MODIFY_DONE(b);
						FOREACH_RND(&dst_resm_i,
						    psc_dynarray_len(&dst_res->res_members))
							/*
							 * We succeed as long as one member
							 * can do the work because all
							 * members share the same backend.
							 * XXX cluster_noshare.
							 */
							if (slmupschedthr_trygarbage(wk,
							    b, off, dst_res,
							    dst_resm_i.ri_rnd_idx))
								PFL_GOTOERR(restart, 1);
						BMAPOD_MODIFY_START(b);
						break;
					case BREPLST_REPL_SCHED:
					case BREPLST_TRUNCPNDG_SCHED:
					case BREPLST_GARBAGE_SCHED:
						/*
						 * We must keep the wkrq
						 * in mem so
						 * repl_reset_scheduled()
						 * can revert the state.
						 */
						has_work = 1;
						break;
					}
					BMAPOD_MODIFY_DONE(b);
					bmap_op_done(b);
					if (rc)
						RESET_RND_ITER(&bmap_i);
				}
			}

 skipfile:
			/*
			 * At this point, we did not find a
			 * bmap, src/dst resource, etc. involving our
			 * site needed by this uswi.
			 */
			psc_mutex_lock(&wk->uswi_mutex);
			if (has_work || wk->uswi_gen != uswi_gen) {
				psc_multiwait_addcond_masked(&smi->smi_mw,
				    &wk->uswi_mwcond, wk->uswi_gen != uswi_gen);
				psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
				psc_multiwait_setcondwakeable(&smi->smi_mw,
				    &wk->uswi_mwcond, 1);
				uswi_unref_nowake(wk);
			} else {
				slmupschedthr_removeq(wk);
				goto restart;
			}
		}
		psc_multiwait(&smi->smi_mw, &dummy);
		/*
		 * XXX look at the event and process it directly
		 * instead of doing all this work again.
		 */
	}
}

void
slmupschedthr_spawnall(void)
{
	struct slmupsched_thread *smut;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	locked = CONF_RLOCK();
	CONF_FOREACH_SITE(site) {
		thr = pscthr_init(SLMTHRT_UPSCHED, 0,
		    slmupschedthr_main, NULL, sizeof(*smut),
		    "slmupschedthr-%s", site->site_name +
		    strspn(site->site_name, "@"));
		smut = slmupschedthr(thr);
		smut->smut_site = site;
		pscthr_setready(thr);
	}
	CONF_URLOCK(locked);
}

/**
 * _uswi_access - Obtain processing access to a update_scheduler
 *	request.  This routine assumes the refcnt has already been
 *	bumped.
 * @wk: update_scheduler request to access, locked on return.
 * Returns Boolean true on success or false if the request is going
 * away.
 */
int
_uswi_access(struct up_sched_work_item *wk, int keep_locked)
{
	int locked, rc = 1;

	USWI_RLOCK(wk);

	/* Wait for someone else to finish processing. */
	locked = PLL_HASLOCK(&upsched_listhd);
	USWI_WAIT(wk, locked);
	if (wk->uswi_flags & USWIF_DIE) {
		/* Release if going away. */
		USWI_DECREF(wk, USWI_REFT_LOOKUP);
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
		rc = 0;

		if (!keep_locked)
			psc_mutex_unlock(&wk->uswi_mutex);
	} else {
		wk->uswi_flags |= USWIF_BUSY;
		USWI_ULOCK(wk);
	}
	return (rc);
}

int
_uswi_unref(const struct pfl_callerinfo *pci,
    struct up_sched_work_item *wk, int wake)
{
	psc_mutex_reqlock(&wk->uswi_mutex);
	wk->uswi_flags &= ~USWIF_BUSY;

	if (psc_atomic32_read(&wk->uswi_refcnt) == 2 &&
	    uswi_trykill(wk))
		return (1);

	USWI_DECREF(wk, USWI_REFT_LOOKUP);
	if (wake)
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
	psc_mutex_unlock(&wk->uswi_mutex);
	return (0);
}

struct up_sched_work_item *
uswi_find(const struct slash_fidgen *fgp)
{
	struct up_sched_work_item q, *wk;
	struct fidc_membh fcmh;
	int locked;

	fcmh.fcmh_fg = *fgp;
	q.uswi_fcmh = &fcmh;

	locked = UPSCHED_MGR_RLOCK();
	wk = SPLAY_FIND(upschedtree, &upsched_tree, &q);
	if (wk == NULL) {
		UPSCHED_MGR_URLOCK(locked);
		return (NULL);
	}
	psc_mutex_lock(&wk->uswi_mutex);
	USWI_INCREF(wk, USWI_REFT_LOOKUP);
	UPSCHED_MGR_URLOCK(locked);

	/* uswi_access() drops the refcnt on failure */
	if (uswi_access(wk))
		return (wk);
	return (NULL);
}

int
uswi_getslfid(__unusedx slfid_t *fidp)
{
	/* XXX should I be FID_ANY? */
	*fidp = 0;
	return (0);
}

/**
 * uswi_init - Initialize an update scheduler work item (uswi).
 */
void
uswi_init(struct up_sched_work_item *wk, slfid_t fid)
{
	memset(wk, 0, sizeof(*wk));
	INIT_PSC_LISTENTRY(&wk->uswi_lentry);
	wk->uswi_flags |= USWIF_BUSY;
	psc_mutex_init(&wk->uswi_mutex);
	psc_multiwaitcond_init(&wk->uswi_mwcond,
	    NULL, 0, "upsched-%016"SLPRIxFID, fid);
	psc_atomic32_set(&wk->uswi_refcnt, 0);
}

void
upsched_scandir(void)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	sl_replica_t iosv[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	struct fidc_membh *fcmh;
	struct slash_fidgen fg;
	struct pscfs_dirent *d;
	struct bmapc_memb *b;
	char *buf, fn[NAME_MAX];
	off64_t off, toff;
	size_t siz, tsiz;
	uint32_t j;
	void *data;

	rc = mdsio_opendir(mds_upschdir_inum, &rootcreds, NULL, &data);
	if (rc)
		psc_fatalx("mdsio_opendir %s: %s", SL_RPATH_UPSCH_DIR,
		    slstrerror(rc));

	off = 0;
	siz = 8 * 1024;
	buf = PSCALLOC(siz);

	for (;;) {
		rc = mdsio_readdir(&rootcreds, siz,
			   off, buf, &tsiz, NULL, NULL, 0, data);
		if (rc)
			psc_fatalx("mdsio_readdir %s: %s",
			    SL_RPATH_UPSCH_DIR, slstrerror(rc));
		if (tsiz == 0)
			break;
		for (toff = 0; toff < (off64_t)tsiz;
		    toff += PFL_DIRENT_SIZE(d->pfd_namelen)) {
			d = (void *)(buf + toff);
			off = d->pfd_off;

			if (strlcpy(fn, d->pfd_name, sizeof(fn)) >
			    sizeof(fn))
				psc_assert("impossible");
			if (d->pfd_namelen < sizeof(fn))
				fn[d->pfd_namelen] = '\0';

			if (fn[0] == '.')
				continue;

			memset(&fg, 0, sizeof(fg));
			fg.fg_fid = strtoll(fn, NULL, 16);
			fg.fg_gen = FGEN_ANY;

			rc = mds_repl_loadino(&fg, &fcmh);
			if (rc) {
				/* XXX if ENOENT, remove from repldir and continue */
				psclog_errorx("mds_repl_loadino: %s",
				    slstrerror(rc));
				mdsio_unlink(mds_upschdir_inum, NULL, fn,
				    &rootcreds, NULL, NULL);
				continue;
			}

			rc = uswi_findoradd(&fg, &wk);
			if (rc)
				psc_fatal("uswi_findoradd: %s",
				    slstrerror(rc));

			psc_mutex_lock(&wk->uswi_mutex);
			wk->uswi_fcmh = fcmh;
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_mutex_unlock(&wk->uswi_mutex);

			brepls_init(tract, -1);
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;
			tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;

			brepls_init(retifset, 0);
			retifset[BREPLST_REPL_SCHED] = 1;
			retifset[BREPLST_TRUNCPNDG_SCHED] = 1;
			retifset[BREPLST_GARBAGE_SCHED] = 1;

			/*
			 * If we crashed, revert all inflight SCHED'ed
			 * bmaps so they get resent.
			 */
			for (j = 0;; j++) {
				if (bmap_getf(wk->uswi_fcmh, j,
				    SL_WRITE, BMAPGETF_LOAD |
				    BMAPGETF_NOAUTOINST, &b))
					break;

				if (mds_repl_bmap_walk(b, tract,
				    retifset, 0, NULL, 0))
					mds_bmap_write_repls_rel(b);
				else
					bmap_op_done(b);
			}

			/*
			 * Requeue pending updates on all registered
			 * sites.  If there is no work to do, it will be
			 * promptly removed by the slmupschedthr.
			 */
			for (j = 0; j < USWI_NREPLS(wk); j++)
				iosv[j].bs_id = USWI_GETREPL(wk, j).bs_id;
			uswi_enqueue_sites(wk, iosv, USWI_NREPLS(wk));
			uswi_unref(wk);

			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		}
		off += tsiz;
	}
	rc = mdsio_release(&rootcreds, data);
	if (rc)
		psc_fatalx("mdsio_release %s: %s", SL_RPATH_UPSCH_DIR,
		    slstrerror(rc));

	PSCFREE(buf);
}

int
uswi_findoradd(const struct slash_fidgen *fgp,
    struct up_sched_work_item **wkp)
{
	struct up_sched_work_item *newrq = NULL;
	char fn[PATH_MAX];
	void *mdsio_data;
	int rc = 0;

	*wkp = uswi_find(fgp);
	if (*wkp)
		goto out;

	newrq = psc_pool_get(upsched_pool);
	uswi_init(newrq, fgp->fg_fid);

	rc = mds_repl_loadino(fgp, &newrq->uswi_fcmh);
	if (rc)
		goto out;
	if (fcmh_isdir(newrq->uswi_fcmh)) {
		rc = EISDIR;
		goto out;
	}

	rc = snprintf(fn, sizeof(fn), SLPRI_FID, fgp->fg_fid);
	if (rc == -1) {
		rc = errno;
		goto out;
	}
	if (rc >= (int)sizeof(fn)) {
		rc = ENAMETOOLONG;
		goto out;
	}

	rc = mdsio_opencreatef(mds_upschdir_inum, &rootcreds,
	    O_CREAT | O_WRONLY, MDSIO_OPENCRF_NOLINK, 0600, fn,
	    NULL, NULL, &mdsio_data, NULL, uswi_getslfid, 0);
	if (rc)
		goto out;
	mdsio_release(&rootcreds, mdsio_data);

	UPSCHED_MGR_LOCK();
	*wkp = uswi_find(fgp);
	if (*wkp) {
		if (UPSCHED_MGR_HASLOCK())
			UPSCHED_MGR_ULOCK();
		fcmh_op_done_type(newrq->uswi_fcmh,
		    FCMH_OPCNT_LOOKUP_FIDC);
		goto out;
	}

	psc_mutex_lock(&newrq->uswi_mutex);
	USWI_INCREF(newrq, USWI_REFT_TREE);
	USWI_INCREF(newrq, USWI_REFT_LOOKUP);

	SPLAY_INSERT(upschedtree, &upsched_tree, newrq);
	pll_addtail(&upsched_listhd, newrq);
	UPSCHED_MGR_ULOCK();

	psc_mutex_unlock(&newrq->uswi_mutex);

	*wkp = newrq;
	newrq = NULL;

 out:
	if (rc && newrq && newrq->uswi_fcmh)
		fcmh_op_done_type(newrq->uswi_fcmh,
		    FCMH_OPCNT_LOOKUP_FIDC);

	if (newrq)
		psc_pool_return(upsched_pool, newrq);

	return (rc);
}

void
uswi_enqueue_sites(struct up_sched_work_item *wk,
    const sl_replica_t *iosv, int nios)
{
	struct site_mds_info *smi;
	struct sl_site *site;
	int locked, n;

	locked = psc_mutex_reqlock(&wk->uswi_mutex);
	wk->uswi_gen++;
	for (n = 0; n < nios; n++) {
		site = libsl_resid2site(iosv[n].bs_id);
		smi = site2smi(site);

		spinlock(&smi->smi_lock);
		if (!psc_dynarray_exists(&smi->smi_upq, wk)) {
			psc_dynarray_add(&smi->smi_upq, wk);
			USWI_INCREF(wk, USWI_REFT_SITEUPQ);
		}
		psc_multiwaitcond_wakeup(&smi->smi_mwcond);
		freelock(&smi->smi_lock);
	}
	psc_mutex_ureqlock(&wk->uswi_mutex, locked);
}

void
dump_uswi(struct up_sched_work_item *wk)
{
	DEBUG_USWI(PLL_MAX, wk, "");
}
