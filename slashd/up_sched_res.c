/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/param.h>

#include <dirent.h>
#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rsx.h"
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
int			 upsched_gen;
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
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_site *site;
	char fn[FID_MAX_PATH];
	sl_bmapno_t n;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site->site_pri;

	locked = reqlock(&smi->smi_lock);
	smi->smi_flags |= SMIF_DIRTYQ;
	psc_dynarray_remove(&smi->smi_upq, wk);
	ureqlock(&smi->smi_lock, locked);

	psc_pthread_mutex_reqlock(&wk->uswi_mutex);
	USWI_DECREF(wk, USWI_REFT_SITEUPQ);

	if (wk->uswi_flags & USWIF_DIE) {
		/* someone is already waiting for this to go away */
		uswi_unref(wk);
		return;
	}

	while (wk->uswi_flags & USWIF_BUSY) {
		psc_assert(psc_atomic32_read(&wk->uswi_refcnt) > 1);
		psc_multiwaitcond_wait(&wk->uswi_mwcond, &wk->uswi_mutex);
		psc_pthread_mutex_lock(&wk->uswi_mutex);
	}

	/*
	 * If someone bumps the generation while we're processing, we'll
	 * know there is work to do and that the uswi shouldn't go away.
	 */
	uswi_gen = wk->uswi_gen;
	wk->uswi_flags |= USWIF_BUSY;
	psc_pthread_mutex_unlock(&wk->uswi_mutex);

	/* Scan for any OLD states. */
	brepls_init(retifset, 1);
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;

	/* Scan bmaps to see if the inode should disappear. */
	for (n = 0; n < USWI_NBMAPS(wk); n++) {
		if (mds_bmap_load(wk->uswi_fcmh, n, &bcm))
			continue;

		rc = mds_repl_bmap_walk_all(bcm, NULL,
		    retifset, REPL_WALKF_SCIRCUIT);
		mds_repl_bmap_rel(bcm);
		if (rc)
			goto keep;
	}

	UPSCHED_MGR_LOCK();
	psc_pthread_mutex_lock(&wk->uswi_mutex);
	if (wk->uswi_gen != uswi_gen) {
		UPSCHED_MGR_ULOCK();
 keep:
		uswi_unref(wk);
		return;
	}

	/*
	 * All states are INACTIVE/ACTIVE;
	 * remove it and its persistent link.
	 */
	rc = snprintf(fn, sizeof(fn), SLPRI_FID, USWI_FID(wk));
	if (rc == -1)
		rc = errno;
	else if (rc >= (int)sizeof(fn))
		rc = ENAMETOOLONG;
	else
		rc = mdsio_unlink(mds_upschdir_inum, fn, &rootcreds,
		    NULL);
	if (rc)
		psc_error("trying to remove upsch link: %s",
		    slstrerror(rc));
	uswi_kill(wk);
}

__static void
uswi_kill(struct up_sched_work_item *wk)
{
	psc_pthread_mutex_ensure_locked(&wk->uswi_mutex);

	UPSCHED_MGR_ENSURE_LOCKED();
	PSC_SPLAY_XREMOVE(upschedtree, &upsched_tree, wk);
	pll_remove(&upsched_listhd, wk);
	UPSCHED_MGR_ULOCK();

	USWI_DECREF(wk, USWI_REFT_TREE);

	wk->uswi_flags |= USWIF_DIE;
	wk->uswi_flags &= ~USWIF_BUSY;

	while (psc_atomic32_read(&wk->uswi_refcnt) > 1) {
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
		psc_multiwaitcond_wait(&wk->uswi_mwcond,
		    &wk->uswi_mutex);
		psc_pthread_mutex_lock(&wk->uswi_mutex);
	}

	if (wk->uswi_fcmh)
		fcmh_op_done_type(wk->uswi_fcmh,
		    FCMH_OPCNT_LOOKUP_FIDC);

	psc_pool_return(upsched_pool, wk);
}

int
slmupschedthr_tryrepldst(struct up_sched_work_item *wk,
    struct bmapc_memb *bcm, int off, struct sl_resm *src_resm,
    struct sl_resource *dst_res, int j)
{
	int tract[NBREPLST], retifset[NBREPLST], amt = 0, rc = 0;
	struct resm_mds_info *src_rmmi, *dst_rmmi;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct slashrpc_cservice *csvc;
	struct slmupsched_thread *smut;
	struct pscrpc_request *rq;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;

	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site->site_pri;

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);

	dst_rmmi = dst_resm->resm_pri;
	src_rmmi = src_resm->resm_pri;

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
	    &dst_rmmi->rmmi_mwcond))
		psc_multiwait_addcond(&smi->smi_mw,
		    &dst_rmmi->rmmi_mwcond);

	csvc = slm_geticsvc_nb(dst_resm, NULL);
	if (csvc == NULL)
		goto fail;

	amt = mds_repl_nodes_adjbusy(src_rmmi, dst_rmmi,
	    slm_bmap_calc_repltraffic(bcm));
	if (amt == 0) {
		/* add "src to become unbusy" condition to multiwait */
		if (!psc_multiwait_hascond(&smi->smi_mw,
		    &src_rmmi->rmmi_mwcond))
			psc_multiwait_addcond(&smi->smi_mw,
			    &src_rmmi->rmmi_mwcond);
		goto fail;
	}

	/* Issue replication work request */
	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		goto fail;
	mq->nid = src_resm->resm_nid;
	mq->len = SLASH_BMAP_SIZE;
	if (bcm->bcm_bmapno == USWI_NBMAPS(wk) - 1)
		mq->len = fcmh_2_fsz(wk->uswi_fcmh) % SLASH_BMAP_SIZE;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = bcm->bcm_bmapno;
	BHGEN_GET(bcm, &mq->bgen);

	/*
	 * Mark as SCHED now in case the reply RPC comes in after we
	 * finish here.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(bcm, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	/*
	 * If it was still QUEUED, which means we marked it SCHED, then
	 * proceed; otherwise, bail: perhaps the user dequeued the
	 * replication request or something.
	 */
	if (rc == BREPLST_REPL_QUEUED) {
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	}
	pscrpc_req_finished(rq);
	if (rc == 0) {
		mds_repl_bmap_rel(bcm);
		uswi_unref(wk);
		sl_csvc_decref(csvc);
		return (1);
	}

	/* handle error return failure */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	mds_repl_bmap_apply(bcm, tract, NULL, off);

 fail:
	if (amt)
		mds_repl_nodes_adjbusy(src_rmmi, dst_rmmi, -amt);
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc)
		psc_warnx("replication arrangement failed rc=%d", rc);
	return (0);
}

int
slmupschedthr_tryptrunc(struct up_sched_work_item *wk,
    struct bmapc_memb *bcm, int off, struct sl_resource *dst_res,
    int idx)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
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
	smi = site->site_pri;

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, idx);
	dst_rmmi = dst_resm->resm_pri;

	brepls_init(retifset, 0);
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;

	/*
	 * Another ION is already handling the ptrunc CRC recomputation;
	 * go do something else.
	 */
	if (mds_repl_bmap_walk_all(bcm, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		return (-1);

	csvc = slm_geticsvc_nb(dst_resm, NULL);
	if (csvc == NULL) {
		if (!psc_multiwait_hascond(&smi->smi_mw,
		    &dst_rmmi->rmmi_mwcond))
			psc_multiwait_addcond(&smi->smi_mw,
			    &dst_rmmi->rmmi_mwcond);
		return (0);
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		goto fail;
	mq->crc = 1;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = bcm->bcm_bmapno;
	mq->offset = fcmh_2_fsz(wk->uswi_fcmh) % SLASH_BMAP_SIZE;

	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;
	mds_repl_bmap_apply(bcm, tract, NULL, off);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);

	if (rc == 0) {
		mds_repl_bmap_rel(bcm);
		uswi_unref(wk);
		sl_csvc_decref(csvc);
		return (1);
	}

	/* handle error return failure */
	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;
	mds_repl_bmap_apply(bcm, tract, NULL, off);

 fail:
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc)
		psc_warnx("partial truncation failed rc=%d", rc);
	return (0);
}

int
slmupschedthr_trygarbage(struct up_sched_work_item *wk,
    struct bmapc_memb *bcm, int off, struct sl_resource *dst_res, int j)
{
	int tract[NBREPLST], retifset[NBREPLST], rc = 0;
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
	smi = site->site_pri;

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);
	dst_rmmi = dst_resm->resm_pri;

	/*
	 * At this point, add this connection to our multiwait.
	 * If a wakeup event comes while we are timing our own
	 * establishment attempt below, we will wake up immediately
	 * when we multiwait; otherwise, we the connection will
	 * wake us when it becomes available.
	 */
	if (!psc_multiwait_hascond(&smi->smi_mw,
	    &dst_rmmi->rmmi_mwcond))
		psc_multiwait_addcond(&smi->smi_mw,
		    &dst_rmmi->rmmi_mwcond);

	csvc = slm_geticsvc_nb(dst_resm, NULL);
	if (csvc == NULL)
		goto fail;

	/* Issue garbage reclaim request */
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		goto fail;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = bcm->bcm_bmapno;
	mq->bgen = bmap_2_bgen(bcm);

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE] = BREPLST_GARBAGE_SCHED;

	brepls_init_idx(retifset);

	/* mark it as SCHED here in case the RPC finishes really quickly... */
	rc = mds_repl_bmap_apply(bcm, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	if (rc == BREPLST_GARBAGE) {
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	}
	pscrpc_req_finished(rq);
	if (rc == 0) {
		mds_repl_bmap_rel(bcm);
		uswi_unref(wk);
		sl_csvc_decref(csvc);
		return (1);
	}

	/* handle error return failure */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;
	mds_repl_bmap_apply(bcm, tract, NULL, off);

 fail:
	if (csvc)
		sl_csvc_decref(csvc);
	if (rc)
		psc_warnx("garbage reclamation failed rc=%d", rc);
	return (0);
}

void
slmupschedthr_main(struct psc_thread *thr)
{
	int uswi_gen, iosidx, off, rc, has_work, val;
	struct rnd_iterator src_resm_i, dst_resm_i, bmap_i;
	struct rnd_iterator wk_i, src_res_i, dst_res_i;
	struct sl_resource *src_res, *dst_res;
	struct slmupsched_thread *smut;
	struct slashrpc_cservice *csvc;
	struct up_sched_work_item *wk;
	struct site_mds_info *smi;
	struct sl_resm *src_resm;
	struct bmapc_memb *bcm;
	struct sl_site *site;
	void *dummy;

	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site->site_pri;
	while (pscthr_run()) {
		if (0)
 restart:
			sched_yield();

		/* select or wait for a repl rq */
		spinlock(&smi->smi_lock);

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

		smi->smi_flags &= ~SMIF_DIRTYQ;

		FOREACH_RND(&wk_i, psc_dynarray_len(&smi->smi_upq)) {
			reqlock(&smi->smi_lock);
			if (smi->smi_flags & SMIF_DIRTYQ) {
				freelock(&smi->smi_lock);
				goto restart;
			}

			wk = psc_dynarray_getpos(&smi->smi_upq, wk_i.ri_rnd_idx);
			USWI_INCREF(wk, USWI_REFT_LOOKUP);
			freelock(&smi->smi_lock);

			rc = uswi_access(wk);
			if (rc == 0) {
				/* repl must be going away, drop it */
				slmupschedthr_removeq(wk);
				goto restart;
			}

#if 0
			rc = mds_inox_ensure_loaded(USWI_INOH(wk));
			if (rc) {
				psc_warnx("couldn't load inoh repl table: %s",
				    slstrerror(rc));
				slmupschedthr_removeq(wk);
				goto restart;
			}
#endif

			has_work = 0;

			psc_pthread_mutex_lock(&wk->uswi_mutex);
			uswi_gen = wk->uswi_gen;
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_pthread_mutex_unlock(&wk->uswi_mutex);

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

				FCMH_LOCK(wk->uswi_fcmh);
				if (wk->uswi_fcmh->fcmh_flags & FCMH_IN_PTRUNC ||
				    wk->uswi_fcmh->fcmh_sstb.sst_nxbmaps) {
					has_work = 1;

					bmap_i.ri_n = USWI_NBMAPS(wk);
					bmap_i.ri_iter = bmap_i.ri_n - 1;
					bmap_i.ri_rnd_idx =
					    fcmh_2_fsz(wk->uswi_fcmh) /
					    SLASH_BMAP_SIZE;
					uswi_gen = wk->uswi_gen;
					FCMH_ULOCK(wk->uswi_fcmh);
					goto handle_bmap;
				}
				FCMH_ULOCK(wk->uswi_fcmh);

				/*
				 * Select random bmap then scan
				 * sequentially to check for work.
				 */
				FOREACH_RND(&bmap_i, USWI_NBMAPS(wk)) {
					if (uswi_gen != wk->uswi_gen)
						goto skiprepl;
 handle_bmap:
					if (mds_bmap_load(wk->uswi_fcmh,
					    bmap_i.ri_rnd_idx, &bcm))
						continue;

					has_work = 1;
					BMAPOD_MODIFY_START(bcm);
					val = SL_REPL_GET_BMAP_IOS_STAT(
					    bcm->bcm_repls, off);
					switch (val) {
					case BREPLST_REPL_QUEUED:
						if (bmap_2_bmi(bcm)->bmdsi_wr_ion)
							break;

						/* Got a bmap; now look for a source. */
						FOREACH_RND(&src_res_i, USWI_NREPLS(wk)) {
							if (uswi_gen != wk->uswi_gen) {
								BMAPOD_MODIFY_DONE(bcm);
								mds_repl_bmap_rel(bcm);
								goto skiprepl;
							}
							src_res = libsl_id2res(
							    USWI_GETREPL(wk,
							    src_res_i.ri_rnd_idx).bs_id);

							/* skip ourself and old/inactive replicas */
							if (src_res_i.ri_rnd_idx == iosidx ||
							    SL_REPL_GET_BMAP_IOS_STAT(bcm->bcm_repls,
							    SL_BITS_PER_REPLICA *
							    src_res_i.ri_rnd_idx) != BREPLST_VALID)
								continue;

							BMAPOD_MODIFY_DONE(bcm);

							/* search source nids for an idle, online connection */
							FOREACH_RND(&src_resm_i,
							    psc_dynarray_len(&src_res->res_members)) {
								src_resm = psc_dynarray_getpos(
								    &src_res->res_members,
								    src_resm_i.ri_rnd_idx);
								csvc = slm_geticsvc_nb(src_resm, NULL);
								if (csvc == NULL) {
									if (!psc_multiwait_hascond(&smi->smi_mw,
									    &resm2rmmi(src_resm)->rmmi_mwcond))
										if (psc_multiwait_addcond(&smi->smi_mw,
										    &resm2rmmi(src_resm)->rmmi_mwcond))
											psc_fatal("multiwait_addcond");
									continue;
								}
								sl_csvc_decref(csvc);

								/* scan destination resms */
								FOREACH_RND(&dst_resm_i,
								    psc_dynarray_len(&dst_res->res_members))
									if (slmupschedthr_tryrepldst(wk,
									    bcm, off, src_resm, dst_res,
									    dst_resm_i.ri_rnd_idx))
										goto restart;
							}
							BMAPOD_MODIFY_START(bcm);
						}
						break;
					case BREPLST_TRUNCPNDG:
						BMAPOD_MODIFY_DONE(bcm);
						FOREACH_RND(&dst_resm_i,
						    psc_dynarray_len(&dst_res->res_members)) {
							rc = slmupschedthr_tryptrunc(wk,
							    bcm, off, dst_res,
							    dst_resm_i.ri_rnd_idx);
							if (rc < 0)
								break;
							if (rc > 0)
								goto restart;
						}
						if (rc)
							RESET_RND_ITER(&bmap_i);
						BMAPOD_MODIFY_START(bcm);
						break;
					case BREPLST_GARBAGE_SCHED:
					case BREPLST_TRUNCPNDG_SCHED:
						RESET_RND_ITER(&bmap_i);
						break;
					case BREPLST_GARBAGE:
						if (wk->uswi_fcmh->fcmh_flags & FCMH_IN_PTRUNC &&
						    wk->uswi_fcmh->fcmh_sstb.sst_nxbmaps == 0)
							break;

						BMAPOD_MODIFY_DONE(bcm);
						FOREACH_RND(&dst_resm_i,
						    psc_dynarray_len(&dst_res->res_members))
							/*
							 * We succeed as long as one member
							 * can do the work because all
							 * members share the same backend.
							 */
							if (slmupschedthr_trygarbage(wk,
							    bcm, off, dst_res,
							    dst_resm_i.ri_rnd_idx))
								goto restart;
						RESET_RND_ITER(&bmap_i);
						BMAPOD_MODIFY_START(bcm);
						break;
					case BREPLST_VALID:
					case BREPLST_INVALID:
						has_work = 0;
						break;
					}
					BMAPOD_MODIFY_DONE(bcm);
					mds_repl_bmap_rel(bcm);
				}
			}
 skiprepl:
			/*
			 * XXX - when BREPLST_GARBAGE/SCHED all get fully
			 * reclaimed, delete metadata.
			 */

			/*
			 * At this point, we did not find a block/src/dst
			 * resource involving our site needed by this uswi.
			 */
			psc_pthread_mutex_lock(&wk->uswi_mutex);
			if (has_work || wk->uswi_gen != uswi_gen) {
				psc_multiwait_addcond_masked(&smi->smi_mw,
				    &wk->uswi_mwcond, 0);
				uswi_unref(wk);

				/*
				 * This should be safe since the wk
				 * is refcounted in our dynarray.
				 */
				psc_multiwait_setcondwakeable(&smi->smi_mw,
				    &wk->uswi_mwcond, 1);
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
		thr = pscthr_init(SLMTHRT_UPSCHED, 0, slmupschedthr_main,
		    NULL, sizeof(*smut), "slmupschedthr-%s",
		    site->site_name + strspn(site->site_name, "@"));
		smut = slmupschedthr(thr);
		smut->smut_site = site;
		pscthr_setready(thr);
	}
	CONF_URLOCK(locked);
}

/**
 * uswi_access - Obtain processing access to a update_scheduler request.
 *	This routine assumes the refcnt has already been bumped.
 * @wk: update_scheduler request to access, locked on return.
 * Returns Boolean true on success or false if the request is going away.
 */
int
uswi_access(struct up_sched_work_item *wk)
{
	int locked, rc = 1;

	psc_pthread_mutex_reqlock(&wk->uswi_mutex);

	/* Wait for someone else to finish processing. */
	locked = PLL_HASLOCK(&upsched_listhd);
	while (wk->uswi_flags & USWIF_BUSY) {
		if (locked)
			UPSCHED_MGR_ULOCK();
		psc_multiwaitcond_wait(&wk->uswi_mwcond, &wk->uswi_mutex);
		if (locked)
			UPSCHED_MGR_LOCK();
		psc_pthread_mutex_lock(&wk->uswi_mutex);
	}

	if (wk->uswi_flags & USWIF_DIE) {
		/* Release if going away. */
		USWI_DECREF(wk, USWI_REFT_LOOKUP);
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
		rc = 0;
	} else {
		wk->uswi_flags |= USWIF_BUSY;
		psc_pthread_mutex_unlock(&wk->uswi_mutex);
	}
	return (rc);
}

void
uswi_unref(struct up_sched_work_item *wk)
{
	psc_pthread_mutex_reqlock(&wk->uswi_mutex);
	wk->uswi_flags &= ~USWIF_BUSY;
	USWI_DECREF(wk, USWI_REFT_LOOKUP);
	psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
	psc_pthread_mutex_unlock(&wk->uswi_mutex);
}

struct up_sched_work_item *
uswi_find(const struct slash_fidgen *fgp, int *locked)
{
	struct up_sched_work_item q, *wk;
	struct fidc_membh fcmh;
	int dummy;

	if (locked == NULL)
		locked = &dummy;

	fcmh.fcmh_fg = *fgp;
	q.uswi_fcmh = &fcmh;

	*locked = UPSCHED_MGR_RLOCK();
	wk = SPLAY_FIND(upschedtree, &upsched_tree, &q);
	if (wk == NULL) {
		UPSCHED_MGR_URLOCK(*locked);
		return (NULL);
	}
	psc_pthread_mutex_lock(&wk->uswi_mutex);
	USWI_INCREF(wk, USWI_REFT_LOOKUP);
	UPSCHED_MGR_ULOCK();
	*locked = 0;

	/* uswi_access() drops the refcnt on failure */
	if (uswi_access(wk))
		return (wk);
	return (NULL);
}

slfid_t
uswi_getslfid(void)
{
	/* XXX should I be FID_ANY? */
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
	psc_pthread_mutex_init(&wk->uswi_mutex);
	psc_multiwaitcond_init(&wk->uswi_mwcond,
	    NULL, 0, "upsched-"SLPRI_FID, fid);
	psc_atomic32_set(&wk->uswi_refcnt, 0);
}

void
upsched_scandir(void)
{
	sl_replica_t iosv[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	int rc, tract[NBREPLST];
	char *buf, fn[NAME_MAX];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct slash_fidgen fg;
	struct pscfs_dirent *d;
	off64_t off, toff;
	size_t siz, tsiz;
	uint32_t j;
	void *data;

	rc = mdsio_opendir(mds_upschdir_inum, &rootcreds, NULL, &data);
	if (rc)
		psc_fatalx("mdsio_opendir %s: %s", SL_PATH_UPSCH,
		    slstrerror(rc));

	off = 0;
	siz = 8 * 1024;
	buf = PSCALLOC(siz);

	for (;;) {
		rc = mdsio_readdir(&rootcreds, siz,
			   off, buf, &tsiz, NULL, NULL, 0, data);
		if (rc)
			psc_fatalx("mdsio_readdir %s: %s", SL_PATH_UPSCH,
			    slstrerror(rc));
		if (tsiz == 0)
			break;
		for (toff = 0; toff < (off64_t)tsiz;
		    toff += PFL_DIRENT_SIZE(d->pfd_namelen)) {
			d = (void *)(buf + toff);
			off = d->pfd_off;

			if (strlcpy(fn, d->pfd_name, sizeof(fn)) > sizeof(fn))
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
				psc_errorx("mds_repl_loadino: %s",
				    slstrerror(rc));
				mdsio_unlink(mds_upschdir_inum, fn,
				    &rootcreds, NULL);
				continue;
			}

			rc = uswi_findoradd(&fg, &wk);
			if (rc)
				psc_fatal("uswi_findoradd: %s",
				    slstrerror(rc));

			psc_pthread_mutex_lock(&wk->uswi_mutex);
			wk->uswi_fcmh = fcmh;
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_pthread_mutex_unlock(&wk->uswi_mutex);

			brepls_init(tract, -1);
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;
			tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;

			/*
			 * If we crashed, revert all inflight SCHED'ed
			 * bmaps so they get resent.
			 */
			for (j = 0; j < USWI_NBMAPS(wk); j++) {
				if (mds_bmap_load(wk->uswi_fcmh, j, &bcm))
					continue;

				mds_repl_bmap_walk(bcm, tract,
				    NULL, 0, NULL, 0);
				mds_repl_bmap_rel(bcm);
			}

			/*
			 * Requeue pending updates on all registered sites.
			 * If there is no work to do, it will be promptly
			 * removed by the slmupschedthr.
			 */
			for (j = 0; j < USWI_NREPLS(wk); j++)
				iosv[j].bs_id = USWI_GETREPL(wk, j).bs_id;
			uswi_enqueue_sites(wk, iosv, USWI_NREPLS(wk));
			uswi_unref(wk);
		}
		off += tsiz;
	}
	rc = mdsio_release(&rootcreds, data);
	if (rc)
		psc_fatalx("mdsio_release %s: %s", SL_PATH_UPSCH,
		    slstrerror(rc));

	PSCFREE(buf);
}

int
uswi_findoradd(const struct slash_fidgen *fgp,
    struct up_sched_work_item **wkp)
{
	struct up_sched_work_item *newrq = NULL;
	int rc, gen, locked;
	char fn[PATH_MAX];
	void *mdsio_data;

	rc = 0;
	do {
		UPSCHED_MGR_LOCK();
		*wkp = uswi_find(fgp, &locked);
		if (*wkp)
			goto out;

		/*
		 * If the tree stayed locked, the request exists but we
		 * can't use it e.g. because it is going away.
		 */
	} while (!locked);

	gen = upsched_gen;
	UPSCHED_MGR_ULOCK();
	locked = 0;

	newrq = psc_pool_get(upsched_pool);
	uswi_init(newrq, fgp->fg_fid);

	rc = mds_repl_loadino(fgp, &newrq->uswi_fcmh);
	if (rc)
		goto out;

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
	    NULL, NULL, &mdsio_data, NULL, uswi_getslfid);
	if (rc)
		goto out;
	mdsio_release(&rootcreds, mdsio_data);

	UPSCHED_MGR_LOCK();
	locked = 1;
	if (gen != upsched_gen) {
		do {
			*wkp = uswi_find(fgp, &locked);
			if (*wkp) {
				fcmh_op_done_type(newrq->uswi_fcmh,
				    FCMH_OPCNT_LOOKUP_FIDC);
				goto out;
			}
			UPSCHED_MGR_RLOCK();
		} while (!locked);
	}

	psc_pthread_mutex_lock(&newrq->uswi_mutex);
	USWI_INCREF(newrq, USWI_REFT_TREE);
	USWI_INCREF(newrq, USWI_REFT_LOOKUP);

	SPLAY_INSERT(upschedtree, &upsched_tree, newrq);
	pll_addtail(&upsched_listhd, newrq);
	upsched_gen++;

	psc_pthread_mutex_unlock(&newrq->uswi_mutex);

	*wkp = newrq;
	newrq = NULL;

 out:
	if (locked)
		UPSCHED_MGR_ULOCK();

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

	locked = psc_pthread_mutex_reqlock(&wk->uswi_mutex);
	wk->uswi_gen++;
	for (n = 0; n < nios; n++) {
		site = libsl_resid2site(iosv[n].bs_id);
		smi = site->site_pri;

		spinlock(&smi->smi_lock);
		if (!psc_dynarray_exists(&smi->smi_upq, wk)) {
			psc_dynarray_add(&smi->smi_upq, wk);
			smi->smi_flags |= SMIF_DIRTYQ;
			USWI_INCREF(wk, USWI_REFT_SITEUPQ);
		}
		psc_multiwaitcond_wakeup(&smi->smi_mwcond);
		freelock(&smi->smi_lock);
	}
	psc_pthread_mutex_ureqlock(&wk->uswi_mutex, locked);
}

void
dump_uswi(struct up_sched_work_item *wk)
{
	USWI_DEBUG(PLL_MAX, wk, "");
}
