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

/*
 * up_sched_res (update scheduler for site resources): this interface
 * provides the mechanism for managing updates such as
 * truncation/deletion garbage collection and replication activity to
 * peer resources.
 */

#include <sys/param.h>

#include <linux/fuse.h>

#include <dirent.h>
#include <stdio.h>

#include "pfl/cdefs.h"
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
#include "up_sched_res.h"

struct upschedtree	 upsched_tree = SPLAY_INITIALIZER(&upsched_tree);
int			 upsched_gen;
struct psc_poolmgr	*upsched_pool;
struct psc_lockedlist	 upsched_listhd =
    PLL_INITIALIZER(&upsched_listhd, struct up_sched_work_item,
	uswi_lentry);

/**
 * slmupschedthr_removeq
 *
 * XXX this needs to remove any ios' that are empty in all bmaps from the inode.
 */
void
slmupschedthr_removeq(struct up_sched_work_item *wk)
{
	int locked, uswi_gen, rc, retifset[NBMAPST];
	struct slmupsched_thread *smut;
	struct site_mds_info *smi;
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_site *site;
	char fn[IMNS_NAME_MAX];
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
	psc_atomic32_dec(&wk->uswi_refcnt);

	if (wk->uswi_flags & USWIF_DIE) {
		/* someone is already waiting for this to go away */
		uswi_unref(wk);
		return;
	}

	/* XXX */
	psc_assert((wk->uswi_flags & USWIF_BUSY) == 0);

	/*
	 * If someone bumps the generation while we're processing, we'll
	 * know there is work to do and that the uswi shouldn't go away.
	 */
	uswi_gen = wk->uswi_gen;
	wk->uswi_flags |= USWIF_BUSY;
	psc_pthread_mutex_unlock(&wk->uswi_mutex);

	/* Scan for any OLD states. */
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;
	retifset[BREPLST_REPL_QUEUED] = 1;
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_TRUNCPNDG] = 1;
	retifset[BREPLST_GARBAGE] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;

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
		UPSCHED_MGR_UNLOCK();
 keep:
		uswi_unref(wk);
		return;
	}

	/*
	 * All states are INACTIVE/ACTIVE;
	 * remove it and its persistent link.
	 */
	rc = snprintf(fn, sizeof(fn), "%016"PRIx64, USWI_FID(wk));
	if (rc == -1)
		rc = errno;
	else if (rc >= (int)sizeof(fn))
		rc = ENAMETOOLONG;
	else
		rc = mdsio_unlink(mds_upschdir_inum, fn, &rootcreds, NULL);
	uswi_kill(wk);
}

void
uswi_kill(struct up_sched_work_item *wk)
{
	UPSCHED_MGR_ENSURE_LOCKED();
	psc_pthread_mutex_ensure_locked(&wk->uswi_mutex);

	PSC_SPLAY_XREMOVE(upschedtree, &upsched_tree, wk);
	pll_remove(&upsched_listhd, wk);
	UPSCHED_MGR_UNLOCK();

	psc_atomic32_dec(&wk->uswi_refcnt);	/* removed from tree */
	wk->uswi_flags |= USWIF_DIE;
	wk->uswi_flags &= ~USWIF_BUSY;

	while (psc_atomic32_read(&wk->uswi_refcnt) > 1) {
		psc_multiwaitcond_wakeup(&wk->uswi_mwcond);
		psc_multiwaitcond_wait(&wk->uswi_mwcond, &wk->uswi_mutex);
		psc_pthread_mutex_lock(&wk->uswi_mutex);
	}

	if (wk->uswi_fcmh)
		fcmh_op_done_type(wk->uswi_fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	psc_pool_return(upsched_pool, wk);
}

int
slmupschedthr_tryrepldst(struct up_sched_work_item *wk,
    struct bmapc_memb *bcm, int off, struct sl_resm *src_resm,
    struct sl_resource *dst_res, int j)
{
	int tract[NBMAPST], retifset[NBMAPST], we_set_busy, rc;
	struct resm_mds_info *src_rmmi, *dst_rmmi;
	struct srm_repl_schedwk_req *mq;
	struct slashrpc_cservice *csvc;
	struct slmupsched_thread *smut;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;

	we_set_busy = 0;
	thr = pscthr_get();
	smut = slmupschedthr(thr);
	site = smut->smut_site;
	smi = site->site_pri;

	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);

	dst_rmmi = dst_resm->resm_pri;
	src_rmmi = src_resm->resm_pri;

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

	csvc = slm_geticsvc(dst_resm);
	if (csvc == NULL)
		goto fail;

	if (!mds_repl_nodes_setbusy(src_rmmi, dst_rmmi, 1)) {
		/* add "src to become unbusy" to multiwait */
		if (!psc_multiwait_hascond(&smi->smi_mw,
		    &src_rmmi->rmmi_mwcond))
			psc_multiwait_addcond(&smi->smi_mw,
			    &src_rmmi->rmmi_mwcond);
		goto fail;
	}

	we_set_busy = 1;

	/* Issue replication work request */
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRIM_VERSION,
	    SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		goto fail;
	mq->nid = src_resm->resm_nid;
	mq->len = SLASH_BMAP_SIZE;
	if (bcm->bcm_blkno == USWI_NBMAPS(wk) - 1)
		mq->len = fcmh_2_fsz(wk->uswi_fcmh) % SLASH_BMAP_SIZE;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = bcm->bcm_blkno;
	mq->bgen = bmap_2_bgen(bcm);

	tract[BREPLST_VALID] = -1;
	tract[BREPLST_INVALID] = -1;
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	tract[BREPLST_REPL_SCHED] = -1;
	tract[BREPLST_TRUNCPNDG] = -1;
	tract[BREPLST_GARBAGE] = -1;
	tract[BREPLST_GARBAGE_SCHED] = -1;

	retifset[BREPLST_VALID] = BREPLST_VALID;
	retifset[BREPLST_INVALID] = BREPLST_INVALID;
	retifset[BREPLST_REPL_QUEUED] = BREPLST_REPL_QUEUED;
	retifset[BREPLST_REPL_SCHED] = BREPLST_REPL_SCHED;
	retifset[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;
	retifset[BREPLST_GARBAGE] = BREPLST_GARBAGE;
	retifset[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_SCHED;

	/* mark it as SCHED here in case the RPC finishes really quickly... */
	rc = mds_repl_bmap_apply(bcm, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	if (rc == BREPLST_REPL_QUEUED) {
		rc = SL_RSX_WAITREP(rq, mp);
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
	tract[BREPLST_VALID] = -1;
	tract[BREPLST_INVALID] = -1;
	tract[BREPLST_REPL_QUEUED] = -1;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_TRUNCPNDG] = -1;
	tract[BREPLST_GARBAGE] = -1;
	tract[BREPLST_GARBAGE_SCHED] = -1;

	mds_repl_bmap_apply(bcm, tract, NULL, off);

 fail:
	if (we_set_busy)
		mds_repl_nodes_setbusy(src_rmmi, dst_rmmi, 0);
	if (csvc)
		sl_csvc_decref(csvc);
	return (0);
}

int
slmupschedthr_trygarbage(struct up_sched_work_item *wk,
    struct bmapc_memb *bcm, int off, struct sl_resource *dst_res, int j)
{
	int tract[NBMAPST], retifset[NBMAPST], rc;
	struct slashrpc_cservice *csvc;
	struct slmupsched_thread *smut;
	struct resm_mds_info *dst_rmmi;
	struct srm_garbage_req *mq;
	struct srm_generic_rep *mp;
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

	csvc = slm_geticsvc(dst_resm);
	if (csvc == NULL)
		goto fail;

	/* Issue garbage reclaim request */
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRIM_VERSION,
	    SRMT_GARBAGE, rq, mq, mp);
	if (rc)
		goto fail;
	mq->fg = *USWI_FG(wk);
	mq->bmapno = bcm->bcm_blkno;
	mq->bgen = bmap_2_bgen(bcm);

	tract[BREPLST_VALID] = -1;
	tract[BREPLST_INVALID] = -1;
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	tract[BREPLST_REPL_SCHED] = -1;
	tract[BREPLST_TRUNCPNDG] = -1;
	tract[BREPLST_GARBAGE] = BREPLST_GARBAGE_SCHED;
	tract[BREPLST_GARBAGE_SCHED] = -1;

	retifset[BREPLST_VALID] = BREPLST_VALID;
	retifset[BREPLST_INVALID] = BREPLST_INVALID;
	retifset[BREPLST_REPL_QUEUED] = BREPLST_REPL_QUEUED;
	retifset[BREPLST_REPL_SCHED] = BREPLST_REPL_SCHED;
	retifset[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;
	retifset[BREPLST_GARBAGE] = BREPLST_GARBAGE;
	retifset[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_SCHED;

	/* mark it as SCHED here in case the RPC finishes really quickly... */
	rc = mds_repl_bmap_apply(bcm, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		psc_fatalx("invalid bmap replica state: %d", rc);

	if (rc == BREPLST_REPL_QUEUED) {
		rc = SL_RSX_WAITREP(rq, mp);
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
	tract[BREPLST_VALID] = -1;
	tract[BREPLST_INVALID] = -1;
	tract[BREPLST_REPL_QUEUED] = -1;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_TRUNCPNDG] = -1;
	tract[BREPLST_GARBAGE] = -1;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;

	mds_repl_bmap_apply(bcm, tract, NULL, off);

 fail:
	if (csvc)
		sl_csvc_decref(csvc);
	return (0);
}

void
slmupschedthr_main(struct psc_thread *thr)
{
	int ris, is, rir, ir, rin, rid, in, val, nmemb, ndst;
	int uswi_gen, iosidx, nios, nrq, off, j, k, rc, has_work;
	struct sl_resource *src_res, *dst_res;
	struct slmupsched_thread *smut;
	struct up_sched_work_item *wk;
	struct slash_bmap_od *bmapod;
	struct site_mds_info *smi;
	struct sl_resm *src_resm;
	struct bmapc_memb *bcm;
	struct sl_site *site;
	sl_bmapno_t bmapno, nb, ib;
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

		nrq = psc_dynarray_len(&smi->smi_upq);
		rir = psc_random32u(nrq);
		for (ir = 0; ir < nrq; rir = (rir + 1) % nrq, ir++) {
			reqlock(&smi->smi_lock);
			if (smi->smi_flags & SMIF_DIRTYQ) {
				freelock(&smi->smi_lock);
				goto restart;
			}

			wk = psc_dynarray_getpos(&smi->smi_upq, rir);
			psc_atomic32_inc(&wk->uswi_refcnt);
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

			/* find a resource in our site this uswi is destined for */
			iosidx = -1;
			DYNARRAY_FOREACH(dst_res, j, &site->site_resources) {
				iosidx = mds_repl_ios_lookup(USWI_INOH(wk),
				    dst_res->res_id);
				if (iosidx < 0)
					continue;
				off = SL_BITS_PER_REPLICA * iosidx;

				/* got a replication request; find a bmap this ios needs */
				nb = USWI_NBMAPS(wk);
				bmapno = psc_random32u(nb);
				for (ib = 0; ib < nb; ib++,
				    bmapno = (bmapno + 1) % nb) {
					if (uswi_gen != wk->uswi_gen)
						goto skiprepl;
					rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
					if (rc)
						continue;

					BMAP_LOCK(bcm);
					bmapod = bcm->bcm_od;
					val = SL_REPL_GET_BMAP_IOS_STAT(
					    bmapod->bh_repls, off);
					switch (val) {
					case BREPLST_REPL_QUEUED:
						has_work = 1;
//						if (bmap is leased to an ION)
//							break;
						BMAP_ULOCK(bcm);

						/* Got a bmap; now look for a source. */
						nios = USWI_NREPLS(wk);
						ris = psc_random32u(nios);
						for (is = 0; is < nios; is++,
						    ris = (ris + 1) % nios) {
							if (uswi_gen != wk->uswi_gen) {
								mds_repl_bmap_rel(bcm);
								goto skiprepl;
							}
							src_res = libsl_id2res(USWI_GETREPL(wk, ris).bs_id);

							/* skip ourself and old/inactive replicas */
							if (ris == iosidx ||
							    SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls,
							    SL_BITS_PER_REPLICA * ris) != BREPLST_VALID)
								continue;

							/* search source nids for an idle, online connection */
							nmemb = psc_dynarray_len(&src_res->res_members);
							rin = psc_random32u(nmemb);
							for (in = 0; in < nmemb; in++,
							    rin = (rin + 1) % nmemb) {
								struct slashrpc_cservice *csvc;

								src_resm = psc_dynarray_getpos(&src_res->res_members, rin);
								csvc = slm_geticsvc(src_resm);
								if (csvc == NULL) {
									if (!psc_multiwait_hascond(&smi->smi_mw,
									    &resm2rmmi(src_resm)->rmmi_mwcond))
										if (psc_multiwait_addcond(&smi->smi_mw,
										    &resm2rmmi(src_resm)->rmmi_mwcond))
											psc_fatal("multiwait_addcond");
									continue;
								}
								sl_csvc_decref(csvc);

								/* look for a destination resm */
								/* XXX random? */
								for (k = 0; k < psc_dynarray_len(&dst_res->res_members); k++)
									if (slmupschedthr_tryrepldst(wk, bcm,
									    off, src_resm, dst_res, k))
										goto restart;
							}
						}
						break;
					case BREPLST_GARBAGE:
						has_work = 1;
						BMAP_ULOCK(bcm);

						/* look for a destination resm */
						ndst = psc_dynarray_len(&dst_res->res_members);
						rid = psc_random32u(ndst);
						for (k = 0; k < psc_dynarray_len(&dst_res->res_members);
						    k++, rid = (rid + 1) % ndst)
							if (slmupschedthr_trygarbage(wk,
							    bcm, off, dst_res, rid))
								goto restart;
						break;
					case BREPLST_REPL_SCHED:
						has_work = 1;
						/* FALLTHROUGH */
					}
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

	locked = PLL_RLOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(site, &globalConfig.gconf_sites) {
		thr = pscthr_init(SLMTHRT_UPSCHED, 0, slmupschedthr_main,
		    NULL, sizeof(*smut), "slmupschedthr-%s",
		    site->site_name + strspn(site->site_name, "@"));
		smut = slmupschedthr(thr);
		smut->smut_site = site;
		pscthr_setready(thr);
	}
	PLL_URLOCK(&globalConfig.gconf_sites, locked);
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
	int rc = 1;

	psc_pthread_mutex_reqlock(&wk->uswi_mutex);

	/* Wait for someone else to finish processing. */
	while (wk->uswi_flags & USWIF_BUSY) {
		psc_multiwaitcond_wait(&wk->uswi_mwcond, &wk->uswi_mutex);
		psc_pthread_mutex_lock(&wk->uswi_mutex);
	}

	if (wk->uswi_flags & USWIF_DIE) {
		/* Release if going away. */
		psc_atomic32_dec(&wk->uswi_refcnt);
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
	psc_assert(psc_atomic32_read(&wk->uswi_refcnt) > 0);
	psc_atomic32_dec(&wk->uswi_refcnt);
	wk->uswi_flags &= ~USWIF_BUSY;
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
	psc_atomic32_inc(&wk->uswi_refcnt);
	UPSCHED_MGR_UNLOCK();
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

void
uswi_init(struct up_sched_work_item *wk, slfid_t fid)
{
	memset(wk, 0, sizeof(*wk));
	wk->uswi_flags |= USWIF_BUSY;
	psc_pthread_mutex_init(&wk->uswi_mutex);
	psc_multiwaitcond_init(&wk->uswi_mwcond,
	    NULL, 0, "upsched-%lx", fid);
	psc_atomic32_set(&wk->uswi_refcnt, 1);
}

void
upsched_scandir(void)
{
	sl_replica_t iosv[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	int rc, tract[NBMAPST];
	char *buf, fn[NAME_MAX];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct slash_fidgen fg;
	struct fuse_dirent *d;
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
		    toff += FUSE_DIRENT_SIZE(d)) {
			d = (void *)(buf + toff);
			off = d->off;

			if (strlcpy(fn, d->name, sizeof(fn)) > sizeof(fn))
				psc_assert("impossible");
			if (d->namelen < sizeof(fn))
				fn[d->namelen] = '\0';

			if (fn[0] == '.')
				continue;

			memset(&fg, 0, sizeof(fg));
			fg.fg_fid = strtoll(fn, NULL, 16);
			fg.fg_gen = FIDGEN_ANY;

			rc = mds_repl_loadino(&fg, &fcmh);
			if (rc)
				/* XXX if ENOENT, remove from repldir and continue */
				psc_fatalx("mds_repl_loadino: %s",
				    slstrerror(rc));

			wk = psc_pool_get(upsched_pool);
			rc = uswi_findoradd(&fg, &wk);
			if (rc)
				psc_fatal("uswi_findoradd: %s",
				    slstrerror(rc));

			psc_pthread_mutex_lock(&wk->uswi_mutex);
			wk->uswi_fcmh = fcmh;
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_pthread_mutex_unlock(&wk->uswi_mutex);

			tract[BREPLST_INVALID] = -1;
			tract[BREPLST_VALID] = -1;
			tract[BREPLST_REPL_QUEUED] = -1;
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			tract[BREPLST_TRUNCPNDG] = -1;
			tract[BREPLST_GARBAGE] = -1;
			tract[BREPLST_GARBAGE_SCHED] = -1;

			/*
			 * If we crashed, revert all inflight SCHED'ed
			 * bmaps to OLD.
			 */
			for (j = 0; j < USWI_NBMAPS(wk); j++) {
				if (mds_bmap_load(wk->uswi_fcmh, j, &bcm))
					continue;

				mds_repl_bmap_walk(bcm, tract,
				    NULL, 0, NULL, 0);
				mds_repl_bmap_rel(bcm);
			}

			/*
			 * Requeue pending replications on all sites.
			 * If there is no work to do, it will be promptly
			 * removed by the slmupschedthr.
			 */
			for (j = 0; j < USWI_NREPLS(wk); j++)
				iosv[j].bs_id = USWI_GETREPL(wk, j).bs_id;
			uswi_enqueue_sites(wk, iosv, USWI_NREPLS(wk));
		}
		off += tsiz;
	}
	rc = mdsio_release(&rootcreds, data);
	if (rc)
		psc_fatalx("mdsio_release %s: %s", SL_PATH_UPSCH,
		    slstrerror(rc));

	free(buf);
}

int
uswi_findoradd(const struct slash_fidgen *fgp, struct up_sched_work_item **wkp)
{
	struct up_sched_work_item *newrq = NULL;
	char fn[PATH_MAX];
	void *mdsio_data;
	int rc, gen, locked;

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
	UPSCHED_MGR_UNLOCK();
	locked = 0;

	newrq = psc_pool_get(upsched_pool);
	uswi_init(newrq, fgp->fg_fid);

	rc = mds_repl_loadino(fgp, &newrq->uswi_fcmh);
	if (rc)
		goto out;

	rc = snprintf(fn, sizeof(fn), "%016"PRIx64, fgp->fg_fid);
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
	    NULL, NULL, NULL, &mdsio_data, NULL, uswi_getslfid);
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

	SPLAY_INSERT(upschedtree, &upsched_tree, newrq);
	pll_addtail(&upsched_listhd, newrq);
	upsched_gen++;
	psc_atomic32_inc(&newrq->uswi_refcnt);

	*wkp = newrq;
	newrq = NULL;

 out:
	if (locked)
		UPSCHED_MGR_UNLOCK();

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
			psc_atomic32_inc(&wk->uswi_refcnt);
		}
		psc_multiwaitcond_wakeup(&smi->smi_mwcond);
		freelock(&smi->smi_lock);
	}
	psc_pthread_mutex_ureqlock(&wk->uswi_mutex, locked);
}
