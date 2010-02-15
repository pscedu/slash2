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

#include <stdio.h>

#include "pfl/cdefs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_rpc/rsx.h"
#include "psc_util/multiwait.h"
#include "psc_util/pthrutil.h"
#include "psc_util/random.h"
#include "psc_util/thread.h"

#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"

void
slmreplqthr_removeq(struct sl_replrq *rrq)
{
	struct slmreplq_thread *smrt;
	struct site_mds_info *smi;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	thr = pscthr_get();
	smrt = slmreplqthr(thr);
	site = smrt->smrt_site;
	smi = site->site_pri;

	locked = reqlock(&smi->smi_lock);
	smi->smi_flags |= SMIF_DIRTYQ;
	psc_dynarray_remove(&smi->smi_replq, rrq);
	ureqlock(&smi->smi_lock, locked);

	psc_pthread_mutex_reqlock(&rrq->rrq_mutex);
	psc_atomic32_dec(&rrq->rrq_refcnt);
	mds_repl_tryrmqfile(rrq);
}

int
slmreplqthr_trydst(struct sl_replrq *rrq, struct bmapc_memb *bcm, int off,
    struct sl_resm *src_resm, struct sl_resource *dst_res, int j)
{
	int tract[4], we_set_busy, rc;
	struct mds_resm_info *src_rmmi, *dst_rmmi;
	struct srm_repl_schedwk_req *mq;
	struct slashrpc_cservice *csvc;
	struct slmreplq_thread *smrt;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct site_mds_info *smi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;

	we_set_busy = 0;
	thr = pscthr_get();
	smrt = slmreplqthr(thr);
	site = smrt->smrt_site;
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
	rc = RSX_NEWREQ(csvc->csvc_import, SRIM_VERSION,
	    SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		goto fail;
	mq->nid = src_resm->resm_nid;
	mq->len = SLASH_BMAP_SIZE;
	if (bcm->bcm_blkno == REPLRQ_NBMAPS(rrq) - 1)
		mq->len = fcmh_2_fsz(REPLRQ_FCMH(rrq)) % SLASH_BMAP_SIZE;
	mq->fg = *REPLRQ_FG(rrq);
	mq->bmapno = bcm->bcm_blkno;
	mq->bgen = bmap_2_bgen(bcm);

	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_INACTIVE] = -1;
	tract[SL_REPLST_OLD] = SL_REPLST_SCHED;
	tract[SL_REPLST_SCHED] = -1;

	/* mark it as SCHED here in case the RPC finishes really quickly... */
	BMAP_LOCK(bcm);
	mds_repl_bmap_apply(bcm, tract, NULL, off);
	BMAP_ULOCK(bcm);

	rc = RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	if (rc == 0) {
		mds_repl_bmap_rel(bcm);
		mds_repl_unrefrq(rrq);
		return (1);
	}

	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_INACTIVE] = -1;
	tract[SL_REPLST_OLD] = -1;
	tract[SL_REPLST_SCHED] = SL_REPLST_OLD;

	BMAP_LOCK(bcm);
	mds_repl_bmap_apply(bcm, tract, NULL, off);
	BMAP_ULOCK(bcm);

 fail:
	if (we_set_busy)
		mds_repl_nodes_setbusy(src_rmmi, dst_rmmi, 0);
	return (0);
}

__dead void *
slmreplqthr_main(void *arg)
{
	int iosidx, nios, nrq, off, j, rc, has_repl_work;
	int rrq_gen, ris, is, rir, ir, rin, in, val, nmemb;
	struct sl_resource *src_res, *dst_res;
	struct slmreplq_thread *smrt;
	struct slash_bmap_od *bmapod;
	struct bmap_mds_info *bmdsi;
	struct site_mds_info *smi;
	struct sl_resm *src_resm;
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_replrq *rrq;
	struct sl_site *site;
	sl_bmapno_t bmapno, nb, ib;
	void *dummy;

	thr = arg;
	smrt = slmreplqthr(thr);
	site = smrt->smrt_site;
	smi = site->site_pri;
	for (;;) {
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

		if (psc_dynarray_len(&smi->smi_replq) == 0) {
			freelock(&smi->smi_lock);
			psc_multiwait(&smi->smi_mw, &dummy);
			continue;
		}

		smi->smi_flags &= ~SMIF_DIRTYQ;

		nrq = psc_dynarray_len(&smi->smi_replq);
		rir = psc_random32u(nrq);
		for (ir = 0; ir < nrq; rir = (rir + 1) % nrq, ir++) {
			reqlock(&smi->smi_lock);
			if (smi->smi_flags & SMIF_DIRTYQ) {
				freelock(&smi->smi_lock);
				goto restart;
			}

			rrq = psc_dynarray_getpos(&smi->smi_replq, rir);
			psc_atomic32_inc(&rrq->rrq_refcnt);
			freelock(&smi->smi_lock);

			rc = mds_repl_accessrq(rrq);

			if (rc == 0) {
				/* repl must be going away, drop it */
				slmreplqthr_removeq(rrq);
				goto restart;
			}

			rc = mds_inox_ensure_loaded(rrq->rrq_inoh);
			if (rc) {
				psc_warnx("couldn't load inoh repl table: %s",
				    slstrerror(rc));
				slmreplqthr_removeq(rrq);
				goto restart;
			}

			has_repl_work = 0;

			psc_pthread_mutex_lock(&rrq->rrq_mutex);
			rrq_gen = rrq->rrq_gen;
			rrq->rrq_flags &= ~REPLRQF_BUSY;
			psc_pthread_mutex_unlock(&rrq->rrq_mutex);

			/* find a resource in our site this replrq is destined for */
			iosidx = -1;
			DYNARRAY_FOREACH(dst_res, j, &site->site_resources) {
				iosidx = mds_repl_ios_lookup(rrq->rrq_inoh,
				    dst_res->res_id);
				if (iosidx < 0)
					continue;
				off = SL_BITS_PER_REPLICA * iosidx;

				/* got a replication request; find a bmap this ios needs */
				nb = REPLRQ_NBMAPS(rrq);
				bmapno = psc_random32u(nb);
				for (ib = 0; ib < nb; ib++,
				    bmapno = (bmapno + 1) % nb) {
					if (rrq_gen != rrq->rrq_gen)
						goto skiprrq;
					rc = mds_bmap_load(REPLRQ_FCMH(rrq),
					    bmapno, &bcm);
					if (rc)
						continue;

					BMAP_LOCK(bcm);
					bmdsi = bmap_2_bmdsi(bcm);
					bmapod = bmdsi->bmdsi_od;
					val = SL_REPL_GET_BMAP_IOS_STAT(
					    bmapod->bh_repls, off);
					if (val == SL_REPLST_OLD ||
					    val == SL_REPLST_SCHED)
						has_repl_work = 1;
					if (val != SL_REPLST_OLD)
						goto skipbmap;
//					if (bmap is leased to an ION)
//						goto skipbmap;
					BMAP_ULOCK(bcm);

					/* Got a bmap; now look for a source. */
					nios = REPLRQ_NREPLS(rrq);
					ris = psc_random32u(nios);
					for (is = 0; is < nios; is++,
					    ris = (ris + 1) % nios) {
						if (rrq_gen != rrq->rrq_gen) {
							mds_repl_bmap_rel(bcm);
							goto skiprrq;
						}
						src_res = libsl_id2res(REPLRQ_GETREPL(rrq, ris).bs_id);

						/* skip ourself and old/inactive replicas */
						if (ris == iosidx ||
						    SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls,
						    SL_BITS_PER_REPLICA * ris) != SL_REPLST_ACTIVE)
							continue;

						/* search source nids for an idle, online connection */
						nmemb = psc_dynarray_len(&src_res->res_members);
						rin = psc_random32u(nmemb);
						for (in = 0; in < nmemb; in++,
						    rin = (rin + 1) % nmemb) {
							int k;

							src_resm = psc_dynarray_getpos(&src_res->res_members, rin);
							if (slm_geticsvc(src_resm) == NULL) {
								if (!psc_multiwait_hascond(&smi->smi_mw,
								    &resm2mrmi(src_resm)->rmmi_mwcond))
									if (psc_multiwait_addcond(&smi->smi_mw,
									    &resm2mrmi(src_resm)->rmmi_mwcond))
										psc_fatal("multiwait_addcond");
								continue;
							}

							/* look for a destination resm */
							for (k = 0; k < psc_dynarray_len(&dst_res->res_members); k++)
								if (slmreplqthr_trydst(rrq, bcm,
								    off, src_resm, dst_res, k))
									goto restart;
						}
					}
 skipbmap:
					mds_repl_bmap_rel(bcm);
				}
			}
 skiprrq:
			/*
			 * At this point, we did not find a block/src/dst
			 * resource involving our site needed by this replrq.
			 */
			psc_pthread_mutex_lock(&rrq->rrq_mutex);
			if (has_repl_work || rrq->rrq_gen != rrq_gen) {
				psc_multiwait_addcond_masked(&smi->smi_mw,
				    &rrq->rrq_mwcond, 0);
				mds_repl_unrefrq(rrq);

				/*
				 * This should be safe since the rrq
				 * is refcounted in our dynarray.
				 */
				psc_multiwait_setcondwakeable(&smi->smi_mw,
				    &rrq->rrq_mwcond, 1);
			} else {
				slmreplqthr_removeq(rrq);
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

#if 0
__dead void *
slmtruncthr_main(void *arg)
{

	thr = arg;
	smtrt = slmtruncthr(thr);
	site = smrt->smrt_site;
	smi = site->site_pri;
	for (;;) {
//		trq = lc_get(smi->smi_truncq);

		for (j = 0; j < site->site_nres; j++) {
			res = site->site_resv[j];
			rin = psc_random32u(res->res_nnids);
			for (in = 0; in < (int)res->res_nnids; in++,
			    rin = (rin + 1) % res->res_nnids) {
				resm = libsl_nid2resm(res->res_nids[rin]);
				if (slm_geticsvc(resm) == NULL)
					continue;
		}

	}
}
#endif

void
slmreplqthr_spawnall(void)
{
	struct slmreplq_thread *smrt;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	locked = PLL_RLOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(site, &globalConfig.gconf_sites) {
		thr = pscthr_init(SLMTHRT_REPLQ, 0, slmreplqthr_main,
		    NULL, sizeof(*smrt), "slmreplqthr-%s",
		    site->site_name + strspn(site->site_name, "@"));
		smrt = slmreplqthr(thr);
		smrt->smrt_site = site;
		pscthr_setready(thr);
	}
	PLL_URLOCK(&globalConfig.gconf_sites, locked);
}
