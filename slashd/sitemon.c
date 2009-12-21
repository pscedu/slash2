/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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
slmreplthr_removeq(struct sl_replrq *rrq)
{
	struct slmrepl_thread *smrt;
	struct mds_site_info *msi;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	thr = pscthr_get();
	smrt = slmreplthr(thr);
	site = smrt->smrt_site;
	msi = site->site_pri;

	locked = reqlock(&msi->msi_lock);
	msi->msi_flags |= MSIF_DIRTYQ;
	psc_dynarray_remove(&msi->msi_replq, rrq);
	ureqlock(&msi->msi_lock, locked);

	psc_pthread_mutex_reqlock(&rrq->rrq_mutex);
	psc_atomic32_dec(&rrq->rrq_refcnt);
	mds_repl_tryrmqfile(rrq);
}

int
slmreplthr_trydst(struct sl_replrq *rrq, struct bmapc_memb *bcm, int off,
    struct sl_resm *src_resm, struct sl_resource *dst_res, int j)
{
	struct mds_resm_info *src_mrmi, *dst_mrmi;
	struct srm_repl_schedwk_req *mq;
	struct slashrpc_cservice *csvc;
	struct slash_bmap_od *bmapod;
	struct slmrepl_thread *smrt;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct mds_site_info *msi;
	struct sl_resm *dst_resm;
	struct psc_thread *thr;
	struct sl_site *site;
	int we_set_busy, rc;

	we_set_busy = 0;
	thr = pscthr_get();
	smrt = slmreplthr(thr);
	site = smrt->smrt_site;
	msi = site->site_pri;

	dst_resm = libsl_nid2resm(dst_res->res_nids[j]);

	dst_mrmi = dst_resm->resm_pri;
	src_mrmi = src_resm->resm_pri;

	csvc = slm_geticsvc(dst_resm);
	if (csvc == NULL)
		goto fail;

	if (!mds_repl_nodes_setbusy(src_mrmi, dst_mrmi, 1)) {
		/* multiwait for the src to become unbusy */
		if (!psc_multiwait_hascond(&msi->msi_mw,
		    &src_mrmi->mrmi_mwcond))
			psc_multiwait_addcond(&msi->msi_mw,
			    &src_mrmi->mrmi_mwcond);
		if (!psc_multiwait_hascond(&msi->msi_mw,
		    &dst_mrmi->mrmi_mwcond))
			psc_multiwait_addcond(&msi->msi_mw,
			    &dst_mrmi->mrmi_mwcond);
		goto fail;
	}

	we_set_busy = 1;

	/* Issue replication work request */
	rc = RSX_NEWREQ(csvc->csvc_import, SRIM_VERSION,
	    SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		goto fail;
	mq->nid = src_resm->resm_nid;
	mq->len = SLASH_BMAP_SIZE; /* XXX use bmap length */
	mq->fg = *REPLRQ_FG(rrq);
	mq->bmapno = bcm->bcm_blkno;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	if (rc || mp->rc)
		goto fail;

	BMAP_LOCK(bcm);
	bmapod = bmap_2_bmdsi(bcm)->bmdsi_od;
	SL_REPL_SET_BMAP_IOS_STAT(bmapod->bh_repls,
	    off, SL_REPL_SCHED);
	mds_repl_bmap_rel(bcm);
	mds_repl_unrefrq(rrq);
	return (1);

 fail:
	if (we_set_busy)
		mds_repl_nodes_setbusy(src_mrmi, dst_mrmi, 0);
	if (!psc_multiwait_hascond(&msi->msi_mw,
	    &dst_mrmi->mrmi_mwcond))
		psc_multiwait_addcond(&msi->msi_mw,
		    &dst_mrmi->mrmi_mwcond);
	return (0);
}

__dead void *
slmreplthr_main(void *arg)
{
	int iosidx, nios, nrq, off, j, rc, has_repl_work;
	int rrq_gen, ris, is, rir, ir, rin, in, val;
	struct sl_resource *src_res, *dst_res;
	struct slash_bmap_od *bmapod;
	struct bmap_mds_info *bmdsi;
	struct slmrepl_thread *smrt;
	struct mds_site_info *msi;
	struct sl_resm *src_resm;
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_replrq *rrq;
	struct sl_site *site;
	sl_bmapno_t bmapno, nb, ib;
	void *dummy;

	thr = arg;
	smrt = slmreplthr(thr);
	site = smrt->smrt_site;
	msi = site->site_pri;
	for (;;) {
 restart:
		sched_yield();
		/* select or wait for a repl rq */
		spinlock(&msi->msi_lock);

		psc_multiwait_reset(&msi->msi_mw);
		if (psc_multiwait_addcond(&msi->msi_mw,
		    &msi->msi_mwcond) == -1)
			psc_fatal("psc_multiwait_addcond");
		psc_multiwait_entercritsect(&msi->msi_mw);

		if (psc_dynarray_len(&msi->msi_replq) == 0) {
			freelock(&msi->msi_lock);
			psc_multiwait(&msi->msi_mw, &dummy);
			continue;
		}

		msi->msi_flags &= ~MSIF_DIRTYQ;

		nrq = psc_dynarray_len(&msi->msi_replq);
		rir = psc_random32u(nrq);
		for (ir = 0; ir < nrq; rir = (rir + 1) % nrq, ir++) {
			reqlock(&msi->msi_lock);
			if (msi->msi_flags & MSIF_DIRTYQ) {
				freelock(&msi->msi_lock);
				goto restart;
			}

			rrq = psc_dynarray_getpos(&msi->msi_replq, rir);
			psc_atomic32_inc(&rrq->rrq_refcnt);
			freelock(&msi->msi_lock);

			rc = mds_repl_accessrq(rrq);

			if (rc == 0) {
				/* repl must be going away, drop it */
				slmreplthr_removeq(rrq);
				goto restart;
			}

			rc = mds_inox_ensure_loaded(rrq->rrq_inoh);
			if (rc) {
				psc_warnx("couldn't load inoh repl table: %s",
				    slstrerror(rc));
				slmreplthr_removeq(rrq);
				goto restart;
			}

			has_repl_work = 0;

			psc_pthread_mutex_lock(&rrq->rrq_mutex);
			rrq_gen = rrq->rrq_gen;
			psc_pthread_mutex_unlock(&rrq->rrq_mutex);

			/* find a resource in our site this replrq is destined for */
			iosidx = -1;
			for (j = 0; j < site->site_nres; j++) {
				dst_res = site->site_resv[j];
				iosidx = mds_repl_ios_lookup(rrq->rrq_inoh,
				    dst_res->res_id);
				if (iosidx < 0)
					continue;
				off = SL_BITS_PER_REPLICA * iosidx;

				/* got a replication request; find a bmap this ios needs */
				/* XXX lock fcmh to prohibit nbmaps changes? */
				nb = REPLRQ_NBMAPS(rrq);
				bmapno = psc_random32u(nb);
				for (ib = 0; ib < nb; ib++,
				    bmapno = (bmapno + 1) % nb) {
					rc = mds_bmap_load(REPLRQ_FCMH(rrq),
					    bmapno, &bcm);
					if (rc)
						continue;

					/*
					 * XXX if bmap has been recently modified or is
					 * still open, hold off on this bmap for now.
					 */
					BMAP_LOCK(bcm);
					bmdsi = bmap_2_bmdsi(bcm);
					bmapod = bmdsi->bmdsi_od;
					val = SL_REPL_GET_BMAP_IOS_STAT(
					    bmapod->bh_repls, off);
					if (val == SL_REPL_OLD ||
					    val == SL_REPL_SCHED)
						has_repl_work = 1;
					if (val != SL_REPL_OLD)
						goto nextbmap;
//					if (bmap is leased to an ION)
//						goto nextbmap;
					BMAP_ULOCK(bcm);

					/* Got a bmap; now look for a source. */
					nios = REPLRQ_NREPLS(rrq);
					ris = psc_random32u(nios);
					for (is = 0; is < nios; is++,
					    ris = (ris + 1) % nios) {
						src_res = libsl_id2res(REPLRQ_GETREPL(rrq, ris).bs_id);

						/* skip ourself and old/inactive replicas */
						if (ris == iosidx ||
						    SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls,
						    SL_BITS_PER_REPLICA * ris) != SL_REPL_ACTIVE)
							continue;

						/* search source nids for an idle, online connection */
						rin = psc_random32u(src_res->res_nnids);
						for (in = 0; in < (int)src_res->res_nnids; in++,
						    rin = (rin + 1) % src_res->res_nnids) {
							int k;

							src_resm = libsl_nid2resm(src_res->res_nids[rin]);
							if (slm_geticsvc(src_resm) == NULL) {
								if (!psc_multiwait_hascond(&msi->msi_mw,
								    &resm2mrmi(src_resm)->mrmi_mwcond))
									if (psc_multiwait_addcond(&msi->msi_mw,
									    &resm2mrmi(src_resm)->mrmi_mwcond))
										psc_fatal("multiwait_addcond");
								continue;
							}

							/* look for a destination resm */
							for (k = 0; k < (int)dst_res->res_nnids; k++)
								if (slmreplthr_trydst(rrq, bcm,
								    off, src_resm, dst_res, k))
									goto restart;
						}
					}
 nextbmap:
					mds_repl_bmap_rel(bcm);
				}
			}
			/*
			 * At this point, we did not find a block/src/dst
			 * resource involving our site needed by this replrq.
			 */
			psc_pthread_mutex_lock(&rrq->rrq_mutex);
			if (has_repl_work || rrq->rrq_gen != rrq_gen) {
				/*
				 * This should be safe since the rrq
				 * is refcounted in our dynarray.
				 */
				psc_multiwait_addcond_masked(&msi->msi_mw,
				    &rrq->rrq_mwcond, 0);
				mds_repl_unrefrq(rrq);
				psc_multiwait_setcondwakeable(&msi->msi_mw,
				    &rrq->rrq_mwcond, 1);
			} else {
				slmreplthr_removeq(rrq);
				goto restart;
			}
		}
		psc_multiwait(&msi->msi_mw, &dummy);
		/*
		 * XXX look at the event and process it directly
		 * instead of doing all this work again.
		 */
	}
}

void
slmreplthr_spawnall(void)
{
	struct slmrepl_thread *smrt;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	locked = PLL_RLOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(site, &globalConfig.gconf_sites) {
		thr = pscthr_init(SLMTHRT_REPL, 0, slmreplthr_main,
		    NULL, sizeof(*smrt), "slmreplthr-%s",
		    site->site_name + strspn(site->site_name, "@"));
		smrt = slmreplthr(thr);
		smrt->smrt_site = site;
		pscthr_setready(thr);
	}
	PLL_URLOCK(&globalConfig.gconf_sites, locked);
}
