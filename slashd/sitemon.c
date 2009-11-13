/* $Id$ */

#include <stdio.h>

#include "pfl/cdefs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_rpc/rsx.h"
#include "psc_util/random.h"
#include "psc_util/thread.h"

#include "repl_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"

void
slmsmthr_removeq(struct sl_replrq *rrq)
{
	struct slmsm_thread *smsmt;
	struct mds_site_info *msi;
	struct psc_thread *thr;
	struct sl_site *site;
	int locked;

	thr = pscthr_get();
	smsmt = slmsmthr(thr);
	site = smsmt->smsmt_site;
	msi = site->site_pri;

	locked = reqlock(&msi->msi_lock);
	if (psc_dynarray_exists(&msi->msi_replq, rrq))
		psc_dynarray_remove(&msi->msi_replq, rrq);
	ureqlock(&msi->msi_lock, locked);

	mds_repl_tryrmqfile(rrq);
}

struct sl_resm *
slmsmthr_find_dst_ion(struct mds_resm_info *src)
{
	struct slmsm_thread *smsmt;
	struct mds_resm_info *dst;
	struct psc_thread *thr;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *site;
	uint32_t j;
	int n;

	thr = pscthr_get();
	smsmt = slmsmthr(thr);
	site = smsmt->smsmt_site;

	spinlock(&repl_busytable_lock);
	for (n = 0; n < site->site_nres; n++) {
		r = site->site_resv[n];
		for (j = 0; j < r->res_nnids; j++) {
			resm = libsl_nid2resm(r->res_nids[j]);
			dst = resm->resm_pri;
			if (!mds_repl_nodes_getbusy(src, dst))
				goto out;
		}
	}
	resm = NULL;
 out:
	freelock(&repl_busytable_lock);
	return (resm);
}

__dead void *
slmsmthr_main(void *arg)
{
	int iosidx, val, nios, off, rc, ris, is, ir, rin, in;
	sl_bmapno_t bmapno, nb, ib;
	struct mds_resm_info *src_mri, *dst_mri;
	struct sl_resm *src_resm, *dst_resm;
	struct srm_repl_schedwk_req *mq;
	struct slash_bmap_od *bmapod;
	struct srm_generic_rep *mp;
	struct slmsm_thread *smsmt;
	struct pscrpc_request *rq;
	struct mds_site_info *msi;
	struct sl_resource *res;
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_replrq *rrq;
	struct sl_site *site;

	thr = arg;
	smsmt = slmsmthr(thr);
	site = smsmt->smsmt_site;
	msi = site->site_pri;
	for (;;) {
		sched_yield();

		/* find/wait for a resm */

		spinlock(&msi->msi_lock);
		if (psc_dynarray_len(&msi->msi_replq) == 0) {
			psc_waitq_wait(&msi->msi_waitq, &msi->msi_lock);
			continue;
		}
		rrq = psc_dynarray_getpos(&msi->msi_replq,
		    psc_random32u(psc_dynarray_len(&msi->msi_replq)));
		rc = mds_repl_accessrq(rrq);
		freelock(&msi->msi_lock);

		if (rc == 0) {
			/* repl must be going away, drop it */
			slmsmthr_removeq(rrq);
			continue;
		}

		/* find which resource in our site this repl is destined for */
		iosidx = -1;
		for (ir = 0; ir < site->site_nres; ir++) {
			iosidx = mds_repl_ios_lookup(rrq->rrq_inoh,
			    site->site_resv[ir]->res_id);
			if (iosidx >= 0)
				break;
		}
		if (iosidx < 0) {
			slmsmthr_removeq(rrq);
			continue;
		}
		freelock(&rrq->rrq_lock);

		off = SL_BITS_PER_REPLICA * iosidx;

		/* got a replication request; find a bmap this ios needs */
		/* XXX lock fcmh to prohibit nbmaps changes? */
		nb = REPLRQ_NBMAPS(rrq);
		bmapno = psc_random32u(nb);
		for (ib = 0; ib < nb; ib++,
		    bmapno = (bmapno + 1) % nb) {
			if (mds_bmap_load(REPLRQ_FCMH(rrq), bmapno, &bcm))
				/* XXX check inode new bmap policy? */
				continue;

			/*
			 * XXX if bmap has been recently modified or is
			 * still open, hold off on this bmap for now.
			 */
			BMAP_LOCK(bcm);
			bmapod = bmap_2_bmdsi(bcm)->bmdsi_od;
			val = SL_REPL_GET_BMAP_IOS_STAT(
			    bmapod->bh_repls, off);
			if (val == SL_REPL_OLD)
				goto brepl;
			mds_repl_bmap_rel(bcm);
		}

#if 0
		if (nb == REPLRQ_NBMAPS(rrq) &&
		    all data valid &&
		    no temp flags have been cleared from mods &&
		    inode new bmap policy not persist &&
		    every bmap not persistent)
			/* couldn't find any bmaps; remove from queue */
			slmsmthr_removeq(rrq);
		else
#endif
			mds_repl_unrefrq(rrq);
		spinlock(&msi->msi_lock);
		psc_waitq_waitrel_ms(&msi->msi_waitq, &msi->msi_lock, 1);
		continue;

 brepl:
		BMAP_ULOCK(bcm);

		rc = mds_repl_inoh_ensure_loaded(rrq->rrq_inoh);
		if (rc) {
			psc_warnx("couldn't load inoh repl table: %s",
			    slstrerror(rc));
			goto release;
		}

		/*
		 * Got a bmap to replicate; need to find a source.
		 * First, select a random IOS that has it.
		 */
		src_mri = dst_mri = NULL;
		src_resm = dst_resm = NULL;
		nios = REPLRQ_NREPLS(rrq);
		ris = psc_random32u(nios);
		for (is = 0; is < nios; is++,
		    ris = (ris + 1) % nios) {
			res = libsl_id2res(REPLRQ_GETREPL(rrq, ris).bs_id);

			/* skip ourself and old/inactive replicas */
			if (ris == iosidx ||
			    SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls,
			    SL_BITS_PER_REPLICA * ris) != SL_REPL_ACTIVE)
				continue;

			/* search nids for an idle, online connection */
			rin = psc_random32u(res->res_nnids);
			for (in = 0; in < (int)res->res_nnids; in++,
			    rin = (rin + 1) % res->res_nnids) {
				src_resm = libsl_nid2resm(res->res_nids[rin]);
				src_mri = src_resm->resm_pri;
				spinlock(&src_mri->mri_lock);
				if (src_mri->mri_csvc && (dst_resm =
				    slmsmthr_find_dst_ion(src_mri)) != NULL)
					goto issue;
				freelock(&src_mri->mri_lock);
			}
		}
		psc_error("could not find a replica");
 issue:
		if (dst_resm == NULL) {
			spinlock(&msi->msi_lock);
			psc_waitq_waitrel_ms(&msi->msi_waitq,
			    &msi->msi_lock, 1);
			rc = -1;
			goto release;
		}
		freelock(&src_mri->mri_lock);

		/* Issue replication work request */
		rc = RSX_NEWREQ(dst_mri->mri_csvc->csvc_import,
		    SRIM_VERSION, SRMT_REPL_SCHEDWK, rq, mq, mp);
		if (rc == 0) {
			mq->nid = src_resm->resm_nid;
			mq->fg = *REPLRQ_FG(rrq);
			mq->bmapno = bmapno;
			rc = RSX_WAITREP(rq, mp);
			pscrpc_req_finished(rq);
		}

 release:
		if (rc == 0) {
			SL_REPL_SET_BMAP_IOS_STAT(
			    bmapod->bh_repls, off, SL_REPL_SCHED);
			mds_repl_nodes_setbusy(src_mri, dst_mri, 1);
		}
		mds_repl_bmap_rel(bcm);
		mds_repl_unrefrq(rrq);
	}
}

void
sitemons_spawn(void)
{
	struct slmsm_thread *smsmt;
	struct psc_thread *thr;
	struct sl_site *site;

	PLL_FOREACH(site, &globalConfig.gconf_sites) {
		thr = pscthr_init(SLMTHRT_SITEMON, 0, slmsmthr_main,
		    NULL, sizeof(*smsmt), "slmsmthr-%s",
		    site->site_name + strcspn(site->site_name, "@"));
		smsmt = slmsmthr(thr);
		smsmt->smsmt_site = site;
		pscthr_setready(thr);
	}
}
