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

	thr = pscthr_get();
	smsmt = slmsmthr(thr);
	site = smsmt->smsmt_site;
	msi = site->site_pri;

	spinlock(&msi->msi_lock);
	if (psc_dynarray_exists(&msi->msi_replq, rrq))
		psc_dynarray_remove(&msi->msi_replq, rrq);
	freelock(&msi->msi_lock);

	/*
	 * XXX if this rrq is not a member of any other replq's,
	 * delete the rrq.
	 */

	mds_repl_unrefrq(rrq);
}

__dead void *
slmsmthr_main(void *arg)
{
	int iosidx, val, nios, off, rc, ris, is, rir, ir, rin, in;
	sl_bmapno_t bmapno, nb, ib;
	struct slash_bmap_od *bmapod;
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	struct slmsm_thread *smsmt;
	struct pscrpc_request *rq;
	struct mds_site_info *msi;
	struct mds_resm_info *mri;
	struct sl_site *site, *s;
	struct sl_resource *res;
	struct bmapc_memb *bcm;
	struct psc_thread *thr;
	struct sl_replrq *rrq;
	struct sl_resm *resm;

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
		psc_atomic32_inc(&rrq->rrq_refcnt);
		freelock(&msi->msi_lock);

		rc = mds_repl_accessrq(rrq);

		if (rc == 0 || (iosidx = mds_repl_ios_lookup(rrq->rrq_inoh,
		    site->site_id)) < 0) {
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
			bcm = mds_bmap_load(REPLRQ_FCMH(rrq), bmapno);
			if (bcm == NULL)
				/* XXX check inode new bmap policy? */
				continue;

			/*
			 * XXX if bmap has been recently modified or is
			 * still open, hold off on this bmap for now.
			 */
			BMAP_LOCK(bcm);
			val = SL_REPL_GET_BMAP_IOS_STAT(
			    bmapod->bh_repls, off);
			if (val == SL_REPL_OLD ||
			    val == SL_REPL_TOO_OLD)
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
		rc = mds_repl_inoh_ensure_loaded(rrq->rrq_inoh);
		if (rc) {
			psc_warnx("couldn't load inoh repl table: %s",
			    slstrerror(rc));
			continue;
		}

		/*
		 * Got a bmap to replicate; need to find a source.
		 * First, select a random IOS that has it.
		 */
		mri = NULL;
		resm = NULL;
		nios = REPLRQ_NREPLS(rrq);
		ris = psc_random32u(nios);
		for (is = 0; is < nios; is++,
		    ris = (ris + 1) % nios) {
			s = libsl_id2site(REPLRQ_GETREPL(rrq, ris).bs_id);

			/* skip ourself */
			if (s == site)
				continue;

			/* Next, pick a random resource at this site. */
			rir = psc_random32u(s->site_nres);
			for (ir = 0; ir < s->site_nres; ir++,
			    rir = (rir + 1) % s->site_nres) {
				res = s->site_resv[rir];

				/*
				 * Finally, pick a random ION that is
				 * online and not already busy.
				 */
				rin = psc_random32u(res->res_nnids);
				for (in = 0; in < (int)res->res_nnids; in++,
				    rin = (rin + 1) % res->res_nnids) {
					resm = libsl_nid2resm(res->res_nids[rin]);
					mri = resm->resm_pri;
					spinlock(&mri->mri_lock);
					if (mri->mri_csvc &&
					    (mri->mri_flags & MRIF_BUSY) == 0)
						goto issue;
					freelock(&mri->mri_lock);
				}
			}
		}
		psc_error("should have found someone");
 issue:
		mds_repl_bmap_rel(bcm);
		mds_repl_unrefrq(rrq);
		if (resm == NULL) {
			spinlock(&msi->msi_lock);
			psc_waitq_waitrel_ms(&msi->msi_waitq,
			    &msi->msi_lock, 1);
			continue;
		}

		/* Issue replication work request */
		mri->mri_flags |= MRIF_BUSY;
		freelock(&mri->mri_lock);

		rc = RSX_NEWREQ(mri->mri_csvc->csvc_import,
		    SRIM_VERSION, SRMT_REPL_SCHEDWK, rq, mq, mp);
		if (rc)
			goto rpcfail;
		mq->nid = resm->resm_nid;
		mq->fid = REPLRQ_FID(rrq);
		mq->bmapno = bmapno;
		rc = RSX_WAITREP(rq, mp);
		pscrpc_req_finished(rq);

		if (rc) {
 rpcfail:
			spinlock(&mri->mri_lock);
			mri->mri_flags &= ~MRIF_BUSY;
			freelock(&mri->mri_lock);
		}
	}
}

void
sitemons_spawn(void)
{
	struct slmsm_thread *smsmt;
	struct psc_thread *thr;
	struct sl_site *site;

	psclist_for_each_entry(site, &globalConfig.gconf_sites,
	    site_lentry) {
		thr = pscthr_init(SLMTHRT_SITEMON, 0, slmsmthr_main,
		    NULL, sizeof(*smsmt), "slmsmthr-%s",
		    site->site_name + strcspn(site->site_name, "@"));
		smsmt = slmsmthr(thr);
		smsmt->smsmt_site = site;
		pscthr_setready(thr);
	}
}
