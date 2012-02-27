/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for issuing RPC requests to CLI from MDS.
 */

#define PSC_SUBSYS PSS_RPC

#include <sys/param.h>

#include <dirent.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"
#include "psc_util/thread.h"

#include "bmap_mds.h"
#include "fid.h"
#include "inode.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "up_sched_res.h"

int
slmrmcthr_replst_slave_eof(struct slm_replst_workreq *rsw,
    struct up_sched_work_item *wk)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = SL_RSX_NEWREQ(rsw->rsw_csvc, SRMT_REPL_GETST_SLAVE, rq, mq,
	    mp);
	if (rc)
		return (rc);

	mq->fg = *USWI_FG(wk);
	mq->id = rsw->rsw_cid;
	mq->rc = EOF;
	rc = SL_RSX_WAITREP(rsw->rsw_csvc, rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrmcthr_replst_slave_waitrep(struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq, struct up_sched_work_item *wk)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	struct iovec iov;
	int rc = 0;
	size_t nb;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	nb = howmany(srcm->srcm_page_bitpos, NBBY);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mq->len = nb;
	mq->nbmaps = srcm->srcm_page_bitpos / (SL_BITS_PER_REPLICA *
	    USWI_INOH(wk)->inoh_ino.ino_nrepls + SL_NBITS_REPLST_BHDR);
	if (nb > sizeof(mq->buf)) {
		iov.iov_base = srcm->srcm_page;
		iov.iov_len = nb;
		rc = rsx_bulkclient(rq, BULK_GET_SOURCE,
		    SRCM_BULK_PORTAL, &iov, 1);
	} else
		memcpy(mq->buf, srcm->srcm_page, nb);
	if (rc == 0) {
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	}

	memset(srcm->srcm_page, 0, nb);

	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrcmthr_walk_brepls(struct slm_replst_workreq *rsw,
      struct up_sched_work_item *wk, struct bmapc_memb *bcm,
      sl_bmapno_t n, struct pscrpc_request **rqp)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct srsm_replst_bhdr bhdr;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	int nbits, rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);
	nbits = USWI_INOH(wk)->inoh_ino.ino_nrepls *
	    SL_BITS_PER_REPLICA + SL_NBITS_REPLST_BHDR;
	if (howmany(srcm->srcm_page_bitpos + nbits,
	    NBBY) > SRM_REPLST_PAGESIZ || *rqp == NULL) {
		if (*rqp) {
			rc = slmrmcthr_replst_slave_waitrep(
			    rsw->rsw_csvc, *rqp, wk);
			*rqp = NULL;
			if (rc)
				return (rc);
		}

		rc = SL_RSX_NEWREQ(rsw->rsw_csvc, SRMT_REPL_GETST_SLAVE,
		    *rqp, mq, mp);
		if (rc)
			return (rc);
		mq->id = rsw->rsw_cid;
		mq->boff = n;
		mq->fg = *USWI_FG(wk);

		srcm->srcm_page_bitpos = 0;
	}
	memset(&bhdr, 0, sizeof(bhdr));
	bhdr.srsb_replpol = bmap_2_replpol(bcm);
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos,
	    &bhdr, 0, SL_NBITS_REPLST_BHDR);
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos +
	    SL_NBITS_REPLST_BHDR, bcm->bcm_repls, 0,
	    USWI_INOH(wk)->inoh_ino.ino_nrepls * SL_BITS_PER_REPLICA);
	srcm->srcm_page_bitpos += nbits;
	return (0);
}

/**
 * slm_rcm_issue_getreplst - Issue a GETREPLST reply to a CLI from MDS.
 */
int
slm_rcm_issue_getreplst(struct slm_replst_workreq *rsw,
    struct up_sched_work_item *wk, int is_eof)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = SL_RSX_NEWREQ(rsw->rsw_csvc, SRMT_REPL_GETST, rq, mq, mp);
	if (rc)
		return (rc);
	mq->id = rsw->rsw_cid;
	if (wk) {
		mq->fg = *USWI_FG(wk);
		mq->nrepls = USWI_NREPLS(wk);
		mq->newreplpol = USWI_INO(wk)->ino_replpol;
		memcpy(mq->repls, USWI_INO(wk)->ino_repls,
		    MIN(mq->nrepls, SL_DEF_REPLICAS) * sizeof(*mq->repls));
		if (mq->nrepls > SL_DEF_REPLICAS)
			memcpy(mq->repls + SL_DEF_REPLICAS,
			    USWI_INOH(wk)->inoh_extras->inox_repls,
			    (USWI_NREPLS(wk) - SL_DEF_REPLICAS) *
			    sizeof(*mq->repls));
	}
	if (is_eof)
		mq->rc = EOF;

	USWI_LOCK(wk);
	wk->uswi_flags &= ~USWIF_BUSY;
	USWI_ULOCK(wk);

	rc = SL_RSX_WAITREP(rsw->rsw_csvc, rq, mp);
	pscrpc_req_finished(rq);

	USWI_LOCK(wk);
	USWI_WAIT(wk, 0);
	wk->uswi_flags |= USWIF_BUSY;
	USWI_ULOCK(wk);

	return (rc);
}

int
slmrcmthr_walk_bmaps(struct slm_replst_workreq *rsw,
    struct up_sched_work_item *wk)
{
	struct pscrpc_request *rq = NULL;
	struct bmapc_memb *bcm;
	sl_bmapno_t n;
	int rc, rc2;

	rc = slm_rcm_issue_getreplst(rsw, wk, 0);
	if (fcmh_isreg(wk->uswi_fcmh)) {
		for (n = 0; rc == 0; n++) {
			if (bmap_getf(wk->uswi_fcmh, n, SL_WRITE,
			    BMAPGETF_LOAD | BMAPGETF_NOAUTOINST, &bcm))
				break;

			BMAP_LOCK(bcm);
			rc = slmrcmthr_walk_brepls(rsw, wk, bcm, n,
			    &rq);
			bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
		}
		if (rq) {
			rc2 = slmrmcthr_replst_slave_waitrep(
			    rsw->rsw_csvc, rq, wk);
			if (rc == 0)
				rc = rc2;
		}
	}
	rc2 = slmrmcthr_replst_slave_eof(rsw, wk);
	if (rc == 0)
		rc = rc2;
	return (rc);
}

void
slmrcmthr_main(struct psc_thread *thr)
{
	struct slm_replst_workreq *rsw;
	struct up_sched_work_item *wk;
	struct slmrcm_thread *srcm;
	struct fidc_membh *fcmh;
	int rc;

	srcm = slmrcmthr(thr);
	while (pscthr_run()) {
		rsw = lc_getwait(&slm_replst_workq);
		srcm->srcm_page_bitpos = SRM_REPLST_PAGESIZ * NBBY;

		if (rsw->rsw_fg.fg_fid == FID_ANY) {
			PLL_LOCK(&upsched_listhd);
			PLL_FOREACH(wk, &upsched_listhd) {
				USWI_INCREF(wk, USWI_REFT_LOOKUP);
				if (!uswi_access(wk))
					continue;
				PLL_ULOCK(&upsched_listhd);

				rc = slmrcmthr_walk_bmaps(rsw, wk);
				PLL_LOCK(&upsched_listhd);
				uswi_unref(wk);
				if (rc)
					break;
			}
			PLL_ULOCK(&upsched_listhd);
		} else if ((wk = uswi_find(&rsw->rsw_fg)) != NULL) {
			slmrcmthr_walk_bmaps(rsw, wk);
			uswi_unref(wk);
		} else if (mds_repl_loadino(&rsw->rsw_fg, &fcmh) == 0) {
			/*
			 * File is not in cache: load it up to report
			 * replication status, grabbing a dummy uswi to
			 * pass around.
			 */
			wk = psc_pool_get(upsched_pool);
			uswi_init(wk, rsw->rsw_fg.fg_fid);
			wk->uswi_fcmh = fcmh;
			slmrcmthr_walk_bmaps(rsw, wk);
			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
			psc_pool_return(upsched_pool, wk);
		}

		/* signal EOF */
		slm_rcm_issue_getreplst(rsw, NULL, 1);

		sl_csvc_decref(rsw->rsw_csvc);
		PSCFREE(rsw);
	}
}
