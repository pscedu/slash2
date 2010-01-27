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
 * Routines for issuing RPC requests for CLIENT from MDS.
 */

#include <sys/param.h>

#include <dirent.h>

#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"
#include "psc_util/thread.h"

#include "bmap_mds.h"
#include "fid.h"
#include "inodeh.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

int
slmrmcthr_replst_slave_eof(struct slm_replst_workreq *rsw)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = RSX_NEWREQ(rsw->rsw_csvc->csvc_import,
	    SRCM_VERSION, SRMT_REPL_GETST_SLAVE, rq, mq, mp);
	if (rc)
		return (rc);

	mq->fg = rsw->rsw_fg;
	mq->id = rsw->rsw_cid;
	mq->rc = EOF;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrmcthr_replst_slave_waitrep(struct pscrpc_request *rq, struct sl_replrq *rrq)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	struct iovec iov;
	int rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);

	iov.iov_base = srcm->srcm_page;
	iov.iov_len = howmany(srcm->srcm_page_bitpos, NBBY);

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mq->len = iov.iov_len;
	mq->nbmaps = srcm->srcm_page_bitpos / (SL_BITS_PER_REPLICA *
	    rrq->rrq_inoh->inoh_ino.ino_nrepls + SL_NBITS_REPLST_BHDR);
	rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
	    SRCM_BULK_PORTAL, &iov, 1);
	if (rc == 0)
		rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrcmthr_walk_brepls(struct slm_replst_workreq *rsw, struct sl_replrq *rrq,
    struct bmapc_memb *bcm, sl_blkno_t n, struct pscrpc_request **rqp)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct srsm_replst_bhdr bhdr;
	struct bmap_mds_info *bmdsi;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	int nbits, rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);
	bmdsi = bmap_2_bmdsi(bcm);

	nbits = rrq->rrq_inoh->inoh_ino.ino_nrepls *
	    SL_BITS_PER_REPLICA + SL_NBITS_REPLST_BHDR;
	if (howmany(srcm->srcm_page_bitpos + nbits,
	    NBBY) > SRM_REPLST_PAGESIZ || *rqp == NULL) {
		if (*rqp) {
			rc = slmrmcthr_replst_slave_waitrep(*rqp, rrq);
			*rqp = NULL;
			if (rc)
				return (rc);
		}

		rc = RSX_NEWREQ(rsw->rsw_csvc->csvc_import,
		    SRCM_VERSION, SRMT_REPL_GETST_SLAVE, *rqp, mq, mp);
		if (rc)
			return (rc);
		mq->id = rsw->rsw_cid;
		mq->boff = n;
		mq->fg = *REPLRQ_FG(rrq);

		srcm->srcm_page_bitpos = 0;
	}
	memset(&bhdr, 0, sizeof(bhdr));
	bhdr.srsb_repl_policy = bmdsi->bmdsi_repl_policy;
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos,
	    &bhdr, 0, SL_NBITS_REPLST_BHDR);
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos +
	    SL_NBITS_REPLST_BHDR, bmdsi->bmdsi_od->bh_repls, 0,
	    rrq->rrq_inoh->inoh_ino.ino_nrepls * SL_BITS_PER_REPLICA);
	srcm->srcm_page_bitpos += nbits;
	return (0);
}

/*
 * slm_rcm_issue_getreplst - issue a GETREPLST reply to a CLIENT from MDS.
 */
int
slm_rcm_issue_getreplst(struct slm_replst_workreq *rsw,
    struct sl_replrq *rrq, int is_eof)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = RSX_NEWREQ(rsw->rsw_csvc->csvc_import,
	    SRCM_VERSION, SRMT_REPL_GETST, rq, mq, mp);
	if (rc)
		return (rc);
	mq->id = rsw->rsw_cid;
	if (rrq) {
		mq->fg = *REPLRQ_FG(rrq);
		mq->nbmaps = REPLRQ_NBMAPS(rrq);
		mq->nrepls = REPLRQ_NREPLS(rrq);
		mq->newreplpol = REPLRQ_INOX(rrq)->inox_newbmap_policy;
		memcpy(mq->repls, REPLRQ_INO(rrq)->ino_repls,
		    MIN(mq->nrepls, INO_DEF_NREPLS) * sizeof(*mq->repls));
		if (mq->nrepls > INO_DEF_NREPLS)
			memcpy(mq->repls + INO_DEF_NREPLS,
			    rrq->rrq_inoh->inoh_extras->inox_repls,
			    (REPLRQ_NREPLS(rrq) - INO_DEF_NREPLS) *
			    sizeof(*mq->repls));
	}
	if (is_eof)
		mq->rc = EOF;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

void *
slmrcmthr_main(__unusedx void *arg)
{
	struct slm_replst_workreq *rsw;
	struct slmrcm_thread *srcm;
	struct pscrpc_request *rq;
	struct fidc_membh *fcmh;
	struct psc_thread *thr;
	struct bmapc_memb *bcm;
	struct sl_replrq *rrq;
	sl_blkno_t n;
	int rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);
	for (;;) {
		rsw = lc_getwait(&slm_replst_workq);

		srcm->srcm_page_bitpos = SRM_REPLST_PAGESIZ * NBBY;

		rc = 0;
		rq = NULL;
		if (rsw->rsw_fg.fg_fid == FID_ANY) {
			spinlock(&replrq_tree_lock);
			SPLAY_FOREACH(rrq, replrqtree, &replrq_tree) {
				slm_rcm_issue_getreplst(rsw, rrq, 0);
				for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
					if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
						continue;
					BMAP_LOCK(bcm);
					rc = slmrcmthr_walk_brepls(rsw, rrq, bcm, n, &rq);
					bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
					if (rc)
						break;
				}
				if (rq) {
					slmrmcthr_replst_slave_waitrep(rq, rrq);
					rq = NULL;
				}
				slmrmcthr_replst_slave_eof(rsw);
				if (rc)
					break;
			}
			freelock(&replrq_tree_lock);
		} else if ((rrq = mds_repl_findrq(&rsw->rsw_fg, NULL)) != NULL) {
			slm_rcm_issue_getreplst(rsw, rrq, 0);
			for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
				if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
					continue;
				BMAP_LOCK(bcm);
				rc = slmrcmthr_walk_brepls(rsw, rrq, bcm, n, &rq);
				bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
				if (rc)
					break;
			}
			if (rq)
				slmrmcthr_replst_slave_waitrep(rq, rrq);
			slmrmcthr_replst_slave_eof(rsw);
			mds_repl_unrefrq(rrq);
		} else if (mds_repl_loadino(&rsw->rsw_fg, &fcmh) == 0) {
			/*
			 * file is not in cache, load it up
			 * to report replication status
			 *
			 * grab a dummy replrq struct to pass around.
			 */
			rrq = psc_pool_get(replrq_pool);
			memset(rrq, 0, sizeof(*rrq));
			rrq->rrq_inoh = fcmh_2_inoh(fcmh);

			slm_rcm_issue_getreplst(rsw, rrq, 0);
			for (n = 0; n < REPLRQ_NBMAPS(rrq); n++) {
				if (mds_bmap_load(REPLRQ_FCMH(rrq), n, &bcm))
					continue;
				BMAP_LOCK(bcm);
				rc = slmrcmthr_walk_brepls(rsw, rrq, bcm, n, &rq);
				bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
				if (rc)
					break;
			}
			if (rq)
				slmrmcthr_replst_slave_waitrep(rq, rrq);
			slmrmcthr_replst_slave_eof(rsw);
			fidc_membh_dropref(fcmh);
			psc_pool_return(replrq_pool, rrq);
		}

		/* signal EOF */
		slm_rcm_issue_getreplst(rsw, NULL, 1);

		PSCFREE(rsw);
		sched_yield();
	}
	/* NOTREACHED */
}

/*
 * slm_rcm_issue_releasebmap - issue a RELEASEBMAP request to a CLIENT from MDS.
 */
int
slm_rcm_issue_releasebmap(struct pscrpc_import *imp)
{
	struct srm_releasebmap_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRCM_VERSION,
	    SRMT_RELEASEBMAP, rq, mq, mp)) != 0)
		return (rc);
	if ((rc = RSX_WAITREP(rq, mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}
