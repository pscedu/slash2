/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Routines for issuing RPC requests to CLI from MDS.
 */

#define PSC_SUBSYS PSS_RPC

#include <sys/param.h>

#include <dirent.h>

#include "pfl/bitflag.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/thread.h"

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

struct psc_listcache	 slm_replst_workq;

int
slmrmcthr_replst_slave_eof(struct slm_replst_workreq *rsw,
    struct fidc_membh *f)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = SL_RSX_NEWREQ(rsw->rsw_csvc, SRMT_REPL_GETST_SLAVE, rq, mq,
	    mp);
	if (rc)
		return (rc);

	mq->fg = f->fcmh_fg;
	mq->id = rsw->rsw_cid;
	mq->rc = EOF;
	rc = SL_RSX_WAITREP(rsw->rsw_csvc, rq, mp); // async
	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrmcthr_replst_slave_fin(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, struct fidc_membh *f)
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
	    fcmh_2_nrepls(f) + SL_NBITS_REPLST_BHDR);

	/* use piggyback or bulk transfer */
	if (nb > sizeof(mq->buf)) {
		iov.iov_base = srcm->srcm_page;
		iov.iov_len = nb;
		rc = slrpc_bulkclient(rq, BULK_GET_SOURCE,
		    SRCM_BULK_PORTAL, &iov, 1);
	} else
		memcpy(mq->buf, srcm->srcm_page, nb);
	if (rc == 0) {
		rc = SL_RSX_WAITREP(csvc, rq, mp); // async
		if (rc == 0)
			rc = mp->rc;
	}

	memset(srcm->srcm_page, 0, nb);

	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrcmthr_walk_brepls(struct slm_replst_workreq *rsw,
    struct fidc_membh *f, struct bmapc_memb *b, sl_bmapno_t n,
    struct pscrpc_request **rqp)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct srt_replst_bhdr bhdr;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;
	int nbits, rc;

	thr = pscthr_get();
	srcm = slmrcmthr(thr);
	nbits = fcmh_2_nrepls(f) * SL_BITS_PER_REPLICA +
	    SL_NBITS_REPLST_BHDR;
	if (howmany(srcm->srcm_page_bitpos + nbits,
	    NBBY) > SRM_REPLST_PAGESIZ || *rqp == NULL) {
		/* finish previous RPC if any */
		if (*rqp) {
			rc = slmrmcthr_replst_slave_fin(
			    rsw->rsw_csvc, *rqp, f);
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
		mq->fg = f->fcmh_fg;

		srcm->srcm_page_bitpos = 0;
	}
	memset(&bhdr, 0, sizeof(bhdr));
	bhdr.srsb_replpol = bmap_2_replpol(b);
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos,
	    &bhdr, 0, SL_NBITS_REPLST_BHDR);
	pfl_bitstr_copy(srcm->srcm_page, srcm->srcm_page_bitpos +
	    SL_NBITS_REPLST_BHDR, bmi->bmi_repls, 0,
	    fcmh_2_nrepls(f) * SL_BITS_PER_REPLICA);
	srcm->srcm_page_bitpos += nbits;
	return (0);
}

/*
 * Issue a GETREPLST reply to a CLI from MDS.
 */
int
slm_rcm_issue_getreplst(struct slm_replst_workreq *rsw,
    struct fidc_membh *f)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	/* this is handled by msrcm_handle_getreplst() */
	rc = SL_RSX_NEWREQ(rsw->rsw_csvc, SRMT_REPL_GETST, rq, mq, mp);
	if (rc)
		return (rc);
	mq->id = rsw->rsw_cid;
	if (f) {
		mq->fg = f->fcmh_fg;
		mq->nrepls = fcmh_2_nrepls(f);
		mq->newreplpol = fcmh_2_replpol(f);
		memcpy(mq->repls, fcmh_2_ino(f)->ino_repls,
		    MIN(mq->nrepls, SL_DEF_REPLICAS) *
		    sizeof(mq->repls[0]));
		if (mq->nrepls > SL_DEF_REPLICAS) {
			mds_inox_ensure_loaded(fcmh_2_inoh(f));
			memcpy(mq->repls + SL_DEF_REPLICAS,
			    fcmh_2_inox(f)->inox_repls,
			    (fcmh_2_nrepls(f) - SL_DEF_REPLICAS) *
			    sizeof(mq->repls[0]));
		}
	} else
		mq->rc = EOF;

	rc = SL_RSX_WAITREP(rsw->rsw_csvc, rq, mp); // XXX async
	pscrpc_req_finished(rq);
	return (rc);
}

int
slmrcmthr_walk_bmaps(struct slm_replst_workreq *rsw,
    struct fidc_membh *f)
{
	struct pscrpc_request *rq = NULL;
	struct bmapc_memb *b;
	sl_bmapno_t n;
	int rc, rc2;

	rc = slm_rcm_issue_getreplst(rsw, f);
	if (rc)
		return (rc);
	if (fcmh_isreg(f)) {
		for (n = 0; rc == 0; n++) {
			/*
			 * Client write-ahead can create empty bmaps
			 * after EOF.  These shouldn't be shown; dumpfid
			 * can be used to examine the entire file for
			 * system debugging.
			 */
			if (n >= fcmh_nvalidbmaps(f))
				    break;

			rc = bmap_getf(f, n, SL_WRITE, BMAPGETF_CREATE |
			    BMAPGETF_NOAUTOINST, &b);
			if (rc == SLERR_BMAP_INVALID)
				break;
			if (rc) {
				// XXX send sl_strerror(rc) message
				break;
			}

			rc = slmrcmthr_walk_brepls(rsw, f, b, n, &rq);
			bmap_op_done(b);
		}
		if (rq) {
			rc2 = slmrmcthr_replst_slave_fin(
			    rsw->rsw_csvc, rq, f);
			if (rc == 0)
				rc = rc2;
		}
	}
	/*
 	 * This appears to be a separate RPC to indicate EOF.
 	 */
	rc2 = slmrmcthr_replst_slave_eof(rsw, f);
	if (rc == 0)
		rc = rc2;
	return (rc);
}

int
slmrcmthr_walk(struct slm_sth *sth, void *p)
{
	struct psc_dynarray *da = p;
	slfid_t fid;

	fid = sqlite3_column_int64(sth->sth_sth, 0);
	psc_dynarray_add(da, (void *)fid);
	return (0);
}

/*
 * Wait for work posted by slm_rmc_handle_getreplst().
 */
void
slmrcmthr_main(struct psc_thread *thr)
{
	struct slm_replst_workreq *rsw;
	struct slmrcm_thread *srcm;
	struct fidc_membh *f;
	int rc;

	srcm = slmrcmthr(thr);
	while (pscthr_run(thr)) {
		/* handle requests for SRMT_REPL_GETST */
		rsw = lc_getwait(&slm_replst_workq);
		srcm->srcm_page_bitpos = SRM_REPLST_PAGESIZ * NBBY;

		OPSTAT_INCR("replst");
		rc = slm_fcmh_get(&rsw->rsw_fg, &f);
		if (!rc) {
			slmrcmthr_walk_bmaps(rsw, f);
			fcmh_op_done(f);
		}

		/*
		 * XXX We should return error code other than EOF as well. The
		 * client side should be able to handle this. Right now, msctl
		 * just returns nothing in case of an error.
		 */

		/* signal EOF */
		slm_rcm_issue_getreplst(rsw, NULL);

		sl_csvc_decref(rsw->rsw_csvc);
		psc_pool_return(slm_repl_status_pool, rsw);
	}
}
