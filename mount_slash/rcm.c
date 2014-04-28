/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2014, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Routines for handling RPC requests for CLI from MDS.
 */

#include "pfl/str.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"
#include "pfl/log.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_cli.h"
#include "ctl_cli.h"
#include "ctlsvr_cli.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

struct msctl_replstq *
mrsq_lookup(int id)
{
	struct msctl_replstq *mrsq;

	PLL_LOCK(&msctl_replsts);
	PLL_FOREACH(mrsq, &msctl_replsts)
		if (mrsq->mrsq_id == id) {
			spinlock(&mrsq->mrsq_lock);
			mrsq->mrsq_refcnt++;
			psclog_debug("mrsq@%p ref=%d incref", mrsq,
			    mrsq->mrsq_refcnt);
			freelock(&mrsq->mrsq_lock);
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	return (mrsq);
}

void
mrsq_release(struct msctl_replstq *mrsq, int rc)
{
	(void)reqlock(&mrsq->mrsq_lock);
	if (mrsq->mrsq_rc == 0)
		mrsq->mrsq_rc = rc;
	psc_assert(mrsq->mrsq_refcnt > 0);
	if (--mrsq->mrsq_refcnt == 0)
		psc_waitq_wakeall(&mrsq->mrsq_waitq);
	psclog_debug("mrsq@%p ref=%d rc=%d decref", mrsq,
	    mrsq->mrsq_refcnt, rc);
	freelock(&mrsq->mrsq_lock);
}

/**
 * msrcm_handle_getreplst - Handle a GETREPLST request for CLI from MDS,
 *	which would have been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctlmsg_replst mrs;
	struct msctl_replstq *mrsq;
	struct psc_ctlmsghdr mh;
	struct sl_resource *res;
	int rc, n;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL)
		return (0);

	mh = *mrsq->mrsq_mh;

	mrs.mrs_fid = mq->fg.fg_fid;

	if (mq->rc) {
		mrsq_release(mrsq, mq->rc);
		return (0);
	}
	mrs.mrs_newreplpol = mq->newreplpol;
	mrs.mrs_nios = mq->nrepls;
	for (n = 0; n < (int)mq->nrepls; n++) {
		res = libsl_id2res(mq->repls[n].bs_id);
		if (res)
			strlcpy(mrs.mrs_iosv[n], res->res_name,
			    sizeof(mrs.mrs_iosv[0]));
		else
			snprintf(mrs.mrs_iosv[n],
			    sizeof(mrs.mrs_iosv[0]),
			    "<unknown IOS %#x>",
			    mq->repls[n].bs_id);
	}
	rc = psc_ctlmsg_sendv(mrsq->mrsq_fd, &mh, &mrs);
	mrsq_release(mrsq, rc ? 0 : EOF);
	return (0);
}

/**
 * msrcm_handle_getreplst_slave - Handle a GETREPLST request for CLI
 *	from MDS, which would have been initiated by a client request
 *	originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst_slave(struct pscrpc_request *rq)
{
	struct msctlmsg_replst_slave *mrsl = NULL;
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct msctl_replstq *mrsq;
	struct iovec iov;
	int rc;

	SL_RSX_ALLOCREP(rq, mq, mp);
	rc = mq->rc;
	if (rc == EOF)
		rc = 0;

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL) {
		mp->rc = -ECANCELED;
		return (mp->rc);
	}

	if (mq->rc && mq->rc != EOF)
		goto out;
	if (mq->len < 0 || mq->len > SRM_REPLST_PAGESIZ)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mrsl = PSCALLOC(sizeof(*mrsl) + mq->len);
	mrsl->mrsl_fid = mrsq->mrsq_fid;
	mrsl->mrsl_boff = mq->boff;
	mrsl->mrsl_nbmaps = mq->nbmaps;
	if (mq->rc == EOF)
		mrsl->mrsl_flags |= MRSLF_EOF;

	if (mq->len > (int)sizeof(mq->buf)) {
		iov.iov_base = mrsl->mrsl_data;
		iov.iov_len = mq->len;
		mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK,
		    SRCM_BULK_PORTAL, &iov, 1);
	} else if (mq->len)
		memcpy(mrsl->mrsl_data, mq->buf, mq->len);
	if (mp->rc == 0) {
		rc = psc_ctlmsg_send(mrsq->mrsq_fd,
		    mrsq->mrsq_mh->mh_id, MSCMT_GETREPLST_SLAVE,
		    mq->len + sizeof(*mrsl), mrsl);
		rc = rc ? 0 : EOF;
	}

 out:
	mrsq_release(mrsq, rc);
	PSCFREE(mrsl);
	return (mp->rc);
}

/**
 * msrcm_handle_releasebmap - Handle a RELEASEBMAP request for CLI from
	MDS.
 * @rq: request.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

/**
 * msrcm_handle_bmap_wake - Handle a BMAP_WAKE request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_bmap_wake(struct pscrpc_request *rq)
{
	struct srm_bmap_wake_req *mq;
	struct srm_bmap_wake_rep *mp;
	struct fidc_membh *c = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = fidc_lookup_load(mq->fg.fg_fid, &c, NULL);
	if (mp->rc)
		goto out;
	if (c->fcmh_flags & FCMH_CLI_TRUNC) {
		FCMH_LOCK(c);
		c->fcmh_flags &= ~FCMH_CLI_TRUNC;
		fcmh_wake_locked(c);
		FCMH_ULOCK(c);
	}

 out:
	if (c)
		fcmh_op_done(c);
	return (0);
}

/**
 * msrcm_handle_bmapdio - Handle a BMAPDIO request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_bmapdio(struct pscrpc_request *rq)
{
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	struct bmapc_memb *b = NULL;
	struct fidc_membh *f = NULL;
	struct bmap_cli_info *bci;

	SL_RSX_ALLOCREP(rq, mq, mp);

	OPSTAT_INCR(SLC_OPST_BMAP_DIO);
	psclog_info("fid="SLPRI_FID" bmapno=%u seq=%"PRId64,
	    mq->fid, mq->blkno, mq->seq);

	/*
	 * XXX it is possible this fcmh won't be in the cache -- force a
	 * load?
	 */
	mp->rc = fidc_lookup_fid(mq->fid, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	DEBUG_FCMH(PLL_DEBUG, f, "bmapno=%u seq=%"PRId64,
	    mq->blkno, mq->seq);

	mp->rc = bmap_lookup(f, mq->blkno, &b);
	if (mp->rc)
		goto out;

	DEBUG_BMAP(PLL_DEBUG, b, "seq=%"PRId64, mq->seq);

	BMAP_LOCK(b);
	if (b->bcm_flags & BMAP_DIO)
		goto out;

	/* Verify that the sequence number matches. */
	bci = bmap_2_bci(b);
	if (bci->bci_sbd.sbd_seq != mq->seq)
		PFL_GOTOERR(out, mp->rc = -ESTALE);

	/* All new read and write I/O's will get BIORQ_DIO. */
	b->bcm_flags |= BMAP_DIO;
	BMAP_ULOCK(b);

	msl_bmap_cache_rls(b);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slc_rcm_handle_readdir(struct pscrpc_request *rq)
{
	struct srm_readdir_ra_req *mq;
	struct srm_readdir_ra_rep *mp;
	struct fidc_membh *d = NULL;
	struct dircache_page *p;
	struct iovec iov[2];
	int stale=0;

	memset(iov, 0, sizeof(iov));

	SL_RSX_ALLOCREP(rq, mq, mp);

//	if (mq->num >=  || mq->size >= )
//		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mp->rc = fidc_lookup_fg(&mq->fg, &d);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	p = dircache_new_page(d, mq->offset, 0);
	if (p == NULL)
		PFL_GOTOERR(out, mp->rc = -ECANCELED);

	iov[0].iov_base = PSCALLOC(mq->size);
	iov[0].iov_len = mq->size;

	iov[1].iov_len = mq->num * sizeof(struct srt_readdir_ent);
	iov[1].iov_base = PSCALLOC(iov[1].iov_len);

	if (mq->size)
		mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK,
		    SRCM_BULK_PORTAL, iov, nitems(iov));

	if (mq->fg.fg_gen != fcmh_2_gen(d)) {
		stale = 1;
		if (!mp->rc)
			mp->rc = -ESTALE;
		OPSTAT_INCR(SLC_OPST_READDIR_STALE);
	}
	if (mp->rc || stale) {
		msl_readdir_error(d, p, mp->rc);
		PSCFREE(iov[0].iov_base);
	} else
		msl_readdir_finish(d, p, mq->eof, mq->num, mq->size,
		    iov);

	PSCFREE(iov[1].iov_base);

 out:
	if (d)
		fcmh_op_done(d);
	if (mp->rc)
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	return (mp->rc);
}

/**
 * slc_rcm_handler - Handle a request for CLI from MDS.
 * @rq: request.
 */
int
slc_rcm_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slc_getmcsvcx(_resm, 0, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRCM_MAGIC, SRCM_VERSION,
		    SLCONNT_MDS);
		break;

	case SRMT_REPL_GETST:
		rc = msrcm_handle_getreplst(rq);
		break;
	case SRMT_REPL_GETST_SLAVE:
		rc = msrcm_handle_getreplst_slave(rq);
		break;

	case SRMT_READDIR:
		rc = slc_rcm_handle_readdir(rq);
		break;

	case SRMT_RELEASEBMAP:
		rc = msrcm_handle_releasebmap(rq);
		break;
	case SRMT_BMAP_WAKE:
		rc = msrcm_handle_bmap_wake(rq);
		break;
	case SRMT_BMAPDIO:
		rc = msrcm_handle_bmapdio(rq);
		break;

	default:
		psclog_errorx("unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
