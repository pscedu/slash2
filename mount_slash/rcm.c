/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for handling RPC requests for CLI from MDS.
 */

#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/str.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_cli.h"
#include "ctl_cli.h"
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
	psclog_warn("lookup: mrsq@%p ref=%d", mrsq, mrsq->mrsq_refcnt);
	return (mrsq);
}

void
mrsq_release(struct msctl_replstq *mrsq, int rc)
{
	int wake = 0;
	(void)reqlock(&mrsq->mrsq_lock);
	if (mrsq->mrsq_rc == 0) {
		wake = 1;
		mrsq->mrsq_rc = rc;
	}
	psc_assert(mrsq->mrsq_refcnt > 0);
	if (--mrsq->mrsq_refcnt == 0)
		wake = 1;

	if (wake)
		psc_waitq_wakeall(&mrsq->mrsq_waitq);
	psclog_warn("release: mrsq@%p ref=%d rc=%d", mrsq,
	    mrsq->mrsq_refcnt, rc);
	freelock(&mrsq->mrsq_lock);
}

/*
 * Handle a SRMT_REPL_GETST request for CLI from MDS, which would have 
 * been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctlmsg_replst mrs;		/* XXX big stack usage */
	struct msctl_replstq *mrsq;
	struct psc_ctlmsghdr mh;
	struct sl_resource *res;
	int rc, n;

	SL_RSX_ALLOCREP(rq, mq, mp);

	psclog_warnx("Handle GETREPLST: id = %d, rc = %d", mq->id, mq->rc);
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
	psclog_warnx("Handle GETREPLST: id = %d, rc = %d, fd = %d - reply1", mq->id, mrsq->mrsq_fd, mq->rc);
	rc = psc_ctlmsg_sendv(mrsq->mrsq_fd, &mh, &mrs);
	psclog_warnx("Handle GETREPLST: id = %d, rc = %d, fd = %d - reply2", mq->id, mrsq->mrsq_fd, mq->rc);
	mrsq_release(mrsq, rc ? 0 : EOF);
	psclog_warnx("Handle GETREPLST: id = %d, rc = %d - reply3", mq->id, mq->rc);
	return (0);
}

/*
 * Handle a SRMT_REPL_GETST_SLAVE request for CLI from MDS, which would 
 * have been initiated by a client request originally.
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

	psclog_warn("Handle GETREPLST_SLAVE: id = %d, rc = %d", mq->id, rc);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL) {
		mp->rc = -PFLERR_CANCELED;
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

/*
 * Handle a RELEASEBMAP request for CLI from MDS.
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

/*
 * Handle a BMAP_WAKE request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_bmap_wake(struct pscrpc_request *rq)
{
	struct srm_bmap_wake_req *mq;
	struct srm_bmap_wake_rep *mp;
	struct fidc_membh *c = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = -sl_fcmh_peek_fid(mq->fg.fg_fid, &c);
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

/*
 * Handle a BMAPDIO request for CLI from MDS.
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

	psclog_diag("fid="SLPRI_FID" bmapno=%u seq=%"PRId64,
	    mq->fid, mq->bno, mq->seq);

	mp->rc = -sl_fcmh_peek_fid(mq->fid, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	DEBUG_FCMH(PLL_DEBUG, f, "bmapno=%u seq=%"PRId64,
	    mq->bno, mq->seq);

	mp->rc = -bmap_lookup(f, mq->bno, &b);
	if (mp->rc)
		goto out;

	DEBUG_BMAP(PLL_DEBUG, b, "seq=%"PRId64, mq->seq);

	if (b->bcm_flags & BMAPF_DIO)
		goto out;

	/* Verify that the sequence number matches. */
	bci = bmap_2_bci(b);
	if (bci->bci_sbd.sbd_seq != mq->seq)
		PFL_GOTOERR(out, mp->rc = -PFLERR_STALE);

	/* All new read and write I/O's will get BIORQ_DIO. */
	b->bcm_flags |= BMAPF_DIO;
	BMAP_ULOCK(b);

	msl_bmap_cache_rls(b);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

/*
 * Handle a request for CLI from MDS.
 * @rq: request.
 */
int
slc_rcm_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slc_getmcsvcxf(_resm, 0, rq->rq_export));
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
	slrpc_rep_out(rq);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
