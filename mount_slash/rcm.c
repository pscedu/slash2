/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2007-2018, Pittsburgh Supercomputing Center
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
		if (mrsq->mrsq_id == id)
			break;
	PLL_ULOCK(&msctl_replsts);
	return (mrsq);
}

void
mrsq_release(struct msctl_replstq *mrsq, int rc)
{
	psclog_diag("release: mrsq@%p rc=%d", mrsq, rc);

	spinlock(&mrsq->mrsq_lock);
	if (rc == 0) {
		freelock(&mrsq->mrsq_lock);
		return;
	}
	/* only wake up in case of error or EOF */
	mrsq->mrsq_rc = rc;
	psc_waitq_wakeall(&mrsq->mrsq_waitq);
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
	struct msctlmsg_replst mrs;		/* > 4KiB stack usage */
	struct msctl_replstq *mrsq;
	struct psc_ctlmsghdr mh;
	struct sl_resource *res;
	int n;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL) {
		psclog_warnx("Handle GETREPLST: id = %d, rc = %d", 
		    mq->id, mq->rc);
		mp->rc = -PFLERR_CANCELED;
		return (mp->rc);
	}

	mh = *mrsq->mrsq_mh;

	mrs.mrs_fid = mq->fg.fg_fid;

	if (mq->rc) {
		/* XXX no need to send msctl an EOF message? */
		if (mq->rc != EOF)
			psclog_warnx("release: mrsq@%p: id=%d, rc=%d", 
			    mrsq, mp->id, mq->rc);
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
	(void) psc_ctlmsg_sendv(mrsq->mrsq_fd, &mh, &mrs, mrsq->mrsq_fdlock);
	/*
 	 * We used to call mrsq_release() here. And if msctl exits for some
 	 * reason (e.g., files are moved), we are going to drop the mrsq,
 	 * causing a lot of PFLERR_CANCELED later.
 	 */
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
	int rc, level;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL) {
		psclog_warnx("Handle GETREPLST_SLAVE: id = %d, rc = %d", 
		    mq->id, mq->rc);
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
		    mq->len + sizeof(*mrsl), mrsl, mrsq->mrsq_fdlock);
		rc = rc ? 0 : EOF;
	}

 out:
	level = (mq->rc && mq->rc != EOF) ? PLL_WARN : PLL_DIAG;
	psclog(level, "Handle GETREPLST_SLAVE: id = %d, rc = %d", mq->id, mq->rc);
	PSCFREE(mrsl);
	return (mp->rc);
}

/*
 * Handle a SRMT_RELEASEBMAP request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	uint32_t i;
	struct srt_bmapdesc *sbd;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct bmapc_memb *b = NULL;
	struct fidc_membh *f = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->nbmaps > MAX_BMAP_RELEASE) {
		mp->rc = -EINVAL;
		return (0);
	}

	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];
		mp->rc = -sl_fcmh_peek_fg(&sbd->sbd_fg, &f);
		if (mp->rc)
			break;

		mp->rc = -bmap_lookup(f, sbd->sbd_bmapno, &b);
		if (mp->rc)
			break;

		/*
 		 * We can't flush dirty data at this moment because
 		 * our MDS thread is waiting and we don't know how
 		 * long the flush will take.
 		 */
		b->bcm_flags |= BMAPF_LEASEEXPIRE;
		msl_bmap_cache_rls(b);
		bmap_op_done(b);
		fcmh_op_done(f);
		OPSTAT_INCR("msl.bmap_reclaim");
		b = NULL;
		f = NULL;
	}
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
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
	    slc_getmcsvcxf(_resm, 0, rq->rq_export, 0));
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
		psclog_errorx("unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
	slrpc_rep_out(rq);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
