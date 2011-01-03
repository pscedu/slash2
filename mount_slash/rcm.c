/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2010, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for handling RPC requests for CLI from MDS.
 */

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_cli.h"
#include "ctl_cli.h"
#include "ctlsvr_cli.h"
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
		if (mrsq->mrsq_fd == id) {
			spinlock(&mrsq->mrsq_lock);
			mrsq->mrsq_refcnt++;
			freelock(&mrsq->mrsq_lock);
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	return (mrsq);
}

void
mrsq_release(struct msctl_replstq *mrsq, int ctlrc)
{
	reqlock(&mrsq->mrsq_lock);
	mrsq->mrsq_ctlrc = ctlrc;
	if (--mrsq->mrsq_refcnt == 0)
		psc_waitq_wakeall(&mrsq->mrsq_waitq);
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
	struct sl_resource *res;
	struct psc_ctlmsghdr mh;
	struct fidc_membh *fcmh;
	int rc, n;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL)
		return (0);

	mh = *mrsq->mrsq_mh;

	if (mrsq->mrsq_fn[0] != '\0')
		strlcpy(mrs.mrs_fn, mrsq->mrsq_fn, sizeof(mrs.mrs_fn));
	else {
		fcmh = fidc_lookup_fg(&mq->fg);
		if (fcmh) {
			/* file is in cache, try to recover filename */
			snprintf(mrs.mrs_fn, sizeof(mrs.mrs_fn),
			    SLPRI_FID, mq->fg.fg_fid);
			fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		} else {
			snprintf(mrs.mrs_fn, sizeof(mrs.mrs_fn),
			    SLPRI_FID, mq->fg.fg_fid);
		}
	}

	if (mq->rc) {
		rc = 1;
		if (mq->rc != EOF)
			rc = psc_ctlsenderr(mrsq->mrsq_fd, &mh, "%s",
			    slstrerror(mq->rc));
		spinlock(&mrsq->mrsq_lock);
		mrsq->mrsq_eof = 1;
		mrsq_release(mrsq, rc);
		return (0);
	}
	mrs.mrs_nbmaps = mq->nbmaps;
	mrs.mrs_newreplpol = mq->newreplpol;
	mrs.mrs_nios = mq->nrepls;
	for (n = 0; n < (int)mq->nrepls; n++) {
		res = libsl_id2res(mq->repls[n].bs_id);
		if (res)
			strlcpy(mrs.mrs_iosv[n], res->res_name, RES_NAME_MAX);
		else
			strlcpy(mrs.mrs_iosv[n], "<unknown IOS>", RES_NAME_MAX);
	}
	rc = psc_ctlmsg_sendv(mrsq->mrsq_fd, &mh, &mrs);
	mrsq_release(mrsq, rc);
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
	struct msctlmsg_replst_slave *mrsl;
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct msctl_replstq *mrsq;
	struct psc_ctlmsghdr mh;
	struct iovec iov;
	int rc;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mrsq = mrsq_lookup(mq->id);
	if (mrsq == NULL)
		return (-ECANCELED);

	mh = *mrsq->mrsq_mh;

	if (mq->rc) {
		rc = 1;
		if (mq->rc != EOF)
			rc = psc_ctlsenderr(mrsq->mrsq_fd, &mh, "%s",
			    slstrerror(mq->rc));
		spinlock(&mrsq->mrsq_lock);
		if (mq->rc != EOF)
			mrsq->mrsq_eof = 1;
		mrsq_release(mrsq, rc);
		return (0);
	}

	if (mq->len < 1 || mq->len > SRM_REPLST_PAGESIZ) {
		mp->rc = EINVAL;

		rc = psc_ctlsenderr(mrsq->mrsq_fd, &mh, "%s",
		    slstrerror(mq->rc));

		spinlock(&mrsq->mrsq_lock);
		mrsq->mrsq_eof = 1;
		mrsq_release(mrsq, rc);
		return (-mp->rc);
	}

	mrsl = PSCALLOC(sizeof(*mrsl) + mq->len);

	if (mrsq->mrsq_fn[0] != '\0')
		strlcpy(mrsl->mrsl_fn, mrsq->mrsq_fn, sizeof(mrsl->mrsl_fn));
	else
		/* XXX try to do a reverse lookup of pathname; check cache maybe? */
		snprintf(mrsl->mrsl_fn, sizeof(mrsl->mrsl_fn), SLPRI_FID,
		    mq->fg.fg_fid);

	iov.iov_base = mrsl->mrsl_data;
	iov.iov_len = mq->len;

	mrsl->mrsl_boff = mq->boff;
	mrsl->mrsl_nbmaps = mq->nbmaps;

	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1);

	if (mp->rc == 0) {
		rc = psc_ctlmsg_send(mrsq->mrsq_fd, mrsq->mrsq_mh->mh_id,
		    MSCMT_GETREPLST_SLAVE, mq->len + sizeof(*mrsl), mrsl);
	} else {
		rc = psc_ctlsenderr(mrsq->mrsq_fd, &mh, "%s",
		    slstrerror(mq->rc));

		spinlock(&mrsq->mrsq_lock);
		mrsq->mrsq_eof = 1;
	}
	mrsq_release(mrsq, rc);
	PSCFREE(mrsl);
	return (mp->rc);
}

/**
 * msrcm_handle_releasebmap - Handle a RELEASEBMAP request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

int
msrcm_handle_bmapdio(struct pscrpc_request *rq)
{
	struct srm_bmap_dio_req *mq;
	struct srm_generic_rep *mp;
	struct bmap_cli_info *bci;
	struct fidc_membh *f;
	struct bmapc_memb *b;

	SL_RSX_ALLOCREP(rq, mq, mp);

	psc_warnx("fid="SLPRI_FID" bmapno=%u seq=%"PRId64,
	    mq->fid, mq->blkno, mq->seq);

	f = fidc_lookup_fid(mq->fid);
	if (!f) {
		mp->rc = ENOENT;
		goto out;
	}

	DEBUG_FCMH(PLL_WARN, f, "bmapno=%u seq=%"PRId64, mq->blkno, mq->seq);

	mp->rc = bmap_lookup(f, mq->blkno, &b);
	if (mp->rc)
		goto out;

	DEBUG_BMAP(PLL_WARN, b, "seq=%"PRId64, mq->seq);

	BMAP_LOCK(b);
	if (b->bcm_flags & BMAP_DIO) {
		BMAP_ULOCK(b);
		goto out;
	}
	/* Verify that the sequence number matches.
	 */
	bci = bmap_2_bci(b);
	if (bci->bci_sbd.sbd_seq != mq->seq) {
		BMAP_ULOCK(b);
		mp->rc = ESTALE;
		goto out;
	}
	/* All new read and write IO's will get BIORQ_DIO.
	 */
	b->bcm_flags |= BMAP_DIO;
	BMAP_ULOCK(b);

	DEBUG_BMAP(PLL_WARN, b, "trying to dump the cache");
	msl_bmap_cache_rls(b);

 out:
	if (f)
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

/**
 * msrcm_handle_connect - Handle a CONNECT request for CLI from MDS.
 * @rq: request.
 */
int
msrcm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCM_MAGIC || mq->version != SRCM_VERSION)
		mp->rc = -EINVAL;
	return (0);
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
	    slc_getmcsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = msrcm_handle_connect(rq);
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
	case SRMT_BMAPDIO:
		rc = msrcm_handle_bmapdio(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
