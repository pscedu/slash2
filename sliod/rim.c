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
 * Routines for handling RPC requests for ION from MDS.
 */

#include <errno.h>
#include <stdio.h>

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/lock.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_iod.h"
#include "fidc_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "sliod.h"

static uint64_t	current_reclaim_xid;
static uint64_t	current_reclaim_batchno;

/**
 * sli_rim_handle_reclaim - Handle RECLAIM RPC from the MDS as a result
 *	of unlink or truncate to zero.  The MDS won't send us a new RPC
 *	until we reply, so we should be thread-safe.
 */
int
sli_rim_handle_reclaim(struct pscrpc_request *rq)
{
	struct srt_reclaim_entry *entryp;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct iovec iov;
	char fidfn[PATH_MAX];
	int i, rc, len;
	uint64_t crc, xid, batchno;

	len = offsetof(struct srt_reclaim_entry, _pad);

	OPSTAT_INCR(SLI_OPST_RECLAIM);
	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->size < len || mq->size > LNET_MTU)
		return (EINVAL);

	if (mq->count != mq->size / len)
		return (EINVAL);

	xid = mq->xid;
	batchno = mq->batchno;
	if (xid > current_reclaim_xid)
		current_reclaim_xid = xid;
	if (batchno > current_reclaim_batchno) {
		psclog_info("reclaim batchno advances from %"PRId64" to "
		    "%"PRId64, current_reclaim_batchno, batchno);
		current_reclaim_batchno = batchno;
	}

	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	mp->rc = rsx_bulkserver(rq, BULK_GET_SINK, SRMM_BULK_PORTAL,
	    &iov, 1);
	if (mp->rc)
		goto out;

	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc) {
		mp->rc = -EINVAL;
		goto out;
	}

	entryp = iov.iov_base;
	for (i = 0; i < mq->count; i++) {
		sli_fg_makepath(&entryp->fg, fidfn);

		/*
		 * We do upfront garbage collection, so ENOENT should be
		 * fine.  Also simply creating a file without any I/O
		 * won't create a backing file on the I/O server.
		 *
		 * Anyway, we don't report an error back to MDS because
		 * it can do nothing.
		 */
		OPSTAT_INCR(SLI_OPST_RECLAIM_FILE);
		rc = unlink(fidfn);
		if (rc == -1) {
			OPSTAT_INCR(SLI_OPST_RECLAIM_FILE_FAIL);
			rc = errno;
		}

		psclog_info("reclaim fid="SLPRI_FG", xid=%"PRId64", rc=%d",
		    SLPRI_FG_ARGS(&entryp->fg), entryp->xid, rc);
		entryp = PSC_AGP(entryp, len);
	}
 out:
	PSCFREE(iov.iov_base);
	return (mp->rc);
}

int
sli_rim_handle_repl_schedwk(struct pscrpc_request *rq)
{
	const struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct sl_resource *res;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY)
		mp->rc = -EINVAL;
	else if (mq->len < 1 || mq->len > SLASH_BMAP_SIZE)
		mp->rc = -EINVAL;
	else {
		res = libsl_id2res(mq->src_resid);
		if (res == NULL)
			mp->rc = -SLERR_ION_UNKNOWN;
		else
			mp->rc = sli_repl_addwk(SLI_REPLWKOP_REPL, res,
			    &mq->fg, mq->bmapno, mq->bgen, mq->len);
	}
	return (0);
}

int
sli_rim_handle_bmap_ptrunc(struct pscrpc_request *rq)
{
	const struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->offset < 0 || mq->offset >= SLASH_BMAP_SIZE) {
		mp->rc = -EINVAL;
		return (0);
	}
	mp->rc = sli_repl_addwk(SLI_REPLWKOP_PTRUNC, NULL, &mq->fg,
	    mq->bmapno, mq->bgen, mq->offset);
	return (0);
}

int
sli_rim_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    sli_getmcsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_REPL_SCHEDWK:
		rc = sli_rim_handle_repl_schedwk(rq);
		break;
	case SRMT_BMAP_PTRUNC:
		rc = sli_rim_handle_bmap_ptrunc(rq);
		break;
	case SRMT_RECLAIM:
		rc = sli_rim_handle_reclaim(rq);
		break;
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRIM_MAGIC, SRIM_VERSION,
		    SLCONNT_MDS);
		break;
	default:
		psclog_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
