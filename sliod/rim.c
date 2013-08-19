/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for handling RPC requests for ION from MDS.
 */

#include <errno.h>
#include <stdio.h>

#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"
#include "pfl/time.h"
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

uint64_t	current_reclaim_xid;
uint64_t	current_reclaim_batchno;

int
sli_rim_handle_batch(struct pscrpc_request *rq)
{
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	int rc;

	SL_RSX_ALLOCREP(rq, mq, mp);

	switch (mq->opc) {
#ifdef HAVE_FALLOC_FL_PUNCH_HOLE
	case SRMT_PRECLAIM: {
		struct srt_preclaim_ent *pe;
		struct iovec iov;
		void *buf;
		int i;

		OPSTAT_INCR(SLI_OPST_HANDLE_PRECLAIM);

		iov.iov_len = mq->nents * sizeof(*pe);
		iov.iov_base = buf = PSCALLOC(iov.iov_len);

		rc = rsx_bulkserver(rq, BULK_GET_SINK, SRIM_BULK_PORTAL,
		    &iov, 1);
		if (rc)
			PFL_GOTOERR(preclaim_out, rc);

		for (pe = buf, i = 0; i < mq->nents; pe++) {
			struct fidc_membh *f;

			mp->rc = sli_fcmh_get(&pe->fg, &f);
			if (mp->rc)
				break;

			/* XXX clear out sliver pages in memory */

			/* XXX lock */
			if (fallocate(fcmh_2_fd(f),
			    HAVE_FALLOC_FL_PUNCH_HOLE,
			    pe->bno * SLASH_BMAP_SIZE,
			    SLASH_BMAP_SIZE) == -1)
				mp->rc = -errno;

			fcmh_op_done(f);
		}

 preclaim_out:
		PSCFREE(buf);
	    }
#endif
	default:
		rc = -PFLERR_NOTSUP;
		break;
	}
	return (rc);
}

/**
 * sli_rim_handle_reclaim - Handle RECLAIM RPC from the MDS as a result
 *	of unlink or truncate to zero.  The MDS won't send us a new RPC
 *	until we reply, so we should be thread-safe.
 */
int
sli_rim_handle_reclaim(struct pscrpc_request *rq)
{
	int i, rc = 0, len;
	char fidfn[PATH_MAX];
	uint64_t crc, xid, batchno;
	struct srt_reclaim_entry *entryp;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct timeval t0, t1, td;
	struct iovec iov;

	len = sizeof(struct srt_reclaim_entry);

	OPSTAT_INCR(SLI_OPST_RECLAIM);
	SL_RSX_ALLOCREP(rq, mq, mp);

	// XXX adjust for RPC overhead in metric?
	if (mq->size < len || mq->size > LNET_MTU)
		PFL_GOTOERR(out, rc = -EINVAL);
	if (mq->count != mq->size / len)
		PFL_GOTOERR(out, rc = -EINVAL);

	xid = mq->xid;
	batchno = mq->batchno;
	if (xid > current_reclaim_xid)
		current_reclaim_xid = xid;
	if (batchno > current_reclaim_batchno) {
		psclog_info("reclaim batchno advances from %"PRId64" to "
		    "%"PRId64, current_reclaim_batchno, batchno);
		current_reclaim_batchno = batchno;
	}

	PFL_GETTIMEVAL(&t0);

	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	rc = rsx_bulkserver(rq, BULK_GET_SINK, SRIM_BULK_PORTAL,
	    &iov, 1);
	if (rc)
		PFL_GOTOERR(out, rc);

	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	entryp = iov.iov_base;
	for (i = 0; i < mq->count; i++) {
		sli_fg_makepath(&entryp->fg, fidfn);

		/*
		 * We do upfront garbage collection, so ENOENT should be
		 * fine.  Also simply creating a file without any I/O
		 * won't create a backing file on the I/O server.
		 *
		 * Anyway, we don't report an error back to MDS because
		 * it can do nothing.  Reporting an error can stall mds
		 * progress.
		 */
		OPSTAT_INCR(SLI_OPST_RECLAIM_FILE);
		if (unlink(fidfn) == -1 && errno != ENOENT) {
			mp->rc = -errno;
			psclog_errorx("error reclaiming %s "
			    "xid=%"PRId64" rc=%d",
			    fidfn, entryp->xid, rc);
		} else
			psclog_info("reclaimed %s "
			    "xid=%"PRId64" successfully",
			    fidfn, entryp->xid);

		entryp = PSC_AGP(entryp, len);
	}
	PFL_GETTIMEVAL(&t1);

	timersub(&t1, &t0, &td);
	if (td.tv_sec > 1)
		psclogs_notice(PSS_TMP,
		    "reclaim processing took %ld.%03ld second(s)",
		    (long)td.tv_sec, (long)td.tv_usec / 1000);

 out:
	PSCFREE(iov.iov_base);
	return (rc);
}

int
sli_rim_handle_repl_schedwk(struct pscrpc_request *rq)
{
	const struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct sl_resource *res;

	OPSTAT_INCR(SLI_OPST_HANDLE_REPL_SCHED);
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
		return (-PFLERR_NOTSUP);
		rc = sli_rim_handle_bmap_ptrunc(rq);
		break;
	case SRMT_BATCH_RQ:
		rc = sli_rim_handle_batch(rq);
		break;
	case SRMT_RECLAIM:
		rc = sli_rim_handle_reclaim(rq);
		break;
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRIM_MAGIC, SRIM_VERSION,
		    SLCONNT_MDS);
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
