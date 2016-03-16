/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
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
#include "pfl/ctlsvr.h"
#include "pfl/lock.h"

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
#include "slvr.h"

uint64_t	sli_current_reclaim_xid;
uint64_t	sli_current_reclaim_batchno;

/*
 * Handle SRMT_BATCH_RQ request from the MDS.
 */
int
sli_rim_handle_batch(struct pscrpc_request *rq)
{
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;
	void *buf;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->len < 1 || mq->len > LNET_MTU)
		return (mp->rc = -EINVAL);

	iov.iov_len = mq->len;
	iov.iov_base = buf = PSCALLOC(mq->len);
	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRIM_BULK_PORTAL, &iov,
	    1);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	switch (mq->opc) {
	case SRMT_REPL_SCHEDWK: {
		struct sli_batch_reply *bchrp;
		struct srt_replwk_reqent *pq;
		struct srt_replwk_repent *pp;

		OPSTAT_INCR("handle-repl-sched");

		bchrp = PSCALLOC(sizeof(*bchrp));
		bchrp->total = mq->len / sizeof(*pq);
		bchrp->buf = pp = PSCALLOC(bchrp->total * sizeof(*pp));
		bchrp->id = mq->bid;

		for (pq = buf;
		    (char *)pq < (char *)buf + mq->len;
		    pq++, pp++)
			sli_repl_addwk(SLI_REPLWKOP_REPL, pq->src_resid,
			    &pq->fg, pq->bno, pq->bgen, pq->len, bchrp, pp);
		break;
	    }

#ifdef HAVE_FALLOC_FL_PUNCH_HOLE
	case SRMT_PRECLAIM: {
		struct srt_preclaim_reqent *pq;

		OPSTAT_INCR("handle-preclaim");

		for (pq = buf;
		    (char *)pq < (char *)buf + mq->len;
		    pq++) {
			struct fidc_membh *f;

			mp->rc = sli_fcmh_get(&pq->fg, &f);
			if (mp->rc)
				continue;

			/* XXX clear out sliver pages in memory */

			/* XXX lock */
			if (fallocate(fcmh_2_fd(f),
			    HAVE_FALLOC_FL_PUNCH_HOLE, pq->bno *
			    SLASH_BMAP_SIZE, SLASH_BMAP_SIZE) == -1)
				mp->rc = -errno;

			fcmh_op_done(f);
		}
		break;
	    }
#endif
	default:
		mp->rc = -PFLERR_NOTSUP;
		break;
	}
 out:
	PSCFREE(buf);
	return (mp->rc);
}

/*
 * Handle RECLAIM RPC from the MDS as a result of unlink or truncate to zero.
 * The MDS won't send us a new RPC until we reply, so we should be thread-safe.
 */
int
sli_rim_handle_reclaim(struct pscrpc_request *rq)
{
	int i, rc = 0, len;
	char fidfn[PATH_MAX];
	uint64_t xid, batchno;
	struct srt_reclaim_entry *entryp;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct timeval t0, t1, td;
	struct iovec iov;

	len = sizeof(struct srt_reclaim_entry);

	OPSTAT_INCR("reclaim");
	SL_RSX_ALLOCREP(rq, mq, mp);

	// XXX adjust for RPC overhead in metric?
	if (mq->size < len || mq->size > LNET_MTU)
		PFL_GOTOERR(out, rc = -EINVAL);
	if (mq->count != mq->size / len)
		PFL_GOTOERR(out, rc = -EINVAL);

	xid = mq->xid;
	batchno = mq->batchno;
	if (xid > sli_current_reclaim_xid)
		sli_current_reclaim_xid = xid;
	if (batchno > sli_current_reclaim_batchno) {
		psclog_info("reclaim batchno advances from %"PRId64" to "
		    "%"PRId64, sli_current_reclaim_batchno, batchno);
		sli_current_reclaim_batchno = batchno;
	}

	PFL_GETTIMEVAL(&t0);

	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRIM_BULK_PORTAL, &iov, 1);
	if (rc)
		PFL_GOTOERR(out, rc);

	entryp = iov.iov_base;
	for (i = 0; i < mq->count; i++) {
		struct fidc_membh *f;

		if (sli_fcmh_peek(&entryp->fg, &f) == 0) {
			FCMH_LOCK(f);
			if (entryp->fg.fg_gen == fcmh_2_gen(f)) {
				if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
					close(fcmh_2_fd(f));
					fcmh_2_fd(f) = -1;
					f->fcmh_flags &= ~FCMH_IOD_BACKFILE;
					OPSTAT_INCR("reclaim-close");
				}
				OPSTAT_INCR("slvr-remove-reclaim");
				slvr_remove_all(f);
			}
			fcmh_op_done(f);
		}

		sli_fg_makepath(&entryp->fg, fidfn);

		/*
		 * We do upfront garbage collection, so ENOENT should be fine.
		 * Also simply creating a file without any I/O won't create a
		 * backing file on the I/O server.
		 *
		 * Anyway, we don't report an error back to MDS because it can
		 * do nothing.  Reporting an error can stall MDS progress.
		 */
		OPSTAT_INCR("reclaim-file");
		if (unlink(fidfn) == -1 && errno != ENOENT) {
			mp->rc = -errno;
			psclog_errorx("error reclaiming %s "
			    "xid=%"PRId64" rc=%d",
			    fidfn, entryp->xid, rc);
		} else
			psclog_diag("reclaimed %s "
			    "xid=%"PRId64" successfully",
			    fidfn, entryp->xid);

		entryp = PSC_AGP(entryp, len);
	}
	PFL_GETTIMEVAL(&t1);

	timersub(&t1, &t0, &td);
	psclog(td.tv_sec >= 1 ? PLL_NOTICE : PLL_DIAG,
	    "reclaim processing for "
	    "batchno %"PRId64" (%d files) took %ld.%01ld "
	    "second(s)", batchno, mq->count, (long)td.tv_sec,
	    (long)td.tv_usec / 1000);

 out:
	PSCFREE(iov.iov_base);
	return (rc);
}

int
sli_rim_handle_bmap_ptrunc(struct pscrpc_request *rq)
{
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct fidc_membh *f;
	struct sl_fidgen *fgp;
	off_t size;
	int fd;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->offset < 0 || mq->offset >= SLASH_BMAP_SIZE) {
		mp->rc = -EINVAL;
		return (0);
	}

	fgp = &mq->fg;

	mp->rc = sli_fcmh_get(fgp, &f);
	if (mp->rc)
		return (mp->rc);

	fd = fcmh_2_fd(f);
	size = SLASH_BMAP_SIZE * mq->bmapno + mq->offset;
	if (ftruncate(fd, size) == -1) {
		mp->rc = pflrpc_portable_errno(-errno);
		DEBUG_FCMH(PLL_ERROR, f, "truncate failed; rc=%d",
		    mp->rc);
		OPSTAT_INCR("ftruncate-failure");
	} else
		OPSTAT_INCR("ftruncate");

	slvr_crc_update(f, mq->bmapno, mq->offset);

	fcmh_op_done(f);
#if 0
	mp->rc = sli_repl_addwk(SLI_REPLWKOP_PTRUNC, IOS_ID_ANY,
	    &mq->fg, mq->bmapno, mq->bgen, mq->offset, NULL, NULL);
#endif
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
	case SRMT_BMAP_PTRUNC:
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
	slrpc_rep_out(rq);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
