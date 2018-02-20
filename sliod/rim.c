/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
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

#ifdef HAVE_FALLOC_FL_PUNCH_HOLE
#  include <linux/falloc.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

#include "pfl/ctlsvr.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"
#include "pfl/time.h"

#include "authbuf.h"
#include "batchrpc.h"
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

struct slrpc_batch_req_handler sli_rim_batch_req_handlers[SRMT_TOTAL];

/*
 * Handle a piece of PRECLAIM (partial reclaim) work.  If our backend
 * does not support fallocate(2) for punching a hole, we return ENOTSUP
 * so the MDS leaves us alone.
 *
 * It is called by slrpc_batch_handle_request() via a worker thread.
 */
int
sli_rim_batch_handle_preclaim(__unusedx struct slrpc_batch_rep *bp,
    void *req, void *rep)
{
	int rc;
	struct srt_preclaim_req *q = req;
	struct srt_preclaim_rep *p = rep;
	struct fidc_membh *f;

#ifdef HAVE_FALLOC_FL_PUNCH_HOLE
	/*
 	 * Works on ext4 on Linux 4.4.0-1.el7.x86_64.netboot.
 	 */
	OPSTAT_INCR("preclaim-attempt");
	p->rc = sli_fcmh_get(&q->fg, &f);
	if (p->rc)
		return (0);

	OPSTAT_INCR("slvr-remove-preclaim");
	slvr_remove_all(f);
	/*
 	 * KEEP_SIZE is needed to avoid EOPNOTSUPP errno.
 	 */
	rc = fallocate(fcmh_2_fd(f), FALLOC_FL_PUNCH_HOLE | 
	    FALLOC_FL_KEEP_SIZE, q->bno * SLASH_BMAP_SIZE, 
	    SLASH_BMAP_SIZE);
	if (rc < 0) {
		p->rc = errno;
		OPSTAT_INCR("preclaim-err");
	} else {
		p->rc = 0;
		FCMH_LOCK(f);
		sli_enqueue_update(f);
		OPSTAT_INCR("preclaim-ok");
	}

	fcmh_op_done(f);
	return (0);
#else
	OPSTAT_INCR("preclaim-notsupport");
	(void)q;
	(void)p;
	(void)f;
	p->rc = -PFLERR_NOTSUP;
	return (0);
#endif
}

/*
 * Handle a piece of PTRUNC (partial truncate) work.  When a client
 * issues a truncate(2) call, all data beyond the specified position
 * gets removed, and this operation eventually trickles down to the
 * backend here.
 *
 * XXX	This also needs to truncate any data in the slvr cache.  We
 *	could just convert to mmap data instead of read(2)/write(2)
 *	buffers.
 *
 * XXX	If there is srw_offset, we must send back a CRC update for the
 *	sliver that got chopped.
 *
 * XXX  What happens if a write to the file was initiated after the
 *      truncation, but arrives earlier?
 */
int
sli_rim_handle_bmap_ptrunc(struct pscrpc_request *rq)
{
	struct srt_ptrunc_req *mq;
	struct srt_ptrunc_rep *mp;
	struct fidc_membh *f;
	off_t off;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->offset < 0 || mq->offset >= SLASH_BMAP_SIZE) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = sli_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		return (0);

	OPSTAT_INCR("slvr-remove-truncate");
	slvr_remove_all(f);
	off = SLASH_BMAP_SIZE * mq->bmapno + mq->offset;
	if (ftruncate(fcmh_2_fd(f), off) == -1) {
		mp->rc = errno;
		DEBUG_FCMH(PLL_ERROR, f, "truncate rc=%d", mp->rc);
		OPSTAT_INCR("ptrunc-failure");
	} else {
		/* (gdb) p *((struct pfl_opstat *)pfl_opstats.pda_items[546]) */
		OPSTAT_INCR("ptrunc-success");
	}

	FCMH_LOCK(f);
	sli_enqueue_update(f);
	fcmh_op_done(f);

	return (0);
}

/*
 * Handle RECLAIM RPC from the MDS as a result of unlink or truncate to
 * zero.  The MDS won't send us a new RPC until we reply, so we should
 * be thread-safe.
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

	rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRIM_BULK_PORTAL, &iov,
	    1);
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
				FCMH_ULOCK(f);
				slvr_remove_all(f);
			}
			fcmh_op_done(f);
		}

		sli_fg_makepath(&entryp->fg, fidfn);

		/*
		 * We do upfront garbage collection, so ENOENT should be
		 * fine.  Also simply creating a file without any I/O
		 * won't create a backing file on the I/O server.
		 *
		 * Anyway, we don't report an error back to MDS because
		 * it can do nothing.  Reporting an error can stall MDS
		 * progress.
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
sli_rim_handler(struct pscrpc_request *rq)
{
	struct slrpc_cservice *csvc;
	struct sl_resm *m;
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    sli_getmcsvcx(_resm, rq->rq_export, 0));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRIM_MAGIC, SRIM_VERSION,
		    SLCONNT_MDS);
		break;
	case SRMT_BMAP_PTRUNC:
		rc = sli_rim_handle_bmap_ptrunc(rq);
		break;
	case SRMT_RECLAIM:
		rc = sli_rim_handle_reclaim(rq);
		break;
	case SRMT_BATCH_RQ:
		m = libsl_nid2resm(rq->rq_export->exp_connection->
		    c_peer.nid);
		csvc = sli_getmcsvcx(m, rq->rq_export, 0);
		rc = slrpc_batch_handle_request(csvc, rq,
		    sli_rim_batch_req_handlers);
		if (rc)
			sl_csvc_decref(csvc);
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

void
sli_rim_init(void)
{
	struct slrpc_batch_req_handler *h;

	h = &sli_rim_batch_req_handlers[SRMT_REPL_SCHEDWK];
	h->bqh_cbf = sli_repl_addwk;
	h->bqh_qlen = sizeof(struct srt_replwk_req);
	h->bqh_plen = sizeof(struct srt_replwk_rep);
	h->bqh_rcv_ptl = SRIM_BULK_PORTAL;
	h->bqh_snd_ptl = SRMI_BULK_PORTAL;

	h = &sli_rim_batch_req_handlers[SRMT_PRECLAIM];
	h->bqh_cbf = sli_rim_batch_handle_preclaim;
	h->bqh_qlen = sizeof(struct srt_preclaim_req);
	h->bqh_plen = sizeof(struct srt_preclaim_rep);
	h->bqh_rcv_ptl = SRIM_BULK_PORTAL;
	h->bqh_snd_ptl = SRMI_BULK_PORTAL;
}
