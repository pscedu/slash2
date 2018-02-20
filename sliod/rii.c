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
 * Routines for handling RPC requests for ION from ION.
 */

#include "pfl/ctlsvr.h"
#include "pfl/list.h"
#include "pfl/pool.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_iod.h"
#include "fidc_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slerr.h"
#include "sliod.h"
#include "slvr.h"

#define SRII_REPLREAD_CBARG_WKRQ	0
#define SRII_REPLREAD_CBARG_SLVR	1
#define SRII_REPLREAD_CBARG_CSVC	2
#define SRII_REPLREAD_CBARG_LEN		0

/*
 * We call this function in the following two cases:
 *
 *  (1) When the request for a replication of a sliver has completed;
 *  (2) When the asynchronous I/O for a replication of a sliver has
 *	completed.
 */
__static int
sli_rii_replread_release_sliver(struct sli_repl_workrq *w, int slvridx,
    int rc)
{
	struct slvr *s;
	int slvrsiz;
	struct fidc_membh *f;

	s = w->srw_slvr[slvridx];

	if (rc == -SLERR_AIOWAIT) {
		SLVR_LOCK(s);
		/*
		 * It should be either 1 or 2 (when aio replies early),
		 * but just be paranoid in case peer will resend.
		 */
		psc_assert(s->slvr_refcnt > 0);
		s->slvr_refcnt--;
		s->slvr_flags &= ~SLVRF_FAULTING;

		DEBUG_SLVR(PLL_DIAG, s, "aio wait");
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);
		return (rc);
	}

	if (!rc) {
		slvrsiz = SLASH_SLVR_SIZE;
		if (s->slvr_num == w->srw_len / SLASH_SLVR_SIZE)
			slvrsiz = w->srw_len % SLASH_SLVR_SIZE;
		rc = slvr_fsbytes_wio(s, 0, slvrsiz);
	}

	if (!rc) {
		f = slvr_2_fcmh(s);
		FCMH_LOCK(f);
		sli_enqueue_update(f);
		FCMH_ULOCK(f);
	}

	slvr_io_done(s, rc);
	slvr_wio_done(s);

	spinlock(&w->srw_lock);
	if (!rc)
		w->srw_nslvr_cur++;
	w->srw_slvr[slvridx] = NULL;
	freelock(&w->srw_lock);

	sli_replwk_queue(w);
	sli_replwkrq_decref(w, rc);

	return (rc);
}

/*
 * Handler for sliver replication read request.  This runs at the source
 * IOS of a replication request.
 */
__static int
sli_rii_handle_repl_read(struct pscrpc_request *rq)
{
	const struct srm_repl_read_req *mq;
	struct sli_aiocb_reply *aiocbr = NULL;
	struct srm_repl_read_rep *mp;
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	struct iovec iov;
	struct slvr *s;
	int rv;

	sliriithr(pscthr_get())->sirit_st_nread++;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY)
		PFL_GOTOERR(out, mp->rc = -EINVAL);
	if (mq->len <= 0 || mq->len > SLASH_SLVR_SIZE)
		PFL_GOTOERR(out, mp->rc = -EINVAL);
	if (mq->slvrno < 0 || mq->slvrno >= SLASH_SLVRS_PER_BMAP)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	mp->rc = -sli_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		goto out;

	mp->rc = -bmap_get(f, mq->bmapno, SL_READ, &b);
	if (mp->rc) {
		/*
		 * XXX abort bulk here, otherwise all future RPCs will
		 * fail
		 */
		psclog_errorx("failed to load fid="SLPRI_FID" bmap=%u: %d",
		    mq->fg.fg_fid, mq->bmapno, mp->rc);
		goto out;
	}

	s = slvr_lookup(mq->slvrno, bmap_2_bii(b));

	rv = slvr_io_prep(s, 0, mq->len, SL_READ, 0);
	BMAP_ULOCK(b);

	iov.iov_base = s->slvr_slab;
	iov.iov_len = mq->len;

	if (rv == -SLERR_AIOWAIT) {
		aiocbr = sli_aio_replreply_setup(rq, s, &iov);
		SLVR_LOCK(s);
		/* XXX missing slvr_rio_done()? */
		if (s->slvr_flags & SLVRF_FAULTING) {
			s->slvr_aioreply = aiocbr;
			OPSTAT_INCR("aio-insert");
			SLVR_ULOCK(s);
			pscrpc_msg_add_flags(rq->rq_repmsg,
			    MSG_ABORT_BULK);
			PFL_GOTOERR(out, mp->rc = rv);
		} else {
			/*
			 * AIO has already completed ahead of us.
			 */
			OPSTAT_INCR("aio-race");
			rv = s->slvr_err;
			SLVR_ULOCK(s);
			sli_aio_aiocbr_release(aiocbr);
		}
	} else
		slvr_io_done(s, rv);

	/* 
	 * A negative return value to a RPC handler will clean up
	 * any bulk operation automatically.
	 */	
	if (rv) 
		PFL_GOTOERR(out, mp->rc = rv);

	sli_bwqueued_adj(&sli_bwqueued.sbq_egress, mq->len);

	mp->rc = slrpc_bulkserver(rq, BULK_PUT_SOURCE, SRII_BULK_PORTAL,
	    &iov, 1);

	sli_bwqueued_adj(&sli_bwqueued.sbq_egress, -mq->len);

	/*
	 * Do the authbuf signing here in locked context to ensure the
	 * slab doesn't change.
	 */
	authbuf_sign(rq, PSCRPC_MSG_REPLY);

	slvr_rio_done(s);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (mp->rc);
}

/*
 * Handler for sliver replication aio read request.
 *
 * The peer has completed an async I/O of a previously requested sliver
 * and that sliver has been posted for GET.
 */
__static int
sli_rii_handle_repl_read_aio(struct pscrpc_request *rq)
{
	const struct srm_repl_read_req *mq;
	struct sli_repl_workrq *w = NULL;
	struct srm_repl_read_rep *mp;
	struct bmap *b = NULL;
	struct fidc_membh *f;
	struct iovec iov;
	struct slvr *s;
	int rc, slvridx = 0;

	sliriithr(pscthr_get())->sirit_st_nread++;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY) {
		mp->rc = -EINVAL;
		return (mp->rc);
	}
	if (mq->len <= 0 || mq->len > SLASH_SLVR_SIZE) {
		mp->rc = -EINVAL;
		return (mp->rc);
	}
	if (mq->slvrno < 0 || mq->slvrno >= SLASH_SLVRS_PER_BMAP) {
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	mp->rc = -sli_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		goto out;

	mp->rc = -bmap_get(f, mq->bmapno, SL_READ, &b);
	if (mp->rc) {
		/*
		 * XXX abort bulk here, otherwise all future RPCs will
		 * fail
		 */
		psclog_errorx("failed to load bmap: fid="SLPRI_FID" "
		    "bmap=%u: %s",
		    mq->fg.fg_fid, mq->bmapno, sl_strerror(mp->rc));
		goto out;
	}

	/* Lookup the workrq.  It should have already been created. */
	w = sli_repl_findwq(&mq->fg, mq->bmapno);
	if (!w) {
		psclog_errorx("failed to find work: fid="SLPRI_FID" "
		    "bmap=%u",
		    mq->fg.fg_fid, mq->bmapno);
		PFL_GOTOERR(out, mp->rc = -ENOENT);
	}

	s = slvr_lookup(mq->slvrno, bmap_2_bii(b));

	/* Ensure the sliver is found in the work item's array. */
	for (slvridx = 0; slvridx < (int)nitems(w->srw_slvr);
	     slvridx++)
		if (w->srw_slvr[slvridx] == s)
			break;

	if (slvridx == (int)nitems(w->srw_slvr)) {
		OPSTAT_INCR("repl-no-slot");
		// FATAL?
		DEBUG_SLVR(PLL_ERROR, s,
		    "failed to find slvr in wq=%p", w);
		// XXX leak srw
		PFL_GOTOERR(out, mp->rc = -ENOENT);
	}

	rc = slvr_io_prep(s, 0, SLASH_SLVR_SIZE, SL_WRITE, 0);
	psc_assert(!rc);
	BMAP_ULOCK(b);

	iov.iov_base = s->slvr_slab;
	iov.iov_len = mq->len;

	sli_bwqueued_adj(&sli_bwqueued.sbq_egress, mq->len);

	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRII_BULK_PORTAL,
	    &iov, 1);

	sli_bwqueued_adj(&sli_bwqueued.sbq_egress, -mq->len);

	sli_rii_replread_release_sliver(w, slvridx, mp->rc);

 out:
	if (b)
		bmap_op_done(b);
	fcmh_op_done(f);
	return (mp->rc);
}

/*
 * Callback triggered when an SRMT_REPL_READ request finishes, running
 * in the context of the replica destination.
 */
__static int
sli_rii_replread_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[SRII_REPLREAD_CBARG_CSVC];
	struct sli_repl_workrq *w = args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ];
	struct slvr *s = args->pointer_arg[SRII_REPLREAD_CBARG_SLVR];
	int rc, slvridx;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_repl_read_rep, rc);

	for (slvridx = 0; slvridx < (int)nitems(w->srw_slvr);
	    slvridx++)
		if (w->srw_slvr[slvridx] == s)
			break;
	psc_assert(slvridx < (int)nitems(w->srw_slvr));

	if (rc == -SLERR_AIOWAIT)
		OPSTAT_INCR("issue-replread-aio");
	else if (rc)
		OPSTAT_INCR("issue-replread-error");
	else
		OPSTAT_INCR("issue-replread-cb");
	return (sli_rii_replread_release_sliver(w, slvridx, rc));
}

/*
 * Process a replication request initiated by a SLASH2 client. This runs 
 * at the destination IOS, which issues a REPL_READ RPC to the source IOS.
 */
int
sli_rii_issue_repl_read(struct slrpc_cservice *csvc, int slvrno,
    int slvridx, struct sli_repl_workrq *w)
{
	const struct srm_repl_read_rep *mp;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct iovec iov;
	struct slvr *s;
	int rc;

	psclog_diag("srw %p fg "SLPRI_FID" bmapno %d slvrno %d idx "
	    "%d len %u", w, w->srw_fg.fg_fid, w->srw_bmapno, slvrno,
	    slvridx, w->srw_len);

	/* to be handled by sli_rii_handle_repl_read() */
	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_READ, rq, mq, mp);
	if (rc)
		return (rc);

	mq->len = SLASH_SLVR_SIZE;
	/* adjust the request size for the last sliver */
	if ((unsigned)slvrno == w->srw_len / SLASH_SLVR_SIZE)
		mq->len = w->srw_len % SLASH_SLVR_SIZE;
	mq->fg = w->srw_fg;
	mq->bmapno = w->srw_bmapno;
	mq->slvrno = slvrno;

	psc_atomic32_inc(&w->srw_refcnt);
	PFLOG_REPLWK(PLL_DEBUG, w, "incref");

	psc_assert(w->srw_slvr[slvridx] == SLI_REPL_SLVR_SCHED);

	BMAP_LOCK(w->srw_bcm);
	w->srw_slvr[slvridx] = s =
	    slvr_lookup(slvrno, bmap_2_bii(w->srw_bcm));

	/*
	 * XXX: We should not let EIO sliver stay in the cache.
	 * Otherwise, the following assert will be triggered.
	 */
	rc = slvr_io_prep(s, 0, SLASH_SLVR_SIZE, SL_WRITE, 0);
	BMAP_ULOCK(w->srw_bcm);
	if (rc)
		goto out;

	iov.iov_base = s->slvr_slab;
	iov.iov_len = mq->len;

	rc = slrpc_bulkclient(rq, BULK_PUT_SINK, SRII_BULK_PORTAL, &iov,
	    1);
	if (rc)
		goto out;

	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_SLVR] = s;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_CSVC] = csvc;

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc == 0)
		rq = NULL;

 out:
	if (rc) {
		slvr_io_done(s, rc);
		slvr_wio_done(s);

		spinlock(&w->srw_lock);
		w->srw_slvr[slvridx] = NULL;
		freelock(&w->srw_lock);

		sli_replwk_queue(w);
		sli_replwkrq_decref(w, rc);
		OPSTAT_INCR("issue-replread-error");
	} else {
		OPSTAT_INCR("issue-replread");
	}
	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

int
sli_rii_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    sli_geticsvcx(_resm, rq->rq_export, 0));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRII_MAGIC, SRII_VERSION,
		    SLCONNT_IOD);
		break;
	case SRMT_REPL_READ:
		rc = sli_rii_handle_repl_read(rq);
		break;
	case SRMT_REPL_READAIO:
		rc = sli_rii_handle_repl_read_aio(rq);
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
