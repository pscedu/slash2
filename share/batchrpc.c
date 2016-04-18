/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2016, Google, Inc.
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
 * The notion of 'request' and 'reply' gets confusing very quickly when
 * processing responses; beware.  "Client" and "server" are probably
 * better names.
 */

#include <sys/time.h>

#include <unistd.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/opstats.h"
#include "pfl/pool.h"
#include "pfl/rpc.h"
#include "pfl/thread.h"
#include "pfl/workthr.h"

#include "batchrpc.h"
#include "slashrpc.h"
#include "slconn.h"

struct psc_poolmaster	 slrpc_batch_req_poolmaster;
struct psc_poolmaster	 slrpc_batch_rep_poolmaster;
struct psc_poolmgr	*slrpc_batch_req_pool;
struct psc_poolmgr	*slrpc_batch_rep_pool;

struct psc_listcache	 slrpc_batch_req_delayed;	/* waiting to be filled/timeout to be sent */
struct psc_listcache	 slrpc_batch_req_waitreply;	/* awaiting reply */

struct psc_listcache	 slrpc_batch_rep_retrans;	/* to be retransmitted */

struct slrpc_wkdata_batch_req {
	struct slrpc_batch_req	*bq;
	int			 error;
};

struct slrpc_wkdata_batch_rep {
	struct slrpc_batch_rep	*bp;
	int			 error;
};

/*
 * This routine is used to order batch RPC requests by expiration for
 * transmission time.
 *
 * @a: one batch.
 * @a: another batch.
 */
int
slrpc_batch_cmp(const void *a, const void *b)
{
	const struct slrpc_batch_req *pa = a, *pb = b;

	return (timercmp(&pa->bq_expire, &pb->bq_expire, <));
}

/*
 * Decrement a reference to a batch request.  Requests are initialized
 * with an extra reference in anticipation of an eventual response,
 * which will finalize the reference count to zero and run the reply
 * handler.  If a connection is dropped, the handler is run then.
 *
 * @bq: batch request.
 * @error: general error during RPC communication.
 */
void
slrpc_batch_req_decref(struct slrpc_batch_req *bq, int error)
{
	struct slrpc_batch_rep_handler *h;
	char *q, *p, *scratch;
	int i, n, finish = 0;

	SLRPC_BATCH_REQ_RLOCK(bq);

	if (bq->bq_error == 0)
		bq->bq_error = error;

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "decref");

	/*
	 * If the request was sent out, another reference was taken.
	 */
	if (error && (bq->bq_flags &
	    (BATCHF_WAITREPLY | BATCHF_RQINFL)) == BATCHF_RQINFL) {
		psc_assert(bq->bq_refcnt > 1);
		bq->bq_refcnt--;
	} else
		psc_assert(bq->bq_refcnt > 0);

	if (--bq->bq_refcnt == 0) {
		bq->bq_flags |= BATCHF_FREEING;
		finish = 1;
	}

	SLRPC_BATCH_REQ_ULOCK(bq);

	if (!finish)
		return;

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "destroying");

	if (bq->bq_flags & (BATCHF_WAITREPLY | BATCHF_RQINFL))
		lc_remove(&slrpc_batch_req_waitreply, bq);
	else
		lc_remove(&slrpc_batch_req_delayed, bq);

	lc_remove(bq->bq_res_batches, bq);

	pscrpc_req_finished(bq->bq_rq);
	sl_csvc_decref(bq->bq_csvc);

	/* Run callback on each item contained in batch. */
	h = bq->bq_handler;
	n = bq->bq_replen / h->bph_plen;
	for (q = bq->bq_reqbuf, p = bq->bq_repbuf, i = 0; i < n;
	    i++, q += h->bph_qlen, p += h->bph_plen) {
		scratch = psc_dynarray_getpos(&bq->bq_scratch, i);
		bq->bq_handler->bph_cbf(q, p, scratch, -bq->bq_error);
		PSCFREE(scratch);
	}

	psc_dynarray_free(&bq->bq_scratch);
	psc_pool_return(slrpc_batch_req_pool, bq);
}

/*
 * Perform final cleanup of a batch RPC request, after peer has
 * independently replied signifying all work contained therein as
 * completed.
 *
 * @p: callback argument (batch).
 */
int
slrpc_batch_req_finish_workcb(void *p)
{
	struct slrpc_wkdata_batch_req *wk = p;
	struct slrpc_batch_req *bq = wk->bq;

	slrpc_batch_req_decref(bq, wk->error);
	return (0);
}

void
slrpc_batch_req_sched_finish(struct slrpc_batch_req *bq, int error)
{
	struct slrpc_wkdata_batch_req *wk;
	struct psc_listcache *lc;
	int locked = 0;

	lc = &slrpc_batch_req_waitreply;
	locked = LIST_CACHE_RLOCK(lc);
	if (bq->bq_flags & BATCHF_SCHED_FINISH) {
		LIST_CACHE_URLOCK(lc, locked);
		return;
	}

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "scheduled for finishing");

	bq->bq_flags |= BATCHF_SCHED_FINISH;
	LIST_CACHE_URLOCK(lc, locked);

	wk = pfl_workq_getitem(slrpc_batch_req_finish_workcb,
	    struct slrpc_wkdata_batch_req);
	wk->bq = bq;
	wk->error = error;
	pfl_workq_putitemq(bq->bq_workq, wk);
}

/*
 * Move a batch RPC request to the 'waitreply' list, meaning the batch
 * has been received by peer and is being processed and is awaiting a
 * BATCH_RP signifying all actual work in the batch has completed.
 * As this may be a lot of processing, it is not done in RPC callback
 * context and instead by generic worker thread.
 *
 * @p: work callback.
 */
int
slrpc_batch_req_waitreply_workcb(void *p)
{
	struct slrpc_wkdata_batch_req *wk = p;
	struct slrpc_batch_req *bq = wk->bq;

	SLRPC_BATCH_REQ_LOCK(bq);
	bq->bq_flags &= ~BATCHF_RQINFL;
	bq->bq_flags |= BATCHF_WAITREPLY;
	slrpc_batch_req_decref(bq, 0);
	return (0);
}

void
slrpc_batch_req_sched_waitreply(struct slrpc_batch_req *bq)
{
	struct slrpc_wkdata_batch_req *wk;

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "scheduled for WAITREPLY");

	wk = pfl_workq_getitem(slrpc_batch_req_waitreply_workcb,
	    struct slrpc_wkdata_batch_req);
	wk->bq = bq;
	pfl_workq_putitemq(bq->bq_workq, wk);
}

/*
 * Handle the event when a batch request has been successfully received
 * by peer.  This doesn't mean the processing has finished, just that
 * the work was received and started processing.  A separate RPC with
 * status codes will be sent to us later.
 *
 * @rq: RPC.
 * @av: callback arguments.
 */
int
slrpc_batch_req_send_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slrpc_batch_req *bq = av->pointer_arg[0];
	int error;

	SL_GET_RQ_STATUS_TYPE(bq->bq_csvc, rq, struct srm_batch_rep,
	    error);

	if (error)
		slrpc_batch_req_sched_finish(bq, error);
	else
		slrpc_batch_req_sched_waitreply(bq);
	return (0);
}

/*
 * Transmit a batch RPC request to peer.
 *
 * @bq: batch request to send.
 */
void
slrpc_batch_req_send(struct slrpc_batch_req *bq)
{
	struct pscrpc_request *rq;
	struct psc_listcache *ml;
	struct iovec iov;
	int error;

	ml = &slrpc_batch_req_delayed;

	LIST_CACHE_LOCK_ENSURE(ml);

	rq = bq->bq_rq;

	bq->bq_refcnt++;
	bq->bq_flags |= BATCHF_RQINFL;
	bq->bq_rq = NULL;

	lc_remove(ml, bq);

	lc_add(&slrpc_batch_req_waitreply, bq);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "sending");

	iov.iov_base = bq->bq_reqbuf;
	iov.iov_len = bq->bq_reqlen;
	error = slrpc_bulkclient(rq, BULK_GET_SOURCE, bq->bq_snd_ptl,
	    &iov, 1);

	if (!error) {
		rq->rq_interpret_reply = slrpc_batch_req_send_cb;
		rq->rq_async_args.pointer_arg[0] = bq;
		error = SL_NBRQSET_ADD(bq->bq_csvc, rq);
	}
	if (error) {
		/*
		 * If we failed, check again to see if the connection
		 * has been reestablished since there can be delay in
		 * using this API.
		 */

		bq->bq_refcnt--;
		bq->bq_rq = rq;
		bq->bq_flags &= ~BATCHF_RQINFL;
		slrpc_batch_req_decref(bq, error);
	}
}

/*
 * Event handler for when a batch RPC reply is sent, which signifies
 * that all work in the batch has been processed.
 */
int
slrpc_batch_rep_send_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slrpc_batch_rep *bp = av->pointer_arg[0];
	int error;

	SL_GET_RQ_STATUS_TYPE(bp->bp_csvc, rq, struct srm_batch_rep,
	    error);

	slrpc_batch_rep_decref(bp, error);
	return (0);
}

/*
 * Send out a batch RPC reply.
 *
 * @bp: batch reply to send.
 */
void
slrpc_batch_rep_send(struct slrpc_batch_rep *bp)
{
	struct pscrpc_request *rq = bp->bp_rq;
	struct srm_batch_req *mq;
	struct iovec iov;
	int error;

	slrpc_batch_rep_incref(bp);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	mq->len = bp->bp_replen;
	mq->bid = bp->bp_bid;
	mq->rc = bp->bp_error;

	iov.iov_base = bp->bp_repbuf;
	iov.iov_len = mq->len;
	error = slrpc_bulkclient(rq, BULK_GET_SOURCE,
	    bp->bp_handler->bqh_snd_ptl, &iov, 1);
	if (error)
		PFL_GOTOERR(out, error);

	PFLOG_BATCH_REP(PLL_DIAG, bp, "sending");

	rq->rq_interpret_reply = slrpc_batch_rep_send_cb;
	rq->rq_async_args.pointer_arg[0] = bp;
	error = SL_NBRQSET_ADD(bp->bp_csvc, rq);

 out:
	if (error)
		slrpc_batch_rep_decref(bp, error);
}

/*
 * Increase the reference count of a batch RPC reply.
 */
void
slrpc_batch_rep_incref(struct slrpc_batch_rep *bp)
{
	int waslocked;

	waslocked = SLRPC_BATCH_REP_RLOCK(bp);
	bp->bp_refcnt++;
	PFLOG_BATCH_REP(PLL_DIAG, bp, "incref");
	SLRPC_BATCH_REP_URLOCK(bp, waslocked);
}

/*
 * Drop a reference to a batch RPC reply.  The first time the reference
 * counter reaches zero means the reply can be transmitted back to peer.
 * After that, one additional reference is made and finally dropped when
 * the transmission is confirmed as being received, and we can finally
 * deallocate memory for the batch.
 *
 * @bp: batch reply.
 * @error: general error to apply to the entire batch.
 */
void
_slrpc_batch_rep_decref(const struct pfl_callerinfo *pci,
    struct slrpc_batch_rep *bp, int error)
{
	int done = 0;

	SLRPC_BATCH_REP_RLOCK(bp);
	PFLOG_BATCH_REP(PLL_DIAG, bp, "decref");
	psc_assert(bp->bp_refcnt > 0);
	if (--bp->bp_refcnt == 0)
		done = 1;
	SLRPC_BATCH_REP_ULOCK(bp);

	if (!done)
		return;

	if (error && error != -ENOENT) {
		/*
		 * An error was encountered that applies to the entire
		 * batch RPC reply.  Try another transmission.
		 */
		bp->bp_refcnt++;
		bp->bp_flags &= ~BATCHF_REPLIED;
		lc_add(&slrpc_batch_rep_retrans, bp);
	}

	if (!(bp->bp_flags & BATCHF_REPLIED)) {
		bp->bp_flags |= BATCHF_REPLIED;
		slrpc_batch_rep_send(bp);
		return;
	}

	PFLOG_BATCH_REP(PLL_DIAG, bp, "destroying");

	sl_csvc_decref(bp->bp_csvc);
	psc_pool_return(slrpc_batch_rep_pool, bp);
}

int
slrpc_batch_handle_req_workcb(void *arg)
{
	struct slrpc_wkdata_batch_rep *wk = arg;
	struct slrpc_batch_req_handler *h;
	struct slrpc_batch_rep *bp;
	int i, n, error = 0;
	char *q, *p;

	bp = wk->bp;
	h = bp->bp_handler;
	n = bp->bp_reqlen / h->bqh_qlen;
	for (q = bp->bp_reqbuf, p = bp->bp_repbuf, i = 0; i < n;
	    i++, q += h->bqh_qlen, p += h->bqh_plen) {
		error = h->bqh_cbf(bp, q, p);
		if (error)
			break;
	}

	/*
	 * To avoid tying up the workthr, the callback may actually
	 * reference.  So the destruction of the batch_rep will happen
	 * when any such references are released.
	 */
	slrpc_batch_rep_decref(bp, error);
	return (0);
}

/*
 * Handle an incoming batch RPC request.  All processing registered is
 * executed later by worker threads.
 *
 * @csvc: client service to use to eventually transmit a reply back to
 *	the peer the batch was received from.
 * @rq: RPC containing batch of work to handle.
 * @handlers: list of per-opcode handlers.
 */
int
slrpc_batch_handle_request(struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq, struct slrpc_batch_req_handler *handlers)
{
	struct slrpc_wkdata_batch_rep *wk;
	struct slrpc_batch_req_handler *h;
	struct slrpc_batch_rep *bp;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;
	void *pbuf, *qbuf;
	int error;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->len < 1 || mq->len > LNET_MTU)
		return (mp->rc = -EINVAL);
	if (mq->opc < 0 || mq->opc >= NSRMT)
		return (mp->rc = -EINVAL);
	h = &handlers[mq->opc];
	if (h->bqh_cbf == NULL)
		return (mp->rc = -PFLERR_NOTSUP);
	if (mq->len % h->bqh_qlen)
		return (mp->rc = -EINVAL);

	bp = psc_pool_get(slrpc_batch_rep_pool);
	qbuf = bp->bp_reqbuf;
	pbuf = bp->bp_repbuf;
	memset(bp, 0, sizeof(*bp));
	bp->bp_reqbuf = qbuf;
	bp->bp_repbuf = pbuf;

	iov.iov_len = mq->len;
	iov.iov_base = qbuf;
	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, h->bqh_rcv_ptl,
	    &iov, 1);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	INIT_SPINLOCK(&bp->bp_lock);
	INIT_PSC_LISTENTRY(&bp->bp_lentry);
	bp->bp_bid = mq->bid;
	bp->bp_refcnt = 1;
	bp->bp_reqlen = mq->len;
	bp->bp_handler = h;
	bp->bp_csvc = csvc;
	bp->bp_replen = mq->len / h->bqh_qlen * h->bqh_plen;
	bp->bp_opc = mq->opc;

	error = SL_RSX_NEWREQ(bp->bp_csvc, SRMT_BATCH_RP, bp->bp_rq, mq,
	    mp);
	if (error)
		PFL_GOTOERR(out, error);

	PFLOG_BATCH_REP(PLL_DIAG, bp, "created");

	CSVC_LOCK(csvc);
	sl_csvc_incref(csvc);
	CSVC_ULOCK(csvc);

	wk = pfl_workq_getitem(slrpc_batch_handle_req_workcb,
	    struct slrpc_wkdata_batch_rep);
	wk->bp = bp;
	pfl_workq_putitem(wk);
	bp = NULL;

 out:
	if (bp)
		psc_pool_return(slrpc_batch_rep_pool, bp);
	return (mp->rc);
}

/*
 * Handle a BATCHRP (i.e. a reply to a BATCHRQ) that arrives after a
 * recipient of a BATCHRQ is done processing the contents and sends us a
 * response indicating success/failure.
 *
 * @rq: RPC of batch reply.
 */
int
slrpc_batch_handle_reply(struct pscrpc_request *rq)
{
	struct slrpc_batch_req *bq, *bq_next;
	struct psc_listcache *lc;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;

	memset(&iov, 0, sizeof(iov));

	lc = &slrpc_batch_req_waitreply;
	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->len < 0 || mq->len > LNET_MTU) {
		mp->rc = -EINVAL;
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	}

	LIST_CACHE_LOCK(lc);
	LIST_CACHE_FOREACH_SAFE(bq, bq_next, lc)
		if (mq->bid == bq->bq_bid) {
			if (!mp->rc) {
				iov.iov_base = bq->bq_repbuf;
				iov.iov_len = bq->bq_replen = mq->len;
				mp->rc = slrpc_bulkserver(rq,
				    BULK_GET_SINK, bq->bq_rcv_ptl, &iov,
				    1);
			}

			slrpc_batch_req_sched_finish(bq,
			    mq->rc ? mq->rc : mp->rc);

			break;
		}
	LIST_CACHE_ULOCK(lc);
	if (bq == NULL)
		mp->rc = -ENOENT;
	return (mp->rc);
}

/*
 * Add an item to a batch RPC request.  If doing so fills the batch, it
 * will be sent out immediately; otherwise it will sit around until
 * expiration.
 *
 * @res_batches: list on sl_resource containing batch requests awaiting
 *	reply; used when processing connection drops.
 * @workq: which work queue to place events on.
 * @csvc: client service to peer.
 * @opc: RPC operation code.
 * @rcvptl: receive RPC portal.
 * @sndptl: send RPC portal.
 * @buf: request message to be copied into the batch.
 * @len: length of @buf.
 * @scratch: private data to attach to this item if needed when
 *	processing the reply; will be freed by this API on reply.
 * @handler: callback to run when a reply is received.
 * @expire: number of seconds to wait before sending this batch RPC out.
 *
 * Note that only RPCs of the same opcode can be batched together.
 */
int
slrpc_batch_req_add(struct psc_listcache *res_batches,
    struct psc_listcache *workq, struct slashrpc_cservice *csvc,
    uint32_t opc, int rcvptl, int sndptl, void *buf, size_t len,
    void *scratch, struct slrpc_batch_rep_handler *handler, int expire)
{
	static psc_atomic64_t bid = PSC_ATOMIC64_INIT(0);

	struct slrpc_batch_req *bq;
	struct pscrpc_request *rq;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	void *qbuf, *pbuf;
	int error = 0;

	LIST_CACHE_LOCK(res_batches);
	LIST_CACHE_FOREACH(bq, res_batches)
		if ((bq->bq_flags & (BATCHF_RQINFL |
		    BATCHF_WAITREPLY)) == 0 &&
		    opc == bq->bq_opc) {
			/*
			 * Tack this request onto the existing pending
			 * batch request.
			 */
			sl_csvc_decref(csvc);
			mq = pscrpc_msg_buf(bq->bq_rq->rq_reqmsg, 0,
			    sizeof(*mq));
			goto add;
		}

	/* not found; create */

	error = SL_RSX_NEWREQ(csvc, SRMT_BATCH_RQ, rq, mq, mp);
	if (error)
		PFL_GOTOERR(out, error);

	mq->opc = opc;
	mq->bid = psc_atomic64_inc_getnew(&bid);

	bq = psc_pool_get(slrpc_batch_req_pool);
	qbuf = bq->bq_reqbuf;
	pbuf = bq->bq_repbuf;
	memset(bq, 0, sizeof(*bq));
	bq->bq_reqbuf = qbuf;
	bq->bq_repbuf = pbuf;

	INIT_SPINLOCK(&bq->bq_lock);
	INIT_PSC_LISTENTRY(&bq->bq_lentry_global);
	INIT_PSC_LISTENTRY(&bq->bq_lentry_res);
	bq->bq_rq = rq;
	bq->bq_rcv_ptl = rcvptl;
	bq->bq_snd_ptl = sndptl;
	bq->bq_csvc = csvc;
	bq->bq_bid = mq->bid;
	bq->bq_res_batches = res_batches;
	bq->bq_handler = handler;
	bq->bq_refcnt = 1;
	bq->bq_opc = opc;
	bq->bq_workq = workq;

	PFL_GETTIMEVAL(&bq->bq_expire);
	bq->bq_expire.tv_sec += expire;

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "created");

	lc_add(res_batches, bq);
	lc_add_sorted(&slrpc_batch_req_delayed, bq, slrpc_batch_cmp);

 add:
	memcpy(bq->bq_reqbuf + bq->bq_reqlen, buf, len);
	bq->bq_reqlen += len;
	mq->len += len;
	psc_dynarray_add(&bq->bq_scratch, scratch);

	/*
	 * OK, the requested entry has been added.  If the next
	 * slrpc_batch_req_add() would overflow, send out what we have
	 * now.
	 */
	if (bq->bq_reqlen + len > LNET_MTU)
		slrpc_batch_req_send(bq);

	csvc = NULL;

 out:
	LIST_CACHE_ULOCK(res_batches);
	if (csvc)
		sl_csvc_decref(csvc);
	return (error);
}

/*
 * Main processing for the batch RPC thread.  The thread waits for
 * pending batch sets to expire and sends them out.
 *
 * @thr: thread structure for transmitter.
 */
void
slrpc_batch_thr_main(struct psc_thread *thr)
{
	struct timeval now, stall;
	struct psc_listcache *ml;
	struct slrpc_batch_req *bq;

	ml = &slrpc_batch_req_delayed;

	stall.tv_sec = 0;
	stall.tv_usec = 0;

	while (pscthr_run(thr)) {
		LIST_CACHE_LOCK(ml);
		bq = lc_peekheadwait(ml);
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &bq->bq_expire, >))
			slrpc_batch_req_send(bq);
		else
			timersub(&bq->bq_expire, &now, &stall);
		LIST_CACHE_ULOCK(ml);

		usleep(stall.tv_sec * 1000000 + stall.tv_usec);

		stall.tv_sec = 0;
		stall.tv_usec = 0;
	}
}

/*
 * Finish all batch sets awaiting reply, intended to be called when a
 * peer connection is dropped.
 *
 * @l: list of batches, attached from sl_resource.
 */
void
slrpc_batches_drop(struct psc_listcache *l)
{
	struct slrpc_batch_req *bq;

	LIST_CACHE_LOCK(l);
	LIST_CACHE_FOREACH(bq, l)
		slrpc_batch_req_sched_finish(bq, -ECONNRESET);
	LIST_CACHE_ULOCK(l);
}

int
slrpc_batch_req_ctor(__unusedx struct psc_poolmgr *m, void *item)
{
	struct slrpc_batch_req *bq = item;

	INIT_LISTENTRY(&bq->bq_lentry_global);
	bq->bq_reqbuf = PSCALLOC(LNET_MTU);
	bq->bq_repbuf = PSCALLOC(LNET_MTU);
	return (0);
}

void
slrpc_batch_req_dtor(void *item)
{
	struct slrpc_batch_req *bq = item;

	PSCFREE(bq->bq_reqbuf);
	PSCFREE(bq->bq_repbuf);
}

int
slrpc_batch_rep_ctor(__unusedx struct psc_poolmgr *m, void *item)
{
	struct slrpc_batch_rep *bp = item;

	INIT_LISTENTRY(&bp->bp_lentry);
	bp->bp_reqbuf = PSCALLOC(LNET_MTU);
	bp->bp_repbuf = PSCALLOC(LNET_MTU);
	return (0);
}

void
slrpc_batch_rep_dtor(void *item)
{
	struct slrpc_batch_rep *bp = item;

	PSCFREE(bp->bp_reqbuf);
	PSCFREE(bp->bp_repbuf);
}

/*
 * Initialize global variables for batch RPC API.
 *
 * @thrtype: type ID for batch RPC transmitter threads.
 * @thrprefix: prefix for batch RPC transmitter threads.
 */
void
slrpc_batches_init(int thrtype, const char *thrprefix)
{
	psc_poolmaster_init(&slrpc_batch_req_poolmaster,
	    struct slrpc_batch_req, bq_lentry_global, PPMF_AUTO, 8, 8,
	    0, slrpc_batch_req_ctor, slrpc_batch_req_dtor, NULL,
	    "batchrpcrq");
	slrpc_batch_req_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_req_poolmaster);
	psc_poolmaster_init(&slrpc_batch_rep_poolmaster,
	    struct slrpc_batch_rep, bp_lentry, PPMF_AUTO, 8, 8, 0,
	    slrpc_batch_rep_ctor, slrpc_batch_rep_dtor, NULL,
	    "batchrpcrp");
	slrpc_batch_rep_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_rep_poolmaster);

	lc_reginit(&slrpc_batch_req_delayed, struct slrpc_batch_req,
	    bq_lentry_global, "batchrpcdelay");
	lc_reginit(&slrpc_batch_req_waitreply, struct slrpc_batch_req,
	    bq_lentry_global, "batchrpcwait");

	pscthr_init(thrtype, slrpc_batch_thr_main, NULL, 0,
	    "%sbatchrpcthr", thrprefix);
}

/*
 * Deallocate global variables for batch RPC API.
 */
void
slrpc_batches_destroy(void)
{
	pfl_listcache_destroy_registered(&slrpc_batch_req_delayed);
	pfl_listcache_destroy_registered(&slrpc_batch_req_waitreply);
	pfl_poolmaster_destroy(&slrpc_batch_req_poolmaster);
	pfl_poolmaster_destroy(&slrpc_batch_rep_poolmaster);
}
