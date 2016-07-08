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

struct slrpc_wkdata_batch_req {
	struct slrpc_batch_req	*bq;
	int			 rc;
};

struct slrpc_wkdata_batch_rep {
	struct slrpc_batch_rep	*bp;
	int			 rc;
};

void
slrpc_batch_req_ctor( struct slrpc_batch_req *bq)
{
	INIT_LISTENTRY(&bq->bq_lentry);
	bq->bq_reqbuf = PSCALLOC(LNET_MTU);
	bq->bq_repbuf = PSCALLOC(LNET_MTU);
}

void
slrpc_batch_req_dtor(struct slrpc_batch_req *bq)
{
	PSCFREE(bq->bq_reqbuf);
	PSCFREE(bq->bq_repbuf);
}

void
slrpc_batch_rep_ctor(struct slrpc_batch_rep *bp)
{
	INIT_LISTENTRY(&bp->bp_lentry);
	bp->bp_reqbuf = PSCALLOC(LNET_MTU);
	bp->bp_repbuf = PSCALLOC(LNET_MTU);
}

void
slrpc_batch_rep_dtor(struct slrpc_batch_rep *bp)
{
	PSCFREE(bp->bp_reqbuf);
	PSCFREE(bp->bp_repbuf);
}

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
 * @rc: return code during RPC communication.
 */
void
slrpc_batch_req_done(struct slrpc_batch_req *bq, int rc)
{
	struct slrpc_batch_rep_handler *h;
	char *q, *p, *scratch;
	int i, n;

	if (rc && !bq->bq_rc)
		bq->bq_rc = rc;

	if (abs(rc) == ETIMEDOUT) {
		OPSTAT_INCR("batch-request-timeout");
		psclog_warnx("batch request rc = %d", rc);
	}

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "decref");

	bq->bq_flags |= BATCHF_FREEING;
	freelock(&bq->bq_lock);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "destroying");

	if (bq->bq_flags & (BATCHF_INFL|BATCHF_REPLY))
		lc_remove(&slrpc_batch_req_waitreply, bq);
	else
		lc_remove(&slrpc_batch_req_delayed, bq);

	lc_remove(bq->bq_res_batches, bq);

	pscrpc_req_finished(bq->bq_rq);
	sl_csvc_decref(bq->bq_csvc);

	/* Run callback on each item contained in the batch. */
	h = bq->bq_handler;
	n = bq->bq_replen / h->bph_plen;
	for (q = bq->bq_reqbuf, p = bq->bq_repbuf, i = 0; i < n;
	    i++, q += h->bph_qlen, p += h->bph_plen) {
		scratch = psc_dynarray_getpos(&bq->bq_scratch, i);
		/*
 		 * The callback handle is either slm_batch_repl_cb()
 		 * or slm_batch_preclaim_cb().
 		 */
		bq->bq_handler->bph_cbf(q, p, scratch, -bq->bq_rc);
		PSCFREE(scratch);
	}

	psc_dynarray_free(&bq->bq_scratch);
	slrpc_batch_req_dtor(bq);
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

	spinlock(&bq->bq_lock);
	slrpc_batch_req_done(bq, wk->rc);
	return (0);
}

/*
 * Called if we can't send a batch request or when the reply for
 * the batch request has come back.
 */
void
slrpc_batch_req_sched_finish(struct slrpc_batch_req *bq, int rc)
{
	struct slrpc_wkdata_batch_req *wk;

	spinlock(&bq->bq_lock);
	psc_assert(!(bq->bq_flags & BATCHF_FINISH));
	bq->bq_flags |= BATCHF_FINISH;
	freelock(&bq->bq_lock);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "scheduled for finishing");
	wk = pfl_workq_getitem(slrpc_batch_req_finish_workcb,
	    struct slrpc_wkdata_batch_req);
	wk->bq = bq;
	wk->rc = rc;
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

	spinlock(&bq->bq_lock);
	bq->bq_flags &= ~BATCHF_INFL;
	bq->bq_flags |= BATCHF_REPLY;
	freelock(&bq->bq_lock);
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
 * This means that if we can't batch successfully, we end up using more
 * RPCs.  In addition, we must handle early reply as well. Otherwise,
 * slrpc_batch_handle_reply() won't find our request.
 *
 * @rq: RPC.
 * @av: callback arguments.
 */
int
slrpc_batch_req_send_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slrpc_batch_req *bq = av->pointer_arg[0];
	int rc;

	SL_GET_RQ_STATUS_TYPE(bq->bq_csvc, rq, struct srm_batch_rep,
	    rc);

	if (rc)
		slrpc_batch_req_sched_finish(bq, rc);
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
	struct iovec iov;
	int rc;

	bq->bq_flags &= ~BATCHF_DELAY;
	bq->bq_flags |= BATCHF_INFL;
	lc_remove(&slrpc_batch_req_delayed, bq);
	lc_add(&slrpc_batch_req_waitreply, bq);

	freelock(&bq->bq_lock);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "sending");

	iov.iov_len = bq->bq_reqlen;
	iov.iov_base = bq->bq_reqbuf;
	rc = slrpc_bulkclient(bq->bq_rq, BULK_GET_SOURCE, bq->bq_snd_ptl,
	    &iov, 1);

	if (!rc) {
		bq->bq_rq->rq_interpret_reply = slrpc_batch_req_send_cb;
		bq->bq_rq->rq_async_args.pointer_arg[0] = bq;
		rc = SL_NBRQSET_ADD(bq->bq_csvc, bq->bq_rq);
	}
	if (rc) {
		/*
		 * If we failed, check again to see if the connection
		 * has been reestablished since there can be delay in
		 * using this API.
		 */
		spinlock(&bq->bq_lock);
		slrpc_batch_req_done(bq, rc);
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
	int rc;

	SL_GET_RQ_STATUS_TYPE(bp->bp_csvc, rq, struct srm_batch_rep,
	    rc);

	if (!rc)
		OPSTAT_INCR("batch-reply-ok");
	else
		OPSTAT_INCR("batch-reply-err");

	PFLOG_BATCH_REP(PLL_DIAG, bp, "destroying");
	sl_csvc_decref(bp->bp_csvc);
	slrpc_batch_rep_dtor(bp);
	psc_pool_return(slrpc_batch_rep_pool, bp);

	return (0);
}

/*
 * Send out a batch RPC reply.
 *
 * @bp: batch reply to send.
 */
int
slrpc_batch_rep_send(struct slrpc_batch_rep *bp)
{
	struct pscrpc_request *rq = bp->bp_rq;
	struct srm_batch_req *mq;
	struct iovec iov;
	int rc;

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	mq->len = bp->bp_replen;
	mq->bid = bp->bp_bid;
	mq->rc = bp->bp_rc;

	iov.iov_base = bp->bp_repbuf;
	iov.iov_len = mq->len;

	PFLOG_BATCH_REP(PLL_DIAG, bp, "sending");

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE,
	    bp->bp_handler->bqh_snd_ptl, &iov, 1);
	if (!rc) {
		rq->rq_interpret_reply = slrpc_batch_rep_send_cb;
		rq->rq_async_args.pointer_arg[0] = bp;
		rc = SL_NBRQSET_ADD(bp->bp_csvc, rq);
	}
	if (rc)
		OPSTAT_INCR("batch-reply-err");
	return (rc);
}

void
slrpc_batch_rep_incref(struct slrpc_batch_rep *bp)
{
	PFLOG_BATCH_REP(PLL_DIAG, bp, "incref");
	spinlock(&bp->bp_lock);
	bp->bp_refcnt++;
	freelock(&bp->bp_lock);
}

/*
 * Drop a reference to a batch RPC reply.  The first time the reference
 * counter reaches zero means the reply can be transmitted back to peer.
 * After that, one additional reference is made and finally dropped when
 * the transmission is confirmed as being received, and we can finally
 * deallocate memory for the batch.
 *
 * @bp: batch reply.
 * @rc: return code to apply to the entire batch.
 */
void
slrpc_batch_rep_decref(struct slrpc_batch_rep *bp, int rc)
{
	spinlock(&bp->bp_lock);

	if (rc && !bp->bp_rc)
		bp->bp_rc = rc;

	PFLOG_BATCH_REP(PLL_DIAG, bp, "decref");
	bp->bp_refcnt--;
	psc_assert(bp->bp_refcnt >= 0);
	if (bp->bp_refcnt) {
		freelock(&bp->bp_lock);
		return;
	}
	freelock(&bp->bp_lock);

	rc = slrpc_batch_rep_send(bp);
	if (rc) {
		PFLOG_BATCH_REP(PLL_DIAG, bp, "destroying");
		sl_csvc_decref(bp->bp_csvc);
		slrpc_batch_rep_dtor(bp);
		psc_pool_return(slrpc_batch_rep_pool, bp);
	}
}

/*
 * Add work in the worker thread after receiving a SRMT_BATCH_RQ
 * request from the MDS. See slrpc_batch_handle_request().
 */
int
slrpc_batch_handle_req_workcb(void *arg)
{
	struct slrpc_wkdata_batch_rep *wk = arg;
	struct slrpc_batch_req_handler *h;
	struct slrpc_batch_rep *bp;
	int i, n, rc = 0;
	char *q, *p;

	bp = wk->bp;
	h = bp->bp_handler;
	n = bp->bp_reqlen / h->bqh_qlen;
	psc_assert(n);
	for (q = bp->bp_reqbuf, p = bp->bp_repbuf, i = 0; i < n;
	    i++, q += h->bqh_qlen, p += h->bqh_plen) {
		/*
		 * The callback function is either sli_repl_addwk() 
		 * or sli_rim_batch_handle_preclaim().
		 */ 
		rc = h->bqh_cbf(bp, q, p);
		if (rc)
			break;
	}

	/*
	 * To avoid tying up the workthr, the callback may actually
	 * reference.  So the destruction of the batch_rep will happen
	 * when any such references are released.
	 */
	slrpc_batch_rep_decref(bp, rc);
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
slrpc_batch_handle_request(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, struct slrpc_batch_req_handler *handlers)
{
	struct slrpc_wkdata_batch_rep *wk;
	struct slrpc_batch_req_handler *h;
	struct slrpc_batch_rep *bp;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;
	void *pbuf, *qbuf;
	int rc;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->opc = mq->opc;	
	if (mq->len < 1 || mq->len > LNET_MTU)
		return (mp->rc = -EINVAL);
	if (mq->opc < 0 || mq->opc >= SRMT_TOTAL)
		return (mp->rc = -EINVAL);
	h = &handlers[mq->opc];
	if (h->bqh_cbf == NULL)
		return (mp->rc = -EINVAL);
	if (mq->len % h->bqh_qlen)
		return (mp->rc = -EINVAL);

	bp = psc_pool_get(slrpc_batch_rep_pool);
	slrpc_batch_rep_ctor(bp);
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

	rc = SL_RSX_NEWREQ(bp->bp_csvc, SRMT_BATCH_RP, bp->bp_rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	PFLOG_BATCH_REP(PLL_DIAG, bp, "created");

	wk = pfl_workq_getitem(slrpc_batch_handle_req_workcb,
	    struct slrpc_wkdata_batch_rep);
	wk->bp = bp;
	pfl_workq_putitem(wk);
	bp = NULL;

 out:
	if (bp) {
		slrpc_batch_rep_dtor(bp);
		psc_pool_return(slrpc_batch_rep_pool, bp);
	}
	return (mp->rc);
}

/*
 * Handle a BATCHRP (i.e. a reply to a BATCHRQ) that arrives after a
 * recipient of a BATCHRQ is done processing the contents and sends us
 * a response indicating success/failure.
 *
 * @rq: RPC of batch reply.
 */
int
slrpc_batch_handle_reply(struct pscrpc_request *rq)
{
	struct slrpc_batch_req *bq, *bq_next;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;
	int found = 0, tried = 0;

	memset(&iov, 0, sizeof(iov));

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->len < 0 || mq->len > LNET_MTU) {
		mp->rc = -EINVAL;
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	}

 retry:

	LIST_CACHE_LOCK(&slrpc_batch_req_waitreply);
	LIST_CACHE_FOREACH_SAFE(bq, bq_next, &slrpc_batch_req_waitreply) {
		spinlock(&bq->bq_lock);
		if (mq->bid == bq->bq_bid && (bq->bq_flags & BATCHF_REPLY)) {
			freelock(&bq->bq_lock);
			if (!mp->rc) {
				iov.iov_base = bq->bq_repbuf;
				iov.iov_len = bq->bq_replen = mq->len;
				mp->rc = slrpc_bulkserver(rq,
				    BULK_GET_SINK, bq->bq_rcv_ptl, &iov,
				    1);
			}

			slrpc_batch_req_sched_finish(bq,
			    mq->rc ? mq->rc : mp->rc);

			found = 1;
			break;
		}
		freelock(&bq->bq_lock);
	}
	LIST_CACHE_ULOCK(&slrpc_batch_req_waitreply);
	if (!found && !tried) {
		sleep(1);
		tried = 1;
		OPSTAT_INCR("batch-reply-early");
		goto retry;
	}
	if (!found) {
		mp->rc = -EINVAL;
		OPSTAT_INCR("batch-reply-enoent");
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	}
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
    struct psc_listcache *workq, struct slrpc_cservice *csvc,
    uint32_t opc, int rcvptl, int sndptl, void *buf, size_t len,
    void *scratch, struct slrpc_batch_rep_handler *handler, int expire)
{
	static psc_atomic64_t bid = PSC_ATOMIC64_INIT(0);

	struct slrpc_batch_req *bq;
	struct pscrpc_request *rq;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	void *qbuf, *pbuf;
	int rc = 0;

 lookup:
	LIST_CACHE_LOCK(res_batches);
	LIST_CACHE_FOREACH(bq, res_batches) {
		spinlock(&bq->bq_lock);
		if ((bq->bq_flags & (BATCHF_INFL|BATCHF_REPLY)) == 0 &&
		    opc == bq->bq_opc) {
			/*
			 * Tack this request onto the existing pending
			 * batch request.
			 *
			 * The caller must ensure that the destination 
			 * of the RPC is the same.
			 */
			mq = pscrpc_msg_buf(bq->bq_rq->rq_reqmsg, 0,
			    sizeof(*mq));
			OPSTAT_INCR("batch-add");
			goto add;
		}
		freelock(&bq->bq_lock);
	}

	LIST_CACHE_ULOCK(res_batches);

	/* not found; create */

	OPSTAT_INCR("batch-new");
	rc = SL_RSX_NEWREQ(csvc, SRMT_BATCH_RQ, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->opc = opc;
	mq->bid = psc_atomic64_inc_getnew(&bid);

	bq = psc_pool_get(slrpc_batch_req_pool);
	slrpc_batch_req_ctor(bq);
	qbuf = bq->bq_reqbuf;
	pbuf = bq->bq_repbuf;
	memset(bq, 0, sizeof(*bq));
	bq->bq_reqbuf = qbuf;
	bq->bq_repbuf = pbuf;

	INIT_SPINLOCK(&bq->bq_lock);
	INIT_PSC_LISTENTRY(&bq->bq_lentry);
	INIT_PSC_LISTENTRY(&bq->bq_lentry_res);
	bq->bq_rq = rq;
	bq->bq_rcv_ptl = rcvptl;
	bq->bq_snd_ptl = sndptl;
	bq->bq_csvc = csvc;
	bq->bq_bid = mq->bid;
	bq->bq_res_batches = res_batches;
	bq->bq_handler = handler;
	bq->bq_opc = opc;
	bq->bq_workq = workq;

	PFL_GETTIMEVAL(&bq->bq_expire);
	bq->bq_expire.tv_sec += expire;

	lc_add(res_batches, bq);
	PFLOG_BATCH_REQ(PLL_DIAG, bq, "created");

	CSVC_LOCK(csvc);
	sl_csvc_incref(csvc);
	CSVC_ULOCK(csvc);

	goto lookup;

 add:
	if (!bq->bq_reqlen) {
		bq->bq_flags |= BATCHF_DELAY;
		lc_add_sorted(&slrpc_batch_req_delayed, bq, slrpc_batch_cmp);
	}

	memcpy(bq->bq_reqbuf + bq->bq_reqlen, buf, len);
	bq->bq_reqlen += len;
	mq->len += len;
	psc_dynarray_add(&bq->bq_scratch, scratch);

	/*
	 * OK, the requested entry has been added.  If the next
	 * addition would overflow, send out what we have now.
	 */
	if (bq->bq_reqlen + len > LNET_MTU) {
		OPSTAT_INCR("batch-send-full");
		slrpc_batch_req_send(bq);
	} else
		freelock(&bq->bq_lock);

 out:
	LIST_CACHE_ULOCK(res_batches);
	sl_csvc_decref(csvc);
	return (rc);
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
	struct slrpc_batch_req *bq;

	stall.tv_sec = 0;
	stall.tv_usec = 0;

	while (pscthr_run(thr)) {
		bq = lc_peekheadwait(&slrpc_batch_req_delayed);

		spinlock(&bq->bq_lock);
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &bq->bq_expire, >)) {
			OPSTAT_INCR("batch-send-expire");
			slrpc_batch_req_send(bq);
		} else {
			freelock(&bq->bq_lock);
			timersub(&bq->bq_expire, &now, &stall);
		}

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
	    struct slrpc_batch_req, bq_lentry, PPMF_AUTO, 8, 8,
	    0, NULL, "batchrpcrq");
	slrpc_batch_req_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_req_poolmaster);

	psc_poolmaster_init(&slrpc_batch_rep_poolmaster,
	    struct slrpc_batch_rep, bp_lentry, PPMF_AUTO, 8, 8, 0,
	    NULL, "batchrpcrp");
	slrpc_batch_rep_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_rep_poolmaster);

	lc_reginit(&slrpc_batch_req_delayed, struct slrpc_batch_req,
	    bq_lentry, "batchrpc-delay");
	lc_reginit(&slrpc_batch_req_waitreply, struct slrpc_batch_req,
	    bq_lentry, "batchrpc-wait");

	pscthr_init(thrtype, slrpc_batch_thr_main, 0,
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
