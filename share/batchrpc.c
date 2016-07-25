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

static struct psc_poolmaster	 slrpc_batch_req_poolmaster;
static struct psc_poolmaster	 slrpc_batch_rep_poolmaster;
static struct psc_poolmgr	*slrpc_batch_req_pool;
static struct psc_poolmgr	*slrpc_batch_rep_pool;

struct psc_listcache	 slrpc_batch_req_delayed;	/* to be filled/expired */
struct psc_listcache	 slrpc_batch_req_waitrep;	/* wait reply from peer */

struct slrpc_wkdata_batch_req {
	struct slrpc_batch_req	*bq;
	int			 rc;
};

struct slrpc_wkdata_batch_rep {
	struct slrpc_batch_rep	*bp;
	int			 rc;
};

void
slrpc_batch_req_ctor(struct slrpc_batch_req *bq)
{
    	struct slrpc_batch_rep_handler *h = bq->bq_handler;
	bq->bq_reqbuf = PSCALLOC(SLRPC_BATCH_MAX_COUNT * h->bph_qlen);
	bq->bq_repbuf = PSCALLOC(SLRPC_BATCH_MAX_COUNT * h->bph_plen);
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
    	struct slrpc_batch_req_handler *h = bp->bp_handler;
	bp->bp_reqbuf = PSCALLOC(SLRPC_BATCH_MAX_COUNT * h->bqh_qlen);
	bp->bp_repbuf = PSCALLOC(SLRPC_BATCH_MAX_COUNT * h->bqh_plen);
}

void
slrpc_batch_rep_dtor(struct slrpc_batch_rep *bp)
{
	PSCFREE(bp->bp_reqbuf);
	PSCFREE(bp->bp_repbuf);
}

/*
 * If all things are good, a request is done when the reply comes
 * back. Otherwise, it may be destroyed when the request can't be
 * sent out or a reply won't come back.
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

	spinlock(&bq->bq_lock);
	if (rc && !bq->bq_rc)
		bq->bq_rc = rc;
	psc_assert(!(bq->bq_flags & BATCHF_FREEING));
	bq->bq_flags |= BATCHF_FREEING;
	freelock(&bq->bq_lock);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "destroying");

	if (abs(rc) == ETIMEDOUT) {
		OPSTAT_INCR("batch-request-timeout");
		psclog_warnx("batch request rc = %d", rc);
	}

	if (bq->bq_flags & (BATCHF_INFL|BATCHF_REPLY))
		lc_remove(&slrpc_batch_req_waitrep, bq);
	else
		lc_remove(&slrpc_batch_req_delayed, bq);

	if (bq->bq_flags & BATCHF_REPLY)
		lc_remove(bq->bq_res_batches, bq);
	sl_csvc_decref(bq->bq_csvc);

	/*
	 * Run callback on each item contained in the batch. The handler must 
	 * be either slm_batch_rep_repl or slm_batch_rep_preclaim.
	 */
	h = bq->bq_handler;
	n = bq->bq_reqlen / h->bph_qlen;
	for (q = bq->bq_reqbuf, p = bq->bq_repbuf, i = 0; i < n;
	    i++, q += h->bph_qlen, p += h->bph_plen) {
		scratch = psc_dynarray_getpos(&bq->bq_scratch, i);
		/*
 		 * The callback handle is either slm_batch_repl_cb()
 		 * or slm_batch_preclaim_cb().
 		 */
		bq->bq_handler->bph_cbf(q, bq->bq_replen ? p : NULL, 
		    scratch, -bq->bq_rc);
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

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "finishing, rc = %d", rc);
	wk = pfl_workq_getitem(slrpc_batch_req_finish_workcb,
	    struct slrpc_wkdata_batch_req);

	wk->bq = bq;
	wk->rc = rc;
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

	if (rc) {
		slrpc_batch_req_sched_finish(bq, rc);
	} else {
		spinlock(&bq->bq_lock);
		bq->bq_flags &= ~BATCHF_INFL;
		bq->bq_flags |= BATCHF_REPLY;
		lc_add(bq->bq_res_batches, bq);
		freelock(&bq->bq_lock);
	}
	return (0);
}


/*
 * Transmit a SRMT_BATCH_RQ request to peer.
 *
 * @bq: batch request to send.
 */
void
slrpc_batch_req_send(struct slrpc_batch_req *bq)
{
	int rc;
	struct iovec iov;
	struct slrpc_batch_rep_handler *h = bq->bq_handler;

	psc_assert(!(bq->bq_flags & BATCHF_INFL));
	bq->bq_flags |= BATCHF_INFL;
	bq->bq_flags &= ~BATCHF_DELAY;

	lc_remove(&slrpc_batch_req_delayed, bq);
	lc_add(&slrpc_batch_req_waitrep, bq);

	freelock(&bq->bq_lock);

	PFLOG_BATCH_REQ(PLL_DIAG, bq, "qlen = %d, plen = %d, sending", 
	    h->bph_qlen, h->bph_plen);

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
		pscrpc_req_finished(bq->bq_rq);
		slrpc_batch_req_sched_finish(bq, rc);
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
	mq->opc = bp->bp_opc;
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
	struct slrpc_batch_rep *bp = NULL;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct iovec iov;
	int rc;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->opc = mq->opc;	
	if (mq->opc < 0 || mq->opc >= SRMT_TOTAL)
		return (mp->rc = -EINVAL);

	/* See sli_rim_batch_req_handlers set up in sli_rim_init() */
	h = &handlers[mq->opc];
	if (h->bqh_cbf == NULL)
		return (mp->rc = -EINVAL);

	if (mq->len < h->bqh_qlen || mq->len > LNET_MTU || 
	    mq->len % h->bqh_qlen) {
		psclog_warnx("opc = %d, len = %d, qlen = %d", 
		    mq->opc, mq->len, h->bqh_qlen);
		return (mp->rc = -EINVAL);
	}

	bp = psc_pool_get(slrpc_batch_rep_pool);
	memset(bp, 0, sizeof(*bp));
	bp->bp_handler = h;
	slrpc_batch_rep_ctor(bp);

	iov.iov_len = mq->len;
	iov.iov_base = bp->bp_reqbuf;

	/* retrieve buffer sent by slrpc_batch_req_send() */
	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, h->bqh_rcv_ptl,
	    &iov, 1);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	INIT_SPINLOCK(&bp->bp_lock);
	INIT_PSC_LISTENTRY(&bp->bp_lentry);
	bp->bp_bid = mq->bid;
	bp->bp_refcnt = 1;
	bp->bp_reqlen = mq->len;
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

	LIST_CACHE_LOCK(&slrpc_batch_req_waitrep);
	LIST_CACHE_FOREACH_SAFE(bq, bq_next, &slrpc_batch_req_waitrep) {
		if (!trylock(&bq->bq_lock)) {
			LIST_CACHE_ULOCK(&slrpc_batch_req_waitrep);
			OPSTAT_INCR("batch-reply-yield");
			goto retry;
		}
		if (mq->bid == bq->bq_bid) {
			/* there is time between send and setting the flag */
			if (!(bq->bq_flags & BATCHF_REPLY)) {
				sleep(1);
				freelock(&bq->bq_lock);
				LIST_CACHE_ULOCK(&slrpc_batch_req_waitrep);
				OPSTAT_INCR("batch-reply-wait");
				goto retry;
			}
			freelock(&bq->bq_lock);
			LIST_CACHE_ULOCK(&slrpc_batch_req_waitrep);
			psc_assert((mq->opc == bq->bq_opc));	
			if (!mp->rc) {
				iov.iov_base = bq->bq_repbuf;
				iov.iov_len = bq->bq_replen = mq->len;
				mp->rc = slrpc_bulkserver(rq,
				    BULK_GET_SINK, bq->bq_rcv_ptl, &iov,
				    1);
			}
			/*
 			 * mq is actually a batch request reply here.
 			 * See slrpc_batch_rep_send().
 			 */
			slrpc_batch_req_sched_finish(bq,
			    mp->rc ? mp->rc : mq->rc);

			found = 1;
			goto out;
		}
		freelock(&bq->bq_lock);
	}
	LIST_CACHE_ULOCK(&slrpc_batch_req_waitrep);
 out:
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
    int32_t opc, int rcvptl, int sndptl, void *buf, int len,
    void *scratch, struct slrpc_batch_rep_handler *handler, int expire)
{
	static psc_atomic64_t bid = PSC_ATOMIC64_INIT(0);

	struct slrpc_batch_req *bq, *newbq = NULL;
	struct pscrpc_request *rq;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	int rc = 0;

	psc_assert(handler->bph_qlen == len);

 lookup:

	LIST_CACHE_LOCK(&slrpc_batch_req_delayed);
	LIST_CACHE_FOREACH(bq, &slrpc_batch_req_delayed) {
		spinlock(&bq->bq_lock);
		if ((newbq->bq_res_batches == res_batches) && 
		    (opc == bq->bq_opc)) {
			LIST_CACHE_ULOCK(&slrpc_batch_req_delayed);
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
	if (newbq) {
		bq = newbq;
		newbq = NULL;
		bq->bq_flags |= BATCHF_DELAY;
		PFL_GETTIMEVAL(&bq->bq_expire);
		bq->bq_expire.tv_sec += expire;
		lc_addtail(&slrpc_batch_req_delayed, bq);
		LIST_CACHE_ULOCK(&slrpc_batch_req_delayed);
		goto lookup;
	}
	LIST_CACHE_ULOCK(&slrpc_batch_req_delayed);

	/* not found; create */

	OPSTAT_INCR("batch-alloc");
	rc = SL_RSX_NEWREQ(csvc, SRMT_BATCH_RQ, rq, mq, mp);
	if (rc)
		return (rc);

	mq->opc = opc;
	mq->bid = psc_atomic64_inc_getnew(&bid);

	newbq = psc_pool_get(slrpc_batch_req_pool);
	memset(newbq, 0, sizeof(*newbq));
	newbq->bq_handler = handler;
	slrpc_batch_req_ctor(newbq);

	INIT_SPINLOCK(&newbq->bq_lock);
	INIT_PSC_LISTENTRY(&newbq->bq_lentry);
	INIT_PSC_LISTENTRY(&newbq->bq_lentry_res);
	newbq->bq_rq = rq;
	newbq->bq_rcv_ptl = rcvptl;
	newbq->bq_snd_ptl = sndptl;
	newbq->bq_csvc = csvc;
	newbq->bq_bid = mq->bid;
	newbq->bq_res_batches = res_batches;
	newbq->bq_opc = opc;
	newbq->bq_workq = workq;

	PFLOG_BATCH_REQ(PLL_DIAG, newbq, "created");

	CSVC_LOCK(csvc);
	sl_csvc_incref(csvc);
	CSVC_ULOCK(csvc);

	goto lookup;

 add:

	memcpy(bq->bq_reqbuf + bq->bq_reqlen, buf, len);
	bq->bq_reqlen += len;
	mq->len += len;
	psc_dynarray_add(&bq->bq_scratch, scratch);

	/*
	 * OK, the requested entry has been added.  If the next
	 * addition would overflow, send out what we have now.
	 *
	 * This logical relies on the fact that each request is 
	 * of the same size.
	 */
	bq->bq_cnt++;
	psc_assert(bq->bq_cnt <= SLRPC_BATCH_MAX_COUNT);
	if (bq->bq_cnt == SLRPC_BATCH_MAX_COUNT) {
		OPSTAT_INCR("batch-send-full");
		slrpc_batch_req_send(bq);
	} else
		freelock(&bq->bq_lock);

	if (newbq) {
		OPSTAT_INCR("batch-free");
		pscrpc_req_finished(newbq->bq_rq);
		sl_csvc_decref(newbq->bq_csvc);
		slrpc_batch_req_dtor(newbq);
		psc_pool_return(slrpc_batch_req_pool, newbq);
	}
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
		if (bq->bq_cnt && timercmp(&now, &bq->bq_expire, >)) {
			OPSTAT_INCR("batch-send-expire");
			slrpc_batch_req_send(bq);
		} else {
			timersub(&bq->bq_expire, &now, &stall);
			freelock(&bq->bq_lock);
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
	struct slrpc_batch_req *bq, *dummy;

	LIST_CACHE_LOCK(l);
	LIST_CACHE_FOREACH_SAFE(bq, dummy, l) {
		/* ECONNRESET = 104  */
#if 0
		spinlock(&bq->bq_lock);
		if (!(bq->bq_flags & BATCHF_REPLY)) {
			freelock(&bq->bq_lock);
			continue;
		}
		slrpc_batch_req_sched_finish(bq, -ECONNRESET);
#endif
	}
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
	    0, NULL, "batch-req");
	slrpc_batch_req_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_req_poolmaster);

	psc_poolmaster_init(&slrpc_batch_rep_poolmaster,
	    struct slrpc_batch_rep, bp_lentry, PPMF_AUTO, 8, 8, 0,
	    NULL, "batch-rep");
	slrpc_batch_rep_pool = psc_poolmaster_getmgr(
	    &slrpc_batch_rep_poolmaster);

	lc_reginit(&slrpc_batch_req_delayed, struct slrpc_batch_req,
	    bq_lentry, "batchrpc-delay");
	lc_reginit(&slrpc_batch_req_waitrep, struct slrpc_batch_req,
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
	pfl_listcache_destroy_registered(&slrpc_batch_req_waitrep);
	pfl_poolmaster_destroy(&slrpc_batch_req_poolmaster);
	pfl_poolmaster_destroy(&slrpc_batch_rep_poolmaster);
}
