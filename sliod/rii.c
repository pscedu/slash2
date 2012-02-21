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
 * Routines for handling RPC requests for ION from ION.
 */

#include "psc_ds/list.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/pool.h"

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

psc_atomic64_t		 sli_rpc_replread = PSC_ATOMIC64_INIT(0);
psc_atomic64_t		 sli_rpc_replread_aio = PSC_ATOMIC64_INIT(0);

psc_atomic64_t		 sli_rpc_repl_read = PSC_ATOMIC64_INIT(0);
psc_atomic64_t		 sli_rpc_repl_read_cb = PSC_ATOMIC64_INIT(0);
psc_atomic64_t		 sli_rpc_repl_read_cb_aio = PSC_ATOMIC64_INIT(0);

/**
 * sli_rii_replread_release_sliver: We call this function in three
 * cases:
 *  (1) When we fail to issue a request for a replication of a sliver;
 *  (2) When the request for a replication of a sliver has completed;
 *  (3) When the asynchronous I/O for a replication of a sliver has
 *	completed.
 */
__static int
sli_rii_replread_release_sliver(struct sli_repl_workrq *w, int slvridx,
    int rc)
{
	struct slvr_ref *s;
	int slvrsiz, aio = 0;

	s = w->srw_slvr_refs[slvridx];

	slvrsiz = SLASH_SLVR_SIZE;
	if (s->slvr_num == w->srw_len / SLASH_SLVR_SIZE)
		slvrsiz = w->srw_len % SLASH_SLVR_SIZE;

	if (rc == 0) {
		if (!(slvr_2_crcbits(s) & BMAP_SLVR_CRCABSENT))
			rc = slvr_do_crc(s);

		/* XXX check this return code */
//		if (!rc)
		if (1)
			/* SLVR_DATARDY is set in wio_done
			 *    when the slvr lock is taken again.
			 */
			rc = slvr_fsbytes_wio(s, slvrsiz, 0);
	}
	if (rc) {
		SLVR_LOCK(s);
		if (rc != -SLERR_AIOWAIT) {
			//slvr_clear_inuse(s, 0, slvrsiz);
			psc_vbitmap_clearall(s->slvr_slab->slb_inuse);
			s->slvr_flags |= SLVR_REPLFAIL;
		} else {
			s->slvr_flags |= SLVR_AIOWAIT;
			aio = 1;
			/*
			 * It should be either 1 or 2 (when aio replies
			 * early), but just be panaroid in case peer
			 * will resend.
			 */
			psc_assert(s->slvr_pndgwrts > 0);
			psc_assert(s->slvr_flags & SLVR_REPLDST);
			s->slvr_pndgwrts--;
			s->slvr_flags &= ~(SLVR_REPLDST|SLVR_REPLWIRE);
			SLVR_WAKEUP(s);
		}
		SLVR_ULOCK(s);
	}

	DEBUG_SLVR(PLL_INFO, s, "replread %s rc=%d", aio ?
	    "aiowait" : "complete", rc);

	if (!aio) {
		slvr_wio_done(s);

		spinlock(&w->srw_lock);
		w->srw_nslvr_cur++;
		w->srw_slvr_refs[slvridx] = NULL;
		sli_replwkrq_decref(w, rc);
	}

	return (rc);
}

/**
 * sli_rii_handle_replread - Handler for sliver replication read request.
 * @rq: request.
 * @aio:  this argument signifies that the peer has completed an async I/O
 *    of a previously requested sliver and that sliver has been posted for
 *    GET.  In essense, this flag causes the internals of the RPC handler
 *    to be reversed.
 */
__static int
sli_rii_handle_replread(struct pscrpc_request *rq, int aio)
{
	const struct srm_repl_read_req *mq;
	struct sli_aiocb_reply *aiocbr = NULL;
	struct sli_repl_workrq *w = NULL;
	struct srm_repl_read_rep *mp;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct slvr_ref *s;
	struct iovec iov;
	int rv, slvridx = 0;

	sliriithr(pscthr_get())->sirit_st_nread++;

	bcm = NULL;

	if (aio)
		psc_atomic64_inc(&sli_rpc_replread_aio);
	else
		psc_atomic64_inc(&sli_rpc_replread);

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	if (mq->len <= 0 || mq->len > SLASH_SLVR_SIZE) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	if (mq->slvrno < 0 || mq->slvrno >= SLASH_SLVRS_PER_BMAP) {
		mp->rc = EINVAL;
		return (mp->rc);
	}

	mp->rc = sli_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	mp->rc = bmap_get(fcmh, mq->bmapno, SL_READ, &bcm);
	if (mp->rc) {
		psclog_errorx("failed to load fid "SLPRI_FID" bmap %u: %s",
		    mq->fg.fg_fid, mq->bmapno, slstrerror(mp->rc));
		goto out;
	}

	s = slvr_lookup(mq->slvrno, bmap_2_biodi(bcm), aio ?
		SL_WRITE : SL_READ);

	if (aio) {
		SLVR_LOCK(s);
		/* Aio bit should only be set for slivers which are
		 *    replica destination buffers.
		 */
		if (s->slvr_flags & SLVR_REPLSRC) {
			DEBUG_SLVR(PLL_ERROR, s, "SLVR_REPLSRC set ?");
			abort();
		}
		/* Block until the callback handler has finished.
		 */
		SLVR_WAIT(s, (s->slvr_flags & SLVR_REPLWIRE));
		SLVR_ULOCK(s);
		/* Lookup the workrq.  It should have already been created.
		 */
		w = sli_repl_findwq(&mq->fg, mq->bmapno);
		if (!w) {
			mp->rc = ENOENT;
			DEBUG_SLVR(PLL_ERROR, s,
			   "sli_repl_findwq() failed to find wq");
			//XXX cleanup the sliver ref
			goto out;
		}
		/* Ensure the sliver is found in the work item's array.
		 */
		for (slvridx = 0; slvridx < (int)nitems(w->srw_slvr_refs);
		     slvridx++)
			if (w->srw_slvr_refs[slvridx] == s)
				break;

		if (slvridx == (int)nitems(w->srw_slvr_refs)) {
			mp->rc = ENOENT;
			DEBUG_SLVR(PLL_ERROR, s,
			   "failed to find slvr in wq=%p", w);
			goto out;
		}

	} else {
		SLVR_LOCK(s);
		if (s->slvr_flags & SLVR_REPLSRC) {
			DEBUG_SLVR(PLL_WARN, s, "SLVR_REPLSRC already set");
			s->slvr_pndgreads--;

			psc_assert(s->slvr_pndgreads > 0);
			SLVR_ULOCK(s);

			mp->rc = EALREADY;
			goto out;
		} else
			SLVR_ULOCK(s);
	}

	slvr_slab_prep(s, aio ? SL_WRITE : SL_READ);
	slvr_repl_prep(s, aio ? SLVR_REPLDST : SLVR_REPLSRC);
	rv = slvr_io_prep(s, 0, mq->len, aio ? SL_WRITE : SL_READ, &aiocbr);

	iov.iov_base = s->slvr_slab->slb_base;
	iov.iov_len = mq->len;

	if (aiocbr) {
		/* Ran into an async I/O.  It's possible that this sliod
		 *   is an archival_fs.
		 */
		if (s->slvr_flags & SLVR_REPLDST)
			DEBUG_SLVR(PLL_WARN, s, "repldst saw aiowait");

		psc_assert(rv == -SLERR_AIOWAIT);

		SLVR_LOCK(s);
		if (!(s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR))) {
			pll_add(&s->slvr_pndgaios, aiocbr);
			SLVR_ULOCK(s);
			sli_aio_replreply_setup(aiocbr, rq, s, &iov);
			pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
			mp->rc = rv;
			goto out;
		}
		SLVR_ULOCK(s);
		sli_aio_aiocbr_release(aiocbr);
		/* XXX: SLVR_DATAERR */
		rv = 0;
	}

	if (rv) {
		mp->rc = rv;
		goto out;
	}

	mp->rc = rsx_bulkserver(rq, aio ? BULK_GET_SINK :
	    BULK_PUT_SOURCE, SRII_BULK_PORTAL, &iov, 1);

	if (aio)
		sli_rii_replread_release_sliver(w, slvridx, mp->rc);
	else
		slvr_rio_done(s);

 out:
	if (bcm)
		bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (mp->rc);
}

/**
 * sli_rii_replread_cb - Callback triggered when an SRMT_REPL_READ request
 *	issued finishes.
 */
__static int
sli_rii_replread_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct sli_repl_workrq *w;
	struct srm_io_rep *mp;
	struct slvr_ref *s;
	int rc, slvridx;

	w = args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ];
	s = args->pointer_arg[SRII_REPLREAD_CBARG_SLVR];

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		goto out;

	rc = rq->rq_status;
	if (rc)
		goto out;
	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	if (mp)
		rc = mp->rc;
	else
		rc = EBADMSG;

 out:
	for (slvridx = 0; slvridx < (int)nitems(w->srw_slvr_refs);
	    slvridx++)
		if (w->srw_slvr_refs[slvridx] == s)
			break;
	psc_assert(slvridx < (int)nitems(w->srw_slvr_refs));

	if (rc == -SLERR_AIOWAIT)
		psc_atomic64_inc(&sli_rpc_repl_read_cb_aio);
	else
		psc_atomic64_inc(&sli_rpc_repl_read_cb);
	return (sli_rii_replread_release_sliver(w, slvridx, rc));
}

int
sli_rii_issue_repl_read(struct slashrpc_cservice *csvc, int slvrno,
    int slvridx, struct sli_repl_workrq *w)
{
	const struct srm_repl_read_rep *mp;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct slvr_ref *s;
	struct iovec iov;
	int rc;

	psclog_info("srw %p fg "SLPRI_FID" bmapno %d slvrno %d idx "
	    "%d len %u", w, w->srw_fg.fg_fid, w->srw_bmapno, slvrno,
	    slvridx, w->srw_len);

	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_READ, rq, mq, mp);
	if (rc)
		return (rc);

	psc_atomic64_inc(&sli_rpc_repl_read);

	mq->len = SLASH_SLVR_SIZE;
	/* adjust the request size for the last sliver */
	if ((unsigned)slvrno == w->srw_len / SLASH_SLVR_SIZE)
		mq->len = w->srw_len % SLASH_SLVR_SIZE;
	mq->fg = w->srw_fg;
	mq->bmapno = w->srw_bmapno;
	mq->slvrno = slvrno;

	psc_assert(w->srw_slvr_refs[slvridx] == SLI_REPL_SLVR_SCHED);
	w->srw_slvr_refs[slvridx] = s =
		slvr_lookup(slvrno, bmap_2_biodi(w->srw_bcm), SL_WRITE);

	slvr_slab_prep(s, SL_WRITE);
	slvr_repl_prep(s, SLVR_REPLDST|SLVR_REPLWIRE);
	rc = slvr_io_prep(s, 0, mq->len, SL_WRITE, NULL);
	if (rc)
		goto out;

	iov.iov_base = s->slvr_slab->slb_base;
	iov.iov_len = mq->len;

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRII_BULK_PORTAL, &iov,
	    1);
	if (rc)
		goto out;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_SLVR] = s;

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	psc_atomic32_inc(&w->srw_refcnt);
	psc_assert(pscrpc_nbreqset_add(&sli_replwk_nbset, rq) == 0);

 out:
	if (rc)
		sli_rii_replread_release_sliver(w, slvridx, rc);
	return (rc);
}

int
sli_rii_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    sli_geticsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRII_MAGIC, SRII_VERSION,
		    SLCONNT_IOD);
		break;
	case SRMT_REPL_READ:
		rc = sli_rii_handle_replread(rq, 0);
		break;
	case SRMT_REPL_READAIO:
		rc = sli_rii_handle_replread(rq, 1);
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
