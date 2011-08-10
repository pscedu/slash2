/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

int
sli_rii_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_connect_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRII_MAGIC || mq->version != SRII_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/**
 * sli_rii_handle_replread - Handle a REPL_READ request from another
 *	I/O node.  We load the sliver for READ and attach the data
 *	buffer to the RPC reply bulk.
 */
int
sli_rii_handle_replread(struct pscrpc_request *rq)
{
	const struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct slvr_ref *slvr_ref;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct iovec iov;

	bcm = NULL;

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

	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);

	slvr_ref = slvr_lookup(mq->slvrno, bmap_2_biodi(bcm), SL_READ);

	SLVR_LOCK(slvr_ref);
	if (slvr_ref->slvr_flags & SLVR_REPLSRC) {
		DEBUG_SLVR(PLL_WARN, slvr_ref, "SLVR_REPLSRC already set");
		slvr_ref->slvr_pndgreads--;

		psc_assert(slvr_ref->slvr_pndgreads > 0);

		mp->rc = EALREADY;
		SLVR_ULOCK(slvr_ref);
		goto out;
	} else		
		SLVR_ULOCK(slvr_ref);

	slvr_slab_prep(slvr_ref, SL_READ);
	slvr_repl_prep(slvr_ref, SLVR_REPLSRC);
	slvr_io_prep(NULL, NULL, slvr_ref, 0, mq->len, SL_READ);
	iov.iov_base = slvr_ref->slvr_slab->slb_base;
	iov.iov_len = mq->len;

	mp->rc = rsx_bulkserver(rq, BULK_PUT_SOURCE, SRII_BULK_PORTAL,
	    &iov, 1);

	slvr_io_done(slvr_ref, 0, mq->len, SL_READ);

 out:
	if (bcm)
		bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (mp->rc);
}

int
sli_rii_replread_release_sliver(struct sli_repl_workrq *w,
    int slvridx, int rc)
{
	struct slvr_ref *s;
	int slvrsiz;

	s = w->srw_slvr_refs[slvridx];

	slvrsiz = SLASH_SLVR_SIZE;
	if (s->slvr_num == w->srw_len / SLASH_SLVR_SIZE)
		slvrsiz = w->srw_len % SLASH_SLVR_SIZE;
	if (rc == 0) {
		SLVR_LOCK(s);
		s->slvr_crc_loff = s->slvr_crc_soff = 0;
		s->slvr_crc_eoff = slvrsiz;
		SLVR_ULOCK(s);
		rc = slvr_do_crc(s);
		/* XXX check this return code */
//		if (!rc)
		if (1) {
			/* SLVR_DATARDY is set in wio_done
			 *    when the slvr lock is taken again.
			 */
			rc = slvr_fsbytes_wio(NULL, s, slvrsiz, 0);
		}
	}
	if (rc) {
		SLVR_LOCK(s);
		slvr_clear_inuse(s, 0, slvrsiz);
		s->slvr_flags |= SLVR_REPLFAIL;
		SLVR_ULOCK(s);
	}

	DEBUG_SLVR(PLL_INFO, s, "replread complete rc=%d", rc);

	slvr_io_done(s, 0, w->srw_len, SL_WRITE);
	w->srw_slvr_refs[slvridx] = NULL;
	return (rc);
}

/**
 * sli_rii_replread_cb - Callback triggered when an REPL_READ request
 *	issued finishes.
 */
int
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
	for (slvridx = 0; slvridx < (int)nitems(w->srw_slvr_refs); slvridx++)
		if (w->srw_slvr_refs[slvridx] == s)
			break;
	psc_assert(slvridx < (int)nitems(w->srw_slvr_refs));
	rc = sli_rii_replread_release_sliver(w, slvridx, rc);

	spinlock(&w->srw_lock);
	w->srw_nslvr_cur++;
	/* place back on pending queue until the last sliver finishes or error */
	if (psclist_disjoint(&w->srw_state_lentry)) {
		lc_add(&sli_replwkq_pending, w);
		psc_atomic32_inc(&w->srw_refcnt);
	}
	sli_replwkrq_decref(w, rc);
	return (rc);
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

	psclog_dbg("srw %p fg "SLPRI_FID" bmapno %d slvrno %d idx "
	    "%d len %u", w, w->srw_fg.fg_fid, w->srw_bmapno, slvrno,
	    slvridx, w->srw_len);

	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_READ, rq, mq, mp);
	if (rc)
		return (rc);

	mq->len = SLASH_SLVR_SIZE;
	if ((unsigned)slvrno == w->srw_len / SLASH_SLVR_SIZE)
		mq->len = w->srw_len % SLASH_SLVR_SIZE;
	mq->fg = w->srw_fg;
	mq->bmapno = w->srw_bmapno;
	mq->slvrno = slvrno;

	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);
	w->srw_slvr_refs[slvridx] = s =
	    slvr_lookup(slvrno, bmap_2_biodi(w->srw_bcm), SL_WRITE);

	slvr_slab_prep(s, SL_WRITE);

	slvr_repl_prep(s, SLVR_REPLDST);

	rc = slvr_io_prep(NULL, NULL, s, 0, mq->len, SL_WRITE);
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
	psc_atomic32_inc(&w->srw_refcnt);

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
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
		rc = sli_rii_handle_connect(rq);
		break;
	case SRMT_REPL_READ:
		rc = sli_rii_handle_replread(rq);
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
