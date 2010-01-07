/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_ds/pool.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

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
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRII_MAGIC || mq->version != SRII_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
sli_rii_handle_replread(struct pscrpc_request *rq)
{
	const struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slvr_ref *slvr_ref;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct iovec iov;

	bcm = NULL;

	RSX_ALLOCREP(rq, mq, mp);
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

	fcmh = iod_inode_lookup(&mq->fg);
	mp->rc = iod_inode_open(fcmh, SL_READ);
	if (mp->rc) {
		DEBUG_FCMH(PLL_ERROR, fcmh, "iod_inode_open: %s",
		    slstrerror(mp->rc));
		goto out;
	}

	mp->rc = iod_bmap_load(fcmh, mq->bmapno, SL_READ, &bcm);
	if (mp->rc) {
		psc_errorx("failed to load fid %lx bmap %u: %s",
		    mq->fg.fg_fid, mq->bmapno, slstrerror(mp->rc));
		goto out;
	}

	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);

	slvr_ref = slvr_lookup(mq->slvrno, bmap_2_biodi(bcm), SL_READ);
	slvr_slab_prep(slvr_ref, SL_READ);
	slvr_repl_prep(slvr_ref, SLVR_REPLSRC);
	slvr_io_prep(slvr_ref, 0, mq->len, SL_READ);
	iov.iov_base = slvr_ref->slvr_slab->slb_base;
	iov.iov_len = mq->len;

	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRII_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);

	slvr_io_done(slvr_ref, SL_READ);

 out:
	if (bcm)
		bmap_op_done(bcm);
	fidc_membh_dropref(fcmh);
	return (mp->rc);
}

int
sli_rii_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = sli_rii_handle_connect(rq);
		break;
	case SRMT_REPL_READ:
		rc = sli_rii_handle_replread(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
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
	if (rc == 0)
		rc = slvr_fsbytes_wio(s, slvrsiz, 0);
	if (rc)
		slvr_clear_inuse(s, 0, slvrsiz);
	slvr_io_done(s, SL_WRITE);
	w->srw_slvr_refs[slvridx] = NULL;
	return (rc);
}

int
sli_rii_replread_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct sli_repl_workrq *w;
	struct srm_io_rep *mp;
	struct slvr_ref *s;
	int rc, slvridx, slvrno;

	w = args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ];
	s = args->pointer_arg[SRII_REPLREAD_CBARG_SLVR];
	slvrno = s->slvr_num;

	rc = rq->rq_status;
	if (rc)
		goto out;
	mp = psc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	if (mp)
		rc = mp->rc;
	else
		rc = EBADMSG;

 out:
	for (slvridx = 0; slvridx < nitems(w->srw_slvr_refs); slvridx++)
		if (w->srw_slvr_refs[slvridx] == s)
			break;
	psc_assert(slvridx < nitems(w->srw_slvr_refs));
	rc = sli_rii_replread_release_sliver(w, slvridx, rc);

	BMAP_LOCK(w->srw_bcm);
	bmap_2_crcbits(w->srw_bcm, slvrno) |= BMAP_SLVR_WANTREPL;
	BMAP_ULOCK(w->srw_bcm);

	spinlock(&w->srw_lock);
	psc_vbitmap_unset(w->srw_inflight, slvridx);

	/* place back on pending queue until the last sliver finishes or error */
	if (psclist_disjoint(&w->srw_state_lentry)) {
		lc_add(&sli_replwkq_pending, w);
		psc_atomic32_inc(&w->srw_refcnt);
	}
	sli_replwkrq_decref(w, rc);
	return (rc);
}

int
sli_rii_issue_repl_read(struct pscrpc_import *imp, int slvrno,
    int slvridx, struct sli_repl_workrq *w)
{
	const struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct slvr_ref *s;
	struct iovec iov;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRII_VERSION,
	    SRMT_REPL_READ, rq, mq, mp)) != 0)
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
	slvr_io_prep(s, 0, mq->len, SL_WRITE);
	iov.iov_base = s->slvr_slab->slb_base;
	iov.iov_len = mq->len;

	rc = rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRII_BULK_PORTAL, &iov, 1);
	if (rc)
		goto out;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_SLVR] = s;
	psc_atomic32_inc(&w->srw_refcnt);

	nbreqset_add(&sli_replwk_nbset, rq);

 out:
	if (rc)
		sli_rii_replread_release_sliver(w, slvridx, rc);
	return (rc);
}
