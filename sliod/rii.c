/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#define SRII_REPLREAD_CBARG_WKRQ 0

struct psclist_head io_server_conns = PSCLIST_HEAD_INIT(io_server_conns);

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
	int i, slvrsiz, tsize, slvrno, nslvrs;
	struct iovec iov[REPL_MAX_INFLIGHT_SLVRS];
	const struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slvr_ref *slvr_ref[2];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;

	bcm = NULL;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	/* check each individually here to avoid overflow in the addition following */
	if (mq->len <= 0 || mq->len > SLASH_SLVR_SIZE * REPL_MAX_INFLIGHT_SLVRS) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	if (mq->offset < 0 || mq->offset > SLASH_BMAP_SIZE - SLASH_SLVR_SIZE) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	if (mq->len + mq->offset > SLASH_BMAP_SIZE) {
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

	tsize = mq->len;
	slvrno = (SLASH_BMAP_SIZE * bcm->bcm_blkno + mq->offset) / SLASH_SLVR_SIZE;
	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);
	psc_assert((mq->offset % SLASH_SLVR_SIZE) == 0);
	nslvrs = howmany(tsize, SLASH_SLVR_SIZE);
	for (i = 0; i < nslvrs; i++) {
		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bcm), SL_READ);
		slvr_slab_prep(slvr_ref[i], SL_READ);
		slvr_repl_prep(slvr_ref[i], SLVR_REPLSRC);
		slvrsiz = MIN(tsize, SLASH_SLVR_SIZE);
		slvr_io_prep(slvr_ref[i], 0, slvrsiz, SL_READ);
		iov[i].iov_base = slvr_ref[i]->slvr_slab->slb_base;
		iov[i].iov_len = slvrsiz;
		tsize -= slvrsiz;
	}

	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRII_BULK_PORTAL, iov, nslvrs);
	if (desc)
		pscrpc_free_bulk(desc);

	for (i = 0; i < nslvrs; i++)
		slvr_io_done(slvr_ref[i], SL_READ);

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
sli_rii_replread_clean_slivers(struct sli_repl_workrq *w, int rc)
{
	int i, nslvrs, tsize, slvrno, slvrsiz;

	tsize = w->srw_xferlen;
	slvrno = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE +
	    w->srw_offset) / SLASH_SLVR_SIZE;
	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);
	psc_assert((w->srw_offset % SLASH_SLVR_SIZE) == 0);
	nslvrs = howmany(tsize, SLASH_SLVR_SIZE);
	for (i = 0; i < nslvrs; i++) {
		slvrsiz = MIN(SLASH_BLKS_PER_SLVR * SLASH_SLVR_BLKSZ, tsize);
		if (rc == 0)
			rc = slvr_fsbytes_wio(w->srw_slvr_ref[i], slvrsiz, 0);
		if (rc)
			slvr_clear_inuse(w->srw_slvr_ref[i], 0, slvrsiz);
		slvr_io_done(w->srw_slvr_ref[i], SL_WRITE);
		tsize -= slvrsiz;
		w->srw_offset += slvrsiz;
	}
	return (rc);
}

int
sli_rii_replread_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct sli_repl_workrq *w;
	struct srm_io_rep *mp;
	int rc;

	w = args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ];

	rc = rq->rq_status;
	if (rc)
		goto out;
	mp = psc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	if (mp)
		rc = mp->rc;
	else
		rc = EBADMSG;

 out:
	rc = sli_rii_replread_clean_slivers(w, rc);

	lc_remove(&sli_replwkq_inflight, w);
	if (rc || w->srw_offset == SLASH_BMAP_SIZE) {
		w->srw_status = rc;
		lc_add(&sli_replwkq_finished, w);
	} else
		/* place back on pending queue until the last sliver finishes */
		lc_add(&sli_replwkq_pending, w);
	return (rc);
}

int
sli_rii_issue_repl_read(struct pscrpc_import *imp, struct sli_repl_workrq *w)
{
	int i, rc, tsize, slvrsiz, slvrno, nslvrs;
	struct iovec iov[REPL_MAX_INFLIGHT_SLVRS];
	const struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;

	w->srw_xferlen = 0;

	if ((rc = RSX_NEWREQ(imp, SRII_VERSION,
	    SRMT_REPL_READ, rq, mq, mp)) != 0)
		goto out;

	w->srw_xferlen = tsize = MIN(w->srw_len, SLASH_SLVR_SIZE * REPL_MAX_INFLIGHT_SLVRS);
	slvrno = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE + w->srw_offset) / SLASH_SLVR_SIZE;
	psc_assert((SLASH_BMAP_SIZE % SLASH_SLVR_SIZE) == 0);
	psc_assert((w->srw_offset % SLASH_SLVR_SIZE) == 0);
	nslvrs = howmany(tsize, SLASH_SLVR_SIZE);
	for (i = 0; i < nslvrs; i++) {
		slvrsiz = MIN(tsize, SLASH_SLVR_SIZE);
		w->srw_slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(w->srw_bcm), SL_WRITE);
		slvr_slab_prep(w->srw_slvr_ref[i], SL_WRITE);
		slvr_repl_prep(w->srw_slvr_ref[i], SLVR_REPLDST);
		slvr_io_prep(w->srw_slvr_ref[i], 0, slvrsiz, SL_WRITE);
		iov[i].iov_base = w->srw_slvr_ref[i]->slvr_slab->slb_base;
		iov[i].iov_len = slvrsiz;
		tsize -= slvrsiz;
	}

	mq->fg = w->srw_fg;
	mq->len = tsize;
	mq->bmapno = w->srw_bmapno;
	mq->offset = w->srw_offset;

	rc = rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRII_BULK_PORTAL, iov, nslvrs);
	if (rc)
		goto out;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;

	bmap_op_start(w->srw_bcm);
	fidc_membh_incref(w->srw_fcmh);
	nbreqset_add(&sli_replwk_nbset, rq);

 out:
	if (rc)
		sli_rii_replread_clean_slivers(w, rc);
	if (w->srw_bcm)
		bmap_op_done(w->srw_bcm);
	if (w->srw_fcmh)
		fidc_membh_dropref(w->srw_fcmh);
	return (rc);
}
