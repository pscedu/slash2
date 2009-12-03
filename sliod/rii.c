/* $Id$ */

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
	int i, csize, tsize, slvrno, nslvrs, slvroff;
	const struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slvr_ref *slvr_ref[2];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct iovec iov[2];

	bcm = NULL;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY) {
		mp->rc = EINVAL;
		return (mp->rc);
	}
	if (mq->len <= 0 || mq->len > SLASH_BMAP_SIZE) {
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
	slvrno = (SLASH_BMAP_SIZE * bcm->bcm_blkno) / SLASH_SLVR_SIZE;
	slvroff = (SLASH_BMAP_SIZE * bcm->bcm_blkno) % SLASH_SLVR_SIZE;
	nslvrs = howmany(SLASH_SLVR_SIZE, MIN(SLASH_BMAP_SIZE, mq->len));
	for (i = 0; i < nslvrs; i++, slvroff = 0) {
		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bcm), SL_READ);
		slvr_slab_prep(slvr_ref[i], SL_READ);
		slvr_repl_prep(slvr_ref[i], SLVR_REPLSRC);
		csize = MIN(tsize, SLASH_SLVR_SIZE - slvroff);
		slvr_io_prep(slvr_ref[i], slvroff, csize, SL_READ);
		iov[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + slvroff;
		iov[i].iov_len = csize;
		tsize -= csize;
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
sli_rii_replread_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	int nslvrs, sblk, rc, tsize, slvrno, slvroff, csize, i = 0;
	struct sli_repl_workrq *w;
	struct srm_io_rep *mp;

	w = args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ];
	nslvrs = howmany(SLASH_SLVR_SIZE, MIN(SLASH_BMAP_SIZE, w->srw_len));

	rc = rq->rq_status;
	if (rc)
		goto out;
	mp = psc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	if (mp == NULL) {
		rc = EBADMSG;
		goto out;
	}
	if (mp->rc) {
		rc = mp->rc;
		goto out;
	}

	tsize = w->srw_len;
	slvrno = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE) / SLASH_SLVR_SIZE;
	slvroff = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE) % SLASH_SLVR_SIZE;
	sblk = slvroff / SLASH_SLVR_BLKSZ;
	if (slvroff & SLASH_SLVR_BLKMASK)
		tsize += slvroff & SLASH_SLVR_BLKMASK;
	for (; i < nslvrs; i++) {
		csize = MIN((SLASH_BLKS_PER_SLVR - sblk) *
		    SLASH_SLVR_BLKSZ, tsize);
		if ((rc = slvr_fsbytes_wio(w->srw_slvr_ref[i], csize, sblk)))
			break;
		sblk = 0;
		slvr_io_done(w->srw_slvr_ref[i], SL_WRITE);
		tsize -= csize;
	}
 out:
	for (; i < nslvrs; i++)
		slvr_io_done(w->srw_slvr_ref[i], SL_WRITE);
	bmap_op_done(w->srw_bcm);
	fidc_membh_dropref(w->srw_fcmh);
	sli_repl_finishwk(w, rc);
	return (rc);
}

int
sli_rii_issue_repl_read(struct pscrpc_import *imp, struct sli_repl_workrq *w)
{
	int i, rc, tsize, csize, slvrno, nslvrs, slvroff;
	const struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct iovec iov[2];

	nslvrs = 0;
	w->srw_fcmh = iod_inode_lookup(&w->srw_fg);
	rc = iod_inode_open(w->srw_fcmh, SL_WRITE);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, w->srw_fcmh, "iod_inode_open");
		goto out;
	}

	if ((rc = RSX_NEWREQ(imp, SRII_VERSION,
	    SRMT_REPL_READ, rq, mq, mp)) != 0)
		goto out;
	mq->fg = w->srw_fg;
	mq->len = w->srw_len;
	mq->bmapno = w->srw_bmapno;

	rc = iod_bmap_load(w->srw_fcmh, w->srw_bmapno, SL_WRITE, &w->srw_bcm);
	if (rc) {
		psc_errorx("iod_map_load %u: %s",
		    w->srw_bmapno, slstrerror(rc));
		goto out;
	}

	nslvrs = 1;
	tsize = w->srw_len;
	slvrno = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE) / SLASH_SLVR_SIZE;
	slvroff = (w->srw_bcm->bcm_blkno * SLASH_BMAP_SIZE) % SLASH_SLVR_SIZE;
	nslvrs = howmany(SLASH_SLVR_SIZE, MIN(SLASH_BMAP_SIZE, w->srw_len));
	for (i = 0; i < nslvrs; i++, slvroff = 0) {
		csize = MIN(tsize, SLASH_SLVR_SIZE - slvroff);
		w->srw_slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(w->srw_bcm), SL_WRITE);
		slvr_slab_prep(w->srw_slvr_ref[i], SL_WRITE);
		slvr_repl_prep(w->srw_slvr_ref[i], SLVR_REPLDST);
		slvr_io_prep(w->srw_slvr_ref[i], slvroff, csize, SL_WRITE);
		iov[i].iov_base = w->srw_slvr_ref[i]->slvr_slab->slb_base + slvroff;
		iov[i].iov_len = csize;
		tsize -= csize;
	}

	rc = rsx_bulkclient(rq, &desc, BULK_PUT_SINK,
	    SRII_BULK_PORTAL, iov, nslvrs);
	if (rc)
		goto out;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;

	atomic_inc(&w->srw_bcm->bcm_opcnt);
	fidc_membh_incref(w->srw_fcmh);
	nbreqset_add(&sli_replwk_nbset, rq);

 out:
	if (rc)
		for (i = 0; i < nslvrs; i++)
			slvr_io_done(w->srw_slvr_ref[i], SL_WRITE);
	if (w->srw_bcm)
		bmap_op_done(w->srw_bcm);
	if (w->srw_fcmh)
		fidc_membh_dropref(w->srw_fcmh);
	return (rc);
}
