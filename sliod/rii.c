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

#include "fidc_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"

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
	const struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct iovec iov;

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
	if (iod_inode_open(fcmh, SL_READ)) {
		DEBUG_FCMH(PLL_ERROR, fcmh, "iod_inode_open");
		mp->rc = EIO;
		goto out;
	}

	if (iod_bmap_load(fcmh, mq->bmapno, SL_READ, &bcm)) {
		psc_errorx("failed to load fid %lx bmap %u",
		    mq->fg.fg_fid, mq->bmapno);
		mp->rc = EIO;
		goto out;
	}

//	iov.iov_base = ;
//	iov.iov_len = mq->len;

	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRII_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);

 out:
	/* XXX release our reference to the bmap */
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
	struct srm_io_rep *mp;
	int rc;

	rc = rq->rq_status;
	mp = psc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	if (mp == NULL) {
		if (rc == 0)
			rc = EBADMSG;
	} else {
		if (rc == 0 && mp->rc)
			rc = mp->rc;
	}

	sli_repl_finishwk(args->pointer_arg[SRII_REPLREAD_CBARG_WKRQ], rc);
	return (rc);
}

int
sli_rii_issue_repl_read(struct pscrpc_import *imp, struct sli_repl_workrq *w)
{
	const struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct iovec iov;
	int rc;

	w->srw_srb = psc_pool_get(sli_replwkbuf_pool);

	if ((rc = RSX_NEWREQ(imp, SRII_VERSION,
	    SRMT_REPL_READ, rq, mq, mp)) != 0)
		return (rc);
	mq->fg = w->srw_fg;
	mq->len = w->srw_len;
	mq->bmapno = w->srw_bmapno;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_replread_cb;
	rq->rq_async_args.pointer_arg[SRII_REPLREAD_CBARG_WKRQ] = w;

	iov.iov_base = w->srw_srb->srb_data;
	iov.iov_len = w->srw_len;
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRII_BULK_PORTAL,
	    &iov, 1);

	nbreqset_add(&sli_replwk_nbset, rq);
	return (0);
}
