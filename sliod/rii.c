/* $Id$ */

/*
 * Routines for handling RPC requests for ION from ION.
 */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"

#define SRII_READ_CBARG_WKRQ 0

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
sli_rii_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = sli_rii_handle_connect(rq);
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
sli_rii_read_callback(struct pscrpc_request *rq, struct pscrpc_async_args *args)
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

	sli_repl_finishwk(args->pointer_arg[SRII_READ_CBARG_WKRQ], rc);
	return (rc);
}

int
sli_rii_issue_read(struct pscrpc_import *imp, struct sli_repl_workrq *w)
{
	const struct srm_repl_read_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_repl_read_req *mq;
	struct pscrpc_request *rq;
	struct iovec iov;
	int rc;

//	p = mmap(fcoo_fd);
//	iov.iov_base = p

	if ((rc = RSX_NEWREQ(imp, SRII_VERSION,
	    SRMT_REPL_READ, rq, mq, mp)) != 0)
		return (rc);
	mq->fid = w->srw_fid;
	mq->len = w->srw_len;
	mq->bmapno = w->srw_bmapno;

	/* Setup state for callbacks */
	rq->rq_interpret_reply = sli_rii_read_callback;
	rq->rq_async_args.pointer_arg[SRII_READ_CBARG_WKRQ] = w;

	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRII_BULK_PORTAL,
	    &iov, 1);
	return (0);
}

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

/*
 * sli_rii_addconn - initiate a connection to ION from ION.
 */
int
sli_rii_addconn(const char *name)
{
	struct slashrpc_cservice *csvc;
	struct io_server_conn *isc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	csvc = rpc_csvc_create(SRII_REQ_PORTAL, SRII_REP_PORTAL);
	if (rpc_issue_connect(nid, csvc->csvc_import,
	    SRII_MAGIC, SRII_VERSION))
		return (-1);

	isc = PSCALLOC(sizeof(*isc));
	isc->isc_csvc = csvc;
	psclist_xadd(&isc->isc_lentry, &io_server_conns);
	return (0);
}
