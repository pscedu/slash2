/* $Id$ */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "sliod.h"
#include "slashrpc.h"
#include "rpc.h"

struct psclist_head io_server_conns = PSCLIST_HEAD_INIT(io_server_conns);

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

int
slrii_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRII_MAGIC || mq->version != SRII_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slrii_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrii_handle_connect(rq);
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
slrii_addconn(const char *name)
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
