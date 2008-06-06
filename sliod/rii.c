/* $Id$ */

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
slrii_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		rc = pscrpc_error(rq);
		goto done;
	}
	target_send_reply_msg(rq, rc, 0);

 done:
	return (rc);
}

int
slrii_issue_connect(const char *name)
{
	struct io_server_conn *isc;
	lnet_nid_t nid;

	isc = PSCALLOC(sizeof(*isc));

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	isc->isc_csvc = rpc_csvc_create(SRII_REQ_PORTAL, SRII_REP_PORTAL);
	if (rpc_issue_connect(nid, isc->isc_csvc->csvc_import,
	    SRII_MAGIC, SRII_VERSION))
		psc_error("rpc_connect %s", name);
	psclist_xadd(&isc->isc_lentry, &io_server_conns);
}
