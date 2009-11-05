/* $Id$ */

/*
 * Routines for handling RPC requests for ION from MDS.
 */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"

int
slrim_handle_repl_schedwk(struct pscrpc_request *rq)
{
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	sli_repl_addwk(mq->nid, mq->fid, mq->bmapno);
	return (0);
}

int
slrim_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRIM_MAGIC || mq->version != SRIM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
sli_rim_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_REPL_SCHEDWK:
		rc = slrim_handle_repl_schedwk(rq);
		break;
	case SRMT_CONNECT:
		rc = slrim_handle_connect(rq);
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
sli_rmi_issue_connect(const char *name)
{
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	if (rpc_issue_connect(nid, rmi_csvc->csvc_import,
	    SRMI_MAGIC, SRMI_VERSION)) {
		psc_error("rpc_connect %s", name);
		return (-1);
	}
	return (0);
}

int
sli_rmi_issue_repl_schedwk(uint64_t nid, uint64_t fid, sl_bmapno_t bmapno,
    int rv)
{
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = RSX_NEWREQ(rmi_csvc->csvc_import,
	    SRMI_VERSION, SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		return (rc);
	mq->nid = nid;
	mq->fid = fid;
	mq->bmapno = bmapno;
	mq->rc = rv;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}
