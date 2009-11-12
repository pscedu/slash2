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
sli_rim_handle_repl_schedwk(struct pscrpc_request *rq)
{
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	sli_repl_addwk(mq->nid, &mq->fg, mq->bmapno);
	return (0);
}

int
sli_rim_handle_connect(struct pscrpc_request *rq)
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
		rc = sli_rim_handle_repl_schedwk(rq);
		break;
	case SRMT_CONNECT:
		rc = sli_rim_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
