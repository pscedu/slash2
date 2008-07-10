/* $Id$ */

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"

#include "slashrpc.h"

/*
 * msrcm_handle_releasebmap - handle a RELEASEBMAP request for client from MDS.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	struct srm_releasebmap_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

/*
 * msrcm_handle_connect - handle a CONNECT request for client from MDS.
 */
int
msrcm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCM_MAGIC || mq->version != SRCM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/*
 * msrcm_handler - handle a request for client from MDS.
 */
int
msrcm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_RELEASEBMAP:
		rc = msrcm_handle_releasebmap(rq);
		break;
	case SRMT_CONNECT:
		rc = msrcm_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
