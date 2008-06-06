/* $Id$ */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "cfd.h"
#include "fid.h"
#include "rpc.h"
#include "slashd.h"
#include "slashrpc.h"

#define SRMI_NTHREADS	8
#define SRMI_NBUFS	1024
#define SRMI_BUFSZ	128
#define SRMI_REPSZ	128
#define SRMI_SVCNAME	"slrmithr"

int
slrmi_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMI_MAGIC || mq->version != SRMI_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slrmi_svc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrmi_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}

/**
 * slrmi_init - start up the MDS <-> I/O threads
 */
void
slrmi_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = SRMI_NBUFS;
	svh->svh_bufsz      = SRMI_BUFSZ;
	svh->svh_reqsz      = SRMI_BUFSZ;
	svh->svh_repsz      = SRMI_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type       = SLTHRT_RMI;
	svh->svh_nthreads   = SRMI_NTHREADS;
	svh->svh_handler    = slrmi_svc_handler;

	strlcpy(svh->svh_svc_name, SRMI_SVCNAME, sizeof(svh->svh_svc_name));

	pscrpc_thread_spawn(svh, struct slash_rmithr);
}
