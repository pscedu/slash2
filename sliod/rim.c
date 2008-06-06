/* $Id$ */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "sliod.h"
#include "slashrpc.h"

#define SRIM_NTHREADS	8
#define SRIM_NBUFS	1024
#define SRIM_BUFSZ	256
#define SRIM_REPSZ	256
#define SRIM_SVCNAME	"slrimthr"

int
slrim_svc_handler(struct pscrpc_request *rq)
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


/**
 * slrim_init - start up the I/O threads via pscrpc_thread_spawn()
 */
void
slrim_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = SRIM_NBUFS;
	svh->svh_bufsz      = SRIM_BUFSZ;
	svh->svh_reqsz      = SRIM_BUFSZ;
	svh->svh_repsz      = SRIM_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type       = SLIOTHRT_RIM;
	svh->svh_nthreads   = SRIM_NTHREADS;
	svh->svh_handler    = slrim_svc_handler;

	strlcpy(svh->svh_svc_name, SRIM_SVCNAME, sizeof(svh->svh_svc_name));

	pscrpc_thread_spawn(svh, struct slash_rimthr);
}
