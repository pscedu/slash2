/* $Id$ */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_rpc/rpclog.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "slashrpc.h"
#include "slashd.h"
#include "rpc.h"
#include "cfd.h"

#define SRB_NTHREADS	8
#define SRB_NBUFS	1024
#define SRB_BUFSZ	128
#define SRB_REPSZ	128
#define SRB_SVCNAME	"slrpcbethr"

int
slbe_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRB_MAGIC || mq->version != SRB_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slbe_getfid(struct pscrpc_request *rq)
{
	struct slashrpc_export *sexp, qexp;
	struct pscrpc_connection c;
	struct pscrpc_export exp;
	struct srm_getfid_req *mq;
	struct srm_getfid_rep *mp;
	struct cfdent *cfdent, qcfd;

	RSX_ALLOCREP(rq, mq, mp);
	exp.exp_connection = &c;
	exp.exp_connection->c_peer.nid = mq->nid;
	exp.exp_connection->c_peer.pid = mq->pid;
	qexp.exp = &exp;

	spinlock(&sexptreelock);
	sexp = SPLAY_FIND(sexptree, &sexptree, &qexp);
	if (sexp) {
		qcfd.cfd = mq->cfd;
		spinlock(&sexp->exp->exp_lock);
		cfdent = SPLAY_FIND(cfdtree, &sexp->cfdtree, &qcfd);
		if (cfdent)
			COPYFID(&mp->fid, &cfdent->fid);
		else
			mp->rc = -ENOENT;
		freelock(&sexp->exp->exp_lock);
	} else
		mp->rc = -ENOENT;
	freelock(&sexptreelock);
	return (0);
}

int
slbe_svc_handler(struct pscrpc_request *req)
{
	int rc = 0;

	ENTRY;
	DEBUG_REQ(PLL_TRACE, req, "new req");

	switch (req->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slbe_connect(req);
		break;
	case SRMT_GETFID:
		rc = slbe_getfid(req);
		break;
	default:
		psc_errorx("Unexpected opcode %d", req->rq_reqmsg->opc);
		req->rq_status = -ENOSYS;
		rc = pscrpc_error(req);
		goto done;
	}
	psc_info("req->rq_status == %d", req->rq_status);
	target_send_reply_msg (req, rc, 0);

 done:
	RETURN(rc);
}

/**
 * slbe_init - start up the MDS <-> I/O backend threads
 */
void
slbe_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = SRB_NBUFS;
	svh->svh_bufsz      = SRB_BUFSZ;
	svh->svh_reqsz      = SRB_BUFSZ;
	svh->svh_repsz      = SRB_REPSZ;
	svh->svh_req_portal = SRB_REQ_PORTAL;
	svh->svh_rep_portal = SRB_REP_PORTAL;
	svh->svh_type       = SLTHRT_RPCBE;
	svh->svh_nthreads   = SRB_NTHREADS;
	svh->svh_handler    = slbe_svc_handler;

	snprintf(svh->svh_svc_name, PSCRPC_SVCNAME_MAX, "%s", SRB_SVCNAME);

	pscrpc_thread_spawn(svh, struct slash_rpcbethr);
}
