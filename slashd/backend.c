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
slrmi_getfid(struct pscrpc_request *rq)
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
slrmi_svc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrmi_connect(rq);
		break;
	case SRMT_GETFID:
		rc = slrmi_getfid(rq);
		break;
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
 * slrmi_init - start up the MDS <-> I/O backend threads
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
