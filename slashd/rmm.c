/* $Id$ */

/*
 * Routines for handling RPC requests for MDS from MDS.
 */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "cfd.h"
#include "fid.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

struct psclist_head mds_server_conns = PSCLIST_HEAD_INIT(mds_server_conns);

struct mds_server_conn {
	struct psclist_head		 msc_lentry;
	struct slashrpc_cservice	*msc_csvc;
};

/*
 * slm_rmm_handle_connect - handle a CONNECT request from another MDS.
 */
int
slm_rmm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMM_MAGIC || mq->version != SRMM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/*
 * slm_rmm_handler - handle a request from another MDS.
 */
int
slm_rmm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slm_rmm_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}

/*
 * slm_rmm_addconn - initiate a connection to MDS from MDS.
 */
int
slm_rmm_addconn(const char *name)
{
	struct slashrpc_cservice *csvc;
	struct mds_server_conn *msc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	csvc = rpc_csvc_create(SRMM_REQ_PORTAL, SRMM_REP_PORTAL);
	if (rpc_issue_connect(nid, csvc->csvc_import,
	    SRMM_MAGIC, SRMM_VERSION))
		return (-1);

	msc = PSCALLOC(sizeof(*msc));
	msc->msc_csvc = csvc;
	psclist_xadd(&msc->msc_lentry, &mds_server_conns);
	return (0);
}
