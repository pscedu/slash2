/* $Id$ */

#include <stdint.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"

int
sli_rmi_connect(const char *name)
{
	struct sl_resource *res;
	struct sl_resm *resm;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		nid = res->res_nids[0];
	} else {
		resm = libsl_nid2resm(nid);
		res = resm->resm_res;
	}
	if (res == NULL)
		psc_fatalx("%s: unknown resource", name);
	if (!res->res_type != SLREST_MDS)
		psc_fatalx("%s: not an MDS", name);

	if (rpc_issue_connect(nid, rmi_csvc->csvc_import,
	    SRMI_MAGIC, SRMI_VERSION)) {
		psc_error("rpc_connect %s", name);
		return (-1);
	}
	return (0);
}

int
sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *w)
{
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	rc = RSX_NEWREQ(rmi_csvc->csvc_import,
	    SRMI_VERSION, SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		return (rc);
	mq->nid = w->srw_nid;
	mq->fg = w->srw_fg;
	mq->bmapno = w->srw_bmapno;
	mq->rc = w->srw_status;
	rc = RSX_WAITREP(rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}
