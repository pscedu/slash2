/* $Id$ */

/*
 * Routines for issuing RPC requests for CLIENT from MDS.
 */

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"

#include "slashrpc.h"

/*
 * slrcm_issue_releasebmap - issue a RELEASEBMAP request to a CLIENT from MDS.
 */
int
slrcm_issue_releasebmap(struct pscrpc_import *imp)
{
	struct srm_releasebmap_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRCM_VERSION,
	    SRMT_RELEASEBMAP, rq, mq, mp)) != 0)
		return (rc);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}
