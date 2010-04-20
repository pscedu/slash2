/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <stdint.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "sliod.h"

struct sl_resm *rmi_resm;

int
sli_rmi_getimp(struct slashrpc_cservice **csvcp)
{
	do {
		*csvcp = sli_getmcsvc(rmi_resm);
		if (*csvcp == NULL)
			/* XXX try to connect to another MDS */
			psc_fatalx("unable to establish MDS connection");
	} while (*csvcp == NULL);
	return (0);
}

int
sli_rmi_setmds(const char *name)
{
	struct sl_resource *res;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL)
			psc_fatalx("%s: unknown resource", name);
		rmi_resm = psc_dynarray_getpos(&res->res_members, 0);
	} else
		rmi_resm = libsl_nid2resm(nid);
	return (0);
}

int
sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *w)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	int rc;

	rc = sli_rmi_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMI_VERSION,
	    SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		goto out;
	mq->nid = w->srw_nid;
	mq->fg = w->srw_fg;
	mq->bmapno = w->srw_bmapno;
	mq->bgen = w->srw_bgen;
	mq->rc = w->srw_status;
	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}
