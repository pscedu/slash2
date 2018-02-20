/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2015, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Routines for issuing RPC requests to MDS from ION.
 */

#include <sys/types.h>
#include <sys/wait.h>

#include <stdint.h>

#include "pfl/rpc.h"
#include "pfl/rsx.h"

#include "batchrpc.h"
#include "bmap_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"
#include "sliod.h"

/* async RPC pointers */
#define SLI_CBARG_CSVC                  0

struct sl_resm *rmi_resm;

int
sli_rmi_getcsvc(struct slrpc_cservice **csvcp)
{
	for (;;) {
		*csvcp = sli_getmcsvc(rmi_resm, 0);
		if (*csvcp)
			break;

		sl_csvc_waitrel_s(rmi_resm->resm_csvc, CSVC_CONN_INTV);
	}
	return (0);
}

void
sli_rmi_setmds(const char *name)
{
	int i, j;
	struct sl_resm *resm;
	struct sl_site *s;
	struct sl_resource *r;
	struct sl_resource *res;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL)
			psc_fatalx("invalid MDS name %s", name);
		rmi_resm = psc_dynarray_getpos(&res->res_members, 0);
	} else
		rmi_resm = libsl_nid2resm(nid);

	sli_getmcsvc_nb(rmi_resm, 0);
	slconnthr_watch(sliconnthr, rmi_resm->resm_csvc, 
	    CSVCF_PING, NULL, NULL);

	/* Inform our IOS peers, not needed for correctness */
	CONF_LOCK();
	CONF_FOREACH_RESM(s, r, i, resm, j) {
		if (resm == nodeResm || resm == rmi_resm ||
		    (RES_ISCLUSTER(r)))
			continue;
		sli_geticsvc_nb(resm, 0);
	}
	CONF_ULOCK();
}

int
sli_rmi_brelease_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	int rc;
	struct slrpc_cservice *csvc = args->pointer_arg[SLI_CBARG_CSVC];

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_release_rep,
	    rc);

	sl_csvc_decref(csvc);
	return (0);
}

void
sli_rmi_issue_bmap_release(struct srm_bmap_release_req *brr)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct slrpc_cservice *csvc;
	struct pscrpc_request *rq;
	int rc;

	rc = sli_rmi_getcsvc(&csvc);
	if (rc) {
		psclog_errorx("failed to get MDS import; rc=%d", rc);
		return;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc) {
		psclog_errorx("failed to generate new req; rc=%d", rc);
		sl_csvc_decref(csvc);
		return;
	}

	*mq = *brr;

	rq->rq_interpret_reply = sli_rmi_brelease_cb;
	rq->rq_async_args.pointer_arg[SLI_CBARG_CSVC] = csvc;

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		psclog_errorx("RELEASEBMAP failed rc=%d", rc);
		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
}
