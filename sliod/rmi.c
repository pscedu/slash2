/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
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
sli_rmi_getcsvc(struct slashrpc_cservice **csvcp)
{
	int rc;

	*csvcp = sli_getmcsvc(rmi_resm);
	if (*csvcp)
		return (0);

	for (;;) {
		rc = 0;
		CSVC_LOCK(rmi_resm->resm_csvc);
		*csvcp = sli_getmcsvc(rmi_resm);
		if (*csvcp)
			break;
		sl_csvc_waitrel_s(rmi_resm->resm_csvc,
		    CSVC_RECONNECT_INTV);
	}
	CSVC_ULOCK(rmi_resm->resm_csvc);
	return (rc);
}

int
sli_rmi_setmds(const char *name)
{
	struct slashrpc_cservice *csvc;
	struct sl_resource *res;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL)
			return (SLERR_RES_UNKNOWN);
		rmi_resm = psc_dynarray_getpos(&res->res_members, 0);
	} else
		rmi_resm = libsl_nid2resm(nid);

	if (sli_rmi_getcsvc(&csvc))
		psclog_errorx("error connecting to MDS");
	else {
		slconnthr_watch(sliconnthr, csvc, CSVCF_PING, NULL,
		    NULL);
		sl_csvc_decref(csvc);
	}
	return (0);
}

int
sli_rmi_issue_repl_schedwk(struct sli_repl_workrq *w)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	int rc;

	if (w->srw_op == SLI_REPLWKOP_REPL) {
		w->srw_pp->rc = w->srw_status;
		if (psc_atomic32_inc_getnew(&w->srw_bchrp->ndone) <
		    w->srw_bchrp->total)
			return (0);
	}

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		goto out;

	if (w->srw_op == SLI_REPLWKOP_PTRUNC) {
		struct srm_bmap_ptrunc_req *mq;
		struct srm_bmap_ptrunc_rep *mp;

		rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq,
		    mp);
		if (rc)
			goto out;
		mq->bmapno = w->srw_bmapno;
		mq->bgen = w->srw_bgen;
		mq->rc = w->srw_status;
		mq->fg = w->srw_fg;
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	} else {
		struct srm_batch_req *mq;
		struct srm_batch_rep *mp;
		struct iovec iov;

		rc = SL_RSX_NEWREQ(csvc, SRMT_BATCH_RP, rq, mq, mp);
		if (rc)
			goto out;
		mq->opc = SRMT_REPL_SCHEDWK;
		mq->len = w->srw_bchrp->total * sizeof(*w->srw_pp);
		mq->bid = w->srw_bchrp->id;

		iov.iov_base = w->srw_bchrp->buf;
		iov.iov_len = mq->len;
		rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
		    &iov, 1);
		if (rc == 0)
			rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	}
	if (w->srw_status)
		/* e.g., PFLERR_NOTCONN = 503 */
		psclog_errorx("sent error rc=%d", w->srw_status);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

int
sli_rmi_brelease_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	int rc;
	struct slashrpc_cservice *csvc = args->pointer_arg[SLI_CBARG_CSVC];

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_release_rep,
	    rc);

	sl_csvc_decref(csvc);
	return (0);
}

void
sli_rmi_issue_bmap_release(struct srm_bmap_release_req *brr)
{
	rc = sli_rmi_getcsvc(&csvc);
	if (rc) {
		psclog_errorx("failed to get MDS import rc=%d",
		    rc);
		continue;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc) {
		psclog_errorx("failed to generate new req "
		    "rc=%d", rc);
		sl_csvc_decref(csvc);
		continue;
	}

	memcpy(mq, &brr, sizeof(*mq));

	rq->rq_interpret_reply = sli_rmi_brelease_cb;
	rq->rq_async_args.pointer_arg[SLI_CBARG_CSVC] = csvc;

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		psclog_errorx("RELEASEBMAP failed rc=%d", rc);
		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
}
