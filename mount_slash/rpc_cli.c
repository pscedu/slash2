/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/str.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "ctlsvr_cli.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

struct sl_resm			*slc_rmc_resm;

/**
 * slc_rpc_initsvc: Initialize CLI RPC services.
 */
void
slc_rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh;

	/* Setup request service for client from MDS. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRCM_NBUFS;
	svh->svh_bufsz = SRCM_BUFSZ;
	svh->svh_reqsz = SRCM_BUFSZ;
	svh->svh_repsz = SRCM_REPSZ;
	svh->svh_req_portal = SRCM_REQ_PORTAL;
	svh->svh_rep_portal = SRCM_REP_PORTAL;
	svh->svh_type = MSTHRT_RCM;
	svh->svh_nthreads = SRCM_NTHREADS;
	svh->svh_handler = slc_rcm_handler;
	strlcpy(svh->svh_svc_name, SRCM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct msrcm_thread);
}

int
slc_rmc_setmds(const char *name)
{
	struct sl_resource *res;
	struct sl_resm *old;
	lnet_nid_t nid;

	old = slc_rmc_resm;
	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL)
			return (SLERR_RES_UNKNOWN);
		slc_rmc_resm = psc_dynarray_getpos(&res->res_members, 0);
	} else
		slc_rmc_resm = libsl_nid2resm(nid);

	/* XXX kill any old MDS and purge any bmap updates being held */
//	sl_csvc_disable(old->resm_csvc);
#if 0
	slconnthr_spawn(slc_rmc_resm, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,
	    SRMC_MAGIC, SRMC_VERSION,
	    &resm2rmci(slc_rmc_resm)->rmci_mutex, CSVCF_USE_MULTIWAIT,
	    &resm2rmci(slc_rmc_resm)->rmci_mwc,
	    SLCONNT_MDS, MSTHRT_CONN, "ms");
#endif
	return (0);
}

/**
 * slc_rmc_retry_pfcc - Determine if process doesn't want to wait or if
 *	maximum allowed timeout has been reached for MDS communication.
 */
int
slc_rmc_retry_pfcc(__unusedx struct pscfs_clientctx *pfcc, int *rc)
{
	int retry = 1;

#if 0
	switch (*rc) {
	case ECONNABORTED:
	case ECONNREFUSED:
	case ECONNRESET:
	case EHOSTDOWN:
	case EHOSTUNREACH:
	case EIO:
	case ENETDOWN:
	case ENETRESET:
	case ENETUNREACH:
	case ENONET:
	case ENOTCONN:
	case ETIMEDOUT:
		break;
	default:
		return (0);
	}

	retry = global setting
	retry = read_proc_env(ctx->pid, "");
	retry = hard timeout
#endif
	*rc = retry ? 0 : ENOTCONN;
	return (retry);
}

int
slc_rmc_getimp(struct pscfs_clientctx *pfcc, struct sl_resm *resm,
    struct slashrpc_cservice **csvcp)
{
	int rc = 0;

	do {
		*csvcp = slc_getmcsvc(resm);
		if (*csvcp == NULL) {
			rc = resm->resm_csvc->csvc_lasterrno;
			if (slc_rmc_retry_pfcc(pfcc, &rc))
				continue;
		}
	} while (*csvcp == NULL && rc == 0);
	return (rc);
}

int
slc_rmc_getimp1(struct slashrpc_cservice **csvcp, struct sl_resm *resm)
{
	int rc = 0;

	*csvcp = slc_getmcsvc(resm);
	if (*csvcp == NULL)
		rc = resm->resm_csvc->csvc_lasterrno;
	return (rc);
}

void
sl_resm_hldrop(struct sl_resm *resm)
{
	struct msctl_replstq *mrsq;

	if (resm->resm_csvc->csvc_ctype == SLREST_MDS) {
		PLL_LOCK(&msctl_replsts);
		PLL_FOREACH(mrsq, &msctl_replsts)
			mrsq_release(mrsq, ECONNRESET);
		PLL_ULOCK(&msctl_replsts);
	}
}

struct sl_expcli_ops sl_expcli_ops;
