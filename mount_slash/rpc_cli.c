/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/list.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"

#include "ctlsvr_cli.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

struct sl_resm			*slc_rmc_resm;
struct pscrpc_svc_handle	*msl_rci_svh;
struct pscrpc_svc_handle	*msl_rcm_svh; 

__static void
slc_rci_init(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	psc_multiwait_init(&msrcithr(thr)->mrci_mw, "%s",
	    thr->pscthr_name);
}

__static void
slc_rcm_init(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	psc_multiwait_init(&msrcmthr(thr)->mrcm_mw, "%s",
	    thr->pscthr_name);
}

/**
 * slc_rpc_initsvc: Initialize CLI RPC services.
 */
void
slc_rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh; 

	/* Setup request service for CLI from MDS. */
	msl_rcm_svh = svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRCM_NBUFS;
	svh->svh_bufsz = SRCM_BUFSZ;
	svh->svh_reqsz = SRCM_BUFSZ;
	svh->svh_repsz = SRCM_REPSZ;
	svh->svh_req_portal = SRCM_REQ_PORTAL;
	svh->svh_rep_portal = SRCM_REP_PORTAL;
	svh->svh_type = MSTHRT_RCM;
	svh->svh_nthreads = SRCM_NTHREADS;
	svh->svh_handler = slc_rcm_handler;
	svh->svh_initf = slc_rcm_init;
	strlcpy(svh->svh_svc_name, SRCM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct msrcm_thread);

	/* Setup request service for CLI from ION. */
	msl_rci_svh = svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRCI_NBUFS;
	svh->svh_bufsz = SRCI_BUFSZ;
	svh->svh_reqsz = SRCI_BUFSZ;
	svh->svh_repsz = SRCI_REPSZ;
	svh->svh_req_portal = SRCI_REQ_PORTAL;
	svh->svh_rep_portal = SRCI_REP_PORTAL;
	svh->svh_type = MSTHRT_RCI;
	svh->svh_nthreads = SRCI_NTHREADS;
	svh->svh_handler = slc_rci_handler;
	svh->svh_initf = slc_rci_init;
	strlcpy(svh->svh_svc_name, SRCI_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct msrci_thread);
}

int
slc_rmc_setmds(const char *name)
{
	struct sl_resource *res;
//	struct sl_resm *old;
	lnet_nid_t nid;

//	old = slc_rmc_resm;
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
	slconnthr_watch(slc_rmc_resm, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,
	slconnthr_spawn(slc_rmc_resm, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,
	    SRMC_MAGIC, SRMC_VERSION, 0,
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
slc_rmc_retry_pfcc(const struct pscfs_clientctx *pfcc, int *rc)
{
	int retry = 1;

	switch (abs(*rc)) {
	case ECONNABORTED:
	case ECONNREFUSED:
	case ECONNRESET:
	case EHOSTDOWN:
	case EHOSTUNREACH:
	case EIO:
	case ENETDOWN:
	case ENETRESET:
	case ENETUNREACH:
#ifdef ENONET
	case ENONET:
#endif
	case ENOTCONN:
	case ETIMEDOUT:
		break;
	default:
		return (0);
	}

//	retry = global setting
	if (pfcc)
//		retry = read_proc_env(ctx->pid, "");
		;
	else
		retry = 0;
//	retry = hard timeout
	*rc = retry ? 0 : ETIMEDOUT;
	return (retry);
}

int
slc_rmc_getcsvc(const struct pscfs_clientctx *pfcc,
    struct sl_resm *resm, struct slrpc_cservice **csvcp)
{
	int rc;

	*csvcp = slc_getmcsvc(resm);
	if (*csvcp)
		return (0);

	for (;;) {
		rc = 0;
		CSVC_LOCK(resm->resm_csvc);
		*csvcp = slc_getmcsvc(resm);
		if (*csvcp)
			break;

		rc = resm->resm_csvc->csvc_lasterrno;
		if (!slc_rmc_retry_pfcc(pfcc, &rc))
			break;
		sl_csvc_waitrel_s(resm->resm_csvc, CSVC_RECONNECT_INTV);
	}
	CSVC_ULOCK(resm->resm_csvc);
	return (rc);
}

int
slc_rmc_getcsvc1(struct slrpc_cservice **csvcp, struct sl_resm *resm)
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

	if (resm->resm_type == SLREST_MDS) {
		PLL_LOCK(&msctl_replsts);
		PLL_FOREACH(mrsq, &msctl_replsts) {
			spinlock(&mrsq->mrsq_lock);
			mrsq->mrsq_refcnt++;
			mrsq_release(mrsq, ECONNRESET);
		}
		PLL_ULOCK(&msctl_replsts);
	} else if (resm->resm_type == SLREST_ARCHIVAL_FS) {
		struct psc_listcache *lc;
		struct slc_async_req *car;

		lc = &resm2rmci(resm)->rmci_async_reqs;
		while ((car = lc_getnb(lc)) != NULL) {
			car->car_cbf(NULL, ECONNRESET, &car->car_argv);

			psclog_diag("return car=%p car_id=%"PRIx64" q=%p",
			    car, car->car_id, car->car_fsrqinfo);

			psc_pool_return(slc_async_req_pool, car);
		}
	}
}

struct sl_expcli_ops sl_expcli_ops;
struct slrpc_ops slrpc_ops;
