/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/list.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"

#include "ctl_cli.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

/*
 * This is the default MDS. Do not use it directly in order
 * to support global mount.
 */
struct sl_resm			*msl_rmc_resm;
struct pscrpc_svc_handle	*msl_rci_svh;
struct pscrpc_svc_handle	*msl_rcm_svh;

void
msl_resm_throttle_wake(struct sl_resm *m)
{
	struct resprof_cli_info *rpci;

	rpci = res2rpci(m->resm_res);
	RPCI_LOCK(rpci);
	rpci->rpci_infl_rpcs--;
	RPCI_WAKE(rpci);
	RPCI_ULOCK(rpci);
}

int
msl_resm_throttle_yield(struct sl_resm *m)
{
	int max, rc = 0;
	struct resprof_cli_info *rpci;

	if (m->resm_type == SLREST_MDS) {
		max = msl_mds_max_inflight_rpcs;
	} else {
		max = msl_ios_max_inflight_rpcs;
	}

	rpci = res2rpci(m->resm_res);
        RPCI_LOCK(rpci);
        if (rpci->rpci_infl_rpcs >= max)
                rc = -EAGAIN;
	RPCI_ULOCK(rpci);
	return rc;
}

void
msl_resm_throttle_wait(struct sl_resm *m)
{
	struct timespec ts0, ts1, tsd;
	struct resprof_cli_info *rpci;
	int account = 0, max;

	if (m->resm_type == SLREST_MDS) {
		max = msl_mds_max_inflight_rpcs;
	} else {
		max = msl_ios_max_inflight_rpcs;
	}

	rpci = res2rpci(m->resm_res);
	/*
	 * XXX use resm multiwait?
	 */
	RPCI_LOCK(rpci);
	while (rpci->rpci_infl_rpcs >= max) {
		if (!account) {
			PFL_GETTIMESPEC(&ts0);
			account = 1;
		}
		RPCI_WAIT(rpci);
		RPCI_LOCK(rpci);
	}
	rpci->rpci_infl_rpcs++;
	if (rpci->rpci_infl_rpcs > rpci->rpci_max_infl_rpcs)
		rpci->rpci_max_infl_rpcs = rpci->rpci_infl_rpcs;
	RPCI_ULOCK(rpci);
	if (account) {
		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("msl.throttle-wait-usecs",
		    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);
	}
}

__static void
slc_rci_init(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	pfl_multiwait_init(&msrcithr(thr)->mrci_mw, "%s",
	    thr->pscthr_name);
}

__static void
slc_rcm_init(void)
{
	struct psc_thread *thr;

	thr = pscthr_get();

	pfl_multiwait_init(&msrcmthr(thr)->mrcm_mw, "%s",
	    thr->pscthr_name);
}

/*
 * Initialize CLI RPC services.
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

/*
 * This function is called once at mount time.  If the MDS changes, we
 * have to remount.
 */
int
slc_rmc_setmds(const char *name)
{
	struct sl_resource *res;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL)
			return (SLERR_RES_UNKNOWN);
		msl_rmc_resm = psc_dynarray_getpos(&res->res_members, 0);
	} else
		msl_rmc_resm = libsl_nid2resm(nid);

	return (0);
}

/*
 * Determine if process doesn't want to wait or if maximum allowed
 * timeout has been reached for MDS communication.
 */
int
slc_rmc_retry(struct pscfs_req *pfr, int *rc)
{
	int retry;

	switch (abs(*rc)) {
	case PFLERR_TIMEDOUT:

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

	/*
	 * Translate error codes from the SLASH2 level to the OS level.
	 */
	case PFLERR_NOTSUP:
		*rc = ENOTSUP;
		/* FALLTHROUGH */
	default:
		return (0);
	}

	retry = 1;

	/*
	 * We only need to set returned rc if we are not
	 * going to retry.
	 */
	if (pfr) {
		if (pfr->pfr_interrupted) {
			retry = 0;
			*rc = EINTR;
		}
	} else {
		retry = 0;
		*rc = ETIMEDOUT;
	}

	if (retry) {
		usleep(10);
		if (pfr && pfr->pfr_interrupted) {
			retry = 0;
			*rc = EINTR;
		}
	}
	return (retry);
}

int
slc_rmc_getcsvc(struct pscfs_req *pfr,
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
		if (!slc_rmc_retry(pfr, &rc))
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

			psc_pool_return(msl_async_req_pool, car);
		}
	}
}

void
slc_rpc_req_out(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct sl_resm *m;

	m = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	msl_resm_throttle_wait(m);
}

void
slc_rpc_req_out_failed(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct sl_resm *m;

	m = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	msl_resm_throttle_wake(m);
}

void
slc_rpc_rep_in(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct sl_resm *m;

	m = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	msl_resm_throttle_wake(m);
}

struct sl_expcli_ops sl_expcli_ops;
struct slrpc_ops slrpc_ops = {
	.slrpc_req_out = slc_rpc_req_out,
	.slrpc_req_out_failed = slc_rpc_req_out_failed,
	.slrpc_rep_in = slc_rpc_rep_in,
};
