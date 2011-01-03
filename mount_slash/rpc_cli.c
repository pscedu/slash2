/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/str.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"

/**
 * slc_rpc_initsvc: Initialize CLI RPC services.
 */
void
slc_rpc_initsvc(void)
{
	pscrpc_svc_handle_t *svh;

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

	slconnthr_spawn(slc_rmc_resm, SRMC_REQ_PORTAL, SRMC_REP_PORTAL,
	    SRMC_MAGIC, SRMC_VERSION,
	    &resm2rmci(slc_rmc_resm)->rmci_lock, 0,
	    &resm2rmci(slc_rmc_resm)->rmci_waitq,
	    SLCONNT_MDS, MSTHRT_CONN, "ms");

	return (0);
}

int
slc_rmc_getimp(struct slashrpc_cservice **csvcp)
{
	int wait = 1;

	do {
		*csvcp = slc_getmcsvc(slc_rmc_resm);
#if 0
		ctx = fuse_get_context(rq);
		// if process doesn't want to wait
			wait = 0
#endif
	} while (*csvcp == NULL && wait);
	return (0);
}

void
sl_resm_hldrop(__unusedx struct sl_resm *resm)
{
}

struct sl_expcli_ops sl_expcli_ops;
