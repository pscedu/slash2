/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "mdsrpc.h"
#include "slashd.h"
#include "slashrpc.h"

lnet_process_id_t lpid;

void
rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Setup request service for MDS from ION. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRMI_NBUFS;
	svh->svh_bufsz = SRMI_BUFSZ;
	svh->svh_reqsz = SRMI_BUFSZ;
	svh->svh_repsz = SRMI_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMI;
	svh->svh_nthreads = SRMI_NTHREADS;
	svh->svh_handler = slrmi_handler;
	strlcpy(svh->svh_svc_name, SRMI_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmi_thread);

	/* Setup request service for MDS from MDS. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRMM_NBUFS;
	svh->svh_bufsz = SRMM_BUFSZ;
	svh->svh_reqsz = SRMM_BUFSZ;
	svh->svh_repsz = SRMM_REPSZ;
	svh->svh_req_portal = SRMM_REQ_PORTAL;
	svh->svh_rep_portal = SRMM_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMM;
	svh->svh_nthreads = SRMM_NTHREADS;
	svh->svh_handler = slrmm_handler;
	strlcpy(svh->svh_svc_name, SRMM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmm_thread);

	/* Setup request service for MDS from client. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRMC_NBUFS;
	svh->svh_bufsz = SRMC_BUFSZ;
	svh->svh_reqsz = SRMC_BUFSZ;
	svh->svh_repsz = SRMC_REPSZ;
	svh->svh_req_portal = SRMC_REQ_PORTAL;
	svh->svh_rep_portal = SRMC_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMC;
	svh->svh_nthreads = SRMC_NTHREADS;
	svh->svh_handler = slrmc_handler;
	strlcpy(svh->svh_svc_name, SRMC_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmc_thread);
}
