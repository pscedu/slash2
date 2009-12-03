/* $Id$ */

#include "pfl/cdefs.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"

lnet_process_id_t	lpid;
void (*slexp_freef[SLNCONNT])(struct pscrpc_export *);

/*
 * slc_rpc_initsvc: initialize RPC services.
 */
void
slc_rpc_initsvc(void)
{
	pscrpc_svc_handle_t *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

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
	struct sl_resm *resm;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY) {
		res = libsl_str2res(name);
		if (res == NULL) {
			psc_fatalx("%s: unknown resource", name);
			return (SLERR_RES_UNKNOWN);
		}
		nid = res->res_nids[0];
	} else {
		resm = libsl_nid2resm(nid);
		res = resm->resm_res;
	}
	slc_rmc_resm = resm;
	return (0);
}

struct pscrpc_import *
slc_rmc_getimp(void)
{
	struct slashrpc_cservice *csvc;

	do {
		csvc = slc_getmconn(slc_rmc_resm);
		if (csvc == NULL)
			/* XXX try to connect to another MDS */
			psc_fatalx("unable to establish MDS connection");
	} while (csvc == NULL);
	return (csvc->csvc_import);
}
