/* $Id$ */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "pfl/cdefs.h"
#include "psc_util/strlcpy.h"

#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"

struct slashrpc_cservice *mds_csvc;

lnet_process_id_t lpid;

/* Slash RPC channel for client from MDS. */
#define SRCM_NTHREADS	8
#define SRCM_NBUFS	512
#define SRCM_BUFSZ	384
#define SRCM_REPSZ	384
#define SRCM_SVCNAME	"msrcmthr"

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
	svh->svh_handler = msrcm_handler;
	strlcpy(svh->svh_svc_name, SRCM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct msrcm_thread);

	/* Setup MDS <- client service */
	mds_csvc = rpc_csvc_create(SRMC_REQ_PORTAL, SRMC_REP_PORTAL);
}

int
msrmc_connect(const char *name)
{
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);
	if (rpc_issue_connect(nid, mds_import, SRMC_MAGIC, SRMC_VERSION))
		psc_error("rpc_connect %s", name);
	return (0);
}
