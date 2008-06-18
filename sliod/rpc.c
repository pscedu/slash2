/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/cdefs.h"
#include "psc_util/strlcpy.h"

#include "rpc.h"
#include "sliod.h"
#include "slashrpc.h"

struct slashrpc_cservice *rim_csvc;

struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	spinlock(&exp->exp_lock);
	if (exp->exp_private == NULL) {
		exp->exp_private = PSCALLOC(sizeof(struct slashrpc_export));
		exp->exp_destroycb = slashrpc_export_destroy;
	}
	freelock(&exp->exp_lock);
	return (exp->exp_private);
}

void
slashrpc_export_destroy(__unusedx void *data)
{
}

/**
 * rpcsvc_init - create and initialize RPC services.
 */
void
rpcsvc_init(void)
{
	pscrpc_svc_handle_t *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Create client service to issue requests to the MDS server. */
	rim_csvc = rpc_csvc_create(SRMI_REQ_PORTAL, SRMI_REP_PORTAL);

	/* Create server service to handle requests from clients. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRIC_NBUFS;
	svh->svh_bufsz = SRIC_BUFSZ;
	svh->svh_reqsz = SRIC_BUFSZ;
	svh->svh_repsz = SRIC_REPSZ;
	svh->svh_req_portal = SRCI_REQ_PORTAL;
	svh->svh_rep_portal = SRCI_REP_PORTAL;
	svh->svh_type = SLIOTHRT_RIC;
	svh->svh_nthreads = SRIC_NTHREADS;
	svh->svh_handler = slric_handler;
	strlcpy(svh->svh_svc_name, SRIC_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_ricthr);

	/* Create server service to handle requests from the MDS server. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRIM_NBUFS;
	svh->svh_bufsz = SRIM_BUFSZ;
	svh->svh_reqsz = SRIM_BUFSZ;
	svh->svh_repsz = SRIM_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type = SLIOTHRT_RIM;
	svh->svh_nthreads = SRIM_NTHREADS;
	svh->svh_handler = slrim_handler;
	strlcpy(svh->svh_svc_name, SRIM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_rimthr);

	/* Create server service to handle requests from other I/O servers. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRII_NBUFS;
	svh->svh_bufsz = SRII_BUFSZ;
	svh->svh_reqsz = SRII_BUFSZ;
	svh->svh_repsz = SRII_REPSZ;
	svh->svh_req_portal = SRII_REQ_PORTAL;
	svh->svh_rep_portal = SRII_REP_PORTAL;
	svh->svh_type = SLIOTHRT_RII;
	svh->svh_nthreads = SRII_NTHREADS;
	svh->svh_handler = slrii_handler;
	strlcpy(svh->svh_svc_name, SRII_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_riithr);
}
