/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "pfl/cdefs.h"
#include "psc_util/strlcpy.h"

#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"

lnet_process_id_t		 lpid;
struct slashrpc_cservice	*rmi_csvc;

/**
 * rpc_initsvc - create and initialize RPC services.
 */
void
rpc_initsvc(void)
{
	pscrpc_svc_handle_t *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Create server service to handle requests from clients. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLI_RIC_NBUFS;
	svh->svh_bufsz = SLI_RIC_BUFSZ;
	svh->svh_reqsz = SLI_RIC_BUFSZ;
	svh->svh_repsz = SLI_RIC_REPSZ;
	svh->svh_req_portal = SRIC_REQ_PORTAL;
	svh->svh_rep_portal = SRIC_REP_PORTAL;
	svh->svh_type = SLITHRT_RIC;
	svh->svh_nthreads = SLI_RIC_NTHREADS;
	svh->svh_handler = sli_ric_handler;
	strlcpy(svh->svh_svc_name, SLI_RIC_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct sliric_thread);

	/* Create server service to handle requests from the MDS server. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLI_RIM_NBUFS;
	svh->svh_bufsz = SLI_RIM_BUFSZ;
	svh->svh_reqsz = SLI_RIM_BUFSZ;
	svh->svh_repsz = SLI_RIM_REPSZ;
	svh->svh_req_portal = SRIM_REQ_PORTAL;
	svh->svh_rep_portal = SRIM_REP_PORTAL;
	svh->svh_type = SLITHRT_RIM;
	svh->svh_nthreads = SLI_RIM_NTHREADS;
	svh->svh_handler = sli_rim_handler;
	strlcpy(svh->svh_svc_name, SLI_RIM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirim_thread);

	/* Create server service to handle requests from other I/O servers. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SLI_RII_NBUFS;
	svh->svh_bufsz = SLI_RII_BUFSZ;
	svh->svh_reqsz = SLI_RII_BUFSZ;
	svh->svh_repsz = SLI_RII_REPSZ;
	svh->svh_req_portal = SRII_REQ_PORTAL;
	svh->svh_rep_portal = SRII_REP_PORTAL;
	svh->svh_type = SLITHRT_RII;
	svh->svh_nthreads = SLI_RII_NTHREADS;
	svh->svh_handler = sli_rii_handler;
	strlcpy(svh->svh_svc_name, SLI_RII_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirii_thread);

	/* Create client service to issue requests to MDS. */
	rmi_csvc = rpc_csvc_create(SRMI_REQ_PORTAL, SRMI_REP_PORTAL);
}
