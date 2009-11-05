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
	svh->svh_nbufs = SRIC_NBUFS;
	svh->svh_bufsz = SRIC_BUFSZ;
	svh->svh_reqsz = SRIC_BUFSZ;
	svh->svh_repsz = SRIC_REPSZ;
	svh->svh_req_portal = SRIC_REQ_PORTAL;
	svh->svh_rep_portal = SRIC_REP_PORTAL;
	svh->svh_type = SLITHRT_RIC;
	svh->svh_nthreads = SRIC_NTHREADS;
	svh->svh_handler = slric_handler;
	strlcpy(svh->svh_svc_name, SRIC_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct sliric_thread);

	/* Create server service to handle requests from the MDS server. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRIM_NBUFS;
	svh->svh_bufsz = SRIM_BUFSZ;
	svh->svh_reqsz = SRIM_BUFSZ;
	svh->svh_repsz = SRIM_REPSZ;
	svh->svh_req_portal = SRIM_REQ_PORTAL;
	svh->svh_rep_portal = SRIM_REP_PORTAL;
	svh->svh_type = SLITHRT_RIM;
	svh->svh_nthreads = SRIM_NTHREADS;
	svh->svh_handler = sli_rim_handler;
	strlcpy(svh->svh_svc_name, SRIM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirim_thread);

	/* Create server service to handle requests from other I/O servers. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRII_NBUFS;
	svh->svh_bufsz = SRII_BUFSZ;
	svh->svh_reqsz = SRII_BUFSZ;
	svh->svh_repsz = SRII_REPSZ;
	svh->svh_req_portal = SRII_REQ_PORTAL;
	svh->svh_rep_portal = SRII_REP_PORTAL;
	svh->svh_type = SLITHRT_RII;
	svh->svh_nthreads = SRII_NTHREADS;
	svh->svh_handler = sli_rii_handler;
	strlcpy(svh->svh_svc_name, SRII_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirii_thread);

	/* Create client service to issue requests to MDS. */
	rmi_csvc = rpc_csvc_create(SRMI_REQ_PORTAL, SRMI_REP_PORTAL);
}
