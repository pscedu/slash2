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

lnet_process_id_t lpid;

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

/*
 * rpc_connect - attempt connection initiation with a peer.
 * @server: NID of server peer.
 * @ptl: portal ID to initiate over.
 * @magic: agreed-upon connection message key.
 * @version: communication protocol version.
 */
int
rpc_issue_connect(lnet_nid_t server, struct pscrpc_import *imp, u64 magic,
    u32 version)
{
	lnet_process_id_t server_id = { server, 0 };
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	imp->imp_connection = pscrpc_get_connection(server_id, lpid.nid, NULL);
	imp->imp_connection->c_peer.pid = SLASH_SVR_PID;

	rc = rsx_newreq(imp, version, SRMT_CONNECT, sizeof(*mq), 0, &rq, &mq);
	if (rc)
		return (rc);
	mq->magic = magic;
	mq->version = version;
	if ((rc = rsx_waitrep(rq, 0, &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			imp->imp_state = PSC_IMP_FULL;
	}
	pscrpc_req_finished(rq);
	return (rc);
}

/*
 * rpc_csvc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
struct slashrpc_cservice *
rpc_csvc_create(u32 rqptl, u32 rpptl)
{
	struct slashrpc_cservice *csvc;
	struct pscrpc_import *imp;

	csvc = PSCALLOC(sizeof(*csvc));

	INIT_PSCLIST_HEAD(&csvc->csvc_old_imports);
	LOCK_INIT(&csvc->csvc_lock);

	csvc->csvc_failed = 0;
	csvc->csvc_initialized = 0;

	if ((imp = new_import()) == NULL)
		psc_fatalx("new_import");
	csvc->csvc_import = imp;

	imp->imp_client = PSCALLOC(sizeof(*imp->imp_client));
	imp->imp_client->cli_request_portal = rqptl;
	imp->imp_client->cli_reply_portal = rpptl;
	imp->imp_max_retries = 2;
	return (csvc);
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
