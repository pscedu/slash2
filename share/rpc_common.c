/* $Id$ */

#include <stdio.h>

#include "psc_types.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "slashrpc.h"

/*
 * rpc_issue_connect - attempt connection initiation with a peer.
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
	struct pscrpc_request *rq;
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	int rc;

	imp->imp_connection = pscrpc_get_connection(server_id, lpid.nid, NULL);
	imp->imp_connection->c_peer.pid = SLASH_SVR_PID;

	if ((rc = RSX_NEWREQ(imp, version, SRMT_CONNECT, rq, mq, mp)) != 0)
		return (rc);
	mq->magic = magic;
	mq->version = version;
	if ((rc = RSX_WAITREP(rq, mp)) == 0) {
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
