/* $Id$ */

#include "psc_types.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"

#include "mount_slash.h"
#include "slashrpc.h"

struct slashrpc_service *mds_svc;
struct slashrpc_service *io_svc;

/*
 * rpc_connect - attempt connection initiation with a peer.
 * @server: NID of server peer.
 * @ptl: portal ID to initiate over.
 * @magic: agreed-upon connection message key.
 * @version: communication protocol version.
 */
__static int
rpc_connect(lnet_nid_t server, struct pscrpc_import *imp, u64 magic,
    u32 version)
{
	lnet_process_id_t server_id = { server, 0 };
	struct srm_generic_rep *mp;
	struct srm_connect_req *mq;
	struct pscrpc_request *rq;
	lnet_process_id_t id;
	int rc;

	if (LNetGetId(1, &id))
		psc_fatalx("LNetGetId");

	imp->imp_connection = pscrpc_get_connection(server_id, id.nid, NULL);
	imp->imp_connection->c_peer.pid = SLASH_SVR_PID;

	if ((rc = RSX_NEWREQ(mds_import, version, SRMT_CONNECT,
	    rq, mq, mp)) != 0)
		return (rc);
	mq->magic = magic;
	mq->version = version;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			imp->imp_state = PSC_IMP_FULL;
	}
	pscrpc_req_finished(rq);
	return (rc);
}

/*
 * rpc_svc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
__static struct slashrpc_service *
rpc_svc_create(u32 rqptl, u32 rpptl)
{
	struct slashrpc_service *svc;

	svc = PSCALLOC(sizeof(*svc));

	INIT_PSCLIST_HEAD(&svc->svc_old_imports);
	LOCK_INIT(&svc->svc_lock);

	svc->svc_failed = 0;
	svc->svc_initialized = 0;

	if ((svc->svc_import = new_import()) == NULL)
		psc_fatalx("new_import");

	svc->svc_import->imp_client =
	    PSCALLOC(sizeof(*svc->svc_import->imp_client));
	svc->svc_import->imp_client->cli_request_portal = rqptl;
	svc->svc_import->imp_client->cli_reply_portal = rpptl;

	svc->svc_import->imp_max_retries = 2;
	return (svc);
}

/*
 * rpc_svc_init: initialize client RPC services.
 */
void
rpc_svc_init(void)
{
	lnet_nid_t nid;
	char *snid;
	int rc;

	rc = pscrpc_init_portals(PSC_CLIENT);
	if (rc)
		psc_fatal("Failed to intialize portals");

	snid = getenv("SLASH_SERVER_NID");
	if (snid == NULL)
		psc_fatalx("SLASH_SERVER_NID not set");
	nid = libcfs_str2nid(snid);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid SLASH_SERVER_NID: %s", snid);

	/* Setup client MDS service */
	mds_svc = rpc_svc_create(SRCM_REQ_PORTAL, SRCM_REP_PORTAL);
	if (rpc_connect(nid, mds_import, SRCM_MAGIC, SRCM_VERSION))
		psc_error("rpc_connect %s", snid);

#if 0
	/* Setup client I/O service */
	io_svc = rpc_svc_create(SRCI_REQ_PORTAL, SRCI_REP_PORTAL);
	if (rpc_connect(nid, RPCSVC_IO, SRIC_MAGIC, SRCI_VERSION))
		psc_error("rpc_connect %s", snid);
#endif
}
