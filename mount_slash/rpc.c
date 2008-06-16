/* $Id$ */

#include "psc_types.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"

#include "mount_slash.h"
#include "slashrpc.h"

struct slashrpc_cservice *mds_csvc;
struct psclist_head io_server_conns = PSCLIST_HEAD_INIT(io_server_conns);

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

	if ((rc = RSX_NEWREQ(mds_import, version,
	    SRMT_CONNECT, rq, mq, mp)) != 0)
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
__static struct slashrpc_cservice *
rpc_svc_create(u32 rqptl, u32 rpptl)
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

/*
 * rpc_svc_init: initialize client RPC services.
 */
void
rpc_svc_init(void)
{
	int rc;

	rc = pscrpc_init_portals(PSC_CLIENT);
	if (rc)
		psc_fatal("Failed to intialize portals");

	/* Setup client <-> MDS service */
	mds_csvc = rpc_svc_create(SRCM_REQ_PORTAL, SRCM_REP_PORTAL);
}

int
slrcm_connect(const char *name)
{
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	if (rpc_connect(nid, mds_import, SRCM_MAGIC, SRCM_VERSION))
		psc_error("rpc_connect %s", name);
	return (0);
}

int
slrci_connect(const char *name)
{
	struct io_server_conn *isc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	isc = PSCALLOC(sizeof(*isc));

	isc->isc_csvc = rpc_svc_create(SRCI_REQ_PORTAL,
	    SRCI_REP_PORTAL);
	if (rpc_connect(nid, isc->isc_csvc->csvc_import,
	    SRCI_MAGIC, SRCI_VERSION))
		psc_error("rpc_connect %s", name);
	psclist_xadd(&isc->isc_lentry, &io_server_conns);
	return (0);
}

struct slashrpc_cservice *
ion_get(void)
{
	static psc_spinlock_t lock = LOCK_INITIALIZER;
	static struct io_server_conn *isc;
	struct slash_cservice *csvc;

	spinlock(&lock);
	if (psclist_empty(&io_server_conns))
		psc_fatalx("no I/O nodes available");
	if (isc == NULL || isc == &io_server_conns)
		isc = psclist_first(&io_server_conns);
	else {
		csvc = isc->isc_csvc;
		isc = psclist_next_entry(isc, isc_lentry);
	}
	freelock(&lock);
	return (csvc);
}
