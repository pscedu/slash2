/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"

#include "rpc.h"
#include "slashrpc.h"

struct slashrpc_service *ric_svc;
struct slashrpc_service *rim_svc;
struct slashrpc_service *rii_svc;

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

int
rpc_connect(lnet_nid_t server, struct slashrpc_service *svc, u64 magic,
    u32 version)
{
	lnet_process_id_t server_id = { server, 0 };
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct pscrpc_import *imp;
	lnet_process_id_t id;
	int rc;

	if (LNetGetId(1, &id))
		psc_fatalx("LNetGetId");

	imp = svc->svc_import;
	imp->imp_connection = pscrpc_get_connection(server_id, id.nid, NULL);
	imp->imp_connection->c_peer.pid = SLASH_SVR_PID;

	rc = rsx_newreq(imp, version, SRMT_CONNECT, sizeof(*mq), 0, &rq, &mq);
	if (rc)
		return (rc);
	mq->magic = magic;
	mq->version = version;

	rc = rsx_waitrep(rq, 0, &mp);
	pscrpc_req_finished(rq);
	if (rc == 0)
		rc = mp->rc;
	if (rc == 0)
		imp->imp_state = PSC_IMP_FULL;
	return (rc);
}

/*
 * rpc_svc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
struct slashrpc_service *
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
 * rpc_svc_init: initialize RPC services.
 */
void
rpc_svc_init(void)
{
	ric_svc = rpc_svc_create(SRCI_REQ_PORTAL, SRCI_REP_PORTAL);
	rim_svc = rpc_svc_create(SRMI_REQ_PORTAL, SRMI_REP_PORTAL);
	rii_svc = rpc_svc_create(SRII_REQ_PORTAL, SRII_REP_PORTAL);
}

void
rpc_svc_connect(void)
{
	lnet_nid_t nid;
	char *snid;

	snid = getenv("SLASH_SERVER_NID");
	if (snid == NULL)
		psc_fatalx("SLASH_SERVER_NID not set");
	nid = libcfs_str2nid(snid);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid SLASH_SERVER_NID: %s", snid);

	if (rpc_connect(nid, rim_svc, SRMI_MAGIC, SRMI_VERSION))
		psc_error("rpc_connect %s", snid);
}
