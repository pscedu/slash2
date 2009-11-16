/* $Id$ */

#include <stdio.h>

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
rpc_issue_connect(lnet_nid_t server, struct pscrpc_import *imp, uint64_t magic,
    uint32_t version)
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

void
slashrpc_csvc_free(struct slashrpc_cservice *csvc)
{
	pscrpc_import_put(csvc->csvc_import);
	free(csvc);
}

/*
 * rpc_csvc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
struct slashrpc_cservice *
rpc_csvc_create(uint32_t rqptl, uint32_t rpptl)
{
	struct slashrpc_cservice *csvc;
	struct pscrpc_import *imp;

	csvc = PSCALLOC(sizeof(*csvc));

	if ((imp = pscrpc_new_import()) == NULL)
		psc_fatalx("pscrpc_new_import");
	csvc->csvc_import = imp;

	imp->imp_client->cli_request_portal = rqptl;
	imp->imp_client->cli_reply_portal = rpptl;
	imp->imp_max_retries = 2;
	return (csvc);
}

struct slashrpc_cservice *
rpc_csvc_fromexp(struct pscrpc_export *exp, uint32_t rqptl, uint32_t rpptl)
{
	struct slashrpc_cservice *csvc;

	csvc = rpc_csvc_create(rqptl, rpptl);
	atomic_inc(&exp->exp_connection->c_refcount);
	csvc->csvc_import->imp_connection = exp->exp_connection;
	return (csvc);
}

struct slashrpc_cservice *
slconn_get(struct slashrpc_cservice **csvcp, struct pscrpc_export *exp,
    lnet_nid_t peernid, uint32_t rqptl, uint32_t rpptl, uint64_t magic,
    uint32_t version, psc_spinlock_t *lk, void (*wakef)(void *),
    void *wakearg, enum slconn_type ctype)
{
	struct slashrpc_cservice *csvc = NULL;
	struct sl_resm *resm;
	int rc = 0, locked;

	if (lk)
		locked = reqlock(lk);

	if (exp)
		peernid = exp->exp_connection->c_peer.nid;
	psc_assert(peernid != LNET_NID_ANY);

	if (*csvcp == NULL) {
		/* ensure peer is of the given type */
		switch (ctype) {
		case SLCONNT_CLI:
			break;
		case SLCONNT_IOD:
			resm = libsl_nid2resm(peernid);
			if (resm == NULL || resm->resm_res->res_mds)
				goto out;
			break;
		case SLCONNT_MDS:
			resm = libsl_nid2resm(peernid);
			if (resm == NULL || !resm->resm_res->res_mds)
				goto out;
			break;
		}

		/* initialize service */
		*csvcp = rpc_csvc_create(rqptl, rpptl);
	}
	csvc = *csvcp;
	if ((csvc->csvc_flags & CSVCF_INIT) == 0 ||
	    ((csvc->csvc_flags & CSVCF_FAILED) &&
	    csvc->csvc_mtime + 30 < time(NULL))) {
		if (exp) {
			atomic_inc(&exp->exp_connection->c_refcount);
			csvc->csvc_import->imp_connection = exp->exp_connection;
		} else {
			rc = rpc_issue_connect(peernid, csvc->csvc_import,
			    magic, version);
			csvc->csvc_mtime = time(NULL);
			if (rc)
				csvc->csvc_flags |= CSVCF_FAILED;
		}
		if (rc == 0) {
			if (wakef && wakearg)
				wakef(wakearg);
			csvc->csvc_flags |= CSVCF_INIT;
			csvc->csvc_flags &= ~CSVCF_FAILED;
		}
	}
	if (csvc->csvc_flags & CSVCF_FAILED)
		csvc = NULL;

 out:
	if (lk)
		ureqlock(lk, locked);
	return (csvc);
}
