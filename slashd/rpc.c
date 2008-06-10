/* $Id$ */

#include <stdio.h>

#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/strlcpy.h"

#include "rpc.h"
#include "slashrpc.h"
#include "slashd.h"

struct slashrpc_cservice *rmm_csvc;

lnet_process_id_t lpid;

struct sexptree sexptree;
psc_spinlock_t sexptreelock = LOCK_INITIALIZER;

SPLAY_GENERATE(sexptree, slashrpc_export, entry, sexpcmp);

/*
 * slashrpc_export_get - access our application-specific variables associated
 *	with an LNET connection.
 * @exp: RPC export of peer.
 */
struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	spinlock(&exp->exp_lock);
	if (exp->exp_private == NULL) {
		exp->exp_private = PSCALLOC(sizeof(struct slashrpc_export));
		exp->exp_destroycb = slashrpc_export_destroy;

		spinlock(&sexptreelock);
		if (SPLAY_INSERT(sexptree, &sexptree, exp->exp_private))
			psc_fatalx("export already registered");
		freelock(&sexptreelock);
	}
	freelock(&exp->exp_lock);
	return (exp->exp_private);
}

void
slashrpc_export_destroy(void *data)
{
	struct slashrpc_export *sexp = data;
	struct cfdent *c, *next;

	for (c = SPLAY_MIN(cfdtree, &sexp->cfdtree); c; c = next) {
		next = SPLAY_NEXT(cfdtree, &sexp->cfdtree, c);
		SPLAY_REMOVE(cfdtree, &sexp->cfdtree, c);
		free(c);
	}
	spinlock(&sexptreelock);
	SPLAY_REMOVE(sexptree, &sexptree, sexp);
	freelock(&sexptreelock);
	free(sexp);
}

int
sexpcmp(const void *a, const void *b)
{
	const struct slashrpc_export *sa = a, *sb = b;
	const lnet_process_id_t *pa = &sa->exp->exp_connection->c_peer;
	const lnet_process_id_t *pb = &sb->exp->exp_connection->c_peer;

	if (pa->nid < pb->nid)
		return (-1);
	else if (pa->nid > pb->nid)
		return (1);

	if (pa->pid < pb->pid)
		return (-1);
	else if (pa->pid > pb->pid)
		return (1);

	return (0);
}

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

#define SRMM_NTHREADS	8
#define SRMM_NBUFS	1024
#define SRMM_BUFSZ	128
#define SRMM_REPSZ	128
#define SRMM_SVCNAME	"slrmmthr"

#define SRMI_NTHREADS	8
#define SRMI_NBUFS	1024
#define SRMI_BUFSZ	128
#define SRMI_REPSZ	128
#define SRMI_SVCNAME	"slrmithr"

#define SRCM_NTHREADS	8
#define SRCM_NBUFS	1024
#define SRCM_BUFSZ	128
#define SRCM_REPSZ	128
#define SRCM_SVCNAME	"slrmcthr"

void
rpcsvc_init(void)
{
	pscrpc_svc_handle_t *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Bring up MDS <-> I/O server service. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRMI_NBUFS;
	svh->svh_bufsz = SRMI_BUFSZ;
	svh->svh_reqsz = SRMI_BUFSZ;
	svh->svh_repsz = SRMI_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type = SLTHRT_RMI;
	svh->svh_nthreads = SRMI_NTHREADS;
	svh->svh_handler = slrmi_handler;
	strlcpy(svh->svh_svc_name, SRMI_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_rmithr);

	/* Bring up MDS <-> MDS service. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRMM_NBUFS;
	svh->svh_bufsz = SRMM_BUFSZ;
	svh->svh_reqsz = SRMM_BUFSZ;
	svh->svh_repsz = SRMM_REPSZ;
	svh->svh_req_portal = SRMM_REQ_PORTAL;
	svh->svh_rep_portal = SRMM_REP_PORTAL;
	svh->svh_type = SLTHRT_RMM;
	svh->svh_nthreads = SRMM_NTHREADS;
	svh->svh_handler = slrmm_handler;
	strlcpy(svh->svh_svc_name, SRMM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_rmmthr);

	/* Bring up MDS <-> client service. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRCM_NBUFS;
	svh->svh_bufsz = SRCM_BUFSZ;
	svh->svh_reqsz = SRCM_BUFSZ;
	svh->svh_repsz = SRCM_REPSZ;
	svh->svh_req_portal = SRCM_REQ_PORTAL;
	svh->svh_rep_portal = SRCM_REP_PORTAL;
	svh->svh_type = SLTHRT_RMC;
	svh->svh_nthreads = SRCM_NTHREADS;
	svh->svh_handler = slrmc_handler;
	strlcpy(svh->svh_svc_name, SRCM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slash_rmcthr);
}
