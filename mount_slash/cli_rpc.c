/* $Id$ */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/cdefs.h"
#include "psc_util/strlcpy.h"

#include "mount_slash.h"
#include "slashrpc.h"

struct slashrpc_cservice *mds_csvc;
struct psclist_head io_server_conns = PSCLIST_HEAD_INIT(io_server_conns);

lnet_process_id_t lpid;

/* Slash RPC channel for client from MDS. */
#define SRCM_NTHREADS	8
#define SRCM_NBUFS	512
#define SRCM_BUFSZ	384
#define SRCM_REPSZ	384
#define SRCM_SVCNAME	"msrcmthr"

/*
 * rpc_initsvc: initialize RPC services.
 */
void
rpc_initsvc(void)
{
	pscrpc_svc_handle_t *svh;

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Setup request service for client from MDS. */
	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRCM_NBUFS;
	svh->svh_bufsz = SRCM_BUFSZ;
	svh->svh_reqsz = SRCM_BUFSZ;
	svh->svh_repsz = SRCM_REPSZ;
	svh->svh_req_portal = SRCM_REQ_PORTAL;
	svh->svh_rep_portal = SRCM_REP_PORTAL;
	svh->svh_type = MSTHRT_RCM;
	svh->svh_nthreads = SRCM_NTHREADS;
	svh->svh_handler = msrcm_handler;
	strlcpy(svh->svh_svc_name, SRCM_SVCNAME, sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct msrcm_thread);

	/* Setup client <-> MDS service */
	mds_csvc = rpc_csvc_create(SRMC_REQ_PORTAL, SRMC_REP_PORTAL);
}

int
msrmc_connect(const char *name)
{
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);
	if (rpc_issue_connect(nid, mds_import, SRMC_MAGIC, SRMC_VERSION))
		psc_error("rpc_connect %s", name);
	return (0);
}

int
msric_connect(const char *name)
{
	struct io_server_conn *isc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	isc = PSCALLOC(sizeof(*isc));
	isc->isc_csvc = rpc_csvc_create(SRIC_REQ_PORTAL,
	    SRIC_REP_PORTAL);
	/* XXX use srm_ic_connect_req */
	if (rpc_issue_connect(nid, isc->isc_csvc->csvc_import,
	    SRIC_MAGIC, SRIC_VERSION))
		psc_error("rpc_connect %s", name);
	psclist_xadd(&isc->isc_lentry, &io_server_conns);
	return (0);
}

struct slashrpc_cservice *
ion_get(void)
{
	static psc_spinlock_t lock = LOCK_INITIALIZER;
	static struct io_server_conn *isc;
	struct slashrpc_cservice *csvc;
	struct psclist_head *e;

	spinlock(&lock);
	if (psclist_empty(&io_server_conns))
		psc_fatalx("no I/O nodes available");
	if (isc == NULL)
		isc = psclist_first_entry(&io_server_conns,
		    struct io_server_conn, isc_lentry);
	csvc = isc->isc_csvc;
	e = psclist_next(&isc->isc_lentry);
	if (e == &io_server_conns)
		isc = NULL;
	else
		isc = psclist_entry(e, struct io_server_conn,
		    isc_lentry);
	freelock(&lock);
	return (csvc);
}
