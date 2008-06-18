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

lnet_process_id_t lpid;

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

	if (LNetGetId(1, &lpid))
		psc_fatalx("LNetGetId");

	/* Setup client <-> MDS service */
	mds_csvc = rpc_csvc_create(SRCM_REQ_PORTAL, SRCM_REP_PORTAL);
}

int
slrcm_connect(const char *name)
{
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);
	if (rpc_issue_connect(nid, mds_import, SRCM_MAGIC, SRCM_VERSION))
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
	isc->isc_csvc = rpc_csvc_create(SRCI_REQ_PORTAL,
	    SRCI_REP_PORTAL);
	if (rpc_issue_connect(nid, isc->isc_csvc->csvc_import,
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
	struct slashrpc_cservice *csvc;
	struct psclist_head *e;

	spinlock(&lock);
	if (psclist_empty(&io_server_conns))
		psc_fatalx("no I/O nodes available");
	if (isc == NULL)
		isc = psclist_first_entry(&io_server_conns,
		    struct io_server_conn, isc_lentry);
	else {
		csvc = isc->isc_csvc;
		e = psclist_next(&isc->isc_lentry);
		if (e == &io_server_conns)
			isc = NULL;
		else
			isc = psclist_entry(e, struct io_server_conn,
			    isc_lentry);
	}
	freelock(&lock);
	return (csvc);
}
