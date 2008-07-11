/* $Id$ */

/*
 * Routines for issuing RPC requests for ION from ION.
 */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"

#include "slashrpc.h"
#include "rpc.h"

struct psclist_head io_server_conns = PSCLIST_HEAD_INIT(io_server_conns);

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

/*
 * slrii_addconn - initiate a connection to ION from ION.
 */
int
slrii_addconn(const char *name)
{
	struct slashrpc_cservice *csvc;
	struct io_server_conn *isc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	csvc = rpc_csvc_create(SRII_REQ_PORTAL, SRII_REP_PORTAL);
	if (rpc_issue_connect(nid, csvc->csvc_import,
	    SRII_MAGIC, SRII_VERSION))
		return (-1);

	isc = PSCALLOC(sizeof(*isc));
	isc->isc_csvc = csvc;
	psclist_xadd(&isc->isc_lentry, &io_server_conns);
	return (0);
}
