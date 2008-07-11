/* $Id$ */

/*
 * Routines for issuing RPC requests for MDS from MDS.
 */

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"

#include "slashrpc.h"
#include "rpc.h"

struct psclist_head mds_server_conns = PSCLIST_HEAD_INIT(mds_server_conns);

struct mds_server_conn {
	struct psclist_head		 msc_lentry;
	struct slashrpc_cservice	*msc_csvc;
};

/*
 * slrmm_addconn - initiate a connection to MDS from MDS.
 */
int
slrmm_addconn(const char *name)
{
	struct slashrpc_cservice *csvc;
	struct mds_server_conn *msc;
	lnet_nid_t nid;

	nid = libcfs_str2nid(name);
	if (nid == LNET_NID_ANY)
		psc_fatalx("invalid server name: %s", name);

	csvc = rpc_csvc_create(SRMM_REQ_PORTAL, SRMM_REP_PORTAL);
	if (rpc_issue_connect(nid, csvc->csvc_import,
	    SRMM_MAGIC, SRMM_VERSION))
		return (-1);

	msc = PSCALLOC(sizeof(*msc));
	msc->msc_csvc = csvc;
	psclist_xadd(&msc->msc_lentry, &mds_server_conns);
	return (0);
}
