/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/multiwait.h"

#include "slashrpc.h"

/*
 * slashrpc_issue_connect - attempt connection initiation with a peer.
 * @server: NID of server peer.
 * @ptl: portal ID to initiate over.
 * @magic: agreed-upon connection message key.
 * @version: communication protocol version.
 */
__static int
slashrpc_issue_connect(lnet_nid_t server, struct pscrpc_import *imp,
    uint64_t magic, uint32_t version)
{
	lnet_process_id_t server_id = { server, 0 };
	struct pscrpc_request *rq;
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	lnet_nid_t nid;
	int rc;

	nid = pscrpc_getnidforpeer(&lnet_nids, server);
	if (nid == LNET_NID_ANY)
		return (ENETUNREACH);

	if (imp->imp_connection)
		pscrpc_put_connection(imp->imp_connection);
	imp->imp_connection = pscrpc_get_connection(server_id, nid, NULL);
	imp->imp_connection->c_imp = imp;
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
sl_csvc_free(struct slashrpc_cservice *csvc)
{
//	psc_assert(psc_atomic32_read(&csvc->csvc_refcnt) == 0);
	pscrpc_import_put(csvc->csvc_import);
	free(csvc);
}

__weak void
psc_multiwaitcond_wakeup(__unusedx struct psc_multiwaitcond *arg)
{
	psc_fatalx("unimplemented stub");
}

void
sl_csvc_decref(struct slashrpc_cservice *csvc)
{
	CSVC_RLOCK(csvc);
	psc_atomic32_dec(&csvc->csvc_refcnt);
	if (csvc->csvc_flags & CSVCF_USE_MULTIWAIT)
		psc_multiwaitcond_wakeup(csvc->csvc_waitinfo);
	else
		psc_waitq_wakeall(csvc->csvc_waitinfo);
	CSVC_ULOCK(csvc);
}

void
sl_csvc_incref(struct slashrpc_cservice *csvc)
{
	CSVC_LOCK_ENSURE(csvc);
	psc_atomic32_inc(&csvc->csvc_refcnt);
}

/*
 * sl_csvc_create - create a client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
__static struct slashrpc_cservice *
sl_csvc_create(uint32_t rqptl, uint32_t rpptl)
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
sl_csvc_get(struct slashrpc_cservice **csvcp, int flags,
    struct pscrpc_export *exp, lnet_nid_t peernid, uint32_t rqptl,
    uint32_t rpptl, uint64_t magic, uint32_t version,
    psc_spinlock_t *lockp, void *waitinfo,
    enum slconn_type ctype)
{
	struct slashrpc_cservice *csvc = NULL;
	struct sl_resm *resm;
	int rc = 0, locked;

	locked = reqlock(lockp);
	if (exp)
		peernid = exp->exp_connection->c_peer.nid;
	psc_assert(peernid != LNET_NID_ANY);

	csvc = *csvcp;
	if (csvc == NULL) {
		/* ensure peer is of the given type */
		switch (ctype) {
		case SLCONNT_CLI:
			break;
		case SLCONNT_IOD:
			resm = libsl_nid2resm(peernid);
			if (resm->resm_res->res_type == SLREST_MDS)
				goto out;
			break;
		case SLCONNT_MDS:
			resm = libsl_nid2resm(peernid);
			if (resm->resm_res->res_type != SLREST_MDS)
				goto out;
			break;
		default:
			psc_fatalx("%d: bad connection type", ctype);
		}

		/* initialize service */
		csvc = *csvcp = sl_csvc_create(rqptl, rpptl);
		csvc->csvc_flags = flags;
		csvc->csvc_lockp = lockp;
		csvc->csvc_waitinfo = waitinfo;
		csvc->csvc_import->imp_failed = 1;
	}

 restart:
	if (csvc->csvc_import->imp_failed == 0 &&
	    csvc->csvc_import->imp_invalid == 0)
		goto out;
//	|| imp->imp_generation != csvc_impgen

	if (exp) {
		struct pscrpc_connection *c;

		c = csvc->csvc_import->imp_connection;
		atomic_inc(&exp->exp_connection->c_refcount);
		csvc->csvc_import->imp_connection = exp->exp_connection;
		csvc->csvc_import->imp_connection->c_imp = csvc->csvc_import;

		/*
		 * If an export was specified, the peer has already
		 * established a connection to our service, so just
		 * reuse the underhood connection to establish a
		 * connection back to his service.
		 */
		if (c)
			pscrpc_put_connection(c);
	} else if (csvc->csvc_flags & CSVCF_CONNECTING) {
		if (csvc->csvc_flags & CSVCF_USE_MULTIWAIT) {
			psc_fatalx("multiwaits not implemented");
//			psc_multiwait_addcond(ml, wakearg);
//			csvc = NULL;
//			goto out;
		} else {
			psc_waitq_wait(csvc->csvc_waitinfo, csvc->csvc_lockp);
			CSVC_RLOCK(csvc);
		}
		goto restart;
	} else if (csvc->csvc_mtime + CSVC_RECONNECT_INTV < time(NULL)) {
		csvc->csvc_flags |= CSVCF_CONNECTING;
		CSVC_ULOCK(csvc);

		rc = slashrpc_issue_connect(peernid,
		    csvc->csvc_import, magic, version);

		CSVC_RLOCK(csvc);
		csvc->csvc_flags &= ~CSVCF_CONNECTING;
		csvc->csvc_mtime = time(NULL);
		if (rc) {
			csvc->csvc_import->imp_failed = 1;
			csvc->csvc_lasterrno = rc;
			csvc = NULL;
			goto out;
		}
	} else {
		rc = csvc->csvc_lasterrno;
		csvc = NULL;
		goto out;
	}
	if (rc == 0) {
		csvc->csvc_import->imp_failed = 0;
		csvc->csvc_import->imp_invalid = 0;
		if (csvc->csvc_flags & CSVCF_USE_MULTIWAIT)
			psc_multiwaitcond_wakeup(csvc->csvc_waitinfo);
		else
			psc_waitq_wakeall(csvc->csvc_waitinfo);
	}

 out:
	if (csvc)
		sl_csvc_incref(csvc);
	ureqlock(lockp, locked);
//	errno = rc;
	return (csvc);
}

/*
 * slexp_get - access private data associated with an LNET peer.
 * @exp: RPC export of peer.
 * @peertype: peer type of connection.
 */
struct slashrpc_export *
slexp_get(struct pscrpc_export *exp, enum slconn_type peertype)
{
	struct slashrpc_export *slexp;
	int locked;

	locked = reqlock(&exp->exp_lock);
	if (exp->exp_private == NULL) {
		slexp = exp->exp_private = PSCALLOC(sizeof(*slexp));
		slexp->slexp_export = exp;
		slexp->slexp_peertype = peertype;
		exp->exp_hldropf = slexp_destroy;
	} else {
		slexp = exp->exp_private;
		psc_assert(slexp->slexp_export == exp);
		psc_assert(slexp->slexp_peertype == peertype);
	}
	ureqlock(&exp->exp_lock, locked);
	return (slexp);
}

void
slexp_destroy(void *data)
{
	struct slashrpc_export *slexp = data;
	struct pscrpc_export *exp = slexp->slexp_export;

	psc_assert(exp);
	/* There's no way to set this from the drop_callback() */
	if (!(slexp->slexp_flags & SLEXPF_CLOSING))
		slexp->slexp_flags |= SLEXPF_CLOSING;

	if (slexp_freef[slexp->slexp_peertype])
		slexp_freef[slexp->slexp_peertype](exp);

	/* OK, no one else should be in here */
	exp->exp_private = NULL;
	PSCFREE(slexp);
}
