/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2007-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <sys/types.h>
#include <sys/statvfs.h>

#include <gcrypt.h>
#include <stdio.h>

#include "pfl/alloc.h"
#include "pfl/export.h"
#include "pfl/list.h"
#include "pfl/lock.h"
#include "pfl/multiwait.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/time.h"
#include "pfl/str.h"

#include "authbuf.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"

#define CBARG_CSVC	0
#define CBARG_STKVER	1
#define CBARG_UPTIME	2
#define CBARG_OLDIMPORT	3
#define CBARG_NEWIMPORT	4

struct pscrpc_request_set *sl_nbrqset;

struct psc_poolmaster	 sl_csvc_poolmaster;
struct psc_poolmgr	*sl_csvc_pool;

struct psc_lockedlist	 sl_clients = PLL_INIT(&sl_clients,
    struct slrpc_cservice, csvc_lentry);

struct pfl_rwlock	 sl_conn_lock;
psc_spinlock_t		 sl_watch_lock = SPINLOCK_INIT;

void	sl_imp_hldrop_cli(void *);
void	sl_imp_hldrop_resm(void *);

/*
 * Create a new generic RPC request.  Common processing in all SLASH2
 * communication happens herein.
 */
int
slrpc_newgenreq(struct slrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	int qlens[] = { qlen, sizeof(struct srt_authbuf_footer) };
	int plens[] = { plen, sizeof(struct srt_authbuf_footer) };

	return (RSX_NEWREQN(csvc->csvc_import, csvc->csvc_version, op,
	    *rqp, nitems(qlens), qlens, nitems(plens), plens,
	    *(void **)mqp));
}

/*
 * Wait for a reply on a synchronous RPC issued as an RPC client.
 * Common processing to all SLASH2 communication happens herein.
 */
int
slrpc_waitrep(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, int plen, void *mpp)
{
	int rc;

	if (slrpc_ops.slrpc_req_out)
		slrpc_ops.slrpc_req_out(csvc, rq);
	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	rc = pfl_rsx_waitrep(rq, plen, mpp);
	return (rc);
}

int
slrpc_allocrepn(struct pscrpc_request *rq, void *mq0p, int q0len,
    void *mp0p, int np, const int *plens, int rcoff)
{
	int *rcp;

	RSX_ALLOCREPNRC(rq, *(void **)mq0p, q0len, *(void **)mp0p, np,
	    plens, rcoff);
	rcp = PSC_AGP(*(void **)mp0p, rcoff);
	*rcp = authbuf_check(rq, PSCRPC_MSG_REQUEST, 0);
	if (*rcp == 0 && slrpc_ops.slrpc_req_in)
		slrpc_ops.slrpc_req_in(rq);
	return (*rcp);
}

int
slrpc_allocgenrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	int plens[] = { plen, sizeof(struct srt_authbuf_footer) };

	return (slrpc_allocrepn(rq, mqp, qlen, mpp, nitems(plens),
	    plens, rcoff));
}

/*
 * Execute any hooks in the server before sending a reply back to an RPC
 * client.
 */
void
slrpc_rep_out(struct pscrpc_request *rq)
{
	if (slrpc_ops.slrpc_rep_out)
		slrpc_ops.slrpc_rep_out(rq);
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
}

/*
 * Generic RPC reply received processing, as well as execution of any
 * callbacks registered.
 */
int
slrpc_rep_in(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, int flags, int error)
{
	/*
	 * XXX this horrible hack.  if one side thinks there's
	 * no connection, how can the other side not?
	 */
	if (error == -PFLERR_NOTCONN && csvc)
		sl_csvc_disconnect(csvc);

	if (!error)
		error = authbuf_check(rq, PSCRPC_MSG_REPLY, flags);
	if (slrpc_ops.slrpc_rep_in)
		slrpc_ops.slrpc_rep_in(csvc, rq, error);
	return (error);
}

void
sl_csvc_online(struct slrpc_cservice *csvc)
{
	LOCK_ENSURE(&csvc->csvc_lock);

	/*
	 * Hit a crash here on FreeBSD on sliod due to NULL import field below.
	 * The type is SLCONNT_MDS, the last errno is ETIMEDOUT (-60).
	 */
	csvc->csvc_import->imp_state = PSCRPC_IMP_FULL;
	csvc->csvc_import->imp_failed = 0;
	csvc->csvc_import->imp_invalid = 0;

	csvc->csvc_flags &= ~(CSVCF_CONNECTING | CSVCF_DISCONNECTING);
	csvc->csvc_flags |= CSVCF_CONNECTED;
}

void
slrpc_connect_finish(struct slrpc_cservice *csvc,
    struct pscrpc_import *imp, struct pscrpc_import *old, int success)
{
	CSVC_LOCK(csvc);
	if (success) {
		if (csvc->csvc_import != imp)
			csvc->csvc_import = imp;
	} else {
		if (csvc->csvc_import == imp)
			csvc->csvc_import = old;
	}
	CSVC_ULOCK(csvc);

	if (!success) {
		pscrpc_abort_inflight(imp);
		pscrpc_drop_conns(&imp->imp_connection->c_peer);
		pscrpc_import_put(imp);
	}
}

/*
 * Non-blocking CONNECT callback.
 */
int
slrpc_connect_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct srm_connect_rep *mp = NULL;
	struct pscrpc_import *oimp = args->pointer_arg[CBARG_OLDIMPORT];
	struct pscrpc_import *imp = args->pointer_arg[CBARG_NEWIMPORT];
	struct slrpc_cservice *csvc = args->pointer_arg[CBARG_CSVC];
	uint32_t *stkversp = args->pointer_arg[CBARG_STKVER];
	uint64_t *uptimep = args->pointer_arg[CBARG_UPTIME];
	struct timespec tv1, tv2;
	int rc;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);

	if (rc) {
		slrpc_connect_finish(csvc, imp, oimp, 0);
		CSVC_LOCK(csvc);
		csvc->csvc_flags &= ~CSVCF_CONNECTING;
	} else {
		tv1.tv_sec = mp->uptime;
		tv1.tv_nsec = 0;
		_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv2);
		timespecsub(&tv2, &tv1, &tv1);
		*uptimep = tv1.tv_sec;
		*stkversp = mp->stkvers;
		slrpc_connect_finish(csvc, imp, oimp, 1);
		CSVC_LOCK(csvc);
		sl_csvc_online(csvc);
	}
	clock_gettime(CLOCK_MONOTONIC, &csvc->csvc_mtime);
	csvc->csvc_lasterrno = rc;
	psc_waitq_wakeall(&csvc->csvc_waitq);
	sl_csvc_decref_locked(csvc);
	return (0);
}

struct pscrpc_import *
slrpc_new_import(struct slrpc_cservice *csvc)
{
	struct pscrpc_import *imp;

	imp = pscrpc_new_import();
	if (imp == NULL)
		psc_fatalx("pscrpc_new_import");
	imp->imp_cli_request_portal = csvc->csvc_rqptl;
	imp->imp_cli_reply_portal = csvc->csvc_rpptl;

	imp->imp_max_retries = pfl_rpc_max_retry;

//	imp->imp_igntimeout = 1;	/* XXX only if archiver */
	imp->imp_igntimeout = 0;
	imp->imp_hldropf = csvc->csvc_hldropf;
	imp->imp_hldrop_arg = csvc->csvc_hldroparg;
	return (imp);
}

/*
 * Attempt connection initiation with a peer.
 * @local: LNET NID to connect from.
 * @server: NID of server peer.
 * @csvc: client service to peer.
 * @flags: operation flags.
 * @stkversp: value-result pointer to SLASH2 stack version for peer.
 */
__static int
slrpc_issue_connect(lnet_nid_t local, lnet_nid_t server,
    struct slrpc_cservice *csvc, int flags,
    __unusedx struct pfl_multiwait *mw, 
    uint32_t *stkversp, uint64_t *uptimep, __unusedx int timeout)
{
	lnet_process_id_t server_id = { server, PSCRPC_SVR_PID };
	struct pscrpc_import *imp, *oimp = NULL;
	struct pscrpc_connection *c;
	struct srm_connect_req *mq;
	struct srm_connect_rep *mp;
	struct pscrpc_request *rq;
	struct timespec tv1, tv2;
	int rc;

	c = pscrpc_get_connection(server_id, local, NULL);

	CSVC_LOCK(csvc);

	if (csvc->csvc_import == NULL) {
		imp = slrpc_new_import(csvc);
		csvc->csvc_import = imp;
	} else
		imp = csvc->csvc_import;

	if (imp->imp_connection)
		pscrpc_put_connection(imp->imp_connection);
	imp->imp_connection = c;
	imp->imp_connection->c_imp = imp;
	imp->imp_connection->c_peer.pid = PSCRPC_SVR_PID;

	CSVC_ULOCK(csvc);

	/* handled by slrpc_handle_connect() */
	rc = SL_RSX_NEWREQ(csvc, SRMT_CONNECT, rq, mq, mp);
	if (rc) {
		slrpc_connect_finish(csvc, imp, oimp, 0);
		return (rc);
	}
	mq->magic = csvc->csvc_magic;
	mq->version = csvc->csvc_version;
	mq->stkvers = sl_stk_version;

	_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv1);
	timespecsub(&tv1, &pfl_uptime, &tv1);
	mq->uptime = tv1.tv_sec;

	if (flags & CSVCF_NONBLOCK) {

		rq->rq_silent_timeout = 1;
		rq->rq_interpret_reply = slrpc_connect_cb;
		rq->rq_async_args.pointer_arg[CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[CBARG_STKVER] = stkversp;
		rq->rq_async_args.pointer_arg[CBARG_UPTIME] = uptimep;
		rq->rq_async_args.pointer_arg[CBARG_OLDIMPORT] = oimp;
		rq->rq_async_args.pointer_arg[CBARG_NEWIMPORT] = imp;
		rc = SL_NBRQSET_ADD(csvc, rq);
		if (rc) {
			pscrpc_req_finished(rq);
			slrpc_connect_finish(csvc, imp, oimp, 0);
			return (rc);
		}
		return (EWOULDBLOCK);
	}

	rc = SL_RSX_WAITREP(csvc, rq, mp);

	/* we could get -ETIMEDOUT without MDS invloved */
	rc = abs(rc);
	if (rc == 0)
		/* XXX MDS side should return a negative value */
		rc = abs(mp->rc);

	if (!rc) {
		*stkversp = mp->stkvers;

		tv1.tv_sec = mp->uptime;
		tv1.tv_nsec = 0;
		_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv2);
		timespecsub(&tv2, &tv1, &tv1);
		*uptimep = tv1.tv_sec;
	}
	pscrpc_req_finished(rq);

	slrpc_connect_finish(csvc, imp, oimp, rc == 0);
	return (rc);
}

int
slrpc_ping_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[0];
	struct srm_ping_rep *mp;
	int rc;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);

	CSVC_LOCK(csvc);
	clock_gettime(CLOCK_MONOTONIC, &csvc->csvc_mtime);
       	csvc->csvc_flags &= ~CSVCF_PINGING;
	sl_csvc_decref_locked(csvc);
	return (0);
}

int
slrpc_issue_ping(struct slrpc_cservice *csvc, int st_rc)
{
	struct pscrpc_request *rq;
	struct srm_ping_req *mq;
	struct srm_ping_rep *mp;
	int rc;

	/* handled by slm_rmi_handle_ping() */
	rc = SL_RSX_NEWREQ(csvc, SRMT_PING, rq, mq, mp);
	if (rc)
		return (rc);
	mq->rc = st_rc;
	mq->upnonce = sl_sys_upnonce;
	rq->rq_interpret_reply = slrpc_ping_cb;
	rq->rq_async_args.pointer_arg[0] = csvc;

	CSVC_LOCK(csvc);
	sl_csvc_incref(csvc);
	CSVC_ULOCK(csvc);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
	return (rc);
}

int
slrpc_handle_connect(struct pscrpc_request *rq, uint64_t magic,
    uint32_t version, enum slconn_type peertype)
{
	struct pscrpc_export *e = rq->rq_export;
	const struct srm_connect_req *mq;
	struct srm_connect_rep *mp;
	struct sl_resm *m;
	char buf[PSCRPC_NIDSTR_SIZE];
	struct timespec tv1, tv2;
	struct sl_exp_cli *expc;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != magic || mq->version != version)
		mp->rc = -EPERM;

	/* stkvers and uptime are returned in slctlrep_getconn() */
	tv1.tv_sec = mq->uptime;
	tv1.tv_nsec = 0;
	_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv2);
	timespecsub(&tv2, &tv1, &tv1);

	switch (peertype) {
	case SLCONNT_CLI:
		if (e->exp_private)
			/*
			 * No additional state is maintained in the
			 * export so this is not a fatal condition but
			 * should be noted.
			 */
			DEBUG_REQ(PLL_WARN, rq, buf,
			    "duplicate connect msg detected");

		/*
		 * Establish a SLCONNT_CLI connection to our newly
		 * arrived client. This is called when a client 
		 * connects to the MDS or an IOS.
		 */
		expc = sl_exp_getpri_cli(e, 1);
		expc->expc_stkvers = mq->stkvers;
		expc->expc_uptime = tv1.tv_sec; 
		break;
	case SLCONNT_IOD:
		m = libsl_try_nid2resm(rq->rq_peer.nid);
		if (m == NULL) {
			mp->rc = -SLERR_ION_UNKNOWN;
			break;
		}
		if (!RES_ISFS(m->resm_res)) {
			mp->rc = -SLERR_RES_BADTYPE;
			break;
		}
		m->resm_res->res_stkvers = mq->stkvers;
		m->resm_res->res_uptime = tv1.tv_sec;
		break;
	case SLCONNT_MDS:
		m = libsl_try_nid2resm(rq->rq_peer.nid);
		if (m == NULL) {
			mp->rc = -SLERR_RES_UNKNOWN;
			break;
		}
		if (m->resm_type != SLREST_MDS) {
			mp->rc = -SLERR_RES_BADTYPE;
			break;
		}
		m->resm_res->res_stkvers = mq->stkvers;
		m->resm_res->res_uptime = tv1.tv_sec;
		break;
	default:
		psc_fatal("choke");
	}
	mp->stkvers = sl_stk_version;
	timespecsub(&tv2, &pfl_uptime, &tv1);
	mp->uptime = tv1.tv_sec; 
	return (0);
}

void
_sl_csvc_waitrelv(struct slrpc_cservice *csvc, long s, long ns)
{
	struct timespec ts;

	ts.tv_sec = s;
	ts.tv_nsec = ns;

	spinlock(&csvc->csvc_lock);
	psc_waitq_waitrel_ts(&csvc->csvc_waitq, &csvc->csvc_lock, &ts);
}

/*
 * Determine service connection useability.
 * @csvc: client service.
 */
int
sl_csvc_useable(struct slrpc_cservice *csvc)
{
	int flags;
	
	LOCK_ENSURE(&csvc->csvc_lock);
	if (csvc->csvc_import == NULL ||
	    csvc->csvc_import->imp_failed ||
	    csvc->csvc_import->imp_invalid)
		return (0);
	flags = CSVCF_CONNECTED | CSVCF_DISCONNECTING | CSVCF_CONNECTING;
	return ((csvc->csvc_flags & flags) == CSVCF_CONNECTED);
}

/*
 * Account for releasing the use of a remote service connection.
 * @csvc: client service.
 */
void
_sl_csvc_decref(const struct pfl_callerinfo *pci,
    struct slrpc_cservice *csvc, int locked)
{
	struct pscrpc_import *imp;
	int rc;

	/*
 	 * Recursive locking won't let me unlock later unless you
 	 * ignore it.
 	 */
	if (!locked)
		CSVC_LOCK(csvc);

	/*
 	 * 09/07/2017: pscrpc_drop_conns() --> pscrpc_fail_import()
 	 * sl_imp_hldrop_cli() makes rc = -1.
 	 *
 	 * 10/12/1027: Similar crash coming from usocklnd_tear_peer_conn().
 	 */
	rc = --csvc->csvc_refcnt;
	psc_assert(rc >= 0);
	psclog_diag("after drop ref csvc = %p, refcnt = %d", 
	    csvc, csvc->csvc_refcnt);
	if (rc > 0) {
		CSVC_ULOCK(csvc);
		return;
	}
	psc_assert(!(csvc->csvc_flags & CSVCF_WATCH));
	psc_assert(!(csvc->csvc_flags & CSVCF_TOFREE));
	csvc->csvc_flags |= CSVCF_TOFREE;

	/*
	 * Drop lock before potentially grabbing the list lock.
	 * This is different from the locking order we use in
	 * instantiation. However, I have not found any reason
	 * why this causes the crash.
	 */
	CSVC_ULOCK(csvc);

	if (csvc->csvc_peertype == SLCONNT_CLI) {
		csvc->csvc_flags &= ~CSVCF_ONLIST;
		pll_remove(&sl_clients, csvc);
	}

	/*
	 * Due to the nature of non-blocking CONNECT, the import 
	 * may or may not actually be present.
	 */
	imp = csvc->csvc_import;
	if (imp)
		pscrpc_import_put(imp);

	DEBUG_CSVC(PLL_DIAG, csvc, "freed");
	psc_pool_return(sl_csvc_pool, csvc);
}

/*
 * Account for starting to use a remote service connection.
 * @csvc: client service.
 */
void
sl_csvc_incref(struct slrpc_cservice *csvc)
{
	LOCK_ENSURE(&csvc->csvc_lock);
	csvc->csvc_refcnt++;
	psclog_diag("after take ref csvc = %p, refcnt = %d", 
	    csvc, csvc->csvc_refcnt);
}

/*
 * Mark a csvc as no longer usable.
 * @csvc: client service.
 */
void
_sl_csvc_disconnect(const struct pfl_callerinfo *pci,
    struct slrpc_cservice *csvc, int locked)
{

	if (!locked)
		CSVC_LOCK(csvc);
	if (!(csvc->csvc_flags & CSVCF_DISCONNECTING)) {
		csvc->csvc_flags |= CSVCF_DISCONNECTING;
		csvc->csvc_flags &= ~CSVCF_CONNECTED;
	}
	if (!locked)
		CSVC_ULOCK(csvc);
}

/*
 * Create a new client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
__static struct slrpc_cservice *
sl_csvc_create(uint32_t rqptl, uint32_t rpptl, void (*hldropf)(void *),
    void *hldroparg)
{
	struct slrpc_cservice *csvc;

	csvc = psc_pool_get(sl_csvc_pool);
	memset(csvc, 0, sizeof(*csvc));

	INIT_SPINLOCK(&csvc->csvc_lock);
	psc_waitq_init(&csvc->csvc_waitq, "csvc");

	INIT_PSC_LISTENTRY(&csvc->csvc_lentry);
	csvc->csvc_rqptl = rqptl;
	csvc->csvc_rpptl = rpptl;
	csvc->csvc_hldropf = hldropf;
	csvc->csvc_hldroparg = hldroparg ? hldroparg : csvc;
	return (csvc);
}

lnet_nid_t
slrpc_getpeernid(struct pscrpc_export *exp,
    struct psc_dynarray *peernids)
{
	struct sl_resm_nid *nr;
	lnet_process_id_t *pp;
	lnet_nid_t peernid;
	int i, j;

	if (exp)
		peernid = exp->exp_connection->c_peer.nid;
	else {
		peernid = LNET_NID_ANY;
		/* prefer directly connected NIDs */
		DYNARRAY_FOREACH(nr, i, peernids) {
			DYNARRAY_FOREACH(pp, j, &sl_lnet_prids) {
				if (LNET_NIDNET(nr->resmnid_nid) ==
				    LNET_NIDNET(pp->nid)) {
					peernid = nr->resmnid_nid;
					goto foundnid;
				}
			}
		}

 foundnid:
		if (peernid == LNET_NID_ANY) {
			nr = psc_dynarray_getpos(peernids, 0);
			peernid = nr->resmnid_nid;
		}
	}
	psc_assert(peernid != LNET_NID_ANY);
	return (peernid);
}

int
csvc_cli_cmp(const void *a, const void *b)
{
	const struct slrpc_cservice *ca = a, *cb = b;

	/* XXX race */
	if (ca->csvc_import->imp_connection == NULL ||
	    cb->csvc_import->imp_connection == NULL)
		return (CMP(ca, cb));

	return (CMP(ca->csvc_import->imp_connection->c_peer.nid,
	    cb->csvc_import->imp_connection->c_peer.nid));
}

/*
 * Acquire or create a client RPC service.
 * @csvcp: value-result permanent storage for connection structures.
 * @flags: CSVCF_* flags the connection should take on, only used for
 *	csvc initialization.
 * @exp: RPC peer export.  This or @peernids is required.
 * @peernids: RPC peer network address(es) (NID).  This or @exp is
 *	required.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 * @magic: connection magic bits.
 * @version: version of application protocol.
 * @peertype: peer type.
 * @mw: multiwait structure, used in non-blocking acquisitions.
 *
 * If we acquire a connection successfully, this function will return
 * the same slrpc_cservice struct pointer as referred to by its
 * first argument csvcp.  Otherwise, it returns NULL, but the structure
 * is left in the location referred to by csvcp for retry.
 */
struct slrpc_cservice *
_sl_csvc_get(const struct pfl_callerinfo *pci,
    struct slrpc_cservice **csvcp, int flags,
    struct pscrpc_export *exp, struct psc_dynarray *peernids,
    uint32_t rqptl, uint32_t rpptl, uint64_t magic, uint32_t version,
    enum slconn_type peertype, struct pfl_multiwait *mw, int timeout)
{
	int rc = 0, success;
	void *hldropf, *hldroparg;
	uint64_t *uptimep = NULL;
	uint32_t *stkversp = NULL;
	struct slrpc_cservice *csvc = NULL;
	struct sl_resm *resm = NULL; /* gcc */
	struct timespec now;
	lnet_nid_t peernid;
	struct sl_exp_cli *expc;
	struct sl_resm_nid *nr;
	lnet_process_id_t *pp;
	int i, j, new = 0;
	uint64_t delta;

	if (peertype != SLCONNT_CLI && 
	    peertype != SLCONNT_MDS && 
	    peertype != SLCONNT_IOD)
		psc_fatalx("%d: bad peer connection type", peertype);

	/* first grab read lock */
	pfl_rwlock_rdlock(&sl_conn_lock);
	if (*csvcp) {
		csvc = *csvcp;
		CSVC_LOCK(csvc);
		sl_csvc_incref(csvc);
		CSVC_ULOCK(csvc);
		pfl_rwlock_unlock(&sl_conn_lock);
		goto gotit;
	}

	/* upgrade to write lock */
	pfl_rwlock_unlock(&sl_conn_lock);
	pfl_rwlock_wrlock(&sl_conn_lock);
	if (*csvcp) {
		csvc = *csvcp;
		CSVC_LOCK(csvc);
		sl_csvc_incref(csvc);
		CSVC_ULOCK(csvc);
		pfl_rwlock_unlock(&sl_conn_lock);
		goto gotit;
	}

	switch (peertype) {
	case SLCONNT_CLI:
		/*
		 * From Lustre document: An import is the client side 
		 * of the connection to a target. An export is the server 
		 * side. A client has one import for every target.
		 * A service has one export for every client.
		 *
		 * This is the case when a client connects to MDS or IOS.
		 *
		 * The csvc will be dropped by both sl_imp_hldrop_cli()
		 * and sl_exp_hldrop_cli().  So the reference will be
		 * initialized to two as well.
		 */
		hldropf = sl_imp_hldrop_cli;
		hldroparg = NULL;
		break;
	case SLCONNT_IOD:
	case SLCONNT_MDS:
		peernid = slrpc_getpeernid(exp, peernids);
		resm = libsl_nid2resm(peernid);

		if (peertype == SLCONNT_MDS) {
			if (resm->resm_type != SLREST_MDS)
				psc_fatalx("csvc requested type is MDS "
				    "but resource is MDS");
		} else {
			if (resm->resm_type == SLREST_MDS)
				psc_fatalx("csvc requested type is IOS "
				    "but resource is MDS");
		}

		hldropf = sl_imp_hldrop_resm;
		hldroparg = resm;
		break;
	}

	/* initialize service */
	csvc = sl_csvc_create(rqptl, rpptl, hldropf, hldroparg);
	csvc->csvc_params.scp_csvcp = csvcp;

	/* one for our pointer, the other for current connection attempt */
	csvc->csvc_refcnt = 2;
	csvc->csvc_flags = flags;
	csvc->csvc_peertype = peertype;
	csvc->csvc_peernids = peernids;
	csvc->csvc_version = version;
	csvc->csvc_magic = magic;

	/* publish new csvc */
	new = 1;
	*csvcp = csvc;
	pfl_rwlock_unlock(&sl_conn_lock);

 gotit:

	psclog_diag("%s csvc = %p, refcnt = %d", 
	    new ? "create" : "reuse",  csvc, csvc->csvc_refcnt);

	CSVC_LOCK(csvc);
	psc_assert(csvc->csvc_peertype == peertype);

	switch (peertype) {
	case SLCONNT_CLI:
		expc = (void *)csvc->csvc_params.scp_csvcp;
		stkversp = &expc->expc_stkvers;
		uptimep = &expc->expc_uptime;
		break;
	case SLCONNT_IOD:
	case SLCONNT_MDS:
		resm = (void *)csvc->csvc_params.scp_csvcp;
		stkversp = &resm->resm_res->res_stkvers;
		uptimep = &resm->resm_res->res_uptime;
		break;
	}

 recheck:

	success = 1;
	if (sl_csvc_useable(csvc))
		goto out2;

	clock_gettime(CLOCK_MONOTONIC, &now);

	if (exp) {
		struct pscrpc_connection *c;

		if (csvc->csvc_import == NULL)
			csvc->csvc_import = slrpc_new_import(csvc);

		/*
		 * If an export was specified, the peer has already
		 * established a connection to our service, so just
		 * reuse the underhood connection to establish a
		 * connection back to his service.
		 *
		 * The idea is to share the same connection between
		 * an export and an import.  Note we use a local
		 * variable to keep the existing connection intact
		 * until the export connection is assigned to us.
		 */
		c = csvc->csvc_import->imp_connection;

		atomic_inc(&exp->exp_connection->c_refcount);
		// XXX pscrpc_connection_addref();
		csvc->csvc_import->imp_connection = exp->exp_connection;
		csvc->csvc_import->imp_connection->c_imp =
		    csvc->csvc_import;

		if (c)
			pscrpc_put_connection(c);

		csvc->csvc_mtime = now;
		goto out1;
	}

	if (csvc->csvc_flags & CSVCF_CONNECTING) {

		if (flags & CSVCF_NONBLOCK) {
			success = 0;	
			OPSTAT_INCR("csvc-nowait");
			goto out2;
		}

		OPSTAT_INCR("csvc-wait");

		psc_waitq_wait(&csvc->csvc_waitq, &csvc->csvc_lock);
		CSVC_LOCK(csvc);

		OPSTAT_INCR("csvc-wait-recheck");
		goto recheck;
	}

	if (flags & CSVCF_NORECON) {
		success = 0;	
		goto out2;
	}

	if (!timeout && csvc->csvc_mtime.tv_sec + CSVC_CONN_INTV > now.tv_sec) {
		if (flags & CSVCF_NONBLOCK) {
			success = 0;	
			goto out2;
		}
		OPSTAT_INCR("csvc-delay");
		delta = csvc->csvc_mtime.tv_sec + CSVC_CONN_INTV - now.tv_sec;
		CSVC_ULOCK(csvc);
		sleep(delta);
		CSVC_LOCK(csvc);
		goto recheck;
	}

	csvc->csvc_flags |= CSVCF_CONNECTING;
	csvc->csvc_flags &= ~CSVCF_DISCONNECTING;
	/*
 	 * We can't assert CSVCF_CONNECTED is not set below because 
	 * sl_csvc_useable() has other ideas why it is not connected.
	 */
	csvc->csvc_flags &= ~CSVCF_CONNECTED;
	CSVC_ULOCK(csvc);

	/*
	 * Don't clear CSVCF_CONNECTING and wake up waiters in between until 
	 * we have tried all possibilities. Also, at most one async connection
	 * is allowed.
	 */
	rc = ENETUNREACH;
	DYNARRAY_FOREACH(nr, i, peernids) {
	    DYNARRAY_FOREACH(pp, j, &sl_lnet_prids) {
		if (LNET_NIDNET(nr->resmnid_nid) == LNET_NIDNET(pp->nid)) {
			rc = slrpc_issue_connect( pp->nid, nr->resmnid_nid,
			    csvc, flags, mw, stkversp, uptimep, timeout);
			if (rc == 0 || rc == EWOULDBLOCK)
				goto proc_conn;
		}
	    }
	}

 proc_conn:

	CSVC_LOCK(csvc);

	if (rc == EWOULDBLOCK) {
		/* will drop shortly, this allows us to return NULL */
 		sl_csvc_incref(csvc);
		success = 0;	
		goto out2;
	}

	csvc->csvc_lasterrno = rc;
	clock_gettime(CLOCK_MONOTONIC, &csvc->csvc_mtime);
	csvc->csvc_flags &= ~CSVCF_CONNECTING;
	psc_waitq_wakeall(&csvc->csvc_waitq);
	if (rc) {
		if (csvc->csvc_import)
			csvc->csvc_import->imp_failed = 1;
		/*
		 * The csvc stays allocated but is marked as
		 * unusable until the next connection
		 * establishment attempt.
		 *
		 * Our caller is signified about this failure
		 * via the NULL return here.
		 */
		success = 0;	
		goto out2;
	}

 out1:

	sl_csvc_online(csvc);

	/*
 	 * In theory, a client csvc should go away once it is not online.
 	 * Apparently, there is a race condition somewhere that has caused
 	 * trouble on our production system. Hence CSVCF_ONLIST.
 	 *
 	 * To examine csvc_flags, use the following:
 	 *
 	 * (gdb) p csvc->csvc_params.scp_flags
 	 *
 	 * 07/10/2017: Hit a crash with flag 11000010, this indicates that 
 	 * there is a code path that can reach the csvc after its reference 
 	 * count is zero and marked to free.
 	 *
 	 * Hit again, this comes from the mdscoh_req code path.  The problem
 	 * is that bml->bml_exp is not protected by a reference count, which
 	 * probably in turn can't ensure the csvc is still there.
 	 *
 	 * Hit again from slm_ptrunc_prepare().
 	 */
	if (peertype == SLCONNT_CLI && !(csvc->csvc_flags & CSVCF_ONLIST)) {
		csvc->csvc_flags |= CSVCF_ONLIST;
		pll_add_sorted(&sl_clients, csvc, csvc_cli_cmp);
	}
 out2:

	if (!success) {
 		sl_csvc_decref_locked(csvc);
		csvc = NULL;
	} else
		CSVC_ULOCK(csvc);
	return (csvc);
}

/*
 * Logic for peer resource connection monitor.  It is used to ensure a
 * connection to a peer is up by occasionally sending PINGs.
 *
 * MDS - needs to check pings from IONs
 * ION - needs to send PINGs to MDS
 * CLI - needs to check if MDS is up periodically (not strictly needed).
 */
void
slconnthr_main(struct psc_thread *thr)
{
	struct timespec ts;
	struct slrpc_cservice *csvc;
	struct slconn_thread *sct;
	struct slconn_params *scp;
	int i, rc, pingrc = 0;
	void *dummy;

	sct = thr->pscthr_private;
	while (pscthr_run(thr)) {

		/*
		 * Retrieve the current health status and send the
		 * result via PING RPC later.  IOS only.
		 */
		clock_gettime(CLOCK_MONOTONIC, &ts);
		if (sct->sct_pingupc) { 
			/* slirmiconnthr_upcall() */
			pingrc = sct->sct_pingupc(sct->sct_pingupcarg);
			if (pingrc)
				psclog_diag("sct_pingupc "
				    "failed (rc=%d)", pingrc);
		}

		pfl_multiwait_entercritsect(&sct->sct_mw);

		spinlock(&sl_watch_lock);
		/*
		 * csvc being watched should never be freed because
		 * they are associated with resources. Otherwise, we
		 * would need a reference to keep them on this list.
		 */
		DYNARRAY_FOREACH(scp, i, &sct->sct_monres) {
			freelock(&sl_watch_lock);
			csvc = sl_csvc_get(scp->scp_csvcp,
			    scp->scp_flags | CSVCF_NONBLOCK, NULL,
			    scp->scp_peernids,
			    scp->scp_rqptl, scp->scp_rpptl,
			    scp->scp_magic, scp->scp_version,
			    scp->scp_peertype, NULL, 0);
			if (csvc == NULL)
				/*
				 * XXX: Allow manual activity to try to
				 * reconnect while we wait.
				 */
				goto next;

			CSVC_LOCK(csvc);
			/*
			 * Only used by MDS to watch for its I/O servers.  
			 * And scp_useablef is always mds_sliod_alive().
			 *
			 * Note that the above sl_csvc_get() only
			 * returns csvc when the connection is already
			 * established.
			 *
			 * XXX We don't really need this logic, because
			 * slm_try_sliodresm() will check it as well.
			 *
			 * We mark it as disconnected, and yet we don't
			 * try to connect to IOS (we use CSVCF_NORECON).
			 * We might as well let the PINGs from IOS to
			 * take care things.
			 *
			 * I drop my IOS into gdb for 30+minutes, the
			 * export/import drop functions are not called.
			 * But here, we mark the csvc as disconnected.
			 * This is why I need slm_rmi_handle_ping() to
			 * bring the csvc back online.
			 */
			if (scp->scp_useablef &&
			    !scp->scp_useablef(scp->scp_useablearg))
				sl_csvc_disconnect_locked(csvc);

			/*
			 * Ping MDS and send my health status. IOS only.
			 */
			if (sl_csvc_useable(csvc) &&
			    (scp->scp_flags & CSVCF_PING) && 
			    !(scp->scp_flags & CSVCF_PINGING)) {
			        scp->scp_flags |= CSVCF_PINGING;
				CSVC_ULOCK(csvc);
				rc = slrpc_issue_ping(csvc, pingrc);
				CSVC_LOCK(csvc);
				if (rc) {
					sl_csvc_disconnect_locked(csvc);
			        	scp->scp_flags &= ~CSVCF_PINGING;
				}
				memcpy(&csvc->csvc_mtime, &ts, sizeof(ts));
			}

			sl_csvc_decref_locked(csvc);
 next:
			spinlock(&sl_watch_lock);
		}
		freelock(&sl_watch_lock);
		/* 05/06/2017: Sigbus */
		pfl_multiwait_secs(&sct->sct_mw, &dummy, CSVC_PING_INTV);
		//pfl_multiwait_secs(&sct->sct_mw, &dummy, 100000);
	}
}

/*
 * Spawn a connection thread.  The thread maintains an established
 * connection to any peers for which it is configured.
 */
struct psc_thread *
slconnthr_spawn(int thrtype, const char *thrnamepre,
    int (*pingupc)(void *), void *pingupcarg)
{
	struct slconn_thread *sct;
	struct psc_thread *thr;

	thr = pscthr_init(thrtype, slconnthr_main, sizeof(*sct),
	    "%sconnthr", thrnamepre);
	sct = thr->pscthr_private;
	sct->sct_pingupc = pingupc;
	sct->sct_pingupcarg = pingupcarg;
	pfl_multiwait_init(&sct->sct_mw, "resmon");
	pfl_multiwaitcond_init(&sct->sct_mwc, NULL, 0, "connection");
	pfl_multiwait_addcond(&sct->sct_mw, &sct->sct_mwc);
	pscthr_setready(thr);
	return (thr);
}

/*
 * Add a peer to the list of peers to which a connection thread
 * maintains an established connection.
 */
void
slconnthr_watch(struct psc_thread *thr, struct slrpc_cservice *csvc,
    int flags, int (*useablef)(void *), void *useablearg)
{
	struct slconn_thread *sct;
	struct slconn_params *scp;
	int rc;

	sct = thr->pscthr_private;
	scp = &csvc->csvc_params;

	spinlock(&sl_watch_lock);
	rc = psc_dynarray_exists(&sct->sct_monres, scp);
	if (!rc)
		psc_dynarray_add(&sct->sct_monres, scp);
	freelock(&sl_watch_lock);
	if (rc)
		return;

	/*
	 * 07/10/2017:
	 *
	 * Odd output from slmctl -sc from the MDS:
	 *
 	 *  stor013s1  ...    -O--       0    8     2  201d22h18m
 	 *
 	 * We should have a W flag, and the uptime is bogus as well.
 	 * The sliod is otherwise functional though. This is fixed 
 	 * by restarting the corresponding sliod.
 	 */
	CSVC_LOCK(csvc);
	scp->scp_flags |= flags | CSVCF_WATCH;
	scp->scp_useablef = useablef;
	scp->scp_useablearg = useablearg;
	CSVC_ULOCK(csvc);

}

void
sl_imp_hldrop_cli(void *arg)
{
    	struct slrpc_cservice *csvc = arg;

	CSVC_LOCK(csvc);
	sl_csvc_disconnect_locked(csvc);
	sl_csvc_decref_locked(csvc);
	OPSTAT_INCR("rpc.import-drop-client");
}

void
sl_imp_hldrop_resm(void *arg)
{
	struct sl_resm *resm = arg;

	sl_csvc_disconnect(resm->resm_csvc);
	OPSTAT_INCR("rpc.import-drop-resm");
}

/*
 * Callback triggered when an export to a CLIENT fails.
 * @exp: export to RPC CLI peer.
 */
void
sl_exp_hldrop_cli(struct pscrpc_export *exp)
{
	struct slrpc_cservice *csvc;
	struct sl_exp_cli *expc;

	EXPORT_LOCK(exp);
	expc = exp->exp_private;
	exp->exp_private = NULL;
	EXPORT_ULOCK(exp);

	csvc = expc->expc_csvc;
	CSVC_LOCK(csvc);
	sl_csvc_disconnect_locked(csvc);
	sl_csvc_decref_locked(csvc);

	OPSTAT_INCR("rpc.export-drop-client");
	PSCFREE(expc);
}

/*
 * Callback triggered when an export to a resource member fails.
 * @exp: export to RPC peer.
 */
void
sl_exp_hldrop_resm(struct pscrpc_export *exp)
{
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	struct sl_resm *resm;

	resm = libsl_nid2resm(exp->exp_connection->c_peer.nid);
	if (resm) {
		/* (gdb) p resm->resm_csvc->csvc_refcnt */
		psclog_warnx("drop export %p, resm = %p, name = %s", 
		    exp, resm, resm->resm_name);
		sl_csvc_disconnect(resm->resm_csvc);
		sl_resm_hldrop(resm);
	} else {
		pscrpc_nid2str(exp->exp_connection->c_peer.nid, nidbuf);
		psclog_warnx("no resm for %s", nidbuf);
	}
	OPSTAT_INCR("rpc.export-drop-resm");
}

/*
 * Get pscrpc_export private data specific to CLIENT peer.
 * @exp: RPC export to CLI peer.
 *
 * populate is only true when we handles a connection request.
 * See slrpc_handle_connect().
 */
void *
sl_exp_getpri_cli(struct pscrpc_export *exp, int populate)
{
	void *p;
	int locked;
	static struct slrpc_cservice *csvc;

	locked = EXPORT_RLOCK(exp);
	if (exp->exp_private == NULL && populate) {
		/* mexpc_allocpri() or iexpc_allocpri() */
		csvc = sl_expcli_ops.secop_allocpri(exp);
		/*
		 * This is probably Okay because we re-use the 
		 * connection from the client that has already 
		 * been established. See comments in _sl_csvc_get().
		 */
		psc_assert(csvc);
		exp->exp_hldropf = sl_exp_hldrop_cli;
	}
	p = exp->exp_private;
	EXPORT_URLOCK(exp, locked);
	return (p);
}

void
slrpc_bulk_sign(__unusedx struct pscrpc_request *rq, void *buf,
    struct iovec *iov, int n)
{
	char ebuf[BUFSIZ];
	gcry_error_t gerr;
	gcry_md_hd_t hd;
	int i;

	gerr = gcry_md_copy(&hd, sl_authbuf_hd);
	if (gerr) {
		gpg_strerror_r(gerr, ebuf, sizeof(ebuf));
		psc_fatalx("gcry_md_copy: %s [%d]", ebuf, gerr);
	}

	for (i = 0; i < n; i++)
		gcry_md_write(hd, iov[i].iov_base, iov[i].iov_len);

	memcpy(buf, gcry_md_read(hd, 0), AUTHBUF_ALGLEN);

	gcry_md_close(hd);
}

int
slrpc_bulk_check(struct pscrpc_request *rq, const void *hbuf,
    struct iovec *iov, int n)
{
	char tbuf[AUTHBUF_ALGLEN];
	char buf[PSCRPC_NIDSTR_SIZE];
	int rc = 0;

	slrpc_bulk_sign(rq, tbuf, iov, n);
	if (memcmp(tbuf, hbuf, AUTHBUF_ALGLEN)) {
		DEBUG_REQ(PLL_FATAL, rq, buf, "authbuf did not hash "
		    "correctly -- ensure key files are synced");
		rc = SLERR_AUTHBUF_BADHASH;
	}

	return (rc);
}

int
slrpc_bulk_checkmsg(struct pscrpc_request *rq, struct pscrpc_msg *m,
    struct iovec *iov, int n)
{
	struct srt_authbuf_footer *saf;

	/*
	 * XXX can this flag be arbitrarily spoofed to ignore this check?
	 */
	if (m->flags & MSG_ABORT_BULK)
		return (0);
	saf = pscrpc_msg_buf(m, m->bufcount - 1, sizeof(*saf));
	return (slrpc_bulk_check(rq, saf->saf_bulkhash, iov, n));
}

/*
 * Perform high level SLASH2 bulk RPC setup.
 *
 * Notes:
 *
 *	This entails performing the "authbuf" message hashing to ensure
 *	integrity.  Bulk RPCs contain an additional hash comprised of:
 *
 *	  field 0               field 1                  nfields-1
 *	  [RPC request struct 1][RPC request struct 2]...[authbuf][bulk]
 *
 *	The hash is made over the authbuf struct (nfields - 1) itself
 *	contained in the last section of the RPC itself then over the
 *	bulk contents.
 */
int
slrpc_bulkserver(struct pscrpc_request *rq, int type, int chan,
    struct iovec *iov, int n)
{
	struct srt_authbuf_footer *saf;
	struct pscrpc_msg *m;
	int rc;

	m = type == BULK_PUT_SOURCE ? rq->rq_repmsg : rq->rq_reqmsg;
	saf = pscrpc_msg_buf(m, m->bufcount - 1, sizeof(*saf));

	if (type == BULK_PUT_SOURCE)
		slrpc_bulk_sign(rq, saf->saf_bulkhash, iov, n);
	rc = rsx_bulkserver(rq, type, chan, iov, n);
	if (rc)
		goto out;

	if (type == BULK_GET_SINK)
		rc = slrpc_bulk_check(rq, saf->saf_bulkhash, iov, n);

 out:
	if (rc == -ETIMEDOUT) {
		OPSTAT_INCR("rpc.bulk-timeout");
		rc = -PFLERR_TIMEDOUT;
	}
	return (rc);
}

/*
 * Perform RPC bulk operation as an RPC client.
 */
int
slrpc_bulkclient(struct pscrpc_request *rq, int type, int chan,
    struct iovec *iov, int n)
{
	return (rsx_bulkclient(rq, type, chan, iov, n));
}

/*
 * Set up internal SLASH2 RPC layer structures.
 */
void
slrpc_initcli(void)
{
	pfl_rwlock_init(&sl_conn_lock);
	psc_poolmaster_init(&sl_csvc_poolmaster,
	    struct slrpc_cservice, csvc_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, "csvc");
	sl_csvc_pool = psc_poolmaster_getmgr(&sl_csvc_poolmaster);
}

/*
 * Tear down SLASH2 RPC layer.
 */
void
slrpc_destroy(void)
{
	pfl_poolmaster_destroy(&sl_csvc_poolmaster);
}

const char *
slrpc_getname_for_opcode(int opc)
{
	if (opc < 1 || opc >= SRMT_TOTAL)
		return (NULL);
	return (slrpc_names[opc]);
}
