/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/statvfs.h>

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_rpc/export.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/multiwait.h"

#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"

struct psc_lockedlist	client_csvcs = PLL_INIT(&client_csvcs,
    struct slashrpc_cservice, csvc_lentry);

__weak void
psc_multiwaitcond_wakeup(__unusedx struct psc_multiwaitcond *arg)
{
	psc_fatalx("unimplemented stub");
}

__weak int
psc_multiwaitcond_waitrel_ts(__unusedx struct psc_multiwaitcond *arg,
    __unusedx struct pfl_mutex *mutex, __unusedx const struct timespec *ts)
{
	psc_fatalx("unimplemented stub");
}

__weak int
_psc_multiwait_addcond(__unusedx struct psc_multiwait *mw,
    __unusedx struct psc_multiwaitcond *cond, __unusedx int masked)
{
	psc_fatalx("unimplemented stub");
}

int
slrpc_newgenreq(struct slashrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	int qlens[] = { qlen, sizeof(struct srt_authbuf_footer) };
	int plens[] = { plen, sizeof(struct srt_authbuf_footer) };

	return (RSX_NEWREQN(csvc->csvc_import, csvc->csvc_version, op,
	    *rqp, nitems(qlens), qlens, nitems(plens), plens,
	    *(void **)mqp));
}

__weak int
slrpc_newreq(struct slashrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	return (slrpc_newgenreq(csvc, op, rqp, qlen, plen, mqp));
}

int
slrpc_waitgenrep(struct pscrpc_request *rq, int plen, void *mpp)
{
	int rc;

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	rc = pfl_rsx_waitrep(rq, plen, mpp);
	if (rc == 0)
		rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	return (rc);
}

__weak int
slrpc_waitrep(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq, int plen, void *mpp)
{
	return (slrpc_waitgenrep(rq, plen, mpp));
}

int
slrpc_allocrepn(struct pscrpc_request *rq, void *mq0p, int q0len,
    void *mp0p, int np, const int *plens, int rcoff)
{
	int *rcp;

	RSX_ALLOCREPNRC(rq, *(void **)mq0p, q0len, *(void **)mp0p, np,
	    plens, rcoff);
	rcp = PSC_AGP(*(void **)mp0p, rcoff);
	*rcp = authbuf_check(rq, PSCRPC_MSG_REQUEST);
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

__weak int
slrpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	return (slrpc_allocgenrep(rq, mqp, qlen, mpp, plen, rcoff));
}

void
sl_csvc_online(struct slashrpc_cservice *csvc)
{
	sl_csvc_lock_ensure(csvc);

	csvc->csvc_import->imp_state = PSCRPC_IMP_FULL;
	csvc->csvc_import->imp_failed = 0;
	csvc->csvc_import->imp_invalid = 0;

	psc_atomic32_clearmask(&csvc->csvc_flags,
	    CSVCF_CONNECTING);
	psc_atomic32_setmask(&csvc->csvc_flags,
	    CSVCF_CONNECTED);

	csvc->csvc_lasterrno = 0;

	if (sl_csvc_usemultiwait(csvc))
		psc_multiwaitcond_wakeup(csvc->csvc_waitinfo);
}

int
slrpc_connect_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc;
	struct srm_connect_rep *mp;
	int rc;

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc == 0)
		rc = rq->rq_status;
	if (rc == 0) {
		mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
		rc = mp->rc;
	}

	csvc = args->pointer_arg[0];
	sl_csvc_lock(csvc);
	csvc->csvc_mtime = time(NULL);
	psc_atomic32_clearmask(&csvc->csvc_flags,
	    CSVCF_CONNECTING);
	if (rc) {
		csvc->csvc_import->imp_failed = 1;
		csvc->csvc_lasterrno = rc;
	} else
		sl_csvc_online(csvc);
	sl_csvc_wake(csvc);
	sl_csvc_unlock(csvc);
	return (0);
}

/**
 * slrpc_issue_connect - Attempt connection initiation with a peer.
 * @server: NID of server peer.
 * @csvc: client service to peer.
 * @flags: operation flags.
 */
__static int
slrpc_issue_connect(lnet_nid_t server, struct slashrpc_cservice *csvc,
    int flags)
{
	lnet_process_id_t prid, server_id = { server, PSCRPC_SVR_PID };
	struct srm_connect_req *mq;
	struct srm_connect_rep *mp;
	struct pscrpc_request *rq;
	struct pscrpc_import *imp;
	int rc;

	pscrpc_getpridforpeer(&prid, &lnet_prids, server);
	if (prid.nid == LNET_NID_ANY)
		return (ENETUNREACH);

	imp = csvc->csvc_import;
	if (imp->imp_connection)
		pscrpc_put_connection(imp->imp_connection);
	imp->imp_connection = pscrpc_get_connection(server_id, prid.nid,
	    NULL);
	imp->imp_connection->c_imp = imp;
	imp->imp_connection->c_peer.pid = PSCRPC_SVR_PID;

	rc = SL_RSX_NEWREQ(csvc, SRMT_CONNECT, rq, mq, mp);
	if (rc)
		return (rc);
	rq->rq_timeoutable = 1;
	mq->magic = csvc->csvc_magic;
	mq->version = csvc->csvc_version;

	if (flags & CSVCF_NONBLOCK) {
		if (flags & CSVCF_USE_MULTIWAIT) {
			rq->rq_interpret_reply = slrpc_connect_cb;
			rq->rq_async_args.pointer_arg[0] = csvc;
			authbuf_sign(rq, PSCRPC_MSG_REQUEST);
			rc = pscrpc_nbreqset_add(sl_nbrqset, rq);
			if (rc)
				return (rc);
			return (EWOULDBLOCK);
		}
		psclog_warnx("unable to try non-blocking connect without "
		    "multiwait, reverting to blocking connect");
	}

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slrpc_issue_ping(struct slashrpc_cservice *csvc)
{
	const struct srm_ping_req *mq;
	struct pscrpc_request *rq;
	struct srm_ping_rep *mp;
	int rc;

	rc = SL_RSX_NEWREQ(csvc, SRMT_PING, rq, mq, mp);
	if (rc)
		return (rc);
	rq->rq_timeoutable = 1;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	pscrpc_req_finished(rq);
	return (rc);
}

void
sl_csvc_wake(struct slashrpc_cservice *csvc)
{
	sl_csvc_lock_ensure(csvc);
	if (sl_csvc_usemultiwait(csvc))
		psc_multiwaitcond_wakeup(csvc->csvc_waitinfo);
	else
		psc_waitq_wakeall(csvc->csvc_waitinfo);
}

void
_sl_csvc_waitrelv(struct slashrpc_cservice *csvc, long s, long ns)
{
	struct timespec ts;

	ts.tv_sec = s;
	ts.tv_nsec = ns;

	sl_csvc_lock_ensure(csvc);
	if (sl_csvc_usemultiwait(csvc))
		psc_multiwaitcond_waitrel(csvc->csvc_waitinfo,
		    csvc->csvc_mutex, &ts);
	else
		psc_waitq_waitrel(csvc->csvc_waitinfo,
		    csvc->csvc_lock, &ts);
}

int
sl_csvc_usemultiwait(struct slashrpc_cservice *csvc)
{
	return (psc_atomic32_read(&csvc->csvc_flags) & CSVCF_USE_MULTIWAIT);
}

/**
 * sl_csvc_useable - Determine service connection useability.
 * @csvc: client service.
 */
int
sl_csvc_useable(struct slashrpc_cservice *csvc)
{
	sl_csvc_lock_ensure(csvc);
	if (csvc->csvc_import->imp_failed ||
	    csvc->csvc_import->imp_invalid)
		return (0);
	return ((psc_atomic32_read(&csvc->csvc_flags) &
	  (CSVCF_CONNECTED | CSVCF_ABANDON)) == CSVCF_CONNECTED);
}

/**
 * sl_csvc_markfree - Mark that a connection will be freed when the last
 *	reference goes away.  This should never be performed on service
 *	connections on resms, only for service connections to clients.
 * @csvc: client service.
 */
void
sl_csvc_markfree(struct slashrpc_cservice *csvc)
{
	int locked;

	locked = sl_csvc_reqlock(csvc);
	psc_atomic32_setmask(&csvc->csvc_flags,
	    CSVCF_ABANDON | CSVCF_WANTFREE);
	psc_atomic32_clearmask(&csvc->csvc_flags,
	    CSVCF_CONNECTED | CSVCF_CONNECTING);
	csvc->csvc_lasterrno = 0;
	DEBUG_CSVC(PLL_DEBUG, csvc, "marked WANTFREE");
	sl_csvc_ureqlock(csvc, locked);
}

/**
 * sl_csvc_decref - Account for releasing the use of a remote service
 *	connection.
 * @csvc: client service.
 */
void
sl_csvc_decref_pci(const struct pfl_callerinfo *pci,
    struct slashrpc_cservice *csvc)
{
	int rc;

	sl_csvc_reqlock(csvc);
	rc = psc_atomic32_dec_getnew(&csvc->csvc_refcnt);
	psc_assert(rc >= 0);
	DEBUG_CSVC(PLL_INFO, csvc, "decref");
	if (rc == 0) {
		if (psc_atomic32_read(&csvc->csvc_flags) & CSVCF_WANTFREE) {
			/*
			 * This should only apply to mount_slash clients
			 * the MDS stops communication with.
			 */
			pscrpc_import_put(csvc->csvc_import);
			if (csvc->csvc_ctype == SLCONNT_CLI)
				pll_remove(&client_csvcs, csvc);
			DEBUG_CSVC(PLL_INFO, csvc, "freed");
			PSCFREE(csvc->csvc_lockinfo.lm_ptr);
			PSCFREE(csvc);
			return;
		}
	}
	sl_csvc_unlock(csvc);
}

/**
 * sl_csvc_incref - Account for starting to use a remote service
 *	connection.
 * @csvc: client service.
 */
void
sl_csvc_incref(struct slashrpc_cservice *csvc)
{
	sl_csvc_lock_ensure(csvc);
	psc_atomic32_inc(&csvc->csvc_refcnt);
	DEBUG_CSVC(PLL_DEBUG, csvc, "incref");
}

/**
 * sl_csvc_disconnect - Perform actual network disconnect to a remote
 *	service.
 * @csvc: client service.
 */
void
sl_csvc_disconnect_pci(const struct pfl_callerinfo *pci,
    struct slashrpc_cservice *csvc)
{
	int locked;

	locked = sl_csvc_reqlock(csvc);
	psc_atomic32_clearmask(&csvc->csvc_flags, CSVCF_CONNECTED);
	csvc->csvc_lasterrno = 0;
	sl_csvc_wake(csvc);
	sl_csvc_ureqlock(csvc, locked);

	pscrpc_abort_inflight(csvc->csvc_import);
}

void
sl_imp_hldrop_cli(void *csvc)
{
	sl_csvc_markfree(csvc);
	sl_csvc_disconnect(csvc);
	sl_csvc_decref(csvc);
}

void
sl_imp_hldrop_resm(void *arg)
{
	struct sl_resm *resm = arg;

	sl_csvc_disconnect(resm->resm_csvc);
}

/**
 * sl_csvc_disable - Mark a connection as no longer available.
 * @csvc: client service.
 */
void
sl_csvc_disable_pci(const struct pfl_callerinfo *pci,
    struct slashrpc_cservice *csvc)
{
	int locked;

	locked = sl_csvc_reqlock(csvc);
	psc_atomic32_setmask(&csvc->csvc_flags, CSVCF_ABANDON);
	psc_atomic32_clearmask(&csvc->csvc_flags, CSVCF_CONNECTED |
	    CSVCF_CONNECTING);
	csvc->csvc_lasterrno = 0;
	sl_csvc_wake(csvc);
	sl_csvc_ureqlock(csvc, locked);
}

/**
 * sl_csvc_create - Create a new client RPC service.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 */
__static struct slashrpc_cservice *
sl_csvc_create(uint32_t rqptl, uint32_t rpptl)
{
	struct slashrpc_cservice *csvc;
	struct pscrpc_import *imp;

	csvc = PSCALLOC(sizeof(*csvc));
	INIT_PSC_LISTENTRY(&csvc->csvc_lentry);

	if ((imp = pscrpc_new_import()) == NULL)
		psc_fatalx("pscrpc_new_import");
	csvc->csvc_import = imp;

	imp->imp_client->cli_request_portal = rqptl;
	imp->imp_client->cli_reply_portal = rpptl;
	imp->imp_max_retries = 2;
	//imp->imp_igntimeout = 1;	/* XXX only if archiver */
	imp->imp_igntimeout = 0;
	return (csvc);
}

/**
 * sl_csvc_get - Acquire or create a client RPC service.
 * @csvcp: value-result permanent storage for connection structures.
 * @flags: CSVCF_* flags the connection should take on, only used for
 *	csvc initialization.
 * @exp: RPC peer export.  This or @peernid is required.
 * @peernid: RPC peer network address (NID).  This or @exp is required.
 * @rqptl: request portal ID.
 * @rpptl: reply portal ID.
 * @magic: connection magic bits.
 * @version: version of application protocol.
 * @lockp: point to lock for mutually exclusive access to critical
 *	sections involving this connection structure, whereever @csvcp
 *	is stored.
 * @waitinfo: waitq or multiwaitcond argument to wait/wakeup depending
 *	on connection availability.
 * @ctype: peer type.
 * @arg: user data.
 *
 * If we acquire a connection successfully, this function will return
 * the same slashrpc_cservice struct pointer as referred to by its
 * first argument csvcp.  Otherwise, it returns NULL, but the structure
 * is left in the location referred to by csvcp for retry.
 */
struct slashrpc_cservice *
sl_csvc_get_pci(const struct pfl_callerinfo *pci,
    struct slashrpc_cservice **csvcp, int flags,
    struct pscrpc_export *exp, lnet_nid_t peernid, uint32_t rqptl,
    uint32_t rpptl, uint64_t magic, uint32_t version, void *lockp,
    void *waitinfo, enum slconn_type ctype, void *arg)
{
	struct slashrpc_cservice *csvc;
	struct sl_resm *resm;
	union lockmutex lm;
	int rc = 0, locked;

	lm.lm_ptr = lockp;

	if (flags & CSVCF_USE_MULTIWAIT)
		locked = psc_mutex_reqlock(lm.lm_mutex);
	else
		locked = reqlock(lm.lm_lock);
	if (exp)
		peernid = exp->exp_connection->c_peer.nid;
	psc_assert(peernid != LNET_NID_ANY);

	csvc = *csvcp;
	if (csvc == NULL) {
		/* initialize service */
		csvc = *csvcp = sl_csvc_create(rqptl, rpptl);
		psc_atomic32_set(&csvc->csvc_flags, flags);
		csvc->csvc_lockinfo.lm_ptr = lockp;
		csvc->csvc_waitinfo = waitinfo;
		csvc->csvc_ctype = ctype;
		csvc->csvc_version = version;
		csvc->csvc_magic = magic;

		/* ensure that our peer is of the given resource type */
		switch (ctype) {
		case SLCONNT_CLI:
			csvc->csvc_import->imp_hldropf = sl_imp_hldrop_cli;
			csvc->csvc_import->imp_hldrop_arg = csvc;
			sl_csvc_incref(csvc);
			break;
		case SLCONNT_IOD:
			resm = libsl_nid2resm(peernid);
			if (resm->resm_res->res_type == SLREST_MDS)
				psc_fatalx("csvc requested type is IOD "
				    "but resource is MDS");
			csvc->csvc_import->imp_hldropf = sl_imp_hldrop_resm;
			csvc->csvc_import->imp_hldrop_arg = resm;
			break;
		case SLCONNT_MDS:
			resm = libsl_nid2resm(peernid);
			if (resm->resm_res->res_type != SLREST_MDS)
				psc_fatalx("csvc requested type is MDS "
				    "but resource is IOD");
			csvc->csvc_import->imp_hldropf = sl_imp_hldrop_resm;
			csvc->csvc_import->imp_hldrop_arg = resm;
			break;
		default:
			psc_fatalx("%d: bad connection type", ctype);
		}

		if (ctype == SLCONNT_CLI)
			pll_add(&client_csvcs, csvc);
	}

 restart:
	if (sl_csvc_useable(csvc))
		goto out;

	if (exp) {
		struct pscrpc_connection *c;

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
		csvc->csvc_import->imp_connection = exp->exp_connection;
		csvc->csvc_import->imp_connection->c_imp = csvc->csvc_import;

		if (c)
			pscrpc_put_connection(c);

		csvc->csvc_mtime = time(NULL);

	} else if (psc_atomic32_read(&csvc->csvc_flags) & CSVCF_CONNECTING) {

		if (flags & CSVCF_NONBLOCK) {
			csvc = NULL;
			goto out;
		}

		if (sl_csvc_usemultiwait(csvc))
			psc_multiwaitcond_wait(csvc->csvc_waitinfo,
			    csvc->csvc_mutex);
		else
			psc_waitq_wait(csvc->csvc_waitinfo,
			    csvc->csvc_lock);
		sl_csvc_lock(csvc);
		goto restart;

	} else if (flags & CSVCF_NORECON) {

		rc = ENOTCONN;
		csvc = NULL;
		goto out;

	} else if (csvc->csvc_lasterrno == 0 ||
	    csvc->csvc_mtime + CSVC_RECONNECT_INTV < time(NULL)) {

		psc_atomic32_setmask(&csvc->csvc_flags,
		    CSVCF_CONNECTING);
		sl_csvc_unlock(csvc);

		rc = slrpc_issue_connect(peernid, csvc, flags);

		sl_csvc_lock(csvc);

		if (rc == EWOULDBLOCK) {
			csvc = NULL;
			goto out;
		}

		csvc->csvc_mtime = time(NULL); /* XXX use monotonic */
		psc_atomic32_clearmask(&csvc->csvc_flags,
		    CSVCF_CONNECTING);
		if (rc) {
			csvc->csvc_import->imp_failed = 1;
			csvc->csvc_lasterrno = rc;
			/*
			 * Leave the slashrpc_cservice structure in
			 * csvcp intact, while return NULL to signal
			 * that we fail to establish a connection.
			 */
			csvc = NULL;
		}
	} else {
		rc = csvc->csvc_lasterrno;
		csvc = NULL;
		goto out;
	}
	if (rc == 0)
		sl_csvc_online(csvc);
	sl_csvc_wake(*csvcp);

 out:
	if (csvc)
		sl_csvc_incref(csvc);
	else if ((flags & CSVCF_NONBLOCK) &&
	    sl_csvc_usemultiwait(*csvcp) && arg)
		psc_multiwait_addcond(arg,
		    (*csvcp)->csvc_waitinfo);
	sl_csvc_ureqlock(*csvcp, locked);
	return (csvc);
}

void
slconnthr_main(struct psc_thread *thr)
{
	struct slashrpc_cservice *csvc;
	struct slconn_thread *sct;
	struct sl_resm *resm;
	time_t now, dst;
	int rc;

	sct = thr->pscthr_private;
	resm = sct->sct_resm;
	if (sct->sct_flags & CSVCF_USE_MULTIWAIT)
		psc_mutex_lock(sct->sct_lockinfo.lm_mutex);
	else
		spinlock(sct->sct_lockinfo.lm_lock);
	do {
		if (sct->sct_flags & CSVCF_USE_MULTIWAIT)
			psc_mutex_unlock(sct->sct_lockinfo.lm_mutex);
		else
			freelock(sct->sct_lockinfo.lm_lock);

		/* Now just PING for connection lifetime. */
		for (;;) {
			csvc = sl_csvc_get(&resm->resm_csvc, sct->sct_flags,
			    NULL, resm->resm_nid, sct->sct_rqptl, sct->sct_rpptl,
			    sct->sct_magic, sct->sct_version,
			    sct->sct_lockinfo.lm_ptr, sct->sct_waitinfo,
			    sct->sct_peertype, NULL);

			if (csvc == NULL) {
				time_t mtime;

				sl_csvc_lock(resm->resm_csvc);
				csvc = resm->resm_csvc;
				if (sl_csvc_useable(csvc)) {
					sl_csvc_incref(csvc);
					goto online;
				}
				mtime = csvc->csvc_mtime;
				/*
				 * Allow manual activity to try to
				 * reconnect while we wait.
				 */
				csvc->csvc_mtime = 0;
				/*
				 * Subtract the amount of time someone
				 * manually retried (and failed) instead of
				 * waiting an entire interval after we woke
				 * after our last failed attempt.
				 */
				sl_csvc_waitrel_s(csvc,
				    CSVC_RECONNECT_INTV - (time(NULL) -
				    mtime));
				continue;
			}

			sl_csvc_lock(csvc);
			if (!sl_csvc_useable(csvc)) {
				sl_csvc_decref(csvc);
				break;
			}

 online:
			sl_csvc_unlock(csvc);
			rc = slrpc_issue_ping(csvc);
			/* XXX race */
			if (rc) {
				sl_csvc_lock(csvc);
				sl_csvc_disconnect(csvc);
			}
			sl_csvc_decref(csvc);

			if (rc)
				break;

			now = time(NULL);
			dst = now + 60;
			do {
				sl_csvc_lock(csvc);
				if (!sl_csvc_useable(csvc))
					dst = now;
				sl_csvc_waitrel_s(csvc, dst - now);
				now = time(NULL);
			} while (dst > now);
		}

		sl_csvc_lock(csvc);
	} while (pscthr_run() && (psc_atomic32_read(
	    &csvc->csvc_flags) & CSVCF_ABANDON) == 0);
	sl_csvc_decref(csvc);
}

void
slconnthr_spawn(struct sl_resm *resm, uint32_t rqptl, uint32_t rpptl,
    uint64_t magic, uint32_t version, void *lockp, int flags,
    void *waitinfo, enum slconn_type peertype, int thrtype,
    const char *thrnamepre)
{
	struct slconn_thread *sct;
	struct psc_thread *thr;

	thr = pscthr_init(thrtype, 0, slconnthr_main, NULL,
	    sizeof(*sct), "%sconnthr-%s", thrnamepre,
	    resm->resm_res->res_name);
	sct = thr->pscthr_private;
	sct->sct_resm = resm;
	sct->sct_rqptl = rqptl;
	sct->sct_rpptl = rpptl;
	sct->sct_magic = magic;
	sct->sct_version = version;
	sct->sct_lockinfo.lm_ptr = lockp;
	sct->sct_flags = flags;
	sct->sct_waitinfo = waitinfo;
	sct->sct_peertype = peertype;
	pscthr_setready(thr);
}

/**
 * sl_exp_hldrop_resm - Callback triggered when an export to a resource
 *	member fails.
 * @exp: export to RPC peer.
 */
void
sl_exp_hldrop_resm(struct pscrpc_export *exp)
{
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	struct sl_resm *resm;

	resm = libsl_nid2resm(exp->exp_connection->c_peer.nid);
	if (resm) {
		sl_csvc_disconnect(resm->resm_csvc);
		sl_resm_hldrop(resm);
	} else {
		pscrpc_nid2str(exp->exp_connection->c_peer.nid, nidbuf);
		psclog_warnx("no resm for %s", nidbuf);
	}
}

/**
 * sl_exp_hldrop_cli - Callback triggered when an export to a CLIENT fails.
 * @exp: export to RPC CLI peer.
 */
void
sl_exp_hldrop_cli(struct pscrpc_export *exp)
{
	struct slashrpc_cservice **csvcp = exp->exp_private;

	if (csvcp == NULL)
		return;

	if (sl_expcli_ops.secop_destroy)
		sl_expcli_ops.secop_destroy(exp->exp_private);
	sl_csvc_reqlock(*csvcp);
	sl_csvc_markfree(*csvcp);
	sl_csvc_disconnect(*csvcp);
	sl_csvc_decref(*csvcp);
	PSCFREE(exp->exp_private);
}

/**
 * sl_exp_getpri_cli - Get pscrpc_export private data specific to CLIENT
 *	peer.
 * @exp: RPC export to CLI peer.
 */
void *
sl_exp_getpri_cli(struct pscrpc_export *exp)
{
	int locked;
	void *p;

	locked = EXPORT_RLOCK(exp);
	if (exp->exp_private == NULL) {
		sl_expcli_ops.secop_allocpri(exp);
		exp->exp_hldropf = sl_exp_hldrop_cli;
	}
	p = exp->exp_private;
	EXPORT_URLOCK(exp, locked);
	return (p);
}
