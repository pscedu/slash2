/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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

#define PSC_SUBSYS PSS_RPC

#include <stdio.h>

#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"
#include "pfl/tree.h"
#include "pfl/workthr.h"

#include "bmap_mds.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

#include "zfs-fuse/zfs_slashlib.h"

struct pscrpc_svc_handle slm_rmi_svc;
struct pscrpc_svc_handle slm_rmm_svc;
struct pscrpc_svc_handle slm_rmc_svc;

struct psc_poolmaster	 batchrq_pool_master;
struct psc_poolmgr	*batchrq_pool;
struct psc_spinlock	 batchrqs_lock = SPINLOCK_INIT;
struct psc_listcache	 batchrqs_delayed;	/* waiting to be filled/timeout to be sent */
struct psc_listcache	 batchrqs_waitreply;	/* awaiting reply */

#define BATCHMGR_LOCK()		spinlock(&batchrqs_lock)
#define BATCHMGR_ULOCK()	freelock(&batchrqs_lock)

void
slm_rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh;
	struct slmrcm_thread *srcm;
	struct psc_thread *thr;

	/* Setup request service for MDS from ION. */
	svh = &slm_rmi_svc;
	svh->svh_nbufs = SLM_RMI_NBUFS;
	svh->svh_bufsz = SLM_RMI_BUFSZ;
	svh->svh_reqsz = SLM_RMI_BUFSZ;
	svh->svh_repsz = SLM_RMI_REPSZ;
	svh->svh_req_portal = SRMI_REQ_PORTAL;
	svh->svh_rep_portal = SRMI_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMI;
	svh->svh_nthreads = SLM_RMI_NTHREADS;
	svh->svh_handler = slm_rmi_handler;
	strlcpy(svh->svh_svc_name, SLM_RMI_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmi_thread);

	/* Setup request service for MDS from MDS. */
	svh = &slm_rmm_svc;
	svh->svh_nbufs = SLM_RMM_NBUFS;
	svh->svh_bufsz = SLM_RMM_BUFSZ;
	svh->svh_reqsz = SLM_RMM_BUFSZ;
	svh->svh_repsz = SLM_RMM_REPSZ;
	svh->svh_req_portal = SRMM_REQ_PORTAL;
	svh->svh_rep_portal = SRMM_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMM;
	svh->svh_nthreads = SLM_RMM_NTHREADS;
	svh->svh_handler = slm_rmm_handler;
	strlcpy(svh->svh_svc_name, SLM_RMM_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmm_thread);

	/* Setup request service for MDS from client. */
	svh = &slm_rmc_svc;
	svh->svh_nbufs = SLM_RMC_NBUFS;
	svh->svh_bufsz = SLM_RMC_BUFSZ;
	svh->svh_reqsz = SLM_RMC_BUFSZ;
	svh->svh_repsz = SLM_RMC_REPSZ;
	svh->svh_req_portal = SRMC_REQ_PORTAL;
	svh->svh_rep_portal = SRMC_REP_PORTAL;
	svh->svh_type = SLMTHRT_RMC;
	svh->svh_nthreads = SLM_RMC_NTHREADS;
	svh->svh_handler = slm_rmc_handler;
	strlcpy(svh->svh_svc_name, SLM_RMC_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slmrmc_thread);

	thr = pscthr_init(SLMTHRT_RCM, slmrcmthr_main, NULL,
	    sizeof(*srcm), "slmrcmthr");
	srcm = thr->pscthr_private;
	srcm->srcm_page = PSCALLOC(SRM_REPLST_PAGESIZ);
	pscthr_setready(thr);
}

void
slm_rpc_ion_unpack_statfs(struct pscrpc_request *rq,
    struct pscrpc_msg *m, int idx)
{
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;
	struct rpmi_ios *si;
	struct {
		struct srt_statfs	f;
		struct srt_bwqueued	bwq;
	} *data;

	psc_assert(idx >= 0);
	data = pscrpc_msg_buf(m, idx, sizeof(*data));
	if (data == NULL) {
		DEBUG_REQ(PLL_ERROR, rq, "unable to import statfs");
		return;
	}
	resm = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	if (resm == NULL) {
		psclog_errorx("unknown peer");
		return;
	}
	if (data->f.sf_frsize == 0) {
		DEBUG_REQ(PLL_MAX, rq, "%s sent bogus STATFS",
		    resm->resm_name);
		return;
	}
	rpmi = res2rpmi(resm->resm_res);
	si = rpmi->rpmi_info;
	RPMI_LOCK(rpmi);
	si->si_ssfb = data->f;

	si->si_bw_ingress.bwd_inflight = 0;
	si->si_bw_egress.bwd_inflight = 0;
	si->si_bw_aggr.bwd_inflight = 0;
	si->si_bw_ingress.bwd_queued = data->bwq.sbq_ingress;
	si->si_bw_egress.bwd_queued = data->bwq.sbq_egress;
	si->si_bw_aggr.bwd_queued = data->bwq.sbq_aggr;
	RPMI_ULOCK(rpmi);
}

int
slm_rpc_newreq(struct slashrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	if (csvc->csvc_peertype == SLCONNT_IOD) {
		int flags = 0, np = 1, plens[3] = { plen };
		int qlens[] = {
			qlen,
			sizeof(struct srt_authbuf_footer)
		};
		struct resprof_mds_info *rpmi;
		struct sl_resm *resm;
		struct rpmi_ios *si;
		struct timespec now;
		int rc;

		PFL_GETTIMESPEC(&now);

		resm = csvc->csvc_import->imp_hldrop_arg; /* XXX hack */
		rpmi = res2rpmi(resm->resm_res);
		si = rpmi->rpmi_info;
		if (timespeccmp(&now, &si->si_ssfb_send, >)) {
			plens[np++] = sizeof(struct srt_statfs) +
			    sizeof(struct srt_bwqueued);
			flags |= SLRPC_MSGF_STATFS;
			RPMI_LOCK(rpmi);
			si->si_ssfb_send = now;
			si->si_ssfb_send.tv_sec += 1;
			RPMI_ULOCK(rpmi);
		}
		if (np > 1) {
			plens[np++] = sizeof(struct srt_authbuf_footer);
			rc = RSX_NEWREQN(csvc->csvc_import,
			    csvc->csvc_version, op, *rqp, nitems(qlens),
			    qlens, nitems(plens), plens, *(void **)mqp);
			if (rc == 0)
				(*rqp)->rq_reqmsg->flags |= flags;
			return (rc);
		}
	}
	return (slrpc_newgenreq(csvc, op, rqp, qlen, plen, mqp));
}

void
slm_rpc_req_out(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct pscrpc_msg *m = rq->rq_reqmsg;

	if (m->opc == SRMT_CONNECT) {
		struct srm_connect_req *mq;

		if (m->bufcount < 1) {
			DEBUG_REQ(PLL_ERROR, rq, "unable to export fsuuid");
			return;
		}
		mq = pscrpc_msg_buf(m, 0, sizeof(*mq));
		if (mq == NULL) {
			DEBUG_REQ(PLL_ERROR, rq, "unable to export fsuuid");
			return;
		}

		mq->fsuuid = zfs_mounts[current_vfsid].zm_uuid;
	}
}

void
slm_rpc_req_in(struct pscrpc_request *rq)
{
	struct pscrpc_msg *m = rq->rq_reqmsg;

	if (rq->rq_rqbd->rqbd_service == slm_rmi_svc.svh_service && m) {
		int idx = 1;

		if (m->flags & SLRPC_MSGF_STATFS)
			slm_rpc_ion_unpack_statfs(rq, m, idx++);
	}
}

void
slm_rpc_rep_in(struct slashrpc_cservice *csvc, struct pscrpc_request *rq)
{
	struct pscrpc_msg *m = rq->rq_repmsg;

	if (csvc->csvc_peertype == SLCONNT_IOD && m) {
		int idx = 1;

		if (m->flags & SLRPC_MSGF_STATFS)
			slm_rpc_ion_unpack_statfs(rq, m, idx++);
	}
}

int
slm_rpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen0,
    void *mpp, int plen, int rcoff)
{
	int rc;

	rc = slrpc_allocgenrep(rq, mqp, qlen0, mpp, plen, rcoff);
	if (rc == 0 && rq->rq_reqmsg->opc == SRMT_CONNECT) {
		struct srm_connect_rep *mp = *(void **)mpp;

		mp->fsuuid = zfs_mounts[current_vfsid].zm_uuid;
	}
	return (rc);
}

int
batchrq_cmp(const void *a, const void *b)
{
	const struct batchrq *pa = a, *pb = b;

	return (timercmp(&pa->br_expire, &pb->br_expire, <));
}

void
batchrq_decref(struct batchrq *br, int rc)
{
	int finish = 0;

	if (br->br_rc == 0)
		br->br_rc = rc;

	PFLOG_BATCHRPC(PLL_DIAG, br, "decref");

	psc_assert(br->br_refcnt > 0);
	if (--br->br_refcnt == 0) {
		struct psc_listcache *l;

		finish = 1;
		if (br->br_flags & (BATCHF_WAITREPLY |
		    BATCHF_RQINFL))
			lc_remove(&batchrqs_waitreply, br);
		else
			lc_remove(&batchrqs_delayed, br);
		l = batchrq_2_lc(br);
		lc_remove(l, br);
	}

	BATCHMGR_ULOCK();

	if (finish) {
		pscrpc_req_finished(br->br_rq);
		sl_csvc_decref(br->br_csvc);

		br->br_cbf(br, br->br_rc);

		PSCFREE(br->br_buf);
		PSCFREE(br->br_reply);
		psc_dynarray_free(&br->br_scratch);

		psc_pool_return(batchrq_pool, br);
	}
}

int
batchrq_finish_wkcb(void *p)
{
	struct slm_wkdata_batchrq_cb *wk = p;
	struct batchrq *br = wk->br;

	BATCHMGR_LOCK();
	batchrq_decref(br, wk->rc);
	return (0);
}

void
batchrq_sched_finish(struct batchrq *br, int rc)
{
	int locked = 0;
	struct psc_listcache *lc;
	struct slm_wkdata_batchrq_cb *wk;

	lc = &batchrqs_waitreply;
	locked = LIST_CACHE_RLOCK(lc);
	if (br->br_flags & BATCHF_CLEANUP) {
		LIST_CACHE_URLOCK(lc, locked);
		return;
	}

	br->br_flags |= BATCHF_CLEANUP;
	LIST_CACHE_URLOCK(lc, locked);

	wk = pfl_workq_getitem(batchrq_finish_wkcb,
	    struct slm_wkdata_batchrq_cb);

	wk->br = br;
	wk->rc = rc;
	pfl_workq_putitemq(&slm_db_lopri_workq, wk);
}

int
batchrq_waitreply_wkcb(void *p)
{
	struct slm_wkdata_batchrq_cb *wk = p;
	struct batchrq *br = wk->br;

	BATCHMGR_LOCK();
	br->br_flags &= ~BATCHF_RQINFL;
	br->br_flags |= BATCHF_WAITREPLY;
	batchrq_decref(br, 0);
	return (0);
}

void
batchrq_sched_waitreply(struct batchrq *br)
{
	struct slm_wkdata_batchrq_cb *wk;

	wk = pfl_workq_getitem(batchrq_waitreply_wkcb,
	    struct slm_wkdata_batchrq_cb);
	wk->br = br;
	pfl_workq_putitemq(&slm_db_lopri_workq, wk);
}

int
sl_batchrqsend_cb(struct pscrpc_request *rq, struct pscrpc_async_args *av)
{
	struct batchrq *br = av->pointer_arg[0];
	int rc;

	SL_GET_RQ_STATUS_TYPE(br->br_csvc, rq, struct srm_batch_rep,
	    rc);

	if (rc)
		batchrq_sched_finish(br, rc);
	else
		batchrq_sched_waitreply(br);
	return (0);
}

void
batchrq_send(struct batchrq *br)
{
	struct pscrpc_request *rq;
	struct psc_listcache *ml;
	struct iovec iov;
	int rc;

	ml = &batchrqs_delayed;

	/*
	 * The following list lock is the same as BATCHMGR_LOCK() due to
	 * the way list lock is set up.
	 */
	LIST_CACHE_LOCK_ENSURE(ml);

	rq = br->br_rq;

	br->br_refcnt++;
	br->br_flags |= BATCHF_RQINFL;
	br->br_rq = NULL;

	lc_remove(ml, br);

	lc_add(&batchrqs_waitreply, br);

	PFLOG_BATCHRPC(PLL_DIAG, br, "sending");

	iov.iov_base = br->br_buf;
	iov.iov_len = br->br_len;
	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, br->br_snd_ptl, &iov,
	    1);

	if (!rc) {
		rq->rq_interpret_reply = sl_batchrqsend_cb;
		rq->rq_async_args.pointer_arg[0] = br;
		rc = SL_NBRQSET_ADD(br->br_csvc, rq);
	}
	if (rc) {
		/*
		 * If we failed, check again to see if the connection
		 * has been reestablished since there can be delay in
		 * using this API.
		 */

		br->br_refcnt--;
		br->br_rq = rq;
		br->br_flags &= ~BATCHF_RQINFL;
		batchrq_decref(br, rc);
		BATCHMGR_LOCK();
	}
}

/*
 * Handle a BATCHRP (i.e. a reply to a BATCHRQ) that arrives after a
 * recipient of a BATCHRQ is done processing the contents and sends us a
 * response indicating success/failure.
 */
int
sl_handle_batchrp(struct pscrpc_request *rq)
{
	struct psc_listcache *lc;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct batchrq *br, *brn;
	struct iovec iov;

	memset(&iov, 0, sizeof(iov));

	lc = &batchrqs_waitreply;
	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->len < 0 || mq->len > LNET_MTU) {
		mp->rc = -EINVAL;
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	} else
		iov.iov_base = PSCALLOC(mq->len);

	LIST_CACHE_LOCK(lc);
	LIST_CACHE_FOREACH_SAFE(br, brn, lc)
		if (mq->bid == br->br_bid) {
			if (!mp->rc) {
				br->br_reply = iov.iov_base;
				iov.iov_len = br->br_replen = mq->len;
				mp->rc = slrpc_bulkserver(rq,
				    BULK_GET_SINK, br->br_rcv_ptl,
				    &iov, 1);
				iov.iov_base = NULL;
			}

			batchrq_sched_finish(br, mq->rc ? mq->rc :
			    mp->rc);

			break;
		}
	LIST_CACHE_ULOCK(lc);
	if (br == NULL)
		mp->rc = -EINVAL;
	PSCFREE(iov.iov_base);
	return (mp->rc);
}

int
batchrq_add(struct sl_resource *r, struct slashrpc_cservice *csvc,
    uint32_t opc, int rcvptl, int sndptl, void *buf, size_t len,
    void *scratch, void (*cbf)(struct batchrq *, int), int expire)
{
	static psc_atomic64_t bid = PSC_ATOMIC64_INIT(0);
	struct psc_listcache *l, *ml;
	struct pscrpc_request *rq;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct batchrq *br;
	int rc = 0;

	BATCHMGR_LOCK();

	ml = &batchrqs_delayed;

	l = &res2rpmi(r)->rpmi_batchrqs;
	LIST_CACHE_FOREACH(br, l)
		if ((br->br_flags & (BATCHF_RQINFL |
		    BATCHF_WAITREPLY)) == 0 &&
		    opc == br->br_rq->rq_reqmsg->opc) {
			/*
			 * Tack this request onto the pending batch set.
			 */
			sl_csvc_decref(csvc);
			mq = pscrpc_msg_buf(br->br_rq->rq_reqmsg, 0,
			    sizeof(*mq));
			goto add;
		}

	/* not found; create */

	rc = SL_RSX_NEWREQ(csvc, SRMT_BATCH_RQ, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->opc = opc;
	mq->bid = psc_atomic64_inc_getnew(&bid);

	br = psc_pool_get(batchrq_pool);
	memset(br, 0, sizeof(*br));
	psc_dynarray_init(&br->br_scratch);
	INIT_PSC_LISTENTRY(&br->br_lentry);
	INIT_PSC_LISTENTRY(&br->br_lentry_ml);
	br->br_rq = rq;
	br->br_rcv_ptl = rcvptl;
	br->br_snd_ptl = sndptl;
	br->br_csvc = csvc;
	br->br_bid = mq->bid;
	br->br_res = r;
	br->br_cbf = cbf;
	br->br_buf = PSCALLOC(LNET_MTU);
	br->br_refcnt = 1;

	PFL_GETTIMEVAL(&br->br_expire);
	br->br_expire.tv_sec += expire;

	PFLOG_BATCHRPC(PLL_DIAG, br, "created");

	lc_add(l, br);
	lc_add_sorted(ml, br, batchrq_cmp);

 add:
	memcpy(br->br_buf + br->br_len, buf, len);
	br->br_len += len;
	mq->len += len;
	if (scratch)
		psc_dynarray_add(&br->br_scratch, scratch);

	/*
	 * OK, the requested entry has been added.  If the next
	 * batch_add() would overflow, send out what we have now.
	 */
	if (br->br_len + len > LNET_MTU)
		batchrq_send(br);

	csvc = NULL;

 out:
	BATCHMGR_ULOCK();
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
slmbchrqthr_main(struct psc_thread *thr)
{
	struct timeval now, stall;
	struct psc_listcache *ml;
	struct batchrq *br;

	ml = &batchrqs_delayed;

	stall.tv_sec = 0;
	stall.tv_usec = 0;

	while (pscthr_run(thr)) {
		LIST_CACHE_LOCK(ml);
		br = lc_peekheadwait(ml);
		PFL_GETTIMEVAL(&now);
		if (timercmp(&now, &br->br_expire, >))
			batchrq_send(br);
		else
			timersub(&br->br_expire, &now, &stall);
		LIST_CACHE_ULOCK(ml);

		usleep(stall.tv_sec * 1000000 + stall.tv_usec);

		stall.tv_sec = 0;
		stall.tv_usec = 0;
	}
}

void
slmbchrqthr_spawn(void)
{
	psc_poolmaster_init(&batchrq_pool_master,
	    struct batchrq, br_lentry_ml, PPMF_AUTO, 8, 8, 0,
	    NULL, NULL, NULL, "bchrq");
	batchrq_pool = psc_poolmaster_getmgr(&batchrq_pool_master);

	lc_reginit(&batchrqs_delayed, struct batchrq, br_lentry_ml,
	    "bchrqdelay");
	batchrqs_delayed.plc_flags |= PLLF_EXTLOCK;
	batchrqs_delayed.plc_lockp = &batchrqs_lock;

	lc_reginit(&batchrqs_waitreply, struct batchrq, br_lentry_ml,
	    "bchrqwait");
	batchrqs_waitreply.plc_flags |= PLLF_EXTLOCK;
	batchrqs_waitreply.plc_lockp = &batchrqs_lock;

	pscthr_init(SLMTHRT_BATCHRQ, slmbchrqthr_main, NULL, 0,
	    "slmbchrqthr");
}

void
sl_resm_hldrop(struct sl_resm *resm)
{
	if (resm->resm_type == SLREST_MDS) {
	} else {
		struct psc_listcache *l;
		struct batchrq *br;

		upschq_resm(resm, UPDT_HLDROP);

		l = &res2rpmi(resm->resm_res)->rpmi_batchrqs;
		LIST_CACHE_LOCK(l);
		LIST_CACHE_FOREACH(br, l)
			batchrq_sched_finish(br, -ECONNRESET);
		LIST_CACHE_ULOCK(l);
	}
}

struct slrpc_ops slrpc_ops = {
	slm_rpc_newreq,
	slm_rpc_req_in,
	slm_rpc_req_out,
	NULL,
	slm_rpc_allocrep,
	slm_rpc_rep_in,
	NULL
};
