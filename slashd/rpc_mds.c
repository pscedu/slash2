/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
struct psc_listcache	 batchrqs_pndg;
struct psc_listcache	 batchrqs_wait;

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

	thr = pscthr_init(SLMTHRT_RCM, 0, slmrcmthr_main,
	    NULL, sizeof(*srcm), "slmrcmthr");
	srcm = thr->pscthr_private;
	srcm->srcm_page = PSCALLOC(SRM_REPLST_PAGESIZ);
	pscthr_setready(thr);
}

void
sl_resm_hldrop(struct sl_resm *resm)
{
	if (resm->resm_type == SLREST_MDS) {
	} else {
		sl_replica_t repl;

		repl.bs_id = resm->resm_res_id;
		slm_iosv_setbusy(&repl, 1);
		upschq_resm(resm, UPDT_HLDROP);
	}
}

void
slm_rpc_ion_unpack_statfs(struct pscrpc_request *rq, int type)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct srt_statfs *f;
	struct pscrpc_msg *m;
	struct sl_resm *resm;
	int locked;

	m = type == PSCRPC_MSG_REPLY ? rq->rq_repmsg : rq->rq_reqmsg;
	if (m == NULL) {
		DEBUG_REQ(PLL_ERROR, rq, "unable to import statfs");
		return;
	}
	if (m->bufcount < 2) {
		DEBUG_REQ(PLL_ERROR, rq, "unable to import statfs");
		return;
	}
	f = pscrpc_msg_buf(m, m->bufcount - 2, sizeof(*f));
	if (f == NULL) {
		DEBUG_REQ(PLL_ERROR, rq, "unable to import statfs");
		return;
	}
	resm = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	if (resm == NULL) {
		psclog_errorx("unknown peer");
		return;
	}
	if (f->sf_bsize == 0) {
		DEBUG_REQ(PLL_MAX, rq, "%s sent bogus STATFS",
		    resm->resm_name);
		return;
	}
	si = res2iosinfo(resm->resm_res);
	rpmi = res2rpmi(resm->resm_res);
	locked = RPMI_RLOCK(rpmi);
	memcpy(&si->si_ssfb, f, sizeof(*f));
	RPMI_URLOCK(rpmi, locked);
}

int
slrpc_newreq(struct slashrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	int rc;

	if (csvc->csvc_peertype == SLCONNT_IOD &&
	    op == SRMT_PING) {
		int qlens[] = {
			qlen,
			sizeof(struct srt_authbuf_footer)
		};
		int plens[] = {
			plen,
			sizeof(struct srt_statfs),
			sizeof(struct srt_authbuf_footer)
		};

		rc = RSX_NEWREQN(csvc->csvc_import, csvc->csvc_version,
		    op, *rqp, nitems(qlens), qlens, nitems(plens),
		    plens, *(void **)mqp);
	} else
		rc = slrpc_newgenreq(csvc, op, rqp, qlen, plen, mqp);
	return (rc);
}

void
slrpc_req_out(__unusedx struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	if (rq->rq_reqmsg->opc == SRMT_CONNECT) {
		struct srm_connect_req *mq;
		struct pscrpc_msg *m;

		m = rq->rq_reqmsg;
		if (m == NULL) {
			DEBUG_REQ(PLL_ERROR, rq, "unable to export fsuuid");
			return;
		}
		if (m->bufcount < 1) {
			DEBUG_REQ(PLL_ERROR, rq, "unable to export fsuuid");
			return;
		}
		mq = pscrpc_msg_buf(m, 0, sizeof(*mq));
		if (mq == NULL) {
			DEBUG_REQ(PLL_ERROR, rq, "unable to export fsuuid");
			return;
		}

		mq->fsuuid = zfsMount[current_vfsid].uuid;
	}
}

void
slrpc_rep_in(struct slashrpc_cservice *csvc, struct pscrpc_request *rq)
{
	if (csvc->csvc_peertype == SLCONNT_IOD &&
	    rq->rq_reqmsg->opc == SRMT_PING)
		slm_rpc_ion_unpack_statfs(rq, PSCRPC_MSG_REPLY);
}

void
slrpc_req_in(struct pscrpc_request *rq)
{
	if (rq->rq_rqbd->rqbd_service == slm_rmi_svc.svh_service &&
	    rq->rq_reqmsg->opc == SRMT_PING)
		slm_rpc_ion_unpack_statfs(rq, PSCRPC_MSG_REQUEST);
}

int
slrpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	int rc;

	if (rq->rq_rqbd->rqbd_service == slm_rmi_svc.svh_service) {
		int plens[] = {
			plen,
			sizeof(struct srt_authbuf_footer)
		};

		rc = slrpc_allocrepn(rq, mqp, qlen, mpp,
		    nitems(plens), plens, rcoff);
	} else
		rc = slrpc_allocgenrep(rq, mqp, qlen, mpp, plen, rcoff);
	if (rc == 0 && rq->rq_reqmsg->opc == SRMT_CONNECT) {
		struct srm_connect_rep *mp = *(void **)mpp;

		mp->fsuuid = zfsMount[current_vfsid].uuid;
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
batchrq_finish(struct batchrq *br, int rc)
{
	br->br_cbf(br, rc);
	if (br->br_rq)
		pscrpc_req_finished(br->br_rq);
	if (br->br_csvc)
		sl_csvc_decref(br->br_csvc);

	lc_remove(&batchrqs_wait, br);

	PSCFREE(br->br_buf);
	PSCFREE(br->br_reply);
	psc_dynarray_free(&br->br_scratch);

	psc_pool_return(batchrq_pool, br);
}

int
batchrq_handle_wkcb(void *p)
{
	struct slm_wkdata_batchrq_cb *wk = p;

	batchrq_finish(wk->br, wk->rc);
	return (0);
}

int
batchrq_send_cb(struct pscrpc_request *rq, struct pscrpc_async_args *av)
{
	struct batchrq *br = av->pointer_arg[0];
	struct slm_wkdata_batchrq_cb *wk;
	int rc;

	SL_GET_RQ_STATUS_TYPE(br->br_csvc, rq, struct srm_batch_rep,
	    rc);
	if (rc == 0)
		slrpc_rep_in(br->br_csvc, rq);
	/* nbrqset clears this for us */
	br->br_rq = NULL;
	if (rc) {
		wk = pfl_workq_getitem(batchrq_handle_wkcb,
		    struct slm_wkdata_batchrq_cb);
		wk->br = br;
		wk->rc = rc;
		pfl_workq_putitem(wk);
	}
	return (0);
}

void
batchrq_send(struct batchrq *br)
{
	struct psc_listcache *l, *ml;
	struct pscrpc_request *rq;
	struct iovec iov;
	int rc;

	ml = &batchrqs_pndg;
	l = batchrq_2_lc(br);

	LIST_CACHE_ENSURE_LOCKED(ml);

	lc_remove(l, br);
	lc_remove(ml, br);

	lc_add(&batchrqs_wait, br);

	rq = br->br_rq;

	iov.iov_base = br->br_buf;
	iov.iov_len = br->br_len;
	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, br->br_snd_ptl, &iov,
	    1);
	if (rc)
		goto err;

	rq->rq_interpret_reply = batchrq_send_cb;
	rq->rq_async_args.pointer_arg[0] = br;
	rc = SL_NBRQSET_ADD(br->br_csvc, rq);
	if (rc)
 err:
		batchrq_finish(br, rc);
}

int
batchrq_handle(struct pscrpc_request *rq)
{
	struct slm_wkdata_batchrq_cb *wk;
	struct psc_listcache *lc;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct batchrq *br, *brn;
	struct iovec iov;

	memset(&iov, 0, sizeof(iov));

	lc = &batchrqs_wait;
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
				slrpc_bulkclient(rq, BULK_GET_SINK,
				    br->br_rcv_ptl, &iov, 1);
			}

			wk = pfl_workq_getitem(batchrq_handle_wkcb,
			    struct slm_wkdata_batchrq_cb);
			wk->br = br;
			wk->rc = mq->rc;
			pfl_workq_putitem(wk);

			iov.iov_base = NULL;

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
    int opc, int rcvptl, int sndptl, void *buf, size_t len,
    void *scratch, void (*cbf)(struct batchrq *, int), int expire)
{
	static psc_atomic64_t bid = PSC_ATOMIC64_INIT(0);
	struct psc_listcache *l, *ml;
	struct pscrpc_request *rq;
	struct srm_batch_req *mq;
	struct srm_batch_rep *mp;
	struct batchrq *br;
	int rc = 0;

	ml = &batchrqs_pndg;
	LIST_CACHE_LOCK(ml);

	l = &res2rpmi(r)->rpmi_batchrqs;
	LIST_CACHE_LOCK(l);
	LIST_CACHE_FOREACH(br, l)
		if (br->br_rq->rq_reqmsg->opc) {
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

	PFL_GETTIMEVAL(&br->br_expire);
	br->br_expire.tv_sec += expire;

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
	LIST_CACHE_ULOCK(l);
	LIST_CACHE_ULOCK(ml);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
slmbchrqthr_main(struct psc_thread *thr)
{
	struct timeval now, wait;
	struct batchrq *br, *brn;
	struct psc_listcache *ml;

	ml = &batchrqs_pndg;

	wait.tv_sec = 0;
	wait.tv_usec = 0;

	while (pscthr_run(thr)) {
		usleep(wait.tv_sec * 1000000 + wait.tv_usec);

		wait.tv_sec = 1;
		wait.tv_usec = 0;

		PFL_GETTIMEVAL(&now);

		LIST_CACHE_LOCK(ml);
		LIST_CACHE_FOREACH_SAFE(br, brn, ml) {
			if (timercmp(&now, &br->br_expire, >)) {
				batchrq_send(br);
				continue;
			}
			timersub(&br->br_expire, &now, &wait);
			break;
		}
		LIST_CACHE_ULOCK(ml);
	}
}

void
slmbchrqthr_spawn(void)
{
	psc_poolmaster_init(&batchrq_pool_master,
	    struct batchrq, br_lentry_ml, PPMF_AUTO, 8, 8, 0,
	    NULL, NULL, NULL, "bchrq");
	batchrq_pool = psc_poolmaster_getmgr(&batchrq_pool_master);

	lc_reginit(&batchrqs_pndg, struct batchrq, br_lentry_ml,
	    "bchrqpndg");
	lc_reginit(&batchrqs_wait, struct batchrq, br_lentry_ml,
	    "bchrqwait");

	pscthr_init(SLMTHRT_BATCHRQ, 0, slmbchrqthr_main, NULL, 0,
	    "slmbchrqthr");
}
