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

#include "batchrpc.h"
#include "bmap_mds.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

#include "zfs-fuse/zfs_slashlib.h"

struct pscrpc_svc_handle slm_rmi_svc;
struct pscrpc_svc_handle slm_rmm_svc;
struct pscrpc_svc_handle slm_rmc_svc;

void
slm_rpc_initsvc(void)
{
	int i;
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

	for (i = 0; i < 2; i++) {
		thr = pscthr_init(SLMTHRT_RCM, slmrcmthr_main,
		    sizeof(*srcm), "slmrcmthr%d", i);
		srcm = thr->pscthr_private;
		srcm->srcm_page = PSCALLOC(SRM_REPLST_PAGESIZ);
		pscthr_setready(thr);
	}
}

void
slm_rpc_ion_unpack_statfs(struct pscrpc_request *rq,
    struct pscrpc_msg *m, int idx)
{
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;
	struct rpmi_ios *si;
	char buf[PSCRPC_NIDSTR_SIZE];
	struct {
		struct srt_statfs	f;
		struct srt_bwqueued	bwq;
	} *data;

	psc_assert(idx >= 0);
	data = pscrpc_msg_buf(m, idx, sizeof(*data));
	if (data == NULL) {
		DEBUG_REQ(PLL_ERROR, rq, buf, "unable to import statfs");
		return;
	}
	resm = libsl_nid2resm(pscrpc_req_getconn(rq)->c_peer.nid);
	if (resm == NULL) {
		psclog_errorx("unknown peer");
		return;
	}
	if (data->f.sf_frsize == 0) {
		DEBUG_REQ(PLL_MAX, rq, buf, "%s sent bogus STATFS",
		    resm->resm_name);
		return;
	}
	rpmi = res2rpmi(resm->resm_res);
	si = rpmi->rpmi_info;
	RPMI_LOCK(rpmi);
	si->si_ssfb = data->f;
	RPMI_ULOCK(rpmi);
}

int
slm_rpc_newreq(struct slrpc_cservice *csvc, int op,
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
slm_rpc_req_out(__unusedx struct slrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct pscrpc_msg *m = rq->rq_reqmsg;
	char buf[PSCRPC_NIDSTR_SIZE];

	if (m->opc == SRMT_CONNECT) {
		struct srm_connect_req *mq;

		if (m->bufcount < 1) {
			DEBUG_REQ(PLL_ERROR, rq, buf, "unable to export fsuuid");
			return;
		}
		mq = pscrpc_msg_buf(m, 0, sizeof(*mq));
		if (mq == NULL) {
			DEBUG_REQ(PLL_ERROR, rq, buf, "unable to export fsuuid");
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
slm_rpc_rep_in(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, int error)
{
	struct pscrpc_msg *m = rq->rq_repmsg;

	if (error)
		return;
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

void
slmbchrqthr_spawn(void)
{
	slrpc_batches_init(SLMTHRT_BATCHRPC, SL_SLMDS, "slm");
}

void
sl_resm_hldrop(struct sl_resm *resm)
{
	if (resm->resm_type == SLREST_MDS)
		psclog_warnx("Unexpected resource type, resm = %p", resm);
	else
		slrpc_batches_drop(resm->resm_res);
	sl_csvc_decref(resm->resm_csvc);
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
