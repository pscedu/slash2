/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/str.h"
#include "pfl/tree.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"

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
slm_rpc_ion_pack_bmapminseq(struct pscrpc_msg *m)
{
	struct srt_bmapminseq *sbms;

	if (m == NULL) {
		psclog_errorx("unable to export bmapminseq");
		return;
	}
	sbms = pscrpc_msg_buf(m, m->bufcount - 2, sizeof(*sbms));
	if (sbms == NULL) {
		psclog_errorx("unable to export bmapminseq");
		return;
	}
	mds_bmap_getcurseq(NULL, &sbms->bminseq);
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

	if (csvc->csvc_peertype == SLCONNT_IOD) {
		int qlens[] = {
			qlen,
			sizeof(struct srt_bmapminseq),
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
slrpc_req_out(struct slashrpc_cservice *csvc, struct pscrpc_request *rq)
{
#if 0
	if (rq->rq_reqmsg->opc == SRMT_CONNECT) {
		struct srm_connect_req *mq = *(void **)mqp;

		mq->fsuuid = zfsMount[current_vfsid].uuid;
	}
#endif
	if (csvc->csvc_peertype == SLCONNT_IOD)
		slm_rpc_ion_pack_bmapminseq(rq->rq_reqmsg);
}

void
slrpc_rep_in(struct slashrpc_cservice *csvc, struct pscrpc_request *rq)
{
	if (csvc->csvc_peertype == SLCONNT_IOD)
		slm_rpc_ion_unpack_statfs(rq, PSCRPC_MSG_REPLY);
}

void
slrpc_req_in(struct pscrpc_request *rq)
{
	if (rq->rq_rqbd->rqbd_service == slm_rmi_svc.svh_service) {
		slm_rpc_ion_unpack_statfs(rq, PSCRPC_MSG_REQUEST);
		slm_rpc_ion_pack_bmapminseq(rq->rq_repmsg);
	}
}

int
slrpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	int rc;

	if (rq->rq_rqbd->rqbd_service == slm_rmi_svc.svh_service) {
		int plens[] = {
			plen,
			sizeof(struct srt_bmapminseq),
			sizeof(struct srt_authbuf_footer)
		};

		rc = slrpc_allocrepn(rq, mqp, qlen, mpp, nitems(plens),
		    plens, rcoff);
	} else
		rc = slrpc_allocgenrep(rq, mqp, qlen, mpp, plen, rcoff);
	if (rc == 0 && rq->rq_reqmsg->opc == SRMT_CONNECT) {
		struct srm_connect_rep *mp = *(void **)mpp;

		mp->fsuuid = zfsMount[current_vfsid].uuid;
	}
	return (rc);
}
