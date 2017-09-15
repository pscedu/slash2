/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/str.h"
#include "pfl/tree.h"

#include "bmap_iod.h"
#include "mkfn.h"
#include "pathnames.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconn.h"
#include "sliod.h"

uint64_t		 sli_fsuuid;

struct pscrpc_svc_handle sli_ric_svc;
struct pscrpc_svc_handle sli_rii_svc;
struct pscrpc_svc_handle sli_rim_svc;

/*
 * Create and initialize RPC services.
 */
void
sli_rpc_initsvc(void)
{
	struct pscrpc_svc_handle *svh;

	/* Create server service to handle requests from clients. */
	svh = &sli_ric_svc;
	svh->svh_nbufs = SLI_RIC_NBUFS;
	svh->svh_bufsz = SLI_RIC_BUFSZ;
	svh->svh_reqsz = SLI_RIC_BUFSZ;
	svh->svh_repsz = SLI_RIC_REPSZ;
	svh->svh_req_portal = SRIC_REQ_PORTAL;
	svh->svh_rep_portal = SRIC_REP_PORTAL;
	svh->svh_type = SLITHRT_RIC;
	svh->svh_nthreads = SLI_RIC_NTHREADS;
	svh->svh_handler = sli_ric_handler;
	strlcpy(svh->svh_svc_name, SLI_RIC_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct sliric_thread);

	/* Create server service to handle requests from the MDS server. */
	svh = &sli_rim_svc;
	svh->svh_nbufs = SLI_RIM_NBUFS;
	svh->svh_bufsz = SLI_RIM_BUFSZ;
	svh->svh_reqsz = SLI_RIM_BUFSZ;
	svh->svh_repsz = SLI_RIM_REPSZ;
	svh->svh_req_portal = SRIM_REQ_PORTAL;
	svh->svh_rep_portal = SRIM_REP_PORTAL;
	svh->svh_type = SLITHRT_RIM;
	svh->svh_nthreads = SLI_RIM_NTHREADS;
	svh->svh_handler = sli_rim_handler;
	strlcpy(svh->svh_svc_name, SLI_RIM_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirim_thread);

	/* Create server service to handle requests from other I/O servers. */
	svh = &sli_rii_svc;
	svh->svh_nbufs = SLI_RII_NBUFS;
	svh->svh_bufsz = SLI_RII_BUFSZ;
	svh->svh_reqsz = SLI_RII_BUFSZ;
	svh->svh_repsz = SLI_RII_REPSZ;
	svh->svh_req_portal = SRII_REQ_PORTAL;
	svh->svh_rep_portal = SRII_REP_PORTAL;
	svh->svh_type = SLITHRT_RII;
	svh->svh_nthreads = SLI_RII_NTHREADS;
	svh->svh_handler = sli_rii_handler;
	strlcpy(svh->svh_svc_name, SLI_RII_SVCNAME,
	    sizeof(svh->svh_svc_name));
	pscrpc_thread_spawn(svh, struct slirii_thread);

	sli_rim_init();
}

void
sl_resm_hldrop(struct sl_resm *resm)
{
	psclog_warnx("drop resource %p, type = %d", resm, resm->resm_type);
	sl_csvc_decref(resm->resm_csvc);
}

int
sli_rci_ctl_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[0];
	int rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_ctl_rep, rc);
	sl_csvc_decref(csvc);
	return (0);
}

void
sli_rci_ctl_health_send(struct slrpc_cservice *csvc)
{
	struct pscrpc_request *rq;
	struct srt_ctlsetopt *c;
	struct srm_ctl_rep *mp;
	struct srm_ctl_req *mq;
	int rc;

	rc = SL_RSX_NEWREQ(csvc, SRMT_CTL, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return;
	}
	mq->opc = SRM_CTLOP_SETOPT;
	c = (void *)mq->buf;
	c->opt = SRMCTL_OPT_HEALTH;
	c->opv = sli_selftest_result;

	rq->rq_interpret_reply = sli_rci_ctl_cb;
	rq->rq_async_args.pointer_arg[0] = csvc;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
}

void
sli_rpc_mds_unpack_fsuuid(struct pscrpc_request *rq, int msgtype)
{
	struct srm_connect_rep *mp;
	struct srm_connect_req *mq;
	struct pscrpc_msg *m;
	uint64_t fsuuid;
	char buf[PSCRPC_NIDSTR_SIZE];

	if (rq->rq_status)
		return;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else
		m = rq->rq_repmsg;

	if (m == NULL)
		goto error;
	if (m->bufcount < 1)
		goto error;
	if (msgtype == PSCRPC_MSG_REQUEST) {
		mq = pscrpc_msg_buf(m, 0, sizeof(*mq));
		if (mq == NULL)
			goto error;
		fsuuid = mq->fsuuid;
	} else {
		mp = pscrpc_msg_buf(m, 0, sizeof(*mp));
		if (mp == NULL)
			goto error;
		fsuuid = mp->fsuuid;
	}
	if (!fsuuid) {
		DEBUG_REQ(PLL_WARN, rq, buf, "invalid zero fsuuid");
		return;
	}

	if (!sli_fsuuid) {
		char fn[PATH_MAX];
		struct stat stb;

		xmkfn(fn, "%s/%s/%"PRIx64"/%s",
		    slcfg_local->cfg_fsroot, SL_RPATH_META_DIR,
		    fsuuid, SL_RPATH_FIDNS_DIR);

		if (stat(fn, &stb))
			psc_fatal("stat %s", fn);
		if (!S_ISDIR(stb.st_mode)) {
			errno = ENOTDIR;
			psc_fatal("%s", fn);
		}

		globalConfig.gconf_fsuuid = sli_fsuuid = fsuuid;
	}

	if (globalConfig.gconf_fsuuid != fsuuid)
		psc_fatalx("mismatching UUIDs detected!  "
		    "gconf_fsuuid=%"PRIx64" mds_fsuuid=%"PRIx64,
		    globalConfig.gconf_fsuuid, fsuuid);

	return;

 error:
	psclog_errorx("no message; msg=%p opc=%d bufc=%d",
	    m, m ? (int)m->opc : -1, m ? (int)m->bufcount : -1);
}

void
sli_rpc_mds_pack_statfs(struct pscrpc_msg *m, int idx)
{
	struct {
		struct srt_statfs	f;
		struct srt_bwqueued	bwq;
	} *data;

	psc_assert(idx > 0 && (uint32_t)idx < m->bufcount);
	data = pscrpc_msg_buf(m, idx, sizeof(*data));
	if (data == NULL) {
		psclog_errorx("unable to pack statfs");
		return;
	}

	spinlock(&sli_ssfb_lock);
	data->f = sli_ssfb;
	freelock(&sli_ssfb_lock);

	spinlock(&sli_bwqueued_lock);
	data->bwq = sli_bwqueued;
	freelock(&sli_bwqueued_lock);

	/*
	 * XXX pack a generation number to prevent against stale data
	 * via out-of-order receptions.
	 */
}

int
sli_rpc_newreq(struct slrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	if (csvc->csvc_peertype == SLCONNT_MDS) {
		int rc, flags = 0, nq = 1, qlens[3] = { qlen };
		struct timespec now;

		PFL_GETTIMESPEC(&now);

		if (timespeccmp(&now, &sli_ssfb_send, >)) {
			/*
			 * slistatfsthr_main() wake up every 60
			 * seconds, so 30 seconds should be more
			 * than enough.
			 */
			qlens[nq++] = sizeof(struct srt_statfs) +
			    sizeof(struct srt_bwqueued);
			spinlock(&sli_ssfb_lock);
			sli_ssfb_send = now;
			sli_ssfb_send.tv_sec += 30;
			freelock(&sli_ssfb_lock);
			flags |= SLRPC_MSGF_STATFS;
		}
		if (nq > 1) {
			int plens[] = {
				plen,
				sizeof(struct srt_authbuf_footer)
			};

			qlens[nq++] = sizeof(struct srt_authbuf_footer);
			rc = RSX_NEWREQN(csvc->csvc_import,
			    csvc->csvc_version, op, *rqp, nq, qlens,
			    nitems(plens), plens, *(void **)mqp);
			if (rc == 0)
				(*rqp)->rq_reqmsg->flags |= flags;
			return (rc);
		}
	}
	return (slrpc_newgenreq(csvc, op, rqp, qlen, plen, mqp));
}

void
sli_rpc_req_out(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq)
{
	struct pscrpc_msg *m = rq->rq_reqmsg;

	if (csvc->csvc_peertype == SLCONNT_MDS && m) {
		int idx = 1;

		if (m->flags & SLRPC_MSGF_STATFS)
			sli_rpc_mds_pack_statfs(m, idx++);
	}
}

void
sli_rpc_req_in(struct pscrpc_request *rq)
{
	if (rq->rq_rqbd->rqbd_service == sli_rim_svc.svh_service)
		if (rq->rq_reqmsg->opc == SRMT_CONNECT)
			sli_rpc_mds_unpack_fsuuid(rq,
			    PSCRPC_MSG_REQUEST);
}

void
sli_rpc_rep_in(struct slrpc_cservice *csvc,
    struct pscrpc_request *rq, int error)
{
	if (error)
		return;
	if (csvc->csvc_peertype == SLCONNT_MDS)
		if (rq->rq_reqmsg->opc == SRMT_CONNECT)
			sli_rpc_mds_unpack_fsuuid(rq, PSCRPC_MSG_REPLY);
}

void
sli_rpc_rep_out(struct pscrpc_request *rq)
{
	struct pscrpc_msg *qm = rq->rq_reqmsg;
	struct pscrpc_msg *pm = rq->rq_repmsg;

	if (rq->rq_rqbd->rqbd_service == sli_rim_svc.svh_service) {
		int idx = 1;

		if (qm->flags & SLRPC_MSGF_STATFS)
			sli_rpc_mds_pack_statfs(pm, idx++);
	}
}

int
sli_rpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	if (rq->rq_rqbd->rqbd_service == sli_rim_svc.svh_service) {
		struct pscrpc_msg *qm = rq->rq_reqmsg;
		int np = 1, plens[3] = { plen };

		if (qm->flags & SLRPC_MSGF_STATFS)
			plens[np++] = sizeof(struct srt_statfs) +
			    sizeof(struct srt_bwqueued);
		if (np > 1) {
			plens[np++] = sizeof(struct srt_authbuf_footer);
			return (slrpc_allocrepn(rq, mqp, qlen, mpp, np,
			    plens, rcoff));
		}
	}
	return (slrpc_allocgenrep(rq, mqp, qlen, mpp, plen, rcoff));
}

struct slrpc_ops slrpc_ops = {
	sli_rpc_newreq,
	sli_rpc_req_in,
	sli_rpc_req_out,
	NULL,
	sli_rpc_allocrep,
	sli_rpc_rep_in,
	sli_rpc_rep_out
};
