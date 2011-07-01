/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/str.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

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

/**
 * sli_rpc_initsvc - create and initialize RPC services.
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
}

void
sl_resm_hldrop(__unusedx struct sl_resm *resm)
{
}

void
sli_rpc_mds_unpack_bminseq(struct pscrpc_request *rq, int msgtype)
{
	struct srt_bmapminseq *sbms;
	struct pscrpc_msg *m;

	if (rq->rq_status)
		return;

	if (msgtype == PSCRPC_MSG_REQUEST)
		m = rq->rq_reqmsg;
	else
		m = rq->rq_repmsg;
	if (m == NULL)
		goto error;
	if (m->bufcount < 3)
		goto error;
	sbms = pscrpc_msg_buf(m, m->bufcount - 2, sizeof(*sbms));
	if (sbms == NULL)
		goto error;
	bim_updateseq(sbms->bminseq);
	return;

 error:
	psclog_errorx("no message; msg=%p opc=%d bufc=%d",
	    m, m ? (int)m->opc : -1, m ? (int)m->bufcount : -1);
}

void
sli_rpc_mds_unpack_fsuuid(struct pscrpc_request *rq, int msgtype)
{
	struct srm_connect_rep *mp;
	struct srm_connect_req *mq;
	struct pscrpc_msg *m;
	uint64_t fsuuid;

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
		psclog_warnx("invalid zero fsuuid");
		return;
	}
	if (!sli_fsuuid) {
		char *endp, buf[17], fn[PATH_MAX];
		FILE *fp;

		buf[0] = '\0';
		xmkfn(fn, "%s/%s/%s", globalConfig.gconf_fsroot,
		    SL_RPATH_META_DIR, SL_FN_FSUUID);
		fp = fopen(fn, "r");
		if (fp) {
			if (fgets(buf, sizeof(buf), fp) == NULL)
				psclog_errorx("%s", fn);
			fclose(fp);

			sli_fsuuid = strtol(buf, &endp, 16);
			if (endp == buf || *endp != '\n') {
				psclog_errorx("invalid fsuuid in %s: "
				    "%s", fn, buf);
				return;
			}
		}

		if (!sli_fsuuid) {
			sli_fsuuid = fsuuid;

			fp = fopen(fn, "w");
			if (fp == NULL)
				psc_fatal("open %s", fn);
			fprintf(fp, "%"PRIx64"\n", sli_fsuuid);
			fclose(fp);
		}
	}
	if (sli_fsuuid != fsuuid)
		psclog_errorx("fsuuid don't match: %"PRIx64" vs "
		    "%"PRIx64, sli_fsuuid, fsuuid);
	return;

 error:
	psclog_errorx("no message; msg=%p opc=%d bufc=%d",
	    m, m ? (int)m->opc : -1, m ? (int)m->bufcount : -1);
}

void
sli_rpc_mds_pack_statfs(struct pscrpc_msg *m)
{
	struct srt_statfs *f;

	if (m == NULL) {
		psclog_errorx("msg is NULL");
		return;
	}
	f = pscrpc_msg_buf(m, m->bufcount - 2, sizeof(*f));
	if (f == NULL) {
		psclog_errorx("unable to pack statfs");
		return;
	}
	spinlock(&sli_ssfb_lock);
	memcpy(f, &sli_ssfb, sizeof(*f));
	freelock(&sli_ssfb_lock);
}

int
slrpc_newreq(struct slashrpc_cservice *csvc, int op,
    struct pscrpc_request **rqp, int qlen, int plen, void *mqp)
{
	if (csvc->csvc_ctype == SLCONNT_MDS) {
		int qlens[] = {
			qlen,
			sizeof(struct srt_statfs),
			sizeof(struct srt_authbuf_footer)
		};
		int plens[] = {
			plen,
			sizeof(struct srt_bmapminseq),
			sizeof(struct srt_authbuf_footer)
		};

		return (RSX_NEWREQN(csvc->csvc_import,
		    csvc->csvc_version, op, *rqp, nitems(qlens), qlens,
		    nitems(plens), plens, *(void **)mqp));
	}
	return (slrpc_newgenreq(csvc, op, rqp, qlen, plen, mqp));
}

int
slrpc_waitrep(struct slashrpc_cservice *csvc,
    struct pscrpc_request *rq, int plen, void *mpp)
{
	int rc;

	if (csvc->csvc_ctype == SLCONNT_MDS)
		sli_rpc_mds_pack_statfs(rq->rq_reqmsg);
	rc = slrpc_waitgenrep(rq, plen, mpp);
	if (csvc->csvc_ctype == SLCONNT_MDS) {
		sli_rpc_mds_unpack_bminseq(rq, PSCRPC_MSG_REPLY);
		if (rq->rq_reqmsg->opc == SRMT_CONNECT)
			sli_rpc_mds_unpack_fsuuid(rq, PSCRPC_MSG_REPLY);
	}
	return (rc);
}

int
slrpc_allocrep(struct pscrpc_request *rq, void *mqp, int qlen,
    void *mpp, int plen, int rcoff)
{
	if (rq->rq_rqbd->rqbd_service == sli_rim_svc.svh_service) {
		int rc, plens[] = {
			plen,
			sizeof(struct srt_statfs),
			sizeof(struct srt_authbuf_footer)
		};

		rc = slrpc_allocrepn(rq, mqp, qlen, mpp, nitems(plens),
		    plens, rcoff);
		sli_rpc_mds_unpack_bminseq(rq, PSCRPC_MSG_REQUEST);
		sli_rpc_mds_pack_statfs(rq->rq_repmsg);
		if (rc == 0 && rq->rq_reqmsg->opc == SRMT_CONNECT)
			sli_rpc_mds_unpack_fsuuid(rq, PSCRPC_MSG_REQUEST);
		return (rc);
	}
	return (slrpc_allocgenrep(rq, mqp, qlen, mpp, plen, rcoff));
}
