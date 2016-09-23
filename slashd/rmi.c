/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
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

/*
 * Routines for handling RPC requests for MDS from ION.
 */

#define PSC_SUBSYS PSS_RPC

#include <stdio.h>

#include "pfl/fs.h"
#include "pfl/str.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"
#include "pfl/lock.h"

#include "authbuf.h"
#include "batchrpc.h"
#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "mdsio.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

/*
 * Handle a BMAPGETCRCS request from ION, so the ION can load the CRCs
 * for a bmap and verify them against the data he has for the region of
 * data that bmap represents.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_getcrcs(struct pscrpc_request *rq)
{
	struct fidc_membh *f;
	struct srm_getbmap_full_req *mq;
	struct srm_getbmap_full_rep *mp;
	struct bmap_mds_info *bmi;
	struct bmap *b = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
#if 0
	int i;

	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		mp->rc = bmapdesc_access_check(&mq->sbd, mq->rw,
		    sl_resprof->res_id, *np);
		if (mp->rc == 0)
			break;
	}
	if (mp->rc)
		return (mp->rc);
#endif

	mp->rc = slm_fcmh_get(&mq->fg, &f);
	if (mp->rc) 
		return (0);

	FCMH_LOCK(f);
	FCMH_WAIT_BUSY(f, 1);

	mp->rc = bmap_get(f, mq->bmapno, SL_WRITE, &b);
	if (!mp->rc) {
		DEBUG_BMAP(PLL_DIAG, b, "reply to sliod.");
		bmi = bmap_2_bmi(b);
		memcpy(&mp->crcs, bmi->bmi_crcs, sizeof(mp->crcs));
		memcpy(&mp->crcstates, bmi->bmi_crcstates, sizeof(mp->crcstates));
		bmap_op_done(b);
	}

	FCMH_UNBUSY(f, 1);
	fcmh_op_done(f);
	return (0);
}

/*
 * Handle a BMAPCRCWRT request from ION, which receives the CRCs for the
 * data contained in a bmap, checks their integrity during transmission,
 * and records them in our metadata file system.
 * @rq: request.
 *
 * XXX should we check if an actual lease is out??
 */
int
slm_rmi_handle_bmap_crcwrt(struct pscrpc_request *rq)
{
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct iovec *iovs;
	size_t len = 0;
	uint32_t i;
	off_t off;
	void *buf;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->ncrc_updates > MAX_BMAP_NCRC_UPDATES) {
		psclog_errorx("ncrc_updates=%u is > %d",
		    mq->ncrc_updates, MAX_BMAP_NCRC_UPDATES);
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	len = mq->ncrc_updates * sizeof(struct srt_bmap_crcup);
	for (i = 0; i < mq->ncrc_updates; i++) {
		/* XXX sanity check mq->ncrcs_per_update[i] */
		len += mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire);
	}

	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	buf = PSCALLOC(len);

	for (i = 0, off = 0; i < mq->ncrc_updates; i++) {
		iovs[i].iov_base = buf + off;
		iovs[i].iov_len = (mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire)) +
		    sizeof(struct srt_bmap_crcup);

		off += iovs[i].iov_len;
	}

	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);
	if (mp->rc) {
		psclog_errorx("slrpc_bulkserver() rc=%d", mp->rc);
		goto out;
	}

	for (i = 0, off = 0; i < mq->ncrc_updates; i++) {
		struct srt_bmap_crcup *c = iovs[i].iov_base;
		uint32_t j;
		int rc;

		/*
		 * Due to poor code structure, the IOS will occasionally
		 * send us known junk.  Until that gets fixed, do a hack
		 * and ignore updates to old generations.
		 */
		if (c->fg.fg_fid == FID_ANY)
			continue;

		/*
		 * Does the bulk payload agree with the original
		 * request?
		 */
		if (c->nups != mq->ncrcs_per_update[i]) {
			psclog_errorx("nups(%u) != ncrcs_per_update(%u)",
			    c->nups, mq->ncrcs_per_update[i]);
			mp->crcup_rc[i] = -EINVAL;
		}

		/* Verify slot number validity. */
		for (j = 0; j < c->nups; j++)
			if (c->crcs[j].slot >= SLASH_SLVRS_PER_BMAP)
				mp->crcup_rc[i] = -ERANGE;

		/* Look up the bmap in the cache and write the CRCs. */
		rc = mds_bmap_crc_write(c,
		    libsl_nid2iosid(rq->rq_conn->c_peer.nid), mq);
		if (rc)
			mp->crcup_rc[i] = rc;
		if (mp->crcup_rc[i]) {
			/*
			 * A rash of EBADF (-9) errors can food
			 * network monitoring tools.  So let us
			 * tone down the log level as a workaround.
			 */
			psclog_info(
			    "mds_bmap_crc_write() failed: "
			    "fid="SLPRI_FID", rc=%d",
			    c->fg.fg_fid, mp->crcup_rc[i]);
		}
	}

 out:
	PSCFREE(buf);
	PSCFREE(iovs);

	return (mp->rc);
}

int
slm_rmi_handle_rls_bmap(struct pscrpc_request *rq)
{
	return (mds_handle_rls_bmap(rq, 1));
}

int
slm_rmi_handle_bmap_getminseq(struct pscrpc_request *rq)
{
	struct srm_getbmapminseq_req *mq;
	struct srm_getbmapminseq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mds_bmap_getcurseq(NULL, &mp->seqno);
	return (0);
}

int
slm_rmi_handle_import(__unusedx struct pscrpc_request *rq)
{

	/* please find old old in git history */
	return (0);
}

int
slm_rmi_handle_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	struct fidc_membh *d;
	struct srt_stat sstb;
	int rc, vfsid;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		return (0);
	if (vfsid != current_vfsid) {
		mp->rc = -EINVAL;
		return (0);
	}

	sstb = mq->sstb;
	mq->sstb.sst_uid = 0;
	mq->sstb.sst_gid = 0;
	rc = slm_mkdir(vfsid, mq, mp, MDSIO_OPENCRF_NOMTIM, &d);
	if (rc)
		return (rc);
	if (mp->rc && mp->rc != -EEXIST)
		return (0);
	FCMH_LOCK(d);
	/*
	 * XXX if mp->rc == -EEXIST, only update attrs if target isn't
	 * newer
	 */
	rc = -mds_fcmh_setattr(vfsid, d,
	    PSCFS_SETATTRF_UID | PSCFS_SETATTRF_GID |
	    PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME |
	    PSCFS_SETATTRF_CTIME, &sstb);
	fcmh_op_done(d);
	if (rc)
		mp->rc = rc;
	return (0);
}

int
slm_rmi_handle_symlink(struct pscrpc_request *rq)
{
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (slm_symlink(rq, mq, mp, SRMI_BULK_PORTAL));
}

/*
 * Handle a PING request from ION.
 * @rq: request.
 */
int
slm_rmi_handle_ping(struct pscrpc_request *rq)
{
	const struct srm_ping_req *mq;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct srm_ping_rep *mp;
	struct slrpc_cservice *csvc;
	struct sl_resm *m;

	SL_RSX_ALLOCREP(rq, mq, mp);
	m = libsl_try_nid2resm(rq->rq_peer.nid);
	if (m == NULL)
		mp->rc = -SLERR_ION_UNKNOWN;
	else {
		/*
 		 * If I put sliod under gdb, and let it continue after a 
 		 * while, the MDS thinks the sliod is down, but the sliod 
 		 * thinks it is still connected to the MDS. So it does not 
 		 * issue CONNECT RPCs. It does issue PING RPCs. For some 
 		 * reason, the SL_EXP_REGISTER_RESM() does not re-establish 
 		 * the connection (maybe becuase exp_hldropf is not NULL).
 		 *
 		 * It could be that we did not clear export when we return
 		 * it to pscrpc_export_pool. Or the export was not dropped.
 		 *
 		 * Anyway, the following code is added to solve this problem.
 		 */
		csvc = m->resm_csvc;
		CSVC_LOCK(csvc);
		clock_gettime(CLOCK_MONOTONIC, &csvc->csvc_mtime);
		if (!(csvc->csvc_flags & CSVCF_CONNECTED)) {
			sl_csvc_online(csvc);
			psclog_warnx("csvc %p for resource %s is back online", 
			    csvc, m->resm_name);
		}
		CSVC_ULOCK(csvc);

		si = res2iosinfo(m->resm_res);
		if (clock_gettime(CLOCK_MONOTONIC,
		    &si->si_lastcomm) == -1)
			psc_fatal("clock_gettime");

		rpmi = res2rpmi(m->resm_res);
		if (si->si_flags & SIF_DISABLE_ADVLEASE &&
		    !mq->rc) {
			psclog_warnx("self-test from %s succeeded; "
			    "re-enabling write lease assignment",
			    m->resm_name);
			RPMI_LOCK(rpmi);
			si->si_flags &= ~SIF_DISABLE_ADVLEASE;
			RPMI_ULOCK(rpmi);
		}
		if ((si->si_flags & SIF_DISABLE_ADVLEASE) == 0 &&
		    mq->rc) {
			psclog_warnx("self-test from %s failed; "
			    "disabling write lease assignment",
			    m->resm_name);
			RPMI_LOCK(rpmi);
			si->si_flags |= SIF_DISABLE_ADVLEASE;
			RPMI_ULOCK(rpmi);
		}
	}
	return (0);
}

/*
 * Handle a request for MDS from ION.
 * @rq: request.
 */
int
slm_rmi_handler(struct pscrpc_request *rq)
{
	int rc;

	/* (gdb) call libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid) */
	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export, 
	    slm_geticsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {

	/* bmap messages */
	case SRMT_BMAPCRCWRT:
		rc = slm_rmi_handle_bmap_crcwrt(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = slm_rmi_handle_rls_bmap(rq);
		break;
	case SRMT_GETBMAPCRCS:
		rc = slm_rmi_handle_bmap_getcrcs(rq);
		break;
	case SRMT_GETBMAPMINSEQ:
		rc = slm_rmi_handle_bmap_getminseq(rq);
		break;

	/* control messages */
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRMI_MAGIC, SRMI_VERSION,
		    SLCONNT_IOD);
		if (!rc) {
			struct sl_resm *m;

			m = libsl_nid2resm(rq->rq_peer.nid);
			clock_gettime(CLOCK_MONOTONIC,
			    &res2iosinfo(m->resm_res)->si_lastcomm);

			slconnthr_watch(slmconnthr, m->resm_csvc,
			    CSVCF_NORECON, mds_sliod_alive,
			    res2iosinfo(m->resm_res));

			upschq_resm(m, UPDT_PAGEIN);
		}
		break;
	case SRMT_PING:
		rc = slm_rmi_handle_ping(rq);
		break;

	/* import/export messages */
	case SRMT_LOOKUP:
		rc = slm_rmc_handle_lookup(rq);
		break;
	case SRMT_IMPORT:
		rc = slm_rmi_handle_import(rq);
		break;
	case SRMT_MKDIR:
		rc = slm_rmi_handle_mkdir(rq);
		break;
	case SRMT_SYMLINK:
		rc = slm_rmi_handle_symlink(rq);
		break;

	/* miscellaneous messages */
	case SRMT_BATCH_RP:
		rc = slrpc_batch_handle_reply(rq);
		break;

	default:
		psclog_errorx("unexpected opcode %d", rq->rq_reqmsg->opc);
		rc = -PFLERR_NOSYS;
		break;
	}

	slrpc_rep_out(rq);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
