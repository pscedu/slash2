/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2008-2018, Pittsburgh Supercomputing Center
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
	mp->rc = bmap_get(f, mq->bmapno, SL_WRITE, &b);
	if (!mp->rc) {
		DEBUG_BMAP(PLL_DIAG, b, "reply to sliod.");
		bmi = bmap_2_bmi(b);
		memcpy(&mp->crcs, bmi->bmi_crcs, sizeof(mp->crcs));
		memcpy(&mp->crcstates, bmi->bmi_crcstates, sizeof(mp->crcstates));
		bmap_op_done(b);
	}
	fcmh_op_done(f);
	return (0);
}


int
mds_file_update(sl_ios_id_t iosid, struct srt_update_rec *recp)
{
	struct fidc_membh *f;
	struct srt_stat sstb;
	int rc, fl, idx, vfsid;
	uint64_t nblks;
	struct slash_inode_handle *ih;

        rc = slm_fcmh_get(&recp->fg, &f); 
        if (rc) 
		return (rc);

	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	if (rc) {
		rc = -rc;
		goto out;
	}

	ih = fcmh_2_inoh(f);
	idx = mds_repl_ios_lookup(vfsid, ih, iosid);
	if (idx < 0) {
		psclog_warnx("CRC update: invalid IOS %x", iosid);
		rc = idx;
		goto out;
	}

	if (idx < SL_DEF_REPLICAS)
		nblks = fcmh_2_ino(f)->ino_repl_nblks[idx];
	else
		nblks = fcmh_2_inox(f)->inox_repl_nblks[idx - SL_DEF_REPLICAS];

	/*
	 * Only update the block usage when there is a real change.
	 */
	if (recp->nblks != nblks) {
		sstb.sst_blocks = fcmh_2_nblks(f) + recp->nblks - nblks;
		fl = SL_SETATTRF_NBLKS;

		fcmh_set_repl_nblks(f, idx, recp->nblks);

		/* use nolog because mdslog_bmap_crc() will cover this */
		FCMH_LOCK(f);
		rc = mds_fcmh_setattr_nolog(vfsid, f, fl, &sstb);
		if (rc)
			psclog_error("unable to setattr: rc=%d", rc);

		FCMH_LOCK(f);
		if (idx < SL_DEF_REPLICAS)
			mds_inode_write(vfsid, ih, NULL, NULL);
		else
			mds_inox_write(vfsid, ih, NULL, NULL);
		FCMH_ULOCK(f);
	}

 out:
	if (f)
		fcmh_op_done(f);
	return (rc);
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
slm_rmi_handle_update(struct pscrpc_request *rq)
{
	struct srm_updatefile_req *mq;
	struct srm_updatefile_rep *mp;
	int rc;
	uint32_t i;
	sl_ios_id_t iosid;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->count > MAX_FILE_UPDATES || mq->count <= 0) {
		psclog_errorx("updates=%u is > %d",
		    mq->count, MAX_FILE_UPDATES);
		mp->rc = -EINVAL;
		return (mp->rc);
	}
	iosid = libsl_nid2iosid(rq->rq_conn->c_peer.nid);

	for (i = 0; i < mq->count; i++) {
		rc = mds_file_update(iosid, &mq->updates[i]);
		if (!rc) 
			continue;
		psclog_errorx("Update failed: "SLPRI_FG,
			SLPRI_FG_ARGS(&mq->updates[i].fg));
	}
	return (0);
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

	if (slm_quiesce)
		return (-EAGAIN);

	/* (gdb) call libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid) */
	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export, 
	    slm_geticsvcx(_resm, rq->rq_export, 0));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {

	/* bmap messages */
	case SRMT_UPDATEFILE:
		rc = slm_rmi_handle_update(rq);
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
