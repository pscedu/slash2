/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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

/**
 * slm_rmi_handle_bmap_getcrcs - Handle a BMAPGETCRCS request from ION,
 *	so the ION can load the CRCs for a bmap and verify them against
 *	the data he has for the region of data that bmap represents.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_getcrcs(struct pscrpc_request *rq)
{
	struct srm_getbmap_full_req *mq;
	struct srm_getbmap_full_rep *mp;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_info *bmi;

	SL_RSX_ALLOCREP(rq, mq, mp);
#if 0
	int i;

	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		mp->rc = bmapdesc_access_check(&mq->sbd, mq->rw,
		    nodeInfo.node_res->res_id, *np);
		if (mp->rc == 0)
			break;
	}
	if (mp->rc)
		return (mp->rc);
#endif

	mp->rc = mds_bmap_load_fg(&mq->fg, mq->bmapno, &b);
	if (mp->rc)
		return (mp->rc);

	psc_assert(b);

	DEBUG_BMAP(PLL_INFO, b, "sending to sliod");

	bmi = bmap_2_bmi(b);
	memcpy(&mp->crcs, bmi->bmi_crcs, sizeof(mp->crcs));
	memcpy(&mp->crcstates, bmi->bmi_crcstates, sizeof(mp->crcstates));
	bmap_op_done(b);
	return (0);
}

/**
 * slm_rmi_handle_bmap_crcwrt - Handle a BMAPCRCWRT request from ION,
 *	which receives the CRCs for the data contained in a bmap, checks
 *	their integrity during transmission, and records them in our
 *	metadata file system.
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
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	for (i = 0; i < mq->ncrc_updates; i++) {
		// XXX sanity check mq->ncrcs_per_update[i]
		len += mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire);
	}

	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	buf = PSCALLOC(len);

	for (i = 0, off = 0; i < mq->ncrc_updates; i++) {
		iovs[i].iov_base = buf + off;
		iovs[i].iov_len = (mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire)) +
		    sizeof(struct srm_bmap_crcup);

		off += iovs[i].iov_len;
	}

	mp->rc = slrpc_bulkserver(rq, BULK_GET_SINK, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);
	if (mp->rc) {
		psclog_errorx("slrpc_bulkserver() rc=%d", mp->rc);
		goto out;
	}

	for (i = 0, off = 0; i < mq->ncrc_updates; i++) {
		struct srm_bmap_crcup *c = iovs[i].iov_base;
		uint32_t j;
		int rc;

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
			if (c->crcs[j].slot >= SLASH_CRCS_PER_BMAP)
				mp->crcup_rc[i] = -ERANGE;

		/* Look up the bmap in the cache and write the CRCs. */
		rc = mds_bmap_crc_write(c,
		    libsl_nid2iosid(rq->rq_conn->c_peer.nid), mq);
		if (rc)
			mp->crcup_rc[i] = rc;
		if (mp->crcup_rc[i])
			psclog(mp->crcup_rc[i] == -SLERR_GEN_OLD ?
			    PLL_INFO : PLL_ERROR,
			    "mds_bmap_crc_write() failed: "
			    "fid="SLPRI_FID", rc=%d",
			    c->fg.fg_fid, mp->crcup_rc[i]);
	}

 out:
	PSCFREE(buf);
	PSCFREE(iovs);

	return (mp->rc);
}

/**
 * slm_rmi_handle_repl_schedwk - Handle a REPL_SCHEDWK request from ION,
 *	which is notification pertaining to the status of a replication
 *	request, either successful finish or failure.
 * @rq: request.
 */
int
slm_rmi_handle_repl_schedwk(struct pscrpc_request *rq)
{
	int tract[NBREPLST], retifset[NBREPLST], iosidx, src_iosidx;
	struct sl_resm *dst_resm = NULL, *src_resm = NULL;
	struct slm_update_data *upd = NULL;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct sl_resource *src_res;
	struct bmapc_memb *b = NULL;
	struct fidc_membh *f = NULL;
	sl_bmapgen_t gen;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = -slm_fcmh_get(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	/* XXX should we trust them to tell us who the src was? */
	src_res = libsl_id2res(mq->src_resid);
	if (src_res == NULL)
		PFL_GOTOERR(out, mp->rc = -SLERR_ION_UNKNOWN);
	src_resm = psc_dynarray_getpos(&src_res->res_members, 0);
	dst_resm = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (dst_resm == NULL)
		PFL_GOTOERR(out, mp->rc = -SLERR_ION_UNKNOWN);

	iosidx = mds_repl_ios_lookup(current_vfsid, fcmh_2_inoh(f),
	    dst_resm->resm_res->res_id);
	if (iosidx < 0) {
		DEBUG_FCMH(PLL_ERROR, f,
		    "res %s not found in file",
		    dst_resm->resm_name);
		PFL_GOTOERR(out, mp->rc = -SLERR_REPL_NOT_ACT);
	}

	if (bmap_getf(f, mq->bmapno, SL_WRITE,
	    BMAPGETF_LOAD | BMAPGETF_NOAUTOINST, &b)) {
		DEBUG_FCMH(PLL_ERROR, f, "unable to load bmap %d",
		    mq->bmapno);
		PFL_GOTOERR(out, mp->rc = -EIO);
	}

	upd = bmap_2_upd(b);
	brepls_init(tract, -1);

	BHGEN_GET(b, &gen);
	if (mq->rc == 0 && mq->bgen != gen)
		mq->rc = -SLERR_GEN_OLD;
	if (mq->rc) {
		DPRINTF_UPD(PLL_WARN, upd, "rc=%d", mq->rc);

		// XXX impossible
		if (mq->rc == SLERR_BADCRC) {
			/*
			 * Bad CRC, media error perhaps.
			 * Check if other replicas exist.
			 */
			src_iosidx = mds_repl_ios_lookup(current_vfsid,
			    fcmh_2_inoh(f), src_res->res_id);
			if (src_iosidx < 0)
				goto out;

			brepls_init(retifset, 0);
			retifset[BREPLST_VALID] = 1;

			if (mds_repl_bmap_walk(b, NULL, retifset,
			    REPL_WALKF_MODOTH, &src_iosidx, 1)) {
				/*
				 * Other replicas exist.  Mark this
				 * failed source replica as garbage.
				 */
				tract[BREPLST_VALID] = BREPLST_GARBAGE;
				mds_repl_bmap_walk(b, tract, NULL, 0,
				    &src_iosidx, 1);

				/* Try from another replica. */
				brepls_init(tract, -1);
				tract[BREPLST_REPL_SCHED] =
				    BREPLST_REPL_QUEUED;
			} else {
				/* No other replicas exist. */
				tract[BREPLST_REPL_SCHED] =
				    BREPLST_INVALID;
			}
		} else if (mq->rc == SLERR_ION_OFFLINE)
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
		else
			/* otherwise, we assume the ION has cleaned up */
			tract[BREPLST_REPL_SCHED] = BREPLST_INVALID;
	} else {
		/*
		 * If the MDS crashed and came back up, the state will
		 * have changed from SCHED->OLD, so change OLD->ACTIVE
		 * here for that case as well.
		 */
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		tract[BREPLST_REPL_SCHED] = BREPLST_VALID;
	}

	brepls_init_idx(retifset);

	mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx, 1);
	mds_bmap_write_logrepls(b);

 out:
	if (mq->rc || mp->rc)
		psclog_warnx("reply from replication arrangement; "
		    "src=%s dst=%s qrc=%d prc=%d",
		    src_resm ? src_resm->resm_name : NULL,
		    dst_resm ? dst_resm->resm_name : NULL,
		    mq->rc, mp->rc);

	if (src_resm && dst_resm && b) {
		upd_rpmi_remove(res2rpmi(dst_resm->resm_res), upd);
		mds_repl_nodes_adjbusy(src_resm, dst_resm,
		    -slm_bmap_calc_repltraffic(b), NULL);
		upschq_resm(dst_resm, UPDT_PAGEIN);
		//upschq_resm(src_resm, UPDT_PAGEIN);
	}
	if (b)
		slm_repl_bmap_rel(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmi_handle_rls_bmap(struct pscrpc_request *rq)
{
	return (mds_handle_rls_bmap(rq, 1));
}

/**
 * slm_rmi_handle_bmap_ptrunc - Handle a BMAP_PTRUNC reply from ION.
 *	This means a client has trashed some partial truncation garbage.
 *	Note: if a sliod resolved a ptrunc CRC recalculation, this path
 *	is not taken; CRCWRT is issued as notification instead.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_ptrunc(struct pscrpc_request *rq)
{
	int rc, iosidx, tract[NBREPLST];
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct bmapc_memb *b = NULL;
	struct fidc_membh *f = NULL;
	sl_bmapno_t bno;

	SL_RSX_ALLOCREP(rq, mq, mp);

	mp->rc = fidc_lookup_fg(&mq->fg, &f);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mp->rc = bmap_get(f, mq->bmapno, SL_WRITE, &b);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	iosidx = mds_repl_ios_lookup(current_vfsid,
	    fcmh_2_inoh(b->bcm_fcmh),
	    libsl_nid2resm(rq->rq_export->exp_connection->
	    c_peer.nid)->resm_res_id);

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_INVALID;
	mds_repl_bmap_walk(b, tract, NULL, 0, &iosidx, 1);
	mds_bmap_write_repls_rel(b);

//	brepls_init(retifset, 1);
	tract[BREPLST_GARBAGE] = BREPLST_INVALID;

	bno = fcmh_nallbmaps(f);
	if (bno)
		bno--;
	for (;; bno--) {
		if (bmap_get(f, bno, SL_WRITE, &b))
			continue;
		mds_repl_bmap_walk(b, tract, NULL, 0, &iosidx, 1);
		mds_bmap_write_repls_rel(b);

		if (bno == 0)
			break;
	}
	/* XXX upsch_wakeup */
	/* truncate metafile to remove garbage collected bmap */
//	mdsio_setattr(METASIZE)
//	fcmh_set_repl_nblks(f, idx, sjbc->sjbc_repl_nblks);

 out:
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_rmi_handle_bmap_getminseq(struct pscrpc_request *rq)
{
	struct srm_getbmapminseq_req *mq;
	struct srm_getbmapminseq_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (mds_bmap_getcurseq(NULL, &mp->seqno));
}

int
slm_rmi_handle_import(struct pscrpc_request *rq)
{
	int fl, rc, rc2, idx, vfsid;
	struct fidc_membh *p = NULL, *c = NULL;
	struct srm_import_req *mq;
	struct srm_import_rep *mp;
	struct slash_creds cr;
	struct bmapc_memb *b;
	struct srt_stat sstb;
	struct sl_resm *m;
	sl_bmapno_t bno;
	void *mfh;
	int64_t fsiz;
	uint32_t i;
	struct bmap_mds_info *bmi;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slfid_to_vfsid(mq->pfg.fg_fid, &vfsid);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);
	if (vfsid != current_vfsid)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	m = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (m == NULL)
		PFL_GOTOERR(out, mp->rc = -SLERR_IOS_UNKNOWN);

	/*
	 * Lookup the parent directory in the cache so that the SLASH2
	 * inode can be translated into the inode for the underlying fs.
	 */
	mp->rc = -slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		PFL_GOTOERR(out, mp->rc);

	mq->cpn[sizeof(mq->cpn) - 1] = '\0';

	mds_reserve_slot(1);
	rc = mdsio_opencreatef(current_vfsid, fcmh_2_mfid(p),
	    &rootcreds, O_CREAT | O_EXCL | O_RDWR, MDSIO_OPENCRF_NOMTIM,
	    mq->sstb.sst_mode, mq->cpn, NULL, &sstb, &mfh,
	    mdslog_namespace, slm_get_next_slashfid, 0);
	mds_unreserve_slot(1);
	mp->rc = -rc;

	if (rc && rc != EEXIST)
		PFL_GOTOERR(out, mp->rc);

	if (rc == EEXIST) {
		rc2 = mdsio_lookup(current_vfsid, fcmh_2_mfid(p),
		    mq->cpn, NULL, &rootcreds, &sstb);
		if (rc2)
			PFL_GOTOERR(out, mp->rc = -rc2);

		if (IS_REMOTE_FID(sstb.sst_fid))
			PFL_GOTOERR(out, mp->rc = -PFLERR_NOTSUP);
	} else
		mdsio_release(current_vfsid, &cr, mfh);

	mp->fg = sstb.sst_fg;
	if (S_ISDIR(sstb.sst_mode))
		PFL_GOTOERR(out, mp->rc = -EISDIR);

	rc2 = -slm_fcmh_get(&sstb.sst_fg, &c);
	if (rc2)
		PFL_GOTOERR(out, mp->rc = rc2);

	if (rc == EEXIST) {
		/* if mtime is newer, apply updates */
		if (timespeccmp(&mq->sstb.sst_mtim,
		    &sstb.sst_mtim, <))
			PFL_GOTOERR(out, mp->rc = -SLERR_REIMPORT_OLD);

		if (mq->flags & SRM_IMPORTF_XREPL) {
			if (mq->sstb.sst_size != sstb.sst_size)
				PFL_GOTOERR(out, mp->rc =
				    -SLERR_IMPORT_XREPL_DIFF);
		} else {
			/* reclaim old data */
			FCMH_WAIT_BUSY(c);
			sstb.sst_fg.fg_gen = fcmh_2_gen(c) + 1;
			sstb.sst_size = 0;
			sstb.sst_blocks = 0;
			for (i = 0; i < fcmh_2_nrepls(c); i++)
				fcmh_set_repl_nblks(c, i, 0);
			/* XXX journal repl_nblks */

			rc = mds_fcmh_setattr(vfsid, c,
			    PSCFS_SETATTRF_DATASIZE | SL_SETATTRF_GEN |
			    SL_SETATTRF_NBLKS, &sstb);

			mds_inodes_odsync(vfsid, c, NULL);
			FCMH_UNBUSY(c);

			if (rc)
				PFL_GOTOERR(out, mp->rc = -rc);
		}
	} else
		slm_fcmh_endow_nolog(vfsid, p, c);

	idx = mds_repl_ios_lookup_add(vfsid, fcmh_2_inoh(c),
	    m->resm_res_id);
	if (idx < 0)
		PFL_GOTOERR(out, mp->rc = rc);
	fsiz = mq->sstb.sst_size;
	for (bno = 0; bno < howmany(mq->sstb.sst_size, SLASH_BMAP_SIZE);
	    bno++) {
		rc = bmap_get(c, bno, SL_WRITE, &b);
		if (rc)
			PFL_GOTOERR(out, mp->rc = rc);

		bmi = bmap_2_bmi(b);
		for (i = 0; i < SLASH_SLVRS_PER_BMAP &&
		    fsiz > 0; fsiz -= SLASH_SLVR_SIZE, i++)
			/*
			 * Mark that data exists but no CRCs are
			 * available.
			 */
			if ((bmi->bmi_crcstates[i] & BMAP_SLVR_DATA) == 0)
				bmi->bmi_crcstates[i] |= BMAP_SLVR_DATA |
				    BMAP_SLVR_CRCABSENT;

		if (mq->flags & SRM_IMPORTF_XREPL) {
			int tract[NBREPLST];

			brepls_init(tract, -1);
			tract[BREPLST_INVALID] = BREPLST_VALID;
			tract[BREPLST_GARBAGE] = BREPLST_VALID;
			mds_repl_bmap_walk(b, tract, NULL, 0, &idx, 1);
		} else
			rc = mds_repl_inv_except(b, idx);
		if (rc)
			PFL_GOTOERR(out, mp->rc = rc);
		rc = mds_bmap_write_repls_rel(b);
		if (rc)
			PFL_GOTOERR(out, mp->rc = rc);
	}

	/* XXX fire off any persistent replications */

	FCMH_WAIT_BUSY(c);
	mp->fg = c->fcmh_sstb.sst_fg;
	/* set nblks regardless of XREPL. */
	fcmh_set_repl_nblks(c, idx, mq->sstb.sst_blocks);
	fl = SL_SETATTRF_NBLKS;
	if ((mq->flags & SRM_IMPORTF_XREPL) == 0) {
		fl |= PSCFS_SETATTRF_DATASIZE | PSCFS_SETATTRF_MTIME |
		    PSCFS_SETATTRF_CTIME | PSCFS_SETATTRF_ATIME |
		    PSCFS_SETATTRF_UID | PSCFS_SETATTRF_GID;
	}
	rc = mds_fcmh_setattr(vfsid, c, fl, &mq->sstb);
	if (rc) {
		FCMH_UNBUSY(c);
		PFL_GOTOERR(out, mp->rc = -rc);
	}

	rc = mds_inodes_odsync(vfsid, c, NULL); /* journal repl_nblks */
	if (rc)
		mp->rc = rc;

	FCMH_UNBUSY(c);

 out:
	psclog_info("import: parent="SLPRI_FG" name=%s rc=%d",
	    SLPRI_FG_ARGS(&p->fcmh_fg), mq->cpn, mp->rc);

	/*
	 * XXX if we created the file but left it in a bad state (e.g.
	 * no repl table), then we should unlink it...
	 */
	if (c)
		fcmh_op_done(c);
	if (p)
		fcmh_op_done(p);

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
	FCMH_WAIT_BUSY(d);
	/*
	 * XXX if mp->rc == -EEXIST, only update attrs if target isn't
	 * newer
	 */
	rc = -mds_fcmh_setattr(vfsid, d,
	    PSCFS_SETATTRF_UID | PSCFS_SETATTRF_GID |
	    PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME |
	    PSCFS_SETATTRF_CTIME, &sstb);
	FCMH_UNBUSY(d);
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

/**
 * slm_rmi_handle_ping - Handle a PING request from ION.
 * @rq: request.
 */
int
slm_rmi_handle_ping(struct pscrpc_request *rq)
{
	const struct srm_ping_req *mq;
	struct srm_ping_rep *mp;
	struct sl_resm *m;

	SL_RSX_ALLOCREP(rq, mq, mp);
	m = libsl_try_nid2resm(rq->rq_peer.nid);
	if (m == NULL)
		mp->rc = -SLERR_ION_UNKNOWN;
	else {
		if (clock_gettime(CLOCK_MONOTONIC,
		    &res2iosinfo(m->resm_res)->si_lastcomm) == -1)
			psc_fatal("clock_gettime");

		if (mq->rc) {
			if (!(res2iosinfo(m->resm_res)->si_flags &
			      SIF_DISABLE_BIA)) {
				psclog_warnx("self-test from %s failed, "
				    "disabling write lease assignment",
				    m->resm_name);
				res2iosinfo(m->resm_res)->si_flags |=
				    SIF_DISABLE_BIA;
			}
		}
	}
	return (0);
}

/**
 * slm_rmi_handler - Handle a request for MDS from ION.
 * @rq: request.
 */
int
slm_rmi_handler(struct pscrpc_request *rq)
{
	int rc;

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

	case SRMT_BMAP_PTRUNC:
		rc = slm_rmi_handle_bmap_ptrunc(rq);
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

	/* replication messages */
	case SRMT_REPL_SCHEDWK:
		rc = slm_rmi_handle_repl_schedwk(rq);
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
		rc = batchrq_handle(rq);
		break;

	default:
		psclog_errorx("unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
