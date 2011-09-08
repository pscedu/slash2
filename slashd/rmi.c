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

/*
 * Routines for handling RPC requests for MDS from ION.
 */

#define PSC_SUBSYS PSS_RPC

#include <stdio.h>

#include "pfl/fs.h"
#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "authbuf.h"
#include "bmap_mds.h"
#include "fid.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "up_sched_res.h"

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

	memcpy(&mp->bod, bmap_2_ondisk(b), sizeof(mp->bod));
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);

	return (0);
}

/**
 * slm_rmi_handle_bmap_crcwrt - Handle a BMAPCRCWRT request from ION,
 *	which receives the CRCs for the data contained in a bmap, checks
 *	their integrity during transmission, and records them in our
 *	metadata file system.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_crcwrt(struct pscrpc_request *rq)
{
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct iovec *iovs;
	size_t len = 0;
	uint64_t crc;
	uint32_t i;
	off_t off;
	void *buf;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->ncrc_updates > MAX_BMAP_NCRC_UPDATES) {
		mp->rc = EINVAL;
		return (mp->rc);
	}

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	for (i = 0; i < mq->ncrc_updates; i++)
		len += mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire);

	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	buf = PSCALLOC(len);

	for (i=0, off=0; i < mq->ncrc_updates; i++) {
		iovs[i].iov_base = buf + off;
		iovs[i].iov_len = (mq->ncrcs_per_update[i] *
		    sizeof(struct srt_bmap_crcwire)) +
		    sizeof(struct srm_bmap_crcup);

		off += iovs[i].iov_len;
	}

	mp->rc = rsx_bulkserver(rq, BULK_GET_SINK, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);
	if (mp->rc) {
		psclog_errorx("rsx_bulkserver() rc=%d", mp->rc);
		goto out;
	}

	/* Check the CRC the CRC's! */
	psc_crc64_calc(&crc, buf, len);
	if (crc != mq->crc) {
		psclog_errorx("crc verification of crcwrt payload failed");
		mp->rc = SLERR_BADCRC;
		goto out;
	}

	for (i = 0, off = 0; i < mq->ncrc_updates; i++) {
		struct srm_bmap_crcup *c = iovs[i].iov_base;
		uint32_t j;

		/* Does the bulk payload agree with the original request?
		 */
		if (c->nups != mq->ncrcs_per_update[i]) {
			psclog_errorx("nups(%u) != ncrcs_per_update(%u)",
			    c->nups, mq->ncrcs_per_update[i]);
			mp->crcup_rc[i] = -EINVAL;
		}

		/* Verify slot number validity.
		 */
		for (j = 0; j < c->nups; j++)
			if (c->crcs[j].slot >= SLASH_CRCS_PER_BMAP)
				mp->crcup_rc[i] = -ERANGE;

		/* Look up the bmap in the cache and write the CRCs.
		 */
		mp->crcup_rc[i] = mds_bmap_crc_write(c,
		    rq->rq_conn->c_peer.nid, mq);
		if (mp->crcup_rc[i])
			psclog_errorx("mds_bmap_crc_write() failed: "
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
 *	which is information pertaining to the status of a replication
 *	request, either succesful finish or failure.
 * @rq: request.
 */
int
slm_rmi_handle_repl_schedwk(struct pscrpc_request *rq)
{
	int tract[NBREPLST], retifset[NBREPLST], iosidx, src_iosidx;
	struct sl_resm *dst_resm = NULL, *src_resm;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm = NULL;
	struct site_mds_info *smi;
	sl_bmapgen_t gen;

	SL_RSX_ALLOCREP(rq, mq, mp);
	wk = uswi_find(&mq->fg);
	if (wk == NULL)
		goto out;

	/* XXX should we trust them to tell us who the src was? */
	src_resm = libsl_nid2resm(mq->nid);
	dst_resm = libsl_nid2resm(rq->rq_export->exp_connection->c_peer.nid);

	iosidx = mds_repl_ios_lookup(USWI_INOH(wk),
	    dst_resm->resm_res->res_id);
	if (iosidx < 0) {
		DEBUG_USWI(PLL_ERROR, wk,
		    "res %s not found found in file",
		    dst_resm->resm_addrbuf);
		goto out;
	}

	if (bmap_getf(wk->uswi_fcmh, mq->bmapno, SL_WRITE,
	    BMAPGETF_LOAD | BMAPGETF_NOAUTOINST, &bcm)) {
		DEBUG_USWI(PLL_ERROR, wk, "unable to load bmap %d",
		    mq->bmapno);
		goto out;
	}

	bmap_op_start_type(bcm, BMAP_OPCNT_REPLWK);

	brepls_init(tract, -1);

	BHGEN_GET(bcm, &gen);
	if (mq->rc == 0 && mq->bgen != gen)
		mq->rc = SLERR_GEN_OLD;
	if (mq->rc) {
		DEBUG_USWI(PLL_WARN, wk, "rc=%d", mq->rc);

		if (mq->rc == SLERR_BADCRC) {
			/*
			 * Bad CRC, media error perhaps.
			 * Check if other replicas exist.
			 */
			src_iosidx = mds_repl_ios_lookup(USWI_INOH(wk),
			    src_resm->resm_res->res_id);
			if (src_iosidx < 0)
				goto out;

			brepls_init(retifset, 0);
			retifset[BREPLST_VALID] = 1;

			if (mds_repl_bmap_walk(bcm, NULL, retifset,
			    REPL_WALKF_MODOTH, &src_iosidx, 1)) {
				/*
				 * Other replicas exist.  Mark this
				 * failed source replica as garbage.
				 */
				tract[BREPLST_VALID] = BREPLST_GARBAGE;
				mds_repl_bmap_walk(bcm, tract, NULL, 0,
				    &src_iosidx, 1);

				/* Try from another replica. */
				brepls_init(tract, -1);
				tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			} else {
				/* No other replicas exist. */
				tract[BREPLST_REPL_SCHED] = BREPLST_INVALID;
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

	brepls_init(retifset, EINVAL);
	retifset[BREPLST_REPL_SCHED] = 0;
	retifset[BREPLST_TRUNCPNDG] = 0;

	mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	mds_bmap_write_repls_rel(bcm);

	smi = site2smi(dst_resm->resm_res->res_site);
	spinlock(&smi->smi_lock);
	psc_multiwaitcond_wakeup(&smi->smi_mwcond);
	freelock(&smi->smi_lock);

 out:
	if (dst_resm && bcm)
		mds_repl_nodes_adjbusy(resm2rmmi(src_resm),
		    resm2rmmi(dst_resm),
		    -slm_bmap_calc_repltraffic(bcm));
	if (bcm)
		bmap_op_done_type(bcm, BMAP_OPCNT_REPLWK);
	if (wk)
		uswi_unref(wk);
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
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct bmapc_memb *b = NULL;
	int iosidx, tract[NBREPLST];
	struct fidc_membh *f;
	sl_bmapno_t bno;

	SL_RSX_ALLOCREP(rq, mq, mp);

	f = fidc_lookup_fg(&mq->fg);
	if (f == NULL) {
		mp->rc = ENOENT;
		return (0);
	}

	mp->rc = mds_bmap_load(f, mq->bmapno, &b);
	if (mp->rc)
		return (0);

	iosidx = mds_repl_ios_lookup(fcmh_2_inoh(b->bcm_fcmh),
	    libsl_nid2resm(rq->rq_export->exp_connection->
	    c_peer.nid)->resm_iosid);

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
		if (mds_bmap_load(f, bno, &b))
			continue;
		mds_repl_bmap_walk(b, tract, NULL, 0, &iosidx, 1);
		mds_bmap_write_repls_rel(b);

		if (bno == 0)
			break;
	}
	/* truncate metafile to remove garbage collected bmap */
//	mdsio_setattr(METASIZE)
	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

/**
 * slm_rmi_handle_connect - Handle a CONNECT request from ION.
 * @rq: request.
 */
int
slm_rmi_handle_connect(struct pscrpc_request *rq)
{
	const struct srm_connect_req *mq;
	struct srm_connect_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMI_MAGIC || mq->version != SRMI_VERSION)
		mp->rc = EINVAL;
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
	struct fidc_membh *p = NULL, *c = NULL;
	struct srm_import_req *mq;
	struct srm_import_rep *mp;
	struct slash_creds cr;
	struct bmapc_memb *b;
	struct srt_stat sstb;
	struct sl_resm *m;
	void *mdsio_data;
	sl_bmapno_t bno;
	uint32_t pol;
	int64_t fsiz;
	int rc, i;

	SL_RSX_ALLOCREP(rq, mq, mp);

	m = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (m == NULL) {
		mp->rc = SLERR_IOS_UNKNOWN;
		goto out;
	}

	/*
	 * Lookup the parent directory in the cache so that the slash2
	 * inode can be translated into the inode for the underlying fs.
	 */
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->cpn[sizeof(mq->cpn) - 1] = '\0';
	psclog_info("import: parent="SLPRI_FG" name=%s",
	    SLPRI_FG_ARGS(&p->fcmh_fg), mq->cpn);

	mds_reserve_slot(1);
	mp->rc = mdsio_opencreate(fcmh_2_mdsio_fid(p), &rootcreds,
	    O_CREAT | O_EXCL | O_RDWR, mq->sstb.sst_mode, mq->cpn, NULL,
	    &sstb, &mdsio_data, mdslog_namespace, slm_get_next_slashfid,
	    0);
	mds_unreserve_slot(1);

	if (mp->rc == EEXIST) {
		rc = mdsio_opencreate(fcmh_2_mdsio_fid(p), &rootcreds,
		    O_RDWR, 0, mq->cpn, NULL, &sstb, &mdsio_data, NULL,
		    NULL, 0);
		if (rc) {
			mp->rc = rc;
			goto out;
		}
		mp->fg = sstb.sst_fg;

		/* if mtime is newer, apply updates */
		if (timespeccmp(&mq->sstb.sst_mtim, &sstb.sst_mtim, <=)) {
			mdsio_release(&cr, mdsio_data);
			goto out;
		}
	} else if (mp->rc)
		goto out;

	mds_reserve_slot(1);
	mp->rc = mdsio_setattr(0, &mq->sstb, PSCFS_SETATTRF_DATASIZE |
	    PSCFS_SETATTRF_UID | PSCFS_SETATTRF_GID |
	    PSCFS_SETATTRF_ATIME | PSCFS_SETATTRF_MTIME |
	    SL_SETATTRF_NBLKS, &rootcreds, NULL, mdsio_data,
	    mdslog_namespace);
	mds_unreserve_slot(1);

	mdsio_release(&cr, mdsio_data);

	if (mp->rc)
		goto out;

	mp->rc = slm_fcmh_get(&sstb.sst_fg, &c);
	if (mp->rc)
		goto out;

	FCMH_LOCK(p);
	pol = p->fcmh_sstb.sstd_freplpol;
	FCMH_ULOCK(p);

	FCMH_LOCK(c);
	fcmh_2_ino(c)->ino_replpol = pol;
	fcmh_2_ino(c)->ino_nrepls = 1;
	fcmh_2_ino(c)->ino_repls[0].bs_id = m->resm_iosid;
	fcmh_2_ino(c)->ino_repl_nblks[0] = mq->sstb.sst_blocks;
	mp->rc = mds_inode_write(fcmh_2_inoh(c), mdslog_ino_repls, c);
	FCMH_ULOCK(c);
	if (mp->rc)
		goto out;

	fsiz = mq->sstb.sst_size;
	for (bno = 0; bno < howmany(mq->sstb.sst_size, SLASH_BMAP_SIZE);
	    bno++) {
		mp->rc = mds_bmap_load(c, bno, &b);
		if (mp->rc)
			goto out;
		for (i = 0; i < SLASH_SLVRS_PER_BMAP &&
		    fsiz > 0; fsiz -= SLASH_SLVR_SIZE, i++)
			/* Mark that data exists but no CRCs are available.
			 */
			b->bcm_crcstates[i] |=
				(BMAP_SLVR_DATA|BMAP_SLVR_CRCABSENT);
		mp->rc = mds_repl_inv_except(b, 0);
		if (mp->rc)
			goto out;
		// XXX write crc table
		mp->rc = mds_bmap_write_repls_rel(b);
		if (mp->rc)
			goto out;
	}
	mp->fg = sstb.sst_fg;

 out:
	/*
	 * XXX if we created the file but left it in a bad state (e.g.
	 * no repl table), then we should unlink it...
	 */
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmi_handle_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	mp->cattr.sst_uid = mq->creds.scr_uid;
	mp->cattr.sst_gid = mq->creds.scr_gid;
	mq->creds.scr_uid = 0;
	mq->creds.scr_uid = 0;
	return (slm_rmc_handle_mkdir(rq,
	    mdsio_slflags_2_setattrmask(PSCFS_SETATTRF_UID |
	    PSCFS_SETATTRF_GID)));
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

	SL_RSX_ALLOCREP(rq, mq, mp);
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
		rc = slm_rmi_handle_connect(rq);
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

	default:
		psclog_errorx("Unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
