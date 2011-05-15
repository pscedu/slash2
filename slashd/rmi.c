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

	mp->rc = mds_bmap_load_ion(&mq->fg, mq->bmapno, &b);
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
		    sizeof(struct srm_bmap_crcwire);

	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	buf = PSCALLOC(len);

	for (i=0, off=0; i < mq->ncrc_updates; i++) {
		iovs[i].iov_base = buf + off;
		iovs[i].iov_len = (mq->ncrcs_per_update[i] *
		    sizeof(struct srm_bmap_crcwire)) +
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
	int tract[NBREPLST], retifset[NBREPLST], iosidx, src_iosidx, rc;
	struct sl_resm *dst_resm, *src_resm;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct up_sched_work_item *wk;
	struct site_mds_info *smi;
	struct bmapc_memb *bcm;
	sl_bmapgen_t gen;

	dst_resm = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
	wk = uswi_find(&mq->fg, NULL);
	if (wk == NULL)
		goto out;

	/* XXX should we trust them to tell us who the src was? */
	src_resm = libsl_nid2resm(mq->nid);
	dst_resm = libsl_nid2resm(rq->rq_export->exp_connection->c_peer.nid);

	iosidx = mds_repl_ios_lookup(USWI_INOH(wk),
	    dst_resm->resm_res->res_id);
	if (iosidx < 0)
		goto out;

	if (!mds_bmap_exists(wk->uswi_fcmh, mq->bmapno))
		goto out;

	if (mds_bmap_load(wk->uswi_fcmh, mq->bmapno, &bcm))
		goto out;

	brepls_init(tract, -1);

	BHGEN_GET(bcm, &gen);
	if (mq->rc || mq->bgen != gen) {
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

			rc = mds_repl_bmap_walk(bcm, NULL, retifset,
			    REPL_WALKF_MODOTH, &src_iosidx, 1);

			if (rc) {
				/*
				 * Other replicas exist.
				 * Mark this failed source replica as
				 * garbage.
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
		 * If the MDS crashed and came back up, the state
		 * will have changed from SCHED->OLD, so change
		 * OLD->ACTIVE here for that case as well.
		 */
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		tract[BREPLST_REPL_SCHED] = BREPLST_VALID;
	}

	brepls_init(retifset, EINVAL);
	retifset[BREPLST_REPL_SCHED] = 0;
	retifset[BREPLST_TRUNCPNDG] = 0;

	mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	mds_repl_bmap_rel(bcm);

	smi = site2smi(dst_resm->resm_res->res_site);
	spinlock(&smi->smi_lock);
	psc_multiwaitcond_wakeup(&smi->smi_mwcond);
	freelock(&smi->smi_lock);
 out:
	if (dst_resm)
		mds_repl_nodes_adjbusy(resm2rmmi(src_resm),
		    resm2rmmi(dst_resm),
		    -slm_bmap_calc_repltraffic(bcm));
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
	struct bmapc_memb *bcm;
	int tract[NBREPLST];
	int iosidx;

	SL_RSX_ALLOCREP(rq, mq, mp);

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_INVALID;

	mp->rc = mds_bmap_load_ion(&mq->fg, mq->bmapno, &bcm);
	if (mp->rc)
		return (0);

	iosidx = mds_repl_ios_lookup(fcmh_2_inoh(bcm->bcm_fcmh),
	    libsl_nid2resm(rq->rq_export->exp_connection->
	    c_peer.nid)->resm_iosid);
	mds_repl_bmap_walk(bcm, tract, NULL, 0, &iosidx, 1);
	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
#if 0
	brepls_init(retifset, 1);
	tract[BREPLST_INVALID] = 0;

	for (i = fcmh_nallbmaps(fcmh), bmapno);
	    i > 0; i--) {
		load bmap
		if ()
			break;
		/* truncate metafile to remove garbage collected bmap */
		mdsio_setattr(METASIZE)
	}
#endif
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

	default:
		psclog_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
