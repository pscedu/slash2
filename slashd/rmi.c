/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "slerr.h"
#include "up_sched_res.h"

/*
 * slm_rmi_handle_bmap_getcrcs - handle a BMAPGETCRCS request from ION,
 *	so the ION can load the CRCs for a bmap and verify them against
 *	the data he has for the region of data that bmap represents.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_getcrcs(struct pscrpc_request *rq)
{
	struct srm_bmap_wire_req *mq;
	struct srm_bmap_wire_rep *mp;
	struct bmapc_memb *b=NULL;

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
	mds_bmap_getcurseq(NULL, &mp->minseq);

	mp->rc = mds_bmap_load_ion(&mq->fg, mq->bmapno, &b);
	if (mp->rc)
		return (mp->rc);

	psc_assert(b);

	DEBUG_BMAP(PLL_INFO, b, "sending to sliod");

	memcpy(&mp->wire, b->bcm_od, sizeof(struct srt_bmap_wire));
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);

	return (0);
}

/*
 * slm_rmi_handle_bmap_crcwrt - handle a BMAPCRCWRT request from ION,
 *	which receives the CRCs for the data contained in a bmap, checks
 *	their integrity during transmission, and records them in our
 *	metadata file system.
 * @rq: request.
 */
int
slm_rmi_handle_bmap_crcwrt(struct pscrpc_request *rq)
{
	struct srm_bmap_crcwrt_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct iovec *iovs;
	void *buf;
	size_t len=0;
	off_t  off;
	int rc;
	psc_crc64_t crc;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);

	len = (mq->ncrc_updates * sizeof(struct srm_bmap_crcup));
	for (i=0; i < mq->ncrc_updates; i++)
		len += (mq->ncrcs_per_update[i] *
			sizeof(struct srm_bmap_crcwire));

	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	buf = PSCALLOC(len);

	for (i=0, off=0; i < mq->ncrc_updates; i++) {
		iovs[i].iov_base = buf + off;
		iovs[i].iov_len = ((mq->ncrcs_per_update[i] *
				    sizeof(struct srm_bmap_crcwire)) +
				   sizeof(struct srm_bmap_crcup));

		off += iovs[i].iov_len;
	}

	rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK, SRMI_BULK_PORTAL,
		       iovs, mq->ncrc_updates);
	if (desc)
		pscrpc_free_bulk(desc);
	else {
		psc_errorx("rsx_bulkserver() rc=%d", rc);
		/* rsx_bulkserver() frees the desc on error.
		 */
		goto out;
	}

	/* CRC the CRC's!
	 */
	psc_crc64_calc(&crc, buf, len);
	if (crc != mq->crc) {
		psc_errorx("crc verification of crcwrt payload failed");
		rc = -1;
		goto out;
	}

	for (i=0, off=0; i < mq->ncrc_updates; i++) {
		struct srm_bmap_crcup *c = iovs[i].iov_base;
		uint32_t j;
		/* Does the bulk payload agree with the original request?
		 */
		if (c->nups != mq->ncrcs_per_update[i]) {
			psc_errorx("nups(%u) != ncrcs_per_update(%u)",
				   c->nups, mq->ncrcs_per_update[i]);
			rc = -EINVAL;
			goto out;
		}
		/* Verify slot number validity.
		 */
		for (j=0; j < c->nups; j++) {
			if (c->crcs[j].slot >= SL_CRCS_PER_BMAP) {
				rc = -ERANGE;
				goto out;
			}
		}
		/* Look up the bmap in the cache and write the crc's.
		 */
		rc = mds_bmap_crc_write(c, rq->rq_conn->c_peer.nid);
		if (rc) {
			psc_errorx("rc(%d) mds_bmap_crc_write() failed", rc);
			goto out;
		}
	}
 out:
	PSCFREE(buf);
	PSCFREE(iovs);

	mds_bmap_getcurseq(NULL, &mp->data);

	return (rc);
}

/*
 * slm_rmi_handle_repl_schedwk - handle a REPL_SCHEDWK request from ION,
 *	which is information pertaining to the status of a replication
 *	request, either succesful finish or failure.
 * @rq: request.
 */
int
slm_rmi_handle_repl_schedwk(struct pscrpc_request *rq)
{
	int tract[SL_NREPLST], retifset[SL_NREPLST], iosidx;
	struct sl_resm *dst_resm, *src_resm;
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	struct site_mds_info *smi;
	struct bmapc_memb *bcm;
	struct up_sched_work_item *wk;
	sl_bmapgen_t gen;

	dst_resm = NULL;

	SL_RSX_ALLOCREP(rq, mq, mp);
	wk = mds_repl_findrq(&mq->fg, NULL);
	if (wk == NULL)
		goto out;

	dst_resm = libsl_nid2resm(rq->rq_export->exp_connection->c_peer.nid);

	iosidx = mds_repl_ios_lookup(USWI_INOH(wk),
		     dst_resm->resm_res->res_id);
	if (iosidx < 0)
		goto out;

	if (!mds_bmap_exists(wk->uswi_fcmh, mq->bmapno))
		goto out;

	if (mds_bmap_load(wk->uswi_fcmh, mq->bmapno, &bcm))
		goto out;

	tract[SL_REPLST_INACTIVE] = -1;
	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_TRUNCPNDG] = -1;
	tract[SL_REPLST_GARBAGE] = -1;
	
	BHGEN_GET(bcm, gen);
	if (mq->rc || mq->bgen != gen) {
		tract[SL_REPLST_OLD] = -1;
		tract[SL_REPLST_SCHED] = SL_REPLST_OLD;
	} else {
		/*
		 * If the MDS crashed and came back up, the state
		 * will have changed from SCHED->OLD, so change
		 * OLD->ACTIVE here for that case as well.
		 */
		tract[SL_REPLST_OLD] = SL_REPLST_ACTIVE;
		tract[SL_REPLST_SCHED] = SL_REPLST_ACTIVE;
	}

	retifset[SL_REPLST_INACTIVE] = EINVAL;
	retifset[SL_REPLST_SCHED] = 0;
	retifset[SL_REPLST_OLD] = EINVAL;
	retifset[SL_REPLST_ACTIVE] = EINVAL;
	retifset[SL_REPLST_TRUNCPNDG] = 0;
	retifset[SL_REPLST_GARBAGE] = EINVAL;

	mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	mds_repl_bmap_rel(bcm);

	smi = dst_resm->resm_res->res_site->site_pri;
	spinlock(&smi->smi_lock);
	psc_multiwaitcond_wakeup(&smi->smi_mwcond);
	freelock(&smi->smi_lock);
 out:
	if (dst_resm) {
		/* XXX should we trust them to tell us who the src was? */
		src_resm = libsl_nid2resm(mq->nid);
		mds_repl_nodes_setbusy(src_resm->resm_pri,
		    dst_resm->resm_pri, 0);
	}
	if (wk)
		mds_repl_unrefrq(wk);

	mds_bmap_getcurseq(NULL, &mp->data);

	return (0);
}

int
slm_rmi_handle_rls_bmap(struct pscrpc_request *rq)
{
	return (mds_handle_rls_bmap(rq));
}

/*
 * slm_rmi_handle_connect - handle a CONNECT request from ION.
 * @rq: request.
 */
int
slm_rmi_handle_connect(struct pscrpc_request *rq)
{
	struct slashrpc_cservice *csvc;
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct sl_resm *resm;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMI_MAGIC || mq->version != SRMI_VERSION)
		mp->rc = -EINVAL;

	/* initialize our reverse stream structures */
	resm = libsl_nid2resm(rq->rq_peer.nid);
	csvc = slm_geticsvcx(resm, rq->rq_export);
//	psc_multiwaitcond_wakeup(csvc->csvc_waitinfo);
	sl_csvc_decref(csvc);

	slm_rmi_getexpdata(rq->rq_export);
	mds_bmap_getcurseq(NULL, &mp->data);
	return (0);
}

/*
 * slm_rmi_handler - handle a request from ION.
 * @rq: request.
 */
int
slm_rmi_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_BMAPCRCWRT:
		rc = slm_rmi_handle_bmap_crcwrt(rq);
		break;
	case SRMT_CONNECT:
		rc = slm_rmi_handle_connect(rq);
		break;
	case SRMT_GETBMAPCRCS:
		rc = slm_rmi_handle_bmap_getcrcs(rq);
		break;
	case SRMT_REPL_SCHEDWK:
		rc = slm_rmi_handle_repl_schedwk(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = slm_rmi_handle_rls_bmap(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
#ifdef AUTHBUF
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
#endif
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}

void
slm_rmi_hldrop(void *p)
{
	struct slm_rmi_expdata *smie = p;
	struct sl_resm *resm;

	resm = libsl_nid2resm(smie->smie_exp->exp_connection->c_peer.nid);
	mds_repl_reset_scheduled(resm->resm_res->res_id);
	mds_repl_node_clearallbusy(resm->resm_pri);
	free(smie);
}

struct slm_rmi_expdata *
slm_rmi_getexpdata(struct pscrpc_export *exp)
{
	struct slm_rmi_expdata *smie, *p;

	smie = NULL;
	spinlock(&exp->exp_lock);
	if (exp->exp_private)
		smie = exp->exp_private;
	freelock(&exp->exp_lock);
	if (smie)
		return (smie);
	p = PSCALLOC(sizeof(*p));
	p->smie_exp = exp;
	spinlock(&exp->exp_lock);
	if (exp->exp_private)
		smie = exp->exp_private;
	else {
		exp->exp_hldropf = slm_rmi_hldrop;
		exp->exp_private = smie = p;
		p = NULL;
	}
	freelock(&exp->exp_lock);
	PSCFREE(p);
	return (smie);
}
