/* $Id$ */

/*
 * Routines for handling RPC requests for MDS from ION.
 */

#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "bmap_mds.h"
#include "cfd.h"
#include "fdbuf.h"
#include "fid.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"

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
	struct pscrpc_bulk_desc *desc;
	__unusedx struct slash_fidgen fg;
	struct bmapc_memb *b=NULL;
	struct iovec iov;
	__unusedx sl_blkno_t bmapno;

	RSX_ALLOCREP(rq, mq, mp);
#if 0
	mp->rc = bdbuf_check(&mq->sbdb, NULL, &fg, &bmapno, rq->rq_peer,
	    lpid.nid, nodeInfo.node_res->res_id, mq->rw);
	if (mp->rc)
		return (-1);
#endif

	mp->rc = mds_bmap_load_ion(&mq->fg, mq->bmapno, &b);
	if (mp->rc)
		return (mp->rc);

	psc_assert(b);

	DEBUG_BMAP(PLL_INFO, b, "sending to sliod");

	iov.iov_len = sizeof(struct slash_bmap_wire);
	iov.iov_base = bmap_2_bmdsiod(b);

	rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE, SRMI_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);

	bmap_op_done(b);

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

	RSX_ALLOCREP(rq, mq, mp);

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
	int tract[4], retifset[4], iosidx;
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;
	struct mds_site_info *msi;
	struct bmapc_memb *bcm;
	struct sl_replrq *rrq;
	struct sl_resm *resm;

	RSX_ALLOCREP(rq, mq, mp);
	rrq = mds_repl_findrq(&mq->fg, NULL);
	if (rrq == NULL) {
		mp->rc = ENOENT;
		goto out;
	}

	resm = libsl_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (resm == NULL) {
		mp->rc = SLERR_ION_UNKNOWN;
		goto out;
	}

	iosidx = mds_repl_ios_lookup(rrq->rrq_inoh, resm->resm_res->res_id);
	if (iosidx < 0) {
		mp->rc = SLERR_ION_NOTREPL;
		goto out;
	}

	if (!mds_bmap_valid(REPLRQ_FCMH(rrq), mq->bmapno)) {
		mp->rc = SLERR_INVALID_BMAP;
		goto out;
	}

	tract[SL_REPL_INACTIVE] = -1;
	tract[SL_REPL_OLD] = -1;
	tract[SL_REPL_ACTIVE] = -1;

	if (mp->rc)
		tract[SL_REPL_SCHED] = SL_REPL_INACTIVE;
	else
		tract[SL_REPL_SCHED] = SL_REPL_ACTIVE;

	retifset[SL_REPL_INACTIVE] = EINVAL;
	retifset[SL_REPL_SCHED] = 0;
	retifset[SL_REPL_OLD] = EINVAL;
	retifset[SL_REPL_ACTIVE] = EINVAL;

	mp->rc = mds_bmap_load(REPLRQ_FCMH(rrq), mq->bmapno, &bcm);
	if (mp->rc)
		goto out;

	BMAP_LOCK(bcm);
	mp->rc = mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	mds_repl_bmap_rel(bcm);

	msi = resm->resm_res->res_site->site_pri;
	spinlock(&msi->msi_lock);
	psc_multilock_cond_wakeup(&msi->msi_mlcond);
	freelock(&msi->msi_lock);
 out:
	if (rrq)
		mds_repl_unrefrq(rrq);
	return (0);
}

/*
 * slm_rmi_handle_connect - handle a CONNECT request from ION.
 * @rq: request.
 */
int
slm_rmi_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct sl_resm *resm;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMI_MAGIC || mq->version != SRMI_VERSION)
		mp->rc = -EINVAL;

	/* initialize our reverse stream structures */
	resm = libsl_nid2resm(rq->rq_peer.nid);
	slm_geticonnx(resm, rq->rq_export);

	slm_rmi_getdata(rq->rq_export);
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
	case SRMT_GETBMAPCRCS:
		rc = slm_rmi_handle_bmap_getcrcs(rq);
		break;
	case SRMT_REPL_SCHEDWK:
		rc = slm_rmi_handle_repl_schedwk(rq);
		break;
	case SRMT_CONNECT:
		rc = slm_rmi_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}

void
slm_rmi_hldrop(void *p)
{
	struct slm_rmi_data *smid = p;
	struct sl_resm *resm;

	resm = libsl_nid2resm(smid->smid_exp->exp_connection->c_peer.nid);
	if (resm)
		mds_repl_reset_scheduled(resm->resm_res->res_id);
	free(smid);
}

struct slm_rmi_data *
slm_rmi_getdata(struct pscrpc_export *exp)
{
	struct slm_rmi_data *smid, *p;

	spinlock(&exp->exp_lock);
	if (exp->exp_private)
		smid = exp->exp_private;
	freelock(&exp->exp_lock);
	if (smid)
		return (smid);
	p = PSCALLOC(sizeof(*p));
	p->smid_exp = exp;
	spinlock(&exp->exp_lock);
	if (exp->exp_private)
		smid = exp->exp_private;
	else {
		exp->exp_hldropf = slm_rmi_hldrop;
		exp->exp_private = smid = p;
		p = NULL;
	}
	freelock(&exp->exp_lock);
	free(p);
	return (smid);
}
