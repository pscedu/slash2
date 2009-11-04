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

#include "cfd.h"
#include "fdbuf.h"
#include "fid.h"
#include "mds_bmap.h"
#include "mdsrpc.h"
#include "slashd.h"
#include "slashrpc.h"

int
slrmi_bmap_getcrcs(struct pscrpc_request *rq)
{
	struct srm_bmap_wire_req *mq;
	struct srm_bmap_wire_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	struct bmapc_memb *b=NULL;
	struct iovec iov;
	sl_blkno_t bmapno;
	int rc;

	RSX_ALLOCREP(rq, mq, mp);
#if 0
	mp->rc = bdbuf_check(&mq->sbdb, NULL, &fg, &bmapno, rq->rq_peer,
			     (mq->rw == SL_WRITE) ? lpid.nid : LNET_NID_ANY,
			     (mq->rw == SL_WRITE) ? nodeInfo.node_res->res_id :
			     IOS_ID_ANY);
#endif

	if (mp->rc)
		return (-1);

	rc = mds_bmap_load_ion(&mq->fg, mq->bmapno, &b);
	if (rc)
		return (rc);

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

int
slrmi_bmap_crcwrt(struct pscrpc_request *rq)
{
	struct srm_bmap_crcwrt_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct iovec *iovs;
	void *buf;
	size_t len=0;
	off_t  off;
	int rc;
	psc_crc_t crc;
	u32 i;

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

	/* Crc the Crc's!
	 */
	psc_crc_calc(&crc, buf, len);
	if (crc != mq->crc) {
		psc_errorx("crc verification of crcwrt payload failed");
		rc = -1;
		goto out;
	}

	for (i=0, off=0; i < mq->ncrc_updates; i++) {
		struct srm_bmap_crcup *c = iovs[i].iov_base;
		u32 j;
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
 * slrmi_handle_connect - handle a CONNECT request from ION.
 */
int
slrmi_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMI_MAGIC || mq->version != SRMI_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/*
 * slrmi_handler - handle a request from ION.
 */
int
slrmi_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_BMAPCRCWRT:
		rc = slrmi_bmap_crcwrt(rq);
		break;

	case SRMT_GETBMAPCRCS:
		rc = slrmi_bmap_getcrcs(rq);
		break;

	case SRMT_CONNECT:
		rc = slrmi_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
