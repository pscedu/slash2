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
#include "fid.h"
#include "fdbuf.h"
#include "rpc.h"
#include "slashdthr.h"
#include "slashrpc.h"
#include "mds_bmap.h"

int 
slrmi_bmap_getcrcs(struct pscrpc_request *rq)
{
	struct srm_bmap_wire_req *mq;
	struct srm_bmap_wire_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	sl_blkno_t bmapno;
	int rc;

	RSX_ALLOCREP(rq, mq, mp);	

	//rc = bdbuf_decrypt(&mq->sbdb, NULL, &fg, &bmapno, rq->rq_peer, lpid.nid
	//		   lnet_process_id_t cli_prid, lnet_nid_t ion_nid, sl_ios_id_t ios_id);
		
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
	rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK, SRIM_BULK_PORTAL,
			    iovs, mq->ncrc_updates);
	pscrpc_free_bulk(desc);
	if (rc)
		goto out;

	/* Crc the Crc's!
	 */
	PSC_CRC_CALC(crc, buf, len);
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
