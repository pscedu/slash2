/* $Id$ */

/*
 * Routines for handling RPC requests for ION from CLIENT.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "fdbuf.h"
#include "fid.h"
#include "fidcache.h"
#include "iod_bmap.h"
#include "rpc.h"
#include "slashrpc.h"
#include "sliod.h"
#include "slvr.h"

int
slric_handle_disconnect(struct pscrpc_request *rq)
{
	struct srm_disconnect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

int
slric_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRIC_MAGIC ||
	    mq->version != SRIC_VERSION)
		mp->rc = -EINVAL;
	return (0);
}


int
slric_handle_io(struct pscrpc_request *rq, int rw)
{
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;	
	struct iovec iovs[2];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct slvr_ref *slvr_ref[2];
	
	sl_blkno_t bmapno, slvrno;
	ssize_t nbytes;
	uint64_t cfd;
	uint32_t tsize, roff;
	void *buf;
	int rc, fd, nslvrs=1, i;

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psc_errorx("invalid size %u, fid:"FIDFMT,
			   mq->size,  FIDFMTARGS(&fg));
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = bdbuf_decrypt(&mq->sbdb, &cfd, &fg,
	    &bmapno, rq->rq_peer, lpid.nid,
	    nodeInfo.node_res->res_id);
	if (mp->rc) {
		psc_errorx("fdbuf_decrypt failed");
		return (0);
	}
	/* Ensure that this request fits into the bmap's address range.
	 */	
	if ((mq->offset + mq->size) >= ((bmapno + 1) * SLASH_BMAP_SIZE)) {
		psc_errorx("req offset / size outside of the bmap's "
			   "address range off=%u len=%u", 
			   mq->offset, mq->size);
		mp->rc = -ERANGE;
		return (0);
	}
	/* Lookup inode and fetch bmap, don't forget to decref bmap
	 *  on failure.
	 */
	fcmh = iod_inode_lookup(fg.fg_fid);
	psc_assert(fcmh);
	/* ATM, not much to do here for write operations.
	 */
	if (iod_bmap_load(fcmh, &mq->sbdb, rw, &bmap)) {
		fidc_membh_dropref(fcmh);
		psc_errorx("failed to load bmap %u", bmapno);
		return (0);
	}	
	/* Ensure the fid in the local filesystem is created and open,
	 *  otherwise fail.
	 */
	if (iod_inode_open(fcmh, rw)) {
		fidc_membh_dropref(fcmh);
		DEBUG_FCMH(PLL_ERROR, fcmh, "error fidopen bmap=%u", bmapno);
		return (0);
	}
	
	slvrno = mq->offset / SLASH_SLVR_SIZE;
	/* We should never have a request size > 1MB, therefore it would 
	 *  never exceed two slivers.
	 */
	if (((mq->offset + mq->size) / SLASH_SLVR_SIZE) > slvrno)
		nslvrs++;

	/* This loop assumes that nslvrs is always <= 2.  Note that  
	 *   once i > 0, roff is always 0.
	 */
	for (i=0, roff=(mq->offset - (slvrno*SLASH_SLVR_SIZE)), tsize=mq->size;
	     i < nslvrs; i++, roff=0) {

		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bmap), 1);
		slvr_slab_prep(slvr_ref[i]);
		/* Fault in pages either for read or RBW.
		 */
		slvr_io_prep(slvr_ref[i], roff, tsize, rw);

		DEBUG_SLVR(PLL_TRACE, slvr_ref[i], "post io_prep");
		/* mq->offset is the offset into the bmap, here we must
		 *  translate it into the offset of the sliver.
		 */
		iovs[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + roff;
		tsize -= iovs[i].iov_len = MIN(tsize, SLASH_SLVR_SIZE - roff);
	}

	mp->rc = rsx_bulkserver(rq, &desc, 
			(rw == SL_WRITE ? BULK_GET_SINK : BULK_PUT_SOURCE), 
			SRIC_BULK_PORTAL, iovs, nslvrs);
		  
	if (mp->rc)
		return (0);

	if (desc)
		pscrpc_free_bulk(desc);

	/* Write the sliver back to the filesystem, but only the blocks
	 *   which are marked '0' in the bitmap.   Here we don't care about
	 *   buffer offsets since we're block aligned now
	 */ 
	for (i=0; i < nslvrs; i++) {	
		if (rw == SL_WRITE)
			if ((rc = slvr_fsbytes_io(slvr_ref[i], SL_WRITE)))
				return (rc);
		slvr_io_done(slvr_ref[i], rw);
	} 

	return (0);
}

int
slric_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0; /* gcc */
	switch (rq->rq_reqmsg->opc) {
	case SRMT_DISCONNECT:
		rc = slric_handle_disconnect(rq);
		break;
	case SRMT_CONNECT:
		rc = slric_handle_connect(rq);
		break;
	case SRMT_READ:
		rc = slric_handle_read(rq);
		break;
	case SRMT_WRITE:
		rc = slric_handle_write(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
