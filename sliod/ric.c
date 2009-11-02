/* $Id$ */

/*
 * Routines for handling RPC requests for ION from CLIENT (ric stands for RPC I/O Client).
 */

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
	uint64_t cfd;
	uint32_t tsize, roff, sblk;
	int rc=0, nslvrs=1, i;

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psc_errorx("invalid size %u, fid:"FIDFMT,
			   mq->size,  FIDFMTARGS(&fg));
		mp->rc = -EINVAL;
		return (-1);
	}
	/* A RBW (read-before-write) request from the client may have a write enabled
	 *   bdbuf which he uses to fault in his page.
	 *
	 * Read requests can get by with looser authentication.
	 */
	mp->rc = bdbuf_check(&mq->sbdb, &cfd, &fg, &bmapno, rq->rq_peer,
			     (rw == SL_READ) ? LNET_NID_ANY:lpid.nid,
			     (rw == SL_READ) ? IOS_ID_ANY: nodeInfo.node_res->res_id);

	if (mp->rc) {
		psc_warnx("bdbuf failed for fid:"FIDFMT,
			  FIDFMTARGS(&fg));
		return (-1);
	}
	/* Ensure that this request fits into the bmap's address range.
	 */
	if ((mq->offset + mq->size) >= ((bmapno + 1) * SLASH_BMAP_SIZE)) {
		psc_errorx("req offset / size outside of the bmap's "
			   "address range off=%u len=%u",
			   mq->offset, mq->size);
		mp->rc = -ERANGE;
		return (-1);
	}
	/* Lookup inode and fetch bmap, don't forget to decref bmap
	 *  on failure.
	 */
	fcmh = iod_inode_lookup(&fg);
	psc_assert(fcmh);
	/* Ensure the fid in the local filesystem is created and open,
	 *  otherwise fail.
	 */
	if (iod_inode_open(fcmh, rw)) {
		DEBUG_FCMH(PLL_ERROR, fcmh, "error fidopen bmap=%u", bmapno);
		rc = -1;
		goto out;
	}
	/* ATM, not much to do here for write operations.
	 */
	if (iod_bmap_load(fcmh, &mq->sbdb, rw, &bmap)) {
		psc_errorx("failed to load bmap %u", bmapno);
		rc = -1;
		goto out;
	}

	DEBUG_FCMH(PLL_INFO, fcmh, "blkno=%u size=%u off=%u rw=%d",
		   bmap->bcm_blkno, mq->size, mq->offset, rw);

	slvrno = mq->offset / SLASH_SLVR_SIZE;
	/* We should never have a request size > 1MB, therefore it would
	 *  never exceed two slivers.
	 */
	if (((mq->offset + (mq->size-1)) / SLASH_SLVR_SIZE) > slvrno)
		nslvrs++;

	/* This loop assumes that nslvrs is always <= 2.  Note that
	 *   once i > 0, roff is always 0.
	 */
	for (i=0, roff=(mq->offset - (slvrno*SLASH_SLVR_SIZE)), tsize=mq->size;
	     i < nslvrs; i++, roff=0) {

		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bmap),
					  SLVR_LOOKUP_ADD);
		slvr_slab_prep(slvr_ref[i], rw);
		/* Fault in pages either for read or RBW.
		 */
		slvr_io_prep(slvr_ref[i], roff, tsize, rw);

		DEBUG_SLVR(PLL_INFO, slvr_ref[i], "post io_prep rw=%d", rw);
		/* mq->offset is the offset into the bmap, here we must
		 *  translate it into the offset of the sliver.
		 */
		iovs[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + roff;
		tsize -= iovs[i].iov_len = MIN(tsize, SLASH_SLVR_SIZE - roff);
		/* Avoid more complicated errors within lnet by ensuring
		 *   that len is non-zero.
		 */
		psc_assert(iovs[i].iov_len > 0);
	}

	psc_assert(!tsize);

	mp->rc = rsx_bulkserver(rq, &desc,
			(rw == SL_WRITE ? BULK_GET_SINK : BULK_PUT_SOURCE),
			SRIC_BULK_PORTAL, iovs, nslvrs);

	if (mp->rc) {
		rc = -1;
		goto out;
	}

	if (desc)
		pscrpc_free_bulk(desc);

	/* Write the sliver back to the filesystem, but only the blocks
	 *   which are marked '0' in the bitmap.   Here we don't care about
	 *   buffer offsets since we're block aligned now
	 */
	if (rw == SL_WRITE) {
		off_t roff = mq->offset - (slvrno * SLASH_SLVR_SIZE);

		tsize = mq->size;
		sblk  = roff / SLASH_SLVR_BLKSZ;

		if (roff & SLASH_SLVR_BLKMASK)
			tsize += roff & SLASH_SLVR_BLKMASK;
	}

	for (i=0; i < nslvrs; i++) {
		if (rw == SL_WRITE) {
			uint32_t tsz = MIN((SLASH_BLKS_PER_SLVR-sblk)
					   * SLASH_SLVR_BLKSZ, tsize);
			tsize -= tsz;
			if ((rc = slvr_fsbytes_wio(slvr_ref[i], tsz, sblk)))
				goto out;
			/* Only the first sliver may use a blk offset.
			 */
			sblk = 0;
		}
		slvr_io_done(slvr_ref[i], rw);
	}

	if (rw == SL_WRITE)
		psc_assert(!tsize);
 out:
	/* XXX In situations where errors occur (such as an ENOSPC from
	 *   iod_inode_open()) then we must have a way to notify other
	 *   threads blocked on DATARDY.
	 */
	fidc_membh_dropref(fcmh);
	return (rc);
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
