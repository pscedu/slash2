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
 * Routines for handling RPC requests for ION from CLIENT.
 */

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"

#include "bmap_iod.h"
#include "fdbuf.h"
#include "fid.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "sliod.h"
#include "slvr.h"

int
sli_ric_handle_disconnect(struct pscrpc_request *rq)
{
	struct srm_disconnect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

int
sli_ric_handle_connect(struct pscrpc_request *rq)
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
sli_ric_handle_io(struct pscrpc_request *rq, enum rw rw)
{
	struct pscrpc_bulk_desc *desc;
	struct slash_fidgen fg;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iovs[2];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct slvr_ref *slvr_ref[2];
	lnet_nid_t *np;

	sl_blkno_t bmapno, slvrno;
	uint32_t csize, tsize, roff, sblk;
	int rc=0, nslvrs, i;

	sblk = 0; /* gcc */

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psc_errorx("invalid size %u, fid:"FIDFMT,
			   mq->size,  FIDFMTARGS(&fg));
		mp->rc = -EINVAL;
		return (-1);
	}
	/* A RBW (read-before-write) request from the client may have a
	 *   write enabled bdbuf which he uses to fault in his page.
	 */
	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		mp->rc = bdbuf_check(&mq->sbdb, NULL, &fg, &bmapno,
		    &rq->rq_peer, *np, nodeResm->resm_res->res_id, rw);
		if (mp->rc == 0)
			goto bdbuf_ok;
	}
	psc_warnx("bdbuf failed for fid:"FIDFMT, FIDFMTARGS(&fg));
	return (-1);

 bdbuf_ok:
	/* Ensure that this request fits into the bmap's address range.
	 *   XXX this check assumes that mq->offset has not been made
	 *     bmap relative (ie it's filewise.
	 */
	//if ((mq->offset + mq->size) >= ((bmapno + 1) * SLASH_BMAP_SIZE)) {
	if ((mq->offset + mq->size - 1) >= SLASH_BMAP_SIZE) {
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
	if ((rc = iod_inode_open(fcmh, rw))) {
		DEBUG_FCMH(PLL_ERROR, fcmh, "error fidopen bmap=%u", bmapno);
		goto out;
	}
	/* ATM, not much to do here for write operations.
	 */
	if (iod_bmap_load(fcmh, bmapno, rw, &bmap)) {
		psc_errorx("failed to load bmap %u", bmapno);
		rc = -1;
		goto out;
	}

	DEBUG_FCMH(PLL_INFO, fcmh, "blkno=%u size=%u off=%u rw=%d",
		   bmap->bcm_blkno, mq->size, mq->offset, rw);
	/*
	 * Currently we have LNET_MTU = SLASH_SLVR_SIZE = 1MB, therefore
	 * we would never exceed two slivers.
	 */
	nslvrs = 1;
	slvrno = mq->offset / SLASH_SLVR_SIZE;
	if (((mq->offset + (mq->size-1)) / SLASH_SLVR_SIZE) > slvrno)
		nslvrs++;

	/* This loop assumes that nslvrs is always <= 2.  Note that
	 *   once i > 0, roff is always 0.
	 */
	for (i=0, roff=(mq->offset - (slvrno*SLASH_SLVR_SIZE)), tsize=mq->size;
	     i < nslvrs; i++, roff=0) {

		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bmap), rw);
		slvr_slab_prep(slvr_ref[i], rw);
		/* Fault in pages either for read or RBW.
		 */
		csize = MIN(tsize, SLASH_SLVR_SIZE - roff);
		slvr_io_prep(slvr_ref[i], roff, csize, rw);

		DEBUG_SLVR(PLL_INFO, slvr_ref[i], "post io_prep rw=%d", rw);
		/* mq->offset is the offset into the bmap, here we must
		 *  translate it into the offset of the sliver.
		 */
		iovs[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + roff;
		tsize -= iovs[i].iov_len = csize;
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
	fcmh_dropref(fcmh);	/* reaper will return it to the pool */
	return (rc);
}

int
sli_ric_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0; /* gcc */
	switch (rq->rq_reqmsg->opc) {
	case SRMT_DISCONNECT:
		rc = sli_ric_handle_disconnect(rq);
		break;
	case SRMT_CONNECT:
		rc = sli_ric_handle_connect(rq);
		break;
	case SRMT_READ:
		rc = sli_ric_handle_read(rq);
		break;
	case SRMT_WRITE:
		rc = sli_ric_handle_write(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
