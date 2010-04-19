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
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconn.h"
#include "sliod.h"
#include "slvr.h"

static int
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

__static int
sli_ric_handle_io(struct pscrpc_request *rq, enum rw rw)
{
	uint32_t csize, tsize, roff, sblk;
	struct pscrpc_bulk_desc *desc;
	struct slvr_ref *slvr_ref[2];
	struct bmap_iod_info *biodi;
	struct slash_fidgen *fgp;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec iovs[2];
	sl_bmapno_t bmapno, slvrno;
	int rc=0, nslvrs, i;
	lnet_nid_t *np;

	sblk = 0; /* gcc */

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	RSX_ALLOCREP(rq, mq, mp);
	fgp = &mq->sbd.sbd_fg;
	bmapno = mq->sbd.sbd_bmapno;

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psc_errorx("invalid size %u, fid:"FIDFMT,
		    mq->size,  FIDFMTARGS(fgp));
		mp->rc = EINVAL;
		return (-1);
	}

	/*
	 * A RBW (read-before-write) request from the client may have a
	 *   write enabled bdbuf which he uses to fault in his page.
	 */
	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		mp->rc = bmapdesc_access_check(&mq->sbd, rw,
		    nodeResm->resm_res->res_id, *np);
		if (mp->rc == 0)
			goto bdbuf_ok;
	}
	psc_warnx("bdbuf failed for fid:"FIDFMT, FIDFMTARGS(fgp));
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

#if 0
	if (mq->sbd.sbd_seq < bim_getcurseq()) {
		/* Reject old bdbufs. */
		psc_warnx("seq %"PRId64" is too old", mq->sbd.sbd_seq);
		mp->rc = -EINVAL;
		return (-1);
	}
#endif

	/* Lookup inode and fetch bmap, don't forget to decref bmap
	 *  on failure.
	 */
	rc = sli_fcmh_get(fgp, &fcmh);
	psc_assert(rc == 0);

	/* ATM, not much to do here for write operations.
	 */
	rc = bmap_get(fcmh, bmapno, rw, &bmap);
	if (rc) {
		psc_errorx("failed to load bmap %u", bmapno);
		goto out;
	}

	bmap_op_start_type(bmap, BMAP_OPCNT_SLVRIO);
	bmap_op_done_type(bmap, BMAP_OPCNT_LOOKUP);

	biodi = bmap_2_biodi(bmap);

	DEBUG_FCMH(PLL_INFO, fcmh, "bmapno=%u size=%u off=%u rw=%d "
		   " bdbuf_seq=%"PRId64" biod_key=%"PRId64,
		   bmap->bcm_blkno, mq->size, mq->offset, rw,
		   mq->sbd.sbd_seq, biodi->biod_cur_seqkey[0]);

	/* If warranted, bump the sequence number.
	 */
	spinlock(&biodi->biod_lock);
	if (mq->sbd.sbd_seq > biodi->biod_cur_seqkey[0]) {
		biodi->biod_cur_seqkey[0] = mq->sbd.sbd_seq;
		biodi->biod_cur_seqkey[1] = mq->sbd.sbd_key;
	}
	freelock(&biodi->biod_lock);

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
		roff = mq->offset - (slvrno * SLASH_SLVR_SIZE);

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

	/* Slvr I/O is done and bcr ops have been scheduled, safe to drop the ref cnt.
	 */
	bmap_op_done_type(bmap, BMAP_OPCNT_SLVRIO);
 out:
	/* XXX In situations where errors occur (such as an ENOSPC from
	 *   iod_inode_open()) then we must have a way to notify other
	 *   threads blocked on DATARDY.
	 */

	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC); /* reaper will return it to the pool */

	return (rc);
}

static int
sli_ric_handle_rlsbmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct srm_bmap_id *bid;
	struct bmap_iod_info *biodi;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	uint32_t i;
	int rc;

	RSX_ALLOCREP(rq, mq, mp);

	for (i=0; i < mq->nbmaps; i++) {
		bid = &mq->bmaps[i];

		rc = sli_fcmh_get(&bid->fg, &f);
		psc_assert(rc == 0);

		rc = bmap_get(f, bid->bmapno, SL_READ, &b);
		if (rc) {
			mp->bidrc[i] = rc;
			psc_errorx("failed to load bmap %u", bid->bmapno);
			continue;
		}
		/* Bmap is attached, safe to unref.
		 */
		fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);

		biodi = bmap_2_biodi(b);
		spinlock(&biodi->biod_lock);

		DEBUG_FCMH((biodi->biod_cur_seqkey[0] < bid->seq) ?
			   PLL_WARN : PLL_INFO,
			   f, "bmapno=%d seq=%"PRId64" key=%"PRId64
			   " biod_seq=%"PRId64" biod_key=%"PRId64,
			   b->bcm_blkno, bid->seq, bid->key,
			   biodi->biod_cur_seqkey[0],
			   biodi->biod_cur_seqkey[1]);
		/* Note:  this technique is flawed because it does not
		 *   track all released seq#'s, only the last one recv'd.
		 */
		biodi->biod_rls_seqkey[0] = bid->seq;
		biodi->biod_rls_seqkey[1] = bid->key;
		biodi->biod_rlsseq = 1;

		freelock(&biodi->biod_lock);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	}
	return (0);
}


int
sli_ric_handler(struct pscrpc_request *rq)
{
	int rc;

	rc = 0; /* gcc */
	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = sli_ric_handle_connect(rq);
		break;
	case SRMT_READ:
		rc = sli_ric_handle_read(rq);
		break;
	case SRMT_WRITE:
		rc = sli_ric_handle_write(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = sli_ric_handle_rlsbmap(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
