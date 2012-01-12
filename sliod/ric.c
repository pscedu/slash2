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
 * Routines for handling RPC requests for ION from CLIENT.
 */

#include <errno.h>
#include <stdio.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/iostats.h"

#include "authbuf.h"
#include "bmap_iod.h"
#include "fid.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "sliod.h"
#include "slvr.h"

extern struct psc_iostats sliod_wr_1b_stat;
extern struct psc_iostats sliod_wr_1k_stat;
extern struct psc_iostats sliod_wr_4k_stat;
extern struct psc_iostats sliod_wr_16k_stat;
extern struct psc_iostats sliod_wr_64k_stat;
extern struct psc_iostats sliod_wr_128k_stat;
extern struct psc_iostats sliod_wr_512k_stat;
extern struct psc_iostats sliod_wr_1m_stat;
extern struct psc_iostats sliod_rd_1m_stat;
extern struct psc_iostats sliod_rd_1b_stat;
extern struct psc_iostats sliod_rd_1k_stat;
extern struct psc_iostats sliod_rd_4k_stat;
extern struct psc_iostats sliod_rd_16k_stat;
extern struct psc_iostats sliod_rd_64k_stat;
extern struct psc_iostats sliod_rd_128k_stat;
extern struct psc_iostats sliod_rd_512k_stat;
extern struct psc_iostats sliod_rd_1m_stat;

__static int
sli_ric_handle_io(struct pscrpc_request *rq, enum rw rw)
{
	uint32_t tsize, sblk, roff[RIC_MAX_SLVRS_PER_IO],
		len[RIC_MAX_SLVRS_PER_IO];
	struct slvr_ref *slvr_ref[RIC_MAX_SLVRS_PER_IO];
	struct iovec iovs[RIC_MAX_SLVRS_PER_IO];
	struct sli_aiocb_reply *aiocbr = NULL;
	struct slash_fidgen *fgp;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bmap;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	sl_bmapno_t bmapno, slvrno;
	int rc = 0, nslvrs, i;
	lnet_process_id_t *pp;
	uint64_t seqno;
	ssize_t rv;

	sblk = 0; /* gcc */

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	SL_RSX_ALLOCREP(rq, mq, mp);
	fgp = &mq->sbd.sbd_fg;
	bmapno = mq->sbd.sbd_bmapno;

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psclog_errorx("invalid size %u, fid:"SLPRI_FG,
		    mq->size, SLPRI_FG_ARGS(fgp));
		mp->rc = EINVAL;
		return (mp->rc);
	}

	if (mq->size < 1024)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_1b_stat : &sliod_rd_1b_stat, 1);
	
	else if (mq->size < 4096)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_1k_stat : &sliod_rd_1k_stat, 1);

	else if (mq->size < 16386)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_4k_stat : &sliod_rd_4k_stat, 1);

	else if (mq->size < 65536)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_16k_stat : &sliod_rd_16k_stat, 1);

	else if (mq->size < 131072)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_64k_stat : &sliod_rd_64k_stat, 1);

	else if (mq->size < 524288)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_128k_stat : &sliod_rd_128k_stat, 1);

	else if (mq->size < 1048576)
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_512k_stat : &sliod_rd_512k_stat, 1);
	else
		psc_iostats_intv_add((rw == SL_WRITE) ? 
		     &sliod_wr_1m_stat : &sliod_rd_1m_stat, 1);	
	/*
	 * A RBW (read-before-write) request from the client may have a
	 *   write enabled bmapdesc which he uses to fault in his page.
	 */
	DYNARRAY_FOREACH(pp, i, &lnet_prids) {
		mp->rc = bmapdesc_access_check(&mq->sbd, rw,
		    nodeResm->resm_res->res_id);
		if (mp->rc == 0) {
			psclog_info("bmapdesc check okay");
			break;
		}
		psclog_notice("bmapdesc resid mismatch - mine %x, peer %x",
		    nodeResm->resm_res->res_id, mq->sbd.sbd_ios);
	}
	if (mp->rc) {
		psclog_warnx("bmapdesc_access_check failed for fid "SLPRI_FG,
		    SLPRI_FG_ARGS(fgp));
		return (mp->rc);
	}

	/* Ensure that this request fits into the bmap's address range.
	 *   XXX this check assumes that mq->offset has not been made
	 *     bmap relative (ie it's filewise).
	 */
	if ((mq->offset + mq->size) > SLASH_BMAP_SIZE) {
		psclog_errorx("req offset / size outside of the bmap's "
		    "address range off=%u len=%u",
		    mq->offset, mq->size);
		mp->rc = ERANGE;
		return (mp->rc);
	}

	seqno = bim_getcurseq();
	if (mq->sbd.sbd_seq < seqno) {
		/* Reject old bmapdesc. */
		psclog_warnx("seq %"PRId64" < bim_getcurseq(%"PRId64")",
		     mq->sbd.sbd_seq, seqno);
		mp->rc = EKEYEXPIRED;
		return (mp->rc);
	}

	/* Lookup inode and fetch bmap, don't forget to decref bmap
	 *  on failure.
	 */
	rc = sli_fcmh_get(fgp, &fcmh);
	psc_assert(rc == 0);

	FCMH_LOCK(fcmh);
	/* Update the utimegen if necessary.
	 */
	if (fcmh->fcmh_sstb.sst_utimgen < mq->utimgen)
		fcmh->fcmh_sstb.sst_utimgen = mq->utimgen;
	FCMH_ULOCK(fcmh);

	/* ATM, not much to do here for write operations.
	 */
	rc = bmap_get(fcmh, bmapno, rw, &bmap);
	if (rc) {
		psclog_errorx("failed to load bmap %u", bmapno);
		goto out;
	}

	DEBUG_FCMH(PLL_INFO, fcmh, "bmapno=%u size=%u off=%u rw=%d "
	    "sbd_seq=%"PRId64, bmap->bcm_bmapno, mq->size, mq->offset, rw,
	    mq->sbd.sbd_seq);

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
	for (i = 0, roff[i] = (mq->offset - (slvrno * SLASH_SLVR_SIZE)),
		     tsize=mq->size;
	     i < nslvrs; i++, roff[i] = 0) {

		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_biodi(bmap), rw);
		slvr_slab_prep(slvr_ref[i], rw);

		/* Fault in pages either for read or RBW.
		 */
		len[i] = MIN(tsize, SLASH_SLVR_SIZE - roff[i]);
		rv = slvr_io_prep(slvr_ref[i], roff[i], len[i], rw,
			  &aiocbr);

		DEBUG_SLVR(((rv && rv != -SLERR_AIOWAIT) ?
		    PLL_WARN : PLL_INFO), slvr_ref[i],
		   "post io_prep rw=%d rv=%zd", rw, rv);

		/* mq->offset is the offset into the bmap, here we must
		 *  translate it into the offset of the sliver.
		 */
		iovs[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + roff[i];
		tsize -= iovs[i].iov_len = len[i];

		/* Avoid more complicated errors within lnet by ensuring
		 *   that len is non-zero.
		 */
		psc_assert(iovs[i].iov_len > 0);
	}

	psc_assert(!tsize);

	if (aiocbr) {
		struct slvr_ref *s;

		/* Setup first since this aiocb needs to be attached
		 *   to an aio'd sliver ASAP.
		 */
		sli_aio_reply_setup(aiocbr, rq, mq->size, mq->offset,
		    slvr_ref, nslvrs, iovs, nslvrs, rw);

		/* Now check for early completion.   If all slvrs are ready,
		 *   then we must reply with the data now.  Otherwise, we'll
		 *   never be woken since the aio cb(s) have been run.
		 */
		spinlock(&aiocbr->aiocbr_lock);

		for (i = 0; i < nslvrs; i++) {
			s = slvr_ref[i];

			SLVR_LOCK(s);
			if (s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR)) {
				DEBUG_SLVR(PLL_NOTIFY, s, 
				   "aio early ready, rw=%d", rw);
				SLVR_ULOCK(s);

			} else {
				/* Attach the reply to the first sliver 
				 *    waiting for aio and return AIOWAIT 
				 *    to client later.
				 */
				pll_add(&s->slvr_pndgaios, aiocbr);
				aiocbr->aiocbr_slvratt = s;
				psc_assert(s->slvr_flags & SLVR_AIOWAIT);
				SLVR_ULOCK(s);

				DEBUG_SLVR(PLL_NOTIFY, s, "aio wait");
				break;
			}
		}
		freelock(&aiocbr->aiocbr_lock);

		if (i != nslvrs) {
			mp->rc = SLERR_AIOWAIT;

			pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
			goto out;
		} else
			/* All slvrs are ready.
			 */
			sli_aio_aiocbr_release(aiocbr);
	}

	mp->rc = rsx_bulkserver(rq,
	    (rw == SL_WRITE ? BULK_GET_SINK : BULK_PUT_SOURCE),
	    SRIC_BULK_PORTAL, iovs, nslvrs);
	if (mp->rc) {
		rc = mp->rc;
		for (i = 0; i < nslvrs; i++) {
			SLVR_LOCK(slvr_ref[i]);
			if (rw == SL_READ)
				slvr_ref[i]->slvr_pndgreads--;
			else
				slvr_ref[i]->slvr_pndgwrts--;

			slvr_clear_inuse(slvr_ref[i], 0, SLASH_SLVR_SIZE);
			slvr_lru_tryunpin_locked(slvr_ref[i]);
			SLVR_ULOCK(slvr_ref[i]);

			DEBUG_SLVR(PLL_WARN, slvr_ref[i],
			   "unwind ref due to bulk error (rw=%d)", rw);
		}
		goto out;
	}

	/*
	 * Write the sliver back to the filesystem, but only the blocks
	 * which are marked '0' in the bitmap.  Here we don't care about
	 * buffer offsets since we're block aligned now
	 */
	if (rw == SL_WRITE) {
		roff[0] = mq->offset - (slvrno * SLASH_SLVR_SIZE);

		tsize = mq->size;
		sblk  = roff[0] / SLASH_SLVR_BLKSZ;

		if (roff[0] & SLASH_SLVR_BLKMASK)
			tsize += roff[0] & SLASH_SLVR_BLKMASK;
	}

	for (i = 0; i < nslvrs; i++) {
		if (rw == SL_WRITE) {
			uint32_t tsz = MIN((SLASH_BLKS_PER_SLVR - sblk) *
			    SLASH_SLVR_BLKSZ, tsize);

			tsize -= tsz;
			rv = slvr_fsbytes_wio(slvr_ref[i], tsz, sblk);
			if (rv)
				goto out;

			/* Only the first sliver may use a blk offset.
			 */
			sblk = 0;
			slvr_wio_done(slvr_ref[i]);
		} else
			slvr_rio_done(slvr_ref[i]);
	}

	if (rw == SL_WRITE)
		psc_assert(!tsize);

 out:
	if (bmap)
		bmap_op_done_type(bmap, BMAP_OPCNT_LOOKUP);

	/* XXX In situations where errors occur (such as an ENOSPC from
	 *   iod_inode_open()) then we must have a way to notify other
	 *   threads blocked on DATARDY.
	 */
	fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (rc);
}

static int
sli_ric_handle_rlsbmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct srt_bmapdesc *sbd, *newsbd, *p;
	struct bmap_iod_info *biod;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	uint32_t i;
	int rc, sync, fsync_time = 0;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->nbmaps > MAX_BMAP_RELEASE) {
		mp->rc = E2BIG;
		goto out;
	}

	for (i = 0, sync = 0; i < mq->nbmaps; i++, sync = 0) {
		sbd = &mq->sbd[i];
		rc = sli_fcmh_get(&sbd->sbd_fg, &f);
		psc_assert(rc == 0);

		/* Fsync here to guarantee that buffers are flushed to
		 *   disk before the MDS releases its odtable entry for
		 *   this bmap.
		 */
		FCMH_LOCK(f);
		if (!(f->fcmh_flags & FCMH_CTOR_DELAYED))
			sync = 1;
		FCMH_ULOCK(f);
		if (sync) {
			fsync_time = CURRENT_SECONDS;
			rc = fsync(fcmh_2_fd(f));
			fsync_time = CURRENT_SECONDS - fsync_time;

			if (fsync_time > 10)
				DEBUG_FCMH(PLL_WARN, f, "long fsync %d", 
				   fsync_time);
		}

		if (rc)
			DEBUG_FCMH(PLL_ERROR, f, "fsync failure rc=%d fd=%d"
			   " errno=%d", rc, fcmh_2_fd(f), errno);

		rc = bmap_get(f, sbd->sbd_bmapno, SL_WRITE, &b);
		if (rc) {
			psclog_errorx("failed to load bmap %u", 
			      sbd->sbd_bmapno);
			fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
			continue;
		} else
			fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);

		newsbd = psc_pool_get(bmap_rls_pool);
		memcpy(newsbd, sbd, sizeof(*sbd));

		biod = bmap_2_biodi(b);
		BIOD_LOCK(biod);
		PLL_FOREACH(p, &biod->biod_rls) {
			if (!memcmp(p, sbd, sizeof(*p))) {
				BIOD_ULOCK(biod);
				bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
				psc_pool_return(bmap_rls_pool, newsbd);
				continue;
			}
		}
		
		DEBUG_FCMH(PLL_INFO, f, "bmapno=%d seq=%"PRId64" key=%"PRId64
		   " (brls=%p)", b->bcm_bmapno, sbd->sbd_seq, sbd->sbd_key, 
		   newsbd);
		
		bmap_op_start_type(bii_2_bmap(biod), BMAP_OPCNT_RLSSCHED);

		pll_add(&biod->biod_rls, newsbd);		
		BMAP_SETATTR(b, BMAP_IOD_RLSSEQ);		
		biod_rlssched_locked(biod);
		BIOD_ULOCK(biod);

		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);		
	}
 out:
	return (0);
}

int
sli_ric_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	if (rq->rq_reqmsg->opc != SRMT_CONNECT) {
		EXPORT_LOCK(rq->rq_export);
		if (rq->rq_export->exp_private == NULL)
			rc = SLERR_NOTCONN;
		EXPORT_ULOCK(rq->rq_export);
		if (rc) {
			DEBUG_REQ(PLL_ERROR, rq,
			  "client has not issued SRMT_CONNECT");
			goto out;
		}
	}

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRIC_MAGIC, SRIC_VERSION,
		    SLCONNT_CLI);
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
		psclog_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
 out:
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}

void
iexpc_allocpri(struct pscrpc_export *exp)
{
	struct sli_cli_csvc_cpart *icccp;
	struct sli_exp_cli *iexpc;

	iexpc = exp->exp_private = PSCALLOC(sizeof(*iexpc));

	/*
	 * This is freed in sl_csvc_decref() since the csvc lock is
	 * operated on until the end compliance with APIs used in
	 * teardown.
	 */
	icccp = iexpc->iexpc_cccp = PSCALLOC(sizeof(*icccp));

	INIT_SPINLOCK(&icccp->icccp_lock);
	psc_waitq_init(&icccp->icccp_waitq);
	sli_getclcsvc(exp);
}

struct sl_expcli_ops sl_expcli_ops = {
	iexpc_allocpri,
	NULL
};
