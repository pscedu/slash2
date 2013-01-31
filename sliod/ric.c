/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/ctlsvr.h"
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
	uint32_t tsize, sblk, roff, len[RIC_MAX_SLVRS_PER_IO];
	struct slvr_ref *slvr_ref[RIC_MAX_SLVRS_PER_IO];
	struct iovec iovs[RIC_MAX_SLVRS_PER_IO];
	struct sli_aiocb_reply *aiocbr = NULL;
	struct slash_fidgen *fgp;
	struct bmapc_memb *bmap;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct fidc_membh *f;
	sl_bmapno_t bmapno, slvrno;
	int rc = 0, nslvrs = 0, i;
	lnet_process_id_t *pp;
	uint64_t seqno;
	ssize_t rv;

	sblk = 0; /* gcc */

	OPSTAT_INCR(SLI_OPST_HANDLE_IO);
	psc_assert(rw == SL_READ || rw == SL_WRITE);

	SL_RSX_ALLOCREP(rq, mq, mp);
	fgp = &mq->sbd.sbd_fg;
	bmapno = mq->sbd.sbd_bmapno;

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psclog_errorx("invalid size %u, fid:"SLPRI_FG,
		    mq->size, SLPRI_FG_ARGS(fgp));
		mp->rc = -EINVAL;
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
	 *     bmap relative (i.e. it's filewise).
	 */
	if ((mq->offset + mq->size) > SLASH_BMAP_SIZE) {
		psclog_errorx("req offset / size outside of the bmap's "
		    "address range off=%u len=%u",
		    mq->offset, mq->size);
		mp->rc = -ERANGE;
		return (mp->rc);
	}

	seqno = bim_getcurseq();
	if (mq->sbd.sbd_seq < seqno) {
		/* Reject old bmapdesc. */
		psclog_warnx("seq %"PRId64" < bim_getcurseq(%"PRId64")",
		    mq->sbd.sbd_seq, seqno);
		mp->rc = -EKEYEXPIRED;
		return (mp->rc);
	}

	/*
	 * Lookup inode and fetch bmap, don't forget to decref bmap on
	 * failure.
	 */
	rc = sli_fcmh_get(fgp, &f);
	psc_assert(rc == 0);

	FCMH_LOCK(f);
	/* Update the utimegen if necessary. */
	if (f->fcmh_sstb.sst_utimgen < mq->utimgen)
		f->fcmh_sstb.sst_utimgen = mq->utimgen;
	FCMH_ULOCK(f);

	/* ATM, not much to do here for write operations. */
	rc = bmap_get(f, bmapno, rw, &bmap);
	if (rc) {
		psclog_errorx("failed to load bmap %u", bmapno);
		goto out;
	}

	DEBUG_FCMH(PLL_INFO, f, "bmapno=%u size=%u off=%u rw=%s "
	    "sbd_seq=%"PRId64, bmap->bcm_bmapno, mq->size, mq->offset,
	    rw == SL_WRITE ? "wr" : "rd", mq->sbd.sbd_seq);

	/*
	 * Currently we have LNET_MTU = SLASH_SLVR_SIZE = 1MiB,
	 * therefore we would never exceed two slivers.
	 */
	nslvrs = 1;
	slvrno = mq->offset / SLASH_SLVR_SIZE;
	if (((mq->offset + (mq->size-1)) / SLASH_SLVR_SIZE) > slvrno)
		nslvrs++;

	for (i = 0; i < nslvrs; i++)
		slvr_ref[i] = NULL;

	/*
	 * This loop assumes that nslvrs is always no larger than
	 * RIC_MAX_SLVRS_PER_IO.
	 *
	 * Note that once i > 0, roff is always 0.
	 */
	roff = mq->offset - slvrno * SLASH_SLVR_SIZE;
	for (i = 0, tsize = mq->size; i < nslvrs; i++, roff = 0) {

		slvr_ref[i] = slvr_lookup(slvrno + i, bmap_2_bii(bmap),
		    rw);
		slvr_slab_prep(slvr_ref[i], rw);

		/* Fault in pages either for read or RBW. */
		len[i] = MIN(tsize, SLASH_SLVR_SIZE - roff);
		rv = slvr_io_prep(slvr_ref[i], roff, len[i], rw,
		    &aiocbr);

		DEBUG_SLVR(((rv && rv != -SLERR_AIOWAIT) ? PLL_WARN : PLL_INFO),
		    slvr_ref[i], "post io_prep rw=%s rv=%zd",
		    rw == SL_WRITE ? "wr" : "rd", rv);
		if (rv && rv != -SLERR_AIOWAIT) {
			rc = rv;
			goto out;
		}

		/*
		 * mq->offset is the offset into the bmap, here we must
		 * translate it into the offset of the sliver.
		 */
		iovs[i].iov_base = slvr_ref[i]->slvr_slab->slb_base + roff;
		tsize -= iovs[i].iov_len = len[i];

		/*
		 * Avoid more complicated errors within lnet by ensuring
		 * that len is non-zero.
		 */
		psc_assert(iovs[i].iov_len > 0);
	}

	psc_assert(!tsize);

	if (aiocbr) {
		struct slvr_ref *s;

		/*
		 * Setup first since this aiocb needs to be attached to
		 * an aio'd sliver ASAP.
		 */
		sli_aio_reply_setup(aiocbr, rq, mq->size, mq->offset,
		    slvr_ref, nslvrs, iovs, nslvrs, rw);

		/*
 		 * XXX The aiocbr could be freed at this point. Need reference.
 		 */

		/*
		 * Now check for early completion.  If all slvrs are
		 * ready, then we must reply with the data now.
		 * Otherwise, we'll never be woken since the aio cb(s)
		 * have been run.
		 */
		spinlock(&aiocbr->aiocbr_lock);

		for (i = 0; i < nslvrs; i++) {
			s = slvr_ref[i];

			SLVR_LOCK(s);
			if (s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR)) {
				DEBUG_SLVR(PLL_NOTIFY, s,
				   "aio early ready, rw=%s",
				   rw == SL_WRITE ? "wr" : "rd");
				SLVR_ULOCK(s);

			} else {
				/*
				 * Attach the reply to the first sliver
				 * waiting for aio and return AIOWAIT to
				 * client later.
				 */
				pll_add(&s->slvr_pndgaios, aiocbr);
				psc_assert(s->slvr_flags & SLVR_AIOWAIT);
				SLVR_ULOCK(s);

				OPSTAT_INCR(SLI_OPST_HANDLE_REPLREAD_INSERT);
				DEBUG_SLVR(PLL_NOTIFY, s, "aio wait");
				break;
			}
		}
		freelock(&aiocbr->aiocbr_lock);

		if (i != nslvrs) {
			mp->rc = SLERR_AIOWAIT;

			pscrpc_msg_add_flags(rq->rq_repmsg,
			    MSG_ABORT_BULK);
			goto out;
		}
		/* All slvrs are ready. */
		sli_aio_aiocbr_release(aiocbr);
	}

	mp->rc = rsx_bulkserver(rq,
	    (rw == SL_WRITE ? BULK_GET_SINK : BULK_PUT_SOURCE),
	    SRIC_BULK_PORTAL, iovs, nslvrs);
	if (mp->rc) {
		rc = mp->rc;
		goto out;
	}

	/*
	 * Write the sliver back to the filesystem, but only the blocks
	 * which are marked '0' in the bitmap.  Here we don't care about
	 * buffer offsets since we're block aligned now
	 */
	if (rw == SL_WRITE) {
		roff = mq->offset - (slvrno * SLASH_SLVR_SIZE);

		tsize = mq->size;
		sblk  = roff / SLASH_SLVR_BLKSZ;

		if (roff & SLASH_SLVR_BLKMASK)
			tsize += roff & SLASH_SLVR_BLKMASK;
	}

	for (i = 0; i < nslvrs; i++) {
		if (rw == SL_WRITE) {
			uint32_t tsz = MIN((SLASH_BLKS_PER_SLVR - sblk) *
			    SLASH_SLVR_BLKSZ, tsize);

			tsize -= tsz;
			rv = slvr_fsbytes_wio(slvr_ref[i], tsz, sblk);
			if (rv && rv != -SLERR_AIOWAIT) {
				rc = rv;
				goto out;
			}

			/* Only the first sliver may use a blk offset. */
			sblk = 0;
			slvr_wio_done(slvr_ref[i]);
		} else
			slvr_rio_done(slvr_ref[i]);

		slvr_ref[i] = NULL;
	}

	if (rw == SL_WRITE)
		psc_assert(!tsize);

 out:
	if (rc) {
		for (i = 0; i < nslvrs; i++) {
			if (slvr_ref[i] == NULL)
				continue;
			SLVR_LOCK(slvr_ref[i]);
			slvr_clear_inuse(slvr_ref[i], 0, SLASH_SLVR_SIZE);
			if (rw == SL_READ) {
				slvr_ref[i]->slvr_pndgreads--;
				slvr_lru_tryunpin_locked(slvr_ref[i]);
			} else {
				slvr_try_crcsched_locked(slvr_ref[i]);
			}

			SLVR_ULOCK(slvr_ref[i]);

			DEBUG_SLVR(PLL_WARN, slvr_ref[i],
			    "unwind ref due to bulk error (rw=%s)",
			    rw == SL_WRITE ? "wr" : "rd");
		}
	}
	if (bmap)
		bmap_op_done(bmap);

	/*
	 * XXX In situations where errors occur (such as an ENOSPC from
	 * iod_inode_open()) then we must have a way to notify other
	 * threads blocked on DATARDY.
	 */
	fcmh_op_done(f);
	return (rc);
}

static int
sli_ric_handle_rlsbmap(struct pscrpc_request *rq)
{
	struct srt_bmapdesc *sbd, *newsbd, *p;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct bmap_iod_info *bii;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	int rc, sync, fsync_time = 0;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);

	OPSTAT_INCR(SLI_OPST_RELEASE_BMAP);
	if (mq->nbmaps > MAX_BMAP_RELEASE)
		PFL_GOTOERR(out, mp->rc = -E2BIG);

	for (i = 0, sync = 0; i < mq->nbmaps; i++, sync = 0) {
		sbd = &mq->sbd[i];
		rc = sli_fcmh_get_rlsbmap(&sbd->sbd_fg, &f);
		psc_assert(rc == 0);

		/* Fsync here to guarantee that buffers are flushed to
		 *   disk before the MDS releases its odtable entry for
		 *   this bmap.
		 */
		FCMH_LOCK(f);
		if (!(f->fcmh_flags & FCMH_NO_BACKFILE))
			sync = 1;
		FCMH_ULOCK(f);
		if (sync) {
			fsync_time = CURRENT_SECONDS;
			rc = fsync(fcmh_2_fd(f));
			fsync_time = CURRENT_SECONDS - fsync_time;

#define NOTIFY_FSYNC_TIMEOUT 10 /* seconds */
			if (fsync_time > NOTIFY_FSYNC_TIMEOUT)
				DEBUG_FCMH(PLL_NOTICE, f,
				    "long fsync %d", fsync_time);
			if (rc)
				DEBUG_FCMH(PLL_ERROR, f,
				    "fsync failure rc=%d fd=%d errno=%d",
				    rc, fcmh_2_fd(f), errno);
		}

		rc = bmap_get(f, sbd->sbd_bmapno, SL_WRITE, &b);
		if (rc) {
			psclog_errorx("failed to load bmap %u",
			    sbd->sbd_bmapno);
			fcmh_op_done(f);
			continue;
		}
		fcmh_op_done(f);

		newsbd = psc_pool_get(bmap_rls_pool);
		memcpy(newsbd, sbd, sizeof(*sbd));

		bii = bmap_2_bii(b);
		BII_LOCK(bii);
		PLL_FOREACH(p, &bii->bii_rls) {
			if (!memcmp(p, sbd, sizeof(*p))) {
				BII_ULOCK(bii);
				bmap_op_done(b);
				psc_pool_return(bmap_rls_pool, newsbd);
				newsbd = NULL;
				break;
			}
		}
		if (!newsbd)
			continue;

		DEBUG_FCMH(PLL_INFO, f, "bmapno=%d seq=%"PRId64" key=%"PRId64
		   " (brls=%p)", b->bcm_bmapno, sbd->sbd_seq, sbd->sbd_key,
		   newsbd);

		bmap_op_start_type(bii_2_bmap(bii), BMAP_OPCNT_RLSSCHED);

		pll_add(&bii->bii_rls, newsbd);
		BMAP_SETATTR(b, BMAP_IOD_RLSSEQ);
		biod_rlssched_locked(bii);
		BII_ULOCK(bii);

		bmap_op_done(b);
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
			rc = -SLERR_NOTCONN;
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
	struct sli_exp_cli *iexpc;

	iexpc = exp->exp_private = PSCALLOC(sizeof(*iexpc));
	sli_getclcsvc(exp);
}

struct sl_expcli_ops sl_expcli_ops = {
	iexpc_allocpri,
	NULL
};
a->aiocbr_csvc = sli_getclcsvc(rq->rq_export);

	spinlock(&a->aiocbr_lock);
	a->aiocbr_flags |= SLI_AIOCBSF_READY;
	freelock(&a->aiocbr_lock);
}

int
sli_aio_register(struct slvr_ref *s, struct sli_aiocb_reply **aiocbrp,
    int issue)
{
	struct sli_aiocb_reply *a;
	struct sli_iocb *iocb;
	struct aiocb *aio;
	int error = SLERR_AIOWAIT;

	a = *aiocbrp;

	if (!a)
		a = *aiocbrp = sli_aio_aiocbr_new();

	DEBUG_SLVR(PLL_DIAG, s, "issue=%d *aiocbrp=%p", issue,
	    *aiocbrp);

	if (!issue)
		/* Not called from slvr_fsio(). */
		goto out;

	iocb = sli_aio_iocb_new(s);

	SLVR_LOCK(s);
	psc_assert(!(s->slvr_flags & SLVR_DATARDY));
	psc_assert(s->slvr_flags & SLVR_AIOWAIT);
	psc_assert(!s->slvr_iocb);

	s->slvr_iocb = iocb;
	SLVR_ULOCK(s);

	aio = &iocb->iocb_aiocb;
	aio->aio_fildes = slvr_2_fd(s);

	/* Read the entire sliver. */
	aio->aio_offset = slvr_2_fileoff(s, 0);
	aio->aio_buf = slvr_2_buf(s, 0);
	aio->aio_nbytes = SLASH_SLVR_SIZE;

	aio->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
	aio->aio_sigevent.sigev_signo = SIGIO;
	aio->aio_sigevent.sigev_value.sival_ptr = (void *)aio;

	lc_add(&sli_iocb_pndg, iocb);
	error = aio_read(aio);
	if (error == 0) {
		error = SLERR_AIOWAIT;
		psclog_info("aio_read: fd=%d iocb=%p sliver=%p",
		    aio->aio_fildes, iocb, s);
	} else {
		psclog_warn("aio_read: fd=%d iocb=%p sliver=%p error=%d",
		    aio->aio_fildes, iocb, s, error);
		lc_remove(&sli_iocb_pndg, iocb);
		slvr_iocb_release(iocb);
	}
 out:
	return (-error);
}

__static ssize_t
slvr_fsio(struct slvr_ref *s, int sblk, uint32_t size, enum rw rw,
    struct sli_aiocb_reply **aiocbr)
{
	int i, nblks, save_errno = 0;
	uint64_t *v8;
	ssize_t	rc;
	size_t off;

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;
	psc_assert(sblk + nblks <= SLASH_BLKS_PER_SLVR);

	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	off = slvr_2_fileoff(s, sblk);

	if (rw == SL_READ) {
		OPSTAT_INCR(SLI_OPST_FSIO_READ);
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		errno = 0;
		if (globalConfig.gconf_async_io) {
			s->slvr_flags |= SLVR_AIOWAIT;
			SLVR_WAKEUP(s);
			SLVR_ULOCK(s);

			return (sli_aio_register(s, aiocbr, 1));
		}
		SLVR_ULOCK(s);

		rc = pread(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
		    off);

		if (psc_fault_here_rc(SLI_FAULT_FSIO_READ_FAIL, &errno,
		    EBADF))
			rc = -1;

		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR(SLI_OPST_FSIO_READ_FAIL);
		}

		/*
		 * XXX this is a bit of a hack.  Here we'll check CRCs
		 *  only when nblks == an entire sliver.  Only RMW will
		 *  have their checks bypassed.  This should probably be
		 *  handled more cleanly, like checking for RMW and then
		 *  grabbing the CRC table; we use the 1MB buffer in
		 *  either case.
		 */

		/* XXX do the right thing when EOF is reached. */
		if (rc > 0 && nblks == SLASH_BLKS_PER_SLVR) {
			int crc_rc;

			crc_rc = slvr_do_crc(s);
			if (crc_rc == SLERR_BADCRC)
				DEBUG_SLVR(PLL_ERROR, s,
				    "bad crc blks=%d off=%zu",
				    nblks, off);
		}
	} else {
		OPSTAT_INCR(SLI_OPST_FSIO_WRITE);
		for (i = 0; i < nblks; i++)
			psc_vbitmap_unset(s->slvr_slab->slb_inuse,
			    sblk + i);

		errno = 0;
		SLVR_ULOCK(s);

		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
		    off);
		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR(SLI_OPST_FSIO_WRITE_FAIL);
		}
	}

	if (rc < 0)
		DEBUG_SLVR(PLL_ERROR, s, "failed (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, off, save_errno);

	else if ((uint32_t)rc != size) {
		DEBUG_SLVR(off + size > slvr_2_fcmh(s)->
		    fcmh_sstb.sst_size ? PLL_DIAG : PLL_NOTICE, s,
		    "short I/O (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, off, save_errno);
	} else {
		v8 = slvr_2_buf(s, sblk);
		DEBUG_SLVR(PLL_INFO, s, "ok %s size=%u off=%zu"
		    " rc=%zd nblks=%d v8(%"PRIx64")",
		    (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    size, off, rc, nblks, *v8);
		rc = 0;
	}

	//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);

	return (rc < 0 ? -save_errno : 0);
}

/**
 * slvr_fsbytes_get - Read in the blocks which have their respective
 *	bits set in slab bitmap, trying to coalesce where possible.
 * @s: the sliver.
 */
ssize_t
slvr_fsbytes_rio(struct slvr_ref *s, struct sli_aiocb_reply **aiocbr)
{
	int i, blk, nblks;
	ssize_t rc;

	psclog_debug("psc_vbitmap_nfree() = %d",
	    psc_vbitmap_nfree(s->slvr_slab->slb_inuse));

	if (!(s->slvr_flags & SLVR_DATARDY))
		psc_assert(s->slvr_flags & SLVR_FAULTING);

	psc_assert(s->slvr_flags & SLVR_PINNED);

	rc = 0;
	blk = 0; /* gcc */
	for (i = 0, nblks = 0; i < SLASH_BLKS_PER_SLVR; i++) {
		if (psc_vbitmap_get(s->slvr_slab->slb_inuse, i)) {
			if (nblks == 0)
				blk = i;

			nblks++;
			continue;
		}
		if (nblks) {
			rc = slvr_fsio(s, blk, nblks * SLASH_SLVR_BLKSZ,
			    SL_READ, aiocbr);
			if (rc)
				goto out;

			/* reset nblks so we won't do it again later */
			nblks = 0;
		}
	}

	if (nblks)
		rc = slvr_fsio(s, blk, nblks * SLASH_SLVR_BLKSZ,
		    SL_READ, aiocbr);
 out:
	if (rc == -SLERR_AIOWAIT)
		return (rc);

	else if (rc) {
		/*
		 * There was a problem; unblock any waiters and tell
		 * them the bad news.
		 */
		SLVR_LOCK(s);
		s->slvr_flags |= SLVR_DATAERR;
		s->slvr_flags &= ~SLVR_FAULTING;
		DEBUG_SLVR(PLL_ERROR, s, "slvr_fsio() error, rc=%zd", rc);
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);
	}

	return (rc);
}

ssize_t
slvr_fsbytes_wio(struct slvr_ref *s, uint32_t size, uint32_t sblk)
{
	DEBUG_SLVR(PLL_INFO, s, "sblk=%u size=%u", sblk, size);

	return (slvr_fsio(s, sblk, size, SL_WRITE, NULL));
}

void
slvr_repl_prep(struct slvr_ref *s, int src_or_dst)
{
	SLVR_LOCK(s);

	if (src_or_dst & SLVR_REPLDST) {
		psc_assert(s->slvr_pndgwrts > 0);

		if (s->slvr_flags & SLVR_DATARDY) {
			/* The slvr is about to be overwritten by this
			 *    replication request. For sanity's sake, wait
			 *    for pending io competion and set 'faulting'
			 *    before proceeding.
			 */
			DEBUG_SLVR(PLL_WARN, s,
				   "mds requested repldst of active slvr");
			SLVR_WAIT(s, ((s->slvr_pndgwrts > 1) ||
				      s->slvr_pndgreads));
			s->slvr_flags &= ~SLVR_DATARDY;
		}

		s->slvr_flags |= (SLVR_FAULTING | src_or_dst);

	} else {
		psc_assert(s->slvr_pndgreads > 0);
	}

	DEBUG_SLVR(PLL_INFO, s, "replica_%s",
	    (src_or_dst & SLVR_REPLDST) ? "dst" : "src");

	SLVR_ULOCK(s);
}

void
slvr_slab_prep(struct slvr_ref *s, enum rw rw)
{
	struct sl_buffer *tmp = NULL;

	//XXX may have to lock bmap instead..
	SLVR_LOCK(s);

 restart:
	/*
	 * slvr_lookup() must pin all slvrs to avoid racing with the
	 * reaper.
	 */
	psc_assert(s->slvr_flags & SLVR_PINNED);

	if (rw == SL_WRITE)
		psc_assert(s->slvr_pndgwrts > 0);
	else
		psc_assert(s->slvr_pndgreads > 0);

 newbuf:
	if (s->slvr_flags & SLVR_NEW) {
		if (!tmp) {
			/*
			 * Drop the lock before potentially blocking in
			 * the pool reaper.  To do this we must first
			 * allocate to a tmp pointer.
			 */

 getbuf:
			SLVR_ULOCK(s);

			tmp = psc_pool_get(sl_bufs_pool);
			sl_buffer_fresh_assertions(tmp);
			sl_buffer_clear(tmp, tmp->slb_blksz * tmp->slb_nblks);
			SLVR_LOCK(s);
			goto newbuf;

		} else
			psc_assert(tmp);

		psc_assert(psclist_disjoint(&s->slvr_lentry));
		s->slvr_flags &= ~SLVR_NEW;
		s->slvr_slab = tmp;
		tmp = NULL;
		/*
		 * Until the slab is added to the sliver, the sliver is
		 * private to the bmap's biod_slvrtree.
		 */
		s->slvr_flags |= SLVR_LRU;
		/* note: lc_addtail() will grab the list lock itself */
		lc_addtail(&lruSlvrs, s);

	} else if ((s->slvr_flags & SLVR_LRU) && !s->slvr_slab) {
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		if (!tmp)
			goto getbuf;
		s->slvr_slab = tmp;
		tmp = NULL;

	} else if (s->slvr_flags & SLVR_SLBFREEING) {
		DEBUG_SLVR(PLL_INFO, s, "caught slbfreeing");
		SLVR_WAIT(s, (s->slvr_flags & SLVR_SLBFREEING));
		goto restart;
	}

	DEBUG_SLVR(PLL_INFO, s, "should have slab");
	psc_assert(s->slvr_slab);
	SLVR_ULOCK(s);

	if (tmp)
		psc_pool_return(sl_bufs_pool, tmp);
}

/**
 * slvr_io_prep - Prepare a sliver for an incoming I/O.  This may entail
 *   faulting 32k aligned regions in from the underlying fs.
 * @s: the sliver
 * @off: offset into the slvr (not bmap or file object)
 * @len: len relative to the slvr
 * @rw: read or write op
 */
ssize_t
slvr_io_prep(struct slvr_ref *s, uint32_t off, uint32_t len, enum rw rw,
    struct sli_aiocb_reply **aiocbr)
{
	int i, blks, unaligned[2] = { -1, -1 };
	ssize_t rc = 0;

	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);

	/*
	 * Note we have taken our read or write references, so the
	 * sliver won't be freed from under us.
	 */
	if (s->slvr_flags & SLVR_FAULTING &&
	    !(s->slvr_flags & SLVR_REPLDST)) {
		/*
		 * Common courtesy requires us to wait for another
		 * thread's work FIRST.  Otherwise, we could bail out
		 * prematurely when the data is ready without
		 * considering the range we want to write.
		 */
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		SLVR_WAIT(s, !(s->slvr_flags &
		    (SLVR_DATARDY | SLVR_DATAERR | SLVR_AIOWAIT)));

		psc_assert((s->slvr_flags &
		    (SLVR_DATARDY | SLVR_DATAERR | SLVR_AIOWAIT)));

		if (s->slvr_flags & SLVR_AIOWAIT) {
			SLVR_ULOCK(s);
			psc_assert(globalConfig.gconf_async_io);

			return (sli_aio_register(s, aiocbr, 0));
		}
	}

	DEBUG_SLVR(s->slvr_flags & SLVR_DATAERR ?
	    PLL_ERROR : PLL_INFO, s,
	    "slvrno=%hu off=%u len=%u rw=%d",
	    s->slvr_num, off, len, rw);

	if (s->slvr_flags & SLVR_DATAERR) {
		rc = -1;
		goto out;

	} else if (s->slvr_flags & SLVR_DATARDY) {
		if (rw == SL_READ)
			goto out;

	} else if (!(s->slvr_flags & SLVR_REPLDST)) {
		/*
		 * Importing data into the sliver is now our
		 * responsibility, other I/O into this region will block
		 * until SLVR_FAULTING is released.
		 */
		s->slvr_flags |= SLVR_FAULTING;
		if (rw == SL_READ) {
			psc_vbitmap_setall(s->slvr_slab->slb_inuse);
			goto do_read;
		}

	} else if (s->slvr_flags & SLVR_REPLDST) {
		/*
		 * The sliver is going to be used for replication.
		 * Ensure proper setup has occurred.
		 */
		psc_assert(!off);
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		psc_assert(s->slvr_pndgreads == 0 &&
		    s->slvr_pndgwrts == 1);

		blks = len / SLASH_SLVR_BLKSZ +
		    (len & SLASH_SLVR_BLKMASK) ? 1 : 0;

		psc_vbitmap_setrange(s->slvr_slab->slb_inuse, 0, blks);
		SLVR_ULOCK(s);

		return (0);
	}

	psc_assert(rw != SL_READ);

	if (!off && len == SLASH_SLVR_SIZE) {
		/* Full sliver write, no need to read blocks from disk.
		 *  All blocks will be dirtied by the incoming network IO.
		 */
		psc_vbitmap_setall(s->slvr_slab->slb_inuse);
		goto out;
	}

	/* FixMe: Check the underlying file size to avoid useless RMW */
	OPSTAT_INCR(SLI_OPST_IO_PREP_RMW);

	/*
	 * Prepare the sliver for a read-modify-write.  Mark the blocks
	 * that need to be read as 1 so that they can be faulted in by
	 * slvr_fsbytes_io().  We can have at most two unaligned writes.
	 */
	if (off) {
		blks = (off / SLASH_SLVR_BLKSZ);
		if (off & SLASH_SLVR_BLKMASK)
			unaligned[0] = blks;

		for (i=0; i <= blks; i++)
			psc_vbitmap_set(s->slvr_slab->slb_inuse, i);
	}
	if ((off + len) < SLASH_SLVR_SIZE) {
		blks = (off + len) / SLASH_SLVR_BLKSZ;
		if ((off + len) & SLASH_SLVR_BLKMASK)
			unaligned[1] = blks;

		/* XXX use psc_vbitmap_setrange() */
		for (i = blks; i < SLASH_BLKS_PER_SLVR; i++)
			psc_vbitmap_set(s->slvr_slab->slb_inuse, i);
	}

	//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);
	psclog_info("psc_vbitmap_nfree()=%d",
	    psc_vbitmap_nfree(s->slvr_slab->slb_inuse));

	/* We must have found some work to do. */
	psc_assert(psc_vbitmap_nfree(s->slvr_slab->slb_inuse) <
		   SLASH_BLKS_PER_SLVR);

	if (s->slvr_flags & SLVR_DATARDY)
		goto invert;

	s->slvr_flags |= SLVR_RDMODWR;

 do_read:
	SLVR_ULOCK(s);
	/* Execute read to fault in needed blocks after dropping
	 *   the lock.  All should be protected by the FAULTING bit.
	 */
	if ((rc = slvr_fsbytes_rio(s, aiocbr)))
		return (rc);

	if (rw == SL_READ) {
		SLVR_LOCK(s);
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;

		psc_vbitmap_invert(s->slvr_slab->slb_inuse);
		//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);
		DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);

		return (0);
	}

	/*
	 * Above, the bits were set for the RMW blocks, now that they
	 * have been read, invert the bitmap so that it properly
	 * represents the blocks to be dirtied by the RPC.
	 */
	SLVR_LOCK(s);

 invert:
	psc_vbitmap_invert(s->slvr_slab->slb_inuse);
	if (unaligned[0] >= 0)
		psc_vbitmap_set(s->slvr_slab->slb_inuse, unaligned[0]);

	if (unaligned[1] >= 0)
		psc_vbitmap_set(s->slvr_slab->slb_inuse, unaligned[1]);

//	psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);

 out:
	if (!rc && s->slvr_flags & SLVR_FAULTING) {
		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;
		DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");
		SLVR_WAKEUP(s);
	}
	SLVR_ULOCK(s);
	return (rc);
}

void
slvr_rio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);

	s->slvr_pndgreads--;
	if (slvr_lru_tryunpin_locked(s)) {
		slvr_lru_requeue(s, 1);
		DEBUG_SLVR(PLL_INFO, s, "decref, unpinned");
	} else
		DEBUG_SLVR(PLL_INFO, s, "decref, ops still pending or dirty");

	SLVR_ULOCK(s);
}

__static void
slvr_schedule_crc_locked(struct slvr_ref *s)
{
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_LRU);

	if (!s->slvr_dirty_cnt) {
		psc_atomic32_inc(&slvr_2_bii(s)->bii_crcdrty_slvrs);
		s->slvr_dirty_cnt++;
	}

	DEBUG_SLVR(PLL_INFO, s, "crc sched (ndirty slvrs=%u)",
	    psc_atomic32_read(&slvr_2_bii(s)->bii_crcdrty_slvrs));

	s->slvr_flags &= ~SLVR_LRU;

	lc_remove(&lruSlvrs, s);
	lc_addqueue(&crcqSlvrs, s);
}

void slvr_slb_free_locked(struct slvr_ref *, struct psc_poolmgr *);

void
slvr_try_crcsched_locked(struct slvr_ref *s)
{
	if ((s->slvr_flags & SLVR_LRU) && s->slvr_pndgwrts > 1)
		slvr_lru_requeue(s, 1);

	/*
	 * If there are no more pending writes, schedule a CRC op.
	 * Increment slvr_compwrts to prevent a CRC op from being
	 * skipped which can happen due to the release of the slvr lock
	 * being released prior to the CRC of the buffer.
	 */
	s->slvr_pndgwrts--;
	s->slvr_compwrts++;

	DEBUG_SLVR(PLL_DEBUG, s, "decref");

	if (!s->slvr_pndgwrts && (s->slvr_flags & SLVR_LRU)) {
		if (s->slvr_flags & SLVR_CRCDIRTY)
			slvr_schedule_crc_locked(s);
		else
			slvr_lru_tryunpin_locked(s);
	}
}

int
slvr_lru_tryunpin_locked(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab);
	if (s->slvr_pndgwrts || s->slvr_pndgreads ||
	    s->slvr_flags & SLVR_CRCDIRTY || s->slvr_flags & SLVR_CRCING)
		return (0);

	psc_assert(s->slvr_flags & SLVR_LRU);
	psc_assert(s->slvr_flags & SLVR_PINNED);

	psc_assert(s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR));
	psc_assert(!(s->slvr_flags & (SLVR_NEW | SLVR_GETSLAB)));

	if ((s->slvr_flags & SLVR_REPLDST) == 0)
		psc_assert(!(s->slvr_flags & SLVR_FAULTING));

	s->slvr_flags &= ~SLVR_PINNED;

	if (s->slvr_flags & (SLVR_DATAERR | SLVR_REPLFAIL)) {
		s->slvr_flags |= SLVR_SLBFREEING;
		slvr_slb_free_locked(s, sl_bufs_pool);
	}

	return (1);
}

/**
 * slvr_wio_done - Called after a write RPC has completed.  The sliver
 *	may be FAULTING which is handled separately from DATARDY.  If
 *	FAULTING, this thread must wake up sleepers on the bmap waitq.
 * Notes: conforming with standard lock ordering, this routine drops
 *    the sliver lock prior to performing list operations.
 */
void
slvr_wio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_pndgwrts > 0);

	if (s->slvr_flags & SLVR_REPLDST) {
		/*
		 * This was a replication dest slvr.  Adjust the slvr
		 * flags so that the slvr may be freed on demand.
		 */
		if (s->slvr_flags & SLVR_REPLFAIL)
			DEBUG_SLVR(PLL_ERROR, s, "replication failure");
		else
			DEBUG_SLVR(PLL_INFO, s, "replication complete");

		psc_assert(s->slvr_pndgwrts == 1);
		psc_assert(s->slvr_flags & SLVR_PINNED);
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		psc_assert(!(s->slvr_flags & SLVR_CRCDIRTY));
		s->slvr_pndgwrts--;
		s->slvr_flags &= ~(SLVR_PINNED | SLVR_AIOWAIT |
		    SLVR_FAULTING | SLVR_REPLDST);

		if (s->slvr_flags & SLVR_REPLFAIL) {
			/*
			 * Perhaps this should block for any readers?
			 * Technically it should be impossible since
			 * this replica has yet to be registered with
			 * the MDS.
			 */
			s->slvr_flags |= SLVR_SLBFREEING;
			slvr_slb_free_locked(s, sl_bufs_pool);
			s->slvr_flags &= ~SLVR_REPLFAIL;

		} else {
			s->slvr_flags |= SLVR_DATARDY;
			SLVR_WAKEUP(s);
		}

		DEBUG_SLVR(PLL_DEBUG, s, "decref");

		slvr_lru_requeue(s, 0);
		SLVR_ULOCK(s);

		return;
	}

	PFL_GETTIMESPEC(&s->slvr_ts);

	s->slvr_flags |= SLVR_CRCDIRTY;
	s->slvr_flags &= ~SLVR_RDMODWR;

	if (!(s->slvr_flags & SLVR_DATARDY))
		DEBUG_SLVR(PLL_FATAL, s, "invalid state");

	slvr_try_crcsched_locked(s);
	SLVR_ULOCK(s);
}

/**
 * slvr_lookup - Lookup or create a sliver reference, ignoring one that
 *	is being freed.
 */
struct slvr_ref *
_slvr_lookup(const struct pfl_callerinfo *pci, uint32_t num,
    struct bmap_iod_info *bii, enum rw rw)
{
	struct slvr_ref *s, *tmp = NULL, ts;

	psc_assert(rw == SL_WRITE || rw == SL_READ);

	ts.slvr_num = num;

 retry:
	BII_LOCK(bii);
	s = SPLAY_FIND(biod_slvrtree, &bii->bii_slvrs, &ts);
	if (s) {
		SLVR_LOCK(s);
		if (s->slvr_flags & SLVR_FREEING) {
			SLVR_ULOCK(s);
			/*
			 * Lock is required to free the slvr.
			 * It must be held here to prevent the slvr
			 * from being freed before we release the lock.
			 */
			BII_ULOCK(bii);
			goto retry;

		} else {
			BII_ULOCK(bii);

			s->slvr_flags |= SLVR_PINNED;

			if (rw == SL_WRITE)
				s->slvr_pndgwrts++;
			else
				s->slvr_pndgreads++;
		}
		SLVR_ULOCK(s);

		if (tmp)
			psc_pool_return(slvr_pool, tmp);

	} else {
		if (!tmp) {
			BII_ULOCK(bii);
			tmp = psc_pool_get(slvr_pool);
			goto retry;
		}

		s = tmp;
		memset(s, 0, sizeof(*s));
		s->slvr_num = num;
		s->slvr_flags = SLVR_NEW | SLVR_SPLAYTREE | SLVR_PINNED;
		s->slvr_pri = bii;
		INIT_PSC_LISTENTRY(&s->slvr_lentry);
		INIT_SPINLOCK(&s->slvr_lock);
		pll_init(&s->slvr_pndgaios, struct sli_aiocb_reply,
		    aiocbr_lentry, &s->slvr_lock);

		if (rw == SL_WRITE)
			s->slvr_pndgwrts = 1;
		else
			s->slvr_pndgreads = 1;

		PSC_SPLAY_XINSERT(biod_slvrtree, &bii->bii_slvrs, s);
		bmap_op_start_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);
		BII_ULOCK(bii);
	}
	DEBUG_SLVR(PLL_DEBUG, s, "incref");
	return (s);
}

__static void
slvr_remove(struct slvr_ref *s)
{
	struct bmap_iod_info *bii;

	DEBUG_SLVR(PLL_DEBUG, s, "freeing slvr");

	/* Slvr should be detached from any listheads. */
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(!(s->slvr_flags & SLVR_SPLAYTREE));
	psc_assert(s->slvr_flags & SLVR_FREEING);

	bii = slvr_2_bii(s);

	BII_LOCK(bii);
	PSC_SPLAY_XREMOVE(biod_slvrtree, &bii->bii_slvrs, s);
	bmap_op_done_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

	psc_pool_return(slvr_pool, s);
}

void
slvr_slb_free_locked(struct slvr_ref *s, struct psc_poolmgr *m)
{
	struct sl_buffer *tmp = s->slvr_slab;

	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_flags & SLVR_SLBFREEING);
	psc_assert(!(s->slvr_flags & SLVR_FREEING));
	psc_assert(s->slvr_slab);

	s->slvr_flags &= ~(SLVR_SLBFREEING | SLVR_DATARDY | SLVR_DATAERR);

	DEBUG_SLVR(PLL_INFO, s, "freeing slvr slab=%p", s->slvr_slab);
	s->slvr_slab = NULL;
	SLVR_WAKEUP(s);

	psc_pool_return(m, tmp);
}

/**
 * slvr_buffer_reap - The reclaim function for sl_bufs_pool.  Note that
 *	our caller psc_pool_get() ensures that we are called
 *	exclusively.
 */
int
slvr_buffer_reap(struct psc_poolmgr *m)
{
	static struct psc_dynarray a;
	struct slvr_ref *s, *dummy;
	int i, n, locked;

	n = 0;
	psc_dynarray_init(&a);

	LIST_CACHE_LOCK(&lruSlvrs);
	LIST_CACHE_FOREACH_SAFE(s, dummy, &lruSlvrs) {
		DEBUG_SLVR(PLL_INFO, s, "considering for reap, nwaiters=%d",
			   atomic_read(&m->ppm_nwaiters));

		/*
		 * We are reaping, so it is fine to back off on some
		 * slivers.  We have to use a reqlock here because
		 * slivers do not have private spinlocks, instead they
		 * use the lock of the bii.  So if this thread tries to
		 * free a slvr from the same bii trylock will abort.
		 */
		if (!SLVR_TRYRLOCK(s, &locked))
			continue;

		/*
		 * Look for slvrs which can be freed;
		 * slvr_lru_freeable() returning true means that no slab
		 * is attached.
		 */
		if (slvr_lru_freeable(s)) {
			psc_dynarray_add(&a, s);
			s->slvr_flags |= SLVR_FREEING;
			lc_remove(&lruSlvrs, s);
			goto next;
		}

		if (slvr_lru_slab_freeable(s)) {
			/*
			 * At this point we know that the slab can be
			 * reclaimed, however the slvr itself may have
			 * to stay.
			 */
			psc_dynarray_add(&a, s);
			s->slvr_flags |= SLVR_SLBFREEING;
			n++;
		}
 next:
		SLVR_URLOCK(s, locked);
		if (n >= atomic_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&lruSlvrs);

	for (i = 0; i < psc_dynarray_len(&a); i++) {
		s = psc_dynarray_getpos(&a, i);

		locked = SLVR_RLOCK(s);

		if (s->slvr_flags & SLVR_SLBFREEING) {
			slvr_slb_free_locked(s, m);
			SLVR_URLOCK(s, locked);

		} else if (s->slvr_flags & SLVR_FREEING) {
			psc_assert(!(s->slvr_flags & SLVR_SLBFREEING));
			psc_assert(!(s->slvr_flags & SLVR_PINNED));
			psc_assert(!s->slvr_slab);

			if (s->slvr_flags & SLVR_SPLAYTREE) {
				s->slvr_flags &= ~SLVR_SPLAYTREE;
				SLVR_ULOCK(s);
				slvr_remove(s);
			} else
				SLVR_URLOCK(s, locked);
		}
	}
	psc_dynarray_free(&a);

	if (!n || n < atomic_read(&m->ppm_nwaiters))
		psc_waitq_wakeone(&slvrWaitq);

	return (n);
}

void
sliaiothr_main(__unusedx struct psc_thread *thr)
{
	struct sli_iocb *iocb, *next;
	sigset_t signal_set;
	int signo;

	sigemptyset(&signal_set);
	sigaddset(&signal_set, SIGIO);

	for (;;) {
		sigwait(&signal_set, &signo);
		psc_assert(signo == SIGIO);

		LIST_CACHE_LOCK(&sli_iocb_pndg);
		LIST_CACHE_FOREACH_SAFE(iocb, next, &sli_iocb_pndg) {
			iocb->iocb_rc = aio_error(&iocb->iocb_aiocb);
			if (iocb->iocb_rc == EINVAL)
				continue;
			if (iocb->iocb_rc == EINPROGRESS)
				continue;
			psc_assert(iocb->iocb_rc != ECANCELED);
			if (iocb->iocb_rc == 0)
				iocb->iocb_len =
				    aio_return(&iocb->iocb_aiocb);

			psc_fault_here_rc(SLI_FAULT_AIO_FAIL,
			    &iocb->iocb_rc, EIO);

			psclog_info("got signal: iocb=%p", iocb);
			lc_remove(&sli_iocb_pndg, iocb);
			LIST_CACHE_ULOCK(&sli_iocb_pndg);
			iocb->iocb_cbf(iocb);	/* slvr_fsaio_done() */
			LIST_CACHE_LOCK(&sli_iocb_pndg);
		}
		LIST_CACHE_ULOCK(&sli_iocb_pndg);
	}
}

void
slvr_cache_init(void)
{
	psc_poolmaster_init(&slvr_poolmaster,
	    struct slvr_ref, slvr_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "slvr");
	slvr_pool = psc_poolmaster_getmgr(&slvr_poolmaster);

	lc_reginit(&lruSlvrs, struct slvr_ref, slvr_lentry, "lruslvrs");
	lc_reginit(&crcqSlvrs, struct slvr_ref, slvr_lentry, "crcqslvrs");

	if (globalConfig.gconf_async_io) {

		psc_poolmaster_init(&sli_iocb_poolmaster,
		    struct sli_iocb, iocb_lentry, PPMF_AUTO, 64, 64,
		    1024, NULL, NULL, NULL, "iocb");
		sli_iocb_pool = psc_poolmaster_getmgr(&sli_iocb_poolmaster);

		psc_poolmaster_init(&sli_aiocbr_poolmaster,
		    struct sli_aiocb_reply, aiocbr_lentry, PPMF_AUTO, 64,
		    64, 1024, NULL, NULL, NULL, "aiocbr");
		sli_aiocbr_pool = psc_poolmaster_getmgr(&sli_aiocbr_poolmaster);

		lc_reginit(&sli_iocb_pndg, struct sli_iocb, iocb_lentry,
		    "iocbpndg");

		pscthr_init(SLITHRT_ASYNC_IO, 0, sliaiothr_main, NULL,
		    0, "sliaiothr");
	}

	sl_buffer_cache_init();

	slvr_worker_init();
}

#if PFL_DEBUG > 0
void
dump_sliver(struct slvr_ref *s)
{
	DEBUG_SLVR(PLL_MAX, s, "");
}

void
dump_sliver_flags(int fl)
{
	int seq = 0;

	PFL_PRFLAG(SLVR_NEW, &fl, &seq);
	PFL_PRFLAG(SLVR_SPLAYTREE, &fl, &seq);
	PFL_PRFLAG(SLVR_FAULTING, &fl, &seq);
	PFL_PRFLAG(SLVR_GETSLAB, &fl, &seq);
	PFL_PRFLAG(SLVR_PINNED, &fl, &seq);
	PFL_PRFLAG(SLVR_DATARDY, &fl, &seq);
	PFL_PRFLAG(SLVR_DATAERR, &fl, &seq);
	PFL_PRFLAG(SLVR_LRU, &fl, &seq);
	PFL_PRFLAG(SLVR_CRCDIRTY, &fl, &seq);
	PFL_PRFLAG(SLVR_CRCING, &fl, &seq);
	PFL_PRFLAG(SLVR_FREEING, &fl, &seq);
	PFL_PRFLAG(SLVR_SLBFREEING, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLDST, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLFAIL, &fl, &seq);
	PFL_PRFLAG(SLVR_AIOWAIT, &fl, &seq);
	PFL_PRFLAG(SLVR_RDMODWR, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLWIRE, &fl, &seq);
	if (fl)
		printf(" unknown: %x", fl);
	printf("\n");
}
#endif
