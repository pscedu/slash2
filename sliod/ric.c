/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Routines for handling RPC requests for ION from CLIENT.
 */

#include <errno.h>
#include <stdio.h>

#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/opstats.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/service.h"

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

#define NOTIFY_FSYNC_TIMEOUT	10		/* seconds */

void				*sli_benchmark_buf;
uint32_t			 sli_benchmark_bufsiz;

int
sli_ric_write_sliver(uint32_t off, uint32_t size, struct slvr **slvrs,
    int nslvrs)
{
	int i, rc = 0;
	sl_bmapno_t slvrno;
	uint32_t roff, sblk, tsize;

	slvrno = off / SLASH_SLVR_SIZE;
	roff = off - (slvrno * SLASH_SLVR_SIZE);

	sblk  = roff / SLASH_SLVR_BLKSZ;
	tsize = size + (roff & SLASH_SLVR_BLKMASK);

	for (i = 0; i < nslvrs; i++) {
		uint32_t tsz = MIN((SLASH_BLKS_PER_SLVR - sblk) *
		    SLASH_SLVR_BLKSZ, tsize);

		tsize -= tsz;
		rc = slvr_fsbytes_wio(slvrs[i], sblk, tsz);
		if (rc) {
			psc_assert(rc != -SLERR_AIOWAIT);
			psclog_warnx("write error rc=%d", rc);
			break;
		}

		/* Only the first sliver may use a blk offset. */
		sblk = 0;
	}
	if (!rc)
		psc_assert(!tsize);

	return (rc);
}

__static int
sli_ric_handle_io(struct pscrpc_request *rq, enum rw rw)
{
	sl_bmapno_t bmapno, slvrno;
	int rc, nslvrs = 0, i, needaio = 0;
	uint32_t tsize, roff, len[RIC_MAX_SLVRS_PER_IO];
	struct slvr *s, *slvr[RIC_MAX_SLVRS_PER_IO];
	struct iovec iovs[RIC_MAX_SLVRS_PER_IO];
	struct sli_aiocb_reply *aiocbr = NULL;
	struct pfl_iostats_grad *ist;
	struct sl_fidgen *fgp;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct fidc_membh *f;
	struct bmap *bmap;
	uint64_t seqno;
	ssize_t rv;
	struct fcmh_iod_info *fii;

	SL_RSX_ALLOCREP(rq, mq, mp);

	fgp = &mq->sbd.sbd_fg;
	bmapno = mq->sbd.sbd_bmapno;

	if (mq->flags & SRM_IOF_DIO)
		OPSTAT_INCR("handle-dio");

	if (mq->size <= 0 || mq->size > LNET_MTU) {
		psclog_errorx("invalid size %u, fid:"SLPRI_FG,
		    mq->size, SLPRI_FG_ARGS(fgp));
		mp->rc = -EINVAL;
		return (mp->rc);
	}

	/* network stack test/benchmarking mode */
	if (mq->flags & SRM_IOF_BENCH) {
		static struct psc_spinlock lock = SPINLOCK_INIT;

		spinlock(&lock);
		if (mq->size > sli_benchmark_bufsiz) {
			sli_benchmark_buf = psc_realloc(
			    sli_benchmark_buf, mq->size, 0);
			sli_benchmark_bufsiz = mq->size;
		}
		freelock(&lock);

		iovs[0].iov_base = sli_benchmark_buf;
		iovs[0].iov_len = mq->size;
		mp->rc = slrpc_bulkserver(rq, rw == SL_WRITE ?
		    BULK_GET_SINK : BULK_PUT_SOURCE, SRIC_BULK_PORTAL,
		    iovs, 1);
		return (mp->rc);
	}

	/*
	 * Ensure that this request fits into the bmap's address range.
	 *
	 * XXX this check assumes that mq->offset has not been made
	 *     bmap relative (i.e. it's filewise).
	 */
	if (mq->offset + mq->size > SLASH_BMAP_SIZE) {
		psclog_errorx("req offset/size outside of the bmap's "
		    "address range off=%u len=%u",
		    mq->offset, mq->size);
		mp->rc = -ERANGE;
		return (mp->rc);
	}

	/*
	 * A RBW (read-before-write) request from the client may have a
	 * write enabled bmapdesc which he uses to fault in his page.
	 */
	mp->rc = bmapdesc_access_check(&mq->sbd, rw,
	    nodeResm->resm_res_id);
	if (mp->rc) {
		psclog_info("bmapdesc check failed for "
		    SLPRI_FG" self %x, peer %x",
		    SLPRI_FG_ARGS(fgp),
		    nodeResm->resm_res->res_id, mq->sbd.sbd_ios);
		return (mp->rc);
	}
	psclog_diag("bmapdesc check okay");

	if (rw == SL_READ)
		(void)pfl_fault_here_rc("sliod/seqno_read_fail",
		    &mp->rc, -PFLERR_KEYEXPIRED);
	else
		(void)pfl_fault_here_rc("sliod/seqno_write_fail",
		    &mp->rc, -PFLERR_KEYEXPIRED);
	if (mp->rc)
		return (mp->rc);

	seqno = bim_getcurseq();
	if (mq->sbd.sbd_seq < seqno) {
		/* Reject old bmapdesc. */
		psclog_warnx("op: %d, seq %"PRId64" < bim_getcurseq(%"PRId64")",
		    rw, mq->sbd.sbd_seq, seqno);
		mp->rc = -PFLERR_KEYEXPIRED;
		OPSTAT_INCR("key-expire");
		return (mp->rc);
	}

	/* XXX move this until after success and do accounting for errors */
	for (ist = sli_iorpc_iostats; ist->size; ist++)
		if (mq->size < ist->size)
			break;
	pfl_opstat_add(rw == SL_WRITE ? ist->rw.wr : ist->rw.rd, 1);

	mp->rc = sli_fcmh_get(fgp, &f);
	if (mp->rc)
		return (mp->rc);

	/*
	 * Currently we have LNET_MTU = SLASH_SLVR_SIZE = 1MiB,
	 * therefore we would never exceed two slivers.
	 */
	nslvrs = 1;
	slvrno = mq->offset / SLASH_SLVR_SIZE;
	if ((mq->offset + mq->size - 1) / SLASH_SLVR_SIZE > slvrno)
		nslvrs++;

	FCMH_LOCK(f);
	/* Update the utimegen if necessary. */
	if (f->fcmh_sstb.sst_utimgen < mq->utimgen)
		f->fcmh_sstb.sst_utimgen = mq->utimgen;

	if (rw == SL_WRITE) {
		/* simplistic tracking of dirty slivers, ignoring duplicates */
		fii = fcmh_2_fii(f);
		fii->fii_ndirty += nslvrs;
		if (!(f->fcmh_flags & FCMH_IOD_DIRTYFILE)) {
			lc_add(&sli_fcmh_dirty, f);
			f->fcmh_flags |= FCMH_IOD_DIRTYFILE;
		}
	}
	FCMH_ULOCK(f);

	rc = mp->rc = bmap_get(f, bmapno, rw, &bmap);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "failed to load bmap %u",
		    bmapno);
		PFL_GOTOERR(out1, rc);
	}

	DEBUG_FCMH(PLL_DIAG, f, "bmapno=%u size=%u off=%u rw=%s "
	    "sbd_seq=%"PRId64, bmap->bcm_bmapno, mq->size, mq->offset,
	    rw == SL_WRITE ? "wr" : "rd", mq->sbd.sbd_seq);


	/* Paranoid: clear more than necessary. */
	for (i = 0; i < RIC_MAX_SLVRS_PER_IO; i++) {
		slvr[i] = NULL;
		iovs[i].iov_len = 0;
		iovs[i].iov_base = 0;
	}

	/*
	 * This loop assumes that nslvrs is always no larger than
	 * RIC_MAX_SLVRS_PER_IO.
	 *
	 * Note that once i > 0, roff is always 0.
	 *
	 * Although we may be trying to set FAULTING bit on two slivers,
	 * they should be ordered by their offsets in the bmap.
	 */
	roff = mq->offset - slvrno * SLASH_SLVR_SIZE;
	for (i = 0, tsize = mq->size; i < nslvrs; i++, roff = 0) {

		slvr[i] = slvr_lookup(slvrno + i, bmap_2_bii(bmap));

		/* Fault in pages either for read or RBW. */
		len[i] = MIN(tsize, SLASH_SLVR_SIZE - roff);

		rv = slvr_io_prep(slvr[i], roff, len[i], rw, 0);

		DEBUG_SLVR(rv && rv != -SLERR_AIOWAIT ?
		    PLL_WARN : PLL_DIAG, slvr[i],
		    "post io_prep rw=%s rv=%zd",
		    rw == SL_WRITE ? "wr" : "rd", rv);
		if (rv == -SLERR_AIOWAIT)
			needaio = 1;
		else if (rv) {
			bmap_op_done(bmap);
			bmap = NULL;
			PFL_GOTOERR(out1, rc = mp->rc = rv);
		}

		/*
		 * mq->offset is the offset into the bmap, here we must
		 * translate it into the offset of the sliver.
		 *
		 * The client should not send us any read request that goes
		 * beyond the EOF. Otherwise, we are in trouble here because
		 * reading beyond EOF should return 0 bytes.
		 */
		iovs[i].iov_base = slvr[i]->slvr_slab->slb_base + roff;
		tsize -= iovs[i].iov_len = len[i];

		/*
		 * Avoid more complicated errors within LNET by ensuring
		 * that len is non-zero.
		 */
		psc_assert(iovs[i].iov_len > 0);
	}
	bmap_op_done(bmap);
	bmap = NULL;

	psc_assert(!tsize);

	if (needaio) {
		aiocbr = sli_aio_reply_setup(rq, mq->size, mq->offset,
		    slvr, nslvrs, iovs, nslvrs, rw);
		if (aiocbr == NULL)
			PFL_GOTOERR(out1, 0);
		if (mq->flags & SRM_IOF_DIO) {
			OPSTAT_INCR("aio-dio");
			aiocbr->aiocbr_flags |= SLI_AIOCBSF_DIO;
		}

		/*
		 * Now check for early completion.  If all slvrs are
		 * ready, then we must reply with the data now.
		 * Otherwise, we'll never be woken since the aio cb(s)
		 * have been run.
		 */
		for (i = 0; i < nslvrs; i++) {
			SLVR_LOCK(slvr[i]);
			if (slvr[i]->slvr_flags & SLVRF_FAULTING) {
				/*
				 * Attach the reply to the first sliver
				 * waiting for aio and return AIOWAIT to
				 * client later.
				 */
				slvr[i]->slvr_aioreply = aiocbr;
				OPSTAT_INCR("aio-return");
				SLVR_ULOCK(slvr[i]);
				DEBUG_SLVR(PLL_DIAG, slvr[i], "aio wait");
				rc = mp->rc = -SLERR_AIOWAIT;
				pscrpc_msg_add_flags(rq->rq_repmsg,
				    MSG_ABORT_BULK);
				goto out2;
			}
			if (!rc)
				rc = slvr[i]->slvr_err;
			DEBUG_SLVR(PLL_DIAG, slvr[i],
			    "aio early ready, rw=%s",
			    rw == SL_WRITE ? "wr" : "rd");
			SLVR_ULOCK(slvr[i]);
		}
		OPSTAT_INCR("aio-abort");

		/* All slvrs are ready. */
		sli_aio_aiocbr_release(aiocbr);
		if (rc) {
			pscrpc_msg_add_flags(rq->rq_repmsg,
			    MSG_ABORT_BULK);
			goto out2;
		}
	}

	/*
	 * We must return an error code to the RPC itself if we don't
	 * call slrpc_bulkserver() or slrpc_bulkclient() as expected.
	 */
	rc = mp->rc = slrpc_bulkserver(rq,
	    rw == SL_WRITE ? BULK_GET_SINK : BULK_PUT_SOURCE,
	    SRIC_BULK_PORTAL, iovs, nslvrs);
	if (rc) {
		psclog_warnx("bulkserver error on %s, rc=%d",
		    rw == SL_WRITE ? "write" : "read", rc);
		PFL_GOTOERR(out1, rc);
	}

	/*
	 * Write the sliver back to the filesystem.
	 */
	if (rw == SL_WRITE) {
		mp->rc = sli_ric_write_sliver(mq->offset, mq->size, slvr,
		    nslvrs);
		goto out1;
	}

 out1:
	for (i = 0; i < nslvrs && slvr[i]; i++) {
		s = slvr[i];

		slvr_io_done(s, rc);
		if (rw == SL_READ)
			slvr_rio_done(slvr[i]);
		else
			slvr_wio_done(slvr[i], 0);
	}

 out2:
	DEBUG_FCMH(PLL_DIAG, f, "bmapno=%u size=%u off=%u rw=%s "
	    "rc=%d", bmapno, mq->size, mq->offset,
	    rw == SL_WRITE ? "wr" : "rd", rc);

	if (bmap)
		bmap_op_done(bmap);

	fcmh_op_done(f);
	return (rc);
}

/*
 * XXX  We probably need a way to make sure that all data have been
 * written before fsync().
 */
__static int
sli_ric_handle_rlsbmap(struct pscrpc_request *rq)
{
	int rc, new, fsync_time = 0;
	struct srt_bmapdesc *sbd, *newsbd, *p;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct timespec ts0, ts1, delta;
	struct bmap_iod_info *bii;
	struct fidc_membh *f;
	struct bmap *b;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->nbmaps > MAX_BMAP_RELEASE)
		PFL_GOTOERR(out, mp->rc = -E2BIG);

#ifdef HAVE_SYNC_FILE_RANGE
	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];
		if (sli_fcmh_peek(&sbd->sbd_fg, &f))
			continue;
		FCMH_LOCK(f);
		if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
			FCMH_ULOCK(f);
			sync_file_range(fcmh_2_fd(f), sbd->sbd_bmapno *
			    SLASH_BMAP_SIZE, SLASH_BMAP_SIZE,
			    SYNC_FILE_RANGE_WRITE);
		}
		fcmh_op_done(f);
	}
#endif

	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];
		rc = sli_fcmh_peek(&sbd->sbd_fg, &f);
		if (rc) {
			OPSTAT_INCR("rlsbmap-fail");
			psclog(rc == ENOENT ? PLL_DIAG : PLL_ERROR,
			    "load fcmh failed; fid="SLPRI_FG" rc=%d",
			    SLPRI_FG_ARGS(&sbd->sbd_fg), rc);
			continue;
		}

		/*
		 * fsync here to guarantee that buffers are flushed to
		 * disk before the MDS releases its odtable entry for
		 * this bmap.
		 */
		FCMH_LOCK(f);
		if (f->fcmh_flags & FCMH_IOD_BACKFILE) {
			int error;

			FCMH_ULOCK(f);

			PFL_GETTIMESPEC(&ts0);

			fsync_time = CURRENT_SECONDS;
#ifdef HAVE_SYNC_FILE_RANGE
			rc = sync_file_range(fcmh_2_fd(f),
			    sbd->sbd_bmapno * SLASH_BMAP_SIZE,
			    SLASH_BMAP_SIZE,
			    SYNC_FILE_RANGE_WAIT_AFTER);
#else
			rc = fsync(fcmh_2_fd(f));
#endif
			fsync_time = CURRENT_SECONDS - fsync_time;

			if (rc == -1)
				error = errno;

			PFL_GETTIMESPEC(&ts1);
			timespecsub(&ts1, &ts0, &delta);
			OPSTAT_ADD("rlsbmap-fsync-usecs",
			    delta.tv_sec * 1000000 + delta.tv_nsec / 1000);

			if (fsync_time > NOTIFY_FSYNC_TIMEOUT) {
				OPSTAT_INCR("fsync-slow");
				DEBUG_FCMH(PLL_NOTICE, f,
				    "long fsync %d", fsync_time);
			}
			if (rc) {
				OPSTAT_INCR("fsync-fail");
				DEBUG_FCMH(PLL_ERROR, f,
				    "fsync failure rc=%d fd=%d errno=%d",
				    rc, fcmh_2_fd(f), error);
			}
			OPSTAT_INCR("fsync");
		} else
			FCMH_ULOCK(f);

		rc = bmap_get(f, sbd->sbd_bmapno, SL_WRITE, &b);
		if (rc) {
			psclog_errorx("failed to load bmap %u",
			    sbd->sbd_bmapno);
			fcmh_op_done(f);
			continue;
		}

		new = 1;
		bii = bmap_2_bii(b);
		PLL_FOREACH(p, &bii->bii_rls) {
			if (!memcmp(p, sbd, sizeof(*p))) {
				new = 0;
				break;
			}
		}
		if (new) {
			BMAP_ULOCK(b);
			newsbd = psc_pool_get(bmap_rls_pool);
			BMAP_LOCK(b);

			/*
			 * Reaper thread has marked this bmap for
			 * release so do a no-op.
			 */
			if (b->bcm_flags & BMAPF_RELEASING) {
				psc_pool_return(bmap_rls_pool, newsbd);
			} else {
				memcpy(newsbd, sbd, sizeof(*sbd));

				DEBUG_BMAP(PLL_DIAG, b, "brls=%p "
				    "seq=%"PRId64" key=%"PRId64,
				    newsbd, sbd->sbd_seq, sbd->sbd_key);

				pll_add(&bii->bii_rls, newsbd);
			}
		}

		bmap_op_done(b);
		fcmh_op_done(f);
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
			rc = -PFLERR_NOTCONN;
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
		if (sli_selftest_rc) {
			struct slashrpc_cservice *csvc;

			csvc = sli_getclcsvc(rq->rq_export);
			if (csvc)
				sli_rci_ctl_health_send(csvc);
		}
		break;
	case SRMT_READ:
		OPSTAT_INCR("handle-read");
		rc = sli_ric_handle_read(rq);
		break;
	case SRMT_WRITE:
		OPSTAT_INCR("handle-write");
		rc = sli_ric_handle_write(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = sli_ric_handle_rlsbmap(rq);
		break;
	default:
		psclog_errorx("unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
 out:
	slrpc_rep_out(rq);
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
	iexpc_allocpri
};
