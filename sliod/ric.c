/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
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

#include <sys/statvfs.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

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

#define MIN_SPACE_RESERVE_GB	32		/* min space in GiB */
#define MIN_SPACE_RESERVE_PCT	5		/* min space percentage */
#define MAX_WRITE_PER_FILE	2048		/* slivers */
#define NOTIFY_FSYNC_TIMEOUT	10		/* seconds */

void				*sli_benchmark_buf;
uint32_t			 sli_benchmark_bufsiz;

int				 sli_sync_max_writes = MAX_WRITE_PER_FILE;
int				 sli_min_space_reserve_gb = MIN_SPACE_RESERVE_GB;
int				 sli_min_space_reserve_pct = MIN_SPACE_RESERVE_PCT;

int				 sli_predio_pipe_size = 16;
int				 sli_predio_max_slivers = 4;

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

/*
 * Check if the local storage is near full and if we are writing
 * into a hole in the given file.
 */
int
sli_has_enough_space(struct fidc_membh *f, uint32_t bmapno,
    uint32_t b_off, uint32_t size)
{
	int fd, percentage;
	off_t rc = -1, f_off;

	if (f) {
		/*
 		 * First off, overwrite is always allowed.
 		 */
		fd = fcmh_2_fd(f);
		f_off = (off_t)bmapno * SLASH_BMAP_SIZE + b_off;

#ifdef SEEK_HOLE
		rc = lseek(fd, f_off, SEEK_HOLE);
#endif
		/*
		 * rc = -1 is possible if the backend file system 
		 * does not support it (e.g. ZFS on FreeBSD 9.0) 
		 * or the offset is beyond EOF.
		 */
		if (rc != -1 && f_off + size <= rc) {
			OPSTAT_INCR("space-overwrite");
			return (1);
		}
	}
	/*
 	 * Set sli_min_space_reserve_pct/gb to zero to disable
 	 * the reserve. We check percentage first because file
 	 * system does not do well when near full.
 	 */
	percentage = sli_statvfs_buf.f_bfree * 100 /
	    sli_statvfs_buf.f_blocks;

	if (percentage < sli_min_space_reserve_pct) {
		OPSTAT_INCR("space-reserve-pct");
		return (0);
	}

	if (sli_statvfs_buf.f_bfree * sli_statvfs_buf.f_bsize
	    < (unsigned long) sli_min_space_reserve_gb * 
	       1024 * 1024 * 1024) {
		OPSTAT_INCR("space-reserve-abs");
		return (0);
	}

	return (1);
}

void
readahead_enqueue(struct fidc_membh *f, off_t off, off_t size)
{
	struct sli_readaheadrq *rarq;

#ifdef MYDEBUG
	psclog_max("readahead: offset = %ld, size = %ld\n", off, size);
#endif
	rarq = psc_pool_get(sli_readaheadrq_pool);
	INIT_PSC_LISTENTRY(&rarq->rarq_lentry);
	rarq->rarq_fg = f->fcmh_fg;
	rarq->rarq_off = off;
	rarq->rarq_size = size;

	/* feed work to slirathr_main() */
	lc_add(&sli_readaheadq, rarq);
}

void
sli_enqueue_update(struct fidc_membh *f)
{
	struct timeval now;
	struct fcmh_iod_info *fii;

	fii = fcmh_2_fii(f);
	PFL_GETTIMEVAL(&now);
	if (!(f->fcmh_flags & FCMH_IOD_UPDATEFILE)) {
		OPSTAT_INCR("fcmh-update-enqueue");
		lc_add(&sli_fcmh_update, fii);
		f->fcmh_flags |= FCMH_IOD_UPDATEFILE;
		fcmh_op_start_type(f, FCMH_OPCNT_UPDATE);
	} else {
		OPSTAT_INCR("fcmh-update-requeue");
		lc_move2tail(&sli_fcmh_update, fii);
	}
	fii->fii_nwrites++;
	fii->fii_lastwrite = now.tv_sec;
}

__static int
sli_ric_handle_io(struct pscrpc_request *rq, enum rw rw)
{
	sl_bmapno_t bmapno, slvrno;
	int rc, nslvrs = 0, i, delta, needaio = 0;
	uint32_t tsize, roff, len[RIC_MAX_SLVRS_PER_IO];
	struct slvr *s, *slvr[RIC_MAX_SLVRS_PER_IO];
	struct iovec iovs[RIC_MAX_SLVRS_PER_IO];
	struct sli_aiocb_reply *aiocbr = NULL;
	struct fcmh_iod_info *fii;
	struct bmap *bmap = NULL;
	struct sl_fidgen *fgp;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct fidc_membh *f;
	uint64_t seqno;
	ssize_t rv;
	off_t off, raoff, rasize;

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
	 * Our client should already make sure that mq->offset is relative
	 * to the start of a bmap (i.e., it's NOT filewise).
	 */
	if (mq->offset + mq->size > SLASH_BMAP_SIZE) {
		psclog_errorx("req offset/size outside of the bmap's "
		    "address range off=%u len=%u",
		    mq->offset, mq->size);
		mp->rc = -ERANGE;
		OPSTAT_INCR("io-erange");
		return (mp->rc);
	}

	/*
	 * A RBW (read-before-write) request from the client may have a
	 * write enabled bmapdesc which he/she uses to fault in his page.
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
		pfl_fault_here_rc(&mp->rc, -PFLERR_KEYEXPIRED,
		    "sliod/seqno_read_fail");
	else
		pfl_fault_here_rc(&mp->rc, -PFLERR_KEYEXPIRED,
		    "sliod/seqno_write_fail");
	if (mp->rc)
		return (mp->rc);

	/*
	 * Limit key checking for write for now until the client
	 * side can extend read lease in the background as well.
	 */
	seqno = bim_getcurseq();
	if (rw == SL_WRITE && mq->sbd.sbd_seq < seqno) {
		/* Reject old bmapdesc. */
		psclog_warnx("%s: seq %"PRId64" < bim_getcurseq(%"PRId64")",
		    rw == SL_WRITE ? "write" : "read", 
		    mq->sbd.sbd_seq, seqno);
#if 0
		psc_fatalx("key expired, mq = %p", mp);
#endif
		mp->rc = -PFLERR_KEYEXPIRED;
		OPSTAT_INCR("key-expire");
		return (mp->rc);
	}

	/*
	 * XXX move this until after success and do accounting for
	 * errors.
	 */
	pfl_opstats_grad_incr(rw == SL_WRITE ?
	    &sli_iorpc_iostats_wr : &sli_iorpc_iostats_rd, mq->size);

	mp->rc = -sli_fcmh_get(fgp, &f);
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

	/* Paranoid: clear more than necessary. */
	for (i = 0; i < RIC_MAX_SLVRS_PER_IO; i++) {
		slvr[i] = NULL;
		iovs[i].iov_len = 0;
		iovs[i].iov_base = 0;
	}

	if (rw == SL_WRITE) {
		if (!sli_has_enough_space(f, bmapno, mq->offset, mq->size)) {
			FCMH_ULOCK(f);
			OPSTAT_INCR("write-out-of-space");
			PFL_GOTOERR(out1, rc = mp->rc = -ENOSPC);
		}

		/*
		 * Simplistic tracking of dirty slivers, ignoring
		 * duplicates.  We rely on clients to absorb them.
		 */
		fii = fcmh_2_fii(f);
		fii->fii_nwrite += nslvrs;
		if (!(f->fcmh_flags & FCMH_IOD_DIRTYFILE) &&
		    fii->fii_nwrite >= sli_sync_max_writes) {
			OPSTAT_INCR("sync-ahead-add");
			lc_add(&sli_fcmh_dirty, fii);
			f->fcmh_flags |= FCMH_IOD_DIRTYFILE;
		}
		/*
		 * We should do this after write is done successfully.
		 * However, it is tricky to handle the AIO case. So
		 * be careful.
		 */ 
		sli_enqueue_update(f);
	}
	FCMH_ULOCK(f);

	rc = bmap_get(f, bmapno, rw, &bmap);
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f, "failed to load bmap %u; rc=%d",
		    bmapno, rc);
		PFL_GOTOERR(out1, mp->rc = -rc);
	}

	DEBUG_FCMH(PLL_DIAG, f, "bmapno=%u size=%u off=%u rw=%s "
	    "sbd_seq=%"PRId64, bmap->bcm_bmapno, mq->size, mq->offset,
	    rw == SL_WRITE ? "wr" : "rd", mq->sbd.sbd_seq);

#ifdef MYDEBUG
	psclog_max("read/write: offset = %d, bmap = %d, size = %d\n", 
	    mq->offset, bmapno, mq->size);
#endif

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
		 * The client should not send us any read request that
		 * goes beyond the EOF.  Otherwise, we are in trouble
		 * here because reading beyond EOF should return 0
		 * bytes.
		 */
		iovs[i].iov_base = slvr[i]->slvr_slab + roff;
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

	if (!sli_predio_max_slivers)
		goto out1;

	FCMH_LOCK(f);

	fii = fcmh_2_fii(f);
#if 0
	delta = SLASH_SLVR_SIZE * 4;
	off = mq->offset + bmapno * SLASH_BMAP_SIZE;
	if (off == fii->fii_predio_lastoff + fii->fii_predio_lastsize) {

		fii->fii_predio_nseq++;
		OPSTAT_INCR("readahead-increase");

	} else if (off > fii->fii_predio_lastoff + fii->fii_predio_lastsize + delta ||
	      off < fii->fii_predio_lastoff + fii->fii_predio_lastsize - delta) {

	    	fii->fii_predio_off = 0;
		fii->fii_predio_nseq = 0;
		OPSTAT_INCR("readahead-reset");
	} else {
		/* tolerate out-of-order arrivals */
	}

	fii->fii_predio_lastoff = off;
	fii->fii_predio_lastsize = mq->size;

	if (!fii->fii_predio_nseq) {
		FCMH_ULOCK(f);
		goto out1;
	}
		
	raoff = mq->offset + bmapno * SLASH_BMAP_SIZE + mq->size;
	if (raoff + sli_predio_pipe_size * SLASH_SLVR_SIZE < 
	    fii->fii_predio_off) {
		OPSTAT_INCR("readahead-pipe");
		FCMH_ULOCK(f);
		goto out1;
	}
	if (raoff >= (off_t)f->fcmh_sstb.sst_size) {
		FCMH_ULOCK(f);
		goto out1;
	}
	if (fii->fii_predio_off) {
		if (fii->fii_predio_off > raoff) {
			OPSTAT_INCR("readahead-skip");
			raoff = fii->fii_predio_off;
		}
	}

	rasize = MAX(SLASH_SLVR_SIZE * fii->fii_predio_nseq * 2, mq->size);
	rasize = MIN(rasize, sli_predio_max_slivers * SLASH_SLVR_SIZE);
	rasize = MIN(rasize, f->fcmh_sstb.sst_size - raoff);

	readahead_enqueue(f, raoff, rasize);
	fii->fii_predio_off = raoff;
#else
	delta = 0;	/* make gcc happy */
	off = mq->offset + bmapno * SLASH_BMAP_SIZE + delta;
	raoff = off + mq->size;
	if (raoff >= (off_t)f->fcmh_sstb.sst_size) {
		FCMH_ULOCK(f);
		goto out1;
	}
	rasize = sli_predio_max_slivers * SLASH_SLVR_SIZE;
	rasize = MIN(rasize, f->fcmh_sstb.sst_size - raoff);

	readahead_enqueue(f, raoff, rasize);

#endif

	FCMH_ULOCK(f);

 out1:
	for (i = 0; i < nslvrs && slvr[i]; i++) {
		s = slvr[i];

		slvr_io_done(s, rc);
		if (rw == SL_READ)
			slvr_rio_done(slvr[i]);
		else
			slvr_wio_done(slvr[i]);
	}

 out2:
	DEBUG_FCMH(rc ? PLL_NOTICE : PLL_DIAG, f,
	    "bmapno=%u size=%u off=%u rw=%s "
	    "rc=%d", bmapno, mq->size, mq->offset,
	    rw == SL_WRITE ? "wr" : "rd", rc);

	if (bmap)
		bmap_op_done(bmap);

	fcmh_op_done(f);

	if (rw == SL_READ)
		pfl_fault_here(&rc, "sliod/read_rpc");
	else
		pfl_fault_here(&rc, "sliod/write_rpc");
	return (rc);
}

__static int
sli_ric_handle_rlsbmap(struct pscrpc_request *rq)
{
	struct srt_bmapdesc *sbd;
	struct bmap_iod_rls *newbrls;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	uint32_t i;

	SL_RSX_ALLOCREP(rq, mq, mp);

	if (mq->nbmaps > MAX_BMAP_RELEASE || !mq->nbmaps)
		PFL_GOTOERR(out, mp->rc = -EINVAL);

	/*
	 * 07/01/2017: Client still experiences timeout with my
	 * big file tests. Perhaps increase the number of threads
	 * or take other measures.
	 */
	for (i = 0; i < mq->nbmaps; i++) {
		sbd = &mq->sbd[i];
		newbrls = psc_pool_get(bmap_rls_pool);
		memcpy(&newbrls->bir_sbd, sbd, sizeof(*sbd));
		lc_add(&sli_bmaplease_releaseq, newbrls);
	}
 out:
	return (0);
}

int
sli_ric_handler(struct pscrpc_request *rq)
{
	int rc = 0;
	char buf[PSCRPC_NIDSTR_SIZE];

	if (rq->rq_reqmsg->opc != SRMT_CONNECT) {
		EXPORT_LOCK(rq->rq_export);
		if (rq->rq_export->exp_private == NULL)
			rc = -PFLERR_NOTCONN;
		EXPORT_ULOCK(rq->rq_export);
		if (rc) {
			DEBUG_REQ(PLL_ERROR, rq, buf,
			  "client has not issued SRMT_CONNECT");
			goto out;
		}
	}

	pfl_fault_here(NULL, RIC_HANDLE_FAULT);

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrpc_handle_connect(rq, SRIC_MAGIC, SRIC_VERSION,
		    SLCONNT_CLI);
		if (sli_selftest_result) {
			struct slrpc_cservice *csvc;

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

/* called from sl_exp_getpri_cli() */
static struct slrpc_cservice *
iexpc_allocpri(struct pscrpc_export *exp)
{
	struct sl_exp_cli *expc;

	expc = exp->exp_private = PSCALLOC(sizeof(*expc));
	return (sli_getclcsvc(exp));
}

struct sl_expcli_ops sl_expcli_ops = {
	iexpc_allocpri
};
