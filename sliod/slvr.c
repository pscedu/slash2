/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
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
 * This file contains definitions for operations on slivers.  Slivers
 * are 1MB sections of bmaps.
 */

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/fault.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/pthrutil.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/treeutil.h"
#include "pfl/vbitmap.h"

#include "bmap_iod.h"
#include "fidc_iod.h"
#include "rpc_iod.h"
#include "slab.h"
#include "slerr.h"
#include "sltypes.h"
#include "slvr.h"

struct psc_poolmaster	 slvr_poolmaster;
struct psc_poolmaster	 sli_aiocbr_poolmaster;
struct psc_poolmaster	 sli_iocb_poolmaster;
struct psc_poolmaster	 sli_readaheadrq_poolmaster;

struct psc_poolmgr	*slvr_pool;
struct psc_poolmgr	*sli_aiocbr_pool;
struct psc_poolmgr	*sli_iocb_pool;
struct psc_poolmgr	*sli_readaheadrq_pool;

struct psc_listcache	 sli_readaheadq;
struct psc_listcache	 sli_iocb_pndg;

psc_atomic64_t		 sli_aio_id = PSC_ATOMIC64_INIT(0);

struct psc_listcache	 sli_lruslvrs;		/* LRU list of clean slivers which may be reaped */

struct psc_listcache	 sli_fcmh_dirty;
struct psc_listcache	 sli_fcmh_update;

extern struct psc_waitq	 sli_slvr_waitq;

SPLAY_GENERATE(biod_slvrtree, slvr, slvr_tentry, slvr_cmp)

#define                  MIN_FREE_SLABS		 16
#define			 SLAB_RECLAIM_BATCH      1

struct psc_listcache     slab_buffers;
int                      slab_buffers_count;    /* total, including free */

struct slab_buffer_entry {
	union {
		struct psc_listentry     slab_lentry;
		char                     slab_buf[SLASH_SLVR_SIZE];
	};
};

int			 use_slab_buffers = 1;

void *
sli_slab_alloc(void)
{
	void *p;

	if (use_slab_buffers) {
		p = lc_getnb(&slab_buffers);
		psc_assert(p);
	} else
		p = PSCALLOC(SLASH_SLVR_SIZE);

	return (p);
}

void
sli_slab_free(void *p)
{
	if (use_slab_buffers) {
		INIT_PSC_LISTENTRY((struct psc_listentry *)p);
		lc_add(&slab_buffers, p);
	} else
		PSCFREE(p);
}


void
sli_aio_aiocbr_release(struct sli_aiocb_reply *a)
{
	psc_assert(psclist_disjoint(&a->aiocbr_lentry));

	if (a->aiocbr_csvc)
		sl_csvc_decref(a->aiocbr_csvc);
	psc_pool_return(sli_aiocbr_pool, a);
}


__static int
slvr_aio_chkslvrs(const struct sli_aiocb_reply *a)
{
	struct slvr *s;
	int i, rc = 0;

	for (i = 0; i < a->aiocbr_nslvrs; i++) {
		s = a->aiocbr_slvrs[i];
		SLVR_LOCK(s);
		psc_assert(s->slvr_flags &
		    (SLVRF_DATARDY | SLVRF_DATAERR));
		if (s->slvr_flags & SLVRF_DATAERR)
			rc = s->slvr_err;
		SLVR_ULOCK(s);
		if (rc)
			return (rc);
	}
	return (rc);
}

__static void
slvr_aio_replreply(struct sli_aiocb_reply *a)
{
	struct pscrpc_request *rq = NULL;
	struct srm_repl_read_req *mq;
	struct srm_repl_read_rep *mp;
	struct slvr *s = NULL;
	int rc;

	psc_assert(a->aiocbr_nslvrs == 1);

	if (!a->aiocbr_csvc)
		goto out;

	if (SL_RSX_NEWREQ(a->aiocbr_csvc, SRMT_REPL_READAIO, rq, mq,
	    mp))
		goto out;

	OPSTAT_INCR("repl-readaio");

	s = a->aiocbr_slvrs[0];

	mq->rc = slvr_aio_chkslvrs(a);
	mq->fg = slvr_2_fcmh(s)->fcmh_fg;
	mq->len = a->aiocbr_len;
	mq->bmapno = slvr_2_bmap(s)->bcm_bmapno;
	mq->slvrno = s->slvr_num;
	if (mq->rc)
		pscrpc_msg_add_flags(rq->rq_reqmsg, MSG_ABORT_BULK);
	else
		mq->rc = slrpc_bulkclient(rq, BULK_GET_SOURCE,
		    SRII_BULK_PORTAL, a->aiocbr_iovs, a->aiocbr_niov);

	rc = SL_RSX_WAITREP(a->aiocbr_csvc, rq, mp);
	pscrpc_req_finished(rq);

	if (rc)
		DEBUG_SLVR(PLL_ERROR, s, "rc=%d", rc);

	slvr_rio_done(s);

 out:
	sli_aio_aiocbr_release(a);
}

__static void
slvr_aio_reply(struct sli_aiocb_reply *a)
{
	struct pscrpc_request *rq = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int rc, i;

	if (!a->aiocbr_csvc)
		goto out;

	if (a->aiocbr_rw == SL_WRITE)
		rc = SL_RSX_NEWREQ(a->aiocbr_csvc, SRMT_WRITE, rq, mq,
		    mp);
	else
		rc = SL_RSX_NEWREQ(a->aiocbr_csvc, SRMT_READ, rq, mq,
		    mp);
	if (rc)
		goto out;

	OPSTAT_INCR("slvr-aio-reply");

	mq->rc = slvr_aio_chkslvrs(a);

	memcpy(&mq->sbd, &a->aiocbr_sbd, sizeof(mq->sbd));
	mq->id = a->aiocbr_id;
	mq->size = a->aiocbr_len;
	mq->offset = a->aiocbr_off;
	if (a->aiocbr_rw == SL_WRITE)
		/*
		 * Notify the client that he may resubmit the write.
		 */
		mq->op = SRMIOP_WR;
	else {
		mq->op = SRMIOP_RD;
		if (mq->rc)
			pscrpc_msg_add_flags(rq->rq_reqmsg,
			    MSG_ABORT_BULK);
		else if (!(a->aiocbr_flags & SLI_AIOCBSF_DIO))
			mq->rc = slrpc_bulkclient(rq, BULK_GET_SOURCE,
			    SRCI_BULK_PORTAL, a->aiocbr_iovs,
			    a->aiocbr_niov);
	}

	rc = SL_RSX_WAITREP(a->aiocbr_csvc, rq, mp);
	pscrpc_req_finished(rq);

 out:
	for (i = 0; i < a->aiocbr_nslvrs; i++) {
		if (a->aiocbr_rw == SL_READ)
			slvr_rio_done(a->aiocbr_slvrs[i]);
		else
			slvr_wio_done(a->aiocbr_slvrs[i]);
	}

	sli_aio_aiocbr_release(a);
}

__static void
slvr_aio_tryreply(struct sli_aiocb_reply *a)
{
	int i;
	struct slvr *s;

	for (i = 0; i < a->aiocbr_nslvrs; i++) {
		s = a->aiocbr_slvrs[i];
		SLVR_LOCK(s);
		if (s->slvr_flags & SLVRF_FAULTING) {
			SLVR_ULOCK(s);
			return;
		}
		SLVR_ULOCK(s);
	}

	if (a->aiocbr_flags & SLI_AIOCBSF_REPL)
		slvr_aio_replreply(a);
	else
		slvr_aio_reply(a);
}

__static void
slvr_iocb_release(struct sli_iocb *iocb)
{
	psc_pool_return(sli_iocb_pool, iocb);
}

__static void
slvr_fsaio_done(struct sli_iocb *iocb)
{
	struct sli_aiocb_reply *a;
	struct slvr *s;
	int rc;

	s = iocb->iocb_slvr;
	rc = iocb->iocb_rc;

	SLVR_LOCK(s);
	psc_assert(iocb == s->slvr_iocb);
	psc_assert(s->slvr_flags & SLVRF_FAULTING);
	psc_assert(!(s->slvr_flags & (SLVRF_DATARDY | SLVRF_DATAERR)));

	/* Prevent additions from new requests. */
	s->slvr_flags &= ~SLVRF_FAULTING;

	slvr_iocb_release(s->slvr_iocb);
	s->slvr_iocb = NULL;

	if (rc) {
		/*
		 * There was a problem; unblock any waiters and
		 * tell them the bad news.
		 */
		s->slvr_flags |= SLVRF_DATAERR;
		DEBUG_SLVR(PLL_ERROR, s, "error, rc=%d", rc);
		s->slvr_err = rc;
	} else {
		s->slvr_flags |= SLVRF_DATARDY;
		DEBUG_SLVR(PLL_DIAG, s, "FAULTING -> DATARDY");
	}

	a = s->slvr_aioreply;
	s->slvr_aioreply = NULL;

	SLVR_WAKEUP(s);
	SLVR_ULOCK(s);

	if (a)
		slvr_aio_tryreply(a);
}

__static struct sli_iocb *
sli_aio_iocb_new(struct slvr *s)
{
	struct sli_iocb *iocb;

	OPSTAT_INCR("iocb-get");
	iocb = psc_pool_get(sli_iocb_pool);
	memset(iocb, 0, sizeof(*iocb));
	INIT_LISTENTRY(&iocb->iocb_lentry);
	/*
	 * XXX Take a reference count here to avoid complicated
	 * logic used to decide who owns the last reference count.
	 * That way, the I/O path can be unified for both AIO
	 * and non-AIO cases.
	 */
	iocb->iocb_slvr = s;
	iocb->iocb_cbf = slvr_fsaio_done;

	return (iocb);
}

struct sli_aiocb_reply *
sli_aio_replreply_setup(struct pscrpc_request *rq, struct slvr *s,
    struct iovec *iov)
{
	struct srm_repl_read_rep *mp;
	struct sli_aiocb_reply *a;

	a = psc_pool_get(sli_aiocbr_pool);
	memset(a, 0, sizeof(*a));

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	mp->id = a->aiocbr_id = psc_atomic64_inc_getnew(&sli_aio_id);

	memcpy(a->aiocbr_iovs, iov, sizeof(*iov));
	a->aiocbr_slvrs[0] = s;
	a->aiocbr_niov = 1;
	a->aiocbr_len = iov->iov_len;
	a->aiocbr_off = 0;
	a->aiocbr_nslvrs = 1;

	/* Ref taken here must persist until reply is attempted. */
	a->aiocbr_csvc = sli_geticsvcx(libsl_try_nid2resm(
	    rq->rq_peer.nid), rq->rq_export, 0);

	a->aiocbr_flags = SLI_AIOCBSF_REPL;
	INIT_PSC_LISTENTRY(&a->aiocbr_lentry);

	return (a);
}

struct sli_aiocb_reply *
sli_aio_reply_setup(struct pscrpc_request *rq, uint32_t len,
    uint32_t off, struct slvr **slvrs, int nslvrs, struct iovec *iovs,
    int niovs, enum rw rw)
{
	struct slrpc_cservice *csvc;
	struct sli_aiocb_reply *a;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int i;

	csvc = sli_getclcsvc(rq->rq_export, 0);
	if (csvc == NULL)
		return (NULL);

	a = psc_pool_get(sli_aiocbr_pool);
	memset(a, 0, sizeof(*a));

	/*
	 * XXX take reference count here, so we can unify/simplify
	 * code path later.
	 */
	for (i = 0; i < nslvrs; i++)
		a->aiocbr_slvrs[i] = slvrs[i];

	a->aiocbr_nslvrs = nslvrs;
	psc_assert(niovs == a->aiocbr_nslvrs);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	memcpy(&a->aiocbr_sbd, &mq->sbd, sizeof(mq->sbd));

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	mp->id = a->aiocbr_id = psc_atomic64_inc_getnew(&sli_aio_id);

	memcpy(a->aiocbr_iovs, iovs, niovs * sizeof(*iovs));

	a->aiocbr_niov = niovs;
	a->aiocbr_len = len;
	a->aiocbr_off = off;
	a->aiocbr_rw = rw;
	a->aiocbr_csvc = csvc;

	a->aiocbr_flags = SLI_AIOCBSF_NONE;
	INIT_PSC_LISTENTRY(&a->aiocbr_lentry);

	return (a);
}

int
sli_aio_register(struct slvr *s)
{
	struct sli_iocb *iocb;
	struct aiocb *aio;
	int error;

	iocb = sli_aio_iocb_new(s);

	SLVR_LOCK(s);
	s->slvr_flags |= SLVRF_FAULTING;
	s->slvr_iocb = iocb;
	SLVR_WAKEUP(s);
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
		psclog_diag("aio_read: fd=%d iocb=%p sliver=%p",
		    aio->aio_fildes, iocb, s);
	} else {
		psclog_warnx("aio_read: fd=%d iocb=%p sliver=%p error=%d",
		    aio->aio_fildes, iocb, s, error);
		lc_remove(&sli_iocb_pndg, iocb);
		slvr_iocb_release(iocb);
	}

	return (-error);
}

__static ssize_t
slvr_fsio(struct slvr *s, uint32_t off, uint32_t size, enum rw rw)
{
	int sblk, nblks, save_errno = 0;
	struct timespec ts0, ts1, tsd;
	struct fidc_membh *f;
	uint64_t *v8;
	ssize_t	rc;
	size_t foff;

	f = slvr_2_fcmh(s);
	if (!(f->fcmh_flags & FCMH_IOD_BACKFILE)) {
		psclog_warnx("no backing file: "SLPRI_FG" fd=%d",
		    SLPRI_FG_ARGS(&f->fcmh_fg), slvr_2_fd(s));
		OPSTAT_INCR("no-backfile");
		return (-EBADF);
	}

	errno = 0;
	if (rw == SL_READ) {
		OPSTAT_INCR("fsio-read");

		if (slcfg_local->cfg_async_io)
			return (sli_aio_register(s));

		/*
		 * Do full sliver read, ignoring specific off and len.
		 */
		sblk = 0;
		size = SLASH_SLVR_SIZE;
		foff = slvr_2_fileoff(s, sblk);
		nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;

		PFL_GETTIMESPEC(&ts0);

		save_errno = 0;
		pfl_fault_here_rc(&save_errno, EBADF,
		    "sliod/fsio_read_fail");
		if (save_errno) {
			errno = save_errno;
			rc = -1;
		} else
			rc = pread(slvr_2_fd(s), slvr_2_buf(s, sblk),
			    size, foff);

		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR("fsio-read-fail");
		} else
			pfl_opstat_add(sli_backingstore_iostats.rd, rc);

		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("read-wait-usecs",
		    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);
	} else {
		OPSTAT_INCR("fsio-write");

		sblk = off / SLASH_SLVR_BLKSZ;
		psc_assert((off % SLASH_SLVR_BLKSZ) == 0);
		foff = slvr_2_fileoff(s, sblk);
		nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;

		/*
		 * We incremented pndgwrts so any blocking reads should
		 * wait for this counter to reach zero.
		 */

		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
		    foff);
		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR("fsio-write-fail");
		} else
			pfl_opstat_add(sli_backingstore_iostats.wr, rc);
	}

	if (rc < 0) {
		DEBUG_SLVR(PLL_ERROR, s, "failed (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, foff, save_errno);
		if (rw == SL_WRITE)
			OPSTAT_INCR("write-error");
		else
			OPSTAT_INCR("read-error");

	} else if ((uint32_t)rc != size) {
		DEBUG_SLVR(foff + size > slvr_2_fcmh(s)->
		    fcmh_sstb.sst_size ? PLL_DIAG : PLL_NOTICE, s,
		    "short I/O (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, foff, save_errno);
		if (rw == SL_WRITE)
			OPSTAT_INCR("write-short");
		else
			OPSTAT_INCR("read-short");
	} else {
		v8 = slvr_2_buf(s, sblk);
		DEBUG_SLVR(PLL_DIAG, s, "ok %s size=%u off=%zu"
		    " rc=%zd nblks=%d v8(%"PRIx64")",
		    (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    size, foff, rc, nblks, *v8);
		if (rw == SL_WRITE)
			OPSTAT_INCR("write-perfect");
		else
			OPSTAT_INCR("read-perfect");
	}

	return (rc < 0 ? -save_errno : 0);
}

/*
 * Read in a sliver or a portion of it.
 * @s: the sliver.
 */
ssize_t
slvr_fsbytes_rio(struct slvr *s, uint32_t off, uint32_t size)
{
	ssize_t rc;

	/*
	 * If we hit -SLERR_AIOWAIT, the AIO callback handler
	 * slvr_fsaio_done() will set the state of the sliver (DATARDY
	 * or DATAERR). Otherwise, our caller should set them.
	 */
	rc = slvr_fsio(s, off, size, SL_READ);
	return (rc);
}

ssize_t
slvr_fsbytes_wio(struct slvr *s, uint32_t sblk, uint32_t size)
{
	DEBUG_SLVR(PLL_DIAG, s, "sblk=%u size=%u", sblk, size);

	return (slvr_fsio(s, sblk * SLASH_SLVR_BLKSZ, size, SL_WRITE));
}

/*
 * Prepare a sliver for an incoming I/O.  This may entail faulting 32k
 * aligned regions in from the underlying fs.
 *
 * @s: the sliver
 * @off: offset into the slvr (not bmap or file object)
 * @len: len relative to the slvr
 * @rw: read or write op
 * @flags: operational flags.
 */
ssize_t
slvr_io_prep(struct slvr *s, uint32_t off, uint32_t len, enum rw rw, 
    int readahead)
{
	struct bmap *b = slvr_2_bmap(s);
	ssize_t rc = 0;

	BMAP_ULOCK(b);

	SLVR_LOCK(s);

	/*
	 * Note we have taken our read or write references, so the
	 * sliver won't be freed from under us.
	 */
	SLVR_WAIT(s, s->slvr_flags & SLVRF_FAULTING);

	DEBUG_SLVR(PLL_DIAG, s,
	    "slvrno=%hu off=%u len=%u rw=%d",
	    s->slvr_num, off, len, rw);

	if (s->slvr_flags & SLVRF_DATAERR) {
		rc = s->slvr_err;
		goto out1;
	}

	/*
	 * Mark the sliver until we are done read and write with it.
	 */
	s->slvr_flags |= SLVRF_FAULTING;

	if (s->slvr_flags & SLVRF_DATARDY) {
		if (!readahead && (s->slvr_flags & SLVRF_READAHEAD)) {
			s->slvr_flags &= ~SLVRF_READAHEAD;
			OPSTAT_INCR("readahead-hit");
		}
		goto out1;
	}
	if (!readahead && rw == SL_READ)
		OPSTAT_INCR("readahead-miss");

	if (rw == SL_WRITE && !off && len == SLASH_SLVR_SIZE) {
		/*
		 * Full sliver write, no need to read blocks from disk.
		 * All blocks will be dirtied by the incoming network
		 * IO.
		 */
		goto out1;
	}
	SLVR_ULOCK(s);

	/*
	 * Execute read to fault in needed blocks after dropping the
	 * lock.  All should be protected by the FAULTING bit.
	 */
	rc = slvr_fsbytes_rio(s, off, len);
	if (!rc && readahead)
		s->slvr_flags |= SLVRF_READAHEAD;
	goto out2;

 out1:
	SLVR_ULOCK(s);
 out2:
	BMAP_LOCK(b);
	return (rc);
}


void
slvr_remove(struct slvr *s)
{
	struct bmap_iod_info *bii;

	DEBUG_SLVR(PLL_DEBUG, s, "freeing slvr");

	psc_assert(s->slvr_refcnt == 0);
	psc_assert(!(s->slvr_flags & SLVRF_LRU));

	bii = slvr_2_bii(s);

	/* (gdb) p *((struct bmap *)bii - 1) */
	BII_LOCK(bii);
	PSC_SPLAY_XREMOVE(biod_slvrtree, &bii->bii_slvrs, s);
	bmap_op_done_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

	if (s->slvr_flags & SLVRF_READAHEAD)
		OPSTAT_INCR("readahead-waste");
	sli_slab_free(s->slvr_slab);
	psc_pool_return(slvr_pool, s);
}

/*
 * Remove vestige of old contents when the generation of a file changes
 * or when the file is unlinked.
 */
void
slvr_remove_all(struct fidc_membh *f)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct timespec ts0, ts1, delta;
	struct bmap_iod_info *bii;
	struct bmap *b;
	struct slvr *s;
	int i;

	PFL_GETTIMESPEC(&ts0);

	/*
 	 * If we are called by sli_rim_handle_reclaim(), we already 
 	 * to the FCMH lock, so take the read lock below is redundant
 	 * and does not get us anything.
 	 *
	 * Use two loops to avoid entanglement with some background
	 * operations.
	 */

 restart:

	pfl_rwlock_rdlock(&f->fcmh_rwlock);
	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
		BMAP_LOCK(b);
		if (b->bcm_flags & BMAPF_TOFREE) {
			BMAP_ULOCK(b);
			continue;
		}
		/*
		 * If slab_cache_reap() tries to free a sliver, this
		 * reference count should hold the bmap. Otherwise,
		 * slab_cache_reap() will try to grab the fcmh_rwlock
		 * to remove the bmap from the fcmh tree and we have
		 * a deadlock.
		 *
		 * Unfortunately, we still hit a deadlock due to this
		 * race. More invetigation is needed.
		 */
		bmap_op_start_type(b, BMAP_OPCNT_SLVR);

		bii = bmap_2_bii(b);
		while ((s = SPLAY_ROOT(&bii->bii_slvrs))) {
			if (!SLVR_TRYLOCK(s)) {
				BII_ULOCK(bii);
				pscthr_yield();
				BII_LOCK(bii);
				continue;
			}
			/*
 			 * Our reaper could drop a sliver and then its
 			 * associated bmap.  And bmap_remove() needs to
 			 * take fcmh_rwlock. It is not pretty. Let us
 			 * restart if so.
 			 */
			if (s->slvr_refcnt || (s->slvr_flags &
			    (SLVRF_FREEING | SLVRF_FAULTING))) {
				SLVR_ULOCK(s);
				BII_ULOCK(bii);
				pfl_rwlock_unlock(&f->fcmh_rwlock);
				bmap_op_done_type(b, BMAP_OPCNT_SLVR);
				OPSTAT_INCR("slvr-remove-restart");
				pscthr_yield();
				goto restart;
			}
			s->slvr_flags |= SLVRF_FREEING;
			if (s->slvr_flags & SLVRF_LRU) {
				s->slvr_flags &= ~SLVRF_LRU;
				lc_remove(&sli_lruslvrs, s);
			}

			SLVR_ULOCK(s);
			BII_ULOCK(bii);

			slvr_remove(s);

			BII_LOCK(bii);
		}
		psc_dynarray_add(&a, b);
		BMAP_ULOCK(b);
	}
	pfl_rwlock_unlock(&f->fcmh_rwlock);

	DYNARRAY_FOREACH(b, i, &a)
		bmap_op_done_type(b, BMAP_OPCNT_SLVR);
	psc_dynarray_free(&a);

	PFL_GETTIMESPEC(&ts1);
	timespecsub(&ts1, &ts0, &delta);
	OPSTAT_ADD("slvr-remove-all-wait-usecs",
	    delta.tv_sec * 1000000 + delta.tv_nsec / 1000);
}

/*
 * Note that the sliver may be freed by this function.
 */
__static void
slvr_lru_tryunpin_locked(struct slvr *s)
{
	int wakeup = 0;

	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab);
	if (s->slvr_refcnt) {
		SLVR_ULOCK(s);
		return;
	}
	if (s->slvr_flags & SLVRF_LRU) {
		s->slvr_flags &= ~SLVRF_LRU;
		lc_remove(&sli_lruslvrs, s);
	}

#if 0
	if (s->slvr_flags & SLVRF_DATAERR) {
		/*
		 * This is safe because we hold the sliver lock
		 * even if our reference count is now zero.
		 */
		s->slvr_flags |= SLVRF_FREEING;
		SLVR_ULOCK(s);
		OPSTAT_INCR("slvr-err-remove");
		slvr_remove(s);
		return;
	}
	psc_assert(s->slvr_flags & SLVRF_DATARDY);

#endif


	/*
	 * Locking convention: it is legal to request for a list lock
	 * while holding the sliver lock.  On the other hand, when you
	 * already hold the list lock, you should drop the list lock
	 * first before asking for the sliver lock or you should use
	 * trylock().
	 */
	s->slvr_flags |= SLVRF_LRU;
	if (s->slvr_flags & SLVRF_DATAERR) {
		wakeup = 1;
		lc_addhead(&sli_lruslvrs, s);
	} else
		lc_addtail(&sli_lruslvrs, s);
	SLVR_ULOCK(s);

	/*
 	 * I am holding the bmap lock here, do don't call reaper 
 	 * directly to avoid a potential deadlock.
 	 *
 	 * We reap proactively instead of on demand to avoid ENOMEM
 	 * situation, which we don't handle gracefully right now.
 	 */
	if (wakeup || slvr_pool->ppm_nfree < MIN_FREE_SLABS) {
		OPSTAT_INCR("slvr-wakeone");
		psc_waitq_wakeone(&sli_slvr_waitq);
	}
}

void
slvr_io_done(struct slvr *s, int rc)
{
	SLVR_LOCK(s);
	s->slvr_flags &= ~(SLVRF_FAULTING | SLVRF_DATARDY);
	if (rc) {
		s->slvr_err = rc;
		s->slvr_flags |= SLVRF_DATAERR;
		DEBUG_SLVR(PLL_DIAG, s, "FAULTING --> DATAERR");
	} else {
		s->slvr_flags |= SLVRF_DATARDY;
		DEBUG_SLVR(PLL_DIAG, s, "FAULTING --> DATARDY");

	}
	SLVR_WAKEUP(s);
	SLVR_ULOCK(s);
}

/*
 * Called after a read on the given sliver has completed.
 */
void
slvr_rio_done(struct slvr *s)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_refcnt > 0);

	s->slvr_refcnt--;
	DEBUG_SLVR(PLL_DIAG, s, "decref");
	slvr_lru_tryunpin_locked(s);
}

/*
 * Called after a write on the given sliver has completed.
 */
void
slvr_wio_done(struct slvr *s)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_refcnt > 0);

	s->slvr_refcnt--;
	DEBUG_SLVR(PLL_DIAG, s, "decref");

	slvr_lru_tryunpin_locked(s);
}

/*
 * Lookup or create a sliver reference, ignoring one that is being
 * freed.
 */
struct slvr *
slvr_lookup(uint32_t num, struct bmap_iod_info *bii)
{
	struct slvr *s, *tmp1 = NULL, ts;
	void *tmp2 = NULL;
	int alloc = 0;

	ts.slvr_num = num;

	BII_LOCK_ENSURE(bii);
 retry:
	s = SPLAY_FIND(biod_slvrtree, &bii->bii_slvrs, &ts);
	if (s) {
		SLVR_LOCK(s);
		/*
		 * Reuse SLVRF_DATAERR slivers is tricky, we might as
		 * well start from fresh.
		 */
		if (s->slvr_flags & (SLVRF_FREEING | SLVRF_DATAERR)) {
			SLVR_ULOCK(s);
			/*
			 * Lock is required to free the slvr.
			 * It must be held here to prevent the slvr
			 * from being freed before we release the lock.
			 */
			BII_ULOCK(bii);
			usleep(1);
			BII_LOCK(bii);
			goto retry;
		}

		OPSTAT_INCR("slvr-cache-hit");

		s->slvr_refcnt++;

		SLVR_ULOCK(s);
	} else {
		if (!alloc) {
			alloc = 1;
			BII_ULOCK(bii);
			tmp1 = psc_pool_get(slvr_pool);
			tmp2 = sli_slab_alloc();
			BII_LOCK(bii);
			goto retry;
		}

		alloc = 0;

		OPSTAT_INCR("slvr-cache-miss");

		s = tmp1;
		memset(s, 0, sizeof(*s));
		s->slvr_num = num;
		s->slvr_bii = bii;
		INIT_PSC_LISTENTRY(&s->slvr_lentry);
		INIT_SPINLOCK(&s->slvr_lock);

		memset(tmp2, 0, SLASH_SLVR_SIZE);
		s->slvr_slab = tmp2;
		s->slvr_refcnt = 1;

		PSC_SPLAY_XINSERT(biod_slvrtree, &bii->bii_slvrs, s);
		bmap_op_start_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

	}
	if (alloc) {
		sli_slab_free(tmp2);
		psc_pool_return(slvr_pool, tmp1);
	}
	return (s);
}

/*
 * The reclaim function for slvr_pool.  Note that our caller
 * psc_pool_get() ensures that we are called exclusively.
 */

int
slab_cache_reap(struct psc_poolmgr *m)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct slvr *s;
	struct psc_thread *thr;
	int i, haswork, nitems = 0;

	thr = pscthr_get();
	psc_assert(m == slvr_pool);

	psc_dynarray_ensurelen(&a, SLAB_RECLAIM_BATCH);

again:
	haswork = 0;
	LIST_CACHE_LOCK(&sli_lruslvrs);
	LIST_CACHE_FOREACH(s, &sli_lruslvrs) {

		haswork = 1;
		DEBUG_SLVR(PLL_DIAG, s, "considering for reap");

		/*
		 * We are reaping so it is fine to back off on some
		 * slivers.  We have to use a reqlock here because
		 * slivers do not have private spinlocks; instead they
		 * use the lock of the bii.  So if this thread tries to
		 * free a slvr from the same bii trylock will abort.
		 */
		if (!SLVR_TRYLOCK(s))
			continue;

		if (s->slvr_refcnt || (s->slvr_flags & SLVRF_FREEING)) {
			SLVR_ULOCK(s);
			continue;
		}
		psc_assert(s->slvr_flags & SLVRF_LRU);
		s->slvr_flags |= SLVRF_FREEING;
		s->slvr_flags &= ~SLVRF_LRU;
		lc_remove(&sli_lruslvrs, s);
		SLVR_ULOCK(s);

		psc_dynarray_add(&a, s);
		if (psc_dynarray_len(&a) >= SLAB_RECLAIM_BATCH &&
		    psc_dynarray_len(&a) >= 
		    psc_atomic32_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&sli_lruslvrs);
	DYNARRAY_FOREACH(s, i, &a)
		slvr_remove(s);

	nitems += psc_dynarray_len(&a);
	psc_dynarray_reset(&a);

	if (thr->pscthr_type == SLITHRT_BREAP &&
	     m->ppm_nfree < MIN_FREE_SLABS && haswork) {
		pscthr_yield();
		OPSTAT_INCR("sli.reap-loop");
		goto again;
	}

	psc_dynarray_free(&a);

	OPSTAT_INCR("slab-lru-reap");
	return (nitems);
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

			pfl_fault_here_rc(&iocb->iocb_rc, EIO,
			    "sliod/aio_fail");

			psclog_diag("got signal: iocb=%p", iocb);
			lc_remove(&sli_iocb_pndg, iocb);
			LIST_CACHE_ULOCK(&sli_iocb_pndg);
			iocb->iocb_cbf(iocb);	/* slvr_fsaio_done() */
			LIST_CACHE_LOCK(&sli_iocb_pndg);
		}
		LIST_CACHE_ULOCK(&sli_iocb_pndg);
	}
}

void
slirathr_main(struct psc_thread *thr)
{
	struct sli_readaheadrq *rarq;
	struct bmapc_memb *b;
	struct fidc_membh *f;
	struct slvr *s;
	int i, rc, slvrno, nslvr;
	sl_bmapno_t bno;

	while (pscthr_run(thr)) {
		f = NULL;
		b = NULL;

		if (slcfg_local->cfg_async_io)
			break;

		rarq = lc_getwait(&sli_readaheadq);
		if (sli_fcmh_peek(&rarq->rarq_fg, &f))
			goto next;

		bno = rarq->rarq_off / SLASH_BMAP_SIZE;
		slvrno = (rarq->rarq_off % SLASH_BMAP_SIZE) / SLASH_SLVR_SIZE;
		nslvr = rarq->rarq_size / SLASH_SLVR_SIZE;
		if (rarq->rarq_size % SLASH_SLVR_SIZE)
			nslvr++;

		for (i = 0; i < nslvr; i++) {
			if (i + slvrno >= SLASH_SLVRS_PER_BMAP) {
				bno++;
				slvrno = 0;
				if (b) {
					bmap_op_done(b);
					b = NULL;
				}
			}
			if (!b) {
				if (bmap_get(f, bno, SL_READ, &b))
					goto next;
			}
			s = slvr_lookup(slvrno + i, bmap_2_bii(b));
			rc = slvr_io_prep(s, 0, SLASH_SLVR_SIZE, SL_READ, 1);
			/*
			 * FixMe: This cause asserts on flags when we
			 * encounter AIOWAIT. We need a unified way to
			 * perform I/O done on each sliver instead of
			 * sprinkling them all over the place.
			 *
			 * 09/24/2017, rc = -511, we have't release bmap
			 * lock, so we deadlock.
			 */
			slvr_io_done(s, rc);
			slvr_rio_done(s);
		}

 next:
		if (b)
			bmap_op_done(b);
		if (f)
			fcmh_op_done(f);
		psc_pool_return(sli_readaheadrq_pool, rarq);
	}
}

void
slvr_cache_init(void)
{
	void *p;
	int i, nbuf;

	psc_assert(SLASH_SLVR_SIZE <= LNET_MTU);

	if (slcfg_local->cfg_slab_cache_size < SLAB_MIN_CACHE)
		psc_fatalx("invalid slab_cache_size setting; "
		    "minimum allowed is %zu", SLAB_MIN_CACHE);

	nbuf = slcfg_local->cfg_slab_cache_size / SLASH_SLVR_SIZE;

	psc_poolmaster_init(&slvr_poolmaster,
	    struct slvr, slvr_lentry, PPMF_AUTO, nbuf,
	    nbuf, nbuf, slab_cache_reap, "slvr");
	slvr_pool = psc_poolmaster_getmgr(&slvr_poolmaster);

	if (!use_slab_buffers)
		goto next;

	lc_reginit(&slab_buffers, struct slab_buffer_entry,
		slab_lentry, "slabbuffers"); 

	for (i = 0; i < nbuf; i++) {
		p = mmap(NULL, SLASH_SLVR_SIZE, PROT_READ|PROT_WRITE,
			MAP_ANONYMOUS|MAP_SHARED, -1, 0); 

		if (p == MAP_FAILED)
			psc_fatalx("Please raise vm.max_map_count limit");

		OPSTAT_INCR("mmap-success");
		slab_buffers_count++;
		INIT_PSC_LISTENTRY((struct psc_listentry *)p);
		lc_add(&slab_buffers, p);
	}   

 next:
	psc_poolmaster_init(&sli_readaheadrq_poolmaster,
	    struct sli_readaheadrq, rarq_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, "readaheadrq");
	sli_readaheadrq_pool = psc_poolmaster_getmgr(
	    &sli_readaheadrq_poolmaster);

	lc_reginit(&sli_readaheadq, struct sli_readaheadrq, rarq_lentry,
	    "readaheadq");

	lc_reginit(&sli_lruslvrs, struct slvr, slvr_lentry, "lruslvrs");

	lc_reginit(&sli_fcmh_dirty, struct fcmh_iod_info, fii_lentry,
	    "fcmhdirty");

	lc_reginit(&sli_fcmh_update, struct fcmh_iod_info, fii_lentry2,
	    "fcmhupdate");

	if (slcfg_local->cfg_async_io) {
		psc_poolmaster_init(&sli_iocb_poolmaster,
		    struct sli_iocb, iocb_lentry, PPMF_AUTO, 64, 64,
		    1024, NULL, "iocb");
		sli_iocb_pool = psc_poolmaster_getmgr(&sli_iocb_poolmaster);

		psc_poolmaster_init(&sli_aiocbr_poolmaster,
		    struct sli_aiocb_reply, aiocbr_lentry, PPMF_AUTO, 64,
		    64, 1024, NULL, "aiocbr");
		sli_aiocbr_pool = psc_poolmaster_getmgr(&sli_aiocbr_poolmaster);

		lc_reginit(&sli_iocb_pndg, struct sli_iocb, iocb_lentry,
		    "iocbpndg");

		pscthr_init(SLITHRT_AIO, sliaiothr_main, 0, "sliaiothr");
	}

	for (i = 0; i < NSLVR_READAHEAD_THRS; i++)
		pscthr_init(SLITHRT_READAHEAD, slirathr_main, 0,
		    "slirathr%d", i);

	for (i = 0; i < NSLVRSYNC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_SYNC, slisyncthr_main, 0,
		    "slisyncthr%d", i);

	slab_cache_init(nbuf);

	_psc_poolmaster_init(&sli_upd_poolmaster,
	    sizeof(struct sli_update),
	    offsetof(struct sli_update, sli_lentry), PPMF_AUTO, 64,
	    64, 0, NULL, NULL, "sliupd");
	sli_upd_pool = psc_poolmaster_getmgr(&sli_upd_poolmaster);
}

#if PFL_DEBUG > 0
void
dump_sliver(struct slvr *s)
{
	DEBUG_SLVR(PLL_MAX, s, "");
}

void
dump_sliver_flags(int fl)
{
	int seq = 0;

	PFL_PRFLAG(SLVRF_FAULTING, &fl, &seq);
	PFL_PRFLAG(SLVRF_DATARDY, &fl, &seq);
	PFL_PRFLAG(SLVRF_DATAERR, &fl, &seq);
	PFL_PRFLAG(SLVRF_LRU, &fl, &seq);
	PFL_PRFLAG(SLVRF_FREEING, &fl, &seq);
	PFL_PRFLAG(SLVRF_ACCESSED, &fl, &seq);
	if (fl)
		printf(" unknown: %x", fl);
	printf("\n");
}
#endif
