/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
struct psc_listcache	 sli_crcqslvrs;		/* Slivers ready to be CRC'd and have their
						 * CRCs shipped to the MDS. */

SPLAY_GENERATE(biod_slvrtree, slvr, slvr_tentry, slvr_cmp)

/*
 * Take the CRC of the data contained within a sliver and add the update
 * to a bcr.
 * @s: the sliver reference.
 * Notes:  Don't hold the lock while taking the CRC.
 * Returns: errno on failure, 0 on success, -1 on not applicable.
 */
int
slvr_do_crc(struct slvr *s, uint64_t *crcp)
{
	uint64_t crc;

	SLVR_LOCK_ENSURE(s);
	psc_assert((s->slvr_flags & SLVRF_FAULTING ||
		    s->slvr_flags & SLVRF_CRCDIRTY));

	if (s->slvr_flags & SLVRF_CRCDIRTY) {
		/*
		 * SLVRF_CRCDIRTY means that DATARDY has been set and
		 * that a write dirtied the buffer and invalidated the
		 * CRC.
		 */
		DEBUG_SLVR(PLL_DIAG, s, "crc");

#ifdef ADLERCRC32
		// XXX not a running CRC?  double check for correctness
		crc = adler32(crc, slvr_2_buf(s, 0) + soff,
		    (int)(eoff - soff));
#else
		psc_crc64_calc(&crc, slvr_2_buf(s, 0), SLASH_SLVR_SIZE);
#endif

		DEBUG_SLVR(PLL_DIAG, s, "crc=%"PSCPRIxCRC64, crc);

		*crcp = crc;
		slvr_2_crc(s) = crc;
		slvr_2_crcbits(s) |= BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_INFO, slvr_2_bmap(s),
		    "CRC update: slvr=%hu, crc=%"PSCPRIxCRC64,
		    s->slvr_num, slvr_2_crc(s));
	} else if (s->slvr_flags & SLVRF_FAULTING) {
		if (slvr_2_crcbits(s) & BMAP_SLVR_CRCABSENT)
			return (SLERR_CRCABSENT);

		if ((slvr_2_crcbits(s) & BMAP_SLVR_DATA) &&
		    (slvr_2_crcbits(s) & BMAP_SLVR_CRC)) {

			psc_crc64_calc(&crc, slvr_2_buf(s, 0),
			    SLASH_SLVR_SIZE);

			if (crc != slvr_2_crc(s)) {
				DEBUG_BMAP(PLL_INFO, slvr_2_bmap(s),
				    "CRC failure: slvr=%hu, crc="
				    "%"PSCPRIxCRC64,
				    s->slvr_num, slvr_2_crc(s));
				return (PFLERR_BADCRC);
			}
		} else {
			return (0);
		}
	}

	return (-1);
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
			slvr_wio_done(a->aiocbr_slvrs[i], 0);
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
	    rq->rq_peer.nid), rq->rq_export);

	a->aiocbr_flags = SLI_AIOCBSF_REPL;
	INIT_PSC_LISTENTRY(&a->aiocbr_lentry);

	return (a);
}

struct sli_aiocb_reply *
sli_aio_reply_setup(struct pscrpc_request *rq, uint32_t len,
    uint32_t off, struct slvr **slvrs, int nslvrs, struct iovec *iovs,
    int niovs, enum rw rw)
{
	struct slashrpc_cservice *csvc;
	struct sli_aiocb_reply *a;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	int i;

	csvc = sli_getclcsvc(rq->rq_export);
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

		rc = pread(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
		    foff);

		if (psc_fault_here_rc(SLI_FAULT_FSIO_READ_FAIL, &errno,
		    EBADF))
			rc = -1;

		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR("fsio-read-fail");
		} else if (rc) {
			int crc_rc;

			pfl_opstat_add(sli_backingstore_iostats.rd, rc);

			/*
			 * When a file is truncated, the generation
			 * number increments and all CRCs should be
			 * invalid.  Luckily we can use a simple check
			 * here without resorting to a complicated
			 * protocol.
			 */
			SLVR_LOCK(s);
			crc_rc = slvr_do_crc(s, NULL);
			SLVR_ULOCK(s);

			if (crc_rc == PFLERR_BADCRC) {
				OPSTAT_INCR("fsio-read-crc-bad");
				DEBUG_SLVR(PLL_ERROR, s,
				    "bad crc blks=%d off=%zu",
				    nblks, foff);
			} else {
				OPSTAT_INCR("fsio-read-crc-good");
			}
		}

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

	if (rc < 0)
		DEBUG_SLVR(PLL_ERROR, s, "failed (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, foff, save_errno);

	else if ((uint32_t)rc != size) {
		DEBUG_SLVR(foff + size > slvr_2_fcmh(s)->
		    fcmh_sstb.sst_size ? PLL_DIAG : PLL_NOTICE, s,
		    "short I/O (rc=%zd, size=%u) "
		    "%s blks=%d off=%zu errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, foff, save_errno);
	} else {
		v8 = slvr_2_buf(s, sblk);
		DEBUG_SLVR(PLL_DIAG, s, "ok %s size=%u off=%zu"
		    " rc=%zd nblks=%d v8(%"PRIx64")",
		    (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    size, foff, rc, nblks, *v8);
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
    __unusedx int flags)
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

	if (s->slvr_flags & SLVRF_DATARDY)
		goto out1;

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
	goto out2;

 out1:
	SLVR_ULOCK(s);
 out2:
	BMAP_LOCK(b);
	return (rc);
}

__static void
slvr_schedule_crc_locked(struct slvr *s)
{
	s->slvr_flags &= ~SLVRF_LRU;
	s->slvr_flags |= SLVRF_CRCDIRTY;
	PFL_GETTIMESPEC(&s->slvr_ts);
	DEBUG_SLVR(PLL_DIAG, s, "sched crc");

	lc_remove(&sli_lruslvrs, s);
	lc_addqueue(&sli_crcqslvrs, s);
}

void
slvr_remove(struct slvr *s)
{
	struct bmap_iod_info *bii;

	DEBUG_SLVR(PLL_DEBUG, s, "freeing slvr");

	OPSTAT_INCR("slvr-put");

	SLVR_LOCK(s);
	if (s->slvr_flags & SLVRF_LRU)
		lc_remove(&sli_lruslvrs, s);
	else
		lc_remove(&sli_crcqslvrs, s);
	SLVR_ULOCK(s);

	bii = slvr_2_bii(s);

	BII_LOCK(bii);
	PSC_SPLAY_XREMOVE(biod_slvrtree, &bii->bii_slvrs, s);
	bmap_op_done_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

	if (s->slvr_slab)
		psc_pool_return(sl_bufs_pool, s->slvr_slab);
	psc_pool_return(slvr_pool, s);
}

/*
 * Remove vestige of old contents when the generation of a file changes
 * or when the file is unlinked.
 */
void
slvr_remove_all(struct fidc_membh *f)
{
	int i;
	struct bmap *b;
	struct slvr *s;
	struct bmap_iod_info *bii;
	struct psc_dynarray a;

	/*
	 * Use two loops to avoid entangled with some background
	 * operations.
	 */
	psc_dynarray_init(&a);
	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {

		bmap_op_start_type(b, BMAP_OPCNT_SLVR);
		psc_dynarray_add(&a, b);

		bii = bmap_2_bii(b);
		BII_LOCK(bii);
		while ((s = SPLAY_ROOT(&bii->bii_slvrs))) {

			if (!SLVR_TRYLOCK(s)) {
				BII_ULOCK(bii);
				pscthr_yield();
				BII_LOCK(bii);
				continue;
			}
			if (s->slvr_refcnt ||
		           (s->slvr_flags & (SLVRF_FREEING | SLVRF_FAULTING))) {
				SLVR_ULOCK(s);
				BII_ULOCK(bii);
				pscthr_yield();
				BII_LOCK(bii);
				continue;
			}
			s->slvr_flags |= SLVRF_FREEING;

			SLVR_ULOCK(s);
			BII_ULOCK(bii);

			slvr_remove(s);

			BII_LOCK(bii);
		}
		BII_ULOCK(bii);
	}
	DYNARRAY_FOREACH(b, i, &a)
		bmap_op_done_type(b, BMAP_OPCNT_SLVR);

	psc_dynarray_free(&a);
}

/*
 * Note that the sliver may be freed by this fuction.
 */
__static void 
slvr_lru_tryunpin_locked(struct slvr *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab);
	if (s->slvr_refcnt || (s->slvr_flags & SLVRF_CRCDIRTY)) {
		SLVR_ULOCK(s);
		return;
	}

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

	psc_assert(s->slvr_flags & SLVRF_LRU);
	psc_assert(s->slvr_flags & SLVRF_DATARDY);

	/*
	 * Locking convention: it is legal to request for a list lock
	 * while holding the sliver lock.  On the other hand, when you
	 * already hold the list lock, you should drop the list lock
	 * first before asking for the sliver lock or you should use
	 * trylock().
	 */
	lc_move2tail(&sli_lruslvrs, s);
	SLVR_ULOCK(s);
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
	SLVR_RLOCK(s);
	psc_assert(s->slvr_refcnt > 0);

	s->slvr_refcnt--;
	DEBUG_SLVR(PLL_DIAG, s, "decref");
	slvr_lru_tryunpin_locked(s);
}

/*
 * Called after a write on the given sliver has completed.
 */
void
slvr_wio_done(struct slvr *s, int repl)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_refcnt > 0);

	s->slvr_refcnt--;
	DEBUG_SLVR(PLL_DIAG, s, "decref");

	if (s->slvr_flags & SLVRF_DATAERR || repl) {
		slvr_lru_tryunpin_locked(s);
	} else {
		if (s->slvr_flags & SLVRF_LRU)
			slvr_schedule_crc_locked(s);
		SLVR_ULOCK(s);
	}
}

/*
 * Lookup or create a sliver reference, ignoring one that is being
 * freed.
 */
struct slvr *
_slvr_lookup(const struct pfl_callerinfo *pci, uint32_t num,
    struct bmap_iod_info *bii)
{
	struct slvr *s, *tmp1 = NULL, ts;
	struct sl_buffer *tmp2 = NULL;
	int alloc = 0;

	ts.slvr_num = num;

	BII_LOCK_ENSURE(bii);
 retry:
	s = SPLAY_FIND(biod_slvrtree, &bii->bii_slvrs, &ts);
	if (s) {
		SLVR_LOCK(s);
		/*
		 * Reuse SLVRF_DATAERR slivers is tricky, we might as well
		 * start from fresh.
		 */
		if (s->slvr_flags & (SLVRF_FREEING|SLVRF_DATAERR)) {
			SLVR_ULOCK(s);
			/*
			 * Lock is required to free the slvr.
			 * It must be held here to prevent the slvr
			 * from being freed before we release the lock.
			 */
			BII_ULOCK(bii);
			pscthr_yield();
			BII_LOCK(bii);
			goto retry;
		}

		s->slvr_refcnt++;

		SLVR_ULOCK(s);
	} else {
		if (!alloc) {
			alloc = 1;
			BII_ULOCK(bii);
			tmp1 = psc_pool_get(slvr_pool);
			tmp2 = psc_pool_get(sl_bufs_pool);
			BII_LOCK(bii);
			goto retry;
		}

		alloc = 0;

		OPSTAT_INCR("slvr-get");

		s = tmp1;
		memset(s, 0, sizeof(*s));
		s->slvr_num = num;
		s->slvr_bii = bii;
		INIT_PSC_LISTENTRY(&s->slvr_lentry);
		INIT_SPINLOCK(&s->slvr_lock);

		memset(tmp2->slb_base, 0, SLASH_SLVR_SIZE);
		s->slvr_slab = tmp2;
		s->slvr_refcnt = 1;

		PSC_SPLAY_XINSERT(biod_slvrtree, &bii->bii_slvrs, s);
		bmap_op_start_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

		/*
		 * Until the slab is added to the sliver, the sliver is
		 * private to the bmap's biod_slvrtree.
		 */
		s->slvr_flags |= SLVRF_LRU;
		/* note: lc_addtail() will grab the list lock itself */
		lc_addtail(&sli_lruslvrs, s);

	}
	if (alloc) {
		psc_pool_return(slvr_pool, tmp1);
		psc_pool_return(sl_bufs_pool, tmp2);
	}
	return (s);
}

/*
 * The reclaim function for sl_bufs_pool.  Note that our caller
 * psc_pool_get() ensures that we are called exclusively.
 */
int
slvr_buffer_reap(struct psc_poolmgr *m)
{
	static struct psc_dynarray a;
	struct slvr *s, *dummy;
	int i, n;

	psc_dynarray_init(&a);

	LIST_CACHE_LOCK(&sli_lruslvrs);
	LIST_CACHE_FOREACH_SAFE(s, dummy, &sli_lruslvrs) {
		DEBUG_SLVR(PLL_DIAG, s,
		    "considering for reap, nwaiters=%d",
		    psc_atomic32_read(&m->ppm_nwaiters));

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

		psc_dynarray_add(&a, s);
		s->slvr_flags |= SLVRF_FREEING;
		SLVR_ULOCK(s);

		if (psc_dynarray_len(&a) >=
		    psc_atomic32_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&sli_lruslvrs);

	n = psc_dynarray_len(&a);
	DYNARRAY_FOREACH(s, i, &a)
		slvr_remove(s);
	psc_dynarray_free(&a);

	if (!n || n < psc_atomic32_read(&m->ppm_nwaiters))
		psc_waitq_wakeone(&sli_slvr_waitq);

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

			(void)psc_fault_here_rc(SLI_FAULT_AIO_FAIL,
			    &iocb->iocb_rc, EIO);

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
slvr_cache_init(void)
{
	psc_poolmaster_init(&slvr_poolmaster,
	    struct slvr, slvr_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "slvr");
	slvr_pool = psc_poolmaster_getmgr(&slvr_poolmaster);

	psc_poolmaster_init(&sli_readaheadrq_poolmaster,
	    struct sli_readaheadrq, rarq_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "readaheadrq");
	sli_readaheadrq_pool = psc_poolmaster_getmgr(
	    &sli_readaheadrq_poolmaster);

	lc_reginit(&sli_readaheadq, struct sli_readaheadrq, rarq_lentry,
	    "readaheadq");

	lc_reginit(&sli_lruslvrs, struct slvr, slvr_lentry, "lruslvrs");
	lc_reginit(&sli_crcqslvrs, struct slvr, slvr_lentry, "crcqslvrs");

	if (slcfg_local->cfg_async_io) {
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

		pscthr_init(SLITHRT_AIO, sliaiothr_main, NULL, 0,
		    "sliaiothr");
	}

	sl_buffer_cache_init();
	slvr_worker_init();
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
	PFL_PRFLAG(SLVRF_CRCDIRTY, &fl, &seq);
	PFL_PRFLAG(SLVRF_FREEING, &fl, &seq);
	PFL_PRFLAG(SLVRF_ACCESSED, &fl, &seq);
	if (fl)
		printf(" unknown: %x", fl);
	printf("\n");
}
#endif
