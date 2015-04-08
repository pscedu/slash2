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
	psc_assert((s->slvr_flags & SLVR_PINNED) &&
		   (s->slvr_flags & SLVR_FAULTING ||
		    s->slvr_flags & SLVR_CRCDIRTY));

	if (s->slvr_flags & SLVR_FAULTING) {
		if (slvr_2_crcbits(s) & BMAP_SLVR_CRCABSENT)
			return (SLERR_CRCABSENT);

		/*
		 * This thread holds faulting status so all others are
		 * waiting on us which means that exclusive access to
		 * slvr contents is ours until we set SLVR_DATARDY.
		 */
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

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

	} else if (s->slvr_flags & SLVR_CRCDIRTY) {
		/*
		 * SLVR_CRCDIRTY means that DATARDY has been set and
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

		s->slvr_flags &= ~SLVR_CRCDIRTY;
		DEBUG_SLVR(PLL_DIAG, s, "crc=%"PSCPRIxCRC64, crc);

		*crcp = crc;
		slvr_2_crc(s) = crc;
		slvr_2_crcbits(s) |= BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_INFO, slvr_2_bmap(s),
		    "CRC update: slvr=%hu, crc=%"PSCPRIxCRC64,
		    s->slvr_num, slvr_2_crc(s));
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
		    (SLVR_DATARDY | SLVR_DATAERR));
		if (s->slvr_flags & SLVR_DATAERR)
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

	OPSTAT_INCR("repl_readaio");

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

	OPSTAT_INCR("slvr_aio_reply");

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
		if (s->slvr_flags & SLVR_FAULTING) {
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
	OPSTAT_INCR("iocb_free");
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
	psc_assert(s->slvr_flags & SLVR_FAULTING);
	psc_assert(!(s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR)));

	/* Prevent additions from new requests. */
	s->slvr_flags &= ~SLVR_FAULTING;

	slvr_iocb_release(s->slvr_iocb);
	s->slvr_iocb = NULL;

	if (rc) {
		/*
		 * There was a problem; unblock any waiters and
		 * tell them the bad news.
		 */
		s->slvr_flags |= SLVR_DATAERR;
		DEBUG_SLVR(PLL_ERROR, s, "error, rc=%d", rc);
		s->slvr_err = rc;
	} else {
		s->slvr_flags |= SLVR_DATARDY;
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

	OPSTAT_INCR("iocb_get");
	iocb = psc_pool_get(sli_iocb_pool);
	memset(iocb, 0, sizeof(*iocb));
	INIT_LISTENTRY(&iocb->iocb_lentry);
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
	s->slvr_flags |= SLVR_FAULTING;
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
		return (-EBADF);
	}

	errno = 0;
	if (rw == SL_READ) {
		OPSTAT_INCR("fsio_read");

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
			OPSTAT_INCR("fsio_read_fail");
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
				OPSTAT_INCR("fsio_read_crc_bad");
				DEBUG_SLVR(PLL_ERROR, s,
				    "bad crc blks=%d off=%zu",
				    nblks, foff);
			} else {
				OPSTAT_INCR("fsio_read_crc_good");
			}
		}

		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("read_wait_usecs",
		    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);
	} else {
		OPSTAT_INCR("fsio_write");

		sblk = off / SLASH_SLVR_BLKSZ;
		psc_assert((off % SLASH_SLVR_BLKSZ) == 0);
		foff = slvr_2_fileoff(s, sblk);
		nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;

		SLVR_LOCK(s);
		SLVR_WAIT(s, s->slvr_blkgreads > 0);
		SLVR_ULOCK(s);

		/*
		 * We incremented pndgwrts so any blocking reads should
		 * wait for this counter to reach zero.
		 */

		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
		    foff);
		if (rc == -1) {
			save_errno = errno;
			OPSTAT_INCR("fsio_write_fail");
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
	ssize_t rc = 0;

	rc = slvr_fsio(s, off, size, SL_READ);

	if (rc && rc != -SLERR_AIOWAIT) {
		/*
		 * There was a problem; unblock any waiters and tell
		 * them the bad news.
		 */
		SLVR_LOCK(s);
		s->slvr_err = rc;
		s->slvr_flags |= SLVR_DATAERR;
		s->slvr_flags &= ~SLVR_FAULTING;
		DEBUG_SLVR(PLL_ERROR, s, "slvr_fsio() error, rc=%zd",
		    rc);
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);
	}

	return (rc);
}

ssize_t
slvr_fsbytes_wio(struct slvr *s, uint32_t sblk, uint32_t size)
{
	DEBUG_SLVR(PLL_DIAG, s, "sblk=%u size=%u", sblk, size);

	return (slvr_fsio(s, sblk * SLASH_SLVR_BLKSZ, size, SL_WRITE));
}

/*
 * Prepare a sliver as a replication target.
 */
void
slvr_repl_prep(struct slvr *s)
{
	SLVR_LOCK(s);
	SLVR_WAIT(s, s->slvr_flags & SLVR_FAULTING);

	if (s->slvr_flags & SLVR_DATARDY) {
		/*
		 * The slvr is about to be overwritten by this
		 * replication request.  For sanity's sake, wait
		 * for pending IO competion and set 'faulting'
		 * before proceeding.
		 */
		DEBUG_SLVR(PLL_WARN, s,
		    "MDS requested repldst of active slvr");
		SLVR_WAIT(s, s->slvr_pndgwrts > 1 || s->slvr_pndgreads);
		s->slvr_flags &= ~SLVR_DATARDY;
	}

	s->slvr_flags |= SLVR_FAULTING | SLVR_REPLWIRE;

	SLVR_ULOCK(s);
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
    int flags)
{
	ssize_t rc = 0;

	SLVR_LOCK(s);

	/*
	 * Note we have taken our read or write references, so the
	 * sliver won't be freed from under us.
	 */
	SLVR_WAIT(s, s->slvr_flags & SLVR_FAULTING);

	DEBUG_SLVR(s->slvr_flags & SLVR_DATAERR ?
	    PLL_ERROR : PLL_DIAG, s,
	    "slvrno=%hu off=%u len=%u rw=%d",
	    s->slvr_num, off, len, rw);

	if (s->slvr_flags & SLVR_DATAERR) {
		rc = s->slvr_err;
		goto out;
	}

	if (s->slvr_flags & SLVR_DATARDY) {
		if ((flags & SLVRF_READAHEAD) == 0 &&
		    s->slvr_flags & SLVRF_READAHEAD)
			OPSTAT_INCR("readahead-hit");
		goto out;
	}

	/*
	 * Importing data into the sliver is now our responsibility,
	 * other I/O into this region will block until SLVR_FAULTING is
	 * released.
	 */
	s->slvr_flags |= SLVR_FAULTING;

	if (rw == SL_WRITE && !off && len == SLASH_SLVR_SIZE) {
		/*
		 * Full sliver write, no need to read blocks from disk.
		 * All blocks will be dirtied by the incoming network
		 * IO.
		 */
		goto out;
	}

	if (flags & SLVRF_READAHEAD)
		s->slvr_flags |= SLVRF_READAHEAD;

	SLVR_ULOCK(s);

	/*
	 * Execute read to fault in needed blocks after dropping the
	 * lock.  All should be protected by the FAULTING bit.
	 */
	rc = slvr_fsbytes_rio(s, off, len);
	if (rc)
		return (rc);

	if (rw == SL_READ) {
		SLVR_LOCK(s);
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		if (flags & SLVR_WRLOCK) {
			s->slvr_flags |= SLVR_WRLOCK;
			s->slvr_owner = pthread_self();
		}
		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;

		DEBUG_SLVR(PLL_DIAG, s, "FAULTING -> DATARDY");
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);

		return (0);
	}

	SLVR_LOCK(s);

 out:
	if (rc == 0) {
		if (flags & SLVR_WRLOCK) {
			s->slvr_flags |= SLVR_WRLOCK;
			s->slvr_owner = pthread_self();
		}
		if (s->slvr_flags & SLVR_FAULTING) {
			s->slvr_flags |= SLVR_DATARDY;
			s->slvr_flags &= ~SLVR_FAULTING;
			DEBUG_SLVR(PLL_DIAG, s, "FAULTING -> DATARDY");
			SLVR_WAKEUP(s);
		}
	}
	SLVR_ULOCK(s);
	return (rc);
}

__static void
slvr_schedule_crc_locked(struct slvr *s)
{
	s->slvr_flags &= ~SLVR_LRU;
	DEBUG_SLVR(PLL_DIAG, s, "sched crc");

	lc_remove(&sli_lruslvrs, s);
	lc_addqueue(&sli_crcqslvrs, s);
}

__static void
slvr_remove(struct slvr *s)
{
	struct bmap_iod_info *bii;
	struct sl_buffer *tmp = s->slvr_slab;

	DEBUG_SLVR(PLL_DEBUG, s, "freeing slvr");

	lc_remove(&sli_lruslvrs, s);

	bii = slvr_2_bii(s);

	BII_LOCK(bii);
	PSC_SPLAY_XREMOVE(biod_slvrtree, &bii->bii_slvrs, s);
	bmap_op_done_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

	if (tmp) {
		s->slvr_slab = NULL;
		psc_pool_return(sl_bufs_pool, tmp);
	}

	if ((s->slvr_flags & (SLVRF_READAHEAD | SLVRF_ACCESSED)) ==
	    SLVRF_READAHEAD)
		OPSTAT_INCR("readahead-waste");

	psc_pool_return(slvr_pool, s);
}

int
slvr_lru_tryunpin_locked(struct slvr *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab);
	if (s->slvr_pndgwrts || s->slvr_pndgreads ||
	    s->slvr_flags & SLVR_CRCDIRTY)
		return (0);

	psc_assert(s->slvr_flags & SLVR_LRU);
	psc_assert(s->slvr_flags & SLVR_PINNED);

	psc_assert(s->slvr_flags & (SLVR_DATARDY | SLVR_DATAERR));

	s->slvr_flags &= ~SLVR_PINNED;

	/*
	 * Locking convention: it is legal to request for a list lock
	 * while holding the sliver lock.  On the other hand, when you
	 * already hold the list lock, you should drop the list lock
	 * first before asking for the sliver lock or you should use
	 * trylock().
	 */
	lc_move2tail(&sli_lruslvrs, s);
	return (1);
}

/*
 * Called after a read on the given sliver has completed.
 */
void
slvr_rio_done(struct slvr *s)
{
	SLVR_RLOCK(s);

	s->slvr_pndgreads--;
	DEBUG_SLVR(PLL_DIAG, s, "read decref");
	if (slvr_lru_tryunpin_locked(s))
		DEBUG_SLVR(PLL_DIAG, s, "decref, unpinned");
	else
		DEBUG_SLVR(PLL_DIAG, s, "decref, ops still pending or "
		    "dirty");

	SLVR_ULOCK(s);
}

/*
 * Called after a write on the given sliver has completed.
 */
void
slvr_wio_done(struct slvr *s, int repl)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_pndgwrts > 0);

	s->slvr_pndgwrts--;
	DEBUG_SLVR(PLL_DIAG, s, "write decref");

	PFL_GETTIMESPEC(&s->slvr_ts);

	if (s->slvr_flags & SLVR_DATAERR || repl) {
		s->slvr_flags &= ~SLVR_CRCDIRTY;
		slvr_lru_tryunpin_locked(s);
	} else {
		s->slvr_flags |= SLVR_CRCDIRTY;
		if (!s->slvr_pndgwrts && (s->slvr_flags & SLVR_LRU))
			slvr_schedule_crc_locked(s);
	}

	SLVR_ULOCK(s);
}

/*
 * Lookup or create a sliver reference, ignoring one that is being
 * freed.
 */
struct slvr *
_slvr_lookup(const struct pfl_callerinfo *pci, uint32_t num,
    struct bmap_iod_info *bii, enum rw rw)
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
		if (s->slvr_flags & SLVR_FREEING) {
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

		s->slvr_flags |= SLVR_PINNED;

		if (rw == SL_WRITE) {
			if (++s->slvr_pndgwrts == 0)
				psc_fatalx("max pndgwrts limit reached");
		} else {
			if (++s->slvr_pndgreads == 0)
				psc_fatalx("max pndgreads limit reached");
		}

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

		s = tmp1;
		memset(s, 0, sizeof(*s));
		s->slvr_num = num;
		s->slvr_flags = SLVR_PINNED;
		s->slvr_bii = bii;
		INIT_PSC_LISTENTRY(&s->slvr_lentry);
		INIT_SPINLOCK(&s->slvr_lock);

		memset(tmp2->slb_base, 0, SLASH_SLVR_SIZE);
		s->slvr_slab = tmp2;

		if (rw == SL_WRITE)
			s->slvr_pndgwrts = 1;
		else
			s->slvr_pndgreads = 1;

		PSC_SPLAY_XINSERT(biod_slvrtree, &bii->bii_slvrs, s);
		bmap_op_start_type(bii_2_bmap(bii), BMAP_OPCNT_SLVR);

		/*
		 * Until the slab is added to the sliver, the sliver is
		 * private to the bmap's biod_slvrtree.
		 */
		s->slvr_flags |= SLVR_LRU;
		/* note: lc_addtail() will grab the list lock itself */
		lc_addtail(&sli_lruslvrs, s);

	}
	if (rw == SL_WRITE)
		DEBUG_SLVR(PLL_DIAG, s, "write incref");
	else
		DEBUG_SLVR(PLL_DIAG, s, "read incref");

	if (alloc) {
		psc_pool_return(slvr_pool, tmp1);
		psc_pool_return(sl_bufs_pool, tmp2);
	}
	return (s);
}

/**
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

		if (s->slvr_flags & SLVR_PINNED) {
			SLVR_ULOCK(s);
			continue;
		}

		psc_dynarray_add(&a, s);
		s->slvr_flags |= SLVR_FREEING;
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
slirathr_main(struct psc_thread *thr)
{
	struct sli_readaheadrq *rarq;
	struct bmapc_memb *b;
	struct fidc_membh *f;
	struct slvr *s;
	int i, slvrno;

	while (pscthr_run(thr)) {
		f = NULL;
		b = NULL;

		if (slcfg_local->cfg_async_io)
			break;

		rarq = lc_getwait(&sli_readaheadq);
		if (sli_fcmh_peek(&rarq->rarq_fg, &f))
			goto skip;
		slvrno = rarq->rarq_off / SLASH_SLVR_SIZE;
		if (bmap_get(f, rarq->rarq_bno, SL_READ, &b))
			goto skip;
		for (i = 0; i < 4; i++) {
			s = slvr_lookup(slvrno + i, bmap_2_bii(b),
			    SL_READ);
			slvr_io_prep(s, 0, SLASH_SLVR_SIZE, SL_READ,
			    SLVRF_READAHEAD);

			/*
			 * FixMe: This cause asserts on flags when we
			 * encounter AIOWAIT. We need a unified way to
			 * perform I/O done on each sliver instead of
			 * sprinkling them all over the place.
			 */
			slvr_rio_done(s);
		}

 skip:
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
	int i;

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

	for (i = 0; i < NSLVR_READAHEAD_THRS; i++)
		pscthr_init(SLITHRT_READAHEAD, slirathr_main, NULL, 0,
		    "slirathr%d", i);

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

	PFL_PRFLAG(SLVR_FAULTING, &fl, &seq);
	PFL_PRFLAG(SLVR_PINNED, &fl, &seq);
	PFL_PRFLAG(SLVR_DATARDY, &fl, &seq);
	PFL_PRFLAG(SLVR_DATAERR, &fl, &seq);
	PFL_PRFLAG(SLVR_LRU, &fl, &seq);
	PFL_PRFLAG(SLVR_CRCDIRTY, &fl, &seq);
	PFL_PRFLAG(SLVR_FREEING, &fl, &seq);
	PFL_PRFLAG(SLVR_AIOWAIT, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLWIRE, &fl, &seq);
	if (fl)
		printf(" unknown: %x", fl);
	printf("\n");
}
#endif
