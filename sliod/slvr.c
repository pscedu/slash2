/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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
 * This file contains definitions for operations on slivers.  Slivers
 * are 1MB sections of bmaps.
 */

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include "psc_ds/listcache.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pthrutil.h"

#include "bmap_iod.h"
#include "buffer.h"
#include "fidc_iod.h"
#include "rpc_iod.h"
#include "slerr.h"
#include "sltypes.h"
#include "slvr.h"

volatile sig_atomic_t	 sli_aio_work;

struct psc_poolmaster	 sli_iocbset_poolmaster;
struct psc_poolmaster	 sli_iocb_poolmaster;

struct psc_poolmgr	*sli_iocbset_pool;
struct psc_poolmgr	*sli_iocb_pool;

struct psc_listcache	 sli_iocb_pndg;

psc_atomic64_t		 sli_aio_id = PSC_ATOMIC64_INIT(0);

struct psc_listcache	lruSlvrs;   /* LRU list of clean slivers which may be reaped */
struct psc_listcache	crcqSlvrs;  /* Slivers ready to be CRC'd and have their
				     * CRCs shipped to the MDS. */

__static SPLAY_GENERATE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

__static void
slvr_lru_requeue(struct slvr_ref *s, int tail)
{
	/*
	 * Locking convention: it is legal to request for a list lock
	 * while holding the sliver lock.  On the other hand, when you
	 * already hold the list lock, you should drop the list lock
	 * first before asking for the sliver lock or you should use
	 * trylock().
	 */
	LIST_CACHE_LOCK(&lruSlvrs);
	if (tail)
		lc_move2tail(&lruSlvrs, s);
	else
		lc_move2head(&lruSlvrs, s);
	LIST_CACHE_ULOCK(&lruSlvrs);
}

/**
 * slvr_do_crc - Given a sliver reference, Take the CRC of the
 *	respective data and attach the ref to an srm_bmap_crcup
	structure.
 * @s: the sliver reference.
 * Notes:  Don't hold the lock while taking the CRC.
 * Returns: errno on failure, 0 on success, -1 on not applicable.
 */
int
slvr_do_crc(struct slvr_ref *s)
{
	uint64_t crc;

	/*
	 * SLVR_FAULTING implies that we're bringing this data buffer
	 *   in from the filesystem.
	 *
	 * SLVR_CRCDIRTY means that DATARDY has been set and that
	 *   a write dirtied the buffer and invalidated the CRC.
	 */
	psc_assert(s->slvr_flags & SLVR_PINNED &&
		   (s->slvr_flags & SLVR_FAULTING ||
		    s->slvr_flags & SLVR_CRCDIRTY));

	if (s->slvr_flags & SLVR_FAULTING) {
		if (!s->slvr_pndgreads && !(s->slvr_flags & SLVR_REPLDST)) {
			/*
			 * Small RMW workaround.
			 *  XXX needs to be rectified, the CRC should
			 *    be taken here.
			 */
			psc_assert(s->slvr_pndgwrts);
			return (-1);
		}

		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		/*
		 * This thread holds faulting status so all others are
		 *  waiting on us which means that exclusive access to
		 *  slvr contents is ours until we set SLVR_DATARDY.
		 *
		 * XXX For now we assume that all blocks are being
		 *  processed, otherwise there's no guarantee that the
		 *  entire slvr was read.
		 */
		if (!(s->slvr_flags & SLVR_REPLDST))
			psc_assert(!psc_vbitmap_nfree(s->slvr_slab->slb_inuse));

		else
			psc_assert((slvr_2_crcbits(s) & BMAP_SLVR_DATA) &&
				   (slvr_2_crcbits(s) & BMAP_SLVR_CRC));

		if ((slvr_2_crcbits(s) & BMAP_SLVR_DATA) &&
		    (slvr_2_crcbits(s) & BMAP_SLVR_CRC)) {
			psc_assert(!s->slvr_crc_soff);

			psc_crc64_calc(&crc, slvr_2_buf(s, 0),
			    SLVR_CRCLEN(s));

			if (crc != slvr_2_crc(s)) {
				DEBUG_SLVR(PLL_ERROR, s, "CRC failed "
				    "want=%"PSCPRIxCRC64" "
				    "got=%"PSCPRIxCRC64" len=%u",
				    slvr_2_crc(s), crc, SLVR_CRCLEN(s));

				DEBUG_BMAP(PLL_ERROR, slvr_2_bmap(s),
				   "CRC failed slvrnum=%hu", s->slvr_num);

				/* Shouldn't need a lock, !SLVR_DATADY
				 */
				s->slvr_crc_eoff = 0;

				return (SLERR_BADCRC);
			}
			s->slvr_crc_eoff = 0;
		} else
			return (0);

	} else if (s->slvr_flags & SLVR_CRCDIRTY) {

		uint32_t soff, eoff;

		SLVR_LOCK(s);
		DEBUG_SLVR(PLL_NOTIFY, s, "len=%u soff=%u loff=%u",
		   SLVR_CRCLEN(s), s->slvr_crc_soff, s->slvr_crc_loff);

		psc_assert(s->slvr_crc_eoff &&
		    (s->slvr_crc_eoff <= SLASH_BMAP_CRCSIZE));

		if (!s->slvr_crc_loff ||
		    s->slvr_crc_soff != s->slvr_crc_loff) {
			/* Detect non-sequential write pattern into the
			 *   slvr.
			 */
			PSC_CRC64_INIT(&s->slvr_crc);
			s->slvr_crc_soff = 0;
			s->slvr_crc_loff = 0;
		}
		/* Copy values in preparation for lock release.
		 */
		soff = s->slvr_crc_soff;
		eoff = s->slvr_crc_eoff;

		SLVR_ULOCK(s);

#ifdef ADLERCRC32
		// XXX not a running CRC?  double check for correctness
		s->slvr_crc = adler32(s->slvr_crc, slvr_2_buf(s, 0) + soff,
		    (int)(eoff - soff));
		crc = s->slvr_crc;
#else
		psc_crc64_add(&s->slvr_crc, slvr_2_buf(s, 0) + soff,
		    (int)(eoff - soff));
		crc = s->slvr_crc;
		PSC_CRC32_FIN(&crc);
#endif

		DEBUG_SLVR(PLL_NOTIFY, s, "crc=%"PSCPRIxCRC64" len=%u soff=%u",
		    crc, SLVR_CRCLEN(s), s->slvr_crc_soff);

		DEBUG_BMAP(PLL_NOTIFY, slvr_2_bmap(s),
		    "slvrnum=%hu", s->slvr_num);

		SLVR_LOCK(s);
		/* loff is only set here.
		 */
		s->slvr_crc_loff = eoff;

		if (!s->slvr_pndgwrts && !s->slvr_compwrts)
			s->slvr_flags &= ~SLVR_CRCDIRTY;
		//XXX needs a bmap lock here, not a biodi lock
		slvr_2_crc(s) = crc;
		slvr_2_crcbits(s) |= BMAP_SLVR_DATA | BMAP_SLVR_CRC;
		SLVR_ULOCK(s);
	} else
		psc_fatal("FAULTING or CRCDIRTY is not set");

	return (-1);
}

void
slvr_clear_inuse(struct slvr_ref *s, int sblk, uint32_t size)
{
	int locked, nblks;

	/* XXX trim startoff from size?? */
	nblks = howmany(size, SLASH_SLVR_BLKSZ);
	locked = SLVR_RLOCK(s);
	psc_vbitmap_unsetrange(s->slvr_slab->slb_inuse, sblk, nblks);
	SLVR_URLOCK(s, locked);
}

void
slvr_fsaio_done(struct sli_iocb *iocb)
{
	struct aiocb *aio = &iocb->iocb_aiocb;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct sli_iocb_set *iocbs;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct slvr_ref *s;
	int rc;

	iocbs = iocb->iocb_set;
	s = iocb->iocb_slvr;
	if (iocb->iocb_rc) {
		/*
		 * There was a problem; unblock any waiters and
		 * tell them the bad news.
		 */
		SLVR_LOCK(s);
		s->slvr_flags |= SLVR_DATAERR;
		DEBUG_SLVR(PLL_ERROR, s, "slvr_fsio() error, rc=%zd",
		    iocb->iocb_rc);
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);
	}
	if (iocb->iocb_rc) {
		SLVR_LOCK(s);
		s->slvr_pndgreads--;
		slvr_clear_inuse(s, 0, SLASH_SLVR_SIZE);
		slvr_lru_tryunpin_locked(s);
		DEBUG_SLVR(PLL_WARN, s,
		    "unwind ref due to async I/O bulk error");
		SLVR_ULOCK(s);
	}

	SLVR_LOCK(s);
	psc_assert(!(s->slvr_flags & SLVR_DATARDY));

	s->slvr_flags |= SLVR_DATARDY;
	s->slvr_flags &= ~SLVR_FAULTING;

	psc_vbitmap_invert(s->slvr_slab->slb_inuse);
	//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);
	DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");
	SLVR_WAKEUP(s);
	SLVR_ULOCK(s);

	slvr_io_done(s, aio->aio_offset, aio->aio_nbytes,
	    iocb->iocb_rw);

	spinlock(&iocbs->iocbs_lock);
	if ((iocbs->iocbs_flags & SLI_IOCBSF_DONE) == 0 ||
	    iocbs->iocbs_refcnt != 1)
		goto out;
	freelock(&iocbs->iocbs_lock);

	/* now perform PUT RPC to client */
	csvc = sli_getclcsvc(iocb->iocb_peer);
	if (csvc == NULL)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(&mq->sbd, &iocb->iocb_sbd, sizeof(mq->sbd));
	mq->id = iocb->iocb_id;
	mq->size = iocb->iocb_aiocb.aio_nbytes;
	mq->offset = aio->aio_offset;
	mq->op = SRMIOP_RD;
	mq->rc = iocb->iocb_rc;
	if (mq->rc)
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	else
		mq->rc = rsx_bulkserver(rq, BULK_PUT_SOURCE,
		    SRCI_BULK_PORTAL, iocbs->iocbs_iovs,
		    iocbs->iocbs_niov);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	reqlock(&iocbs->iocbs_lock);
	if (--iocbs->iocbs_refcnt == 0 &&
	    (iocbs->iocbs_flags & SLI_IOCBSF_DONE))
		psc_pool_return(sli_iocbset_pool, iocbs);
	else
		freelock(&iocbs->iocbs_lock);
	pscrpc_export_put(iocb->iocb_peer);
}

int
sli_aio_register(struct pscrpc_request *rq, struct sli_iocb_set **iocbsp,
    struct slvr_ref *s, uint32_t size, int sblk, enum rw rw, int issue)
{
	struct sli_iocb_set *iocbs;
	struct sli_iocb *iocb;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct aiocb *aio;
	int error = 0;

	iocbs = *iocbsp;
	if (iocbs == NULL) {
		iocbs = *iocbsp = psc_pool_get(sli_iocbset_pool);
		INIT_SPINLOCK(&iocbs->iocbs_lock);
		iocbs->iocbs_refcnt = 1;
	} else {
		spinlock(&iocbs->iocbs_lock);
		iocbs->iocbs_refcnt++;
		freelock(&iocbs->iocbs_lock);
	}

	iocb = psc_pool_get(sli_iocb_pool);
	memset(iocb, 0, sizeof(*iocb));
	iocb->iocb_slvr = s;
	iocb->iocb_cbf = slvr_fsaio_done;
	iocb->iocb_rw = rw;
	iocb->iocb_peer = pscrpc_export_get(rq->rq_export);
	iocb->iocb_set = iocbs;

	aio = &iocb->iocb_aiocb;
	aio->aio_fildes = slvr_2_fd(s);
	aio->aio_offset = slvr_2_fileoff(s, sblk);
	aio->aio_buf = slvr_2_buf(s, sblk);
	aio->aio_nbytes = size;

	LIST_CACHE_LOCK(&sli_iocb_pndg);
	SLVR_LOCK(s);
	if (issue)
		error = aio_read(aio);
	if (error == 0)
		lc_add(&sli_iocb_pndg, iocb);
	SLVR_ULOCK(s);
	LIST_CACHE_ULOCK(&sli_iocb_pndg);

	if (error) {
		pscrpc_export_put(iocb->iocb_peer);
		psc_pool_return(sli_iocb_pool, iocb);
		return (-error);
	}

	/* XXX what about REPL_READ?? */

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	memcpy(&iocb->iocb_sbd, &mq->sbd, sizeof(mq->sbd));

	pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	mp->id = iocb->iocb_id = psc_atomic64_inc_getnew(&sli_aio_id);

	return (-EWOULDBLOCK);
}

__static int
slvr_fsio(struct pscrpc_request *rq, struct sli_iocb_set **iocbs,
    struct slvr_ref *s, int sblk, uint32_t size, enum rw rw, int aio,
    ssize_t aiorc)
{
	int i, nblks, save_errno = 0;
	uint64_t *v8;
	ssize_t	rc;

	nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;

	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(rw == SL_READ || rw == SL_WRITE);

	if (rw == SL_READ) {
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		errno = 0;

		if (aio)
			rc = aiorc;
		else if (globalConfig.gconf_async_io)
			return (sli_aio_register(rq, iocbs, s, size,
			    sblk, rw, 1));
		else
			rc = pread(slvr_2_fd(s), slvr_2_buf(s, sblk),
			    size, slvr_2_fileoff(s, sblk));
		if (rc == -1)
			save_errno = errno;

		/* XXX this is a bit of a hack.  Here we'll check crc's
		 *  only when nblks == an entire sliver.  Only RMW will
		 *  have their checks bypassed.  This should probably be
		 *  handled more cleanly, like checking for RMW and then
		 *  grabbing the crc table, we use the 1MB buffer in
		 *  either case.
		 */

		/* XXX do the right thing when EOF is reached..
		 */
		if (rc > 0 && nblks == SLASH_BLKS_PER_SLVR) {
			int crc_rc;

			s->slvr_crc_soff = 0;
			s->slvr_crc_eoff = rc;

			crc_rc = slvr_do_crc(s);
			if (crc_rc == SLERR_BADCRC)
				DEBUG_SLVR(PLL_ERROR, s,
				    "bad crc blks=%d off=%#"PRIx64,
				    nblks, slvr_2_fileoff(s, sblk));
		}
	} else {
		/* Denote that this block(s) have been synced to the
		 *  filesystem.
		 * Should this check and set of the block bits be
		 *  done for read also?  Probably not because the fs
		 *  is only read once and that's protected by the
		 *  FAULT bit.  Also, we need to know which blocks
		 *  to mark as dirty after an RPC.
		 */
		SLVR_LOCK(s); //ouch.. this may be negatively affecting
			      // performance.
		for (i = 0; i < nblks; i++) {
			//psc_assert(psc_vbitmap_get(s->slvr_slab->slb_inuse,
			//	       sblk + i));
			psc_vbitmap_unset(s->slvr_slab->slb_inuse, sblk + i);
		}
		errno = 0;
		SLVR_ULOCK(s);
		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
			    slvr_2_fileoff(s, sblk));
		if (rc == -1)
			save_errno = errno;
	}

	if (rc < 0)
		DEBUG_SLVR(PLL_ERROR, s, "failed (rc=%zd, size=%u) "
		    "%s blks=%d off=%#"PRIx64" errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, slvr_2_fileoff(s, sblk), save_errno);

	else if ((uint32_t)rc != size)
		DEBUG_SLVR(PLL_NOTICE, s, "short io (rc=%zd, size=%u) "
		    "%s blks=%d off=%"PRIu64" errno=%d",
		    rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    nblks, slvr_2_fileoff(s, sblk), save_errno);
	else {
		v8 = slvr_2_buf(s, sblk);
		DEBUG_SLVR(PLL_INFO, s, "ok %s size=%u off=%"PRIu64" rc=%zd nblks=%d "
		    " v8(%"PRIx64")", (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
		    size, slvr_2_fileoff(s, sblk), rc, nblks, *v8);
		rc = 0;
	}

	//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);

	return ((rc < 0) ? (int)-save_errno : (int)0);
}

/**
 * slvr_fsbytes_get - Read in the blocks which have their respective
 *	bits set in slab bitmap, trying to coalesce where possible.
 * @s: the sliver.
 */
int
slvr_fsbytes_rio(struct pscrpc_request *rq, struct sli_iocb_set **iocbs,
    struct slvr_ref *s)
{
	int i, rc, blk, nblks;

	psclog_trace("psc_vbitmap_nfree() = %d",
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
			rc = slvr_fsio(rq, iocbs, s, blk, nblks *
			    SLASH_SLVR_BLKSZ, SL_READ, 0, 0);
			if (rc)
				goto out;

			/* reset nblks so we won't do it again later */
			nblks = 0;
		}
	}

	if (nblks)
		rc = slvr_fsio(rq, iocbs, s, blk, nblks *
		    SLASH_SLVR_BLKSZ, SL_READ, 0, 0);

 out:
	if (rc) {
		/* There was a problem, unblock any waiters and tell them
		 *   the bad news.
		 */
		SLVR_LOCK(s);
		s->slvr_flags |= SLVR_DATAERR;
		DEBUG_SLVR(PLL_ERROR, s, "slvr_fsio() error, rc=%d", rc);
		SLVR_WAKEUP(s);
		SLVR_ULOCK(s);
	}

	return (rc);
}

int
slvr_fsbytes_wio(struct sli_iocb_set **iocbs, struct slvr_ref *s,
    uint32_t size, uint32_t sblk)
{
	DEBUG_SLVR(PLL_INFO, s, "sblk=%u size=%u", sblk, size);

	return (slvr_fsio(NULL, iocbs, s, sblk, size, SL_WRITE, 0, 0));
}

void
slvr_repl_prep(struct slvr_ref *s, int src_or_dst)
{
	psc_assert((src_or_dst == SLVR_REPLDST) ||
		   (src_or_dst == SLVR_REPLSRC));

	SLVR_LOCK(s);

	psc_assert(!(s->slvr_flags & (SLVR_REPLDST | SLVR_REPLSRC)));

	if (src_or_dst == SLVR_REPLSRC)
		psc_assert(s->slvr_pndgreads > 0);
	else {
		psc_assert(s->slvr_pndgwrts > 0);
		/* The slvr is about to be overwritten by this replication
		 *   request.   For sanity's sake, wait for pending io
		 *   competion and set 'faulting' before proceeding.
		 */
		if (s->slvr_flags & SLVR_DATARDY) {
			SLVR_WAIT(s, ((s->slvr_pndgwrts > 1) ||
				      s->slvr_pndgreads));
			s->slvr_flags &= ~SLVR_DATARDY;
		}
		s->slvr_flags |= SLVR_FAULTING;
		DEBUG_SLVR(PLL_INFO, s, "slvr dest ready");
	}

	s->slvr_flags |= src_or_dst;

	DEBUG_SLVR(PLL_INFO, s, "replica_%s", (src_or_dst == SLVR_REPLSRC) ?
		   "src" : "dst");

	SLVR_ULOCK(s);
}

void
slvr_slab_prep(struct slvr_ref *s, enum rw rw)
{
	struct sl_buffer *tmp = NULL;

	//XXX may have to lock bmap instead..
	SLVR_LOCK(s);

 restart:
	/* slvr_lookup() must pin all slvrs to avoid racing with
	 *   the reaper.
	 */
	psc_assert(s->slvr_flags & SLVR_PINNED);

	if (rw == SL_WRITE)
		psc_assert(s->slvr_pndgwrts > 0);
	else
		psc_assert(s->slvr_pndgreads > 0);

 newbuf:
	if (s->slvr_flags & SLVR_NEW) {
		if (!tmp) {
			/* Drop the lock before potentially blocking
			 *   in the pool reaper.  To do this we
			 *   must first allocate to a tmp pointer.
			 */
 getbuf:
			SLVR_ULOCK(s);

			tmp = psc_pool_get(slBufsPool);
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
		psc_pool_return(slBufsPool, tmp);
}

/**
 * slvr_io_prep - Prepare a sliver for an incoming I/O.  This may entail
 *   faulting 32k aligned regions in from the underlying fs.
 * @s: the sliver
 * @off: offset into the slvr (not bmap or file object)
 * @len: len relative to the slvr
 * @rw: read or write op
 */
int
slvr_io_prep(struct pscrpc_request *rq, struct sli_iocb_set **iocbs,
    struct slvr_ref *s, uint32_t off, uint32_t len, enum rw rw)
{
	int i, rc = 0, blks, unaligned[2] = { -1, -1 };

	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	/*
	 * Note we have taken our read or write references, so the sliver
	 *   won't be freed from under us.
	 */
	if (s->slvr_flags & SLVR_FAULTING && !(s->slvr_flags & SLVR_REPLDST)) {
		/* Common courtesy requires us to wait for another threads'
		 *   work FIRST. Otherwise, we could bail out prematurely
		 *   when the data is ready without considering the range
		 *   we want to write.
		 */
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		if (globalConfig.gconf_async_io)
			return (sli_aio_register(rq, iocbs, s, len, 0,
			    rw, 0));
		SLVR_WAIT(s, !(s->slvr_flags & (SLVR_DATARDY|SLVR_DATAERR)));
		psc_assert((s->slvr_flags & (SLVR_DATARDY|SLVR_DATAERR)));
	}

	DEBUG_SLVR(((s->slvr_flags & SLVR_DATAERR) ? PLL_ERROR : PLL_INFO), s,
		   "slvrno=%hu off=%u len=%u rw=%d",
		   s->slvr_num, off, len, rw);

	if (s->slvr_flags & SLVR_DATAERR) {
		rc = -1;
		goto out;

	} else if (s->slvr_flags & SLVR_DATARDY) {
		if (rw == SL_READ)
			goto out;

	} else if (!(s->slvr_flags & SLVR_REPLDST)) {
		/* Importing data into the sliver is now our responsibility,
		 *  other IO into this region will block until SLVR_FAULTING
		 *  is released.
		 */
		s->slvr_flags |= SLVR_FAULTING;
		if (rw == SL_READ) {
			psc_vbitmap_setall(s->slvr_slab->slb_inuse);
			goto do_read;
		}

	} else if (s->slvr_flags & SLVR_REPLDST) {
		/* The sliver is going to be used for replication.  Ensure
		 *   proper setup has occurred.
		 */
		psc_assert(!off);
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		psc_assert(s->slvr_pndgreads == 0 && s->slvr_pndgwrts == 1);

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
	/* We must have found some work to do.
	 */
	psc_assert(psc_vbitmap_nfree(s->slvr_slab->slb_inuse) <
		   SLASH_BLKS_PER_SLVR);

	if (s->slvr_flags & SLVR_DATARDY)
		goto invert;

 do_read:
	SLVR_ULOCK(s);
	/* Execute read to fault in needed blocks after dropping
	 *   the lock.  All should be protected by the FAULTING bit.
	 */
	if ((rc = slvr_fsbytes_rio(rq, iocbs, s)))
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

	} else {
		/* Above, the bits were set for the RMW blocks, now
		 *  that they have been read, invert the bitmap so that
		 *  it properly represents the blocks to be dirtied by
		 *  the rpc.
		 */
		SLVR_LOCK(s);
 invert:
		psc_vbitmap_invert(s->slvr_slab->slb_inuse);
		if (unaligned[0] >= 0)
			psc_vbitmap_set(s->slvr_slab->slb_inuse, unaligned[0]);

		if (unaligned[1] >= 0)
			psc_vbitmap_set(s->slvr_slab->slb_inuse, unaligned[1]);
		//psc_vbitmap_printbin1(s->slvr_slab->slb_inuse);
 out:
		if (!rc) {
			if (s->slvr_flags & SLVR_FAULTING) {
				s->slvr_flags |= SLVR_DATARDY;
				s->slvr_flags &= ~SLVR_FAULTING;
				DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");
				SLVR_WAKEUP(s);
			}
			SLVR_ULOCK(s);
		}
	}

	return (rc);
}

void
slvr_rio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);

	s->slvr_pndgreads--;
	if (slvr_lru_tryunpin_locked(s)) {
		slvr_lru_requeue(s, 1);
		DEBUG_SLVR(PLL_DEBUG, s, "unpinned");
	} else
		DEBUG_SLVR(PLL_DEBUG, s, "ops still pending or dirty");

	if (s->slvr_flags & SLVR_REPLSRC) {
		psc_assert((s->slvr_flags & SLVR_REPLDST) == 0);
		s->slvr_flags &= ~SLVR_REPLSRC;
	}

	SLVR_ULOCK(s);
}

__static void
slvr_schedule_crc_locked(struct slvr_ref *s)
{
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_LRU);

	slvr_2_biod(s)->biod_crcdrty_slvrs++;

	DEBUG_SLVR(PLL_INFO, s, "crc sched (ndirty slvrs=%u)",
		   slvr_2_biod(s)->biod_crcdrty_slvrs);

	s->slvr_flags &= ~SLVR_LRU;

	lc_remove(&lruSlvrs, s);
	lc_addqueue(&crcqSlvrs, s);
}

void slvr_slb_free_locked(struct slvr_ref *, struct psc_poolmgr *);

/**
 * slvr_wio_done - Called after a write RPC has completed.  The sliver
 *	may be FAULTING which is handled separately from DATARDY.  If
 *	FAULTING, this thread must wake up sleepers on the bmap waitq.
 * Notes: conforming with standard lock ordering, this routine drops
 *    the sliver lock prior to performing list operations.
 */
void
slvr_wio_done(struct slvr_ref *s, uint32_t off, uint32_t len)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_pndgwrts > 0);

	if (s->slvr_flags & SLVR_REPLDST) {
		/* This was a replication dest slvr.  Adjust the slvr flags
		 *    so that the slvr may be freed on demand.
		 */
		if (s->slvr_flags & SLVR_REPLFAIL)
			DEBUG_SLVR(PLL_ERROR, s, "replication failure");
		else
			DEBUG_SLVR(PLL_INFO, s, "replication complete");

		psc_assert(s->slvr_pndgwrts == 1);
		psc_assert(s->slvr_flags & SLVR_PINNED);
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		psc_assert(!(s->slvr_flags & SLVR_REPLSRC));
		psc_assert(!(s->slvr_flags & SLVR_CRCDIRTY));
		s->slvr_pndgwrts--;
		s->slvr_flags &= ~(SLVR_PINNED|SLVR_FAULTING|SLVR_REPLDST);

		if (s->slvr_flags & SLVR_REPLFAIL) {
			/* Perhaps this should block for any readers?
			 *   Technically it should be impossible since this
			 *   replica has yet to be registered with the mds.
			 */
			s->slvr_flags |= SLVR_SLBFREEING;
			slvr_slb_free_locked(s, slBufsPool);
			s->slvr_flags &= ~SLVR_REPLFAIL;

		} else {
			s->slvr_flags |= SLVR_DATARDY;
			SLVR_WAKEUP(s);
		}
		SLVR_ULOCK(s);

		slvr_lru_requeue(s, 0);
		return;
	}

	s->slvr_flags |= SLVR_CRCDIRTY;
	/*
	 * Manage the description of the dirty crc area.  If the slvr's
	 * checksum is not being processed then soff and len may be
	 * adjusted.  If soff doesn't align with loff then the slvr will
	 * be CRC'd from offset 0.
	 */
	s->slvr_crc_soff = off;

	if ((off + len) > s->slvr_crc_eoff)
		s->slvr_crc_eoff =  off + len;

	psc_assert(s->slvr_crc_eoff <= SLASH_BMAP_CRCSIZE);

	if (off != s->slvr_crc_loff)
		s->slvr_crc_loff = 0;

	if (!(s->slvr_flags & SLVR_DATARDY))
		DEBUG_SLVR(PLL_FATAL, s, "invalid state");

	DEBUG_SLVR(PLL_INFO, s, "%s", "datardy");

	if ((s->slvr_flags & SLVR_LRU) && s->slvr_pndgwrts > 1)
		slvr_lru_requeue(s, 1);

	/* If there are no more pending writes, schedule a CRC op.
	 *   Increment slvr_compwrts to prevent a crc op from being skipped
	 *   which can happen due to the release of the slvr lock being
	 *   released prior to the crc of the buffer.
	 */
	s->slvr_pndgwrts--;
	s->slvr_compwrts++;

	if (!s->slvr_pndgwrts && (s->slvr_flags & SLVR_LRU))
		slvr_schedule_crc_locked(s);

	SLVR_ULOCK(s);
}

/**
 * slvr_lookup - Lookup or create a sliver reference, ignoring one that
 *	is being freed.
 */
struct slvr_ref *
slvr_lookup(uint32_t num, struct bmap_iod_info *b, enum rw rw)
{
	struct slvr_ref *s, ts;

	ts.slvr_num = num;

 retry:
	/* Lock order:  BIOD then SLVR.
	 */
	BIOD_LOCK(b);

	s = SPLAY_FIND(biod_slvrtree, &b->biod_slvrs, &ts);

	if (s) {
		SLVR_LOCK(s);
		if (s->slvr_flags & SLVR_FREEING) {
			SLVR_ULOCK(s);
			BIOD_ULOCK(b);
			goto retry;
		}

	} else {
		s = PSCALLOC(sizeof(*s));

		s->slvr_num = num;
		s->slvr_flags = SLVR_NEW | SLVR_SPLAYTREE;
		s->slvr_pri = b;
		s->slvr_slab = NULL;
		INIT_PSC_LISTENTRY(&s->slvr_lentry);
		INIT_SPINLOCK(&s->slvr_lock);

		SPLAY_INSERT(biod_slvrtree, &b->biod_slvrs, s);
		bmap_op_start_type(bii_2_bmap(b), BMAP_OPCNT_SLVR);

		SLVR_LOCK(s);
	}
	BIOD_ULOCK(b);

	s->slvr_flags |= SLVR_PINNED;

	if (rw == SL_WRITE)
		s->slvr_pndgwrts++;
	else if (rw == SL_READ)
		s->slvr_pndgreads++;
	else
		abort();
	SLVR_ULOCK(s);
	return (s);
}

__static void
slvr_remove(struct slvr_ref *s)
{
	struct bmap_iod_info	*b;

	DEBUG_SLVR(PLL_DEBUG, s, "freeing slvr");
	/* Slvr should be detached from any listheads.
	 */
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(!(s->slvr_flags & SLVR_SPLAYTREE));
	psc_assert(s->slvr_flags & SLVR_FREEING);

	b = slvr_2_biod(s);

	BIOD_LOCK(b);
	SPLAY_REMOVE(biod_slvrtree, &b->biod_slvrs, s);
	bmap_op_done_type(bii_2_bmap(b), BMAP_OPCNT_SLVR);

	PSCFREE(s);
}

void
slvr_slb_free_locked(struct slvr_ref *s, struct psc_poolmgr *m)
{
	struct sl_buffer *tmp=s->slvr_slab;

	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_flags & SLVR_SLBFREEING);
	psc_assert(!(s->slvr_flags & SLVR_FREEING));
	psc_assert(s->slvr_slab);

	s->slvr_flags &= ~(SLVR_SLBFREEING | SLVR_DATARDY);

	DEBUG_SLVR(PLL_INFO, s, "freeing slvr slab=%p", s->slvr_slab);
	s->slvr_slab = NULL;
	SLVR_WAKEUP(s);

	psc_pool_return(m, tmp);
}

/**
 * slvr_buffer_reap - The reclaim function for slBufsPool.  Note that
 *	our caller psc_pool_get() ensures that we are called
 *	exclusviely.
 */
int
slvr_buffer_reap(struct psc_poolmgr *m)
{
	struct psc_dynarray a;
	struct slvr_ref *s, *dummy;
	int i, n, locked;

	n = 0;
	psc_dynarray_init(&a);
	LIST_CACHE_LOCK(&lruSlvrs);
	LIST_CACHE_FOREACH_SAFE(s, dummy, &lruSlvrs) {
		DEBUG_SLVR(PLL_INFO, s, "considering for reap, nwaiters=%d",
			   atomic_read(&m->ppm_nwaiters));

		/* We are reaping, so it is fine to back off on some
		 *   slivers.  We have to use a reqlock here because
		 *   slivers do not have private spinlocks, instead
		 *   they use the lock of the biod.  So if this thread
		 *   tries to free a slvr from the same biod trylock
		 *   will abort.
		 */
		if (!SLVR_TRYREQLOCK(s, &locked))
			continue;

		/* Look for slvrs which can be freed, slvr_lru_freeable()
		 *   returning true means that no slab is attached.
		 */
		if (slvr_lru_freeable(s)) {
			psc_dynarray_add(&a, s);
			s->slvr_flags |= SLVR_FREEING;
			lc_remove(&lruSlvrs, s);
			goto next;
		}

		if (slvr_lru_slab_freeable(s)) {
			/* At this point we know that the slab can be
			 *   reclaimed, however the slvr itself may
			 *   have to stay.
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
		}

		else if (s->slvr_flags & SLVR_FREEING) {

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

	return (n);
}

void
sigio_handler(__unusedx int sig)
{
	sli_aio_work = 1;
}

void
sliaiothr_main(__unusedx struct psc_thread *thr)
{
	struct sli_iocb *iocb, *next;

	for (;;) {
		usleep(500);
		if (!sli_aio_work)
			continue;
		sli_aio_work = 0;

		LIST_CACHE_LOCK(&sli_iocb_pndg);
		LIST_CACHE_FOREACH_SAFE(iocb, next, &sli_iocb_pndg) {
			iocb->iocb_rc = aio_error(&iocb->iocb_aiocb);
			if (iocb->iocb_rc == EINPROGRESS)
				continue;
			psc_assert(iocb->iocb_rc != ECANCELED);
			if (iocb->iocb_rc == 0)
				iocb->iocb_rc = aio_return(&iocb->iocb_aiocb);
			iocb->iocb_cbf(iocb);
			psc_pool_return(sli_iocb_pool, iocb);
		}
		LIST_CACHE_ULOCK(&sli_iocb_pndg);
	}
}

void
slvr_cache_init(void)
{
	lc_reginit(&lruSlvrs, struct slvr_ref, slvr_lentry, "lruslvrs");
	lc_reginit(&crcqSlvrs, struct slvr_ref, slvr_lentry, "crcqslvrs");

	if (globalConfig.gconf_async_io) {
		signal(SIGIO, sigio_handler);

		psc_poolmaster_init(&sli_iocb_poolmaster,
		    struct sli_iocb, iocb_lentry, PPMF_AUTO, 64, 64,
		    1024, NULL, NULL, NULL, "iocb");
		sli_iocb_pool = psc_poolmaster_getmgr(&sli_iocb_poolmaster);

		psc_poolmaster_init(&sli_iocbset_poolmaster,
		    struct sli_iocb_set, iocbs_lentry, PPMF_AUTO, 64,
		    64, 1024, NULL, NULL, NULL, "iocbset");
		sli_iocb_pool = psc_poolmaster_getmgr(&sli_iocbset_poolmaster);

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
	PFL_PRFLAG(SLVR_REPLSRC, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLDST, &fl, &seq);
	PFL_PRFLAG(SLVR_REPLFAIL, &fl, &seq);
	if (fl)
		printf(" unknown: %x", fl);
	printf("\n");
}
#endif
