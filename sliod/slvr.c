/* $Id$ */

#include "psc_ds/listcache.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pthrutil.h"

#include "bmap_iod.h"
#include "buffer.h"
#include "slvr.h"

struct psc_listcache lruSlvrs;   /* LRU list of clean slivers which may be reaped */
struct psc_listcache rpcqSlvrs;  /* Slivers ready to be crc'd and have their
				    crc's shipped to the mds. */

__static SPLAY_GENERATE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

__static void
slvr_lru_requeue(struct slvr_ref *s, int tail)
{
	/*
	 * Locking convention: it is legal to request for a list lock while
	 * holding the sliver lock.  On the other hand, when you already hold
	 * the list lock, you should drop the list lock first before asking
	 * for the sliver lock or you should use trylock().
	 */
	LIST_CACHE_LOCK(&lruSlvrs);
	if (tail)
		lc_move2tail(&lruSlvrs, s);
	else
		lc_move2head(&lruSlvrs, s);
	LIST_CACHE_ULOCK(&lruSlvrs);
}

/**
 * slvr_do_crc - given a sliver reference, take the crc of the respective
 *   data and attach the ref to an srm_bmap_crcup structure.
 * @s: the sliver reference.
 * Notes:  Don't hold the lock while taking the crc.
 */
int
slvr_do_crc(struct slvr_ref *s)
{
	psc_assert(s->slvr_flags & SLVR_PINNED);

	/* SLVR_FAULTING implies that we're bringing this data buffer
	 *   in from the filesystem.
	 * SLVR_CRCDIRTY means that DATARDY has been set and that
	 *   a write dirtied the buffer and invalidated the crc.
	 */
	psc_assert(s->slvr_flags & SLVR_FAULTING ||
		   s->slvr_flags & SLVR_CRCDIRTY);

	if (s->slvr_flags & SLVR_FAULTING) {
		if (!s->slvr_pndgreads) {
			/* Small RMW workaround
			 */
			psc_assert(s->slvr_pndgwrts);
			return(1);
		}

		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		/* This thread holds faulting status so all others are
		 *  waiting on us which means that exclusive access to
		 *  slvr contents is ours until we set SLVR_DATARDY.
		 */
		// XXX for now assert that all blocks are being processed,
		//  otherwise there's no guarantee that the entire slvr
		//  was read.
		psc_assert(!vbitmap_nfree(s->slvr_slab->slb_inuse));
		psc_assert(slvr_2_biodi_wire(s));

		if ((slvr_2_crcbits(s) & BMAP_SLVR_DATA) &&
		    (slvr_2_crcbits(s) & BMAP_SLVR_CRC)) {

			psc_crc64_calc(&s->slvr_crc, slvr_2_buf(s, 0),
				     SL_CRC_SIZE);
			if (s->slvr_crc != slvr_2_crc(s)) {
				DEBUG_SLVR(PLL_ERROR, s, "crc failed want=%"
					   PRIx64" got=%"PRIx64,
					   slvr_2_crc(s), s->slvr_crc);
				return (-EINVAL);
			}
		} else
			return (0);

	} else if (s->slvr_flags & SLVR_CRCDIRTY) {
		psc_assert(s->slvr_flags & SLVR_CRCING);

		psc_crc64_calc(&s->slvr_crc, slvr_2_buf(s, 0), SL_CRC_SIZE);

		DEBUG_SLVR(PLL_TRACE, s, "crc=%"PRIx64, s->slvr_crc);

		SLVR_LOCK(s);
		s->slvr_flags &= ~(SLVR_CRCING|SLVR_CRCDIRTY);
		if (slvr_2_biodi_wire(s)) {
			slvr_2_crc(s) = s->slvr_crc;
			slvr_2_crcbits(s) |= (BMAP_SLVR_DATA|BMAP_SLVR_CRC);
		}
		SLVR_ULOCK(s);

	} else
		abort();

	return (1);
}

__static int
slvr_fsio(struct slvr_ref *s, int sblk, uint32_t size, int rw)
{
	int	i;
	ssize_t	rc;
	int	nblks;
	int	save_errno;

	nblks = (size + SLASH_SLVR_BLKSZ - 1) / SLASH_SLVR_BLKSZ;

	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(rw == SL_READ || rw == SL_WRITE);

	if (rw == SL_READ) {
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		rc = pread(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
			   slvr_2_fileoff(s, sblk));
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
		if (nblks == SLASH_BLKS_PER_SLVR) {
			int crc_rc;

			crc_rc = slvr_do_crc(s);
			if (crc_rc == -EINVAL) {
				DEBUG_SLVR(PLL_ERROR, s,
					   "bad crc blks=%d off=%"PRIx64,
					   nblks, slvr_2_fileoff(s, sblk));
				return (crc_rc);
			}
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
		SLVR_LOCK(s);
		for (i = 0; i < nblks; i++) {
			//psc_assert(vbitmap_get(s->slvr_slab->slb_inuse,
			//	       sblk + i));
			vbitmap_unset(s->slvr_slab->slb_inuse, sblk + i);
		}
		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, sblk), size,
			    slvr_2_fileoff(s, sblk));
		SLVR_ULOCK(s);

		save_errno = errno;
	}

	if (rc < 0)
		DEBUG_SLVR(PLL_ERROR, s, "failed (rc=%zd, size=%u) "
			   "%s blks=%d off=%"PRIx64" errno=%d",
			   rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
			   nblks, slvr_2_fileoff(s, sblk), save_errno);

	else if (rc != size)
		DEBUG_SLVR(PLL_ERROR, s, "short io (rc=%zd, size=%u) "
			   "%s blks=%d off=%"PRIu64" errno=%d",
			   rc, size, (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"),
			   nblks, slvr_2_fileoff(s, sblk), save_errno);
	else {
		DEBUG_SLVR(PLL_INFO, s, "ok %s size=%u off=%"PRIu64" rc=%zd nblks=%d",
			   (rw == SL_WRITE ? "SL_WRITE" : "SL_READ"), size,
			   slvr_2_fileoff(s, sblk), rc, nblks);
		rc = 0;
	}

	//vbitmap_printbin1(s->slvr_slab->slb_inuse);

	return ((rc < 0) ? (int)-save_errno : (int)0);
}

/**
 * slvr_fsbytes_get - read in the blocks which have their respective bits set
 *   in slab bitmap, trying to coalesce where possible.
 * @s: the sliver.
 */
int
slvr_fsbytes_rio(struct slvr_ref *s)
{
	int nblks, blk, rc;
	size_t i;

	psc_trace("vbitmap_nfree() = %d",
		  vbitmap_nfree(s->slvr_slab->slb_inuse));

	if (!(s->slvr_flags & SLVR_DATARDY))
		psc_assert(s->slvr_flags & SLVR_FAULTING);

	psc_assert(s->slvr_flags & SLVR_PINNED);

	rc = 0;
	for (i = 0, nblks = 0; i < SLASH_BLKS_PER_SLVR; i++) {
		if (vbitmap_get(s->slvr_slab->slb_inuse, i)) {
			if (nblks == 0) {
				blk = i;
			}
			nblks++;
			continue;
		}
		if (nblks) {
			nblks = 0;
			rc = slvr_fsio(s, blk, nblks * SLASH_SLVR_BLKSZ, SL_READ);
			if (rc) {
				return (rc);
			}
		}
	}
	if (nblks) {
		rc = slvr_fsio(s, blk, nblks * SLASH_SLVR_BLKSZ, SL_READ);
	}
	return (rc);
}

/**
 *
 */
int
slvr_fsbytes_wio(struct slvr_ref *s, uint32_t size, uint32_t sblk)
{
	DEBUG_SLVR(PLL_INFO, s, "sblk=%u size=%u", sblk, size);

	return (slvr_fsio(s, sblk, size, SL_WRITE));
}

void
slvr_repl_prep(struct slvr_ref *s, int src_or_dst)
{
	psc_assert((src_or_dst == SLVR_REPLDST) || 
		   (src_or_dst == SLVR_REPLSRC));

	SLVR_LOCK(s);
	psc_assert(!(s->slvr_flags & SLVR_REPLDST) && 
		   !(s->slvr_flags & SLVR_REPLSRC));
	
	if (src_or_dst == SLVR_REPLSRC)
		psc_assert(s->slvr_pndgreads > 0);
	else
		psc_assert(s->slvr_pndgwrts > 0);	

	s->slvr_flags |= src_or_dst;

	DEBUG_SLVR(PLL_INFO, s, "replica_%s", (src_or_dst == SLVR_REPLSRC) ? 
		   "src" : "dst");

	SLVR_ULOCK(s);
}

void
slvr_slab_prep(struct slvr_ref *s, int rw)
{
	SLVR_LOCK(s);
	/* slvr_lookup() must pin all slvrs to avoid racing with
	 *   the reaper.
	 */
	psc_assert(s->slvr_flags & SLVR_PINNED);

	if (rw == SL_WRITE)
		s->slvr_pndgwrts++;
	else
		s->slvr_pndgreads++;

	if (s->slvr_flags & SLVR_NEW) {
		s->slvr_flags &= ~SLVR_NEW;
		
		psc_assert(psclist_disjoint(&s->slvr_lentry));

		/* note: we grab a second lock here */
		s->slvr_slab = psc_pool_get(slBufsPool);
		sl_buffer_fresh_assertions(s->slvr_slab);

		DEBUG_SLVR(PLL_INFO, s, "should have slab");
		/* Until the slab is added to the sliver, the sliver is private
		 *  to the bmap's biod_slvrtree.
		 */
		s->slvr_flags |= SLVR_LRU;
		/* note: lc_addtail() will grab the list lock itself */
		lc_addtail(&lruSlvrs, s);

	} else if ((s->slvr_flags & SLVR_LRU) && !s->slvr_slab) {
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		s->slvr_slab = psc_pool_get(slBufsPool);
                sl_buffer_fresh_assertions(s->slvr_slab);

                DEBUG_SLVR(PLL_INFO, s, "should have slab");		
	}

	psc_assert(s->slvr_slab);
	SLVR_ULOCK(s);
}


int
slvr_io_prep(struct slvr_ref *s, uint32_t offset, uint32_t size, int rw)
{
	int blks, rc, unaligned[2] = {-1, -1};
	size_t i;

	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	/*
	 * Common courtesy requires us to wait for another threads' work FIRST.
	 * Otherwise, we could bail out prematurely when the data is ready without
	 * considering the range we want to write.
	 *
	 * Note we have taken our read or write references, so the sliver won't
	 * be freed from under us.
	 */
	if (s->slvr_flags & SLVR_FAULTING) {

		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		SLVR_WAIT(s);
		psc_assert(s->slvr_flags & SLVR_DATARDY);

		psc_assert(psclist_conjoint(&s->slvr_lentry));
	}

	DEBUG_SLVR(PLL_INFO, s, "slvrno=%hu off=%u size=%u rw=%o",
		   s->slvr_num, offset, size, rw);

	/* Don't bother marking the bit in the slash_bmap_wire structure,
	 *  in fact slash_bmap_wire may not even be present for this
	 *  sliver.  Just mark the bit in the sliver itself in
	 *  anticipation of the pending write.  The pndgwrts counter
	 *  cannot be used because it's decremented once the write
	 *  completes but prior the re-calculation of the slvr's crc
	 *  which is done asynchronously.
	 */
	if (rw == SL_WRITE) {
		s->slvr_flags |= SLVR_CRCDIRTY;

		if (s->slvr_flags & SLVR_DATARDY)
			/* Either read or write ops can just proceed if
			 *   SLVR_DATARDY is set, the sliver is prepared.
			 */
			goto set_write_dirty;

	} else if (rw == SL_READ) {
		if (s->slvr_flags & SLVR_DATARDY)
			goto out;

	}

	/* Importing data into the sliver is now our responsibility,
	 *  other IO into this region will block until SLVR_FAULTING
	 *  is released.
	 */
	s->slvr_flags |= SLVR_FAULTING;
	if (rw == SL_READ) {
		vbitmap_setall(s->slvr_slab->slb_inuse);
		goto do_read;
	}

 set_write_dirty:
	psc_assert(rw != SL_READ);

	if (!offset && size == SLASH_SLVR_SIZE) {
		/* Full sliver write, no need to read blocks from disk.
		 *  All blocks will be dirtied by the incoming network IO.
		 */
		vbitmap_setall(s->slvr_slab->slb_inuse);
		goto out;
	}
	/*
	 * Prepare the sliver for a read-modify-write.  Mark the blocks
	 * that need to be read as 1 so that they can be faulted in by
	 * slvr_fsbytes_io().  We can have at most two unaligned writes.
	 */
	if (offset) {
		blks = (offset / SLASH_SLVR_BLKSZ);
		if (offset & SLASH_SLVR_BLKMASK)
			unaligned[0] = blks;

		for (i=0; i <= blks; i++)
			vbitmap_set(s->slvr_slab->slb_inuse, i);
	}
	if ((offset + size) < SLASH_SLVR_SIZE) {
		blks = (offset + size) / SLASH_SLVR_BLKSZ;
		if ((offset + size) & SLASH_SLVR_BLKMASK)
			unaligned[1] = blks;

		for (i = blks; i < SLASH_BLKS_PER_SLVR; i++)
			vbitmap_set(s->slvr_slab->slb_inuse, i);
	}

	//vbitmap_printbin1(s->slvr_slab->slb_inuse);
	psc_info("vbitmap_nfree()=%d", vbitmap_nfree(s->slvr_slab->slb_inuse));
	/* We must have found some work to do.
	 */
	psc_assert(vbitmap_nfree(s->slvr_slab->slb_inuse) <
		   (int)SLASH_BLKS_PER_SLVR);

	if (s->slvr_flags & SLVR_DATARDY)
		goto invert;

 do_read:
	SLVR_ULOCK(s);
	/* Execute read to fault in needed blocks after dropping
	 *   the lock.  All should be protected by the FAULTING bit.
	 */
	if ((rc = slvr_fsbytes_rio(s)))
		return (rc);


	if (rw == SL_READ) {
		SLVR_LOCK(s);
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));

		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;

		vbitmap_invert(s->slvr_slab->slb_inuse);
		//vbitmap_printbin1(s->slvr_slab->slb_inuse);
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
		vbitmap_invert(s->slvr_slab->slb_inuse);
		if (unaligned[0] >= 0)
			vbitmap_set(s->slvr_slab->slb_inuse, unaligned[0]);

		if (unaligned[1] >= 0)
			vbitmap_set(s->slvr_slab->slb_inuse, unaligned[1]);
		//vbitmap_printbin1(s->slvr_slab->slb_inuse);
	out:
		SLVR_ULOCK(s);
	}

	return (0);
}

void
slvr_rio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);

	s->slvr_pndgreads--;
	if (!s->slvr_pndgreads         && 
	    !s->slvr_pndgwrts          && 
	    (s->slvr_flags & SLVR_LRU) && 
	    !(s->slvr_flags & SLVR_CRCDIRTY)) {
		/* Requeue does a listcache operation but using trylock so
		 *   no deadlock should occur on its behalf.
		 */
		slvr_lru_requeue(s, 1);
		slvr_lru_unpin(s);
		DEBUG_SLVR(PLL_DEBUG, s, "unpinned");
	} else
		DEBUG_SLVR(PLL_DEBUG, s, "ops still pending or dirty");

	SLVR_ULOCK(s);
}

void
slvr_try_rpcqueue(struct slvr_ref *s)
{
	SLVR_LOCK(s);

	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);

	DEBUG_SLVR(PLL_INFO, s, "try to queue for rpc");
	if (s->slvr_flags & SLVR_RPCPNDG) {
		SLVR_ULOCK(s);
		return;
	}

	if (!s->slvr_pndgwrts) {

		psc_assert(s->slvr_flags & SLVR_LRU);
		s->slvr_flags &= ~SLVR_LRU;
		s->slvr_flags |= SLVR_RPCPNDG;

		lc_remove(&lruSlvrs, s);
		lc_addqueue(&rpcqSlvrs, s);

	}
	SLVR_ULOCK(s);
}

/**
 * slvr_wio_done - called after a write rpc has completed.  The sliver may
 *    be FAULTING which is handled separately from DATARDY.  If FAULTING,
 *    this thread must wake up sleepers on the bmap waitq.
 * Notes: conforming with standard lock ordering, this routine drops
 *    the sliver lock prior to performing list operations.
 */
void
slvr_wio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_pndgwrts > 0);

	s->slvr_flags |= SLVR_CRCDIRTY;       

	if (s->slvr_flags & SLVR_REPLDST) {
		/* This was a replication dest slvr.  Adjust the slvr flags 
		 *    so that the slvr may be freed on demand.
		 */
		DEBUG_SLVR(PLL_INFO, s, "replication complete");

		psc_assert(s->slvr_pndgwrts == 1);
		psc_assert(s->slvr_flags & SLVR_PINNED);
		psc_assert(!(s->slvr_flags & SLVR_CRCDIRTY));
		s->slvr_pndgwrts--;
		s->slvr_flags &= ~SLVR_PINNED;
			       
		SLVR_ULOCK(s);

		slvr_lru_requeue(s, 0);
		return;
	}

	if (s->slvr_flags & SLVR_FAULTING) {
		/* This sliver was being paged-in over the network.
		 */
		psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		psc_assert(!(s->slvr_flags & SLVR_REPLDST));
		
		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;
		
		DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");
		/* Other threads may be waiting for DATARDY to either
		 *   read or write to this sliver.  At this point it's
		 *   safe to wake them up.
		 * Note: when iterating over the lru list for 
		 *   reclaiming, slvrs with pending writes must be 
		 *   skipped.
		 */
		SLVR_WAKEUP(s);
		
	} else if (s->slvr_flags & SLVR_DATARDY) {
		
		DEBUG_SLVR(PLL_INFO, s, "%s", "datardy");
		
		if ((s->slvr_flags & SLVR_LRU) &&
		    s->slvr_pndgwrts > 1)
			slvr_lru_requeue(s, 1);
	} else
		DEBUG_SLVR(PLL_FATAL, s, "invalid state");
	
	if (--s->slvr_pndgwrts == 0 && 
	    !(s->slvr_flags & SLVR_RPCPNDG)) {
		/* No more pending writes, try to schedule the buffer
		 *   to be crc'd.
		 */
		SLVR_ULOCK(s);
		slvr_try_rpcqueue(s);
	} else
		SLVR_ULOCK(s);
}

struct slvr_ref *
slvr_lookup(uint16_t num, struct bmap_iod_info *b, int op)
{
	struct slvr_ref *s, ts;

	psc_assert(b->biod_bmap);
	ts.slvr_num = num;
 retry:
	spinlock(&b->biod_lock);

	s = SPLAY_FIND(biod_slvrtree, &b->biod_slvrs, &ts);
	/* Note, slvr lock and biod lock are the same.
	 */
	if (s && (s->slvr_flags & SLVR_FREEING)) {
		if (op == SLVR_LOOKUP_DEL)
			psc_assert(SPLAY_REMOVE(biod_slvrtree,
					&b->biod_slvrs, s));
		else {
			freelock(&b->biod_lock);
			sched_yield();
			goto retry;
		}

	} else if (!s && (op == SLVR_LOOKUP_ADD)) {
		s = PSCALLOC(sizeof(*s));

		s->slvr_num = num;
		s->slvr_flags = SLVR_NEW | SLVR_SPLAYTREE;
		s->slvr_pri = b;
		s->slvr_slab = NULL;
		INIT_PSCLIST_ENTRY(&s->slvr_lentry);

		SPLAY_INSERT(biod_slvrtree, &b->biod_slvrs, s);
	}

	s->slvr_flags |= SLVR_PINNED;

	freelock(&b->biod_lock);

	return (s);
}

__static void
slvr_remove(struct slvr_ref *s)
{
	struct bmap_iod_info	*b;
	int                      locked;

	DEBUG_SLVR(PLL_WARN, s, "freeing slvr");
	/* Slvr should be detached from any listheads.
	 */
	psc_assert(psclist_disjoint(&s->slvr_lentry));

	b = slvr_2_biod(s);
	locked = reqlock(&b->biod_lock);
	SPLAY_REMOVE(biod_slvrtree, &b->biod_slvrs, s);
	ureqlock(&b->biod_lock, locked);

	PSCFREE(s);
}

/*
 * The reclaim function for the slBufsPoolMaster pool.  Note that our
 *   caller psc_pool_get() ensures that we are called exclusviely.
 */
static int
slvr_buffer_reap(struct psc_poolmgr *m)
{
	int			 i;
	int			 n;
	int                      locked;
	struct dynarray		 a;
	struct slvr_ref		*s;
	struct slvr_ref		*dummy;

	ENTRY;

	n = 0;
	dynarray_init(&a);
	LIST_CACHE_LOCK(&lruSlvrs);
	psclist_for_each_entry_safe(s, dummy, &lruSlvrs.lc_listhd,
				    slvr_lentry) {
		DEBUG_SLVR(PLL_INFO, s, "considering for reap");

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
			dynarray_add(&a, s);
			s->slvr_flags |= SLVR_FREEING;
			psclist_del(&s->slvr_lentry);
			lruSlvrs.lc_size--;
			goto next;
		}

		psc_assert(s->slvr_slab);

		if (slvr_lru_slab_freeable(s)) {
			/* At this point we know that the slab can be
			 *   reclaimed, however the slvr itself may
			 *   have to stay.
			 */
			dynarray_add(&a, s);
			s->slvr_flags |= SLVR_SLBFREEING;
			n++;
		}
	next:

		SLVR_URLOCK(s, locked);
		if (n >= atomic_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&lruSlvrs);

	for (i = 0; i < dynarray_len(&a); i++) {
		s = dynarray_getpos(&a, i);

		if (s->slvr_flags & SLVR_SLBFREEING) {

			psc_assert(!(s->slvr_flags & SLVR_FREEING));
			psc_assert(s->slvr_slab);

			DEBUG_SLVR(PLL_WARN, s, "freeing slvr slab=%p", s->slvr_slab);
			s->slvr_flags &= ~(SLVR_SLBFREEING|SLVR_DATARDY);
			psc_pool_return(m, s->slvr_slab);
			s->slvr_slab = NULL;

		} else if (s->slvr_flags & SLVR_FREEING) {

			psc_assert(!(s->slvr_flags & SLVR_SLBFREEING));
			psc_assert(!s->slvr_slab);
			if (s->slvr_flags & SLVR_SPLAYTREE) {
				s->slvr_flags &= ~SLVR_SPLAYTREE;
				slvr_remove(s);
			}
		}
	}
	dynarray_free(&a);

	return (n);
}

void
slvr_cache_init(void)
{
	lc_reginit(&lruSlvrs,  struct slvr_ref, slvr_lentry, "lruSlvrs");
	lc_reginit(&rpcqSlvrs,  struct slvr_ref, slvr_lentry, "rpcqSlvrs");

	psc_poolmaster_init(&slBufsPoolMaster, struct sl_buffer,
		    slb_mgmt_lentry, PPMF_AUTO, 64, 64, 128,
		    sl_buffer_init, sl_buffer_destroy, slvr_buffer_reap,
		    "svlr_slab", NULL);
	slBufsPool = psc_poolmaster_getmgr(&slBufsPoolMaster);

	slvr_worker_init();
}
