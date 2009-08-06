/* $Id$ */

#include "psc_types.h"

#include "psc_rpc/rpc.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"

#include "slvr.h"
#include "iod_bmap.h"
#include "buffer.h"

struct list_cache lruSlvrs;     /* Clean slivers which may be reaped */
struct list_cache rpcqSlvrs;    /*  */
struct list_cache inflSlvrs;    /* Inflight slivers go here once their
			         *  crc value has been copied into a crcup
			         *  request.  Is needed because the sliver
			         *  can only be inflight once.  Also prevents 
			         *  worker threads from spinning on the dirty 
			         *  list.
			         */



__static SPLAY_GENERATE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);


__static void
slvr_lru_requeue(const struct slvr_ref *s)
{
	if (LIST_CACHE_TRYLOCK(&lruSlvrs)) {
		lc_requeue(&lruSlvrs, s);
		LIST_CACHE_ULOCK(&lruSlvrs);
	}
}
/**
 * slvr_do_crc - given a sliver reference, take the crc of the respective
 *   data and attach the ref to an srm_bmap_crcup structure.
 * @s: the sliver reference.
 * Notes:  Don't hold the lock while taking the crc.
 */
__static int
slvr_do_crc(struct slvr_ref *s)
{
	psc_crc_t crc;

	psc_assert(!SLVR_LOCK_ENSURE(s));
	psc_assert(s->slvr_flags & SLVR_PINNED);
	/* SLVR_FAULTING implies that we're bringing this data buffer
	 *   in from the filesystem.  
	 * SLVR_CRCDIRTY means that DATARDY has been set and that 
	 *   a write dirtied the buffer and invalidated the crc.
	 */
	psc_assert(s->slvr_flags & SLVR_FAULTING || 
		   s->slvr_flags & SLVR_CRCDIRTY);
	
	if (s->slvr_flags & SLVR_FAULTING) {
		psc_assert(!s->slvr_flags & SLVR_DATARDY);
		psc_assert(atomic_read(&s->slvr_pndgreads));
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
			
			psc_crc_calc(&crc, slvr_2_buf(s), SL_CRC_SIZE);
			if (crc != slvr_2_crc(s)) {
				DEBUG_SLVR(PLL_ERROR, s, "crc failed want=%"
					   _P_U64"x got=%"_P_U64"x", 
					   slvr_2_crc(s), crc);
				return (-EINVAL);
			}
		} else
			return (0);

	} else if (s->slvr_flags & SLVR_CRCDIRTY) {
		SLVR_LOCK(s);
		psc_assert(s->slvr_flags & SLVR_SCHEDULED);
		s->slvr_flags |= SLVR_CRCING;
		SLVR_ULOCK(s);
		
		psc_crc_calc(&slvr_2_crc(s), slvr_2_buf(s), SL_CRC_SIZE);
		slvr_2_crcbits(s) |= (BMAP_SLVR_DATA|BMAP_SLVR_CRC);

		SLVR_LOCK(s);
                s->slvr_flags &= ~(SLVR_CRCING|SLVR_CRCDIRTY);
                SLVR_ULOCK(s);
	}

	return (1);
}

#if 0
void
slvr_update(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	s->slvr_updates++;
	clock_gettime(&s->slvr_ts);
	SLVR_ULOCK(s);
	
	if (s->slvr_flags & SLVR_NEW)
		lc_queue(&dirtySlvrs, &s->slvr_lentry);
	else 
		lc_requeue(&dirtySlvrs, &s->slvr_lentry);
}
#endif

__static void
slvr_release(struct slvr_ref *s)
{
        struct bmap_iod_info *biod = s->slvr_pri;

        psc_assert(biod);
	/* Lock the biod which protects the sliver and biod
	 *   tree.  If no one has updated the sliver during the 
	 *   crc and rpc procedures then free it.
	 */
        spinlock(&biod->biod_lock);
        if (s->slvr_updates) {
                freelock(&biod->biod_lock);
                return;
        }

        if (SPLAY_REMOVE(biod_slvrtree, &biod->biod_slvrs, s))
                PSCFREE(s);
        else
                psc_assert("Could not locate sliver for removal");

        freelock(&biod->biod_lock);
}

int
slvr_init(struct slvr_ref *s, uint16_t num, void *pri)
{
	s->slvr_num = num;
	s->slvr_flags = SLVR_NEW;
	s->slvr_pri = pri;
	s->slvr_slab = NULL;
	s->slvr_updates = 0;
	INIT_PSCLIST_ENTRY(&s->slvr_lentry);
}
 
__static void
slvr_getslab(struct slvr_ref *s) {
	struct slvr_ref *s;

	psc_assert(s->slvr_flags & SLVR_GETSLAB);
	psc_assert(!s->slvr_slab);
	
	s->slvr_slab = psc_pool_get(slBufsFreePool);
	sl_buffer_fresh_assertions(s->slvr_slab);

	s->slvr_flags |= SLVR_PINNED;
	s->slvr_flags &= ~SLVR_GETSLAB;
	/* Until the slab is added to the sliver, the sliver is private
	 *  to the bmap's biod_slvrtree.  
	 */
	lc_addtail(&lruSlvrs, s);

	SLVR_WAKEUP(s);
}


__static int
slvr_fsio(struct slvr_ref *s, int blk, int nblks, int rw)
{
	ssize_t rc, len = (nblks * SLASH_SLVR_BLKSZ);

	psc_assert(s->slvr_flags & SLVR_PINNED);
	
	if (rw == SL_READ) {
		psc_assert(s->slvr_flags & SLVR_FAULTING);
		rc = pread(slvr_2_fd(s), slvr_2_buf(s, blk), len,
			   slvr_2_fileoff(s, blk));		
		/* XXX this is a bit of a hack.  Here we'll check crc's
		 *  only when nblks == an entire sliver.  Only RMW will
		 *  have their checks bypassed.  This should probably be
		 *  handled more cleanly, like checking for RMW and then
		 *  grabbing the crc table, we use the 1MB buffer in 
		 *  either case.
		 */
		if (nblks == SLASH_BLKS_PER_SLVR)
			slvr_do_crc(s);
		
	} else {	
		size_t i;

		rc = pwrite(slvr_2_fd(s), slvr_2_buf(s, blk), 
			    len, slvr_2_fileoff(s, blk));

		/* Denote that this block(s) have been synced to the 
		 *  filesystem.
		 * Should this check and set of the block bits be
		 *  done for read also?  Probably not because the fs
		 *  is only read once and that's protected by the 
		 *  FAULT bit.  Also, we need to know which blocks 
		 *  to mark as dirty after an RPC.
		 */
		SLVR_LOCK(s);
		for (i=0; i < nblks; i++) {
			psc_assert(vbitmap_set(s->slvr_slab->slb_inuse,
					       blk + i));
			vbitmap_unset(s->slvr_slab->slb_inuse, blk + i);
		}
		SLVR_ULOCK(s);
	}

	if (rc != len)
		DEBUG_SLVR(PLL_ERROR, s, "failed blks=%d off=%"_P_U64"x", 
			   nblks, slvr_2_fileoff(s, blk));	
	else {
		DEBUG_SLVR(PLL_TRACE, s, "ok blks=%d off=%"_P_U64"x",
			   nblks, slvr_2_fileoff(s, blk));
		rc = 0;
	}
	return (rc ? (int)-errno : (int)0);	
}

/**
 * slvr_fsbytes_get - read in the blocks which have their respective bits set 
 *   in slab bitmap, trying to coalesce where possible.
 * @s: the sliver.
 */
__static int
slvr_fsbytes_io(struct slvr_ref *s, int rw)
{
	int nblks, blk, rc;
	size_t i;
	
	psc_assert(s->slvr_flags & SLVR_FAULTING);
	psc_assert(s->slvr_flags & SLVR_PINNED);

#define slvr_fsbytes_RW							\
	if (nblks) {							\
		if ((rc = (slvr_fsio(s, rw))))				\
			return (rc);					\
		nblks = 0;						\
	}								\
	
	for (i=0, blk=0, nblks=0; i < SLASH_BLKS_PER_SLVR; i++) {
		if (vbitmap_get(s->slvr_slab->slb_inuse, i)) {
			if (nblks) 
				nblks++;
			else { 
				blk = i; 
				nblks = 1; 
			}
		} else {
			slvr_fsbytes_RW;
		}
	}
	slvr_fsbytes_RW;

	return (0);
}

void
slvr_slab_prep(struct slvr_ref *s)
{
	SLVR_LOCK(s);

	if (s->slvr_flags & SLVR_NEW) {
		s->slvr_flags &= ~SLVR_NEW;
		s->slvr_flags |= SLVR_GETSLAB;
		SLVR_ULOCK(s);
		/* Set the flag and drop the lock before entering       
		 *  slvr_getslab().                                     
		 */
		slvr_getslab(s);
		SLVR_LOCK(s);
	} else {
	retry_getslab:
		psc_assert((s->slvr_flags & SLVR_GETSLAB) || s->slvr_slab);

		if (s->slvr_flags & SLVR_GETSLAB) {
			SLVR_WAIT(s);
			goto retry_getslab;
		}	      
	}
	/* At this point the sliver should at least have a slab. 
	 *  Next, with the lock held ensure the slvr / slab pair don't 
	 *  go anywhere.  slvr_lru_pin() does extensive checking to 
	 *  ensure the sliver is in the correct state.
	 */		
	slvr_lru_pin(s);
	SLVR_ULOCK(s);
}


int
slvr_io_prep(struct slvr_ref *s, uint32_t offset, uint32_t size, int rw)
{
	int blks, rc;
	size_t i;

	SLVR_LOCK(s);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(psclist_conjoint(&s->slvr_lentry));

	/* Don't bother marking the bit in the slash_bmap_wire structure, 
	 *  in fact slash_bmap_wire may not even be present for this 
	 *  sliver.  Just mark the bit in the sliver itself in 
	 *  anticipation of the pending write.  The pndgwrts counter 
	 *  cannot be used because it's decremented once the write
	 *  completes but prior the re-calculation of the slvr's crc
	 *  which is done asynchronously.
	 */
	if (rw == SL_WRITE)
		s->slvr_flags |= SLVR_CRCDIRTY;

	atomic_inc(rw == SL_WRITE ? &s->slvr_pndgwrts : &s->slvr_pndgreads);

 retry:
	if (s->slvr_flags & SLVR_DATARDY)
		/* Either read or write ops can just proceed if SLVR_DATARDY
		 *  is set, the sliver is prepared.
		 */
		goto out;

	if (s->slvr_flags & SLVR_FAULTING) {
		/* Another thread is either pulling this sliver from
		 *   the network or disk.  Wait here for it to complete.
		 * The pending write ref taken above should ensure 
		 *   that the sliver isn't freed from under us.
		 * Note that SLVR_WAIT() regains the lock.
		 */
		SLVR_WAIT(s);
		psc_assert(s->slvr_flags & SLVR_DATARDY);
		psc_assert(s->slvr_flags & SLVR_PINNED);
		psc_assert(psclist_conjoint(&s->slvr_lentry));
		goto out;

	} else {
		/* Importing data into the sliver is now our responsibility,
		 *  other IO into this region will block until SLVR_FAULTING
		 *  is released.
		 */
		s->slvr_flags |= SLVR_FAULTING;
		if (rw == SL_READ) {
			vbitmap_setall(s->slvr_slab->slb_inuse);
			goto do_read;
		}
	}

	psc_assert(rw != SL_READ);

	if (!offset && size == SLASH_SLVR_SIZE) {
		/* Full sliver write, no need to read blocks from disk.
		 *  All blocks will be dirtied by the incoming network IO.   
		 */
		vbitmap_setall(s->slvr_slab->slb_inuse);
		goto out;
	}
	/* Prepare the sliver for a read-modify-write.  Mark the blocks 
	 *    NOT affected by the write as '1' so that they can be faulted 
	 *    in by slvr_fsbytes_io().
	 */		
	if (offset) {
		/* Unaffected blocks at the beginning of the sliver.
		 */
		blks = (offset / SLASH_SLVR_BLKSZ);
		if (offset & SLASH_SLVR_BLKMASK)
			++blks;
		
		for (i=0; i < blks; i++)
			vbitmap_set(s->slvr_slab->slb_inuse, i);
	}
	/* Mark any blocks at the end.
	 */
	if ((offset + size) < SLASH_SLVR_SIZE) {
		blks = ((SLASH_SLVR_SIZE - (offset + size)) / 
			SLASH_SLVR_BLKSZ);

		if ((offset + size) & SLASH_SLVR_BLKMASK)
			++blks;

		for (i=SLASH_BLKS_PER_SLVR-1; blks--; i--)
			vbitmap_set(s->slvr_slab->slb_inuse, i);
	}
	/* We must have found some work to do.
	 */
	psc_assert(vbitmap_nfree(s->slvr_slab->slb_inuse) < 
		   SLASH_BLKS_PER_SLVR);

 do_read:
	SLVR_ULOCK(s);
	/* Execute read to fault in needed blocks after dropping
	 *   the lock.  All should be protected by the FAULTING bit.
	 */
	if ((rc = slvr_fsbytes_io(s, SL_READ)))
		return (rc);

	if (rw == SL_WRITE) {
		/* Above, the bits were set for the RMW blocks, now 
		 *  that they have been read, invert the bitmap so that
		 *  it properly represents the blocks to be dirtied by
		 *  the rpc.
		 */
		SLVR_LOCK(s);
		vbitmap_invert(s->slvr_slab->slb_inuse);		
	out:
		SLVR_ULOCK(s);
	} 
	return (0);
}

void
slvr_rio_done(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	
	if (!atomic_read(&s->slvr_pndgwrts) && 
	    atomic_dec_and_test(&s->slvr_pndgreads) && 
	    (s->slvr_flags & SLVR_LRU)) {
		/* Requeue does a listcache operation but using trylock so 
		 *   no deadlock should occur on its behalf.
		 */
		slvr_lru_requeue(s);
		slvr_lru_unpin(s);
		DEBUG_SLVR(PLL_DEBUG, s, "unpinned");
	} else
		DEBUG_SLVR(PLL_DEBUG, s, "ops still pending or dirty");

	SLVR_ULOCK(s);
}

__static void
slvr_try_rpcqueue(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);

	DEBUG_SLVR(PLL_INFO, s, "try to queue for rpc");

	if (s->svr_flags & SLVR_RPCPNDG) {
		/* It's already here or it's in the process of being 
		 *   moved.
		 */
		SLVR_ULOCK(s);
		return;
	}

	if (!atomic_read(&s->slvr_pndgwrts)) { 
		/* No writes are pending, perform the move to the rpcq 
		 *   list.  Set the bit first then drop the lock.
		 */
		s->svr_flags |= SLVR_RPCPNDG;		
		SLVR_ULOCK(s);
		
		lc_remove(&lruSlvrs, s);
		/* Don't drop the SLVR_LRU bit until the sliver has been 
		 *   removed.
		 */
		SLVR_LOCK(s);
		/* If we set SLVR_RPCPNDG then no one else may have 
		 *   unset SLVR_LRU.
		 */
		psc_assert(s->slvr_flags & SLVR_LRU);
		s->slvr_flags &= ~SLVR_LRU;
		SLVR_ULOCK(s);

		lc_queue(rpcqSlvrs, s);

	} else
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
	psc_assert(atomic_read(&s->slvr_pndgwrts) > 0);
	/* CRCDIRTY must have been marked and could not have been unset
	 *   because we have yet to pass this slvr to the crc processing
	 *   threads. 
	 */
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);

	if (s->slvr_flags & SLVR_FAULTING) {
		/* This sliver was being paged-in over the network.
		 */
                psc_assert(!(s->slvr_flags & SLVR_DATARDY));
		DEBUG_SLVR(PLL_INFO, s, "FAULTING -> DATARDY");

		s->slvr_flags |= SLVR_DATARDY;
		s->slvr_flags &= ~SLVR_FAULTING;
		/* Other threads may be waiting for DATARDY to either 
		 *   read or write to this sliver.  At this point it's 
		 *   safe to wake them up.
		 * Note: when iterating over the lru list for reclaiming, 
		 *   slvrs with pending writes must be skipped.
		 */
		SLVR_ULOCK(s);
		SLVR_WAKEUP(s);

        } else {
		psc_assert(s->slvr_flags & SLVR_DATARDY);
		DEBUG_SLVR(PLL_INFO, s, "DATARDY");

		if ((s->slvr_flags & SLVR_LRU) && 
		    atomic_read(&s->slvr_pndgwrts) > 1)
			slvr_lru_requeue(s);
		SLVR_ULOCK(s);
	} 
		
	if (atomic_dec_and_test(&s->slvr_pndgwrts))
		slvr_try_rpcqueue(s);
}

struct slvr_ref *
slvr_lookup(uint16_t num, struct bmap_iod_info *b, int add)
{
        struct slvr_ref *s, ts;

        psc_assert(b->biod_bmap);
        ts.slvr_num = num;

        spinlock(&b->biod_lock);

        s = SPLAY_FIND(biod_slvrtree, &b->biod_slvrs, &ts);
        if (!s && add) {
		s = PSCALLOC(sizeof(*s));
                slvr_init(s, num, b);
                SPLAY_INSERT(biod_slvrtree, &b->biod_slvrs, s);
        }
        freelock(&b->biod_lock);

        return (s);
}

void 
slvr_worker(void)
{
	struct slvr_ref *s;

 start:
	s = lc_getwait(&rpcqSlvrs);

	SLVR_LOCK(s);

	psc_assert(!(s->slvr_flags & SLVR_LRU));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_RPCPNDG);
	
	/* Try our best to determine whether or we should hold off 
	 *   the crc operation, strive to only crc slivers which have 
	 *   no pending writes.  This section directly below may race 
	 *   with slvr_wio_done().
	 */
	if (atomic_read(s->slvr_pndgwrts) > 0) {
		if (!LIST_CACHE_TRYLOCK(&lruSlvrs)) {
			/* Don't deadlock, take the locks in the 
			 *   correct order.
			 */
			SLVR_ULOCK(s);
			/* SLVR_RPCPNDG bit will prevent the sliver from 
			 *   being re-added to the rpcq list.
			 */
			LIST_CACHE_LOCK(&lruSlvrs);
			SLVR_LOCK(s);
		}
		/* Guaranteed to have both locks.
		 */
		if (atomic_read(s->slvr_pndgwrts) > 0) {
			s->slvr_flags &= ~SLVR_RPCPNDG;
			s->slvr_flags |= SLVR_LRU;
			lc_queue(&lruSlvrs, s);
			SLVR_ULOCK(s);
			LIST_CACHE_ULOCK(&lruSlvrs);
			goto start;

		} else
			/* It was > 0 but another write must have just 
			 *   finished.
			 */
			LIST_CACHE_ULOCK(&lruSlvrs);
	}
	/* Ok, we've got a sliver to work on.
	 *   From this point until we set to inflight, the slvr_lentry 
	 *   should be disjointed.
	 */
	s->slvr_flags |= SLVR_SCHEDULED;
	SLVR_ULOCK(s);
	
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));

	psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_DATA);
	psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_CRC);
	
	/*
	  What needs to happen here?
	  . process elements from the dirtySlvr list
	      - involves calling do_crc and adding the sliver's crc 
	      info to a crcup packing rpc.
	      - will need a data structure to track partially filled 
	      crcup structures.
	      - slrmi_bmap_crcwrt() is the handler function on the mds.
	      - one tricky item concerns crcup serialization from a single
	      slvr.  in no circumstances may we have the same sliver 
	      inflight for 2 different updates - the mds has no means to 
	      determine which is the most current.  therefore we should avoid
	      processing slvrs which are still inflight.  conversely, when
	      a crcup is ack'd we must unset the inflight bit and either 
	      place the slvr on the lru or on the dirty list.
	  . need to figure out how items from the lru are reclaimed.
	      - this will most likely happen via the pool reclaim cb.
	*/

}

void
slvr_cache_init(void)
{
	int i;

	lc_reginit(&dirtySlvrs,  struct slvr_ref, slvr_lentry, "dirtySlvrs");
	
}
