/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "psc_types.h"
#include "psc_ds/listcache.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"

#include "slvr.h"
#include "iod_bmap.h"
#include "buffer.h"

struct psc_listcache lruSlvrs;   /* Clean slivers which may be reaped */
struct psc_listcache rpcqSlvrs;  /* Slivers ready to be crc'd and have their
				    crc's shipped to the mds. */

__static SPLAY_GENERATE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

__static void
slvr_lru_requeue(struct slvr_ref *s)
{
	if (LIST_CACHE_TRYLOCK(&lruSlvrs)) {
		lc_move2tail(&lruSlvrs, s);
		LIST_CACHE_ULOCK(&lruSlvrs);
	}
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
		psc_assert(psc_atomic16_read(&s->slvr_pndgreads));
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
			
			psc_crc_calc(&s->slvr_crc, slvr_2_buf(s, 0), 
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
		psc_assert(s->slvr_flags & SLVR_SCHEDULED);
		psc_assert(s->slvr_flags & SLVR_CRCING);
		
		psc_crc_calc(&s->slvr_crc, slvr_2_buf(s, 0), SL_CRC_SIZE);

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
	
	return (0);
}
 
__static void
slvr_getslab(struct slvr_ref *s)
{
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_GETSLAB);
	psc_assert(!s->slvr_slab);
	
	s->slvr_slab = psc_pool_get(slBufsPool);
	sl_buffer_fresh_assertions(s->slvr_slab);

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
		if ((uint32_t)nblks == SLASH_BLKS_PER_SLVR) {
			rc = slvr_do_crc(s);
			if (rc == -EINVAL) {
				DEBUG_SLVR(PLL_ERROR, s, 
					   "bad crc blks=%d off=%"PRIx64, 
					   nblks, slvr_2_fileoff(s, blk));
				return (rc);
			}
		}
		
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
		for (i=0; (ssize_t)i < nblks; i++) {
			psc_assert(vbitmap_xset(s->slvr_slab->slb_inuse,
					       blk + i) == 0);
			vbitmap_unset(s->slvr_slab->slb_inuse, blk + i);
		}
		SLVR_ULOCK(s);
	}

	if (rc != len)
		DEBUG_SLVR(PLL_ERROR, s, "failed blks=%d off=%"PRIx64, 
			   nblks, slvr_2_fileoff(s, blk));	
	else {
		DEBUG_SLVR(PLL_TRACE, s, "ok blks=%d off=%"PRIx64,
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
		if ((rc = (slvr_fsio(s, blk, nblks, rw))))		\
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
	/* Set the pin bit no matter what.
	 */
	s->slvr_flags |= SLVR_PINNED;

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
	psc_assert(s->slvr_slab);
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

	psc_atomic16_inc(rw == SL_WRITE ? 
			 &s->slvr_pndgwrts : &s->slvr_pndgreads);

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
		
		for (i=0; (ssize_t)i < blks; i++)
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
		   (int)SLASH_BLKS_PER_SLVR);

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
	
	if (!psc_atomic16_read(&s->slvr_pndgwrts) && 
	    psc_atomic16_dec_test_zero(&s->slvr_pndgreads) && 
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

void
slvr_try_rpcqueue(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);

	DEBUG_SLVR(PLL_INFO, s, "try to queue for rpc");

	if (s->slvr_flags & SLVR_RPCPNDG) {
		/* It's already here or it's in the process of being 
		 *   moved.
		 */
		SLVR_ULOCK(s);
		return;
	}

	if (!psc_atomic16_read(&s->slvr_pndgwrts)) { 
		/* No writes are pending, perform the move to the rpcq 
		 *   list.  Set the bit first then drop the lock.
		 */
		s->slvr_flags |= SLVR_RPCPNDG;		
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

		lc_addqueue(&rpcqSlvrs, s);

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
	psc_assert(psc_atomic16_read(&s->slvr_pndgwrts) > 0);
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
		    psc_atomic16_read(&s->slvr_pndgwrts) > 1)
			slvr_lru_requeue(s);
		SLVR_ULOCK(s);
	} 
		
	if (psc_atomic16_dec_test_zero(&s->slvr_pndgwrts))
		slvr_try_rpcqueue(s);
}

struct slvr_ref *
slvr_lookup(uint16_t num, struct bmap_iod_info *b, int add)
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
		freelock(&b->biod_lock);
		sched_yield();
		goto retry;

        } else if (!s && add) {
		s = PSCALLOC(sizeof(*s));
                slvr_init(s, num, b);
                SPLAY_INSERT(biod_slvrtree, &b->biod_slvrs, s);
        }
        freelock(&b->biod_lock);

        return (s);
}

__static void
slvr_remove(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	DEBUG_SLVR(PLL_WARN, s, "freeing slvr");

	psc_assert(s->slvr_flags & SLVR_FREEING);
	psc_assert(slvr_lru_freeable(s));
	SLVR_ULOCK(s);

	lc_del(&s->slvr_lentry, &lruSlvrs);

	psc_assert(s == slvr_lookup(s->slvr_num, slvr_2_biod(s), 0));
	
	spinlock(&(slvr_2_biod(s))->biod_lock);
        psc_assert(SPLAY_REMOVE(biod_slvrtree, 
			&(slvr_2_biod(s))->biod_slvrs, s));
	freelock(&(slvr_2_biod(s))->biod_lock);

	PSCFREE(s);
}

int
slvr_buffer_reap(struct psc_poolmgr *m)
{
	struct slvr_ref *s, *tmp;
	static pthread_mutex_t mutex = PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
	struct dynarray a = DYNARRAY_INIT;
	int i, n;

	psc_assert(m == slBufsPool);
	
	ENTRY;

	pthread_mutex_lock(&mutex);
	
	LIST_CACHE_LOCK(&lruSlvrs);
	psclist_for_each_entry_safe(s, tmp, &lruSlvrs.lc_listhd, slvr_lentry) {
		SLVR_LOCK(s);
		DEBUG_SLVR(PLL_INFO, s, "considering for reap");

		/* Look for slvrs which can be freed, slvr_lru_freeable() 
		 *   returning true means that no slab is attached.
		 */
		if (slvr_lru_freeable(s)) {
			dynarray_add(&a, s);
			s->slvr_flags |= SLVR_FREEING;
			lc_del(&s->slvr_lentry, &lruSlvrs);
			continue;
		}			

		if (slvr_lru_slab_freeable(s)) {
			/* At this point we know that the slb can be 
			 *   reclaimed, however the slvr itself may 
			 *   have to stay.
			 */
			dynarray_add(&a, s);
			s->slvr_flags |= SLVR_SLBFREEING;
			n++;
		}
		SLVR_ULOCK(s);

		if (n >= atomic_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&lruSlvrs);

	pthread_mutex_unlock(&mutex);

	for (i=0; i < dynarray_len(&a); i++) {
                s = dynarray_getpos(&a, i);

		psc_assert(s->slvr_flags & SLVR_SLBFREEING || 
			   s->slvr_flags & SLVR_FREEING);

		if (s->slvr_flags & SLVR_SLBFREEING) {
			DEBUG_SLVR(PLL_WARN, s, "freeing slvr slb");
			psc_pool_return(m, (void *)s->slvr_slab);

		} else if (s->slvr_flags & SLVR_FREEING)
			slvr_remove(s);
		
        }

	return (n);
}

void
slvr_cache_init(void)
{
	lc_reginit(&lruSlvrs,  struct slvr_ref, slvr_lentry, "lruSlvrs");
	lc_reginit(&rpcqSlvrs,  struct slvr_ref, slvr_lentry, "rpcqSlvrs");

	psc_poolmaster_init(&slBufsPoolMaster, struct sl_buffer, 
		    slb_mgmt_lentry, PPMF_AUTO, 256, 0, 1024, 
		    sl_buffer_init, sl_buffer_destroy, slvr_buffer_reap, 
		    "svlr_slab", NULL);
        slBufsPool = psc_poolmaster_getmgr(&slBufsPoolMaster);

	slvr_worker_init();
}
