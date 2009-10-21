/* $Id: */

#include "bmpc.h"

struct psc_poolmaster bmpcePoolMaster;
struct psc_poolmgr   *bmpcePoolMgr;
struct bmpc_mem_slbs  bmpcSlabs;
struct psc_listcache  bmpcLru;

static void 
bmpc_slb_init(struct sl_buffer *slb)
{
	slb->slb_inuse = vbitmap_new(BMPC_SLB_NBLKS);
        slb->slb_blksz = BMPC_BUFSZ;
        slb->slb_nblks = BMPC_SLB_NBLKS;
        slb->slb_base  = PSCALLOC(BMPC_SLB_NBLKS * BMPC_BUFSZ);
        atomic_set(&slb->slb_ref, 0);
        atomic_set(&slb->slb_unmapd_ref, 0);
        atomic_set(&slb->slb_inflight, 0);
        LOCK_INIT (&slb->slb_lock);
        slb->slb_flags = SLB_FRESH;
        INIT_PSCLIST_HEAD(&slb->slb_iov_list);
        INIT_PSCLIST_ENTRY(&slb->slb_fcm_lentry);
	INIT_PSCLIST_ENTRY(&slb->slb_mgmt_lentry);
        DEBUG_SLB(PLL_TRACE, slb, "new slb");
}

static void
bmpc_slb_free(struct sl_buffer *slb)
{
        DEBUG_SLB(PLL_TRACE, slb, "freeing slb");
	psc_assert(vbitmap_nfree(slb->slb_inuse) == BMPC_SLB_NBLKS);
	psc_assert(slb->slb_base == NULL);
	psc_assert(psclist_conjoint(&slb->slb_mgmt_lentry));
	psc_assert(!atomic_read(&slb->slb_ref));
	
	PSCFREE(slb);
}

static struct sl_buffer *
bmpc_slb_new(void)
{
	struct sl_buffer *slb;

	slb = TRY_PSCALLOC(sizeof(*slb));
	if (slb)
		bmpc_slb_init(slb);

	return (slb);
}

int
bmpc_grow(int nslbs)
{
	int i=0, nalloced, rc=0;

	lockBmpcSlabs;
	
	nalloced = pll_nitems(&bmpcSlabs.bmms_slbs);	
	psc_assert(nalloced <= BMPC_MAXSLBS);
	
	if (nalloced == BMPC_MAXSLBS) {
		rc = -ENOMEM;
		goto out;
	}

	if (nslbs > (BMPC_MAXSLBS - nalloced))
		nslbs = BMPC_MAXSLBS - nalloced;

	for (i=0; i < nslbs; i++) {
		slb = bmpc_slb_new();
		if (!slb) {
			/* Only complain if nothing was allocated.
			 */
			if (!i) 
				rc = -ENOMEM;
			goto out;
		}
		pll_add(&bmpcSlabs.bmms_slbs, slb);
	}
 out:
	psc_waitq_wakeall(&bmpcSlabs.bmms_waitq);
	ulockBmpcSlabs;

	return (rc);
}

void bmpc_free(void *);

__static void
bmpce_release_locked(struct bmap_pagecache_entry *bmpce, 
		     struct bmap_pagecache *bmpc)
{	
	LOCK_ENSURE(&bmpc->bmpc_lock);

	bmpce_freeprep(bmpce);
	psc_assert(SPLAY_REMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, bmpce));
	psc_assert(psclist_conjoint(&bmpce->bmpce_lentry));
	pll_remove(&bmpc->bmpc_lru, bmpce);
	/* Replace the bmpc memory.
	 */
	bmpc_free(bmpce->bmpce_base);
	psc_pool_return(bmpcePoolMgr, bmpce);
}

/**
 * bmpc_freeall_locked - called when a bmap is being released.  Iterate 
 *    across the tree freeing each bmpce.  Prior to being invoked, all 
 *    bmpce's must be idle (ie have zero refcnts) and be present on bmpc_lru.
 */
void
bmpc_freeall_locked(struct bmap_pagecache *bmpc)
{
	struct bmap_pagecache_entry *bmpce;

	LOCK_ENSURE(&bmpc->bmpc_lock);
	psc_assert(pll_empty(&bmpc->bmpc_pndg));

	while ((bmpce = SPLAY_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, 
				   bmpce_tentry))) {
		spinlock(&bmpce->bmpce_lock);
		DEBUG_BMPCE(PLL_TRACE, bmpce, "freeall");
		bmpce_freeprep(bmpce);
		freelock(&bmpce->bmpce_lock);
		bmpce_release_locked(bmpce);
	}
	psc_assert(SPLAY_EMPTY(&bmpc->bmpc_tree));
	psc_assert(pll_empty(&bmpc->bmpc_lru));
}

/**
 * bmpc_lru_tryfree - attempt to free 'nfree' blocks from the provided
 *    bmap_pagecache structure.  
 * @bmpc:   bmap_pagecache
 * @nfree:  number of blocks to free.
 * @minage: cache age specifier used to ensure fairness across many
 *          bmap_pagecache's.            
 */
__static int
bmpc_lru_tryfree(struct bmap_pagecache *bmpc, int nfree) 
{
	struct bmap_pagecache_entry *bmpce;
	struct timespec ts;
	int freed=0;

	clock_gettime(CLOCK_REALTIME, ts);
	timespecsub(&ts, &bmpcSlabs.bmms_minage, &ts);

	BMPC_LOCK(bmpc);
	PLL_FOREACH_SAFE(bmpce, &bmpc->bmpc_lru) {
		spinlock(&bmpce->bmpce_lock);

		psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));

		if (psc_atomic16_read(&bmpce->bmpce_rdref)) {
			DEBUG_BMPCE(PLL_TRACE, bmpce, "rd ref, skip");
			freelock(&bmpce->bmpce_lock);
			continue;
		}
		if (timespeccmp(ts, &bmpce->bmpce_laccess, <)) {
			DEBUG_BMPCE(PLL_TRACE, bmpce, "too recent, skip");
			freelock(&bmpce->bmpce_lock);
			continue;
		}
		
		DEBUG_BMPCE(PLL_NOTIFY, bmpce, "freeing");
		freelock(&bmpce->bmpce_lock);

		bmpce_release_locked(bmpce, bmpc);
		if (++freed == nfree)
			break;		
	}
	BMPC_ULOCK(bmpc);
	return (freed);
}

__static void
bmpc_reap_locked(void)
{
	struct bmap_pagecache *bmpc;
	struct timespec ts;
	int nfreed=0, waiters=atomic_read(&bmpcSlabs.bmms_waiters);

	LOCK_ENSURE(&bmpcSlabs.bmms_lock);

	if (bmpcSlabs.bmms_reap) {
		/* Wait and return, the thread holding the reap lock
		 *   should have freed a block for us.
		 */
		psc_waitq_wait(&bmpcSlabs.bmms_waitq, &bmpcSlabs.bmms_lock);
		return;
	} else
		/* This thread now holds the reap lock.
		 */
		bmpcSlabs.bmms_reap = 1;

	LIST_CACHE_LOCK(&bmpcLru);
	ulockBmpcSlabs;
	
	lc_sort(&bmpcLru, qsort, bmpc_lru_cmp);
	/* Should be sorted from oldest bmpc to newest.  Skip bmpc whose
	 *   bmpc_oldest time is too recent.
	 */
	clock_gettime(CLOCK_REALTIME, &ts);
	psclist_for_each_entry(bmpc, &bmpcLru.lc_listhd, bmpc_lentry) {
		timespecsub(&ts, &bmpcSlabs.bmms_minage, &ts);
		if (timespeccmp(&ts, &bmpc->bmpc_oldest, <)) {
			psc_trace("skip bmpc=%p, too recent", bmpc);
			continue;
		}
			
		nfreed += bmpc_lru_tryfree(bmpc, waiters);
		if (nfreed) { 
			atomic_sub(nfreed, &bmpcSlabs.bmms_waiters);
			if (atomic_read(&bmpcSlabs.bmms_waiters) < 0)
				atomic_set(&bmpcSlabs.bmms_waiters, 0);	
		}
		if (nfreed == waiters)
			break;
	} 

	if (nfreed) { 
		atomic_sub(nfreed, &bmpcSlabs.bmms_waiters);
		if (atomic_read(&bmpcSlabs.bmms_waiters) < 0)
			atomic_set(&bmpcSlabs.bmms_waiters, 0);	
	}

	if (waiters > nfreed) {
		int nslbs = (waiters - nfreed) / BMPC_SLB_NBLKS;
		
		/* Try to increase the number of slbs, if this fails
		 *   then decrease the LRU minimum age.
		 */
		if (bmpc_grow(nslbs ? nslbs : 1) == -ENOMEM)
			bmpc_decrease_minage();
	}		
}



void
bmpc_free(void *base)
{
	struct sl_buffer *slb;
	int found=0, freeslb=0;

	lockBmpcSlabs;	
	PLL_FOREACH(slb, &bmpcSlabs.bmms_slbs) {
		if (slb->slb_base == (base & ~BMPC_SLBBUFMASK)) {
			found = 1;
			break;
		}
	}
	psc_assert(found);
	psc_assert(!((base & BMPC_SLBBUFMASK) % BMPC_BLKSZ));

	spinlock(&slb->slb_lock);
	vbitmap_unset(&slb->slb_inuse, 
		      (size_t)((base & BMPC_SLBBUFMASK) / BMPC_BLKSZ));
	
	if ((vbitmap_nfree(&slb->slb_inuse) == BMPC_SLB_NBLKS) && 
	    pll_nitems(&bmpcSlabs.bmms_slbs) > BMPC_DEFSLBS) {
		/* The entire slb has been freed, let's remove it
		 *   remove it from the cache and free the memory.
		 */
		pll_remove(&bmpcSlabs.bmms_slbs, slb);
		slb->slb_base = NULL;
		freeslb = 1; 
	}
	freelock(&slb->slb_lock);

	psc_waitq_wakeall(&bmpcSlabs.bmms_waitq);
	ulockBmpcSlabs;

	if (freeslb) {
		bmpc_increase_minage_locked();
		bmpc_slb_free(slb);
	}
}

/**
 * bmpc_mem_alloc - return a pointer to a single block of cache memory.
 */
void *
bmpc_alloc(void) {
	struct sl_buffer *slb;
	void *base=NULL;
	size_t elem;
	int found=0;

 retry:
	lockBmpcSlabs;	
	PLL_FOREACH(slb, &bmpcSlabs.bmms_slbs) {
		spinlock(&slb->slb_lock);
		if (vbitmap_next(&slb->slb_inuse, &elem))
			found = 1;
		freelock(&slb->slb_lock);
		if (found)
			break;
	}

	if (!found) {
		/* bmpc_reap_locked() will drop the lock.
		 */
		bmpc_reap_locked();
		goto retry;

	} else {
		ulockBmpcSlabs;
		base = (char *)slb->slb_base + (elem * BMPC_BLKSZ);
	}

	psc_assert(base);
	return (base);
}


void
bmpc_global_init(void)
{
	bmpcSlabs.bmms_minage = BMPC_DEF_MINAGE;
	LOCK_INIT(&bmpcSlabs.bmms_lock);

	psc_waitq_init(&bmpcSlabs.bmms_waitq);

	pll_init(&bmpcSlabs.bmms_slbs, struct sl_buffer, 
		 slb_mgmt_lentry, &bmpcSlabs.bmms_lock);

	psc_poolmaster_init(&bmpcePoolMaster, struct bmap_pagecache_entry, 
			    bmpce_lentry, PPMF_AUTO, 2048, 2048, 16384, 
			    bmpce_init, NULL, NULL, "bmpce");

	bmpcePoolMgr = psc_poolmaster_getmgr(&bmpcePoolMaster);

	psc_assert(!bmpc_grow(BMPC_DEFSLBS));
}

