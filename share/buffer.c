/* $Id$ */

#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "pfl/cdefs.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "buffer.h"
#include "fidcache.h"

struct psc_poolmaster	 slBufsPoolMaster;
struct psc_poolmgr	*slBufsPool;
list_cache_t slBufsLru;
list_cache_t slBufsPin;

int slCacheBlkSz=32768;
int slCacheNblks=32;
u32 slbFreeDef=100;
u32 slbFreeMax=200;
u32 slbFreeInc=10;

sl_iov_try_memrls   slMemRlsTrylock=NULL;
sl_iov_memrls_ulock slMemRlsUlock=NULL;

typedef struct psc_lockedlist token_t;

static void
sl_buffer_free_assertions(const struct sl_buffer *b)
{
	/* The following asertions must be true: */
	psc_assert(b->slb_flags == SLB_FREE);
	/* any cache nodes pointing to us? */
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	/* do we point to any cache nodes? */
	psc_assert(psclist_empty(&b->slb_iov_list));
	/* all of our blocks in hand? */
	psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	/* prove our disassociation from the fidcm */
	psc_assert(psclist_disjoint(&b->slb_fcm_lentry));
}

static void
sl_buffer_lru_2_free_assertions(const struct sl_buffer *b) {
	psc_assert(b->slb_flags == (SLB_LRU|SLB_FREEING));
	psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
        psc_assert((!atomic_read(&b->slb_inflight)) &&
                   (!atomic_read(&b->slb_inflpndg)));

}

static void
sl_buffer_lru_assertions(const struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_LRU);
	psc_assert(vbitmap_nfree(b->slb_inuse) < b->slb_nblks);
	psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(psclist_conjoint(&b->slb_fcm_lentry));
	psc_assert(atomic_read(&b->slb_ref));
	//	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
                   (!atomic_read(&b->slb_inflpndg)));
}

void
sl_buffer_fresh_assertions(const struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
	vbitmap_printbin1(b->slb_inuse);
	psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(!b->slb_lc_owner); /* Not on any cache mgmt lists */
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(b->slb_base);
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
                   (!atomic_read(&b->slb_inflpndg)));
}

static void
sl_buffer_pin_assertions(const struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_PINNED));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FRESH));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	psc_assert(!psclist_empty(&b->slb_iov_list));
	/* Test this before pinning.. */
	//psc_assert(psclist_disjoint(&b->slb_mgmt_lentry));
	psc_assert(b->slb_base);
	psc_assert((atomic_read(&b->slb_ref) > 0) ||
		   (atomic_read(&b->slb_unmapd_ref) > 0));
	psc_assert((atomic_read(&b->slb_inflight) > 0) ||
		   (atomic_read(&b->slb_inflpndg) > 0));
	psc_assert(atomic_read(&b->slb_inflpndg) >=
		   (atomic_read(&b->slb_inflight)));
}

static void
sl_buffer_pin_2_lru_assertions(const struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_PINNED));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FRESH));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	psc_assert(!psclist_empty(&b->slb_iov_list));
	/* Test this before pinning.. */
	//psc_assert(psclist_disjoint(&b->slb_mgmt_lentry));
	psc_assert(b->slb_base);
	psc_assert((atomic_read(&b->slb_ref) > 0) ||
		   (atomic_read(&b->slb_unmapd_ref) > 0));
	psc_assert(!(atomic_read(&b->slb_inflight)) &&
		   !(atomic_read(&b->slb_inflpndg)));
	psc_assert(vbitmap_nfree(b->slb_inuse) < b->slb_nblks);
	psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(psclist_conjoint(&b->slb_fcm_lentry));
	psc_assert(atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
                   (!atomic_read(&b->slb_inflpndg)));
}

#if 0
static void
sl_buffer_inflight_assertions(struct sl_buffer *b)
{
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));
        psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
	psc_assert(atomic_read(&b->slb_inflight));
}
#endif

static void
sl_buffer_put(struct sl_buffer *slb, list_cache_t *lc)
{
	int locked = reqlock(&slb->slb_lock);


	DEBUG_SLB(PLL_INFO, slb, "adding to %s", lc->lc_name);

	/* Must have been removed already
	 */
	psc_assert(psclist_disjoint(&slb->slb_mgmt_lentry));

	if (lc == &slBufsPool->ppm_lc) {
		psc_assert(ATTR_TEST(slb->slb_flags, SLB_FREEING));
		ATTR_UNSET(slb->slb_flags, SLB_FREEING);
		ATTR_SET(slb->slb_flags, SLB_FREE);
		sl_buffer_free_assertions(slb);
		slb->slb_flags = SLB_FRESH;
		sl_buffer_fresh_assertions(slb);

		psc_pool_return(slBufsPool, slb);

	} else {
		if (lc == &slBufsLru) {
			slb->slb_flags = SLB_LRU;

			if (slb->slb_lc_owner == &slBufsPin) 
				sl_buffer_pin_2_lru_assertions(slb);

			else 
				sl_buffer_lru_assertions(slb);

		} else if (lc == &slBufsPin)
			sl_buffer_pin_assertions(slb);
		else
			psc_fatalx("Invalid listcache address %p", lc);
		lc_queue(lc, &slb->slb_mgmt_lentry);
		slb->slb_lc_owner = lc;
	}
	ureqlock(&slb->slb_lock, locked);
}

/**
 * sl_buffer_get - pull a buffer from the listcache
 * @lc: the list cache in question
 * @block: wait (or not)
 */
static struct sl_buffer *
sl_buffer_get(list_cache_t *lc, int block)
{
	struct sl_buffer *slb;

	psc_assert(lc != &slBufsPool->ppm_lc);

	psc_trace("slb from %s", lc->lc_name);

	slb = (block ? lc_getwait(lc) : lc_getnb(lc));
	return (slb);
}

static struct sl_buffer *
sl_buffer_timedget(list_cache_t *lc)
{
	struct timespec ts;

	psc_warnx("Blocking get for LRU sl_buffer");
	slb_set_alloctimer(&ts);

	//struct sl_buffer *slb = lc_gettimed(lc, abstime);
	//return ((struct sl_buffer *)lc_gettimed(lc, abstime));
	return (lc_gettimed(lc, &ts));
	//return (slb);
}


#define SLB_IOV_VERIFY(v) {						\
		struct sl_buffer *SSs = (v)->oftiov_pri;		\
		int IIi = 0;						\
		psc_assert(SSs->slb_blksz == (v)->oftiov_blksz);	\
		psc_assert((SSs->slb_base <= (v)->oftiov_base) &&	\
			   SLB_SLB2EBASE(SSs) >= SLB_IOV2EBASE((v), SSs)); \
		IIi = ((v)->oftiov_base - SSs->slb_base) % SSs->slb_blksz; \
		psc_assert(!IIi);					\
}

#if 0
static void
sl_slab_tryfree(struct sl_buffer *b) 
{
	int free=0;

	spinlock(&b->slb_lock);
	DEBUG_SLB(PLL_INFO, b, "check");
	if (b->slb_flags & SLB_FREEING)
		free = 1;
	freelock(&b->slb_lock);

	if (!free)
		return;

	psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	sl_buffer_lru_2_free_assertions(b);
	/* Get rid of the LRU bit.
	 */
	b->slb_flags = SLB_FREEING;
	DEBUG_SLB(PLL_INFO, b, "freeing slab via non-cb context");

	lc_remove(&slBufsLru, b);
	pll_remove(b->slb_lc_fcm, b);
	b->slb_lc_owner = NULL;
	b->slb_lc_fcm = NULL;
	INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);
	sl_buffer_put(b, &slBufsPool->ppm_lc);
}
#endif

static int
sl_slab_reap(__unusedx struct psc_poolmgr *pool) {
	struct sl_buffer        *b;
	struct sl_buffer_iovref *r, *t;
	int nslbs=0;
	void *pri_bmap_tmp;

	abort();
	/* Grab one off the lru.  
	 *    XXX it may be better to lock the entire list and 
	 *    iterate over each item to prevent having to restore
	 *    the unreapable items.
	 */
	b = sl_buffer_get(&slBufsLru, 0);
	if (!b) {
		b = sl_buffer_timedget(&slBufsLru);
		if (!b)
			/* Timedout, give up.
			 */
			return (0);
	}
	spinlock(&b->slb_lock);
	/* Ensure slb sanity
	 */
	psc_assert(b->slb_lc_owner == &slBufsLru);
	sl_buffer_lru_assertions(b);
	/* Safe to reclaim, notify fcmh_buffer_cache users.
	 */
	b->slb_lc_owner = NULL;
	//b->slb_flags	= 0;
	ATTR_SET(b->slb_flags, SLB_FREEING);
	freelock(&b->slb_lock);
	/* Iteratively dereference the slb's iovectors.
	 */
	psclist_for_each_entry_safe(r, t, &b->slb_iov_list,
				    slbir_lentry) {

		pri_bmap_tmp = r->slbir_pri_bmap;
	}
	/* Remove the LRU bit now that the slb is to be freed.
	 */
	b->slb_flags &= ~SLB_LRU;
	/* Remove ourselves from the fidcache slab list
	 */	
	pll_remove(b->slb_lc_fcm, b);
	b->slb_lc_fcm = NULL;
	INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);
	psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	/* Tally em up
	 */
	nslbs++;
	/* Put it back in the pool
	 */
	sl_buffer_put(b, &slBufsPool->ppm_lc);

	psc_trace("Reaped %d slabs", nslbs);
	return (nslbs);
}


static int
sl_slab_alloc(int nblks, struct fidc_membh *f)
{
	struct sl_buffer *slb;
	int    fblks=0;

	ENTRY;
	do {
		slb = psc_pool_get(slBufsPool);

		psc_assert(slb);
		DEBUG_SLB(PLL_INFO, slb, "new slb");
		/* Sanity checks
		 */
		psc_assert(!slb->slb_lc_fcm);
		psc_assert(psclist_disjoint(&slb->slb_fcm_lentry));

		sl_buffer_fresh_assertions(slb);
		/* Assign buffer to the fcache member
		 */
		slb->slb_lc_fcm = &f->fcmh_fcoo->fcoo_buffer_cache;
		pll_addstack(slb->slb_lc_fcm, slb);

	} while ((fblks += slb->slb_nblks) < nblks);

	RETURN (fblks);
}


__static void
sl_buffer_pin_locked(struct sl_buffer *slb)
{
	if (ATTR_TEST(slb->slb_flags, SLB_PINNED)) {
		 psc_assert(slb->slb_lc_owner == &slBufsPin);
		 atomic_inc(&slb->slb_inflpndg);
		 return (sl_buffer_pin_assertions(slb));
	}

	if (ATTR_TEST(slb->slb_flags, SLB_FRESH)) {
		slb_fresh_2_pinned(slb);

	} else if (ATTR_TEST(slb->slb_flags, SLB_LRU)) {
		/* Move from LRU to PINNED.
		 * Note: the LRU and FREE managers MUST use
		 *  the listcache api for removing entries
		 *  otherwise there will be race conditions
		 */
		psc_assert(slb->slb_lc_owner == &slBufsLru);
		lc_del(&slb->slb_mgmt_lentry, slb->slb_lc_owner);
		slb_lru_2_pinned(slb);
	} else {
		DEBUG_SLB(PLL_FATAL, slb, "invalid slb");
		psc_fatalx("invalid slb %p", slb);
	}
	atomic_inc(&slb->slb_inflpndg);
	sl_buffer_put(slb, &slBufsPin);
}

/**
 * sl_buffer_unpin_locked - decref and perhaps unpin an slb.
 * Notes:  the slb_inflight ref corresponding to this op must have already been dec'd, meaning that slb_inflpndg must be at least 1 greater than slb_inflight.
 */
#define sl_buffer_unpin_locked(slb)					\
	{								\
		psc_assert((slb)->slb_lc_owner == &slBufsPin);		\
		psc_assert(atomic_read(&(slb)->slb_inflpndg) >		\
			   atomic_read(&(slb)->slb_inflight));		\
		if (atomic_dec_and_test(&(slb)->slb_inflpndg)) {	\
			lc_del(&(slb)->slb_mgmt_lentry, (slb)->slb_lc_owner); \
			slb_pinned_2_lru((slb));			\
			sl_buffer_put((slb), &slBufsLru);		\
		}							\
	}								\

/**
 * sl_buffer_alloc_internal - allocate blocks from the given slab buffer 'b'.
 * @b: slab to alloc from
 * @nblks: preffered number of blocks
 * @iovs: array of iov pointers which index the allocations
 * @niovs: the number of allocations
 * @tok: ensure that the slab still belongs to the requesting fid
 * Returns:  the total number of blocks returned
 */
static size_t
sl_buffer_alloc_internal(struct sl_buffer *slb, size_t nblks, off_t soffa,
			 struct dynarray  *a, token_t *tok)
{
	int n=0,rc=0, tiovs=0;
	ssize_t blks=0;
	struct  sl_buffer_iovref *ref=NULL;

	ENTRY;

	spinlock(&slb->slb_lock);
	/* this would mean that someone else is processing us
	 *   or granted the slb to another fcmh (in which case
	 *   (tok != b->slb_lc_fcm)) - that would mean that the
	 *   slab had been freed and reassigned between now and
	 *   us removing it from the list.
	 */
	DEBUG_SLB(PLL_TRACE, slb,
		  "sl_buffer_alloc_internal, a=%p nblks=%zu, soffa=%"PRIx64,
		  a, nblks, soffa);

	if (ATTR_TEST(slb->slb_flags, SLB_FREEING) || (tok != slb->slb_lc_fcm))
		goto out;

	for (blks=0; (blks < (ssize_t)nblks) && !SLB_FULL(slb);) {
		/* grab a set of blocks, 'n' tells us the starting block
		 */
		n = nblks - blks;
		rc = vbitmap_getncontig(slb->slb_inuse, &n);
		if (!rc)
			break;
		/* deduct returned blocks from remaining
		 */
		blks += rc;
		soffa += rc * slb->slb_blksz;

		ref->slbir_nblks = rc;
		/* 'n' contains the starting bit of the allocation.
		 */
		ref->slbir_base = slb->slb_base + (slb->slb_blksz * n);
		ref->slbir_flags = 0;

		atomic_inc(&slb->slb_unmapd_ref);
		/* Insert the new, unmapped reference into the
		 *  appropriate slot within the list determined
		 *  by the base address.
		 */
		if (psclist_empty(&slb->slb_iov_list)) {
			psc_assert(!atomic_read(&slb->slb_ref));
			psclist_xadd(&ref->slbir_lentry,
				     &slb->slb_iov_list);

		} else {
			struct  sl_buffer_iovref *iref, *tref;
			void   *ebase=NULL;
			int     i=0;

			psclist_for_each_entry_safe(iref, tref,
						    &slb->slb_iov_list, slbir_lentry) {
				/* Probably need a more thorough check here.
				 *  These checks ensure that the bases increase and
				 *  that the new base does not already exist.
				 */
				psc_assert(ebase < iref->slbir_base);
				psc_assert(iref->slbir_base != ref->slbir_base);
				ebase = SLB_REF2EBASE(iref, slb);

				psc_trace("ebase=%p ref->slbir_base=%p",
					  ebase, ref->slbir_base);

				if (ref->slbir_base < ebase) {
					psclist_xadd(&ref->slbir_lentry,
						     &iref->slbir_lentry);
					i=1;
					break;
				}
				//if (ref->slbir_base > ebase)
				//		psclist_xadd(&ref->slbir_lentry,
				//		     &iref->slbir_lentry);
			}
			if (!i) {
				/* Was not added, append to the end of the list.
				 */
				psclist_xadd_tail(&ref->slbir_lentry,
						  &slb->slb_iov_list);
			}
		}
		sl_buffer_pin_locked(slb);
		tiovs++;
	}
 out:
	freelock(&slb->slb_lock);
	DEBUG_SLB(PLL_TRACE, slb, "leaving sl_buffer_alloc_internal blks=%zd",
		  blks);
	RETURN (blks);
}

/**
 * sl_buffer_alloc - allocate memory blocks from slb's already attached 
 *    to our fid and/or the global slb allocator otherwise import new 
 *    slab(s) from the global allocator.
 * @nblks: number of blocks to fetch
 * @iovs:  iov array (allocated by us) to hold each contiguous region.
 * @niovs: tell the caller how many iovs we allocated.
 * @pri:   tree root, which gives the pointer to our fcache handle
 */
int
sl_buffer_alloc(size_t nblks, off_t soffa, struct dynarray *a, void *pri)
{
	ssize_t fblks=0;
	off_t   nr_soffa=soffa;
	struct fidc_membh *f  = pri; /* wrong.. */
	struct psc_lockedlist *ll = &f->fcmh_fcoo->fcoo_buffer_cache;
	struct sl_buffer *slb;

	psc_assert(nblks < (size_t)(slCacheBlkSz/2));
	psc_assert(a);
	psc_assert(pri);
	psc_assert(nblks);

	do {
		/* Fill any previously allocated but incomplete buffers
		 *   by iterating over our private list of slb's.  Allocate
		 *   the remainaing blks by reserving a new slb and alloc'ing
		 *   from there.
		 */
		PLL_LOCK(ll);
		PLL_FOREACH(slb, ll) {
			DEBUG_SLB(PLL_TRACE, slb, "soffa %"PRIx64" trying "
				  "with this slb", soffa);
			if (SLB_FULL(slb))
				continue;

			fblks += sl_buffer_alloc_internal(slb, (nblks-fblks),
							  nr_soffa, a, ll);

			nr_soffa = soffa + (fblks * slb->slb_blksz);

			if (fblks >= (ssize_t)nblks)
				break;
		}
		PLL_ULOCK(ll);
		/* Are more blocks needed?
		 */
		if (fblks < (ssize_t)nblks) {
			/* Request a new slab from the main allocator.
			 *  If this fails then we're forced to punt (or block).
			 */
			if (!sl_slab_alloc((nblks-fblks), f))
				goto enomem;
		}
	} while (fblks < (ssize_t)nblks);

	return (fblks);

 enomem:
	psc_warnx("failed to allocate %zu blocks from fid %p",
		  nblks, f);

	return -ENOMEM;
}

int
sl_buffer_init(__unusedx struct psc_poolmgr *m, void *pri)
{
	struct sl_buffer *slb = pri;

	slb->slb_inuse = vbitmap_new(slCacheNblks);
	slb->slb_blksz = slCacheBlkSz;
	slb->slb_nblks = slCacheNblks;
	slb->slb_base  = PSCALLOC(slCacheNblks * slCacheBlkSz);
	atomic_set(&slb->slb_ref, 0);
	atomic_set(&slb->slb_unmapd_ref, 0);
	atomic_set(&slb->slb_inflight, 0);
	LOCK_INIT (&slb->slb_lock);
	//ATTR_SET  (slb->slb_flags, SLB_FREEING);
	slb->slb_flags = SLB_FRESH;
	INIT_PSCLIST_HEAD(&slb->slb_iov_list);
	//INIT_PSCLIST_ENTRY(&slb->slb_mgmt_lentry);
	INIT_PSCLIST_ENTRY(&slb->slb_fcm_lentry);

	DEBUG_SLB(PLL_TRACE, slb, "new slb");
	//sl_buffer_put(slb, &slBufsPool->ppm_lc);
	return (0);
}

void
sl_buffer_destroy(void *pri)
{
	struct sl_buffer *slb = pri;

	PSCFREE(slb->slb_base);
	vbitmap_free(slb->slb_inuse);
}

void
sl_buffer_cache_init(void)
{
	psc_assert(SLB_SIZE <= LNET_MTU);

	psc_poolmaster_init(&slBufsPoolMaster, struct sl_buffer, slb_mgmt_lentry,
			    PPMF_AUTO, slbFreeDef, 0, slbFreeMax,
			    sl_buffer_init, sl_buffer_destroy, sl_slab_reap, "slab", NULL);
	slBufsPool = psc_poolmaster_getmgr(&slBufsPoolMaster);

	lc_reginit(&slBufsLru,  struct sl_buffer,
		   slb_mgmt_lentry, "slabBufLru");
	lc_reginit(&slBufsPin,  struct sl_buffer,
		   slb_mgmt_lentry, "slabBufPin");
}
