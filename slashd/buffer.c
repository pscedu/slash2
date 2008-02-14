#include <errno.h>

#include "psc_util/lock.h"
#include "psc_util/cdefs.h"
#include "psc_util/assert.h"
#include "psc_ds/listcache.h"

#include "buffer.h"
#include "fidcache.h"
#include "offtree.h"

list_cache_t slBufsFree;
list_cache_t slBufsLru;
list_cache_t slBufsPin;

u32 slCacheBlkSz=16384;
u32 slCacheNblks=16;

#define token_t list_cache_t

static void
sl_buffer_lru_assertions(struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_LRU));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_PIN));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	psc_assert(vbitmap_nfree(&b->slb_inuse) < b->slb_nblks);
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(!PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
	psc_assert(atomic_read(&b->slb_ref));
}

static void
sl_buffer_put(struct sl_buffer *b, list_cache_t *lc)
{
	int locked = reqlock(&b->slb_lock);

	switch(*lc) {
	case (slBufsFree):		
		/* The following asertions must be true: */
		psc_assert(ATTR_TEST(b->slb_flags, SLB_FREEING));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_PIN));
		/* any cache nodes pointing to us? */
		psc_assert(!atomic_read(&b->slb_ref));
		/* do we point to any cache nodes? */
		psc_assert(psclist_empty(&b->slb_iov_list));
		/* all of our blocks in hand? */
		psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);      
		/* prove our disassociation from the fidcm */
		psc_assert(PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
		ATTR_SET(b->slb_flags, SLB_FREE);
		break;

	case (slBufsLru):
		sl_buffer_lru_assertions(b);
		break;

	case (slBufsPin):
		psc_assert(ATTR_TEST(b->slb_flags, SLB_PIN));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
		psc_assert(vbitmap_nfree(&b->slb_inuse) < b->slb_nblks);
		psc_assert(psclist_empty(&b->slb_iov_list));
		psc_assert(!PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
		psc_assert(atomic_read(&b->slb_ref));
		break;

	default:
		psc_fatalx("Invalid listcache address %p", lc);
	}
	ureqlock(&b->slb_lock, locked);	
	
	if (ATTR_TEST(b->slb_flags, SLB_INIT))
		psc_assert(b->slb_lc_owner == NULL);
	else
		lc_del(&b->slb_mgmt_lentry, b->slb_lc_owner);

	lc_queue(lc, &b->slb_mgmt_lentry);
}

/**
 * sl_buffer_get - pull a buffer from the listcache
 * @lc: the list cache in question
 * @block: wait (or not)
 */
static struct sl_buffer *
sl_buffer_get(list_cache_t *lc, int block)
{
	return ((block ? lc_getwait(lc) : lc_getnb(lc)));
}


static void
sl_slab_reap(int nblks, token_t *tok) {
	struct sl_buffer    *b;
	struct offtree_memb *oftm;

	do {
		/* grab one off the lru */
		b = sl_buffer_get(&slBufsLRU, 0);
		if (!b) {
			psc_warnx("Blocking get for LRU sl_buffer");
			b = sl_buffer_get(&slBufsLRU, 1);
			psc_assert(b);
		}

		spinlock(&b->slb_lock);	
		/* is this slab still reapable? */
		if (!ATTR_TEST(b->slb_flags, SLB_LRU) || 
		    (tok != b->slb_lc_fcm)) {
			freelock(&b->slb_lock);
			continue;
		}
		/* yes, ensure slb sanity */
		sl_buffer_lru_assertions(b);
		/* it's safe to reclaim, notify fcmh_buffer_cache users */
		ATTR_SET(b->slb_flags, SLB_FREEING);
		freelock(&b->slb_lock);
		/* remove ourselves from the fidcache slab list   */
		lc_del(&b->slb_fcm_lentry, b->slb_lc_fcm);
		/* ok, we should have it to ourselves now */
		INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);
		/* iteratively dereference the slb's iovectors */
		psclist_for_each_entry(oftm, &b->slb_iov_list, oftiov_lentry) {
			
			sl_oftiov_bfree(oftm->norl.oft_iov);
		}

		
		//psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);
                //psc_assert(PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
		
		nblks -= b->slb_nblks;
		/* put it back in the pool */
		sl_buffer_put(b, slBufsFree);
	} while (nblks > 0);
}

static struct sl_buffer *
sl_slab_alloc(int nblks, token_t *tok) {
	
	do {
		
		
		sl_slab_reap(nblks, tok);
	} while (nblks); 
}
/**
 * sl_oftiov_bfree - free blocks from the slab buffer pointed to by the offtree_iov. 
 * @iov: the offtree_iov using the slab's blocks
 * @r: tree root, used to get oftr_minsz
 * Notes: iov must be OFT_FREEING (but that is not enforced here)
 */
static void
sl_oftiov_bfree(struct offtree_iov *iov)
{
	struct sl_buffer *b = iov->oftiov_pri;
	size_t sbit, nblks;
	int locked=0;

	/* sanity */
	psc_assert(!((iov->oftiov_base / b->slb_base) % b->slb_blksz));

	/* which bits? */	
	sbit  = (iov->oftiov_base - b->slb_base) / b->slb_blksz;
	nblks = iov->oftiov_nblks; 

	psc_assert(nblks);

	locked = reqlock(&b->slb_lock);
	do {
		vbitmap_unset(b->slb_inuse, sbit++);
	} while (nblks--);

	/* is this buffer 'freeable'? */       
	if (vbitmap_nfree(b->slb_inuse) == b->slb_nblks) {
		/* yes, notify others to skip this slb */
		ATTR_SET(b->slb_flags, SLB_FREEING);
		sl_buffer_put(b, &slBufsFree);		
	}
	ureqlock(&b->slb_lock, locked);
}

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
sl_buffer_alloc_internal(struct sl_buffer *b, size_t nblks,
			 struct offtree_iov **iovs, int *niovs, token_t *tok)
{
	int n=0,rc=0,locked=0;
	ssize_t blks;

	locked = reqlock(&b->slb_lock);
	/* this would mean that someone else is processing us 
	 *   or granted the slb to another fcmh
	 */
	if (ATTR_TEST(b->slb_flags, SLB_FREEING) || 
	    (tok != b->slb_lc_fcm))
		goto out;
		
	for (blks=0; (blks <= nblks) && !SLB_FULL(b);) {
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
		if (ATTR_TEST(b->slb_flags, SLB_LRU)) {
			/* note that the LRU and FREE managers MUST use
			 *  the listcache api for removing entries
			 *  otherwise there will be race conditions
			 */
			lc_del(&b->slb_mgmt_lentry, b->slb_lc_owner);	
			ATTR_UNSET(b->slb_flags, SLB_LRU);
			ATTR_SET(b->slb_flags, SLB_PINNED);
			sl_buffer_put(b, &slBufsPIN);
		}
		/* grab a set of blocks */
		rc = vbitmap_getncontig(b->slb_inuse, &n);
		/* if !SLB_FULL() */
		if (!rc)
			break;
		/* deduct returned blocks from remaining */
		blks += rc;
		/* allocate another iov */
		*niovs++;
		*iovs = realloc(*iovs, sizeof(struct offtree_iov *)*(*niovs));
		psc_assert(*iovs);		
		/* associate the slb with the offtree_iov */
		*iovs[*niovs]->oftiov_pri   = b;
		*iovs[*niovs]->oftiov_nblks = rc;
		*iovs[*niovs]->oftiov_base  = b->slb_base + (r->oftr_minsz*n);
		/* just to make sure */
		psc_assert(b->slb_lc_owner == &slBufsPIN);
	}
 out:
	ureqlock(&b->slb_lock, locked);	
	return (blks);
}

/**
 * sl_buffer_alloc - allocate memory blocks from slb's already attached to our fid and/or the global slb allocator.
 * @nblks: number of blocks to fetch
 * @iovs:  iov array (allocated by us) to hold each contiguous region.
 * @niovs: tell the caller how many iovs we allocated.
 * @pri:   tree root, which gives the pointer to our fcache handle
 */
int 
sl_buffer_alloc(size_t nblks, struct offtree_iov **iovs, int *niovs, void *pri)
{
	int     n=0, rc;
	ssize_t rblks = nblks;
	struct offtree_root *r  = pri;
	fcache_mhandle_t    *f  = r->oftr_pri;
	list_cache_t        *lc = &f->fcmh_buffer_cache;
	struct psclist_head  tmpl;
	struct sl_buffer    *b;

	INIT_PSCLIST_HEAD(&tmpl);

	psc_assert(iovs == NULL);
	psc_assert(niovs && pri);
	psc_assert(nblks);

	/* Fill any previously allocated but incomplete buffers */
	spinlock(&lc->lc_lock);
	psclist_for_each_entry(b, &lc->lc_list, slb_fcm_lentry) {
		if (SLB_FULL(b))
			continue;
		rblks -= sl_buffer_alloc_internal(b, rblks, iovs, niovs, 
						  &f->fcmh_buffer_cache);
		if (!rblks)
			break;
	}
	/* free our fid's listcache lock */
	freelock(&lc->lc_lock);

	if (!rblks)
		/* got 'em */
		return(0);
	
	/* Did not get the entire set of blocks from our previous buffers */
	while (rblks) {
		/* request a new slab from the main allocator */
		if (!(b = sl_slab_alloc(rblks)))
			goto enomem;
		
		psc_assert(!(b->slb_len % r->oftr_minsz));
		rblks -= sl_buffer_alloc_internal(b, rblks, iovs, niovs, 
						  &f->fcmh_buffer_cache);
		if (!rblks) 
			break;
	}
	return (0);

 enomem:
	for (n=0; n < *niovs; n++)
		sl_oftiov_bfree(*iovs[n], r);

	return -ENOMEM;
}

static void
sl_buffer_init(struct sl_buffer *b)
{
	b->slb_inuse = vbitmap_new(slCacheNblks);
	b->slb_blksz = slCacheBlkSz;
	b->slb_nblks = slCacheNblks;
	b->slb_base  = PSCALLOC(slCacheNblks * slCacheBlkSz);
	atomic_set(&b->slb_ref, 0);
	LOCK_INIT (&b->slb_lock);
	ATTR_SET  (b->slb_flags, SLB_FREEING);
	INIT_PSCLIST_HEAD(&b->slb_iov_list);
	INIT_PSCLIST_ENTRY(&b->slb_mgmt_lentry);
	INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);	
}

void
sl_buffer_cache_init(void)
{	
	lc_reginit(&slBufsFree, struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufFree");
	lc_reginit(&slBufsLru,  struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufLru");
	lc_reginit(&slBufsPin,  struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufPin");

	slBufsFree.lc_max = SLB_FREE_MAX;

	lc_grow(&slBufsFree, SLB_FREE_DEF, slb_buffer, sl_buffer_init);
}
