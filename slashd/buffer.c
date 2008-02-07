#include "buffer.h"
#include "fidcache.h"
#include "offtree.h"

list_cache_t slBufsFree;
list_cache_t slBufsLru;
list_cache_t slBufsPin;

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

	lc_del(&b->slb_mgmt_lentry, b->slb_lc_owner);
	lc_queue(&b->slb_mgmt_lentry, lc);
}

/**
 * sl_oftiov_bfree - free blocks from the slab buffer pointed to by the offtree_iov. 
 * @iov: the offtree_iov using the slab's blocks
 * @r: tree root, used to get oftr_minsz
 * Notes: iov must be OFT_FREEING (but that is not enforced here)
 */
static void
sl_oftiov_bfree(struct offtree_iov *iov, struct offtree_root *r)
{
	struct sl_buffer *b = iov->oftiov_pri;
	size_t sbit, ebit;
	int locked=0;

	/* sanity */
	psc_assert(!((iov->oftiov_base / b->slb_base) % r->oftr_minsz));
	psc_assert(!(iov->oftiov_len % r->oftr_minsz));

	/* which bits? */	
	sbit = (iov->oftiov_base - b->slb_base) / r->oftr_minsz;
	ebit = iov->oftiov_len / r->oftr_minsz;
	psc_assert((ebit-sbit) > 0);

	locked = reqlock(&b->slb_lock);
	do {
		vbitmap_unset(b->slb_inuse, sbit);
	} while (ebit - (++sbit));

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
 * Returns:  the total number of blocks returned
 */
static size_t 
sl_buffer_alloc_internal(struct sl_buffer *b, size_t nblks,
			 struct offtree_iov **iovs, int *niovs) 
{
	int n=0,rc=0,locked=0;
	ssize_t blks;

	locked = reqlock(&b->slb_lock);
	for (blks=0; (blks <= nblks) && !SLB_FULL(b);) {
		/* this would mean that someone else is processing us */
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
		
		if (ATTR_TEST(b->slb_flags, SLB_LRU) ||
		    ATTR_TEST(b->slb_flags, SLB_FREE)) {
			/* note that the LRU and FREE managers MUST use
			 *  the listcache api for removing entries
			 *  otherwise there will be race conditions
			 */
			lc_del(&b->slb_mgmt_lentry, b->slb_lc_owner);	
			ATTR_UNSET(b->slb_flags, SLB_LRU);
			ATTR_UNSET(b->slb_flags, SLB_FREE);		
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
		*iovs[*niovs]->oftiov_pri  = b;
		*iovs[*niovs]->oftiov_len  = b->slb_len  + (r->oftr_minsz*rc);
		*iovs[*niovs]->oftiov_base = b->slb_base + (r->oftr_minsz*n);
		/* just to make sure */
		psc_assert(b->slb_lc_owner == &slBufsPIN);
	}
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
		rblks -= sl_buffer_alloc_internal(b, rblks, iovs, niovs);
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
		else {
			psc_assert(!(b->slb_len % r->oftr_minsz));
			rblks -= sl_buffer_alloc_internal(b, rblks, 
							  iovs, niovs);
			if (!rblks) 
				break;
		}
	}
	return (0);

 enomem:
	for (n=0; n < *niovs; n++)
		sl_oftiov_bfree(*iovs[n], r);

	return -ENOMEM;
}

void
sl_buffer_init(void)
{
	
	lc_reginit(&slBufsFree, struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufFree");
	lc_reginit(&slBufsLru,  struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufLru");
	lc_reginit(&slBufsPin,  struct sl_buffer, 
		   slb_mgmt_lentry, "slabBufPin");	
}
