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
		psc_assert(!atomic_read(&b->slb_ref));
		psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);
		psc_assert(psclist_empty(&b->slb_iov_list));
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
		/* grab a set of blocks */
		rc = vbitmap_getncontig(b->slb_inuse, &n);
		/* if !SLB_FULL() */
		psc_assert(rc);
		/* deduct returned blocks from remaining */
		blks += rc;
		/* allocate another iov */
		*niovs++;
		*iovs = realloc(*iovs, sizeof(struct offtree_iov *)*(*niovs));
		psc_assert(*iovs);		
		
		*iovs[*niovs]->oftiov_pri  = b;
		*iovs[*niovs]->oftiov_len  = b->slb_len  + (r->oftr_minsz*rc);
		*iovs[*niovs]->oftiov_base = b->slb_base + (r->oftr_minsz*n);
	}
	ureqlock(&b->slb_lock, locked);	
	return (blks);
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
	if (!vbitmap_nfree(b->slb_inuse)) {
		/* yes, notify others to skip this slb */
		ATTR_SET(b->slb_flags, SLB_FREEING);
		sl_buffer_put(b, &slBufsFree);		
	}

	ureqlock(&b->slb_lock, locked);
}

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
		rblks -= sl_buffer_alloc_internal(b, rblks, iovs, niovs);
		if (!rblks)
			break;
	}
	/* free our fid's listcache lock */
	freelock(&lc->lc_lock);

	if (!rblks)
		return(0);

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
