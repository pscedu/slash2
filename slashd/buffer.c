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
sl_buffer_free_assertions(struct sl_buffer *b)
{
	/* The following asertions must be true: */
	psc_assert(b->slb_flags = SLB_FREE);
	/* any cache nodes pointing to us? */
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	/* do we point to any cache nodes? */
	psc_assert(psclist_empty(&b->slb_iov_list));
	/* all of our blocks in hand? */
	psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);      
	/* prove our disassociation from the fidcm */
	psc_assert(PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
}

static void
sl_buffer_lru_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_LRU);
	psc_assert(vbitmap_nfree(&b->slb_inuse) < b->slb_nblks);
	psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(!PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
	psc_assert(atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
}

static void
sl_buffer_fresh_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
	psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
	psc_assert(!b->slb_lc_owner); /* Not on any cache mgmt lists */
	psc_assert(psclist_empty(&b->slb_iov_list));
	psc_assert(b->slb_base);
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
}

static void
sl_buffer_pin_assertions(struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_PIN));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FRESH));	
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
	//psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));
	//psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	//psc_assert(vbitmap_nfree(&b->slb_inuse) < b->slb_nblks);
	//psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(!PSCLIST_INIT_CHECK(&b->slb_fcm_lentry));
	//psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(b->slb_base);
	psc_assert(atomic_read(&b->slb_ref));
}

static void
sl_buffer_put(struct sl_buffer *b, list_cache_t *lc)
{
	int locked = reqlock(&b->slb_lock);
	
	/* Must have been removed already */
	psc_assert(b->slb_lc_owner == NULL);

	switch(*lc) {
	case (&slBufsFree):		
		psc_assert(ATTR_TEST(b->slb_flags, SLB_FREEING));	
		ATTR_UNSET(b->slb_flags, SLB_FREEING));
		ATTR_SET(b->slb_flags, SLB_FREE);
		sl_buffer_free_assertions(b);
		break;

	case (&slBufsLru):
		sl_buffer_lru_assertions(b);
		break;

	case (&slBufsPin):
		sl_buffer_pin_assertions(b);
		break;

	default:
		psc_fatalx("Invalid listcache address %p", lc);
	}
	ureqlock(&b->slb_lock, locked);	
	
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

static int
sl_oftiov_free_check_locked(struct offtree_memb *m) 
{
	struct offtree_iov *v = m->norl.oft_iov;
	
	/* Verify node state */
	psc_assert(ATTR_TEST(m->oft_flags, OFT_LEAF));	
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_FREEING));
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_SPLITTING));
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG));
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_WRITEPNDG));

	/* Verify iov state */
	psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
	psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_MAPPED));
	psc_assert(!ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING));
	
	if (ATTR_TEST(m->oft_flags, OFT_REQPNDG)) {
		psc_assert(ATTR_TEST(m->oft_flags, OFT_READPNDG));
		//psc_waitq_wait(&m->oft_waitq, &m->oft_lock);
		// XXX for now never sleep	
		DEBUG_OFFTIOV(PLL_INFO, v, "iov inuse, skipping");
		return -1;
	} 

	return 0;
}

#define SLB_IOV_VERIFY(v) {						\
		struct sl_buffer *__s = (v)->oftiov_pri;		\
		/* Blksz's must match					\
		 */							\
		psc_assert(__s->slb_blksz == (v)->oftiov_blksz);	\
		/* Slb must encompass iov range				\
		 */							\
		psc_assert((__s->slb_base <= (v)->oftiov_base) &&	\
			   SLB_SLB2EBASE(__s) >= SLB_IOV2EBASE((v), __s)); \
		psc_assert(!(((v)->oftiov_base - __s->slb_base)		\
			     % __s->slb_blksz));			\
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
	struct sl_buffer *slb = iov->oftiov_pri;
	size_t sbit, nblks;
	int    locked=0;

	SLB_IOV_VERIFY(iov);
	/* Which bits? 
	 */	
	sbit  = (iov->oftiov_base - b->slb_base) / b->slb_blksz;
	nblks = iov->oftiov_nblks; 
	psc_assert(nblks);

	locked = reqlock(&b->slb_lock);
	do {
		vbitmap_unset(b->slb_inuse, sbit++);
	} while (nblks--);

	atomic_dec(&slb->slb_ref);
	ureqlock(&b->slb_lock, locked);
}

static void
sl_slab_reap(int nblks) {
	struct sl_buffer        *b;
	struct sl_buffer_iovref *r, *t;
	struct offtree_memb     *m;

	do {
	nextlru:
		/* grab one off the lru */
		b = sl_buffer_get(&slBufsLRU, 0);
		if (!b) {
			psc_warnx("Blocking get for LRU sl_buffer");
			b = sl_buffer_get(&slBufsLRU, 1);
			psc_assert(b);
		}
		spinlock(&b->slb_lock);	

		/* yes, ensure slb sanity */
		psc_assert(b->slb_lc_owner == &slBufsLRU);
		sl_buffer_lru_assertions(b);
		b->slb_lc_owner = NULL;

		/* it's safe to reclaim, notify fcmh_buffer_cache users */
		b->slb_flags = 0;
		ATTR_SET(b->slb_flags, SLB_FREEING);
		freelock(&b->slb_lock);

		/* iteratively dereference the slb's iovectors */
		psclist_for_each_entry(r, t, &b->slb_iov_list, 
				       slbir_lentry) {
			m = r->slbir_pri;
			/* Lock the parent first unless 'm' is the root */
			if (!m->oft_parent)
				psc_assert(ATTR_TEST(m->oft_flags, OFT_ROOT));
			else {
				spinlock(&m->oft_parent->oft_lock);
				psc_assert(ATTR_TEST(m->oft_parent->oft_flags, 
						     OFT_NODE));
			}
			spinlock(&m->oft_lock);
			
			if (sl_oftiov_free_check_locked(m) < 0) {
				freelock(&m->oft_lock);
				freelock(&m->oft_parent->oft_lock);
				/* Leave the slb alone for now
				 */
				ATTR_UNSET(b->slb_flags, SLB_FREEING);
				goto nextlru;
			}
			ATTR_SET(m->oft_flags, OFT_FREEING);		
			sl_oftiov_bfree(m->norl.oft_iov, r);

			psclist_del(&r->slbir_lentry);
			m->norl.oft_iov = NULL;
			offtree_freeleaf_locked(m);
		}
		/* Remove ourselves from the fidcache slab list   
		 */
		lc_del(&b->slb_fcm_lentry, b->slb_lc_fcm);
		INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);
		psc_assert(vbitmap_nfree(&b->slb_inuse) == b->slb_nblks);
		nblks -= b->slb_nblks;
		/* Put it back in the pool
		 */
		sl_buffer_put(b, slBufsFree);
	} while (nblks > 0);
}

static struct sl_buffer *
sl_slab_alloc(int nblks, token_t *tok) 
{	
	do {
		
		
		sl_slab_reap(nblks, tok);
	} while (nblks); 
}


static struct sl_buffer_iovref *
sl_oftiov_locref_locked(struct offtree_iov *iov, struct sl_buffer *slb)
{
	struct sl_buffer_iovref *ref=NULL;
	void                    *ebase=0;

	/* blksz's must match */
	psc_assert(slb->slb_blksz == iov->oftiov_blksz);	
	/* Slb must encompass iov range */
	psc_assert((slb->slb_base <= iov->oftiov_base) &&
		   SLB_SLB2EBASE(slb) >= SLB_IOV2EBASE(iov, slb));

	psclist_for_each_entry_safe(ref, &slb->slb_iov_list, slbir_lentry) {
		psc_assert(ebase < ref->slbir_base);
		ebase = SLB_REF2EBASE(ref, slb->slb_blksz);

		if ((iov->oftiov_base >= ref->slbir_base) && 
		    (iov->oftiov_base <= ebase)) {
			psc_assert(!ATTR_TEST(ref->slbir_flags, SLBREF_REAP));
			psc_assert(SLB_IOV2EBASE(iov, slb) <= ebase);	
			break;
		}
	}
	return ref;
}

void
sl_oftm_addref(struct offtree_memb *m)
{
	struct offtree_iov      *miov = m->norl.oft_iov;
	struct sl_buffer        *slb  = iov->oftiov_pri;
	struct sl_buffer_iovref *oref=NULL, *nref=NULL;
	
	/* Iov's must come from the same slab */
	spinlock(&slb->slb_lock);

	oref = sl_oftiov_locref_locked(miov, slb);
	/* Old ref must be found */
	psc_assert(oref);
	
	if (!ATTR_TEST(oref->slbir_flags, SLBREF_MAPPED)) {
		/* Covers a full or partial mapping.
		 */
		ATTR_SET(oref->slbir_flags, SLBREF_MAPPED);
		atomic_dec(&slb->slb_unmapd_ref);
		oref->slbir_pri = m;
		goto out;
	}	
	/* At least one ref will be added, prep the new ref*/
	nref = PSC_ALLOC(sizeof(*nref));
	nref->slbir_base  = miov->oftiov_base;
	nref->slbir_nblks = miov->oftiov_nblks;
	nref->slbir_pri   = m;
	ATTR_SET(nref->slbir_flags, SLBREF_MAPPED);
	
	if (oref->slbir_base == nref->slbir_base) {
		/* Push oref so that it does not overlap with the 
		 *  new iov.
		 */
		psc_assert(nref->slbir_nblks < oref->slbir_nblks);
		oref->slbir_nblks -= nref->slbir_nblks;
		psc_assert(oref->slbir_nblks > 0);
		oref->slbir_base  += (nref->slbir_nblks * slb->slb_blksz);
		psclist_add(&nref->slbir_lentry, &oref->slbir_lentry);

	} else {
		if (SLB_REF2EBASE(nref, slb->slb_blksz) == 
		    SLB_REF2EBASE(oref, slb->slb_blksz)) {
			/* Ends match up, just shrink oref */
			oref->slbir_nblks -= nref->slbir_nblks;
			psc_assert(oref->slbir_nblks > 0);
			psclist_add_tail(&nref->slbir_lentry, 
					 &oref->slbir_lentry);
		
		} else if (SLB_REF2EBASE(oref, slb->slb_blksz) >
			   SLB_REF2EBASE(nref, slb->slb_blksz)) {
			/* New ref is resides in the middle of 
			 *  old one.
			 */
			struct sl_buffer_iovref *tref = PSC_ALLOC(sizeof(*nref));
			
			psc_assert(!((nref->slbir_base - oref->slbir_base) %
				     slb->slb_blksz));
			
			tref->slbir_base  = SLB_REF2EBASE(nref, slb->slb_blksz) + 1;
			tref->slbir_nblks = oref->slbir_nblks;
			tref->slbir_pri   = m;
			ATTR_SET(tref->slbir_flags, SLBREF_MAPPED);
			
			oref->slbir_nblks = (nref->slbir_base - oref->slbir_base) /
				slb->slb_blksz;
			
			tref->slbir_nblks -= oref->slbir_nblks + nref->slbir_nblks;

			psclist_add_tail(&nref->slbir_lentry, 
					 &oref->slbir_lentry);

			psclist_add_tail(&tref->slbir_lentry, 
					 &nref->slbir_lentry);

			/* Need a total of 2 increments */
			atomic_inc(&ref->slb_ref);
		} else 
			psc_fatalx("nref base %p is < oref base %p", 
				   nref->slbir_base, oref->slbir_base);		
	}
	atomic_inc(&slb->slb_ref);	
 out:
	psc_assert(atomic_read(&slb->slb_unmapd_ref) >= 0);
	psc_assert(atomic_read(&slb->slb_ref) > 0);	
	psc_assert(atomic_read(&slb->slb_ref) <= slb->slb_nblks);
	freelock(&b->slb_lock);
}


/**
 * sl_buffer_register - associate a newly allocated slb to a fidcache.
 * 
 */
static void
sl_buffer_fcache_reg(struct sl_buffer *b, fcache_mhandle_t *f)
{
	sl_buffer_fresh_assertions(b);
	psc_assert(!b->slb_lc_fcm);   /* No other fid's own us       */

	b->slb_lc_fcm = &f->fcmh_buffer_cache;
	lc_stack(&f->fcmh_buffer_cache, &b->slb_fcm_lentry);	
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
	ssize_t blks=0;

	locked = reqlock(&b->slb_lock);
	/* this would mean that someone else is processing us 
	 *   or granted the slb to another fcmh (in which case
	 *   (tok != b->slb_lc_fcm)) - that would mean that the 
	 *   slab had been freed and reassigned between now and 
	 *   us removing it from the list.
	 */
	if (ATTR_TEST(b->slb_flags, SLB_FREEING) || 
	    (tok != b->slb_lc_fcm))
		goto out;
		
	for (blks=0; (blks <= nblks) && !SLB_FULL(b);) {
		psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
		if (ATTR_TEST(b->slb_flags, SLB_FRESH)) {
			sl_buffer_fresh_assertions(b);

			ATTR_UNSET(b->slb_flags, SLB_FRESH);
                        ATTR_SET(b->slb_flags, SLB_PINNED);
                        sl_buffer_put(b, &slBufsPIN);

		} else if (ATTR_TEST(b->slb_flags, SLB_LRU)) {
			/* Move from LRU to PINNED.
			 * Note: the LRU and FREE managers MUST use
			 *  the listcache api for removing entries
			 *  otherwise there will be race conditions
			 */
			sl_buffer_lru_assertions(b);

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
		*iovs[*niovs]->oftiov_flags = 0;
		*iovs[*niovs]->oftiov_pri   = b;
		*iovs[*niovs]->oftiov_nblks = rc;
		*iovs[*niovs]->oftiov_base  = b->slb_base + (b->slb_blksz * n);
		/* just to make sure */
		sl_buffer_lru_assertions(b);
		//psc_assert(b->slb_lc_owner == &slBufsPIN);
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

	do {
		/* Fill any previously allocated but incomplete buffers */
		spinlock(&lc->lc_lock);
		psclist_for_each_entry(b, &lc->lc_list, slb_fcm_lentry) {
			if (SLB_FULL(b))
				continue;			
			rblks -= sl_buffer_alloc_internal(b, rblks, iovs, 
							  niovs, (token *)lc);
			if (!rblks)
				break;
		}
		/* free our fid's listcache lock */
		freelock(&lc->lc_lock);

		if (rblks) {	
			/* request a new slab from the main allocator */
			if (!(b = sl_slab_alloc(rblks)))
				goto enomem;
			
			sl_buffer_fcache_reg(b, f);
		}
	} while (!rblks);

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
