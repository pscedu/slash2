/* $Id$ */

#include <errno.h>
#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/assert.h"
#include "psc_util/cdefs.h"
#include "psc_util/lock.h"

#include "buffer.h"
#include "fidcache.h"
#include "offtree.h"

list_cache_t slBufsFree;
list_cache_t slBufsLru;
list_cache_t slBufsPin;

u32 slCacheBlkSz=32768;
u32 slCacheNblks=32;
u32 slbFreeDef=100;
u32 slbFreeMax=200;
u32 slbFreeInc=10;

sl_oftiov_inflight_callback *slInflightCb=NULL;

#define token_t list_cache_t

static struct sl_buffer_iovref *
sl_oftiov_locref_locked(struct offtree_iov *iov, struct sl_buffer *slb);

static void
sl_buffer_init(void *pri);

static void
sl_buffer_free_assertions(struct sl_buffer *b)
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
sl_buffer_lru_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_LRU);
	psc_assert(vbitmap_nfree(b->slb_inuse) < b->slb_nblks);
	psc_assert(!psclist_empty(&b->slb_iov_list));
	psc_assert(psclist_conjoint(&b->slb_fcm_lentry));
	psc_assert(atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
                   (!atomic_read(&b->slb_inflpndg)));
}

static void
sl_buffer_fresh_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
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
sl_buffer_pin_assertions(struct sl_buffer *b)
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
sl_buffer_inflight_assertions(struct sl_buffer *b)
{
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));   
        psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));  
	psc_assert(atomic_read(&b->slb_inflight));
}

static void
sl_buffer_put(struct sl_buffer *slb, list_cache_t *lc)
{
	int locked = reqlock(&slb->slb_lock);

	/* Must have been removed already
	 */
	psc_assert(slb->slb_lc_owner == NULL);

	DEBUG_SLB(PLL_TRACE, slb, "adding to %s", lc->lc_name);

	if (lc == &slBufsLru)
		sl_buffer_lru_assertions(slb);

	else if (lc == &slBufsPin)
		sl_buffer_pin_assertions(slb);

	else if (lc == &slBufsFree) {
		psc_assert(ATTR_TEST(slb->slb_flags, SLB_FREEING));
		ATTR_UNSET(slb->slb_flags, SLB_FREEING);
		ATTR_SET(slb->slb_flags, SLB_FREE);
		sl_buffer_free_assertions(slb);
		slb->slb_flags = SLB_FRESH;
		sl_buffer_fresh_assertions(slb);
	} else
		psc_fatalx("Invalid listcache address %p", lc);

	lc_queue(lc, &slb->slb_mgmt_lentry);
	slb->slb_lc_owner = lc;

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

	psc_trace("slb from %s", lc->lc_name);

	slb = (block ? lc_getwait(lc) : lc_getnb(lc));
	if (slb)
		INIT_PSCLIST_ENTRY(&slb->slb_fcm_lentry);
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

static int
sl_oftiov_free_check_locked(struct offtree_memb *m)
{
	struct offtree_iov *v = m->oft_norl.oft_iov;
	struct timespec ts;
	int    rc, tried=0;
	/* Verify node state
	 */
	psc_assert(ATTR_TEST(m->oft_flags, OFT_LEAF));
	/* Race condition, we're the freer - no one else
	 */
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_FREEING));
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_SPLITTING));
	psc_assert(!ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG));
	/* If writing, then the slb must be pinned
	 */
	//psc_assert(!ATTR_TEST(m->oft_flags, OFT_WRITEPNDG));
	psc_assert(!atomic_read(&m->oft_wrop_ref));
	/* Verify iov state
	 */
	psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
	psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_MAPPED));
	/* If faulting, then the slb must be pinned
	 */
	psc_assert(!ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING));

 restart:
	if (atomic_read(&m->oft_rdop_ref)) {
		/* Already pulled this guy from the lru, might as
		 *   try to free it.
		 */
		//psc_assert(ATTR_TEST(m->oft_flags, OFT_READPNDG));
		if (tried) {
			DEBUG_OFFTIOV(PLL_INFO, v, "iov inuse, retry fail");
			/*  Re-lock, caller expects to free it.
			 */
			spinlock(&m->oft_lock);
			return -1;
		}
		slb_set_readpndg_timer(&ts);

		DEBUG_OFFTIOV(PLL_INFO, v, "iov inuse, waiting..");

		rc = psc_waitq_timedwait(&m->oft_waitq, &m->oft_lock, &ts);

		if (rc == ETIMEDOUT) {
			DEBUG_OFFTIOV(PLL_INFO, v, "iov inuse, timedout");
			spinlock(&m->oft_lock);
			return -1;
		} else {
			DEBUG_OFFTIOV(PLL_INFO, v, "iov inuse, retrying");
			/*  Re-lock for attribute test.
			 */
			spinlock(&m->oft_lock);
			tried = 1;
			goto restart;
		}
	}
	return 0;
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
/**
 * sl_oftiov_bfree - free blocks from the slab buffer pointed to by the offtree_iov.
 * @iov: the offtree_iov using the slab's blocks
 * @r: tree root, used to get oftr_minsz
 * Notes: iov must be OFT_FREEING (but that is not enforced here)
 */
static void
sl_oftiov_bfree(struct offtree_iov *iov, struct sl_buffer_iovref *r)
{
	struct sl_buffer *slb = iov->oftiov_pri;
	size_t sbit, nblks;
	int    locked=0;

	SLB_IOV_VERIFY(iov);
	/* Which bits are to be released?
	 */
	sbit  = (iov->oftiov_base - slb->slb_base) / slb->slb_blksz;
	nblks = iov->oftiov_nblks;
	psc_assert(nblks);

	locked = reqlock(&slb->slb_lock);
	do {
		vbitmap_unset(slb->slb_inuse, sbit++);
	} while (nblks--);

	atomic_dec(&slb->slb_ref);
	/* Remove the oft iov reference.
	 */
	psclist_del(&r->slbir_lentry);
	PSCFREE(r);

	ureqlock(&slb->slb_lock, locked);
}

static int
sl_slab_reap(int nblks) {
	struct sl_buffer        *b;
	struct sl_buffer_iovref *r, *t;
	struct offtree_memb     *m;
	int    fblks=0, nslbs=0;

	do {
	nextlru:
		/* Grab one off the lru.
		 */
		b = sl_buffer_get(&slBufsLru, 0);
		if (!b) {
			b = sl_buffer_timedget(&slBufsLru);
			if (!b)
				/* Timedout, give up.
				 */
				goto out;
		}
		spinlock(&b->slb_lock);
		/* Ensure slb sanity
		 */
		psc_assert(b->slb_lc_owner == &slBufsLru);
		sl_buffer_lru_assertions(b);
		/* Safe to reclaim, notify fcmh_buffer_cache users.
		 */
		b->slb_lc_owner = NULL;
		b->slb_flags    = 0;
		ATTR_SET(b->slb_flags, SLB_FREEING);
		freelock(&b->slb_lock);
		/* Iteratively dereference the slb's iovectors.
		 */
		psclist_for_each_entry_safe(r, t, &b->slb_iov_list,
					    slbir_lentry) {
			m = (struct offtree_memb *)r->slbir_pri;
			/* Lock the parent first unless 'm' is the root
			 */
			if (!m->oft_parent) {
				oftm_root_verify(m);
			} else {
				spinlock(&m->oft_parent->oft_lock);
				oftm_node_verify(m->oft_parent);
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
			/* Free the slb blocks and remove the iov ref.
			 */
			sl_oftiov_bfree(m->oft_norl.oft_iov, r);
			/* Prep and remove the child tree node.
			 */
			ATTR_SET(m->oft_flags, OFT_FREEING);
			m->oft_norl.oft_iov = NULL;
			/* offtree_freeleaf_locked() releases both locks.
			 */
			offtree_freeleaf_locked(m);
			
		}
		/* Remove ourselves from the fidcache slab list
		 */
		lc_del(&b->slb_fcm_lentry, b->slb_lc_fcm);
		INIT_PSCLIST_ENTRY(&b->slb_fcm_lentry);
		psc_assert(vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
		/* Tally em up
		 */
		nslbs++;
		fblks += b->slb_nblks;
		/* Put it back in the pool
		 */
		sl_buffer_put(b, &slBufsFree);

	} while (fblks < nblks);

 out:
	psc_trace("Reaped (%d/%d) blocks in %d slabs", fblks, nblks, nslbs);
	return (fblks);
}

static int
sl_slab_alloc(int nblks, struct fidcache_memb_handle *f)
{
	struct sl_buffer *slb;
	int    rc, fblks=0, timedout=0;

	ENTRY;
 retry:
	do {
		slb = sl_buffer_get(&slBufsFree, 0);
		if (!slb) {
			if (lc_grow(&slBufsFree, slbFreeInc,
				    sl_buffer_init) > 0)
				goto retry;
			if (timedout)
				/* Already timedout once, give up
				 */
				goto out;
			else
				/* Try to get more
				 */
				goto reap;
		}
		DEBUG_SLB(PLL_TRACE, slb, "new slb");
		/* Sanity checks
		 */
		psc_assert(!slb->slb_lc_fcm);
		psc_assert(psclist_disjoint(&slb->slb_fcm_lentry));

		sl_buffer_fresh_assertions(slb);
		/* Assign buffer to the fcache member
		 */
		slb->slb_lc_fcm = &f->fcmh_buffer_cache;
		lc_stack(&f->fcmh_buffer_cache, &slb->slb_fcm_lentry);

	} while ((fblks += slb->slb_nblks) < nblks);
	/* Got all needed blocks.
	 */
	goto out;

 reap:
	rc = sl_slab_reap(nblks - fblks);
	if (rc <= 0)
		goto out;
	else {
		//if (rc < (nblks - fblks))
		/* Got some but not all, loop once more
		 *  trying to add them to our pool
		 */
		timedout = 1;
		goto retry;
	}
 out:
	RETURN (fblks);
}


static struct sl_buffer_iovref *
sl_oftiov_locref_locked(struct offtree_iov *iov, struct sl_buffer *slb)
{
	struct sl_buffer_iovref *ref=NULL;
	void                    *ebase=0;

	psc_trace("IOV %p slbir_base=%p", iov, iov->oftiov_base);
	/* Blksz's must match
	 */
	psc_assert(slb->slb_blksz == iov->oftiov_blksz);
	/* Slb must encompass iov range
	 */
	psc_assert((slb->slb_base <= iov->oftiov_base) &&
		   SLB_SLB2EBASE(slb) >= SLB_IOV2EBASE(iov, slb));

	psclist_for_each_entry(ref, &slb->slb_iov_list, slbir_lentry) {
		psc_assert(ebase < ref->slbir_base);
		ebase = SLB_REF2EBASE(ref, slb);
		psc_trace("ref=%p iovb=%p slbir_base=%p ebase=%p nblks=%zu",
			  ref, iov->oftiov_base, ref->slbir_base,
			  ebase, ref->slbir_nblks);

		if (iov->oftiov_base == ref->slbir_base) {
			psc_assert(!ATTR_TEST(ref->slbir_flags, SLBREF_REAP));
			/* At this point, remapping the ref to a new oftm
			 *  involves shrinking the original ref so the
			 *  proceeding must be true unless the last portion
			 *  of the oref was remapped.  Otherwise both nblks
			 *  must be equal.
			 */
			if (ATTR_TEST(iov->oftiov_flags, OFTIOV_REMAPPING))
				psc_assert(ref->slbir_nblks >= iov->oftiov_nblks);
			else
				psc_assert(ref->slbir_nblks == iov->oftiov_nblks);

			psc_assert(SLB_IOV2EBASE(iov, slb) <= ebase);
			break;
		}
	}
	psc_trace("ret with ref %p", ref);
	return ref;
}

void
sl_oftm_addref(struct offtree_memb *m)
{
	struct offtree_iov      *miov = m->oft_norl.oft_iov;
	struct sl_buffer        *slb  = miov->oftiov_pri;
	struct sl_buffer_iovref *oref=NULL;

	spinlock(&slb->slb_lock);

	oref = sl_oftiov_locref_locked(miov, slb);
	/* Old ref must be found.
	 */
	psc_assert(oref);
	psc_assert(oref->slbir_base >= slb->slb_base &&
		   oref->slbir_base <= (slb->slb_base +
					(slb->slb_nblks * slb->slb_blksz)));

	DEBUG_OFFTIOV(PLL_TRACE, miov, "sl_oftm_addref");
	//DUMP_SLB(PLL_TRACE, slb, "slb start (treenode %p)", m);

	if (!ATTR_TEST(oref->slbir_flags, SLBREF_MAPPED)) {
		struct sl_buffer_iovref *nref=NULL;

		/* Covers a full or partial mapping.
		 */
		DEBUG_OFT(PLL_TRACE, m, "unmapped slbref");
		/* Do not accept iov's which have already been mapped
		 */
		psc_assert(!ATTR_TEST(miov->oftiov_flags, OFTIOV_MAPPED));
		/* Look out for a short mapping, one that does not
		 *  consume the entire unmapped area.
		 */
		if (miov->oftiov_nblks < oref->slbir_nblks) {
			/* Got one, fork the slbref.
			 */
			nref = PSCALLOC(sizeof(*nref));
			oref->slbir_nblks -= miov->oftiov_nblks;

			DEBUG_OFT(PLL_TRACE, m, "short map slbref, oref=%p nref=%p",
				  oref, nref);

			nref->slbir_pri    = m;
			nref->slbir_base   = oref->slbir_base;
			nref->slbir_nblks  = miov->oftiov_nblks;
			ATTR_SET(nref->slbir_flags, SLBREF_MAPPED);

			oref->slbir_base  += OFT_IOVSZ(miov);

			psclist_xadd_tail(&nref->slbir_lentry,
					  &oref->slbir_lentry);

		} else {
			ATTR_SET(oref->slbir_flags, SLBREF_MAPPED);
			oref->slbir_pri = m;
			atomic_dec(&slb->slb_unmapd_ref);
		}
		atomic_inc(&slb->slb_ref);
		ATTR_SET(miov->oftiov_flags, OFTIOV_MAPPED);

	} else {
		int tblks;
		/* Handle remapping.  If SLBREF_MAPPED then OFTIOV_REMAPPING
		 *   must also be set.
		 */
		if (!ATTR_TEST(miov->oftiov_flags, OFTIOV_REMAPPING)) {
			DUMP_SLB(PLL_TRACE, slb, "bad slb ref");
			psc_fatalx("invalid ref %p", oref);
		}
		tblks  = oref->slbir_nblks;
		tblks -= miov->oftiov_nblks;
		psc_assert(tblks >= 0);

		if (!tblks) {
			/* The end of the ref's range has been reached.
			 *   No need to inc refcnt since no additional
			 *   refs are being added.
			 */
			oref->slbir_nblks = miov->oftiov_nblks;
			oref->slbir_pri   = m;

			psc_assert(ATTR_TEST(miov->oftiov_flags, OFTIOV_REMAP_END));
			ATTR_UNSET(miov->oftiov_flags, OFTIOV_REMAP_END);

			DEBUG_OFT(PLL_TRACE, m, "remap existing slbref, oref=%p",
				  oref);

		} else {
			/* Create a new reference.
			 */
			struct sl_buffer_iovref *nref;

			nref = PSCALLOC(sizeof(*nref));
			oref->slbir_nblks = tblks;

			DEBUG_OFT(PLL_TRACE, m, "remap slbref, oref=%p nref=%p",
				  oref, nref);

			nref->slbir_pri    = m;
			nref->slbir_base   = oref->slbir_base;
			nref->slbir_nblks  = miov->oftiov_nblks;
			ATTR_SET(nref->slbir_flags, SLBREF_MAPPED);

			oref->slbir_base  += OFT_IOVSZ(miov);

			psclist_xadd_tail(&nref->slbir_lentry,
					  &oref->slbir_lentry);
			atomic_inc(&slb->slb_ref);
			ATTR_SET(miov->oftiov_flags, OFTIOV_MAPPED);
		}
		ATTR_UNSET(miov->oftiov_flags, OFTIOV_REMAPPING);
	}
	/* Useful sanity checks which should always be true in
	 *  this context.
	 */
	DUMP_SLB(PLL_TRACE, slb, "slb done");
	psc_assert(atomic_read(&slb->slb_unmapd_ref) >= 0);
	psc_assert(atomic_read(&slb->slb_ref) <= slb->slb_nblks);
	freelock(&slb->slb_lock);
}
		
__static void
sl_buffer_pin_locked(struct sl_buffer *slb)
{
	if (ATTR_TEST(slb->slb_flags, SLB_PINNED)) {
		 psc_assert(slb->slb_lc_owner == &slBufsPin);
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
		psc_assert(atomic_read(&(slb)->slb_inflpndg) <		\
			   atomic_read(&(slb)->slb_inflight));		\
		if (atomic_dec_and_test(&(slb)->slb_inflpndg)) {	\
			lc_del(&(slb)->slb_mgmt_lentry, (slb)->slb_lc_owner); \
			slb_pinned_2_lru((slb));			\
			sl_buffer_put((slb), &slBufsLru);		\
		}							\
	}								\
		
/**
 * sl_oftiov_pin_cb - callback from offtree.c to instruct us to pin the slb contained within the passed in 'iov'.  sl_oftiov_pin_cb does some simple sanity checking and merely calls in sl_buffer_pin_locked().
 *
 */
void
sl_oftiov_pin_cb(struct offtree_iov *iov, int op)
{
	struct offtree_memb *m   = (struct offtree_memb *)iov->oftiov_memb;
	struct sl_buffer    *slb = (struct sl_buffer *)iov->oftiov_pri;
	/* Saneness.
	 */
	//LOCK_ENSURE(&m->oft_lock);
	psc_assert(m);
	psc_assert(m->oft_norl.oft_iov == iov);
	/* If there is no refcnt by the time we're called then no
	 *   guarantee can be made that this slb is currently being freed.
	 * For SL_BUFFER_UNPIN, these refs will be dec'd after this callback
	 *   is issued.
	 */
	psc_assert(atomic_read(&m->oft_rdop_ref) ||
		   atomic_read(&m->oft_wrop_ref));

	DEBUG_OFFTIOV(PLL_TRACE, iov, "op=%d", op);
	DEBUG_SLB(PLL_TRACE, slb, "op=%d", op);

	spinlock(&slb->slb_lock);
	if (op == SL_BUFFER_PIN)
		sl_buffer_pin_locked(slb);
	
	else if (op == SL_BUFFER_UNPIN) {
		sl_buffer_unpin_locked(slb);
	} else 
		psc_fatalx("Unknown op type %d", op);
	
	freelock(&slb->slb_lock);       	
}

/**
 * sl_oftiov_inflight_cb - called from the msl_read_cb() and msl_pagereq_finalize() to communicate inflight reference count changes to the slb layer.
 */
void
sl_oftiov_inflight_cb(struct offtree_iov *iov, int op)
{
	struct sl_buffer *s;

	s = (struct sl_buffer *)iov->oftiov_pri;

	DEBUG_SLB(PLL_TRACE, s, "inflight ref updating op=%d", op);
		
	if (op == SL_INFLIGHT_INC) {
		psc_assert(atomic_read(&s->slb_inflight) >= 0);

		atomic_inc(&s->slb_inflight);

		psc_assert(atomic_read(&s->slb_inflight) <= 
			   atomic_read(&s->slb_inflpndg));
		
	} else if (op == SL_INFLIGHT_DEC) {
		psc_assert(atomic_read(&s->slb_inflight) <=
                           atomic_read(&s->slb_inflpndg));
		psc_assert(atomic_read(&s->slb_inflight) >= 1);

		atomic_dec(&s->slb_inflight);
		
	} else {
		psc_fatalx("Invalid op=%d", op);
	}
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
sl_buffer_alloc_internal(struct sl_buffer *slb, size_t nblks, off_t soffa,
			 struct dynarray  *a, token_t *tok)
{
	int n=0,rc=0, tiovs=0;
	struct  offtree_iov *iov;
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
		  "sl_buffer_alloc_internal, a=%p nblks=%zu, soffa="LPX64,
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
		/* allocate another iov
		 */
		iov = PSCALLOC(sizeof(*iov));
		psc_assert(iov);
		/* associate the slb with the offtree_iov
		 */
		ref = PSCALLOC(sizeof(*ref));
		psc_assert(ref);

		DEBUG_SLB(PLL_INFO, slb, "iov=%p ref=%p blks=%zd, soffa=%zu",
			  iov, ref, blks, soffa);

		iov->oftiov_flags = 0;
		iov->oftiov_pri   = slb;
		iov->oftiov_blksz = slb->slb_blksz;
		/* Track the aligned, application offset.
		 */
		iov->oftiov_off  = soffa;
		soffa           += rc * slb->slb_blksz;

		ref->slbir_nblks = iov->oftiov_nblks = rc;
		/* 'n' contains the starting bit of the allocation.
		 */
		ref->slbir_base  = iov->oftiov_base  =
			slb->slb_base + (slb->slb_blksz * n);
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
		dynarray_add(a, iov);
		DEBUG_OFFTIOV(PLL_TRACE, iov,
			      "new iov(%d)", tiovs);
		tiovs++;
	}
 out:
	freelock(&slb->slb_lock);
	DEBUG_SLB(PLL_TRACE, slb, "leaving sl_buffer_alloc_internal blks=%zd",
		  blks);
	RETURN (blks);
}

/**
 * sl_buffer_alloc - allocate memory blocks from slb's already attached to our fid and/or the global slb allocator otherwise import new slab(s) from the global allocator.
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
	struct offtree_root *r  = pri;
	struct fidcache_memb_handle *f  = r->oftr_pri;
	list_cache_t        *lc = &f->fcmh_buffer_cache;
	//struct psclist_head  tmpl;
	struct sl_buffer    *slb;

	//INIT_PSCLIST_HEAD(&tmpl);

	psc_assert(nblks < (slCacheBlkSz/2));
	psc_assert(a);
	psc_assert(pri);
	psc_assert(nblks);

	do {
		/* Fill any previously allocated but incomplete buffers
		 *   by iterating over our private list of slb's.  Allocate
		 *   the remainaing blks by reserving a new slb and alloc'ing
		 *   from there.
		 */
		spinlock(&lc->lc_lock);
		psclist_for_each_entry(slb, &lc->lc_list, slb_fcm_lentry) {
			DEBUG_SLB(PLL_TRACE, slb,
				  "soffa "LPX64" trying with this slb", soffa);
			if (SLB_FULL(slb))
				continue;

			fblks += sl_buffer_alloc_internal(slb, (nblks-fblks),
							  nr_soffa, a, lc);

			nr_soffa = soffa + (fblks * slb->slb_blksz);

			if (fblks >= (ssize_t)nblks)
				break;
		}
		/* Free our fid's listcache lock.
		 */
		freelock(&lc->lc_lock);
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

static void
sl_buffer_init(void *pri)
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
	//sl_buffer_put(slb, &slBufsFree);
}

void
sl_buffer_cache_init(void)
{
	psc_assert(SLB_SIZE <= LNET_MTU);

	lc_reginit(&slBufsFree, struct sl_buffer,
		   slb_mgmt_lentry, "slabBufFree");
	lc_reginit(&slBufsLru,  struct sl_buffer,
		   slb_mgmt_lentry, "slabBufLru");
	lc_reginit(&slBufsPin,  struct sl_buffer,
		   slb_mgmt_lentry, "slabBufPin");

	slBufsFree.lc_max = slbFreeMax;

	lc_grow(&slBufsFree, slbFreeDef, sl_buffer_init);
	slInflightCb = sl_oftiov_inflight_cb;
}
