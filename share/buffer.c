/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include "pfl/cdefs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"

#include "buffer.h"
#include "cache_params.h"
#include "fidcache.h"

struct psc_poolmaster	 slBufsPoolMaster;
struct psc_poolmgr	*slBufsPool;

sl_iov_try_memrls_t	 slMemRlsTrylock;
sl_iov_memrls_ulock_t	 slMemRlsUlock;

#if 0
__static void
sl_buffer_free_assertions(struct sl_buffer *b)
{
	/* The following asertions must be true: */
	psc_assert(b->slb_flags == SLB_FREE);
	/* any cache nodes pointing to us? */
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	/* do we point to any cache nodes? */
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	/* all of our blocks in hand? */
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	/* prove our disassociation from the fidcm */
	psc_assert(psclist_disjoint(&b->slb_fcmh_lentry));
}

__static void
sl_buffer_lru_2_free_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == (SLB_LRU|SLB_FREEING));
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
		   (!atomic_read(&b->slb_inflpndg)));

}
#endif

__static void
sl_buffer_lru_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_LRU);
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) < b->slb_nblks);
	psc_assert(!psc_listhd_empty(&b->slb_iov_list));
	psc_assert(psclist_conjoint(&b->slb_fcmh_lentry,
	    psc_lentry_hd(&b->slb_fcmh_lentry)));
	psc_assert(atomic_read(&b->slb_ref));
	//	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
		   (!atomic_read(&b->slb_inflpndg)));
}

void
sl_buffer_fresh_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
	psc_vbitmap_printbin1(b->slb_inuse);
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	psc_assert(!b->slb_lc_owner); /* Not on any cache mgmt lists */
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	psc_assert(b->slb_base);
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
		   (!atomic_read(&b->slb_inflpndg)));
}

void
sl_buffer_clear(struct sl_buffer *b, size_t size)
{
	memset(b->slb_base, 0, size);
}

__static void
sl_buffer_pin_assertions(struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_PINNED));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FRESH));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	psc_assert(!psc_listhd_empty(&b->slb_iov_list));
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

__static void
sl_buffer_pin_2_lru_assertions(struct sl_buffer *b)
{
	psc_assert(ATTR_TEST(b->slb_flags, SLB_PINNED));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FRESH));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_LRU));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREEING));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_FREE));
	psc_assert(!psc_listhd_empty(&b->slb_iov_list));
	/* Test this before pinning.. */
	//psc_assert(psclist_disjoint(&b->slb_mgmt_lentry));
	psc_assert(b->slb_base);
	psc_assert((atomic_read(&b->slb_ref) > 0) ||
		   (atomic_read(&b->slb_unmapd_ref) > 0));
	psc_assert(!(atomic_read(&b->slb_inflight)) &&
		   !(atomic_read(&b->slb_inflpndg)));
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) < b->slb_nblks);
	psc_assert(!psc_listhd_empty(&b->slb_iov_list));
	psc_assert(psclist_conjoint(&b->slb_fcmh_lentry,
	    psc_lentry_hd(&b->slb_fcmh_lentry)));
	psc_assert(atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
		   (!atomic_read(&b->slb_inflpndg)));
}

#if 0
__static void
sl_buffer_inflight_assertions(struct sl_buffer *b)
{
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_DIRTY));
	psc_assert(!ATTR_TEST(b->slb_flags, SLB_INFLIGHT));
	psc_assert(atomic_read(&b->slb_inflight));
}

__static void
sl_buffer_put(struct sl_buffer *slb, struct psc_listcache *lc)
{
	int locked = reqlock(&slb->slb_lock);


	DEBUG_SLB(PLL_INFO, slb, "adding to %s", lc->plc_name);

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
		lc_addqueue(lc, slb);
		slb->slb_lc_owner = lc;
	}
	ureqlock(&slb->slb_lock, locked);
}
#endif

/**
 * sl_buffer_get - pull a buffer from the listcache
 * @lc: the list cache in question
 * @block: wait (or not)
 */
__static struct sl_buffer *
sl_buffer_get(struct psc_listcache *lc, int block)
{
	struct sl_buffer *slb;

	psc_assert(lc != &slBufsPool->ppm_lc);

	psclog_trace("slb from %s", lc->plc_name);

	slb = (block ? lc_getwait(lc) : lc_getnb(lc));
	return (slb);
}

__static struct sl_buffer *
sl_buffer_timedget(struct psc_listcache *lc)
{
	struct timespec ts;

	psc_warnx("Blocking get for LRU sl_buffer");
	slb_set_alloctimer(&ts);

	//struct sl_buffer *slb = lc_gettimed(lc, abstime);
	//return (slb);
	//return ((struct sl_buffer *)lc_gettimed(lc, abstime));
	return (lc_gettimed(lc, &ts));
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
__static void
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

	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	sl_buffer_lru_2_free_assertions(b);
	/* Get rid of the LRU bit.
	 */
	b->slb_flags = SLB_FREEING;
	DEBUG_SLB(PLL_INFO, b, "freeing slab via non-cb context");

	lc_remove(&slBufsLru, b);
	pll_remove(b->slb_lc_fcmh, b);
	b->slb_lc_owner = NULL;
	b->slb_lc_fcmh = NULL;
	INIT_PSC_LISTENTRY(&b->slb_fcmh_lentry);
	sl_buffer_put(b, &slBufsPool->ppm_lc);
}

__static int
sl_slab_reap(__unusedx struct psc_poolmgr *pool)
{
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
	pll_remove(b->slb_lc_fcmh, b);
	b->slb_lc_fcmh = NULL;
	INIT_PSC_LISTENTRY(&b->slb_fcmh_lentry);
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	/* Tally em up
	 */
	nslbs++;
	/* Put it back in the pool
	 */
	sl_buffer_put(b, &slBufsPool->ppm_lc);

	psclog_trace("Reaped %d slabs", nslbs);
	return (nslbs);
}

__static void
sl_buffer_pin_locked(struct sl_buffer *slb)
{
	if (ATTR_TEST(slb->slb_flags, SLB_PINNED)) {
		 psc_assert(slb->slb_lc_owner == &slBufsPin);
		 atomic_inc(&slb->slb_inflpndg);
		 sl_buffer_pin_assertions(slb);
		 return;
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
		lc_remove(slb->slb_lc_owner, slb);
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
 * Notes:  the slb_inflight ref corresponding to this op must have
 *	already been dec'd, meaning that slb_inflpndg must be at least
 *	+1 greater than slb_inflight.
 */
#define sl_buffer_unpin_locked(slb)					\
	{								\
		psc_assert((slb)->slb_lc_owner == &slBufsPin);		\
		psc_assert(atomic_read(&(slb)->slb_inflpndg) >		\
			   atomic_read(&(slb)->slb_inflight));		\
		if (atomic_dec_and_test(&(slb)->slb_inflpndg)) {	\
			lc_del(&(slb)->slb_mgmt_lentry,			\
			    (slb)->slb_lc_owner);			\
			slb_pinned_2_lru((slb));			\
			sl_buffer_put((slb), &slBufsLru);		\
		}							\
	}

#endif

int
sl_buffer_init(__unusedx struct psc_poolmgr *m, void *pri)
{
	struct sl_buffer *slb = pri;

	slb->slb_inuse = psc_vbitmap_new(SLB_NBLK);
	slb->slb_blksz = SLB_BLKSZ;
	slb->slb_nblks = SLB_NBLK;
	slb->slb_base  = PSCALLOC(SLB_NBLK * SLB_BLKSZ);
	atomic_set(&slb->slb_ref, 0);
	atomic_set(&slb->slb_unmapd_ref, 0);
	atomic_set(&slb->slb_inflight, 0);
	INIT_SPINLOCK(&slb->slb_lock);
	//ATTR_SET  (slb->slb_flags, SLB_FREEING);
	slb->slb_flags = SLB_FRESH;
	INIT_LISTHEAD(&slb->slb_iov_list);
	INIT_LISTENTRY(&slb->slb_mgmt_lentry);
	INIT_LISTENTRY(&slb->slb_fcmh_lentry);

	DEBUG_SLB(PLL_TRACE, slb, "new slb");
	return (0);
}

void
sl_buffer_destroy(void *pri)
{
	struct sl_buffer *slb = pri;

//	psc_assert(psc_listhd_empty(&slb->slb_iov_list));
//	psc_assert(psclist_disjoint(&slb->slb_fcmh_lentry));

	PSCFREE(slb->slb_base);
	psc_vbitmap_free(slb->slb_inuse);
}

void
sl_buffer_cache_init(void)
{
	int slvr_buffer_reap(struct psc_poolmgr *);

	psc_assert(SLB_SIZE <= LNET_MTU);

	psc_poolmaster_init(&slBufsPoolMaster, struct sl_buffer,
	    slb_mgmt_lentry, PPMF_AUTO, SLB_NDEF, SLB_MIN, SLB_MAX,
	    sl_buffer_init, sl_buffer_destroy, slvr_buffer_reap, "slab", NULL);
	slBufsPool = psc_poolmaster_getmgr(&slBufsPoolMaster);

//	lc_reginit(&slBufsLru,  struct sl_buffer, slb_mgmt_lentry, "slabBufLru");
//	lc_reginit(&slBufsPin,  struct sl_buffer, slb_mgmt_lentry, "slabBufPin");
}
