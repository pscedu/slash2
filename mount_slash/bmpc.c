/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <time.h>

#include "pfl/fsmod.h"
#include "psc_ds/lockedlist.h"
#include "psc_util/atomic.h"
#include "psc_util/pool.h"

#include "bmpc.h"
#include "mount_slash.h"

struct psc_poolmaster	 bmpcePoolMaster;
struct psc_poolmgr	*bmpcePoolMgr;
struct psc_poolmaster    bwcPoolMaster;
struct psc_poolmgr      *bwcPoolMgr;

struct psc_listcache	 bmpcLru;

__static SPLAY_GENERATE(bmap_pagecachetree, bmap_pagecache_entry,
			bmpce_tentry, bmpce_cmp);

/* bwc_init - Initialize write coalescer pool entry.
 */
int
bwc_init(__unusedx struct psc_poolmgr *poolmgr, void *p)
{
	struct bmpc_write_coalescer *bwc = p;

	memset(bwc, 0, sizeof(*bwc));
	INIT_PSC_LISTENTRY(&bwc->bwc_lentry);
	pll_init(&bwc->bwc_pll, struct bmpc_ioreq, biorq_bwc_lentry, NULL);

	return (0);
}

void
bwc_release(struct bmpc_write_coalescer *bwc)
{
	psc_assert(pll_empty(&bwc->bwc_pll));
	bwc_init(bwcPoolMgr, bwc);
	psc_pool_return(bwcPoolMgr, bwc);
}

/**
 * bmpce_init - Initialize a bmap page cache entry.
 */
int
bmpce_init(__unusedx struct psc_poolmgr *poolmgr, void *p)
{
	struct bmap_pagecache_entry *bmpce = p;
	void *base;
	
	base = bmpce->bmpce_base;
	memset(bmpce, 0, sizeof(*bmpce));
	INIT_PSC_LISTENTRY(&bmpce->bmpce_lentry);
	INIT_PSC_LISTENTRY(&bmpce->bmpce_ralentry);
	INIT_SPINLOCK(&bmpce->bmpce_lock);
	pll_init(&bmpce->bmpce_pndgaios, struct msl_fsrqinfo,
	    mfsrq_lentry, &bmpce->bmpce_lock);
	bmpce->bmpce_flags = BMPCE_NEW;
	bmpce->bmpce_base = base;
	if (!bmpce->bmpce_base)
		bmpce->bmpce_base  = psc_alloc(BMPC_BUFSZ, PAF_PAGEALIGN);
	return (0);
}

void
bmpce_destroy(void *p)
{
	struct bmap_pagecache_entry *bmpce = p;

	psc_free(bmpce->bmpce_base, PAF_PAGEALIGN);
}

struct bmap_pagecache_entry *
bmpce_lookup_locked(struct bmap_pagecache *bmpc, struct bmpc_ioreq *biorq,
		    uint32_t off, struct psc_waitq *wq)
{
	struct bmap_pagecache_entry bmpce_search, *bmpce = NULL,
		*bmpce_new = NULL;

	LOCK_ENSURE(&bmpc->bmpc_lock);

	bmpce_search.bmpce_off = off;

	while (!bmpce) {
		bmpce = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
		    &bmpce_search);
		if (bmpce)
			break;

		else if (bmpce_new == NULL) {
			BMPC_ULOCK(bmpc);
			bmpce_new = psc_pool_get(bmpcePoolMgr);
			BMPC_LOCK(bmpc);
			continue;
		} else {
			bmpce = bmpce_new;
			bmpce_new = NULL;
			bmpce->bmpce_off = bmpce_search.bmpce_off;
			bmpce_useprep(bmpce, biorq, wq);

			SPLAY_INSERT(bmap_pagecachetree,
			     &bmpc->bmpc_tree, bmpce);
		}
	}
	if (bmpce_new)
		psc_pool_return(bmpcePoolMgr, bmpce_new);

	return (bmpce);
}


__static void
bmpce_release_locked(struct bmap_pagecache_entry *, 
		     struct bmap_pagecache *);

void
bmpce_handle_lru_locked(struct bmap_pagecache_entry *bmpce,
			struct bmap_pagecache *bmpc, int op, int incref)
{
	psc_assert(op == BIORQ_WRITE || op == BIORQ_READ);

	LOCK_ENSURE(&bmpc->bmpc_lock);
	LOCK_ENSURE(&bmpce->bmpce_lock);

	DEBUG_BMPCE((bmpce->bmpce_flags & BMPCE_EIO) ? PLL_WARN : PLL_INFO,
	    bmpce, "op=%d incref=%d", op, incref);

	psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) >= 0);
	psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) >= 0);

	if (psc_atomic16_read(&bmpce->bmpce_wrref)) {
		psc_assert(!(bmpce->bmpce_flags & BMPCE_LRU));
		psc_assert(!pll_conjoint(&bmpc->bmpc_lru, bmpce));

	} else {
		if (bmpce->bmpce_flags & BMPCE_LRU)
			psc_assert(pll_conjoint(&bmpc->bmpc_lru, bmpce));
	}

	if (incref) {
		PFL_GETTIMESPEC(&bmpce->bmpce_laccess);

		if (op == BIORQ_WRITE) {
			if (bmpce->bmpce_flags & BMPCE_LRU) {
				pll_remove(&bmpc->bmpc_lru, bmpce);
				bmpce->bmpce_flags &= ~BMPCE_LRU;
			}
			psc_atomic16_inc(&bmpce->bmpce_wrref);

		} else {
			if (bmpce->bmpce_flags & BMPCE_LRU) {
				pll_remove(&bmpc->bmpc_lru, bmpce);
				pll_add_sorted(&bmpc->bmpc_lru, bmpce,
					       bmpce_lrusort_cmp1);
			} else
				psc_assert(
				   psc_atomic16_read(&bmpce->bmpce_wrref) ||
				   psc_atomic16_read(&bmpce->bmpce_rdref) ||
				   (bmpce->bmpce_flags & BMPCE_READPNDG)  ||
				   (bmpce->bmpce_flags & BMPCE_DATARDY)   ||
				   (bmpce->bmpce_flags & BMPCE_INIT));

			psc_atomic16_inc(&bmpce->bmpce_rdref);
		}

	} else {
		//if (!(bmpce->bmpce_flags & BMPCE_EIO))
		//	psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		if (op == BIORQ_WRITE) {
			psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
			psc_assert(!(bmpce->bmpce_flags & BMPCE_LRU));
			psc_atomic16_dec(&bmpce->bmpce_wrref);

		} else {
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);
			psc_atomic16_dec(&bmpce->bmpce_rdref);
			if (!psc_atomic16_read(&bmpce->bmpce_rdref))
				bmpce->bmpce_flags &= ~BMPCE_READPNDG;
		}

		if (!(psc_atomic16_read(&bmpce->bmpce_wrref) ||
		      psc_atomic16_read(&bmpce->bmpce_rdref))) {
			/* Last ref on an EIO page so remove it.
			 */
			if (bmpce->bmpce_flags & BMPCE_EIO) {
				DEBUG_BMPCE(PLL_WARN, bmpce, "freeing EIO");

				if (bmpce->bmpce_flags & BMPCE_READPNDG) {
					bmpce->bmpce_flags &= ~BMPCE_READPNDG;
					psc_assert(bmpce->bmpce_waitq);
					BMPCE_WAKE(bmpce);
				}

				bmpce_freeprep(bmpce);
				bmpce_release_locked(bmpce, bmpc);
				return;

			} else if (!(bmpce->bmpce_flags & BMPCE_LRU)) {
				bmpce->bmpce_flags &= ~BMPCE_READPNDG;
				bmpce->bmpce_flags |= BMPCE_LRU;
				pll_add_sorted(&bmpc->bmpc_lru, bmpce,
					       bmpce_lrusort_cmp1);
			}

		} else if (bmpce->bmpce_flags & BMPCE_EIO) {
			/* In cases where EIO is present the lock must be
			 *   freed no matter what.  This is because we
			 *   try to free the bmpce above, which when
			 *   successful, replaces the bmpce to the pool.
			 */
			BMPCE_WAKE(bmpce);
			BMPCE_ULOCK(bmpce);
		}
	}

	if (pll_nitems(&bmpc->bmpc_lru) > 0) {
		bmpce = pll_peekhead(&bmpc->bmpc_lru);
		memcpy(&bmpc->bmpc_oldest, &bmpce->bmpce_laccess,
		       sizeof(struct timespec));
	}
}

int
bmpc_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq *a = x, *b = y;

	if (a->biorq_off == b->biorq_off)
		/*
		 * Larger requests with the same start offset should
		 * have ordering priority.
		 */
		return (CMP(b->biorq_len, a->biorq_len));
	return (CMP(a->biorq_off, b->biorq_off));
}

__static void
bmpce_release_locked(struct bmap_pagecache_entry *bmpce,
		     struct bmap_pagecache *bmpc)
{
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_rdref));
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));
	psc_assert(pll_empty(&bmpce->bmpce_pndgaios));
	psc_assert(bmpce->bmpce_flags == BMPCE_FREEING);

	DEBUG_BMPCE(PLL_INFO, bmpce, "freeing");

	psc_assert(SPLAY_REMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, bmpce));
	if (pll_conjoint(&bmpc->bmpc_lru, bmpce))
		pll_remove(&bmpc->bmpc_lru, bmpce);

	bmpce_init(bmpcePoolMgr, bmpce);
	psc_pool_return(bmpcePoolMgr, bmpce);
}

/**
 * bmpc_freeall_locked - Called when a bmap is being released.  Iterate
 *	across the tree freeing each bmpce.  Prior to being invoked, all
 *	bmpce's must be idle (ie have zero refcnts) and be present on
 *	bmpc_lru.
 */
void
bmpc_freeall_locked(struct bmap_pagecache *bmpc)
{
	struct bmap_pagecache_entry *a, *b;

	LOCK_ENSURE(&bmpc->bmpc_lock);
	psc_assert(pll_empty(&bmpc->bmpc_pndg_biorqs));
	psc_assert(pll_empty(&bmpc->bmpc_new_biorqs));
	psc_assert(pll_empty(&bmpc->bmpc_pndg_ra));

	for (a = SPLAY_MIN(bmap_pagecachetree, &bmpc->bmpc_tree); a; a = b) {
		b = SPLAY_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, a);

		BMPCE_LOCK(a);
		bmpce_freeprep(a);
		BMPCE_ULOCK(a);

		bmpce_release_locked(a, bmpc);
	}
	psc_assert(SPLAY_EMPTY(&bmpc->bmpc_tree));
	psc_assert(pll_empty(&bmpc->bmpc_lru));
}

__static void
bmpc_biorq_seterr(struct bmpc_ioreq *r, int err)
{
	BIORQ_LOCK(r);
	r->biorq_flags |= err;
	BIORQ_ULOCK(r);

	DEBUG_BIORQ(PLL_ERROR, r, "write-back flush failure (err=%d)", err);

	msl_mfh_seterr(r->biorq_fhent);
}

/**
 * bmpc_biorqs_fail - Set the flushrc so that fuse calls blocked in flush()
 *    will awake.
 * Notes: Pending RA pages should fail on their own via RPC callback.
 */
void
bmpc_biorqs_fail(struct bmap_pagecache *bmpc, int err)
{
	struct bmpc_ioreq *r;

	BMPC_LOCK(bmpc);
	PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs) {
		bmpc_biorq_seterr(r, err);
	}

	PLL_FOREACH(r, &bmpc->bmpc_new_biorqs) {
		bmpc_biorq_seterr(r, (err | BIORQ_FLUSHABORT));
	}
	BMPC_ULOCK(bmpc);
}

/**
 * bmpc_lru_tryfree - Attempt to free 'nfree' blocks from the provided
 *    bmap_pagecache structure.
 * @bmpc:   bmap_pagecache
 * @nfree:  number of blocks to free.
 */
__static int
bmpc_lru_tryfree(struct bmap_pagecache *bmpc, int nfree)
{
	struct bmap_pagecache_entry *bmpce, *tmp;
//	struct timespec ts, expire;
	int freed = 0;

//	PFL_GETTIMESPEC(&ts);

	PLL_LOCK(&bmpc->bmpc_lru);
	PLL_FOREACH_SAFE(bmpce, tmp, &bmpc->bmpc_lru) {
		BMPCE_LOCK(bmpce);

		psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));

		if (psc_atomic16_read(&bmpce->bmpce_rdref)) {
			DEBUG_BMPCE(PLL_INFO, bmpce, "rd ref, skip");
			BMPCE_ULOCK(bmpce);
			continue;
		}

		if (bmpce->bmpce_flags & BMPCE_EIO) {
			/* The thread who sets BMPCE_EIO will remove
			 *   this page from the cache.
			 */
			DEBUG_BMPCE(PLL_WARN, bmpce, "BMPCE_EIO, skip");
			BMPCE_ULOCK(bmpce);
			continue;
		}

#if 0
		timespecsub(&bmpce->bmpce_laccess, &ts, &expire);

		if (timespeccmp(&ts, &bmpce->bmpce_laccess, <)) {
			DEBUG_BMPCE(PLL_NOTICE, bmpce,
			    "expire=("PSCPRI_TIMESPEC") too recent, skip",
			    PSCPRI_TIMESPEC_ARGS(&expire));

			BMPCE_ULOCK(bmpce);
			break;

		} else {
			DEBUG_BMPCE(PLL_NOTICE, bmpce,
			    "freeing expire=("PSCPRI_TIMESPEC")",
			    PSCPRI_TIMESPEC_ARGS(&expire));

		}
#else
		DEBUG_BMPCE(PLL_NOTICE, bmpce, "freeing last_access=("
			    PSCPRI_TIMESPEC")",
			    PSCPRI_TIMESPEC_ARGS(&bmpce->bmpce_laccess));
		bmpce_freeprep(bmpce);
		bmpce_release_locked(bmpce, bmpc);
		if (++freed >= nfree)
			break;
#endif
	}

	/* Save CPU, assume that the head of the list is the oldest entry.
	 */
	if (pll_nitems(&bmpc->bmpc_lru) > 0) {
		bmpce = pll_peekhead(&bmpc->bmpc_lru);
		memcpy(&bmpc->bmpc_oldest, &bmpce->bmpce_laccess,
		       sizeof(struct timespec));
	}
	PLL_ULOCK(&bmpc->bmpc_lru);

	return (freed);
}

/**
 * bmpc_reap - Reap bmpce from the LRU list.  Sometimes we free
 *	bmpce directly into the pool, so we can't wait here forever.
 */
__static int
bmpce_reap(struct psc_poolmgr *m)
{
	struct bmap_pagecache *bmpc;
	int nfreed = 0, waiters = atomic_read(&m->ppm_nwaiters);

	LIST_CACHE_LOCK(&bmpcLru);

	lc_sort(&bmpcLru, qsort, bmpc_lru_cmp);
	/* Should be sorted from oldest bmpc to newest.
	 */
	LIST_CACHE_FOREACH(bmpc, &bmpcLru) {
		psclog_dbg("bmpc=%p npages=%d age(%ld:%ld) waiters=%d", bmpc,
		   pll_nitems(&bmpc->bmpc_lru), bmpc->bmpc_oldest.tv_sec,
		   bmpc->bmpc_oldest.tv_nsec, waiters);

		/* First check for LRU items.
		 */
		if (!pll_nitems(&bmpc->bmpc_lru)) {
			psclog_debug("skip bmpc=%p, nothing on lru", bmpc);
			continue;
		}

		nfreed += bmpc_lru_tryfree(bmpc, waiters);

		if (nfreed >= waiters)
			break;
	}
	LIST_CACHE_ULOCK(&bmpcLru);

	psclog_info("nfreed=%d, waiters=%d", nfreed, waiters);

	return (nfreed);
}

void
bmpc_global_init(void)
{
	psc_poolmaster_init(&bmpcePoolMaster,
	    struct bmap_pagecache_entry, bmpce_lentry, PPMF_AUTO, 512,
	    512, 16384, bmpce_init, bmpce_destroy, bmpce_reap, "bmpce");

	bmpcePoolMgr = psc_poolmaster_getmgr(&bmpcePoolMaster);

	psc_poolmaster_init(&bwcPoolMaster,
	    struct bmpc_write_coalescer, bwc_lentry, PPMF_AUTO, 64,
	    64, 0, bwc_init, NULL, NULL, "bwc");

	bwcPoolMgr = psc_poolmaster_getmgr(&bwcPoolMaster);

	lc_reginit(&bmpcLru, struct bmap_pagecache, bmpc_lentry,
	    "bmpclru");
}

#if PFL_DEBUG > 0
void
dump_bmpce_flags(uint32_t flags)
{
	int seq = 0;

	PFL_PRFLAG(BMPCE_NEW, &flags, &seq);
	PFL_PRFLAG(BMPCE_DATARDY, &flags, &seq);
	PFL_PRFLAG(BMPCE_DIRTY2LRU, &flags, &seq);
	PFL_PRFLAG(BMPCE_LRU, &flags, &seq);
	PFL_PRFLAG(BMPCE_FREE, &flags, &seq);
	PFL_PRFLAG(BMPCE_FREEING, &flags, &seq);
	PFL_PRFLAG(BMPCE_INIT, &flags, &seq);
	PFL_PRFLAG(BMPCE_READPNDG, &flags, &seq);
	PFL_PRFLAG(BMPCE_RBWPAGE, &flags, &seq);
	PFL_PRFLAG(BMPCE_RBWRDY, &flags, &seq);
	PFL_PRFLAG(BMPCE_INFLIGHT, &flags, &seq);
	PFL_PRFLAG(BMPCE_EIO, &flags, &seq);
	PFL_PRFLAG(BMPCE_READA, &flags, &seq);
	PFL_PRFLAG(BMPCE_AIOWAIT, &flags, &seq);
	PFL_PRFLAG(BMPCE_SYNCWAIT, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}

void
dump_biorq_flags(uint32_t flags)
{
	int seq = 0;

	PFL_PRFLAG(BIORQ_READ, &flags, &seq);
	PFL_PRFLAG(BIORQ_WRITE, &flags, &seq);
	PFL_PRFLAG(BIORQ_RBWFP, &flags, &seq);
	PFL_PRFLAG(BIORQ_RBWLP, &flags, &seq);
	PFL_PRFLAG(BIORQ_SCHED, &flags, &seq);
	PFL_PRFLAG(BIORQ_INFL, &flags, &seq);
	PFL_PRFLAG(BIORQ_DIO, &flags, &seq);
	PFL_PRFLAG(BIORQ_FORCE_EXPIRE, &flags, &seq);
	PFL_PRFLAG(BIORQ_DESTROY, &flags, &seq);
	PFL_PRFLAG(BIORQ_FLUSHRDY, &flags, &seq);
	PFL_PRFLAG(BIORQ_NOFHENT, &flags, &seq);
	PFL_PRFLAG(BIORQ_APPEND, &flags, &seq);
	PFL_PRFLAG(BIORQ_READAHEAD, &flags, &seq);
	PFL_PRFLAG(BIORQ_RBWFAIL, &flags, &seq);
	PFL_PRFLAG(BIORQ_AIOWAIT, &flags, &seq);
	PFL_PRFLAG(BIORQ_RESCHED, &flags, &seq);
	PFL_PRFLAG(BIORQ_ARCHIVER, &flags, &seq);
	PFL_PRFLAG(BIORQ_FLUSHABORT, &flags, &seq);
	PFL_PRFLAG(BIORQ_EXPIREDLEASE, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif
