/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_ds/treeutil.h"
#include "psc_util/atomic.h"
#include "psc_util/pool.h"
#include "psc_util/ctlsvr.h"

#include "pgcache.h"
#include "bmap_cli.h"
#include "mount_slash.h"

struct psc_poolmaster	 bmpcePoolMaster;
struct psc_poolmgr	*bmpcePoolMgr;
struct psc_poolmaster    bwcPoolMaster;
struct psc_poolmgr	*bwcPoolMgr;

struct psc_listcache	 bmpcLru;

__static SPLAY_GENERATE(bmap_pagecachetree, bmap_pagecache_entry,
			bmpce_tentry, bmpce_cmp);

/**
 * bwc_init - Initialize write coalescer pool entry.
 */
int
bwc_init(__unusedx struct psc_poolmgr *poolmgr, void *p)
{
	struct bmpc_write_coalescer *bwc = p;

	memset(bwc, 0, sizeof(*bwc));
	INIT_PSC_LISTENTRY(&bwc->bwc_lentry);
	pll_init(&bwc->bwc_pll, struct bmpc_ioreq, biorq_bwc_lentry,
	    NULL);
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
	struct bmap_pagecache_entry *e = p;
	void *base;

	base = e->bmpce_base;
	memset(e, 0, sizeof(*e));
	INIT_PSC_LISTENTRY(&e->bmpce_lentry);
	INIT_PSC_LISTENTRY(&e->bmpce_ralentry);
	INIT_SPINLOCK(&e->bmpce_lock);
	pll_init(&e->bmpce_pndgaios, struct bmpc_ioreq,
	    biorq_png_lentry, &e->bmpce_lock);
	e->bmpce_flags = BMPCE_NEW;
	e->bmpce_base = base;
	if (!e->bmpce_base)
		e->bmpce_base = psc_alloc(BMPC_BUFSZ, PAF_PAGEALIGN);
	return (0);
}

void
bmpce_destroy(void *p)
{
	struct bmap_pagecache_entry *e = p;

	psc_free(e->bmpce_base, PAF_PAGEALIGN);
}

struct bmap_pagecache_entry *
bmpce_lookup_locked(struct bmap_pagecache *bmpc, struct bmpc_ioreq *r,
    uint32_t off, struct psc_waitq *wq)
{
	struct bmap_pagecache_entry search, *e = NULL, *e2 = NULL;

	LOCK_ENSURE(&bmpc->bmpc_lock);

	search.bmpce_off = off;

	while (!e) {
		e = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
		    &search);
		if (e) {
			if (e->bmpce_flags & BMPCE_EIO) {
				DEBUG_BMPCE(PLL_WARN, e, "skip an EIO page");
				BMPC_ULOCK(bmpc);
				sched_yield();
				OPSTAT_INCR(SLC_OPST_BIORQ_EIO_PAGE);
				BMPC_LOCK(bmpc);
				continue;
			}
			psc_atomic32_inc(&e->bmpce_ref);
			break;
		}

		if (e2 == NULL) {
			BMPC_ULOCK(bmpc);
			e2 = psc_pool_get(bmpcePoolMgr);
			OPSTAT_INCR(SLC_OPST_BMPCE_GET);
			BMPC_LOCK(bmpc);
			continue;
		} else {
			e = e2;
			e2 = NULL;
			e->bmpce_off = search.bmpce_off;
			bmpce_useprep(e, r, wq);

			OPSTAT_INCR(SLC_OPST_BMPCE_INSERT);
			PSC_SPLAY_XINSERT(bmap_pagecachetree,
			    &bmpc->bmpc_tree, e);
		}
	}
	if (e2) {
		OPSTAT_INCR(SLC_OPST_BMPCE_PUT);
		psc_pool_return(bmpcePoolMgr, e2);
	}
	return (e);
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

void
bmpce_free(struct bmap_pagecache_entry *e,
    struct bmap_pagecache *bmpc)
{
	LOCK_ENSURE(&bmpc->bmpc_lock);
	DEBUG_BMPCE(PLL_INFO, e, "freeing");

	PSC_SPLAY_XREMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, e);

	OPSTAT_INCR(SLC_OPST_BMPCE_PUT);
	bmpce_init(bmpcePoolMgr, e);
	psc_pool_return(bmpcePoolMgr, e);
}

void
bmpce_release_locked(struct bmap_pagecache_entry *e,
    struct bmap_pagecache *bmpc)
{
	int rc;

	LOCK_ENSURE(&bmpc->bmpc_lock);
	LOCK_ENSURE(&e->bmpce_lock);
	rc = psc_atomic32_read(&e->bmpce_ref);
	psc_assert(rc > 0);
	psc_atomic32_dec(&e->bmpce_ref);
	DEBUG_BMPCE(PLL_DIAG, e, "drop reference");
	if (rc > 1) {
		BMPCE_ULOCK(e);
		return;
	}
	psc_assert(pll_empty(&e->bmpce_pndgaios));

	if (e->bmpce_flags & BMPCE_LRU) {
		e->bmpce_flags &= ~BMPCE_LRU;
		pll_remove(&bmpc->bmpc_lru, e);
	}

	if (e->bmpce_flags & BMPCE_DATARDY) {
		PFL_GETTIMESPEC(&e->bmpce_laccess);
		e->bmpce_flags |= BMPCE_LRU;
		pll_add(&bmpc->bmpc_lru, e);
		BMPCE_ULOCK(e);
		return;
	}
	bmpce_free(e, bmpc);
}

struct bmpc_ioreq *
bmpc_biorq_new(struct msl_fsrqinfo *q, struct bmapc_memb *b, char *buf,
    int rqnum, uint32_t off, uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct timespec issue;

	r = psc_pool_get(slc_biorq_pool);

	memset(r, 0, sizeof(*r));

	psc_waitq_init(&r->biorq_waitq);
	INIT_PSC_LISTENTRY(&r->biorq_lentry);
	INIT_PSC_LISTENTRY(&r->biorq_mfh_lentry);
	INIT_PSC_LISTENTRY(&r->biorq_bwc_lentry);
	INIT_PSC_LISTENTRY(&r->biorq_png_lentry);
	INIT_SPINLOCK(&r->biorq_lock);

	PFL_GETTIMESPEC(&issue);
	timespecadd(&issue, &bmapFlushDefMaxAge, &r->biorq_expire);

	r->biorq_off = off;
	r->biorq_ref = 1;
	r->biorq_len = len;
	r->biorq_buf = buf;
	r->biorq_bmap = b;
	r->biorq_flags = op;
	r->biorq_mfh = q->mfsrq_mfh;
	r->biorq_fsrqi = q;
	r->biorq_last_sliod = IOS_ID_ANY;

	mfh_incref(q->mfsrq_mfh);

	/* Add the biorq to the fsrq. */
	msl_fsrqinfo_biorq_add(q, r, rqnum);

	bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

	BMAP_LOCK(b);
	if (b->bcm_flags & BMAP_DIO)
		r->biorq_flags |= BIORQ_DIO;
	if (b->bcm_flags & BMAP_ARCHIVER)
		r->biorq_flags |= BIORQ_ARCHIVER;

	if (op == BIORQ_READ || (r->biorq_flags & BIORQ_DIO)) {
		r->biorq_flags |= BIORQ_PENDING;
		pll_add(&bmap_2_bmpc(b)->bmpc_pndg_biorqs, r);
	} else
		pll_add_sorted(&bmap_2_bmpc(b)->bmpc_new_biorqs, r,
		    bmpc_biorq_cmp);
	BMAP_ULOCK(b);

	OPSTAT_INCR(SLC_OPST_BIORQ_NEW);

	return (r);
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
	psc_assert(pll_empty(&bmpc->bmpc_new_biorqs));
	psc_assert(pll_empty(&bmpc->bmpc_pndg_ra));

	/* DIO rq's are allowed since no cached pages are involved. */
	if (!pll_empty(&bmpc->bmpc_pndg_biorqs)) {
		struct bmpc_ioreq *r;

		PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs)
			psc_assert(r->biorq_flags & BIORQ_DIO);
	}

	for (a = SPLAY_MIN(bmap_pagecachetree, &bmpc->bmpc_tree); a;
	    a = b) {
		b = SPLAY_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, a);

		BMPCE_LOCK(a);
		psc_assert(a->bmpce_flags & BMPCE_LRU);
		pll_remove(&bmpc->bmpc_lru, a);
		bmpce_free(a, bmpc);
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

	/*
	 * XXX, this could be a lease expire situation.
	 */
	DEBUG_BIORQ(PLL_ERROR, r, "write-back flush failure (err=%d)",
	    err);

	mfh_seterr(r->biorq_mfh);
}

/**
 * bmpc_biorqs_fail - Set the flushrc so that fuse calls blocked in
 *	flush() will awake.
 * Notes: Pending RA pages should fail on their own via RPC callback.
 */
void
bmpc_biorqs_fail(struct bmap_pagecache *bmpc, int err)
{
	struct bmpc_ioreq *r;

	BMPC_LOCK(bmpc);
	PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs)
		bmpc_biorq_seterr(r, err);
	PLL_FOREACH(r, &bmpc->bmpc_new_biorqs)
		bmpc_biorq_seterr(r, err);
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
	struct bmap_pagecache_entry *e, *tmp;
	int freed = 0;

	/* same lock used to protect bmap page cache */
	PLL_LOCK(&bmpc->bmpc_lru);
	PLL_FOREACH_SAFE(e, tmp, &bmpc->bmpc_lru) {
		BMPCE_LOCK(e);

		psc_assert(e->bmpce_flags & BMPCE_LRU);
		if (psc_atomic32_read(&e->bmpce_ref)) {
			DEBUG_BMPCE(PLL_INFO, e, "non-zero ref, skip");
			BMPCE_ULOCK(e);
			continue;
		}
		pll_remove(&bmpc->bmpc_lru, e);
		bmpce_free(e, bmpc);
		if (++freed >= nfree)
			break;
	}

	/*
	 * Save CPU, assume that the head of the list is the oldest
	 * entry.
	 */
	if (pll_nitems(&bmpc->bmpc_lru) > 0) {
		e = pll_peekhead(&bmpc->bmpc_lru);
		memcpy(&bmpc->bmpc_oldest, &e->bmpce_laccess,
		    sizeof(struct timespec));
	}
	PLL_ULOCK(&bmpc->bmpc_lru);

	return (freed);
}

/**
 * bmpc_reap - Reap bmpce from the LRU list.  Sometimes we free bmpce
 *	directly into the pool, so we can't wait here forever.
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
		psclog_dbg("bmpc=%p npages=%d age(%ld:%ld) waiters=%d",
		    bmpc, pll_nitems(&bmpc->bmpc_lru),
		    bmpc->bmpc_oldest.tv_sec, bmpc->bmpc_oldest.tv_nsec,
		    waiters);

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
	PFL_PRFLAG(BMPCE_TOFREE, &flags, &seq);
	PFL_PRFLAG(BMPCE_FREEING, &flags, &seq);
	PFL_PRFLAG(BMPCE_INIT, &flags, &seq);
	PFL_PRFLAG(BMPCE_READPNDG, &flags, &seq);
	PFL_PRFLAG(BMPCE_RBWPAGE, &flags, &seq);
	PFL_PRFLAG(BMPCE_RBWRDY, &flags, &seq);
	PFL_PRFLAG(BMPCE_INFLIGHT, &flags, &seq);
	PFL_PRFLAG(BMPCE_EIO, &flags, &seq);
	PFL_PRFLAG(BMPCE_READA, &flags, &seq);
	PFL_PRFLAG(BMPCE_AIOWAIT, &flags, &seq);
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
	PFL_PRFLAG(BIORQ_EXPIREDLEASE, &flags, &seq);
	PFL_PRFLAG(BIORQ_MAXRETRIES, &flags, &seq);
	PFL_PRFLAG(BIORQ_BMAPFAIL, &flags, &seq);
	PFL_PRFLAG(BIORQ_READFAIL, &flags, &seq);
	PFL_PRFLAG(BIORQ_PENDING, &flags, &seq);
	PFL_PRFLAG(BIORQ_WAIT, &flags, &seq);
	PFL_PRFLAG(BIORQ_MFHLIST, &flags, &seq);

	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif
