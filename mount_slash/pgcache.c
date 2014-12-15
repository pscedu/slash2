/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * pgcache - Client user data page cache routines.  read(2) and write(2)
 * data is held in these buffers when the client is not in direct I/O
 * (DIO) mode.
 */

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <time.h>

#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/fsmod.h"
#include "pfl/lockedlist.h"
#include "pfl/pool.h"
#include "pfl/treeutil.h"

#include "pgcache.h"
#include "bmap_cli.h"
#include "mount_slash.h"

struct psc_poolmaster	 bmpce_poolmaster;
struct psc_poolmgr	*bmpce_pool;
struct psc_poolmaster    bwc_poolmaster;
struct psc_poolmgr	*bwc_pool;

struct psc_listcache	 bmpcLru;

SPLAY_GENERATE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
    bmpce_cmp)
SPLAY_GENERATE(bmpc_biorq_tree, bmpc_ioreq, biorq_tentry,
    bmpc_biorq_cmp)

/**
 * bwc_init - Initialize write coalescer pool entry.
 */
int
bwc_init(__unusedx struct psc_poolmgr *poolmgr, void *p)
{
	struct bmpc_write_coalescer *bwc = p;

	memset(bwc, 0, sizeof(*bwc));
	INIT_PSC_LISTENTRY(&bwc->bwc_lentry);
	return (0);
}

void
bwc_release(struct bmpc_write_coalescer *bwc)
{
	bwc_init(bwc_pool, bwc);
	psc_dynarray_free(&bwc->bwc_biorqs);
	psc_pool_return(bwc_pool, bwc);
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
	INIT_SPINLOCK(&e->bmpce_lock);
	pll_init(&e->bmpce_pndgaios, struct bmpc_ioreq,
	    biorq_png_lentry, &e->bmpce_lock);
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
bmpce_lookup_locked(struct bmapc_memb *b, uint32_t off,
    struct psc_waitq *wq)
{
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry search, *e = NULL, *e2 = NULL;

	bmpc = bmap_2_bmpc(b);
	search.bmpce_off = off;

	for (;;) {
		e = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
		    &search);
		if (e) {
			if (e->bmpce_flags & BMPCE_EIO) {
				DEBUG_BMPCE(PLL_WARN, e, "skip an EIO page");
				BMAP_ULOCK(b);
				pscthr_yield();
				OPSTAT_INCR(SLC_OPST_BMPCE_EIO);
				BMAP_LOCK(b);
				continue;
			}
			DEBUG_BMPCE(PLL_DIAG, e, "add reference");
			OPSTAT_INCR(SLC_OPST_BMPCE_HIT);
			psc_atomic32_inc(&e->bmpce_ref);
			break;
		}

		if (e2 == NULL) {
			BMAP_ULOCK(b);
			e2 = psc_pool_get(bmpce_pool);
			OPSTAT_INCR(SLC_OPST_BMPCE_GET);
			BMAP_LOCK(b);
			continue;
		} else {
			e = e2;
			e2 = NULL;
			e->bmpce_off = search.bmpce_off;

			psc_atomic32_set(&e->bmpce_ref, 1);
			e->bmpce_start = e->bmpce_off;
			e->bmpce_len = 0;
			e->bmpce_waitq = wq;

			OPSTAT_INCR(SLC_OPST_BMPCE_INSERT);
			PSC_SPLAY_XINSERT(bmap_pagecachetree,
			    &bmpc->bmpc_tree, e);
			break;
		}
	}
	if (e2) {
		OPSTAT_INCR(SLC_OPST_BMPCE_PUT);
		psc_pool_return(bmpce_pool, e2);
	}
	return (e);
}

void
bmpce_free(struct bmap_pagecache_entry *e,
    struct bmap_pagecache *bmpc)
{
	PSC_SPLAY_XREMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, e);

	OPSTAT_INCR(SLC_OPST_BMPCE_PUT);
	bmpce_init(bmpce_pool, e);
	psc_pool_return(bmpce_pool, e);
}

void
bmpce_release_locked(struct bmap_pagecache_entry *e,
    struct bmap_pagecache *bmpc)
{
	int ref;

	LOCK_ENSURE(&e->bmpce_lock);

	ref = psc_atomic32_read(&e->bmpce_ref);
	if (ref <= 0)
		psc_fatalx("bmpce = %p, ref = %d", e, ref);

	psc_atomic32_dec(&e->bmpce_ref);
	DEBUG_BMPCE(PLL_DIAG, e, "drop reference");
	if (ref > 1) {
		BMPCE_ULOCK(e);
		return;
	}
	psc_assert(pll_empty(&e->bmpce_pndgaios));

	if (e->bmpce_flags & BMPCE_LRU) {
		e->bmpce_flags &= ~BMPCE_LRU;
		pll_remove(&bmpc->bmpc_lru, e);
	}

	if ((e->bmpce_flags & BMPCE_DATARDY) &&
	   !(e->bmpce_flags & BMPCE_EIO) &&
	   !(e->bmpce_flags & BMPCE_DISCARD)) {
		DEBUG_BMPCE(PLL_DIAG, e, "put on LRU");
		PFL_GETPTIMESPEC(&e->bmpce_laccess);
		e->bmpce_flags |= BMPCE_LRU;

		// XXX locking order violation?
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
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct timespec issue;
	struct bmpc_ioreq *r;
	long inflight;

	r = psc_pool_get(slc_biorq_pool);
	memset(r, 0, sizeof(*r));
	INIT_PSC_LISTENTRY(&r->biorq_lentry);
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
	r->biorq_fsrqi = q;
	r->biorq_last_sliod = IOS_ID_ANY;

	/* Add the biorq to the fsrq. */
	msl_fsrqinfo_biorq_add(q, r, rqnum);

	BMAP_LOCK(b);
	bmap_op_start_type(b, BMAP_OPCNT_BIORQ);
	if (b->bcm_flags & BMAP_DIO)
		r->biorq_flags |= BIORQ_DIO;

	pll_add(&bmpc->bmpc_pndg_biorqs, r);

	BMAP_ULOCK(b);

	OPSTAT_INCR(SLC_OPST_BIORQ_ALLOC);

	inflight = OPSTAT_CURR(SLC_OPST_BIORQ_ALLOC) -
	    OPSTAT_CURR(SLC_OPST_BIORQ_DESTROY);
	if (inflight > OPSTAT_CURR(SLC_OPST_BIORQ_MAX))
		OPSTAT_ASSIGN(SLC_OPST_BIORQ_MAX, inflight);

	DEBUG_BIORQ(PLL_DIAG, r, "creating");

	return (r);
}

/**
 * bmpc_freeall_locked - Called when a bmap is being released.  Iterate
 *	across the tree freeing each bmpce.  Prior to being invoked, all
 *	bmpce's must be idle (i.e. have zero refcnts) and be present on
 *	bmpc_lru.
 */
void
bmpc_freeall_locked(struct bmap_pagecache *bmpc)
{
	struct bmap_pagecache_entry *a, *b;

	psc_assert(SPLAY_EMPTY(&bmpc->bmpc_new_biorqs));

	/* DIO rq's are allowed since no cached pages are involved. */
	if (!pll_empty(&bmpc->bmpc_pndg_biorqs)) {
		struct bmpc_ioreq *r;

		PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs)
			psc_assert(r->biorq_flags & BIORQ_DIO);
	}

	/* Remove any LRU pages still associated with the bmap. */
	for (a = SPLAY_MIN(bmap_pagecachetree, &bmpc->bmpc_tree); a;
	    a = b) {
		b = SPLAY_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, a);

		BMPCE_LOCK(a);

		psc_assert(a->bmpce_flags & BMPCE_LRU);
		psc_assert(!psc_atomic32_read(&a->bmpce_ref));

		OPSTAT_INCR(SLC_OPST_BMPCE_BMAP_REAP);
		pll_remove(&bmpc->bmpc_lru, a);
		bmpce_free(a, bmpc);
	}
	psc_assert(SPLAY_EMPTY(&bmpc->bmpc_tree));
	psc_assert(pll_empty(&bmpc->bmpc_lru));
}

void
bmpc_biorqs_flush(struct bmapc_memb *b, int wait)
{
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r;
	int expired;

	bmpc = bmap_2_bmpc(b);

 retry:
	expired = 0;
	BMAP_LOCK(b);
	SPLAY_FOREACH(r, bmpc_biorq_tree, &bmpc->bmpc_new_biorqs) {
		BIORQ_LOCK(r);
		r->biorq_flags |= BIORQ_FORCE_EXPIRE;
		BIORQ_ULOCK(r);
		expired++;
		DEBUG_BIORQ(PLL_DIAG, r, "force expire");
	}
	if (expired) {
		bmap_flushq_wake(BMAPFLSH_EXPIRE);
		if (wait) {
			psc_waitq_waitrel_us(&bmpc->bmpc_waitq,
			    &b->bcm_lock, 3000);
			goto retry;
		}
	}
	BMAP_ULOCK(b);
}

void
bmpc_biorqs_destroy_locked(struct bmapc_memb *b, int rc)
{
	int i;
	struct bmpc_ioreq *r, *tmp;
	struct psc_dynarray a = DYNARRAY_INIT;
	struct bmap_pagecache *bmpc;
	struct bmap_cli_info *bci;

	bci = bmap_2_bci(b);
	if (rc && !bci->bci_flush_rc)
		bci->bci_flush_rc = rc;

	/*
	 * Inflight biorqs are off the splay tree.  So we shouldn't race
	 * to destroy the same biorq.
	 */
	bmpc = bmap_2_bmpc(b);
	for (r = SPLAY_MIN(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs); r;
	    r = tmp) {

		tmp = SPLAY_NEXT(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs, r);

		BIORQ_LOCK(r);
		if (r->biorq_flags & BIORQ_SCHED) {
			BIORQ_ULOCK(r);
			continue;
		}
		PSC_SPLAY_XREMOVE(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs, r);
		BIORQ_ULOCK(r);
		psc_dynarray_add(&a, r);
	}
	BMAP_ULOCK(b);

	DYNARRAY_FOREACH(r, i, &a) {
		msl_bmpces_fail(r, rc);
		msl_biorq_release(r);
	}
	OPSTAT_INCR(SLC_OPST_BIORQ_DESTROY_BATCH);
	psc_dynarray_free(&a);
}

static __inline int
bmpc_lru_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache *a = x, *b = y;

	if (timespeccmp(&a->bmpc_oldest, &b->bmpc_oldest, <))
		return (-1);
	if (timespeccmp(&a->bmpc_oldest, &b->bmpc_oldest, >))
		return (1);
	return (0);
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

	PLL_FOREACH_SAFE(e, tmp, &bmpc->bmpc_lru) {
		if (!BMPCE_TRYLOCK(e))
			continue;

		psc_assert(e->bmpce_flags & BMPCE_LRU);
		if (psc_atomic32_read(&e->bmpce_ref)) {
			DEBUG_BMPCE(PLL_DIAG, e, "non-zero ref, skip");
			BMPCE_ULOCK(e);
			continue;
		}
		OPSTAT_INCR(SLC_OPST_BMPCE_REAP);
		pll_remove(&bmpc->bmpc_lru, e);
		bmpce_free(e, bmpc);
		if (++freed >= nfree)
			break;
	}

	/*
	 * Save CPU: assume that the head of the list is the oldest
	 * entry.
	 */
	if (pll_nitems(&bmpc->bmpc_lru) > 0) {
		e = pll_peekhead(&bmpc->bmpc_lru);
		memcpy(&bmpc->bmpc_oldest, &e->bmpce_laccess,
		    sizeof(struct timespec));
		lc_remove(&bmpcLru, bmpc);
		lc_add_sorted(&bmpcLru, bmpc, bmpc_lru_cmp);
	}

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
	struct bmap *b;
	int nfreed = 0;

	LIST_CACHE_LOCK(&bmpcLru);
	LIST_CACHE_FOREACH(bmpc, &bmpcLru) {
		psclog_debug("bmpc=%p npages=%d age="PSCPRI_TIMESPEC" "
		    "waiters=%d",
		    bmpc, pll_nitems(&bmpc->bmpc_lru),
		    PSCPRI_TIMESPEC_ARGS(&bmpc->bmpc_oldest),
		    psc_atomic32_read(&m->ppm_nwaiters));

		b = bmpc_2_bmap(bmpc);
		if (!BMAP_TRYLOCK(b))
			continue;

		/* First check for LRU items. */
		if (pll_nitems(&bmpc->bmpc_lru)) {
			DEBUG_BMAP(PLL_DIAG, b, "try free");
			nfreed += bmpc_lru_tryfree(bmpc,
			    psc_atomic32_read(&m->ppm_nwaiters));
			DEBUG_BMAP(PLL_DIAG, b, "try free done");
		} else
			psclog_debug("skip bmpc=%p, nothing on lru",
			    bmpc);

		BMAP_ULOCK(b);
		if (nfreed >= psc_atomic32_read(&m->ppm_nwaiters))
			break;
	}
	LIST_CACHE_ULOCK(&bmpcLru);

	psclog_diag("nfreed=%d, waiters=%d", nfreed,
	    psc_atomic32_read(&m->ppm_nwaiters));

	return (nfreed);
}

void
bmpc_global_init(void)
{
	psc_poolmaster_init(&bmpce_poolmaster,
	    struct bmap_pagecache_entry, bmpce_lentry, PPMF_AUTO, 512,
	    512, 16384, bmpce_init, bmpce_destroy, bmpce_reap, "bmpce");

	bmpce_pool = psc_poolmaster_getmgr(&bmpce_poolmaster);

	psc_poolmaster_init(&bwc_poolmaster,
	    struct bmpc_write_coalescer, bwc_lentry, PPMF_AUTO, 64,
	    64, 0, bwc_init, NULL, NULL, "bwc");

	bwc_pool = psc_poolmaster_getmgr(&bwc_poolmaster);

	lc_reginit(&bmpcLru, struct bmap_pagecache, bmpc_lentry,
	    "bmpclru");

	/* make it visible */
	OPSTAT_INCR(SLC_OPST_BIORQ_MAX);
}

#if PFL_DEBUG > 0
void
dump_bmpce_flags(uint32_t flags)
{
	int seq = 0;

	PFL_PRFLAG(BMPCE_DATARDY, &flags, &seq);
	PFL_PRFLAG(BMPCE_LRU, &flags, &seq);
	PFL_PRFLAG(BMPCE_TOFREE, &flags, &seq);
	PFL_PRFLAG(BMPCE_EIO, &flags, &seq);
	PFL_PRFLAG(BMPCE_READA, &flags, &seq);
	PFL_PRFLAG(BMPCE_AIOWAIT, &flags, &seq);
	PFL_PRFLAG(BMPCE_DISCARD, &flags, &seq);
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
	PFL_PRFLAG(BIORQ_SCHED, &flags, &seq);
	PFL_PRFLAG(BIORQ_DIO, &flags, &seq);
	PFL_PRFLAG(BIORQ_FORCE_EXPIRE, &flags, &seq);
	PFL_PRFLAG(BIORQ_DESTROY, &flags, &seq);
	PFL_PRFLAG(BIORQ_FLUSHRDY, &flags, &seq);
	PFL_PRFLAG(BIORQ_WAIT, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif
