/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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

int			 msl_bmpces_min = 16384*2;	/* 16MiB */
int			 msl_bmpces_max = 16384*4; 	/* 512MiB */

struct psc_listcache     bmpcLru;

RB_GENERATE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
    bmpce_cmp)
RB_GENERATE(bmpc_biorq_tree, bmpc_ioreq, biorq_tentry, bmpc_biorq_cmp)

struct psc_listcache	 page_buffers;
int			 page_buffers_count;	/* total, including free */

void
msl_pgcache_init(void)
{
	int i;
	void *p;

	lc_reginit(&page_buffers, struct bmap_page_entry,
	    page_lentry, "pagebuffers");

	for (i = 0; i < bmpce_pool->ppm_min; i++) {
		p = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_SHARED, -1, 0);

		if (p == MAP_FAILED)
			psc_fatalx("Please raise vm.max_map_count limit");

		OPSTAT_INCR("mmap-success");
		page_buffers_count++;
		INIT_PSC_LISTENTRY((struct psc_listentry *)p);
		lc_add(&page_buffers, p);
	}
}

void *
msl_pgcache_get(int wait)
{
	void *p;
	struct timespec ts;
	static int warned = 0, failed = 0;

	p = lc_getnb(&page_buffers);
	if (p)
		return p;
 again:

	LIST_CACHE_LOCK(&page_buffers);
	if (page_buffers_count < bmpce_pool->ppm_max) {
		p = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_SHARED, -1, 0);
		if (p != MAP_FAILED) {
			warned = 0;
			OPSTAT_INCR("mmap-success");
			page_buffers_count++;
			LIST_CACHE_ULOCK(&page_buffers);
			return (p);
		}
		failed = 1;
		OPSTAT_INCR("mmap-failure");
	}
	LIST_CACHE_ULOCK(&page_buffers);

	if (failed && warned < 5) {
		warned++;
		psclog_warnx("Please raise vm.max_map_count for performance");
	}

	if (wait) {
		/*
		 * Use timed wait in case the limit is bumped by sys admin.
		 */
		ts.tv_nsec = 0;
		ts.tv_sec = time(NULL) + 30;
		p = lc_gettimed(&page_buffers, &ts);
		if (!p) {
			OPSTAT_INCR("pagecache-get-retry");
			goto again;
		}
	} else
		p = lc_getnb(&page_buffers);
	return (p);
}

void
msl_pgcache_put(void *p)
{
	int rc;
	/*
 	 * Do not assume that the max value has not changed.
 	 */
	LIST_CACHE_LOCK(&page_buffers);
	if (page_buffers_count > bmpce_pool->ppm_max) {
		rc = munmap(p, BMPC_BUFSZ);
		if (rc)
			OPSTAT_INCR("munmap-drop-failure");
		else
			OPSTAT_INCR("munmap-drop-success");
		page_buffers_count--;
	} else {
		INIT_PSC_LISTENTRY((struct psc_listentry *)p);
		lc_add(&page_buffers, p);
	}
	LIST_CACHE_ULOCK(&page_buffers);
}

void
msl_pgcache_reap(void)
{
	void *p;
	int i, rc, curr, nfree;
	static int count = 0;		/* this assume one reaper */

	/* 
	 * We don't reap if the number of free buffers keeps growing. 
	 * This greatly cuts down the number of mmap() calls.
	 */
	curr = lc_nitems(&page_buffers);
	if (!count || count != curr) {
		count = curr;
		return;
	}
	if (curr <= bmpce_pool->ppm_min)
		return;

	nfree = (curr - bmpce_pool->ppm_min) / 2;
	if (!nfree)
		nfree = 1;
	for (i = 0; i < nfree; i++) {
		p = lc_getnb(&page_buffers);
		if (!p)
			break;
		rc = munmap(p, BMPC_BUFSZ);
		if (rc)
			OPSTAT_INCR("munmap-reap-failure");
		else
			OPSTAT_INCR("munmap-reap-success");
	}
	LIST_CACHE_LOCK(&page_buffers);
	page_buffers_count -= i;
	LIST_CACHE_ULOCK(&page_buffers);
}

/*
 * Initialize write coalescer pool entry.
 */
struct bmpc_write_coalescer *
bwc_alloc(void)
{
	struct bmpc_write_coalescer *bwc;

	bwc = psc_pool_get(bwc_pool);
	memset(bwc, 0, sizeof(*bwc));
	INIT_PSC_LISTENTRY(&bwc->bwc_lentry);
	return (bwc);
}

void
bwc_free(struct bmpc_write_coalescer *bwc)
{
	psc_dynarray_free(&bwc->bwc_biorqs);
	psc_pool_return(bwc_pool, bwc);
}

/*
 * Initialize a bmap page cache entry.
 */
void
bmpce_init(struct bmap_pagecache_entry *e)
{
	memset(e, 0, sizeof(*e));
	INIT_PSC_LISTENTRY(&e->bmpce_lentry);
	INIT_SPINLOCK(&e->bmpce_lock);
	pll_init(&e->bmpce_pndgaios, struct bmpc_ioreq,
	    biorq_aio_lentry, &e->bmpce_lock);
}

int
bmpce_lookup(struct bmpc_ioreq *r, struct bmap *b, int flags,
    uint32_t off, struct psc_waitq *wq)
{
	struct bmap_pagecache_entry q, *e = NULL, *e2 = NULL;
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct bmap_pagecache *bmpc;
	void *page;

	bmpc = bmap_2_bmpc(b);
	q.bmpce_off = off;

	pfl_rwlock_rdlock(&bci->bci_rwlock);

	for (;;) {
		e = RB_FIND(bmap_pagecachetree, &bmpc->bmpc_tree, &q);
		if (e) {
			BMPCE_LOCK(e);
			/*
			 * It is possible that the EIO flag can be cleared
			 * and the page is re-used now.
			 */
			if ((e->bmpce_flags & BMPCEF_EIO) ||
			    (e->bmpce_flags & BMPCEF_TOFREE)) {
				BMPCE_ULOCK(e);
				continue;
			}

			e->bmpce_ref++;
			DEBUG_BMPCE(PLL_DIAG, e, "add reference");
			BMPCE_ULOCK(e);

			OPSTAT_INCR("msl.bmpce-cache-hit");
			break;
		}

		if (e2 == NULL) {
			pfl_rwlock_unlock(&bci->bci_rwlock);

			if (flags & BMPCEF_READAHEAD) {
				e2 = psc_pool_shallowget(bmpce_pool);
				if (e2 == NULL)
					return (EAGAIN);
				page = msl_pgcache_get(0);
				if (page == NULL) {
					psc_pool_return(bmpce_pool, e2);
					return (EAGAIN);
				}
			} else {
				e2 = psc_pool_get(bmpce_pool);
				page = msl_pgcache_get(1);
			}
			pfl_rwlock_wrlock(&bci->bci_rwlock);
			continue;
		} else {
			OPSTAT_INCR("msl.bmpce-cache-miss");

			e = e2;
			e2 = NULL;
			bmpce_init(e);
			e->bmpce_off = off;
			e->bmpce_ref = 1;
			e->bmpce_len = 0;
			e->bmpce_start = off;
			e->bmpce_waitq = wq;
			e->bmpce_flags = flags;
			e->bmpce_bmap = b;
			e->bmpce_base = page;

			PSC_RB_XINSERT(bmap_pagecachetree,
			    &bmpc->bmpc_tree, e);

			DEBUG_BMPCE(PLL_DIAG, e, "creating");
			break;
		}
	}
	pfl_rwlock_unlock(&bci->bci_rwlock);

	if (e2) {
		OPSTAT_INCR("msl.bmpce-gratuitous");
		msl_pgcache_put(page);
		psc_pool_return(bmpce_pool, e2);
	}

	psc_dynarray_add(&r->biorq_pages, e);

	DEBUG_BIORQ(PLL_DIAG, r, "registering bmpce@%p "
	    "n=%d foff=%"PRIx64, e, psc_dynarray_len(&r->biorq_pages),
	    off + bmap_foff(b));

	return (0);
}

void
bmpce_free(struct bmap_pagecache_entry *e)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(e->bmpce_bmap);
	struct bmap_cli_info *bci = bmap_2_bci(e->bmpce_bmap);
	int locked;

	psc_assert(e->bmpce_ref == 0);
	e->bmpce_flags |= BMPCEF_TOFREE;

	BMPCE_ULOCK(e);

	locked = pfl_rwlock_haswrlock(&bci->bci_rwlock);
	if (!locked)
		pfl_rwlock_wrlock(&bci->bci_rwlock);
	PSC_RB_XREMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, e);
	if (!locked)
		pfl_rwlock_unlock(&bci->bci_rwlock);

	if ((e->bmpce_flags & (BMPCEF_READAHEAD | BMPCEF_ACCESSED)) ==
	    BMPCEF_READAHEAD)
		OPSTAT2_ADD("msl.readahead-waste", BMPC_BUFSZ);

	DEBUG_BMPCE(PLL_DIAG, e, "destroying, locked = %d", locked);

	msl_pgcache_put(e->bmpce_base);
	psc_pool_return(bmpce_pool, e);
}

void
bmpce_release_locked(struct bmap_pagecache_entry *e, struct bmap_pagecache *bmpc)
{
	LOCK_ENSURE(&e->bmpce_lock);

	psc_assert(e->bmpce_ref > 0);
	e->bmpce_ref--;
	DEBUG_BMPCE(PLL_DIAG, e, "drop reference");
	if (e->bmpce_ref > 0) {
		BMPCE_ULOCK(e);
		return;
	}

	/* sanity checks */
	psc_assert(pll_empty(&e->bmpce_pndgaios));

	if (e->bmpce_flags & BMPCEF_LRU) {
		e->bmpce_flags &= ~BMPCEF_LRU;
		pll_remove(&bmpc->bmpc_lru, e);
	}

	if ((e->bmpce_flags & BMPCEF_DATARDY) &&
	   !(e->bmpce_flags & BMPCEF_EIO) &&
	   !(e->bmpce_flags & BMPCEF_DISCARD)) {
		DEBUG_BMPCE(PLL_DIAG, e, "put on LRU");
		e->bmpce_flags |= BMPCEF_LRU;

		// XXX locking order violation?
		pll_add(&bmpc->bmpc_lru, e);

		BMPCE_ULOCK(e);
		return;
	}
	bmpce_free(e);
}

struct bmpc_ioreq *
bmpc_biorq_new(struct msl_fsrqinfo *q, struct bmap *b, char *buf,
    uint32_t off, uint32_t len, int flags)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct timespec issue;
	struct bmpc_ioreq *r;

	r = psc_pool_get(msl_biorq_pool);
	memset(r, 0, sizeof(*r));
	INIT_PSC_LISTENTRY(&r->biorq_lentry);
	INIT_PSC_LISTENTRY(&r->biorq_exp_lentry);
	INIT_PSC_LISTENTRY(&r->biorq_aio_lentry);
	INIT_SPINLOCK(&r->biorq_lock);

	PFL_GETTIMESPEC(&issue);
	timespecadd(&issue, &msl_bflush_maxage, &r->biorq_expire);

	r->biorq_off = off;
	r->biorq_ref = 1;
	r->biorq_len = len;
	r->biorq_buf = buf;
	r->biorq_bmap = b;
	r->biorq_flags = flags;
	r->biorq_fsrqi = q;
	r->biorq_last_sliod = IOS_ID_ANY;

	if (b->bcm_flags & BMAPF_DIO) {
		r->biorq_flags |= BIORQ_DIO;
		if (flags & BIORQ_READ) {
			r->biorq_flags |= BIORQ_FREEBUF;
			r->biorq_buf = PSCALLOC(len);
		}
	}
	pll_add(&bmpc->bmpc_pndg_biorqs, r);
	DEBUG_BIORQ(PLL_DIAG, r, "creating");

	return (r);
}

/*
 * Called when a bmap is being released.  Iterate across the tree
 * freeing each bmpce.  Prior to being invoked, all bmpce's must be idle
 * (i.e. have zero refcnts).
 */
void
bmpc_freeall(struct bmap *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct bmap_pagecache_entry *e, *next;

	psc_assert(RB_EMPTY(&bmpc->bmpc_biorqs));

	/* DIO rq's are allowed since no cached pages are involved. */
	if (!pll_empty(&bmpc->bmpc_pndg_biorqs)) {
		struct bmpc_ioreq *r;

		PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs)
			psc_assert(r->biorq_flags & BIORQ_DIO);
	}

	/*
	 * Remove any LRU pages still associated with the bmap.
	 * Only readahead pages can be encountered here. If we
	 * don't treat readahead pages specially, this code can
	 * go away some day.
	 */
	pfl_rwlock_wrlock(&bci->bci_rwlock);
	for (e = RB_MIN(bmap_pagecachetree, &bmpc->bmpc_tree); e;
	    e = next) {
		next = RB_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, e);

		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCEF_DISCARD;
		bmpce_release_locked(e, bmpc);
	}
	pfl_rwlock_unlock(&bci->bci_rwlock);
}


void
bmpc_expire_biorqs(struct bmap_pagecache *bmpc)
{
	struct bmpc_ioreq *r;
	int wake = 0;

	PLL_FOREACH_BACKWARDS(r, &bmpc->bmpc_biorqs_exp) {
		BIORQ_LOCK(r);
		/*
		 * A biorq can only be added at the end of the list.  So
		 * when we encounter an already expired biorq we can
		 * stop since we've already processed it and all biorqs
		 * before it.
		 */
		if (r->biorq_flags & BIORQ_EXPIRE) {
			BIORQ_ULOCK(r);
			break;
		}
		r->biorq_retries = 0;
		r->biorq_flags |= BIORQ_EXPIRE;
		DEBUG_BIORQ(PLL_DIAG, r, "force expire");
		BIORQ_ULOCK(r);
		wake = 1;
	}
	if (wake)
		bmap_flushq_wake(BMAPFLSH_EXPIRE);
}

/*
 * Flush all biorqs on the bmap's `new' biorq list, which all writes get
 * initially placed on.  This routine is called in all flush code paths
 * and before launching read RPCs.
 *
 * @b: bmap to flush.
 */

void
bmpc_biorqs_flush(struct bmap *b)
{
	struct fidc_membh *f;
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(b);
	BMAP_LOCK_ENSURE(b);

	while (bmpc->bmpc_pndg_writes) {
		OPSTAT_INCR("msl.biorq-flush-wait");
		bmpc_expire_biorqs(bmpc);
		BMAP_ULOCK(b);
		f = b->bcm_fcmh;
		FCMH_LOCK(f);
		psc_waitq_waitrel_us(&f->fcmh_waitq, &f->fcmh_lock,
		    100);
		BMAP_LOCK(b);
	}
}

void
bmpce_reap_list(struct psc_dynarray *a, struct psc_listcache *lc,
    int flag, struct psc_poolmgr *m)
{
	struct bmap_pagecache_entry *e, *t;

	LIST_CACHE_LOCK(lc);
	LIST_CACHE_FOREACH_SAFE(e, t, lc) {
		/*
		 * This avoids a deadlock with bmpc_freeall().  In
		 * general, a background reaper should be nice to other
		 * uses.
		 */
		if (!BMPCE_TRYLOCK(e))
			continue;
		/*
 		 * XXX Checking flags here is bogus, we should assert that
 		 * the flag is set because it is on the list.  In addition,
 		 * we should check reference count here.
 		 */ 
		if ((e->bmpce_flags & (flag |
		    BMPCEF_REAPED)) == flag) {
			e->bmpce_flags &= ~flag;
			e->bmpce_flags |= BMPCEF_DISCARD | BMPCEF_REAPED;
			lc_remove(lc, e);
			psc_dynarray_add(a, e);
			DEBUG_BMPCE(PLL_DIAG, e, "reaping from %s",
			    lc->plc_name);
		}
		BMPCE_ULOCK(e);

		if (psc_dynarray_len(a) >=
		    psc_atomic32_read(&m->ppm_nwaiters))
			break;
	}
	if (!psc_dynarray_len(a) && lc_nitems(lc))
		OPSTAT_INCR("msl.bmpce-reap-spin");
	LIST_CACHE_ULOCK(lc);
}

bmpc_lru_tryfree(struct bmap_pagecache *bmpc, int nfree)
{
	struct bmap_pagecache_entry *e, *tmp;
	int freed = 0;

	PLL_FOREACH_SAFE(e, tmp, &bmpc->bmpc_lru) {
		if (!BMPCE_TRYLOCK(e))
			continue;

		psc_assert(e->bmpce_flags & BMPCEF_LRU);
		if (e->bmpce_ref) {
			DEBUG_BMPCE(PLL_DIAG, e, "non-zero ref, skip");
			BMPCE_ULOCK(e);
			continue;
		}
		OPSTAT_INCR("bmpce_reap");
		pll_remove(&bmpc->bmpc_lru, e);
		bmpce_free(e);
		if (++freed >= nfree)
			break;
	}
	return (freed);
}

__static int
bmpce_reap(struct psc_poolmgr *m)
{
	struct bmap_pagecache *bmpc;
	struct bmap *b;
	int nfreed = 0;

	LIST_CACHE_LOCK(&bmpcLru);
	LIST_CACHE_FOREACH(bmpc, &bmpcLru) {
		psclog_debug("bmpc=%p npages=%d, aiters=%d",
		    bmpc, pll_nitems(&bmpc->bmpc_lru),
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
		} else {
			OPSTAT_INCR("bmpce_reap_spin");
			psclog_debug("skip bmpc=%p, nothing on lru",
			    bmpc);
		}

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
	/*
	 * msl_pagecache_maxsize can be set like this: pagecache_maxsize=2G
 	 */
	if (msl_pagecache_maxsize)
		msl_bmpces_max = msl_pagecache_maxsize / BMPC_BUFSZ;
	if (msl_bmpces_max < msl_bmpces_min)
		msl_bmpces_max = msl_bmpces_min;

	psc_poolmaster_init(&bmpce_poolmaster,
	    struct bmap_pagecache_entry, bmpce_lentry, PPMF_AUTO, 
	    msl_bmpces_min, msl_bmpces_min, msl_bmpces_max, 
	    bmpce_reap, "bmpce");
	bmpce_pool = psc_poolmaster_getmgr(&bmpce_poolmaster);

	msl_pgcache_init();

	psc_poolmaster_init(&bwc_poolmaster,
	    struct bmpc_write_coalescer, bwc_lentry, PPMF_AUTO, 64,
	    64, 0, NULL, "bwc");
	bwc_pool = psc_poolmaster_getmgr(&bwc_poolmaster);

}

void
bmap_pagecache_destroy(void)
{
	/* XXX destroy mmap()-based buffer as well */
	pfl_poolmaster_destroy(&bwc_poolmaster);
	pfl_poolmaster_destroy(&bmpce_poolmaster);
}

#if PFL_DEBUG > 0
void
dump_bmpce_flags(uint32_t flags)
{
	int seq = 0;

	PFL_PRFLAG(BMPCEF_DATARDY, &flags, &seq);
	PFL_PRFLAG(BMPCEF_FAULTING, &flags, &seq);
	PFL_PRFLAG(BMPCEF_TOFREE, &flags, &seq);
	PFL_PRFLAG(BMPCEF_EIO, &flags, &seq);
	PFL_PRFLAG(BMPCEF_AIOWAIT, &flags, &seq);
	PFL_PRFLAG(BMPCEF_DISCARD, &flags, &seq);
	PFL_PRFLAG(BMPCEF_READAHEAD, &flags, &seq);
	PFL_PRFLAG(BMPCEF_ACCESSED, &flags, &seq);
	PFL_PRFLAG(BMPCEF_IDLE, &flags, &seq);
	PFL_PRFLAG(BMPCEF_REAPED, &flags, &seq);
	PFL_PRFLAG(BMPCEF_READALC, &flags, &seq);
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
	PFL_PRFLAG(BIORQ_DIO, &flags, &seq);
	PFL_PRFLAG(BIORQ_EXPIRE, &flags, &seq);
	PFL_PRFLAG(BIORQ_DESTROY, &flags, &seq);
	PFL_PRFLAG(BIORQ_FLUSHRDY, &flags, &seq);
	PFL_PRFLAG(BIORQ_FREEBUF, &flags, &seq);
	PFL_PRFLAG(BIORQ_WAIT, &flags, &seq);
	PFL_PRFLAG(BIORQ_ONTREE, &flags, &seq);
	PFL_PRFLAG(BIORQ_READAHEAD, &flags, &seq);
	PFL_PRFLAG(BIORQ_AIOWAKE, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif
