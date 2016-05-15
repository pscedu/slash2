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

int			 msl_bmpces_min = 512;	 	/* 16MiB */
int			 msl_bmpces_max = 16384; 	/* 512MiB */

struct psc_listcache	 msl_idle_pages;
struct psc_listcache	 msl_readahead_pages;

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

	for (i = 0; i < msl_bmpces_min; i++) {
		p = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_SHARED, -1, 0);
		if (!p) {
			OPSTAT_INCR("mmap-failure");
			break;
		}
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

	p = lc_getnb(&page_buffers);
	if (p)
		return p;

	LIST_CACHE_LOCK(&page_buffers);
	if (page_buffers_count < msl_bmpces_max) {
		p = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_SHARED, -1, 0);
		if (p) {
			OPSTAT_INCR("mmap-success");
			page_buffers_count++;
			LIST_CACHE_ULOCK(&page_buffers);
			return (p);
		}
		OPSTAT_INCR("mmap-failure");
	}
	LIST_CACHE_ULOCK(&page_buffers);

	if (wait)
		p = lc_getwait(&page_buffers);
	else
		p = lc_getnb(&page_buffers);
	return (p);
}

void
msl_pgcache_put(void *p)
{
	int rc;

	LIST_CACHE_LOCK(&page_buffers);
	if (page_buffers_count > msl_bmpces_max) {
		rc = munmap(p, BMPC_BUFSZ);
		if (rc)
			OPSTAT_INCR("munmap-failure");
		else
			OPSTAT_INCR("munmap-success");
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
	static int count;		/* this assume one reaper */

	/* 
	 * We don't reap even if the number of free buffers keeps 
	 * growing. This greatly cuts down the mmap() calls.
	 */
	curr = lc_nitems(&page_buffers);
	if (!count || count != curr) {
		count = curr;
		return;
	}
	if (curr <= msl_bmpces_min)
		return;

	nfree = (curr - msl_bmpces_min)/2;
	if (!nfree)
		nfree = 1;
	for (i = 0; i < nfree; i++) {
		p = lc_getnb(&page_buffers);
		if (!p)
			break;
		rc = munmap(p, BMPC_BUFSZ);
		if (rc)
			OPSTAT_INCR("munmap-failure");
		else
			OPSTAT_INCR("munmap-success");
		LIST_CACHE_LOCK(&page_buffers);
		page_buffers_count--;
		LIST_CACHE_ULOCK(&page_buffers);
	}
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
_bmpce_lookup(const struct pfl_callerinfo *pci,
    __unusedx struct bmpc_ioreq *r, struct bmap *b, int flags,
    uint32_t off, struct psc_waitq *wq,
    struct bmap_pagecache_entry **ep)
{
	int remove_idle = 0, remove_readalc = 0, wrlock = 0;
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
			if (e->bmpce_flags & BMPCEF_EIO) {
				if (e->bmpce_flags & BMPCEF_READAHEAD) {
					e->bmpce_flags &= ~BMPCEF_EIO;
				} else {
					DEBUG_BMPCE(PLL_WARN, e,
					    "skipping an EIO page");
					OPSTAT_INCR("msl.bmpce-eio");
					BMPCE_ULOCK(e);

 retry:
					psc_waitq_waitrelf_us(
					    &b->bcm_fcmh->fcmh_waitq,
					    PFL_LOCKPRIMT_RWLOCK,
					    &bci->bci_rwlock, 100);
					if (wrlock)
						pfl_rwlock_wrlock(
						    &bci->bci_rwlock);
					else
						pfl_rwlock_rdlock(
						    &bci->bci_rwlock);
					continue;
				}
			}
			if (e->bmpce_flags & BMPCEF_TOFREE) {
				BMPCE_ULOCK(e);
				goto retry;
			}

			if (e->bmpce_ref == 1 &&
			    !(e->bmpce_flags & BMPCEF_REAPED)) {
				if (e->bmpce_flags &
				    BMPCEF_IDLE) {
					e->bmpce_flags &=
					    ~BMPCEF_IDLE;
					remove_idle = 1;
				} else if (e->bmpce_flags &
				    BMPCEF_READALC) {
					e->bmpce_flags &=
					    ~BMPCEF_READALC;
					remove_readalc = 1;
				} else
					e->bmpce_ref++;
			} else
				e->bmpce_ref++;
			DEBUG_BMPCE(PLL_DIAG, e,
			    "add reference");
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
			wrlock = 1;
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

	if (remove_idle) {
		DEBUG_BMPCE(PLL_DIAG, e, "removing from idle");
		lc_remove(&msl_idle_pages, e);
	} else if (remove_readalc) {
		DEBUG_BMPCE(PLL_DIAG, e, "removing from readalc");
		lc_remove(&msl_readahead_pages, e);
	}

	psc_dynarray_add(&r->biorq_pages, e);

	DEBUG_BIORQ(PLL_DIAG, r, "registering bmpce@%p "
	    "n=%d foff=%"PRIx64, e, psc_dynarray_len(&r->biorq_pages),
	    off + bmap_foff(b));

	*ep = e;
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
bmpce_release(struct bmap_pagecache_entry *e)
{
	LOCK_ENSURE(&e->bmpce_lock);

	psc_assert(e->bmpce_ref > 0);

	if (e->bmpce_ref == 1 && (e->bmpce_flags & (BMPCEF_DATARDY |
	    BMPCEF_EIO | BMPCEF_DISCARD)) == BMPCEF_DATARDY) {
		BMPCE_ULOCK(e);

		/*
		 * XXX Need to recheck flags after grabbing the lock.
		 */
		if ((e->bmpce_flags & (BMPCEF_READAHEAD |
		    BMPCEF_ACCESSED)) == BMPCEF_READAHEAD) {
			LIST_CACHE_LOCK(&msl_readahead_pages);
			BMPCE_LOCK(e);
			if (e->bmpce_ref == 1) {
				DEBUG_BMPCE(PLL_DIAG, e,
				    "add to readahead");
				lc_add(&msl_readahead_pages, e);
				e->bmpce_flags |= BMPCEF_READALC;
				BMPCE_ULOCK(e);
				LIST_CACHE_ULOCK(&msl_readahead_pages);
				return;
			}
			LIST_CACHE_ULOCK(&msl_readahead_pages);
		} else {
			LIST_CACHE_LOCK(&msl_idle_pages);
			BMPCE_LOCK(e);
			if (e->bmpce_ref == 1) {
				DEBUG_BMPCE(PLL_DIAG, e, "add to idle");
				lc_add(&msl_idle_pages, e);
				e->bmpce_flags |= BMPCEF_IDLE;
				BMPCE_ULOCK(e);
				LIST_CACHE_ULOCK(&msl_idle_pages);
				return;
			}
			LIST_CACHE_ULOCK(&msl_idle_pages);
		}
	}

	e->bmpce_ref--;
	DEBUG_BMPCE(PLL_DIAG, e, "drop reference");
	if (e->bmpce_ref > 0) {
		BMPCE_ULOCK(e);
		return;
	}

	/* sanity checks */
	psc_assert(pll_empty(&e->bmpce_pndgaios));

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

	psc_assert(RB_EMPTY(&bmpc->bmpc_new_biorqs));

	/* DIO rq's are allowed since no cached pages are involved. */
	if (!pll_empty(&bmpc->bmpc_pndg_biorqs)) {
		struct bmpc_ioreq *r;

		PLL_FOREACH(r, &bmpc->bmpc_pndg_biorqs)
			psc_assert(r->biorq_flags & BIORQ_DIO);
	}

 restart:
	/*
	 * Remove any LRU pages still associated with the bmap.
	 * Only readahead pages can be encountered here. If we
	 * don't treat readahead pages specially, this code ca
	 * go away some day.
	 */
	pfl_rwlock_wrlock(&bci->bci_rwlock);
	for (e = RB_MIN(bmap_pagecachetree, &bmpc->bmpc_tree); e;
	    e = next) {
		next = RB_NEXT(bmap_pagecachetree, &bmpc->bmpc_tree, e);

		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCEF_DISCARD;
		if (e->bmpce_flags & BMPCEF_REAPED) {
			BMPCE_ULOCK(e);
			pfl_rwlock_unlock(&bci->bci_rwlock);
			goto restart;
		}
		if (e->bmpce_flags & BMPCEF_IDLE) {
			DEBUG_BMPCE(PLL_DIAG, e, "removing from idle");
			lc_remove(&msl_idle_pages, e);
			bmpce_release(e);
		} else if (e->bmpce_flags & BMPCEF_READALC) {
			DEBUG_BMPCE(PLL_DIAG, e,
			    "removing from readalc");
			lc_remove(&msl_readahead_pages, e);
			bmpce_release(e);
		} else
			psc_fatalx("impossible");
	}
	pfl_rwlock_unlock(&bci->bci_rwlock);
}

void
bmpc_expire_biorqs(struct bmap_pagecache *bmpc)
{
	struct bmpc_ioreq *r;
	int wake = 0;

	PLL_FOREACH_BACKWARDS(r, &bmpc->bmpc_new_biorqs_exp) {
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
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(b);
	BMAP_LOCK_ENSURE(b);

	while (bmpc->bmpc_pndg_writes) {
		OPSTAT_INCR("msl.biorq-flush-wait");
		bmpc_expire_biorqs(bmpc);
		psc_waitq_waitrel_us(&bmpc->bmpc_waitq, &b->bcm_lock,
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

/*
 * Reap pages from the idle and/or readahead lists.  The readahead list
 * is only considered if we are desperate and unable to reap anything
 * else.  In such situations (i.e. when no pages are readily available),
 * no more readahead should be issued.
 */
__static int
bmpce_reap(struct psc_poolmgr *m)
{
	struct psc_dynarray a = DYNARRAY_INIT;
	struct bmap_pagecache_entry *e;
	int nfreed, i;

	if (m->ppm_flags & PPMF_DESPERATE)
		bmpce_reap_list(&a, &msl_readahead_pages,
		    BMPCEF_READALC, m);

	if (psc_dynarray_len(&a) < psc_atomic32_read(&m->ppm_nwaiters))
		bmpce_reap_list(&a, &msl_idle_pages, BMPCEF_IDLE, m);

	nfreed = psc_dynarray_len(&a);

	DYNARRAY_FOREACH(e, i, &a) {
		BMPCE_LOCK(e);
		bmpce_release(e);
	}

	psc_dynarray_free(&a);

	OPSTAT_ADD("msl.bmpce-reap", nfreed);

	return (nfreed);
}

void
bmpc_global_init(void)
{
	if (msl_pagecache_maxsize)
		msl_bmpces_max = msl_pagecache_maxsize / BMPC_BUFSZ;

	msl_pgcache_init();

	psc_poolmaster_init(&bmpce_poolmaster,
	    struct bmap_pagecache_entry, bmpce_lentry, PPMF_AUTO, 
	    msl_bmpces_min, msl_bmpces_min, msl_bmpces_max, 
	    bmpce_reap, "bmpce");
	bmpce_pool = psc_poolmaster_getmgr(&bmpce_poolmaster);

	psc_poolmaster_init(&bwc_poolmaster,
	    struct bmpc_write_coalescer, bwc_lentry, PPMF_AUTO, 64,
	    64, 0, NULL, "bwc");
	bwc_pool = psc_poolmaster_getmgr(&bwc_poolmaster);

	lc_reginit(&msl_idle_pages, struct bmap_pagecache_entry,
	    bmpce_lentry, "idlepages");
	lc_reginit(&msl_readahead_pages, struct bmap_pagecache_entry,
	    bmpce_lentry, "readapages");
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
