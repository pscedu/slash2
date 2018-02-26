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
 *
 *
 * XXX used bmpce stuck hours after test in my last bigfile test.
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

#define			 MIN_FREE_PAGES		16	

struct psc_listcache	 msl_readahead_pages;

RB_GENERATE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
    bmpce_cmp)
RB_GENERATE(bmpc_biorq_tree, bmpc_ioreq, biorq_tentry, bmpc_biorq_cmp)

struct psc_listcache	 free_page_buffers;

int			 page_buffer_total;

struct psc_listcache	 bmpcLru;

int			 msl_bmpce_gen;

void
msl_pgcache_init(void)
{
	int i;
	struct bmap_page_entry *entry;

	lc_reginit(&free_page_buffers, struct bmap_page_entry,
	    page_lentry, "pagebuffers");

	/*
 	 * Note that ppm_max can change after we start.
 	 */
	entry = PSCALLOC(sizeof(struct bmap_page_entry) * bmpce_pool->ppm_max); 
	for (i = 0; i < bmpce_pool->ppm_max; i++) {
		psc_assert(entry);
		entry->page_flag = 0;
		page_buffer_total++;
		entry->page_buf = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);

		if (entry->page_buf == MAP_FAILED)
			psc_fatalx("Please raise vm.max_map_count limit");

		INIT_PSC_LISTENTRY(&entry->page_lentry);
		lc_add(&free_page_buffers, entry);
		entry++;
	}
}

struct bmap_page_entry *
msl_pgcache_get(int wait)
{
	struct timespec ts;
	struct bmap_page_entry *entry;
	static int warned = 0, failed = 0;

	entry = lc_getnb(&free_page_buffers);
	if (entry)
		goto out;
 again:

	LIST_CACHE_LOCK(&free_page_buffers);
	if (page_buffer_total < bmpce_pool->ppm_max) {
		entry = PSCALLOC(sizeof(struct bmap_page_entry)); 
		entry->page_buf = mmap(NULL, BMPC_BUFSZ, PROT_READ|PROT_WRITE, 
		    MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
		if (entry->page_buf != MAP_FAILED) {
			warned = 0;
			page_buffer_total++;
			OPSTAT_INCR("mmap-grow-ok");
			INIT_PSC_LISTENTRY(&entry->page_lentry);
			LIST_CACHE_ULOCK(&free_page_buffers);
			entry->page_flag = PAGE_CANFREE;
			return (entry);
		}
		failed = 1;
		PSCFREE(entry); 
		OPSTAT_INCR("mmap-grow-err");
	}
	LIST_CACHE_ULOCK(&free_page_buffers);

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
		entry = lc_gettimed(&free_page_buffers, &ts);
		if (!entry) {
			OPSTAT_INCR("pagecache-get-retry");
			goto again;
		}
	} else
		entry = lc_getnb(&free_page_buffers);

 out:
	if (entry)
		entry->page_flag |= PAGE_MADVISE;

	return (entry);
}

void
msl_pgcache_put(struct bmap_page_entry *entry)
{
	int rc;
	/*
 	 * Do not assume that the max value has not changed.
 	 */
	LIST_CACHE_LOCK(&free_page_buffers);
	if (page_buffer_total <= bmpce_pool->ppm_max) {
		if (entry->page_flag & PAGE_CANFREE)
			lc_addhead(&free_page_buffers, entry);
		else
			lc_addtail(&free_page_buffers, entry);
	} else {
		if (entry->page_flag & PAGE_CANFREE) {
			rc = munmap(entry->page_buf, BMPC_BUFSZ);
			if (!rc)
				OPSTAT_INCR("munmap-success");
			else
				OPSTAT_INCR("munmap-failure");
			PSCFREE(entry);
			page_buffer_total--;
		} else
			lc_add(&free_page_buffers, entry);
	}
	LIST_CACHE_ULOCK(&free_page_buffers);
}

int
msl_pgcache_reap(void)
{
	struct bmap_page_entry *entry, *tmp;
	int rc, nfree, didwork = 0;

	/* (gdb) p bmpce_pool.ppm_u.ppmu_explist.pexl_pll.pll_nitems */
	nfree = bmpce_pool->ppm_nfree; 
	if (bmpce_pool->ppm_nfree > bmpce_pool->ppm_min)
		didwork = 1;
	psc_pool_try_shrink(bmpce_pool, nfree);

	/* I tried the other way, but RSS wouldn't go down as much */
	if (bmpce_pool->ppm_nfree != bmpce_pool->ppm_total)
		return (didwork);
	/*
 	 * Do not assume that the max value has not changed.
 	 */
	LIST_CACHE_LOCK(&free_page_buffers);
	LIST_CACHE_FOREACH_SAFE(entry, tmp, &free_page_buffers) {
		if (entry->page_flag & PAGE_CANFREE) {
			lc_remove(&free_page_buffers, entry);
			rc = munmap(entry->page_buf, BMPC_BUFSZ);
			if (!rc)
				OPSTAT_INCR("munmap-success-reap");
			else
				OPSTAT_INCR("munmap-failure-reap");
			PSCFREE(entry);
			page_buffer_total--;
			continue;
		}
		if (!(entry->page_flag & PAGE_MADVISE))
			continue;
		rc = madvise(entry->page_buf, BMPC_BUFSZ, MADV_DONTNEED);
		if (!rc)
			OPSTAT_INCR("madvise-success-reap");
		else
			OPSTAT_INCR("madvise-failure-reap");
		entry->page_flag &= ~PAGE_MADVISE;
	}
	LIST_CACHE_ULOCK(&free_page_buffers);
	return (1);
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
	int rc = 0, wrlock = 0;
	struct bmap_pagecache_entry q, *e, *e2 = NULL;
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct bmap_pagecache *bmpc;
	struct bmap_page_entry *entry = NULL;
	struct timespec tm;

	bmpc = bmap_2_bmpc(b);
	q.bmpce_off = off;

 restart:

	if (wrlock)
		pfl_rwlock_wrlock(&bci->bci_rwlock);
	else
		pfl_rwlock_rdlock(&bci->bci_rwlock);

	for (;;) {
		e = RB_FIND(bmap_pagecachetree, &bmpc->bmpc_tree, &q);
		if (e) {
			if (flags & BMPCEF_READAHEAD) {
				rc = EEXIST;
				break;
			}
			BMPCE_LOCK(e);
			/*
			 * It is possible that the EIO flag can be cleared
			 * and the page is re-used now.
			 */
			if ((e->bmpce_flags & BMPCEF_EIO) ||
			    (e->bmpce_flags & BMPCEF_TOFREE) ||
			    (e->bmpce_flags & BMPCEF_DISCARD)) {
				BMPCE_ULOCK(e);
				pfl_rwlock_unlock(&bci->bci_rwlock);
				tm.tv_sec = 0;
				tm.tv_nsec = 100000;
				nanosleep(&tm, NULL);
				goto restart;
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
				if (e2 == NULL) {
					rc = EAGAIN;
					goto out;
				}
				entry = msl_pgcache_get(0);
				if (entry == NULL) {
					rc = EAGAIN;
					goto out;
				}
			} else {
				e2 = psc_pool_get(bmpce_pool);
				entry = msl_pgcache_get(1);
			}
			wrlock = 1;
			pfl_rwlock_wrlock(&bci->bci_rwlock);
			continue;
		} else {
			OPSTAT_INCR("msl.bmpce-cache-miss");

			e = e2;
			bmpce_init(e);
			e->bmpce_off = off;
			e->bmpce_ref = 1;
			e->bmpce_len = 0;
			e->bmpce_start = off;
			e->bmpce_waitq = wq;
			e->bmpce_flags = flags;
			e->bmpce_bmap = b;
			e->bmpce_entry = entry;

			e2 = NULL;
			entry = NULL;

			PSC_RB_XINSERT(bmap_pagecachetree,
			    &bmpc->bmpc_tree, e);

			bmap_op_start_type(b, BMAP_OPCNT_BMPCE);

			DEBUG_BMPCE(PLL_DIAG, e, "creating");
			break;
		}
	}
	pfl_rwlock_unlock(&bci->bci_rwlock);

 out:

	if (e2) {
		OPSTAT_INCR("msl.bmpce-gratuitous");
		if (entry)
			msl_pgcache_put(entry);
		psc_pool_return(bmpce_pool, e2);
	}

	if (!rc)
		psc_dynarray_add(&r->biorq_pages, e);

	DEBUG_BIORQ(PLL_DIAG, r, "registering bmpce@%p "
	    "n=%d foff=%"PRIx64" rc = %d", 
	    e, psc_dynarray_len(&r->biorq_pages), off + bmap_foff(b), rc);

	return (rc);
}

void
bmpce_free(struct bmap_pagecache_entry *e, struct bmap_pagecache *bmpc)
{
	struct bmap *b = bmpc_2_bmap(bmpc);
	struct bmap_cli_info *bci = bmap_2_bci(b);

	BMPCE_LOCK_ENSURE(e);

	psc_assert(e->bmpce_ref == 0);
	psc_assert(e->bmpce_flags & BMPCEF_TOFREE);

	DEBUG_BMPCE(PLL_DIAG, e, "destroying");

	if (e->bmpce_flags & BMPCEF_READAHEAD)
		OPSTAT_INCR("msl.readahead-waste");

	BMPCE_ULOCK(e);

	pfl_rwlock_wrlock(&bci->bci_rwlock);
	PSC_RB_XREMOVE(bmap_pagecachetree, &bmpc->bmpc_tree, e);
	pfl_rwlock_unlock(&bci->bci_rwlock);

	msl_pgcache_put(e->bmpce_entry);
	psc_pool_return(bmpce_pool, e);
}

void
bmpce_release_locked(struct bmap_pagecache_entry *e, struct bmap_pagecache *bmpc)
{
	struct bmap *b = e->bmpce_bmap;

	msl_bmpce_gen++;
	LOCK_ENSURE(&e->bmpce_lock);

	psc_assert(bmpc == bmap_2_bmpc(b));
	psc_assert(e->bmpce_ref > 0);
	e->bmpce_ref--;
	DEBUG_BMPCE(PLL_DIAG, e, "drop reference");
	if (e->bmpce_ref > 0) {
		BMPCE_ULOCK(e);
		return;
	}

	/* sanity checks */
	psc_assert(pll_empty(&e->bmpce_pndgaios));

	/*
 	 * This has the side effect of putting the page
 	 * to the end of the list.
 	 */
	if (e->bmpce_flags & BMPCEF_LRU) {
		e->bmpce_flags &= ~BMPCEF_LRU;
		pll_remove(&bmpc->bmpc_lru, e);
	}

	if ((e->bmpce_flags & BMPCEF_DATARDY) &&
	   !(e->bmpce_flags & BMPCEF_EIO) &&
	   !(e->bmpce_flags & BMPCEF_TOFREE) &&
	   !(e->bmpce_flags & BMPCEF_DISCARD)) { 
		DEBUG_BMPCE(PLL_DIAG, e, "put on LRU");

		e->bmpce_flags |= BMPCEF_LRU;

		/*
 		 * The other side must use trylock to
 		 * avoid a deadlock.
 		 */
		pll_add(&bmpc->bmpc_lru, e);

		BMPCE_ULOCK(e);
		if (bmpce_pool->ppm_nfree < MIN_FREE_PAGES) {
			OPSTAT_INCR("msl.bmpce-nfree-reap");
#if 0
			bmpce_reaper(bmpce_pool);
#else
			/* call msreapthr_main() */
			psc_waitq_wakeone(&sl_freap_waitq);
#endif
		}
		return;
	}

	e->bmpce_flags |= BMPCEF_TOFREE;

	bmpce_free(e, bmpc);
	bmap_op_done_type(b, BMAP_OPCNT_BMPCE);
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
		psc_waitq_waitrel_us(&f->fcmh_waitq, &f->fcmh_lock, 100);
		BMAP_LOCK(b);
	}
}

#define	PAGE_RECLAIM_BATCH	1

/* Called from psc_pool_reap() and msl_pgcache_reap() */
int
bmpce_reaper(struct psc_poolmgr *m)
{
	struct bmap *b;
	int i, nfreed, haswork;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *e;
	struct psc_thread *thr;
	struct psc_dynarray a = DYNARRAY_INIT;

	thr = pscthr_get();
	psc_dynarray_ensurelen(&a, PAGE_RECLAIM_BATCH);

 again:

	nfreed = 0;
	haswork = 0;
	LIST_CACHE_LOCK(&bmpcLru);
	LIST_CACHE_FOREACH(bmpc, &bmpcLru) {

		b = bmpc_2_bmap(bmpc);
		if (!pll_nitems(&bmpc->bmpc_lru))
			continue;

		haswork = 1;
		if (!BMAP_TRYLOCK(b))
			continue;
		bmap_op_start_type(b, BMAP_OPCNT_WORK);

		/*
 		 * Hold a list lock can cause deadlock and slow things down.
 		 * So do it in two loops.
 		 */
		PLL_LOCK(&bmpc->bmpc_lru);
		PLL_FOREACH(e, &bmpc->bmpc_lru) {
			if (!BMPCE_TRYLOCK(e))
				continue;

			if (e->bmpce_ref || e->bmpce_flags & BMPCEF_TOFREE) {
				DEBUG_BMPCE(PLL_DIAG, e, "non-zero ref, skip");
				BMPCE_ULOCK(e);
				continue;
			}
			e->bmpce_flags |= BMPCEF_TOFREE;
			BMPCE_ULOCK(e);

			psc_dynarray_add(&a, e);
			nfreed++;
			if (nfreed >= PAGE_RECLAIM_BATCH &&
			    nfreed >= psc_atomic32_read(&m->ppm_nwaiters))
				break;
		}
		PLL_ULOCK(&bmpc->bmpc_lru);
		DYNARRAY_FOREACH(e, i, &a) {
			BMPCE_LOCK(e);
			psc_assert(e->bmpce_flags & BMPCEF_LRU);
			pll_remove(&bmpc->bmpc_lru, e);
			e->bmpce_flags &= ~BMPCEF_LRU;
			bmpce_free(e, bmpc);
			bmap_op_done_type(b, BMAP_OPCNT_BMPCE);
		}
		psc_dynarray_reset(&a);

		bmap_op_done_type(b, BMAP_OPCNT_WORK);

		if (nfreed >= PAGE_RECLAIM_BATCH &&
		    nfreed >= psc_atomic32_read(&m->ppm_nwaiters))
			break;
	
	}
	LIST_CACHE_ULOCK(&bmpcLru);

	/*
	 * I have also tried to let all non PFL_THRT_FS and non
	 * MSTHRT_READAHEAD to work harder, to no avail.
	 */
	if (thr->pscthr_type == MSTHRT_REAP && 
	    m->ppm_nfree < MIN_FREE_PAGES && haswork) {
		pscthr_yield();
		OPSTAT_INCR("msl.reap-loop");
		goto again;
	}

	psc_dynarray_free(&a);
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
	    bmpce_reaper, "bmpce");
	bmpce_pool = psc_poolmaster_getmgr(&bmpce_poolmaster);

	msl_pgcache_init();

	psc_poolmaster_init(&bwc_poolmaster,
	    struct bmpc_write_coalescer, bwc_lentry, PPMF_AUTO, 64,
	    64, 0, NULL, "bwc");
	bwc_pool = psc_poolmaster_getmgr(&bwc_poolmaster);

	lc_reginit(&bmpcLru, struct bmap_pagecache, bmpc_lentry,
	    "bmpclru");
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
