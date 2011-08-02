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

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/pfl.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/iostats.h"
#include "psc_util/log.h"
#include "psc_util/random.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "bmpc.h"
#include "buffer.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"

/* Flushing fs threads wait here for I/O completion. */
struct psc_waitq	msl_fhent_flush_waitq = PSC_WAITQ_INIT;

struct timespec		msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec		msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

struct pscrpc_nbreqset *ra_nbreqset; /* non-blocking set for RA's */

struct psc_iostats	msl_diord_stat;
struct psc_iostats	msl_diowr_stat;
struct psc_iostats	msl_rdcache_stat;
struct psc_iostats	msl_racache_stat;

void bmap_flush_resched(struct bmpc_ioreq *);

int
msl_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq * a = x;
	const struct bmpc_ioreq * b = y;

	if (a->biorq_off == b->biorq_off)
		/*
		 * Larger requests with the same start offset should
		 * have ordering priority.
		 */
		return (CMP(b->biorq_len, a->biorq_len));
	return (CMP(a->biorq_off, b->biorq_off));
}

static int msl_getra(struct msl_fhent *, int, int *);

#define MS_DEF_READAHEAD_PAGES 8

void
msl_bmpce_getbuf(struct bmap_pagecache_entry *bmpce)
{
	void *tmp;
	int locked;

	locked = reqlock(&bmpce->bmpce_lock);
	psc_assert(bmpce->bmpce_flags & BMPCE_GETBUF);
	psc_assert(!bmpce->bmpce_base);
	psc_assert(bmpce->bmpce_waitq);
	BMPCE_ULOCK(bmpce);

	tmp = bmpc_alloc();

	BMPCE_LOCK(bmpce);
	psc_assert(bmpce->bmpce_flags & BMPCE_GETBUF);
	bmpce->bmpce_base = tmp;
	bmpce->bmpce_flags &= ~BMPCE_GETBUF;
	psc_waitq_wakeall(bmpce->bmpce_waitq);
	ureqlock(&bmpce->bmpce_lock, locked);
}

/**
 * msl_biorq_build - Construct a request structure for an I/O issued on
 *	a bmap.
 * Notes: roff is bmap aligned.
 */
__static void
msl_biorq_build(struct bmpc_ioreq **newreq, struct bmapc_memb *b,
    struct msl_fhent *mfh, uint32_t roff, uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *bmpce, bmpce_search, *bmpce_new;
	uint32_t aoff = (roff & ~BMPC_BUFMASK); /* aligned, relative offset */
	uint32_t alen = len + (roff & BMPC_BUFMASK);
	uint64_t foff = roff + bmap_foff(b); /* filewise offset */
	int i, npages = 0, rbw = 0, maxpages, fetchpgs = 0, bkwdra = 0;
	uint64_t fsz = fcmh_getsize(mfh->mfh_fcmh);

	DEBUG_BMAP(PLL_INFO, b,
	    "adding req for (off=%u) (size=%u) (nbmpce=%d)", roff, len,
	    pll_nitems(&(bmap_2_bmpc(b)->bmpc_lru)));

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
	    "adding req for (off=%u) (size=%u)", roff, len);

	psc_assert(len);
	psc_assert((roff + len) <= SLASH_BMAP_SIZE);
	psc_assert(op == BIORQ_WRITE || op == BIORQ_READ);
	*newreq = r = psc_pool_get(slc_biorq_pool);

	bmpc_ioreq_init(r, roff, len, op, b, mfh);

	/* O_APPEND must be sent be sent via directio
	 */
//	if (mfh->mfh_oflags & O_APPEND)
//		r->biorq_flags |= BIORQ_APPEND | BIORQ_DIO;

	/* Take a ref on the bmap now so that it won't go away before
	 *   pndg IO's complete.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

	if (r->biorq_flags & BIORQ_DIO)
		/* The bmap is set to use directio, we may then skip
		 *   cache preparation.
		 */
		goto out;

	bmpc = bmap_2_bmpc(b);
	/* How many pages are needed to accommodate the request?
	 *   Determine and record whether RBW (read-before-write)
	 *   operations are needed on the first or last pages.
	 */
	if (roff & BMPC_BUFMASK && op == BIORQ_WRITE)
		rbw = BIORQ_RBWFP;

	npages = alen / BMPC_BUFSZ;

	if (alen % BMPC_BUFSZ) {
		npages++;
		if (op == BIORQ_WRITE) {
			if (npages == 1 && !rbw)
				rbw = BIORQ_RBWFP;
			else if (npages > 1)
				rbw |= BIORQ_RBWLP;
		}
	}

	psc_assert(npages <= BMPC_IOMAXBLKS);
	if (op == BIORQ_WRITE)
		maxpages = npages;
	else {
		/* Given the provided offset, determine the max pages
		 *    that could be acquired from this bmap.
		 */
		int rapages;

		/* First, query the read ahead struct in the mfh to
		 *   obtain rapages and ra direction.
		 */
		rapages = msl_getra(mfh, npages, &bkwdra);
		if (rapages) {
			int n;

			psc_assert(bkwdra == 0 || bkwdra == 1);
			n = bkwdra ? (aoff / BMPC_BLKSZ) :
				(SLASH_BMAP_SIZE - aoff) / BMPC_BLKSZ;
			/* Read ahead must be contained within this bmap.
			 */
			maxpages = MIN(rapages, n);
			if (!bkwdra) {
				/* Don't prefetch past EOF
				 */
				n = ((fsz - (bmap_foff(b) + roff)) /
				     BMPC_BLKSZ) +
					((fsz % BMPC_BLKSZ) ? 1 : 0);

				maxpages = MIN(maxpages, n);
			}
			if (maxpages < npages)
				maxpages = npages;

			DEBUG_BMAP(PLL_NOTIFY, b, "maxpages=%d npages=%d bkwdra=%d",
				   maxpages, npages, bkwdra);
		} else {
			maxpages = npages;
			bkwdra = 0;
		}
	}

	/* Lock the bmap's page cache and try to locate cached pages
	 *   which correspond to this request.
	 */
	i = 0;
	bmpce_new = NULL;
	BMPC_LOCK(bmpc);
	while (i < maxpages) {
		if (bkwdra)
			bmpce_search.bmpce_off = aoff +
			    ((npages - 1 - i) * BMPC_BUFSZ);
		else
			bmpce_search.bmpce_off = aoff + (i * BMPC_BUFSZ);

		// XXX make work for backward ra!
		spinlock(&mfh->mfh_lock);
		if (i >= npages && bmap_foff(b) +
		    bmpce_search.bmpce_off <= mfh->mfh_ra.mra_raoff) {
			freelock(&mfh->mfh_lock);
			i++;
			continue;
		}
		psclog_info("i=%d npages=%d raoff=%"PRIx64" bmpce_foff=%"PRIx64,
			  i, npages, mfh->mfh_ra.mra_raoff,
			  (off_t)(bmpce_search.bmpce_off + bmap_foff(b)));
		freelock(&mfh->mfh_lock);
 restart:
		bmpce = bmpce_lookup_locked(bmpc, r,
		    (bkwdra ? (aoff + ((npages - 1 - i) * BMPC_BUFSZ)) :
		     aoff + (i * BMPC_BUFSZ)),
		    (i < npages) ? NULL : &r->biorq_bmap->bcm_fcmh->fcmh_waitq);

		BMPCE_LOCK(bmpce);
		if (bmpce->bmpce_flags & BMPCE_INIT)
			fetchpgs++;

		DEBUG_BMPCE(PLL_INFO, bmpce,
		    "i=%d, npages=%d maxpages=%d aoff=%u aoff_search=%u",
		    i, npages, maxpages, aoff, bmpce_search.bmpce_off);

		if (i < npages) {
			if (bmpce->bmpce_flags & BMPCE_EIO) {
				/* Don't take on pages marked with EIO.
				 *   Go back and try again.
				 */
				BMPC_ULOCK(bmpc);
				DEBUG_BMPCE(PLL_WARN, bmpce,
				    "wait and retry for EIO to clear");
				psc_assert(bmpce->bmpce_waitq);
				BMPCE_WAIT(bmpce);
				BMPC_LOCK(bmpc);
				goto restart;
			}
			/* Increment the ref cnt via the lru mgmt
			 *   function for all pages needed to
			 *   fulfill the read and for ra pages
			 *   which need to be retrieved.
			 */
			bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
			psc_dynarray_add(&r->biorq_pages, bmpce);

		} else {
			DEBUG_BMPCE(PLL_INFO, bmpce,
			    "ra (npndg=%d) i=%d biorq_is_my_bmpce=%d raoff=%"PRIx64
			    " bmpce_foff=%"PRIx64, pll_nitems(&mfh->mfh_ra_bmpces), i,
			    biorq_is_my_bmpce(r, bmpce), mfh->mfh_ra.mra_raoff,
			    (off_t)(bmpce_search.bmpce_off + bmap_foff(b)));

			/* These are read-ahead bmpce's.  Only add
			 *   pages which have yet to be retrieved.
			 */
			if (bmpce->bmpce_flags & BMPCE_EIO) {
				/* Don't bother with RA of pages marked EIO.
				 */
				DEBUG_BMPCE(PLL_WARN, bmpce,
				    "no RA for EIO page");

			} else if (biorq_is_my_bmpce(r, bmpce)) {
				/* Other threads will block on the reada
				 *   completion.  The cb handler will decref
				 *   the bmpce.
				 */
				psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
				psc_assert(!(bmpce->bmpce_flags & BMPCE_EIO));
				psc_assert(!bmpce->bmpce_base);
				/* Stash the bmap pointer in 'owner'.
				 */
				bmpce->bmpce_owner = b;
				bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
				bmap_op_start_type(b, BMAP_OPCNT_READA);

				/*
				 * Place the bmpce into our private pll.
				 * This is done so that the ra thread
				 * may coalesce bmpces without sorting
				 * overhead.  In addition, the ra thread
				 * may now use the fh's ra factor for
				 * weighing bw (large requests) vs.
				 * latency (smaller requests).
				 */
				spinlock(&mfh->mfh_lock);
				pll_addtail(&mfh->mfh_ra_bmpces, bmpce);
				if (!(mfh->mfh_flags & MSL_FHENT_RASCHED)) {
					mfh->mfh_flags |= MSL_FHENT_RASCHED;
					freelock(&mfh->mfh_lock);
					lc_addtail(&bmapReadAheadQ, mfh);
				} else
					freelock(&mfh->mfh_lock);

			} else if (bmpce->bmpce_flags & BMPCE_LRU) {
				/*
				 * There's no official read op pending
				 * for this ra page so no read ref is
				 * taken.  The lru is adjusted in
				 * preparation for its possible use.
				 */
				psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);
				PFL_GETTIMESPEC(&bmpce->bmpce_laccess);
				pll_remove(&bmpc->bmpc_lru, bmpce);
				pll_add_sorted(&bmpc->bmpc_lru, bmpce,
				    bmpce_lrusort_cmp1);
			}
			spinlock(&mfh->mfh_lock);
			mfh->mfh_ra.mra_raoff = bmap_foff(b) +
				bmpce_search.bmpce_off;
			freelock(&mfh->mfh_lock);
		}
		BMPCE_ULOCK(bmpce);
		i++;
		if (bkwdra && !bmpce_search.bmpce_off)
			break;
	}
	BMPC_ULOCK(bmpc);

	if (unlikely(bmpce_new))
		psc_pool_return(bmpcePoolMgr, bmpce_new);

	psc_assert(psc_dynarray_len(&r->biorq_pages) == npages);

	maxpages = psc_dynarray_len(&r->biorq_pages);

	if (bkwdra)
		psc_dynarray_reverse(&r->biorq_pages);

	/*
	 * Pass1: Retrieve memory pages from the cache on behalf of our
	 * pages stuck in GETBUF.
	 */
	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);

		if (biorq_is_my_bmpce(r, bmpce) &&
		    (bmpce->bmpce_flags & BMPCE_GETBUF)) {
			uint32_t rfsz = fsz - bmap_foff(b);

			/* Increase the rdref cnt in preparation for any
			 *   RBW ops but only on new pages owned by this
			 *   page cache entry.  For now bypass
			 *   bmpce_handle_lru_locked() for this op.
			 */
			if (!i && (rbw & BIORQ_RBWFP) &&
			    (fsz > foff ||
			     /* If file ends in this page then fetch */
			     (rfsz > bmpce->bmpce_off &&
			      rfsz < bmpce->bmpce_off + BMPC_BLKSZ))) {
				    bmpce->bmpce_flags |= BMPCE_RBWPAGE;
				    psc_atomic16_inc(&bmpce->bmpce_rdref);
				    r->biorq_flags |= BIORQ_RBWFP;

			} else if ((i == (npages - 1) &&
				    (rbw & BIORQ_RBWLP)) &&
				   (fsz > (foff + len) ||
				    (rfsz > bmpce->bmpce_off &&
				     rfsz < bmpce->bmpce_off + BMPC_BLKSZ))) {
				bmpce->bmpce_flags |= BMPCE_RBWPAGE;
				psc_atomic16_inc(&bmpce->bmpce_rdref);
				r->biorq_flags |= BIORQ_RBWLP;
			}
			msl_bmpce_getbuf(bmpce);
		}
		BMPCE_ULOCK(bmpce);
	}

	/* Pass2: Sanity Check
	 */
	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);
		if (i < npages)
			psc_assert(bmpce->bmpce_off ==
			   aoff + (i * BMPC_BUFSZ));

		if (op == BIORQ_WRITE)
			psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
		else
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);

		//		if (biorq_is_my_bmpce(r, bmpce) &&
		//		    (bmpce->bmpce_flags & BMPCE_INIT)) {
		if (biorq_is_my_bmpce(r, bmpce)) {
			/* The page is my reponsibility, ensure a cache
			 *   block has been assigned.
			 */
			psc_assert(bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
			psc_assert(!(bmpce->bmpce_flags & BMPCE_EIO));
			bmpce->bmpce_flags &= ~BMPCE_INIT;

			if (op == BIORQ_READ)
				bmpce->bmpce_flags |= BMPCE_READPNDG;
		}
		BMPCE_ULOCK(bmpce);
		DEBUG_BMPCE(PLL_INFO, bmpce, "bmpce prep done");
	}
 out:
	DEBUG_BIORQ(PLL_NOTIFY, r, "new req (fetchpgs=%d)", fetchpgs);
	if (op == BIORQ_READ || (r->biorq_flags & BIORQ_DIO))
		pll_add(&bmap_2_bmpc(b)->bmpc_pndg_biorqs, r);
	else
		pll_add_sorted(&bmap_2_bmpc(b)->bmpc_new_biorqs, r, msl_biorq_cmp);
}

__static void
bmap_biorq_del(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);

	/* The request must be attached to the bmpc.
	 */
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_WRITE && !(r->biorq_flags & BIORQ_DIO)) {
		atomic_dec(&bmpc->bmpc_pndgwr);
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) >= 0);

		if (atomic_read(&bmpc->bmpc_pndgwr))
			psc_assert(b->bcm_flags & BMAP_DIRTY);

		else {
			b->bcm_flags &= ~BMAP_DIRTY;
			/* Don't assert pll_empty(&bmpc->bmpc_pndg)
			 *   since read requests may be present.
			 */
			DEBUG_BMAP(PLL_INFO, b, "unset DIRTY nitems_pndg(%d)",
			   pll_nitems(&bmpc->bmpc_pndg_biorqs));
		}
	}

	if (!(b->bcm_flags & (BMAP_CLI_FLUSHPROC|BMAP_TIMEOQ)) &&
	    (!bmpc_queued_ios(bmpc))) {
		psc_assert(!atomic_read(&bmpc->bmpc_pndgwr));
		b->bcm_flags |= BMAP_TIMEOQ;
		lc_addtail(&bmapTimeoutQ, b);
	}

	BMPC_ULOCK(bmpc);
	DEBUG_BMAP(PLL_INFO, b, "remove biorq=%p nitems_pndg(%d)",
		   r, pll_nitems(&bmpc->bmpc_pndg_biorqs));

	bmap_op_done_type(b, BMAP_OPCNT_BIORQ);
}

__static void
msl_biorq_unref(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(r->biorq_bmap);
	int i, eio;

	psc_assert(r->biorq_flags & BIORQ_DESTROY);
	psc_assert(!(r->biorq_flags & BIORQ_INFL));

	/* Block here on an of our EIO'd pages waiting for other threads
	 *   to release their references.
	 * Additionally, we need to block for RA pages which have not
	 *   yet been marked as EIO.
	 */
	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    (bmpce->bmpce_flags & BMPCE_EIO)) {

			while (psc_atomic16_read(&bmpce->bmpce_wrref) ||
			    psc_atomic16_read(&bmpce->bmpce_rdref) > 1) {
				BMPCE_WAIT(bmpce);
				BMPCE_LOCK(bmpce);
			}
			/* Only my rd ref should remain.
			 */
			psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref) &&
			    psc_atomic16_read(&bmpce->bmpce_rdref) == 1);
		}
		bmpce->bmpce_flags &= ~BMPCE_INFLIGHT;
		DEBUG_BMPCE(PLL_INFO, bmpce, "unset inflight");
		BMPCE_ULOCK(bmpce);
	}

	BMPC_LOCK(bmpc);
	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		/* bmpce with no reference will be freed by the reaper */
		BMPCE_LOCK(bmpce);
		eio = (bmpce->bmpce_flags & BMPCE_EIO) ? 1 : 0;
		bmpce_handle_lru_locked(bmpce, bmpc,
		    (r->biorq_flags & BIORQ_WRITE) ?
		    BIORQ_WRITE : BIORQ_READ, 0);
		if (!eio)
		    BMPCE_ULOCK(bmpce);
	}
	BMPC_ULOCK(bmpc);
}

__static void
msl_biorq_destroy(struct bmpc_ioreq *r)
{
	struct msl_fhent *f = r->biorq_fhent;
#if FHENT_EARLY_RELEASE
	int fhent = 1;
#endif

	spinlock(&r->biorq_lock);

	/* Reads req's have their BIORQ_SCHED and BIORQ_INFL flags
	 *    cleared in msl_read_cb to unblock waiting
	 *    threads at the earliest possible moment.
	 */
	if (!(r->biorq_flags & BIORQ_DIO)) {
		if (r->biorq_flags & BIORQ_WRITE) {
			if (r->biorq_flags & BIORQ_RBWFAIL)
				/*
				 * Ensure this biorq never got off of
				 * the ground.
				 */
				psc_assert(!(r->biorq_flags &
				     (BIORQ_INFL|BIORQ_SCHED)));
			else {
				psc_assert(r->biorq_flags & BIORQ_INFL);
				psc_assert(r->biorq_flags & BIORQ_SCHED);
				r->biorq_flags &= ~(BIORQ_INFL|BIORQ_SCHED);
			}
		} else
			psc_assert(!(r->biorq_flags &
			     (BIORQ_INFL|BIORQ_SCHED)));
	}

	r->biorq_flags |= BIORQ_DESTROY;

#if FHENT_EARLY_RELEASE
	if (r->biorq_flags & BIORQ_NOFHENT)
		fhent = 0;
#endif

	freelock(&r->biorq_lock);

	DEBUG_BIORQ(PLL_INFO, r, "destroying (nwaiters=%d)",
		    atomic_read(&r->biorq_waitq.wq_nwaiters));

	/* One last shot to wakeup any blocked threads.
	 */
	psc_waitq_wakeall(&r->biorq_waitq);

	msl_biorq_unref(r);
	bmap_biorq_del(r);

#if FHENT_EARLY_RELEASE
	if (fhent) {
		spinlock(&f->mfh_lock);
		pll_remove(&f->mfh_biorqs, r);
		psc_waitq_wakeall(&msl_fhent_flush_waitq);
		freelock(&f->mfh_lock);
	}
#else
	if (pll_conjoint(&f->mfh_biorqs, r)) {
		spinlock(&f->mfh_lock);
		pll_remove(&f->mfh_biorqs, r);
		psc_waitq_wakeall(&msl_fhent_flush_waitq);
		freelock(&f->mfh_lock);
	}
#endif

	psc_dynarray_free(&r->biorq_pages);

	if (r->biorq_rqset)
		pscrpc_set_destroy(r->biorq_rqset); /* XXX assert(#elem == 1) */

	while (atomic_read(&r->biorq_waitq.wq_nwaiters))
		sched_yield();
	//psc_assert(!atomic_read(&r->biorq_waitq.wq_nwaiters));

	psc_pool_return(slc_biorq_pool, r);
}

struct msl_fhent *
msl_fhent_new(struct fidc_membh *f)
{
	struct msl_fhent *mfh;

	fcmh_op_start_type(f, FCMH_OPCNT_OPEN);

	mfh = PSCALLOC(sizeof(*mfh));
	mfh->mfh_fcmh = f;
	INIT_SPINLOCK(&mfh->mfh_lock);
	pll_init(&mfh->mfh_biorqs, struct bmpc_ioreq, biorq_mfh_lentry,
	    &mfh->mfh_lock);
	pll_init(&mfh->mfh_ra_bmpces, struct bmap_pagecache_entry,
	    bmpce_ralentry, &mfh->mfh_lock);
	INIT_PSC_LISTENTRY(&mfh->mfh_lentry);
	MSL_RA_RESET(&mfh->mfh_ra);
	return (mfh);
}

void
bmap_biorq_expire(struct bmapc_memb *b)
{
	struct bmpc_ioreq *biorq;

	/*
	 * Note that the following two lists and the bmapc_memb
	 * structure itself all share the same lock.
	 */
	BMPC_LOCK(bmap_2_bmpc(b));
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_new_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_pndg_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}
	BMPC_ULOCK(bmap_2_bmpc(b));

	/* Minimize biorq scanning via this hint.
	 */
	BMAP_SETATTR(b, BMAP_CLI_BIORQEXPIRE);

	psc_waitq_wakeall(&bmapflushwaitq);
}

void
bmap_biorq_waitempty(struct bmapc_memb *b)
{
	BMAP_LOCK(b);
	bcm_wait_locked(b, (!pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs) ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs)  ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_ra)     ||
			    (b->bcm_flags & BMAP_CLI_FLUSHPROC)));

	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_ra));
	BMAP_ULOCK(b);
}

/**
 * msl_bmap_cache_rls - called from rcm.c (SRMT_BMAPDIO).
 * @b:  the bmap whose cached pages should be released.
 */
void
msl_bmap_cache_rls(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	//psc_assert(b->bcm_flags & BMAP_DIO);

	bmap_biorq_expire(b);
	bmap_biorq_waitempty(b);

	bmpc_lru_del(bmpc);

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}

void
msl_bmap_reap_init(struct bmapc_memb *bmap, const struct srt_bmapdesc *sbd)
{
	struct bmap_cli_info *bci = bmap_2_bci(bmap);
	int locked;

	psc_assert(!pfl_memchk(sbd, 0, sizeof(*sbd)));

	locked = BMAP_RLOCK(bmap);

	bci->bci_sbd = *sbd;
	/* Record the start time,
	 *  XXX the directio status of the bmap needs to be returned by
	 *	the MDS so we can set the proper expiration time.
	 */
	PFL_GETTIMESPEC(&bci->bci_xtime);

	timespecadd(&bci->bci_xtime, &msl_bmap_timeo_inc,
	    &bci->bci_etime);
	timespecadd(&bci->bci_xtime, &msl_bmap_max_lease,
	    &bci->bci_xtime);

	/*
	 * Take the reaper ref cnt early and place the bmap onto the
	 * reap list
	 */
	bmap->bcm_flags |= BMAP_TIMEOQ;
	bmap_op_start_type(bmap, BMAP_OPCNT_REAPER);

	BMAP_URLOCK(bmap, locked);
	/* Add ourselves here, otherwise zero length files
	 *   will not be removed.
	 */
	lc_addtail(&bmapTimeoutQ, bmap);
}

struct slashrpc_cservice *
msl_try_get_replica_res(struct bmapc_memb *bcm, int iosidx)
{
	struct slashrpc_cservice *csvc;
	struct bmap_cli_info *bci;
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *resm;

	fci = fcmh_2_fci(bcm->bcm_fcmh);
	bci = bmap_2_bci(bcm);

	DEBUG_BMAPOD(PLL_INFO, bcm, "iosidx=%d", iosidx);

	if (SL_REPL_GET_BMAP_IOS_STAT(bcm->bcm_repls,
	    iosidx * SL_BITS_PER_REPLICA) != BREPLST_VALID)
		return (NULL);

	res = libsl_id2res(fci->fci_reptbl[iosidx].bs_id);

	FOREACH_RND(&it, psc_dynarray_len(&res->res_members)) {
		resm = psc_dynarray_getpos(&res->res_members,
		    it.ri_rnd_idx);
		csvc = slc_geticsvc_nb(resm);
		if (csvc)
			return (csvc);
	}
	return (NULL);
}

/**
 * msl_bmap_to_csvc - Given a bmap, perform a series of lookups to
 *	locate the ION csvc.  The ION was chosen by the MDS and
 *	returned in the msl_bmap_retrieve routine.
 * @b: the bmap
 * @exclusive: whether to return connections to the specific ION the MDS
 *	told us to use instead of any ION in any IOS whose state is
 *	marked VALID for this bmap.
 * XXX: If the bmap is a read-only then any replica may be accessed (so
 *	long as it is recent).
 */
struct slashrpc_cservice *
msl_bmap_to_csvc(struct bmapc_memb *b, int exclusive)
{
	struct slashrpc_cservice *csvc;
	struct fcmh_cli_info *fci;
	struct psc_multiwait *mw;
	struct rnd_iterator it;
	struct sl_resm *resm;
	int i, locked;
	void *p;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	if (exclusive) {
		locked = BMAP_RLOCK(b);
		resm = libsl_nid2resm(bmap_2_ion(b));
		psc_assert(resm->resm_nid == bmap_2_ion(b));
		BMAP_URLOCK(b, locked);

		csvc = slc_geticsvc(resm);
		if (csvc)
			psc_assert(csvc->csvc_import->imp_connection->
			    c_peer.nid == bmap_2_ion(b));
		return (csvc);
	}

	fci = fcmh_get_pri(b->bcm_fcmh);
	mw = msl_getmw();
	for (i = 0; i < 2; i++) {
		psc_multiwait_reset(mw);
		psc_multiwait_entercritsect(mw);

		/* first, try preferred IOS */
		FOREACH_RND(&it, fci->fci_nrepls) {
			if (fci->fci_reptbl[it.ri_rnd_idx].bs_id != prefIOS)
				continue;
			csvc = msl_try_get_replica_res(b, it.ri_rnd_idx);
			if (csvc) {
				psc_multiwait_leavecritsect(mw);
				return (csvc);
			}
		}

		/* rats, not available; try anyone available now */
		FOREACH_RND(&it, fci->fci_nrepls) {
			if (fci->fci_reptbl[it.ri_rnd_idx].bs_id == prefIOS)
				continue;
			csvc = msl_try_get_replica_res(b,
			    it.ri_rnd_idx);
			if (csvc) {
				psc_multiwait_leavecritsect(mw);
				return (csvc);
			}
		}

		if (i)
			break;

		/*
		 * No connection was immediately available; wait a small
		 * amount of time to wait for any to come online.
		 */
		psc_multiwait_secs(mw, &p, 5);
	}
	psc_multiwait_leavecritsect(mw);
	return (NULL);
}

int
msl_add_async_req(struct pscrpc_request *rq,
    int (*cbf)(struct pscrpc_request *, int, struct pscrpc_async_args *),
    struct pscrpc_async_args *av)
{
	struct msl_aiorqcol *aiorqcol,
	    **aiorqcolp = av->pointer_arg[MSL_CBARG_AIORQCOL];
	struct bmap_pagecache_entry *bmpce;
	struct slc_async_req *car;
	struct psc_dynarray *a;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	int i;

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	m = libsl_nid2resm(rq->rq_peer.nid);

	aiorqcol = *aiorqcolp;
	if (aiorqcol == NULL) {
		aiorqcol = *aiorqcolp = psc_pool_get(slc_aiorqcol_pool);
		INIT_SPINLOCK(&aiorqcol->marc_lock);
		psc_waitq_init(&aiorqcol->marc_waitq);
		aiorqcol->marc_refcnt = 1;
		aiorqcol->marc_pfr = av->pointer_arg[MSL_CBARG_PFR];
		aiorqcol->marc_buf = av->pointer_arg[MSL_CBARG_BUF];
	} else {
		spinlock(&aiorqcol->marc_lock);
		aiorqcol->marc_refcnt++;
		freelock(&aiorqcol->marc_lock);
	}

	car = psc_pool_get(slc_async_req_pool);
	car->car_cbf = cbf;
	car->car_marc = *aiorqcolp;
	car->car_id = mp->id;
	car->car_buf = av->pointer_arg[MSL_CBARG_BUF];
	memcpy(&car->car_argv, av, sizeof(*av));
	lc_add(&resm2rmci(m)->rmci_async_reqs, car);

	if (cbf == msl_read_cb) {
		a = av->pointer_arg[MSL_CBARG_BMPCE];
		DYNARRAY_FOREACH(bmpce, i, a)
			BMPCE_SETATTR(bmpce, BMPCE_AIOWAIT, "set aio");
	}
	return (SLERR_AIOWAIT);
}

/**
 * msl_read_cb - RPC callback used only for read or RBW operations.
 *	The primary purpose is to set the bmpce's to DATARDY so that
 *	other threads waiting for DATARDY may be unblocked.
 *  Note: Unref of the biorq will happen after the pages have been
 *     copied out to the applicaton buffers.
 */
int
msl_read_cb(struct pscrpc_request *rq, int rc,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct psc_dynarray *a = args->pointer_arg[MSL_CBARG_BMPCE];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct bmap_pagecache_entry *bmpce;
	struct bmapc_memb *b;
	int clearpages = 0, i;

	b = r->biorq_bmap;

	psc_assert(a);
	psc_assert(b);

	if (rq)
		DEBUG_REQ(PLL_INFO, rq, "bmap=%p biorq=%p", b, r);
	DEBUG_BMAP(PLL_INFO, b, "callback");
	DEBUG_BIORQ(PLL_INFO, r, "callback bmap=%p", b);

	if (rc) {
		if (rq)
			DEBUG_REQ(PLL_ERROR, rq, "non-zero status %d", rc);
		DEBUG_BMAP(PLL_ERROR, b, "non-zero status %d", rc);
		DEBUG_BIORQ(PLL_ERROR, r, "non-zero status %d", rc);
		goto out;
	}

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);

	/* Call the inflight CB only on the iov's in the dynarray -
	 *   not the iov's in the request since some of those may
	 *   have already been staged in.
	 */
	DYNARRAY_FOREACH(bmpce, i, a) {
		BMPCE_LOCK(bmpce);

		psc_assert(bmpce->bmpce_waitq);
		psc_assert(biorq_is_my_bmpce(r, bmpce));

		if (bmpce->bmpce_flags & BMPCE_RBWPAGE) {
			/* The RBW stuff needs to be managed outside of
			 *   the LRU; this is not the best place but should
			 *   suffice for now.
			 */
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) == 1);
			psc_atomic16_dec(&bmpce->bmpce_rdref);
			bmpce->bmpce_flags |= BMPCE_RBWRDY;
			DEBUG_BMPCE(PLL_INFO, bmpce,
			    "infl dec for RBW, DATARDY not set");
		} else {
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			DEBUG_BMPCE(PLL_INFO, bmpce, "datardy via read_cb");
			psc_waitq_wakeall(bmpce->bmpce_waitq);
			bmpce->bmpce_waitq = NULL;
			bmpce->bmpce_owner = NULL;
		}

		bmpce->bmpce_flags &= ~BMPCE_AIOWAIT;

		if (clearpages) {
			DEBUG_BMPCE(PLL_WARN, bmpce, "clearing page");
			memset(bmpce->bmpce_base, 0, BMPC_BUFSZ);
		}
		BMPCE_ULOCK(bmpce);
	}
	freelock(&r->biorq_lock);

 out:
	if (rc) {
		/* Iterate over all pages in the request setting EIO.
		 *   Additionally, we must wake up any other threads
		 *   blocked on these pages.
		 */
		DYNARRAY_FOREACH(bmpce, i, a) {
			BMPCE_LOCK(bmpce);
			if (bmpce->bmpce_flags)
				bmpce->bmpce_flags &= ~BMPCE_AIOWAIT;
			bmpce->bmpce_flags |= BMPCE_EIO;
			DEBUG_BMPCE(PLL_WARN, bmpce, "set EIO");
			BMPCE_WAKE(bmpce);
			BMPCE_ULOCK(bmpce);
		}
	}
	/* Free the dynarray which was allocated in msl_read_rpc_launch().
	 */
	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);
	return (rc);
}

int
msl_read_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	psc_assert(rq->rq_reqmsg->opc == SRMT_READ);

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);
	if (rc == SLERR_AIOWAIT)
		return (msl_add_async_req(rq, msl_read_cb, args));
	return (msl_read_cb(rq, rc, args));
}

int
msl_readahead_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct bmap_pagecache_entry *bmpce, **bmpces = args->pointer_arg[MSL_CBARG_BMPCE];
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmap_pagecache *bmpc = args->pointer_arg[MSL_CBARG_BMPC];
	struct psc_waitq *wq = NULL;
	struct bmapc_memb *b;
	int rc, i;

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);

	if (rq)
		DEBUG_REQ(PLL_INFO, rq, "bmpces=%p", bmpces);

	BMPC_LOCK(bmpc);
	for (i = 0;; i++) {
		bmpce = bmpces[i];
		if (!bmpce)
			break;

		if (!i)
			b = bmpce->bmpce_owner;
		else
			psc_assert(b == bmpce->bmpce_owner);

		bmpce->bmpce_owner = NULL;

		DEBUG_BMPCE(rc ? PLL_ERROR : PLL_INFO, bmpce, "rc=%d", rc);
		DEBUG_BMAP(rc ? PLL_ERROR : PLL_INFO, b, "rc=%d", rc);

		pll_remove(&bmpc->bmpc_pndg_ra, bmpce);
		BMPCE_LOCK(bmpce);
		if (rc)
			bmpce->bmpce_flags |= BMPCE_EIO;
		else {
			bmpce->bmpce_flags &= ~BMPCE_AIOWAIT;
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			DEBUG_BMPCE(PLL_INFO, bmpce,
			    "datardy via readahead_cb");
			wq = bmpce->bmpce_waitq;
			bmpce->bmpce_waitq = NULL;
		}
		bmpce_handle_lru_locked(bmpce, bmpc, BIORQ_READ, 0);
		if (!rc)
			/* EIO's are always unlocked inside
			 *   bmpce_handle_lru_locked()
			 */
			BMPCE_ULOCK(bmpce);
	}
	BMPC_ULOCK(bmpc);

	if (wq)
		psc_waitq_wakeall(wq);

	sl_csvc_decref(csvc);

	BMAP_LOCK(b);
	if (!(b->bcm_flags & (BMAP_CLI_FLUSHPROC|BMAP_TIMEOQ)) &&
	    (!bmpc_queued_ios(bmpc))) {
		b->bcm_flags |= BMAP_TIMEOQ;
		lc_addtail(&bmapTimeoutQ, b);
	}
	BMAP_ULOCK(b);
	PSCFREE(bmpces);
	return (rc);
}

int
msl_write_rpcset_cb(__unusedx struct pscrpc_request_set *set, void *arg,
    int rc)
{
	struct psc_dynarray *biorqs = arg;
	struct bmpc_ioreq *r;
	int i;

	psclog_info("set=%p rc=%d", set, rc);

	if (rc) {
		DYNARRAY_FOREACH(r, i, biorqs)
			bmap_flush_resched(r);
		return (rc);
	}

	DYNARRAY_FOREACH(r, i, biorqs)
		msl_biorq_destroy(r);
	psc_dynarray_free(biorqs);
	PSCFREE(biorqs);
	return (rc);
}

int
msl_write_rpc_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct psc_dynarray *biorqs = args->pointer_arg[MSL_CBARG_BIORQS];
	struct bmpc_ioreq *r;
	int rc = 0, i;

	DEBUG_REQ(PLL_INFO, rq, "cb");

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);
	if (rc) {
		DYNARRAY_FOREACH(r, i, biorqs)
			bmap_flush_resched(r);
		return (rc);
	}

	DYNARRAY_FOREACH(r, i, biorqs)
		msl_biorq_destroy(r);
	psc_dynarray_free(biorqs);
	PSCFREE(biorqs);
	return (0);
}

int
msl_dio_cb(struct pscrpc_request *rq, int rc, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct srm_io_req *mq;
	int op;

	DEBUG_REQ(PLL_INFO, rq, "cb");

	if (rq == NULL)
		return (rc);

	op = rq->rq_reqmsg->opc;
	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	psc_assert(mq);

	DEBUG_REQ(PLL_DEBUG, rq,
	    "completed dio req (op=%d) off=%u sz=%u rc=%d",
	    op, mq->offset, mq->size, rc);

	sl_csvc_decref(csvc);
	return (rc);
}

int
msl_dio_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);
	if (rc == SLERR_AIOWAIT)
		return (msl_add_async_req(rq, msl_dio_cb, args));
	return (msl_dio_cb(rq, rc, args));
}

__static int
msl_pages_dio_getput(struct pscfs_req *pfr, struct bmpc_ioreq *r,
    char *b, struct msl_aiorqcol **aiorqcol)
{
	struct slashrpc_cservice  *csvc = NULL;
	struct pscrpc_request	  *rq = NULL;
	struct bmapc_memb	  *bcm;
	struct bmap_cli_info	  *bci;
	struct iovec		  *iovs;
	struct srm_io_req	  *mq;
	struct srm_io_rep	  *mp;

	size_t len, nbytes, size = r->biorq_len;
	int i, op, n = 0, rc = 1;

	psc_assert(r->biorq_flags & BIORQ_DIO);
	psc_assert(r->biorq_bmap);
	psc_assert(size);

	bcm = r->biorq_bmap;
	bci = bmap_2_bci(bcm);

	n = howmany(size, LNET_MTU);
	iovs = PSCALLOC(sizeof(*iovs) * n);

 retry:
	DEBUG_BIORQ(PLL_INFO, r, "dio req");

	op = r->biorq_flags & BIORQ_WRITE ? SRMT_WRITE : SRMT_READ;

	csvc = msl_bmap_to_csvc(bcm, op == SRMT_WRITE);
	if (csvc == NULL)
		goto error;

	if (r->biorq_rqset == NULL)
		r->biorq_rqset = pscrpc_prep_set();

	/*
	 * This buffer hasn't been segmented into LNET MTU sized
	 *  chunks.  Set up buffers into 1MB chunks or smaller.
	 */
	for (i = 0, nbytes = 0; i < n; i++, nbytes += len) {
		len = MIN(LNET_MTU, size - nbytes);

		rc = SL_RSX_NEWREQ(csvc, op, rq, mq, mp);
		if (rc)
			goto error;

		rq->rq_interpret_reply = msl_dio_cb0;
		rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BUF] = b + nbytes;
		rq->rq_async_args.pointer_arg[MSL_CBARG_AIORQCOL] = aiorqcol;
		rq->rq_async_args.pointer_arg[MSL_CBARG_PFR] = pfr;

		iovs[i].iov_base = b + nbytes;
		iovs[i].iov_len  = len;

		rc = rsx_bulkclient(rq, (op == SRMT_WRITE ?
		    BULK_GET_SOURCE : BULK_PUT_SINK), SRIC_BULK_PORTAL,
		    &iovs[i], 1);
		if (rc)
			goto error;

		mq->offset = r->biorq_off + nbytes;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		mq->flags |= SRM_IOF_DIO |
		    (r->biorq_flags & BIORQ_APPEND ? SRM_IOF_APPEND : 0);
		memcpy(&mq->sbd, &bci->bci_sbd, sizeof(mq->sbd));

		authbuf_sign(rq, PSCRPC_MSG_REQUEST);
		pscrpc_set_add_new_req(r->biorq_rqset, rq);
		if (pscrpc_push_req(rq)) {
			pscrpc_set_remove_req(r->biorq_rqset, rq);
			goto error;
		}
		sl_csvc_incref(csvc);
	}
	/* Should be no need for a callback since this call is fully
	 *   blocking.
	 */
	psc_assert(nbytes == size);
	rc = pscrpc_set_wait(r->biorq_rqset);
	pscrpc_set_destroy(r->biorq_rqset);
	r->biorq_rqset = NULL;

	PSCFREE(iovs);

	if (rc == SLERR_AIOWAIT) {
		/*
		 * async I/O registered by sliod; we must wait for a
		 * notification from him when it is ready.
		 */
		return (rc);
	}

	psc_iostats_intv_add((op == SRMT_WRITE ?
	    &msl_diowr_stat : &msl_diord_stat), size);

	msl_biorq_destroy(r);

	sl_csvc_decref(csvc);
	return (rc);

 error:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (r->biorq_rqset) {
		spinlock(&r->biorq_rqset->set_lock);
		if (psc_listhd_empty(&r->biorq_rqset->set_requests)) {
			pscrpc_set_destroy(r->biorq_rqset);
			r->biorq_rqset = NULL;
		} else
			freelock(&r->biorq_rqset->set_lock);
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}
	if (msl_offline_retry(r))
		goto retry;
	PSCFREE(iovs);
	return (-1);
}

__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);
	/* This req must already be attached to the cache.
	 *   The BIORQ_FLUSHRDY bit prevents the request
	 *   from being processed prematurely.
	 */
	spinlock(&r->biorq_lock);
	r->biorq_flags |= BIORQ_FLUSHRDY;
	DEBUG_BIORQ(PLL_INFO, r, "BIORQ_FLUSHRDY");
	psc_assert(psclist_conjoint(&r->biorq_lentry,
	    psc_lentry_hd(&r->biorq_lentry)));
	atomic_inc(&bmpc->bmpc_pndgwr);
	freelock(&r->biorq_lock);

	if (b->bcm_flags & BMAP_DIRTY) {
		/* If the bmap is already dirty then at least
		 *   one other writer must be present.
		 */
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 1);
		psc_assert((pll_nitems(&bmpc->bmpc_pndg_biorqs) +
			    pll_nitems(&bmpc->bmpc_new_biorqs)) > 1);

	} else {
		if (b->bcm_flags & BMAP_TIMEOQ) {
			LIST_CACHE_LOCK(&bmapTimeoutQ);
			b->bcm_flags &= ~BMAP_TIMEOQ;
			psc_assert(!(b->bcm_flags & BMAP_DIRTY));

			if (psclist_conjoint(&b->bcm_lentry,
			    &bmapTimeoutQ.plc_listhd))
				lc_remove(&bmapTimeoutQ, b);
			LIST_CACHE_ULOCK(&bmapTimeoutQ);
		}

		b->bcm_flags |= BMAP_DIRTY;

		if (!(b->bcm_flags & BMAP_CLI_FLUSHPROC)) {
			/* Give control of the msdb_lentry to the bmap_flush
			 *   thread.
			 */
			b->bcm_flags |= BMAP_CLI_FLUSHPROC;
			psc_assert(psclist_disjoint(&b->bcm_lentry));
			DEBUG_BMAP(PLL_INFO, b, "add to bmapFlushQ");
			lc_addtail(&bmapFlushQ, b);
		}
	}

	DEBUG_BMAP(PLL_INFO, b, "biorq=%p list_empty(%d)",
		   r, pll_empty(&bmpc->bmpc_pndg_biorqs));
	BMPC_ULOCK(bmpc);
	BMAP_ULOCK(b);
}

void
msl_reada_rpc_launch(struct bmap_pagecache_entry **bmpces, int nbmpce)
{
	struct bmap_pagecache_entry *bmpce, **bmpces_cbarg;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmapc_memb *b = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	uint32_t off = 0;
	int rc, i, added = 0;

	psc_assert(nbmpce > 0);
	psc_assert(nbmpce <= BMPC_MAXBUFSRPC);

	bmpces_cbarg = PSCALLOC((nbmpce + 1) * sizeof(void *));
	/* Terminate the array.
	 */
	bmpces_cbarg[nbmpce] = NULL;

	iovs = PSCALLOC(nbmpce * sizeof(*iovs));

	for (i=0; i < nbmpce; i++) {
		bmpce = bmpces_cbarg[i] = bmpces[i];
		psc_assert(!(bmpce->bmpce_flags & BMPCE_EIO));
		psc_assert(bmpce->bmpce_base);

		if (!i) {
			off = bmpce->bmpce_off;
			b = bmpce->bmpce_owner;
		} else
			psc_assert(b == bmpce->bmpce_owner);

		iovs[i].iov_base = bmpce->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;
	}

	csvc = msl_bmap_to_csvc(b, 0);
	if (csvc == NULL) {
		rc = ENOTCONN;
		goto error;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		goto error;
	//XXX adjust rpc timeout according to bci_xtime

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    nbmpce);
	if (rc)
		goto error;

	PSCFREE(iovs);

	mq->size = BMPC_BUFSZ * nbmpce;
	mq->op = SRMIOP_RD;
	mq->offset = off;
	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(mq->sbd));

	DEBUG_BMAP(PLL_NOTIFY, b, "launching read ahead req off=%u, npages=%d",
		   off, nbmpce);

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = bmpces_cbarg;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPC] = bmap_2_bmpc(b);
	rq->rq_interpret_reply = msl_readahead_cb;
	rq->rq_comp = &rpcComp;

	for (i = 0; i < nbmpce; i++) {
		/* bmpce_ralentry is available at this point, add
		 *   the ra to the pndg list before pushing it out the door.
		 */
		pll_addtail(&bmap_2_bmpc(b)->bmpc_pndg_ra, bmpces[i]);
		/* Once the bmpce's are on the bmpc_pndg_ra list they're
		 *   counted as pending I/O's.  Therefore the bmap won't
		 *   be freed prematurely if we drop ref here.
		 */
		bmap_op_done_type(b, BMAP_OPCNT_READA);
	}

	added = 1;

	rc = pscrpc_nbreqset_add(ra_nbreqset, rq);
	if (!rc)
		return;

 error:
	PSCFREE(iovs);
	PSCFREE(bmpces_cbarg);

	/* Deal with errored read ahead bmpce's.
	 */
	BMPC_LOCK(bmap_2_bmpc(b));
	for (i=0; i < nbmpce; i++) {
		bmpce = bmpces[i];

		if (added)
			pll_remove(&bmap_2_bmpc(b)->bmpc_pndg_ra,
			    bmpces);

		BMPCE_LOCK(bmpce);
		bmpce->bmpce_flags |= BMPCE_EIO;
		bmpce_handle_lru_locked(bmpce, bmap_2_bmpc(b),
		    BIORQ_READ, 0);
	}
	BMPC_ULOCK(bmap_2_bmpc(b));

	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		//pscrpc_abort_bulk(rq->rq_bulk);
		pscrpc_req_finished(rq);
	}
	if (csvc)
		sl_csvc_decref(csvc);
}

__static int
msl_read_rpc_launch(struct pscfs_req *pfr, void *bufp,
    struct bmpc_ioreq *r, int startpage, int npages,
    struct msl_aiorqcol **aiorqcol)
{
	struct slashrpc_cservice *csvc = NULL;
	struct bmap_pagecache_entry *bmpce;
	struct pscrpc_request *rq = NULL;
	struct psc_dynarray *a = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	uint32_t off = 0;
	int rc, i;

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i + startpage);

		BMPCE_LOCK(bmpce);

		psc_assert(biorq_is_my_bmpce(r, bmpce));
		psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
		DEBUG_BMPCE(PLL_INFO, bmpce, "adding to rpc");

		BMPCE_ULOCK(bmpce);

		iovs[i].iov_base = bmpce->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			off = bmpce->bmpce_off;
		psc_dynarray_add(a, bmpce);
	}

 retry:
	csvc = msl_bmap_to_csvc(r->biorq_bmap,
	    r->biorq_bmap->bcm_flags & BMAP_WR);
	if (csvc == NULL) {
		rc = -ENOTCONN;
		goto error;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		goto error;

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    npages);
	if (rc)
		goto error;

	mq->offset = off;
	mq->size = npages * BMPC_BUFSZ;
	mq->op = SRMIOP_RD;
	memcpy(&mq->sbd, bmap_2_sbd(r->biorq_bmap), sizeof(mq->sbd));

	/* Only this fsthr has access to the biorq so locking should
	 *   not be necessary.  BIORQ_INFL can't be set in the caller
	 *   since it's possible that no RPCs will be sent on behalf
	 *   this biorq.
	 */
	if (!(r->biorq_flags & BIORQ_INFL))
		r->biorq_flags |= BIORQ_INFL;

	DEBUG_BIORQ(PLL_DEBUG, r, "launching read req");

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	/* Setup the callback, supplying the dynarray as an argument.
	 */
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = a;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
	rq->rq_async_args.pointer_arg[MSL_CBARG_AIORQCOL] = aiorqcol;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BUF] = bufp;
	rq->rq_async_args.pointer_arg[MSL_CBARG_PFR] = pfr;

	if (!r->biorq_rqset)
		/* XXX Using a set for any type of read may be
		 *   overkill.
		 */
		r->biorq_rqset = pscrpc_prep_set();
	rq->rq_interpret_reply = msl_read_cb0;
	pscrpc_set_add_new_req(r->biorq_rqset, rq);

	rc = pscrpc_push_req(rq);
	if (rc)
		goto error;
	return (0);

 error:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}
	if (msl_offline_retry(r))
		goto retry;

	PSCFREE(a);
	PSCFREE(iovs);
	/* Two pass page cleanup.  First mark as EIO and wake
	 *   up our waiters.  Then remove the pages from the bmpc.
	 */
	BMPC_LOCK(bmap_2_bmpc(r->biorq_bmap));
	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i + startpage);
		/* Didn't get far enough for the waitq to be removed.
		 */
		psc_assert(bmpce->bmpce_waitq);

		BMPCE_LOCK(bmpce);
		bmpce->bmpce_flags |= BMPCE_EIO;
		BMPCE_WAKE(bmpce);
		BMPCE_ULOCK(bmpce);
	}
	BMPC_ULOCK(bmap_2_bmpc(r->biorq_bmap));

	return (rc);
}

__static int
msl_launch_read_rpcs(struct pscfs_req *pfr, void *bufp,
    struct bmpc_ioreq *r, int *psched, struct msl_aiorqcol **aiorqcol)
{
	struct bmap_pagecache_entry *bmpce;
	int rc = 0, i, j = -1;

	*psched = 0;

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		/* If readahead, biorq_getaligned_off needs to account
		 *    for the number of bmpce's inside biorq_pages.
		 */
		bmpce_usecheck(bmpce, BIORQ_READ,
		    biorq_getaligned_off(r, i));

		if (biorq_is_my_bmpce(r, bmpce))
			psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));

		BMPCE_ULOCK(bmpce);

		/* Try to set the start bmpce if it's not yet
		 *   assigned.
		 */
		if (j < 0) {
			if (biorq_is_my_bmpce(r, bmpce))
				j = i;
		} else {
			if (!biorq_is_my_bmpce(r, bmpce)) {
				rc = msl_read_rpc_launch(pfr, bufp, r,
				    j, i - j, aiorqcol);
				j = -1;
				*psched = 1;

			} else if ((i-j) == BMPC_MAXBUFSRPC) {
				rc = msl_read_rpc_launch(pfr, bufp, r,
				    j, i - j, aiorqcol);
				j = i;
				*psched = 1;
			}
		}
	}
	if (j >= 0) {
		/* Catch any unsent frags at the end of the array.
		 */
		rc = msl_read_rpc_launch(pfr, bufp, r, j, i - j,
		    aiorqcol);
		*psched = 1;
	}

	return (rc);
}

/**
 * msl_pages_prefetch - Launch read RPCs for pages that are owned by the
 *	given I/O request.  This function is called to perform a pure
 *	read request or a read-before-write for a write request.
 */
__static int
msl_pages_prefetch(struct pscfs_req *pfr, void *bufp,
    struct bmpc_ioreq *r, struct msl_aiorqcol **aiorqcol)
{
	int sched = 0, rc = 0, npages;
	struct bmap_pagecache_entry *bmpce;
	struct bmapc_memb *bcm;

	bcm    = r->biorq_bmap;
	npages = psc_dynarray_len(&r->biorq_pages);

	DEBUG_BIORQ(PLL_NOTIFY, r, "check prefetch");

	psc_assert(!r->biorq_rqset);

	BIORQ_LOCK(r);
	r->biorq_flags |= BIORQ_SCHED;
	BIORQ_ULOCK(r);

	/* Only read in the pages owned by this request.  To do this
	 *   the below loop marks only the iov slots which correspond
	 *   to page cache entries owned by this request as determined
	 *   by biorq_is_my_bmpce().
	 */
	if (r->biorq_flags & BIORQ_READ) {
		rc = msl_launch_read_rpcs(pfr, bufp, r, &sched, aiorqcol);

	} else { /* BIORQ_WRITE */
		int i;

		if ((r->biorq_flags & (BIORQ_RBWFP|BIORQ_RBWLP)) ==
		    (BIORQ_RBWFP|BIORQ_RBWLP)) {
			for (i = 0; i < 2; i++) {
				if (!i)
					bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
				else
					bmpce = psc_dynarray_getpos(&r->biorq_pages,
					    npages - 1);

				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));

				if (!i)
					rc = msl_read_rpc_launch(pfr,
					    bufp, r, 0, 1, aiorqcol);
				else
					rc |= msl_read_rpc_launch(pfr,
					    bufp, r, npages - 1, 1,
					    aiorqcol);
			}
			sched = 1;

		} else {
			if (r->biorq_flags & BIORQ_RBWFP) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages, 0);
				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_launch(pfr, bufp, r,
				    0, 1, aiorqcol);
				sched = 1;
			}
			if (r->biorq_flags & BIORQ_RBWLP) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages,
				    npages - 1);
				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_launch(pfr, bufp, r,
				    npages - 1, 1, aiorqcol);
				sched = 1;
			}
		}
	}

	if (rc || !sched) {
		BIORQ_LOCK(r);
		r->biorq_flags &= ~BIORQ_SCHED;
		BIORQ_ULOCK(r);
	}
	return (rc);
}

/**
 * msl_pages_blocking_load - Manage data prefetching activities.  This
 *	includes waiting on other threads to complete RPCs for data in
 *	which we're interested.
 */
__static int
msl_pages_blocking_load(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int rc = 0, i;

	if (r->biorq_rqset) {
		rc = pscrpc_set_wait(r->biorq_rqset);
		/*
		 * The set cb is not being used; msl_read_cb() is
		 *   called on every RPC in the set.  This was causing
		 *   the biorq to have its flags mod'd in an incorrect
		 *   fashion.  For now, the following lines will be moved
		 *   here.
		 */
		spinlock(&r->biorq_lock);
		if (rc != -SLERR_AIOWAIT)
			r->biorq_flags &= ~(BIORQ_INFL | BIORQ_SCHED);
		if (!rc) {
			r->biorq_flags &= ~(BIORQ_RBWLP | BIORQ_RBWFP);
			DEBUG_BIORQ(PLL_INFO, r, "read cb complete");
			psc_waitq_wakeall(&r->biorq_waitq);
		}
		freelock(&r->biorq_lock);
		/* Destroy and cleanup the set now.
		 */
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;

		/*
		 * By this point, the bmpce's in biorq_pages have been
		 * released.  Don't try to access them here.
		 */
		if (rc && rc != -SLERR_AIOWAIT)
			return (rc);
	}

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		BMPCE_LOCK(bmpce);
		DEBUG_BMPCE(PLL_INFO, bmpce, " ");

		if (!biorq_is_my_bmpce(r, bmpce)) {
			/* For pages not owned by this request,
			 *    wait for them to become DATARDY
			 *    or to have failed.
			 */
			while (!(bmpce->bmpce_flags &
			    (BMPCE_DATARDY | BMPCE_EIO | BMPCE_AIOWAIT))) {
				/*
				 * If the owner gave up, we will contend
				 * to retry after reacquiring the bmap
				 * lease.
				 */
				if (bmpce->bmpce_flags & BMPCE_AIOWAIT) {
					if (rc == 0)
						rc = -SLERR_AIOWAIT;
					break;
				}
				if (bmpce->bmpce_flags & BMPCE_EIO) {
					rc = -EAGAIN;
					break;
				}

				DEBUG_BMPCE(PLL_NOTIFY, bmpce, "waiting");
				psc_waitq_wait(bmpce->bmpce_waitq,
				    &bmpce->bmpce_lock);
				BMPCE_LOCK(bmpce);
			}
		}

		/* If this a read request OR another thread is dealing
		 *   with this bmpce then check.
		 */
		if ((r->biorq_flags & BIORQ_READ) ||
		    !biorq_is_my_bmpce(r, bmpce)) {
			/* If there was an error, retry or give up. */
			if (bmpce->bmpce_flags & BMPCE_EIO) {
				r->biorq_flags &= ~BIORQ_SCHED;
				// XXX may be redundant because of above
				rc = -EAGAIN;
			}

			if (rc == 0 && (bmpce->bmpce_flags & BMPCE_AIOWAIT))
				rc = -SLERR_AIOWAIT;

			/* Read requests must have had their bmpce's
			 *   put into DATARDY by now (i.e. all RPCs
			 *   must have already been completed).
			 *   Same goes for pages owned by other requests.
			 */
			psc_assert(bmpce->bmpce_flags &
			    (BMPCE_DATARDY | BMPCE_EIO | BMPCE_AIOWAIT));
		}

		BMPCE_ULOCK(bmpce);
	}
	return (rc);
}

/**
 * msl_pages_copyin - Copy user pages into buffer cache and schedule the
 *	slabs to be sent to the ION backend.
 * @r: array of request structs.
 * @buf: the source (application) buffer.
 */
__static size_t
msl_pages_copyin(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	uint32_t toff, tsize, nbytes;
	char *dest, *src;
	int i;

	src    = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;

	DYNARRAY_FOREACH(bmpce, i, &r->biorq_pages) {
		/* All pages are involved, therefore tsize should have value.
		 */
		psc_assert(tsize);

		/* Re-check RBW sanity.  The waitq pointer within the bmpce
		 *   must still be valid in order for this check to work.
		 */
		BMPCE_LOCK(bmpce);
		if (bmpce->bmpce_flags & BMPCE_RBWPAGE) {
			psc_assert(bmpce->bmpce_flags & BMPCE_RBWRDY);
			psc_assert(biorq_is_my_bmpce(r, bmpce));
		}

		/* Set the starting buffer pointer into
		 *  our cache vector.
		 */
		dest = bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			/* The first cache buffer pointer may need
			 *    a bump if the request offset is unaligned.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
				       (toff & ~BMPC_BUFMASK));
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			dest += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else {
			bmpce_usecheck(bmpce, BIORQ_WRITE, toff);
			nbytes = MIN(BMPC_BUFSZ, tsize);
		}

		DEBUG_BMPCE(PLL_NOTIFY, bmpce, "tsize=%u nbytes=%u toff=%u",
			    tsize, nbytes, toff);
		BMPCE_ULOCK(bmpce);
		/* Do the deed.
		 */
		memcpy(dest, src, nbytes);
		/* If the bmpce belongs to this request and is not yet
		 *   DATARDY (ie wasn't an RBW block) then set DATARDY
		 *   and wakeup anyone who was blocked.  Note the waitq
		 *   pointer will hang around until the request has completed.
		 * Note:  wrrefs are held until the
		 */
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    !(bmpce->bmpce_flags & BMPCE_DATARDY)) {
			psc_assert(bmpce->bmpce_owner);
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			bmpce->bmpce_flags &= ~(BMPCE_RBWPAGE|BMPCE_RBWRDY);
			psc_waitq_wakeall(bmpce->bmpce_waitq);
			bmpce->bmpce_waitq = NULL;
			bmpce->bmpce_owner = NULL;
		}
		BMPCE_ULOCK(bmpce);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	/* Queue these iov's for transmission to IOS.
	 */
	msl_pages_schedflush(r);

	return (r->biorq_len);
}

/**
 * msl_pages_copyout - Copy pages to the user application buffer.
 */
size_t
msl_pages_copyout(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	size_t nbytes, tbytes=0, rflen;
	int i, npages, tsize;
	char *dest, *src;
	off_t toff;

	dest   = buf;
	toff   = r->biorq_off;

	rflen  = fcmh_getsize(r->biorq_bmap->bcm_fcmh) -
		bmap_foff(r->biorq_bmap);

	if (biorq_voff_get(r) > rflen) {
		/* The request goes beyond EOF.
		 */
		tsize = rflen - r->biorq_off;
	} else
		tsize = r->biorq_len;

	DEBUG_BIORQ(PLL_INFO, r, "tsize=%d biorq_len=%u biorq_off=%u",
		    tsize, r->biorq_len, r->biorq_off);

	if (!tsize || tsize < 0)
		return (0);

	npages = psc_dynarray_len(&r->biorq_pages);

	psc_assert(npages);

	/* Due to page prefetching, the pages contained in
	 *   biorq_pages may exceed the requested len.
	 */
	for (i = 0; i < npages && tsize; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(bmpce);
		src = bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			src += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		DEBUG_BMPCE(PLL_INFO, bmpce,
		    "tsize=%u nbytes=%zu toff=%"PSCPRIdOFFT,
		    tsize, nbytes, toff);

		psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);
		bmpce_usecheck(bmpce, BIORQ_READ, biorq_getaligned_off(r, i));

		memcpy(dest, src, nbytes);

		if (bmpce->bmpce_flags & BMPCE_READA)
			psc_iostats_intv_add(&msl_racache_stat, nbytes);

		if (!biorq_is_my_bmpce(r, bmpce))
			psc_iostats_intv_add(&msl_rdcache_stat, nbytes);
//		else
//			bmpce->bmpce_owner = NULL;

		BMPCE_ULOCK(bmpce);

		toff   += nbytes;
		dest   += nbytes;
		tbytes += nbytes;
		tsize  -= nbytes;
	}
	psc_assert(!tsize);
	msl_biorq_destroy(r);

	return (tbytes);
}

static int
msl_getra(struct msl_fhent *mfh, int npages, int *bkwd)
{
	int rapages=0;

	spinlock(&mfh->mfh_lock);

	if (mfh->mfh_ra.mra_nseq > 0) {
		psc_assert(mfh->mfh_ra.mra_bkwd == 0 ||
			   mfh->mfh_ra.mra_bkwd == 1);
		rapages = MIN(npages * mfh->mfh_ra.mra_nseq,
			      MS_READAHEAD_MAXPGS);
	}

	*bkwd = mfh->mfh_ra.mra_bkwd;

	DEBUG_FCMH(PLL_NOTIFY, mfh->mfh_fcmh, "rapages=%d bkwd=%d",
		   rapages, *bkwd);

	freelock(&mfh->mfh_lock);
	return (rapages);
}

static void
msl_setra(struct msl_fhent *mfh, size_t size, off_t off)
{
	spinlock(&mfh->mfh_lock);

	if (mfh->mfh_ra.mra_nrios) {
		switch (mfh->mfh_ra.mra_bkwd) {
		case -1: /* not yet determined */
			if (mfh->mfh_ra.mra_loff == (off_t)(off + size)) {
				mfh->mfh_ra.mra_bkwd = 1;
				mfh->mfh_ra.mra_nseq++;

			} else if ((mfh->mfh_ra.mra_loff + mfh->mfh_ra.mra_lsz)
				   == off) {
				mfh->mfh_ra.mra_bkwd = 0;
				mfh->mfh_ra.mra_nseq++;
			}
			break;

		case 0: /* forward read mode */
			if ((mfh->mfh_ra.mra_loff + mfh->mfh_ra.mra_lsz) == off)
				mfh->mfh_ra.mra_nseq++;
			else
				MSL_RA_RESET(&mfh->mfh_ra);
			break;

		case 1:
			if (mfh->mfh_ra.mra_loff == (off_t)(off + size))
				mfh->mfh_ra.mra_nseq++;
			else
				MSL_RA_RESET(&mfh->mfh_ra);
			break;
		default:
			psc_fatalx("invalid value (%d)", mfh->mfh_ra.mra_bkwd);
		}
	}
	mfh->mfh_ra.mra_loff = off;
	mfh->mfh_ra.mra_lsz = size;
	mfh->mfh_ra.mra_nrios++;

	freelock(&mfh->mfh_lock);
}

void
msl_aiorqcol_finish(struct slc_async_req *car, ssize_t rc, size_t len)
{
	struct msl_aiorqcol *aiorqcol;
	void *buf;

	aiorqcol = car->car_marc;
	spinlock(&aiorqcol->marc_lock);
	if (aiorqcol->marc_rc == 0)
		aiorqcol->marc_rc = rc;
	aiorqcol->marc_len += len;
	if (--aiorqcol->marc_refcnt) {
		freelock(&aiorqcol->marc_lock);
		return;
	}

	while ((aiorqcol->marc_flags & MARCF_DONE) == 0) {
		psc_waitq_wait(&aiorqcol->marc_waitq,
		    &aiorqcol->marc_lock);
		spinlock(&aiorqcol->marc_lock);
	}

	rc = aiorqcol->marc_rc;
	buf = aiorqcol->marc_buf;
	pscfs_reply_read(aiorqcol->marc_pfr, buf, aiorqcol->marc_len,
	    -abs(rc));
	psc_pool_return(slc_aiorqcol_pool, aiorqcol);
	PSCFREE(buf);
}

#define MSL_BIORQ_COMPLETE	((void *)0x1)

/**
 * msl_io - I/O gateway routine which bridges pscfs and the SLASH2
 *	client cache and backend.  msl_io() handles the creation of
 *	biorq's and the loading of bmaps (which are attached to the
 *	file's fcmh and is ultimately responsible for data being
 *	prefetched (as needed), copied into or from the cache, and (on
 *	write) being pushed to the correct I/O server.
 * @pfr: file system request, used for tracking potentially asynchronous
 *	activity.
 * @mfh: file handle structure passed to us by pscfs which contains the
 *	pointer to our fcmh.
 * @buf: the application source/dest buffer.
 * @size: size of buffer.
 * @off: file logical offset similar to pwrite().
 * @rw: the operation type (SL_READ or SL_WRITE).
 */
int
msl_io(struct pscfs_req *pfr, struct msl_fhent *mfh, char *buf,
    const size_t size, const off_t off, enum rw rw)
{
	struct msl_aiorqcol *aiorqcol = NULL;
	struct bmpc_ioreq *r[MAX_BMAPS_REQ];
	struct bmapc_memb *b, *bref = NULL;
	size_t s, e, tlen, tsize;
	int nr, i, rc;
	uint64_t fsz;
	off_t roff;
	char *p;

	memset(r, 0, sizeof(r));

	psc_assert(mfh);
	psc_assert(mfh->mfh_fcmh);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
	    "buf=%p size=%zu off=%"PRId64" rw=%d",
	    buf, size, off, rw);

	if (rw == SL_READ)
		msl_setra(mfh, size, off);

	/*
	 * Get the start and end block regions from the input
	 * parameters.
	 */
	s = off / SLASH_BMAP_SIZE;
	e = ((off + size) - 1) / SLASH_BMAP_SIZE;
	nr = e - s + 1;
	if (nr > MAX_BMAPS_REQ) {
		rc = -EINVAL;
		goto out;
	}

 restart:
	rc = 0;

	tsize = size;
	FCMH_LOCK(mfh->mfh_fcmh);
	fsz = fcmh_getsize(mfh->mfh_fcmh);

	if (!size || (rw == SL_READ && off >= (off_t)fsz)) {
		FCMH_ULOCK(mfh->mfh_fcmh);
		goto out;
	}
	/* Catch read ops which extend beyond EOF.
	 */
	if ((rw == SL_READ) && ((tsize + off) > fsz))
		tsize = fsz - off;

	/*
	 * All I/O's block here for pending truncate requests.
	 *
	 * XXX there is a race here.  We should set CLI_TRUNC ourselves
	 * until we are done setting up the I/O to block intervening
	 * truncates.
	 */
	fcmh_wait_locked(mfh->mfh_fcmh,
	    mfh->mfh_fcmh->fcmh_flags & FCMH_CLI_TRUNC);
	FCMH_ULOCK(mfh->mfh_fcmh);

	/* Relativize the length and offset (roff is not aligned). */
	roff  = off - (s * SLASH_BMAP_SIZE);
	psc_assert(roff < SLASH_BMAP_SIZE);

	/* Length of the first bmap request. */
	tlen  = MIN(SLASH_BMAP_SIZE - (size_t)roff, tsize);

	/*
	 * Foreach block range, get its bmap and make a request into its
	 *  page cache.  This first loop retrieves all the pages.
	 */
	for (i = 0; i < nr; i++) {
		if (r[i])
			goto load_next;

		DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
		    "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%d",
		    tsize, tlen, off, roff, rw);

		psc_assert(tsize);

 retry_bmap:
		/*
		 * Load up the bmap; if it's not available then we're
		 * out of luck because we have no idea where the data
		 * is!
		 */
		rc = bmap_get(mfh->mfh_fcmh, s + i, rw, &b);
		if (rc) {
			DEBUG_FCMH(PLL_ERROR, mfh->mfh_fcmh,
			    "bno=%zd sz=%zu tlen=%zu "
			    "off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT" "
			    "rw=%d rc=%d",
			    s + i, tsize, tlen, off, roff, rw, rc);
			if (msl_fd_offline_retry(mfh))
				goto retry_bmap;
			switch (abs(rc)) {
//			case SLERR_BADCRC:
//				rc = EIO;
//				break;
			case SLERR_ION_OFFLINE:
				rc = -EHOSTUNREACH;
				break;
			}
			goto out;
		}

		msl_bmap_lease_tryext(b);
		/*
		 * Re-relativize the offset if this request spans more
		 * than 1 bmap.
		 */
		msl_biorq_build(&r[i], b, mfh,
		    roff - (i * SLASH_BMAP_SIZE), tlen,
		    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);

		/*
		 * If we are not doing direct I/O, launch read for read
		 * requests and pre-read for unaligned write requests.
		 */
		if (!(r[i]->biorq_flags & BIORQ_DIO) &&
		    (r[i]->biorq_flags &
		      (BIORQ_READ | BIORQ_RBWFP | BIORQ_RBWLP))) {
			rc = msl_pages_prefetch(pfr, p, r[i], &aiorqcol);
			if (rc) {
				rc = msl_offline_retry_ignexpire(r[i]);
				r[i]->biorq_flags |= BIORQ_RBWFAIL;
				msl_biorq_destroy(r[i]);
				r[i] = NULL;
				if (rc)
					goto retry_bmap;
				rc = -EIO;
				goto out;
			}
		}

		BMAP_CLI_BUMP_TIMEO(b);

 load_next:
		roff += tlen;
		tsize -= tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);
	}

	/*
	 * Note that the offsets used here are file-wise offsets not
	 * offsets into the buffer.
	 */
	for (i = 0, tlen = 0, tsize = 0, p = buf;
	    i < nr; i++, p += tlen) {
		if (r[i] == MSL_BIORQ_COMPLETE)
			continue;

		/* Associate the biorq's with the mfh. */
		pll_addtail(&mfh->mfh_biorqs, r[i]);

		tlen = r[i]->biorq_len;

		if (r[i]->biorq_flags & BIORQ_DIO) {
			rc = msl_pages_dio_getput(pfr, r[i], p, &aiorqcol);
			if (rc == SLERR_AIOWAIT)
				goto next_ioreq;
			if (rc) {
				pll_remove(&mfh->mfh_biorqs, r[i]);
				rc = msl_offline_retry_ignexpire(r[i]);
				if (rc) {
					msl_biorq_destroy(r[i]);
					r[i] = NULL;
					goto restart;
				}
				rc = -EIO;
				goto out;
			}
		} else {
			/* Block for page fault completion by this or
			 *   other threads which may be working on pages
			 *   which we need.
			 */
			rc = msl_pages_blocking_load(r[i]);
			if (rc == -SLERR_AIOWAIT)
				goto next_ioreq;
			if (rc) {
				rc = msl_offline_retry(r[i]);
				if (rc) {
					/*
					 * The app wants to retry the
					 * failed I/O.  What we must do
					 * in this logic is tricky since
					 * we don't want to re-lease the
					 * bmap.  We hold a fake ref to
					 * the bmap so it doesn't get
					 * reclaimed until bmap_get()
					 * gets its own ref.
					 */
					if (bref)
						bmap_op_done_type(bref,
						    BMAP_OPCNT_BIORQ);
					bref = r[i]->biorq_bmap;
					bmap_op_start_type(bref,
					    BMAP_OPCNT_BIORQ);
				} else
					rc = msl_offline_retry_ignexpire(r[i]);
				if (rc) {
					r[i]->biorq_flags |= BIORQ_RBWFAIL;
					msl_biorq_destroy(r[i]);
					r[i] = NULL;
					goto restart;
				}
				rc = -EIO;
				goto out;
			}

			if (rw == SL_READ)
				tlen = msl_pages_copyout(r[i], p);
			else
				tlen = msl_pages_copyin(r[i], p);
		}
		tsize += tlen;

 next_ioreq:
		r[i] = MSL_BIORQ_COMPLETE;
	}

	if (aiorqcol) {
		spinlock(&aiorqcol->marc_lock);
		aiorqcol->marc_flags |= MARCF_DONE;
		psc_waitq_wakeall(&aiorqcol->marc_waitq);
		freelock(&aiorqcol->marc_lock);
		rc = -SLERR_AIOWAIT;
	} else
		rc = tsize;

	if (rw == SL_WRITE)
		fcmh_setlocalsize(mfh->mfh_fcmh, off + size);

 out:
	if (bref)
		bmap_op_done_type(bref, BMAP_OPCNT_BIORQ);

	for (i = 0; i < nr; i++)
		if (r[i] && r[i] != MSL_BIORQ_COMPLETE)
			msl_biorq_destroy(r[i]);
	return (rc);
}
