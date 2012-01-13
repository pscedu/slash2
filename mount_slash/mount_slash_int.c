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
#include "pfl/fsmod.h"
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

struct pscrpc_nbreqset *pndgReadaReqs; /* non-blocking set for RA's */

struct psc_iostats	msl_diord_stat;
struct psc_iostats	msl_diowr_stat;
struct psc_iostats	msl_rdcache_stat;
struct psc_iostats	msl_racache_stat;

struct psc_iostats	msl_io_1b_stat;
struct psc_iostats	msl_io_1k_stat;
struct psc_iostats	msl_io_4k_stat;
struct psc_iostats	msl_io_16k_stat;
struct psc_iostats	msl_io_64k_stat;
struct psc_iostats	msl_io_128k_stat;
struct psc_iostats	msl_io_512k_stat;
struct psc_iostats	msl_io_1m_stat;

static int msl_getra(struct msl_fhent *, int, int *);

#define MS_DEF_READAHEAD_PAGES 8

/**
 * msl_biorq_build - Construct a request structure for an I/O issued on
 *	a bmap.
 * Notes: roff is bmap aligned.
 */
__static void
msl_biorq_build(struct bmpc_ioreq **newreq, struct bmapc_memb *b,
	struct msl_fhent *mfh, struct msl_fsrqinfo *q, uint32_t roff,
	uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *e, bmpce_search, *bmpce_new;
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

	if (len < 1024)
		psc_iostats_intv_add(&msl_io_1b_stat, 1);
	else if (len < 4096)
		psc_iostats_intv_add(&msl_io_1k_stat, 1);
	else if (len < 16386)
		psc_iostats_intv_add(&msl_io_4k_stat, 1);
	else if (len < 65536)
		psc_iostats_intv_add(&msl_io_16k_stat, 1);
	else if (len < 131072)
		psc_iostats_intv_add(&msl_io_64k_stat, 1);
	else if (len < 524288)
		psc_iostats_intv_add(&msl_io_128k_stat, 1);
	else if (len < 1048576)
		psc_iostats_intv_add(&msl_io_512k_stat, 1);
	else
		psc_iostats_intv_add(&msl_io_1m_stat, 1);
		
	*newreq = r = psc_pool_get(slc_biorq_pool);

	bmpc_ioreq_init(r, roff, len, op, b, mfh, q);

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

			DEBUG_BMAP(PLL_INFO, b, "maxpages=%d npages=%d "
			   "bkwdra=%d", maxpages, npages, bkwdra);
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
		MFH_LOCK(mfh);
		if (i >= npages && bmap_foff(b) +
		    bmpce_search.bmpce_off <= mfh->mfh_ra.mra_raoff) {
			freelock(&mfh->mfh_lock);
			i++;
			continue;
		}
		psclog_info("i=%d npages=%d raoff=%"PRIx64
		    " bmpce_foff=%"PRIx64, i, npages, mfh->mfh_ra.mra_raoff,
		    (off_t)(bmpce_search.bmpce_off + bmap_foff(b)));
		MFH_ULOCK(mfh);
 restart:
		e = bmpce_lookup_locked(bmpc, r,
		    (bkwdra ? (aoff + ((npages - 1 - i) * BMPC_BUFSZ)) :
		     aoff + (i * BMPC_BUFSZ)),
		    (i < npages) ? NULL : &r->biorq_bmap->bcm_fcmh->fcmh_waitq);

		BMPCE_LOCK(e);
		if (e->bmpce_flags & BMPCE_INIT)
			fetchpgs++;

		DEBUG_BMPCE(PLL_INFO, e, "i=%d, npages=%d maxpages=%d aoff=%u "
		    " aoff_search=%u", i, npages, maxpages, aoff,
		    bmpce_search.bmpce_off);

		if (i < npages) {
			if (e->bmpce_flags & BMPCE_EIO) {
				/* Don't take on pages marked with EIO.
				 *   Go back and try again.
				 */
				BMPC_ULOCK(bmpc);
				DEBUG_BMPCE(PLL_WARN, e,
				    "wait and retry for EIO to clear");
				psc_assert(e->bmpce_waitq);
				BMPCE_WAIT(e);
				BMPC_LOCK(bmpc);
				goto restart;
			}
			/* Increment the ref cnt via the lru mgmt
			 *   function for all pages needed to
			 *   fulfill the read and for ra pages
			 *   which need to be retrieved.
			 */
			bmpce_handle_lru_locked(e, bmpc, op, 1);
			psc_dynarray_add(&r->biorq_pages, e);

		} else {
			DEBUG_BMPCE(PLL_INFO, e, "ra (npndg=%d) i=%d "
			    "biorq_is_my_bmpce=%d raoff=%"PRIx64
			    " bmpce_foff=%"PRIx64,
			    pll_nitems(&mfh->mfh_ra_bmpces), i,
			    biorq_is_my_bmpce(r, e), mfh->mfh_ra.mra_raoff,
			    (off_t)(bmpce_search.bmpce_off + bmap_foff(b)));

			/* These are read-ahead bmpce's.  Only add
			 *   pages which have yet to be retrieved.
			 */
			if (e->bmpce_flags & BMPCE_EIO) {
				/* Don't bother with RA of pages marked EIO.
				 */
				DEBUG_BMPCE(PLL_WARN, e, "no RA for EIO page");

			} else if (biorq_is_my_bmpce(r, e)) {
				/* Other threads will block on the reada
				 *   completion.  The cb handler will decref
				 *   the bmpce.
				 */
				psc_assert(e->bmpce_flags & BMPCE_INIT);
				psc_assert(!(e->bmpce_flags & BMPCE_EIO));
				/* Stash the bmap pointer in 'owner'.
				 */
				e->bmpce_owner = b;
				bmpce_handle_lru_locked(e, bmpc, op, 1);
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
				MFH_LOCK(mfh);
				pll_addtail(&mfh->mfh_ra_bmpces, e);
				if (!(mfh->mfh_flags & MSL_FHENT_RASCHED)) {
					mfh->mfh_flags |= MSL_FHENT_RASCHED;
					MFH_ULOCK(mfh);
					lc_addtail(&bmapReadAheadQ, mfh);
				} else
					MFH_ULOCK(mfh);

			} else if (e->bmpce_flags & BMPCE_LRU) {
				/*
				 * There's no official read op pending
				 * for this ra page so no read ref is
				 * taken.  The lru is adjusted in
				 * preparation for its possible use.
				 */
				psc_assert(e->bmpce_flags & BMPCE_DATARDY);
				PFL_GETTIMESPEC(&e->bmpce_laccess);
				pll_remove(&bmpc->bmpc_lru, e);
				pll_add_sorted(&bmpc->bmpc_lru, e,
				    bmpce_lrusort_cmp1);
			}
			MFH_LOCK(mfh);
			mfh->mfh_ra.mra_raoff = bmap_foff(b) +
				bmpce_search.bmpce_off;
			MFH_ULOCK(mfh);
		}
		BMPCE_ULOCK(e);
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
	 * Pass1: Deal with RBW pages
	 */
	for (i=0; i < npages; i++) {
		e = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(e);

		if (biorq_is_my_bmpce(r, e)) {
			uint32_t rfsz = fsz - bmap_foff(b);

			/* Increase the rdref cnt in preparation for any
			 *   RBW ops but only on new pages owned by this
			 *   page cache entry.  For now bypass
			 *   bmpce_handle_lru_locked() for this op.
			 */
			if (!i && (rbw & BIORQ_RBWFP) &&
			    (fsz > foff ||
			     /* If file ends in this page then fetch */
			     (rfsz > e->bmpce_off &&
			      rfsz < e->bmpce_off + BMPC_BLKSZ))) {
				    e->bmpce_flags |= BMPCE_RBWPAGE;
				    psc_atomic16_inc(&e->bmpce_rdref);
				    r->biorq_flags |= BIORQ_RBWFP;

			} else if ((i == (npages - 1) &&
				    (rbw & BIORQ_RBWLP)) &&
				   (fsz > (foff + len) ||
				    (rfsz > e->bmpce_off &&
				     rfsz < e->bmpce_off + BMPC_BLKSZ))) {
				e->bmpce_flags |= BMPCE_RBWPAGE;
				psc_atomic16_inc(&e->bmpce_rdref);
				r->biorq_flags |= BIORQ_RBWLP;
			}
		}
		BMPCE_ULOCK(e);
	}

	/* Pass2: Sanity Check
	 */
	for (i=0; i < npages; i++) {
		e = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(e);
		if (i < npages)
			psc_assert(e->bmpce_off ==
			   aoff + (i * BMPC_BUFSZ));

		if (op == BIORQ_WRITE)
			psc_assert(psc_atomic16_read(&e->bmpce_wrref) > 0);
		else
			psc_assert(psc_atomic16_read(&e->bmpce_rdref) > 0);

		if (biorq_is_my_bmpce(r, e)) {
			/* The page is my reponsibility, ensure a cache
			 *   block has been assigned.
			 */
			psc_assert(e->bmpce_base);
			psc_assert(e->bmpce_flags & BMPCE_INIT);
			psc_assert(!(e->bmpce_flags & BMPCE_EIO));
			e->bmpce_flags &= ~BMPCE_INIT;

			if (op == BIORQ_READ)
				e->bmpce_flags |= BMPCE_READPNDG;
		}
		BMPCE_ULOCK(e);
		DEBUG_BMPCE(PLL_INFO, e, "bmpce prep done");
	}
 out:
	DEBUG_BIORQ(PLL_NOTIFY, r, "new req (fetchpgs=%d)", fetchpgs);
	if (op == BIORQ_READ || (r->biorq_flags & BIORQ_DIO))
		pll_add(&bmap_2_bmpc(b)->bmpc_pndg_biorqs, r);
	else
		pll_add_sorted(&bmap_2_bmpc(b)->bmpc_new_biorqs, r,
		    bmpc_biorq_cmp);
}

/* msl_mfh_seterr - apply error to the mfh_flush_rc so that threads blocked
 *   in flush may error out.
 */
void
msl_mfh_seterr(struct msl_fhent *mfh)
{
	MFH_LOCK(mfh);
	mfh->mfh_flush_rc = -EIO;
	MFH_ULOCK(mfh);
}

__static void
msl_biorq_del(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);

	if (r->biorq_flags & (BIORQ_RESCHED | BIORQ_RBWFAIL | BIORQ_FLUSHABORT))
		pll_remove(&bmpc->bmpc_new_biorqs, r);
	else
		pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_WRITE && !(r->biorq_flags & BIORQ_DIO)) {
		if (!(r->biorq_flags & BIORQ_RBWFAIL))
			atomic_dec(&bmpc->bmpc_pndgwr);
		
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) >= 0);
		/* Signify that a WB operation occurred.
		 */		
		if (!(r->biorq_flags & (BIORQ_FLUSHABORT | BIORQ_RBWFAIL)))
			bmpc->bmpc_compwr++;

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
	    (!bmpc_queued_ios(bmpc)))
		psc_assert(!atomic_read(&bmpc->bmpc_pndgwr));

	BMPC_ULOCK(bmpc);
	DEBUG_BMAP(PLL_INFO, b, "remove biorq=%p nitems_pndg(%d)",
		   r, pll_nitems(&bmpc->bmpc_pndg_biorqs));

	bmap_op_done_type(b, BMAP_OPCNT_BIORQ);
}

void
msl_bmpces_fail(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *e;
	int i;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCE_EIO;
		BMPCE_ULOCK(e);
	}
}

__static void
msl_biorq_unref(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *e;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(r->biorq_bmap);
	int i, eio;

	psc_assert(r->biorq_flags & BIORQ_DESTROY);
	psc_assert(!(r->biorq_flags & (BIORQ_INFL | BIORQ_SCHED)));

	/* Block here on an of our EIO'd pages waiting for other threads
	 *   to release their references.
	 * Additionally, we need to block for RA pages which have not
	 *   yet been marked as EIO.
	 */
	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		if (biorq_is_my_bmpce(r, e) && (e->bmpce_flags & BMPCE_EIO)) {
			if (r->biorq_flags & BIORQ_RBWFAIL) {
				psc_assert(r->biorq_flags & BIORQ_WRITE);
				psc_assert(psc_atomic16_read(&e->bmpce_wrref) >= 1);
				psc_assert(psc_atomic16_read(&e->bmpce_rdref) >= 1);
			}				
			
			while (((r->biorq_flags & BIORQ_RBWFAIL) ? 
				psc_atomic16_read(&e->bmpce_wrref) > 1 : 
				psc_atomic16_read(&e->bmpce_wrref)) ||
			       psc_atomic16_read(&e->bmpce_rdref) > 1) {
				BMPCE_WAIT(e);
				BMPCE_LOCK(e);
			}			
			/* Unless this is an RBWFAIL, only my rd ref should remain.
			 */			
			psc_assert((psc_atomic16_read(&e->bmpce_wrref) ==  
				    !!(r->biorq_flags & BIORQ_RBWFAIL)) &&
				   psc_atomic16_read(&e->bmpce_rdref) == 1);

			if (r->biorq_flags & BIORQ_RBWFAIL)
				psc_atomic16_dec(&e->bmpce_rdref);
		}	
		e->bmpce_flags &= ~BMPCE_INFLIGHT;
		DEBUG_BMPCE(PLL_INFO, e, "unset inflight");
		BMPCE_ULOCK(e);
	}

	BMPC_LOCK(bmpc);
	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		/* bmpce with no reference will be freed by the reaper */
		BMPCE_LOCK(e);
		eio = (e->bmpce_flags & BMPCE_EIO) ? 1 : 0;
		bmpce_handle_lru_locked(e, bmpc,
			(r->biorq_flags & BIORQ_WRITE) ? BIORQ_WRITE : BIORQ_READ, 0);

		if (!eio)
		    BMPCE_ULOCK(e);
	}
	BMPC_ULOCK(bmpc);
}

void
_msl_biorq_destroy(const struct pfl_callerinfo *pci, struct bmpc_ioreq *r)
{
	struct msl_fhent *f = r->biorq_fhent;
#if FHENT_EARLY_RELEASE
	int fhent = 1;
#endif

	BIORQ_LOCK(r);

	psc_assert(!(r->biorq_flags & BIORQ_DESTROY));

	/* Reads req's have their BIORQ_SCHED and BIORQ_INFL flags
	 *    cleared in msl_read_cb to unblock waiting
	 *    threads at the earliest possible moment.
	 */
	if (!(r->biorq_flags & BIORQ_DIO)) {
		if (r->biorq_flags & BIORQ_WRITE) {
			if (r->biorq_flags &
			    ((BIORQ_RBWFAIL | BIORQ_EXPIREDLEASE | 
			      BIORQ_RESCHED | BIORQ_BMAPFAIL | BIORQ_READFAIL)))
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
	BIORQ_ULOCK(r);

	DEBUG_BIORQ(PLL_INFO, r, "destroying (nwaiters=%d)",
		    atomic_read(&r->biorq_waitq.wq_nwaiters));

	/* One last shot to wakeup any blocked threads.
	 */
	psc_waitq_wakeall(&r->biorq_waitq);

	msl_biorq_unref(r);
	msl_biorq_del(r);

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

	if (r->biorq_rqset && !(r->biorq_flags & BIORQ_AIOWAIT)) {
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;
	}

	while (atomic_read(&r->biorq_waitq.wq_nwaiters))
		sched_yield();

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

struct slashrpc_cservice *
msl_try_get_replica_res(struct bmapc_memb *b, int iosidx)
{
	struct slashrpc_cservice *csvc;
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *resm;

	fci = fcmh_2_fci(b->bcm_fcmh);

	DEBUG_BMAPOD(PLL_INFO, b, "iosidx=%d", iosidx);

	if (SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls,
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

__static int
msl_biorq_aio_decref(struct bmpc_ioreq *r)
{
	struct msl_fsrqinfo *q = r->biorq_fsrqi;
	int rc;

	psc_assert(q);
	psc_assert(r->biorq_flags & BIORQ_AIOWAIT);

	MFH_LOCK(r->biorq_fhent);
	rc = --q->mfsrq_ref;
	DEBUG_BIORQ(PLL_NOTIFY, r, "(mfsrq_ref=%d)", q->mfsrq_ref);
	MFH_ULOCK(r->biorq_fhent);

	psc_assert(rc >= 0);

	return (rc);
}

__static void
msl_biorq_aio_prep(struct bmpc_ioreq *r)
{
	struct msl_fsrqinfo *q = r->biorq_fsrqi;

	psc_assert(q);

	BIORQ_LOCK(r);
	r->biorq_flags |= BIORQ_AIOWAIT;
	BIORQ_ULOCK(r);

	MFH_LOCK(r->biorq_fhent);
	q->mfsrq_ref++;
	DEBUG_BIORQ(PLL_NOTIFY, r, "(mfsrq_ref=%d)", q->mfsrq_ref);
	MFH_ULOCK(r->biorq_fhent);
}

__static void
msl_fsrq_aiowait_tryadd_locked(struct bmap_pagecache_entry *e, struct bmpc_ioreq *r)
{
	int locked;

	LOCK_ENSURE(&e->bmpce_lock);

	psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));
	locked = MFH_RLOCK(r->biorq_fhent);
	if (!msl_fsrqinfo_isset(r->biorq_fsrqi, MFSRQ_BMPCEATT)) {
		r->biorq_fsrqi->mfsrq_flags |= MFSRQ_BMPCEATT;
		r->biorq_fsrqi->mfsrq_bmpceatt = e;
		pll_add(&e->bmpce_pndgaios, r->biorq_fsrqi);
	}
	MFH_URLOCK(r->biorq_fhent, locked);
}

__static int
msl_req_aio_add(struct pscrpc_request *rq,
    int (*cbf)(struct pscrpc_request *, int, struct pscrpc_async_args *),
    struct pscrpc_async_args *av)
{
	struct bmpc_ioreq *r = av->pointer_arg[MSL_CBARG_BIORQ];
	struct bmap_pagecache_entry *e, **bmpces;
	struct slc_async_req *car;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	int i;

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	m = libsl_nid2resm(rq->rq_peer.nid);

	car = psc_pool_get(slc_async_req_pool);
	car->car_id = mp->id;
	car->car_cbf = cbf;
	/* pscfs_req has the pointers to each biorq needed for completion.
	 */
	memcpy(&car->car_argv, av, sizeof(*av));

	if (cbf == msl_readahead_cb) {
		/* readahead's are not associated with any biorq.
		 */
		psc_assert(!r);
		bmpces = av->pointer_arg[MSL_CBARG_BMPCE];

		for (i = 0;; i++) {
			e = bmpces[i];
			if (!e)
				break;
			BMPCE_SETATTR(e, BMPCE_AIOWAIT, "set aio");
		}

	} else if (cbf == msl_read_cb) {
		int naio = 0;

		DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
			BMPCE_LOCK(e);
			if (e->bmpce_flags & BMPCE_DATARDY) {
				BMPCE_ULOCK(e);
				continue;

			} else if (!naio)
				msl_fsrq_aiowait_tryadd_locked(e, r);

			BMPCE_SETATTR(e, BMPCE_AIOWAIT, "set aio");
			BMPCE_ULOCK(e);
			naio++;
		}
		/* Should have found at least one aio'd page.
		 */
		psc_assert(naio);
		msl_biorq_aio_prep(r);
		car->car_fsrqinfo = r->biorq_fsrqi;

	} else if (cbf == msl_dio_cb) {
		msl_biorq_aio_prep(r);
		if (r->biorq_flags & BIORQ_WRITE)
			/* The biorq is will be destroyed.
			 */
			av->pointer_arg[MSL_CBARG_BIORQ] = NULL;

		car->car_fsrqinfo = r->biorq_fsrqi;
		car->car_len = r->biorq_len;
	} else
		abort();

	lc_add(&resm2rmci(m)->rmci_async_reqs, car);

	return (SLERR_AIOWAIT);
}

__static void
msl_fsrq_complete(struct msl_fsrqinfo *q)
{
	struct bmpc_ioreq *r;
	size_t len, rc;
	char *buf = q->mfsrq_buf;
	int i;

	psc_assert(!q->mfsrq_ref);

	for (i = 0, len = 0; i < MAX_BMAPS_REQ; i++) {
		r = q->mfsrq_biorq[i];
		if (!r)
			break;

		BIORQ_LOCK(r);
		r->biorq_flags &= ~(BIORQ_INFL | BIORQ_SCHED);
		r->biorq_flags &= ~(BIORQ_RBWLP | BIORQ_RBWFP);
		BIORQ_ULOCK(r);

		DEBUG_BIORQ(PLL_INFO, r, "fsrq_complete");
		if (q->mfsrq_err)
			msl_biorq_destroy(r);

		else if (q->mfsrq_rw == SL_READ) {
			psc_assert(r->biorq_flags & BIORQ_READ);
			/* Support mix of dio and cached reads.  This may occur
			 *   if the read request spans bmaps.
			 */
			if (r->biorq_flags & BIORQ_DIO) {
				len += r->biorq_len;
				msl_biorq_destroy(r);
			} else {
				rc = msl_pages_copyout(r, buf);
				if (!rc)
					break;
				len += rc;
			}
			buf += len;
			psc_assert(len <= q->mfsrq_size);
		} else
			abort();
	}

	if (!q->mfsrq_err) {
		MFH_LOCK(q->mfsrq_fh);
		q->mfsrq_fh->mfh_nbytes_rd += len;
		MFH_ULOCK(q->mfsrq_fh);
	}

	pscfs_reply_read(q->mfsrq_pfr, q->mfsrq_buf, len,
	    -abs(q->mfsrq_err));
}

__static void
msl_fsrq_completion_try(struct msl_fsrqinfo *q)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache_entry *e;
	int i, j;

	psc_assert(q);

	/* Scan through the biorq's and their bmpce's for pages still blocked
	 *    in AIOWAIT.
	 */
	for (i = 0; i < MAX_BMAPS_REQ; i++) {
		r = q->mfsrq_biorq[i];
		if (!r)
			break;

		psc_assert(r->biorq_flags & BIORQ_AIOWAIT);

		DYNARRAY_FOREACH(e, j, &r->biorq_pages) {
			BMPCE_LOCK(e);
			if (e->bmpce_flags & BMPCE_EIO) {
				BMPCE_ULOCK(e);

				MFH_LOCK(r->biorq_fhent);
				q->mfsrq_err = EIO;
				MFH_ULOCK(r->biorq_fhent);

				goto out;

			} else if (!(e->bmpce_flags & BMPCE_DATARDY)) {
				MFH_LOCK(r->biorq_fhent);
				psc_assert(msl_fsrqinfo_isset(r->biorq_fsrqi,
					      MFSRQ_BMPCEATT));
				r->biorq_fsrqi->mfsrq_flags &= ~MFSRQ_BMPCEATT;
				r->biorq_fsrqi->mfsrq_bmpceatt = NULL;
				msl_fsrq_aiowait_tryadd_locked(e, r);
				MFH_ULOCK(r->biorq_fhent);
				BMPCE_ULOCK(e);
				DEBUG_BIORQ(PLL_NOTIFY, r,
				    "still blocked on (bmpce@%p)", e);
				return;
			}
			BMPCE_ULOCK(e);
		}
	}
 out:
	msl_fsrq_complete(q);
}

__static void
msl_bmpce_aio_process(struct bmap_pagecache_entry *e)
{
	struct msl_fsrqinfo *q;

	while ((q = pll_get(&e->bmpce_pndgaios)))
		/* If fsrq cannot be completed. msl_fsrq_completion_try()
		 *   will reattach it to another aio'd bmpce.
		 */
		msl_fsrq_completion_try(q);
}

#define msl_bmpce_rpc_done(e, rc) _msl_bmpce_rpc_done(PFL_CALLERINFOSS(SLSS_BMAP), (e), (rc))

__static void
_msl_bmpce_rpc_done(const struct pfl_callerinfo *pci,
    struct bmap_pagecache_entry *e, int rc)
{
	int aio_completion_try = 0;

	BMPCE_LOCK(e);
	psc_assert(e->bmpce_waitq);

	if (pll_nitems(&e->bmpce_pndgaios))
		aio_completion_try = 1;

	if (rc) {
		e->bmpce_flags |= BMPCE_EIO;
		DEBUG_BMPCE(PLL_WARN, e, "set EIO");
		BMPCE_WAKE(e);

	} else if (e->bmpce_flags & BMPCE_RBWPAGE) {
		/* The RBW stuff needs to be managed outside of
		 *   the LRU; this is not the best place but should
		 *   suffice for now.
		 */
		psc_assert(psc_atomic16_read(&e->bmpce_rdref) == 1);
		psc_atomic16_dec(&e->bmpce_rdref);
		e->bmpce_flags |= BMPCE_RBWRDY;
		DEBUG_BMPCE(PLL_INFO, e, "rdref dec for RBW, !DATARDY");
		aio_completion_try = 0;

	} else {
		e->bmpce_flags |= BMPCE_DATARDY;
		DEBUG_BMPCE(PLL_INFO, e, "datardy via read_cb");
		BMPCE_WAKE(e);

		e->bmpce_waitq = NULL;
		e->bmpce_owner = NULL;
	}
	/* AIOWAIT is removed no matter what.
	 */
	e->bmpce_flags &= ~BMPCE_AIOWAIT;

	BMPCE_ULOCK(e);

	if (aio_completion_try)
		msl_bmpce_aio_process(e);
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
	struct bmap_pagecache_entry *e;
	struct bmapc_memb *b;
	int i, decrefd = 0;

	b = r->biorq_bmap;

	psc_assert(a);
	psc_assert(b);

	if (rq)
		DEBUG_REQ(rc ? PLL_ERROR : PLL_INFO, rq, "bmap=%p biorq=%p",
		  b, r);

	DEBUG_BMAP(rc ? PLL_ERROR : PLL_INFO, b, "sbd_seq=%"PRId64,
	   bmap_2_sbd(b)->sbd_seq);

	DEBUG_BIORQ(rc ? PLL_ERROR : PLL_INFO, r, "rc=%d", rc);

	DYNARRAY_FOREACH(e, i, a) {
		if ((r->biorq_flags & BIORQ_AIOWAIT) &&
		    (e->bmpce_flags & BMPCE_AIOWAIT) && !decrefd) {
			/* Call aio decref one time per-RPC but only
			 *   if a page in the RPC is marked AIO.
			 */
			msl_biorq_aio_decref(r);
			decrefd = 1;
		}
		msl_bmpce_rpc_done(e, rc);
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
		return (msl_req_aio_add(rq, msl_read_cb, args));

	return (msl_read_cb(rq, rc, args));
}

int
msl_readahead_cb(struct pscrpc_request *rq, int rc,
    struct pscrpc_async_args *args)
{
	struct bmap_pagecache_entry *e,
		**bmpces = args->pointer_arg[MSL_CBARG_BMPCE];
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmap_pagecache *bmpc = args->pointer_arg[MSL_CBARG_BMPC];
	struct psc_waitq *wq = NULL;
	struct bmapc_memb *b;
	int i;

	if (rq)
		DEBUG_REQ(PLL_INFO, rq, "bmpces=%p", bmpces);

	for (i = 0, e = bmpces[0], b = e->bmpce_owner; e; i++, e = bmpces[i]) {
		psc_assert(b == e->bmpce_owner);

		if (!i)
			DEBUG_BMAP(rc ? PLL_ERROR : PLL_INFO, b, "sbd_seq=%"
			   PRId64, bmap_2_sbd(b)->sbd_seq);

		DEBUG_BMPCE(rc ? PLL_ERROR : PLL_INFO, e, "rc=%d", rc);

		msl_bmpce_rpc_done(e, rc);

		BMPC_LOCK(bmpc);
		BMPCE_LOCK(e);
		pll_remove(&bmpc->bmpc_pndg_ra, e);
		bmpce_handle_lru_locked(e, bmpc, BIORQ_READ, 0);
		if (!rc)
			/* EIO's are always unlocked inside
			 *   bmpce_handle_lru_locked()
			 */
			BMPCE_ULOCK(e);
		BMPC_ULOCK(bmpc);
	}

	if (wq)
		psc_waitq_wakeall(wq);

	sl_csvc_decref(csvc);

	PSCFREE(bmpces);
	return (rc);
}

int
msl_readahead_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	psc_assert(rq->rq_reqmsg->opc == SRMT_READ);

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);

	if (rc == SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_readahead_cb, args));

	return (msl_readahead_cb(rq, rc, args));
}

int
msl_write_rpc_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmpc_write_coalescer *bwc = 
		args->pointer_arg[MSL_CBARG_BIORQS];
	struct bmpc_ioreq *r;
	int rc = 0, expired_lease = 0, maxretries = 0;

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);

	DEBUG_REQ(rc ? PLL_ERROR : PLL_INFO, rq, "cb");

	r = pll_peekhead(&bwc->bwc_pll);
	BMAP_LOCK(r->biorq_bmap);
	if (r->biorq_bmap->bcm_flags & BMAP_CLI_LEASEEXPIRED) {
		expired_lease = 1;
		PLL_FOREACH(r, &bwc->bwc_pll) {
			/* Lease extension cb must have already marked
			 *   the biorqs with BIORQ_EXPIREDLEASE.
			 */
			BIORQ_LOCK(r);
			psc_assert(r->biorq_flags & BIORQ_EXPIREDLEASE);
			BIORQ_ULOCK(r);
		}
	}
	BMAP_ULOCK(r->biorq_bmap);

	/* Check for max retries on each biorq.
	 */
	if (rc && !expired_lease) {
		PLL_FOREACH(r, &bwc->bwc_pll)
			if (r->biorq_retries >= SL_MAX_IOSREASSIGN) {
				maxretries = 1;
				break;
			}

		if (maxretries)
			bmpc_biorqs_fail(bmap_2_bmpc(r->biorq_bmap), 
				 BIORQ_MAXRETRIES);
	}

	while ((r = pll_get(&bwc->bwc_pll))) {
		if (rc) {
			if (expired_lease || maxretries) {
				/* Cleanup errored I/O requests.
				 */
				msl_bmpces_fail(r);
				msl_biorq_destroy(r);
			} else
				/* Ok to retry this one.
				 */ 
				bmap_flush_resched(r);
		} else
			/* Success.
			 */
			msl_biorq_destroy(r);
	}

	bwc_release(bwc);
	return (0);
}

int
msl_dio_cb(struct pscrpc_request *rq, int rc, struct pscrpc_async_args *args)
{
	//struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct srm_io_req *mq;
	int op;

	DEBUG_REQ(PLL_INFO, rq, "cb");

	if (rq == NULL)
		return (rc);

	op = rq->rq_reqmsg->opc;
	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	psc_assert(mq);

	DEBUG_BIORQ(PLL_INFO, r, "completed dio (op=%d) off=%u sz=%u rc=%d",
	    op, mq->offset, mq->size, rc);

	if (rc && !r->biorq_fsrqi->mfsrq_err)
		r->biorq_fsrqi->mfsrq_err = rc;

	if ((r->biorq_flags & BIORQ_AIOWAIT) && !msl_biorq_aio_decref(r))
		msl_fsrq_complete(r->biorq_fsrqi);

	return (rc);
}

int
msl_dio_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	MSL_GET_RQ_STATUS_TYPE(csvc, rq, srm_io_rep, rc);
	if (rc == SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_dio_cb, args));

	return (msl_dio_cb(rq, rc, args));
}

__static int
msl_pages_dio_getput(struct bmpc_ioreq *r, char *bufp)
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

		rq->rq_bulk_abortable = 1;
		rq->rq_interpret_reply = msl_dio_cb0;
		rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BUF] = bufp + nbytes;
		iovs[i].iov_base = bufp + nbytes;
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
		rc = pscrpc_push_req(rq);
		if (rc) {
			pscrpc_set_remove_req(r->biorq_rqset, rq);
			goto error;
		}
	}
	/* Should be no need for a callback since this call is fully
	 *   blocking.
	 */
	psc_assert(nbytes == size);
	rc = pscrpc_set_wait(r->biorq_rqset);
	pscrpc_set_destroy(r->biorq_rqset);
	r->biorq_rqset = NULL;

	PSCFREE(iovs);

	/*
	 * async I/O registered by sliod; we must wait for a
	 * notification from him when it is ready.
	 */
	if (rc == -SLERR_AIOWAIT)
		goto out;

	else if (rc)
		goto error;

	psc_iostats_intv_add((op == SRMT_WRITE ?
	    &msl_diowr_stat : &msl_diord_stat), size);

	msl_biorq_destroy(r);

 out:
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
	BIORQ_LOCK(r);
	r->biorq_flags |= BIORQ_FLUSHRDY;
	DEBUG_BIORQ(PLL_INFO, r, "BIORQ_FLUSHRDY");
	psc_assert(psclist_conjoint(&r->biorq_lentry,
	    psc_lentry_hd(&r->biorq_lentry)));
	atomic_inc(&bmpc->bmpc_pndgwr);
	BIORQ_ULOCK(r);

	if (b->bcm_flags & BMAP_DIRTY) {
		/* If the bmap is already dirty then at least
		 *   one other writer must be present.
		 */
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 1);
		psc_assert((pll_nitems(&bmpc->bmpc_pndg_biorqs) +
			    pll_nitems(&bmpc->bmpc_new_biorqs)) > 1);

	} else {
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
	bmap_flushq_wake(BMAPFLSH_TIMEOA, &r->biorq_expire);

	DEBUG_BMAP(PLL_INFO, b, "biorq=%p list_empty(%d)",
		   r, pll_empty(&bmpc->bmpc_pndg_biorqs));
	BMPC_ULOCK(bmpc);
	BMAP_ULOCK(b);
}

void
msl_reada_rpc_launch(struct bmap_pagecache_entry **bmpces, int nbmpce)
{
	struct bmap_pagecache_entry *e, **bmpces_cbarg;
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
		e = bmpces_cbarg[i] = bmpces[i];
		psc_assert(!(e->bmpce_flags & BMPCE_EIO));
		psc_assert(e->bmpce_base);

		if (!i) {
			off = e->bmpce_off;
			b = e->bmpce_owner;
		} else
			psc_assert(b == e->bmpce_owner);

		iovs[i].iov_base = e->bmpce_base;
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

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    nbmpce);
	if (rc)
		goto error;

	PSCFREE(iovs);

	mq->size = BMPC_BUFSZ * nbmpce;
	mq->op = SRMIOP_RD;
	mq->offset = off;
	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(mq->sbd));

	DEBUG_BMAP(PLL_NOTIFY, b, "reada req off=%u, npages=%d", off, nbmpce);

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	rq->rq_timeout = 15;
	rq->rq_bulk_abortable = 1;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = bmpces_cbarg;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPC] = bmap_2_bmpc(b);
	rq->rq_interpret_reply = msl_readahead_cb0;
	pscrpc_completion_set(rq,  &rpcComp);

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

	rc = pscrpc_nbreqset_add(pndgReadaReqs, rq);
	if (!rc)
		return;

 error:
	PSCFREE(iovs);
	PSCFREE(bmpces_cbarg);

	/* Deal with errored read ahead bmpce's.
	 */
	BMPC_LOCK(bmap_2_bmpc(b));
	for (i=0; i < nbmpce; i++) {
		e = bmpces[i];

		BMPCE_LOCK(e);
		if (added)
			pll_remove(&bmap_2_bmpc(b)->bmpc_pndg_ra, e);

		e->bmpce_flags |= BMPCE_EIO;
		bmpce_handle_lru_locked(e, bmap_2_bmpc(b),
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
msl_read_rpc_launch(struct bmpc_ioreq *r, int startpage, int npages)
{
	struct slashrpc_cservice *csvc = NULL;
	struct bmap_pagecache_entry *e;
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
		e = psc_dynarray_getpos(&r->biorq_pages, i + startpage);

		BMPCE_LOCK(e);

		psc_assert(biorq_is_my_bmpce(r, e));
		psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));
		DEBUG_BMPCE(PLL_INFO, e, "adding to rpc");

		BMPCE_ULOCK(e);

		iovs[i].iov_base = e->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			off = e->bmpce_off;
		psc_dynarray_add(a, e);
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

	rq->rq_bulk_abortable = 1;
	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    npages);
	if (rc)
		goto error;

	PSCFREE(iovs);

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
		e = psc_dynarray_getpos(&r->biorq_pages, i + startpage);
		/* Didn't get far enough for the waitq to be removed.
		 */
		psc_assert(e->bmpce_waitq);

		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCE_EIO;
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}
	BMPC_ULOCK(bmap_2_bmpc(r->biorq_bmap));

	return (rc);
}

__static int
msl_launch_read_rpcs(struct bmpc_ioreq *r, int *psched)
{
	struct bmap_pagecache_entry *e;
	int rc = 0, i, j = -1;

	*psched = 0;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		/* If readahead, biorq_getaligned_off needs to account
		 *    for the number of bmpce's inside biorq_pages.
		 */
		bmpce_usecheck(e, BIORQ_READ,
		    biorq_getaligned_off(r, i));

		if (biorq_is_my_bmpce(r, e))
			psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));

		BMPCE_ULOCK(e);

		/* Try to set the start bmpce if it's not yet
		 *   assigned.
		 */
		if (j < 0) {
			if (biorq_is_my_bmpce(r, e))
				j = i;
		} else {
			if (!biorq_is_my_bmpce(r, e)) {
				rc = msl_read_rpc_launch(r, j, i - j);
				j = -1;
				*psched = 1;

			} else if ((i-j) == BMPC_MAXBUFSRPC) {
				rc = msl_read_rpc_launch(r, j, i - j);
				j = i;
				*psched = 1;
			}
		}
	}
	if (j >= 0) {
		/* Catch any unsent frags at the end of the array.
		 */
		rc = msl_read_rpc_launch(r, j, i - j);
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
msl_pages_prefetch(struct bmpc_ioreq *r)
{
	int sched = 0, rc = 0, npages;
	struct bmap_pagecache_entry *e;

	npages = psc_dynarray_len(&r->biorq_pages);

	DEBUG_BIORQ(PLL_INFO, r, "check prefetch");

	psc_assert(!r->biorq_rqset);

	BIORQ_LOCK(r);
	r->biorq_flags |= BIORQ_SCHED;
	BIORQ_ULOCK(r);

	/* Only read in the pages owned by this request.  To do this
	 *   the below loop marks only the iov slots which correspond
	 *   to page cache entries owned by this request as determined
	 *   by biorq_is_my_bmpce().
	 */
	if (r->biorq_flags & BIORQ_READ)
		rc = msl_launch_read_rpcs(r, &sched);

	else { /* BIORQ_WRITE */
		int i;

		if ((r->biorq_flags & (BIORQ_RBWFP|BIORQ_RBWLP)) ==
		    (BIORQ_RBWFP|BIORQ_RBWLP)) {
			for (i = 0; i < 2; i++) {
				if (!i)
					e = psc_dynarray_getpos(&r->biorq_pages, i);
				else
					e = psc_dynarray_getpos(&r->biorq_pages, npages - 1);

				psc_assert(biorq_is_my_bmpce(r, e));
				psc_assert(e->bmpce_flags & BMPCE_RBWPAGE);
				psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));

				if (!i)
					rc = msl_read_rpc_launch(r, 0, 1);
				else
					rc |= msl_read_rpc_launch(r,
							  npages - 1, 1);
			}
			sched = 1;

		} else {
			if (r->biorq_flags & BIORQ_RBWFP) {
				e = psc_dynarray_getpos(&r->biorq_pages, 0);
				psc_assert(biorq_is_my_bmpce(r, e));
				psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));
				psc_assert(e->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_launch(r, 0, 1);
				sched = 1;
			}
			if (r->biorq_flags & BIORQ_RBWLP) {
				e = psc_dynarray_getpos(&r->biorq_pages,
				    npages - 1);
				psc_assert(biorq_is_my_bmpce(r, e));
				psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));
				psc_assert(e->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_launch(r, npages - 1, 1);
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
	struct bmap_pagecache_entry *e;
	int rc = 0, i, aio_placed = 0;

	if (r->biorq_rqset) {
		rc = pscrpc_set_wait(r->biorq_rqset);
		/*
		 * The set cb is not being used; msl_read_cb() is
		 *   called on every RPC in the set.  This was causing
		 *   the biorq to have its flags mod'd in an incorrect
		 *   fashion.  For now, the following lines will be moved
		 *   here.
		 */
		BIORQ_LOCK(r);
		if (rc != -SLERR_AIOWAIT)
			r->biorq_flags &= ~(BIORQ_INFL | BIORQ_SCHED);

		if (!rc) {
			r->biorq_flags &= ~(BIORQ_RBWLP | BIORQ_RBWFP);
			DEBUG_BIORQ(PLL_INFO, r, "read cb complete");
			psc_waitq_wakeall(&r->biorq_waitq);

		}
		BIORQ_ULOCK(r);

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

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		DEBUG_BMPCE(PLL_INFO, e, " ");

		if (!biorq_is_my_bmpce(r, e)) {
			/* For pages not owned by this request,
			 *    wait for them to become DATARDY
			 *    or to have failed.
			 */
			while (!(e->bmpce_flags &
			    (BMPCE_DATARDY | BMPCE_EIO | BMPCE_AIOWAIT))) {
				DEBUG_BMPCE(PLL_NOTIFY, e, "waiting");
				BMPCE_WAIT(e);
				BMPCE_LOCK(e);
			}
		}

		/* If this a read request OR another thread is dealing
		 *   with this bmpce then check.
		 */
		if ((r->biorq_flags & BIORQ_READ) ||
		    !biorq_is_my_bmpce(r, e)) {
			/* If there was an error, retry or give up. */
			if (e->bmpce_flags & BMPCE_EIO) {
				r->biorq_flags &= ~BIORQ_SCHED;
				rc = -EAGAIN;
			}

			if (rc == 0 && (e->bmpce_flags & BMPCE_AIOWAIT)) {
				rc = -SLERR_AIOWAIT;
				if (!aio_placed) {
					BIORQ_LOCK(r);
					r->biorq_flags |= BIORQ_AIOWAIT;
					BIORQ_ULOCK(r);

					msl_fsrq_aiowait_tryadd_locked(e, r);
					aio_placed = 1;
				}
			}

			/* Read requests must have had their bmpce's
			 *   put into DATARDY by now (i.e. all RPCs
			 *   must have already been completed).
			 *   Same goes for pages owned by other requests.
			 */
			psc_assert(e->bmpce_flags &
			    (BMPCE_DATARDY | BMPCE_EIO | BMPCE_AIOWAIT));
		}

		BMPCE_ULOCK(e);
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
	struct bmap_pagecache_entry *e;
	uint32_t toff, tsize, nbytes;
	char *dest, *src;
	int i;

	src    = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		/* All pages are involved, therefore tsize should have value.
		 */
		psc_assert(tsize);

		/* Re-check RBW sanity.  The waitq pointer within the bmpce
		 *   must still be valid in order for this check to work.
		 */
		BMPCE_LOCK(e);
		if (e->bmpce_flags & BMPCE_RBWPAGE) {
			psc_assert(e->bmpce_flags & BMPCE_RBWRDY);
			psc_assert(biorq_is_my_bmpce(r, e));
		}

		/* Set the starting buffer pointer into
		 *  our cache vector.
		 */
		dest = e->bmpce_base;
		if (!i && (toff > e->bmpce_off)) {
			/* The first cache buffer pointer may need
			 *    a bump if the request offset is unaligned.
			 */
			bmpce_usecheck(e, BIORQ_WRITE,
				       (toff & ~BMPC_BUFMASK));
			psc_assert((toff - e->bmpce_off) < BMPC_BUFSZ);
			dest += toff - e->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - e->bmpce_off),
				     tsize);
		} else {
			bmpce_usecheck(e, BIORQ_WRITE, toff);
			nbytes = MIN(BMPC_BUFSZ, tsize);
		}

		DEBUG_BMPCE(PLL_DEBUG, e, "tsize=%u nbytes=%u toff=%u",
			    tsize, nbytes, toff);
		BMPCE_ULOCK(e);
		/* Do the deed.
		 */
		memcpy(dest, src, nbytes);
		/* If the bmpce belongs to this request and is not yet
		 *   DATARDY (ie wasn't an RBW block) then set DATARDY
		 *   and wakeup anyone who was blocked.  Note the waitq
		 *   pointer will hang around until the request has completed.
		 * Note:  wrrefs are held until the
		 */
		BMPCE_LOCK(e);
		if (biorq_is_my_bmpce(r, e) &&
		    !(e->bmpce_flags & BMPCE_DATARDY)) {
			psc_assert(e->bmpce_owner);
			e->bmpce_flags |= BMPCE_DATARDY;
			e->bmpce_flags &= ~(BMPCE_RBWPAGE|BMPCE_RBWRDY);
			psc_waitq_wakeall(e->bmpce_waitq);
			e->bmpce_waitq = NULL;
			e->bmpce_owner = NULL;
		}
		BMPCE_ULOCK(e);

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
	struct bmap_pagecache_entry *e;
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
		e = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(e);
		src = e->bmpce_base;
		if (!i && (toff > e->bmpce_off)) {
			psc_assert((toff - e->bmpce_off) < BMPC_BUFSZ);
			src += toff - e->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - e->bmpce_off),
			     tsize);
		} else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		DEBUG_BMPCE(PLL_INFO, e, "tsize=%u nbytes=%zu toff=%"
		    PSCPRIdOFFT, tsize, nbytes, toff);

		psc_assert(e->bmpce_flags & BMPCE_DATARDY);
		bmpce_usecheck(e, BIORQ_READ, biorq_getaligned_off(r, i));

		memcpy(dest, src, nbytes);

		if (e->bmpce_flags & BMPCE_READA)
			psc_iostats_intv_add(&msl_racache_stat, nbytes);

		if (!biorq_is_my_bmpce(r, e))
			psc_iostats_intv_add(&msl_rdcache_stat, nbytes);

		BMPCE_ULOCK(e);

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

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh, "rapages=%d bkwd=%d",
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
			if ((mfh->mfh_ra.mra_loff + mfh->mfh_ra.mra_lsz) ==
			    off)
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

int
msl_fsrqinfo_state(struct msl_fsrqinfo *q, int flag, int set,
    int wait_or_wake)
{
	int rc = 0, locked;

	psc_assert(q);
	locked = MFH_RLOCK(q->mfsrq_fh);
 retry:
	switch (set) {
	case 0:
		if (q->mfsrq_flags & flag)
			rc = 1;
		break;
	case 1:
		if (!(q->mfsrq_flags & flag))
			q->mfsrq_flags |= flag;
		break;
	case -1:
		if (q->mfsrq_flags & flag)
			q->mfsrq_flags &= ~flag;
		break;
	default:
		abort();
	}

	if (wait_or_wake) {
		if (set)
			psc_waitq_wakeall(&msl_fhent_flush_waitq);
		else {
			/* Wait for the condition to become true.
			 */
			if (!rc) {
				psc_waitq_wait(&msl_fhent_flush_waitq,
				       &q->mfsrq_fh->mfh_lock);
				MFH_LOCK(q->mfsrq_fh);
				goto retry;
			}
		}
	}
	MFH_URLOCK(q->mfsrq_fh, locked);

	return (rc);
}

void
msl_fsrqinfo_write(struct msl_fsrqinfo *q)
{
	int rc;
	struct fidc_membh *f = q->mfsrq_fh->mfh_fcmh;
	struct timespec ts;

	MFH_LOCK(q->mfsrq_fh);
	psc_assert(msl_fsrqinfo_aioisset(q));
	q->mfsrq_flags = 0;
	q->mfsrq_ref = 0;
	MFH_ULOCK(q->mfsrq_fh);

	rc = msl_write(q->mfsrq_pfr, q->mfsrq_fh, q->mfsrq_buf,
	    q->mfsrq_size, q->mfsrq_off);
	if (rc < 0) {
		if (rc == -SLERR_AIOWAIT)
			return;
		rc = -rc;
		goto out;
	}
	rc = 0;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&ts);
	f->fcmh_sstb.sst_mtime = ts.tv_sec;
	f->fcmh_sstb.sst_mtime_ns = ts.tv_nsec;
	FCMH_ULOCK(f);

	MFH_LOCK(q->mfsrq_fh);
	q->mfsrq_fh->mfh_nbytes_wr += q->mfsrq_size;
	MFH_ULOCK(q->mfsrq_fh);

 out:
	DEBUG_FCMH(PLL_INFO, f, "write: buf=%p rc=%d sz=%zu "
	    "off=%"PSCPRIdOFFT, q->mfsrq_buf, rc, q->mfsrq_size,
	    q->mfsrq_off);
	pscfs_reply_write(q->mfsrq_pfr, q->mfsrq_size, rc);
}

__static void
msl_fsrqinfo_biorq_add(struct msl_fsrqinfo *q, struct bmpc_ioreq *r,
    int pos)
{
	MFH_LOCK(r->biorq_fhent);
	DEBUG_BIORQ(PLL_INFO, r, "q=%p pos=%d", q, pos);
	q->mfsrq_biorq[pos] = r;
	MFH_ULOCK(r->biorq_fhent);
}

__static struct msl_fsrqinfo *
msl_fsrqinfo_init(struct pscfs_req *pfr, struct msl_fhent *mfh,
    char *buf, size_t size, off_t off, enum rw rw)
{
	struct msl_fsrqinfo *q = pfr->pfr_info;

	if (!q) {
		q = PSCALLOC(sizeof(*q));

		q->mfsrq_fh = mfh;
		q->mfsrq_buf = buf;
		q->mfsrq_size = size;
		q->mfsrq_off = off;
		q->mfsrq_rw = rw;
		q->mfsrq_pfr = pfr;

		INIT_PSC_LISTENTRY(&q->mfsrq_lentry);
		pfr->pfr_info = q;

	} else {
		int i;

		/* The fs request was reissued.  Clear out any old state.
		 */
		psc_assert(q->mfsrq_fh == mfh &&
			   q->mfsrq_buf == buf &&
			   q->mfsrq_size == size &&
			   q->mfsrq_off == off &&
			   q->mfsrq_rw == rw &&
			   q->mfsrq_pfr == pfr);

		for (i = 0; i < MAX_BMAPS_REQ; i++) {
			if (!q->mfsrq_biorq[i])
				break;

			msl_biorq_destroy(q->mfsrq_biorq[i]);
		}
	}
	return (q);
}

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
ssize_t
msl_io(struct pscfs_req *pfr, struct msl_fhent *mfh, char *buf,
    const size_t size, const off_t off, enum rw rw)
{
	struct bmpc_ioreq *r[MAX_BMAPS_REQ];
	struct bmapc_memb *b, *bref = NULL;
	struct fidc_membh *f;
	struct msl_fsrqinfo *q;
	size_t s, e, tlen, tsize;
	uint64_t fsz;
	ssize_t rc;
	off_t roff;
	char *bufp;
	int nr, i;

#define MSL_BIORQ_COMPLETE	((void *)0x1)

	memset(r, 0, sizeof(r));

	psc_assert(mfh);
	psc_assert(mfh->mfh_fcmh);

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_INFO, f, "buf=%p size=%zu off=%"PRId64" rw=%s",
	   buf, size, off, (rw == SL_READ) ? "read" : "write");

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
	/* Initialize some state in the pfr to help with aio requests.
	 */
	q = msl_fsrqinfo_init(pfr, mfh, buf, size, off, rw);

 restart:
	rc = 0;

	tsize = size;
	FCMH_LOCK(f);
	fsz = fcmh_getsize(f);

	if (!size || (rw == SL_READ && off >= (off_t)fsz)) {
		FCMH_ULOCK(f);
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
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_CLI_TRUNC);
	FCMH_ULOCK(f);

	/* Relativize the length and offset (roff is not aligned).
	 */
	roff  = off - (s * SLASH_BMAP_SIZE);
	psc_assert(roff < SLASH_BMAP_SIZE);

	/* Length of the first bmap request.
	 */
	tlen  = MIN(SLASH_BMAP_SIZE - (size_t)roff, tsize);

	/*
	 * Foreach block range, get its bmap and make a request into its
	 *  page cache.  This first loop retrieves all the pages.
	 */
	for (i = 0, bufp = buf; i < nr; i++) {
		if (r[i])
			goto load_next;

		DEBUG_FCMH(PLL_INFO, f, "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%s", tsize, tlen, off, roff,
		    (rw == SL_READ) ? "read" : "write");

		psc_assert(tsize);
 retry_bmap:
		/*
		 * Load up the bmap; if it's not available then we're
		 * out of luck because we have no idea where the data
		 * is!
		 */
		rc = bmap_get(f, s + i, rw, &b);
		if (rc) {
			DEBUG_FCMH(PLL_ERROR, f, "bno=%zd sz=%zu tlen=%zu "
			   "off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT" rw=%s "
			   "rc=%zd", s + i, tsize, tlen, off, roff,
			   (rw == SL_READ) ? "read" : "write", rc);
			if (msl_fd_offline_retry(mfh))
				goto retry_bmap;
			switch (abs(rc)) {
			case SLERR_ION_OFFLINE:
				rc = -EHOSTUNREACH;
				break;
			}
			goto out;
		}

		rc = msl_bmap_lease_tryext(b, NULL, 1);
		if (rc) {
			bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
			goto retry_bmap;
		}
		/*
		 * Re-relativize the offset if this request spans more
		 * than 1 bmap.
		 */
		msl_biorq_build(&r[i], b, mfh, q,
		    roff - (i * SLASH_BMAP_SIZE), tlen,
		    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);

		/* Add the biorq to the fsrq. */
		msl_fsrqinfo_biorq_add(q, r[i], i);
		/*
		 * If we are not doing direct I/O, launch read for read
		 * requests and pre-read for unaligned write requests.
		 */
		if (!(r[i]->biorq_flags & BIORQ_DIO) &&
		    (r[i]->biorq_flags &
		      (BIORQ_READ | BIORQ_RBWFP | BIORQ_RBWLP))) {
			rc = msl_pages_prefetch(r[i]);
			if (rc) {
				rc = msl_offline_retry_ignexpire(r[i]);
				r[i]->biorq_flags |= (r[i]->biorq_flags & BIORQ_READ) ?
					BIORQ_READFAIL : BIORQ_RBWFAIL;
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
		bufp += tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);
	}

	/*
	 * Note that the offsets used here are file-wise offsets not
	 * offsets into the buffer.
	 */
	for (i = 0, tlen = 0, tsize = 0, bufp = buf;
	     i < nr; i++, bufp += tlen) {
		if (r[i] == MSL_BIORQ_COMPLETE)
			continue;

		/* Associate the biorq's with the mfh. */
		pll_addtail(&mfh->mfh_biorqs, r[i]);

		tlen = r[i]->biorq_len;

		if (r[i]->biorq_flags & BIORQ_DIO) {
			rc = msl_pages_dio_getput(r[i], bufp);
			if (rc == -SLERR_AIOWAIT) {
				msl_fsrqinfo_aioset(q);
				goto next_ioreq;
			}
			if (rc) {
				pll_remove(&mfh->mfh_biorqs, r[i]);
				rc = msl_offline_retry_ignexpire(r[i]);
				if (rc) {
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
			if (rc == -SLERR_AIOWAIT) {
				DEBUG_BIORQ(PLL_INFO, r[i], "SLERR_AIOWAIT");
				msl_fsrqinfo_aioset(q);
				goto next_ioreq;
			}
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
					r[i]->biorq_flags |= (r[i]->biorq_flags & BIORQ_READ) ?
						BIORQ_READFAIL : BIORQ_RBWFAIL;
					msl_biorq_destroy(r[i]);
					r[i] = NULL;
					goto restart;
				}
				rc = -EIO;
				goto out;
			}

			if (rw == SL_READ)
				tlen = msl_pages_copyout(r[i], bufp);
			else
				tlen = msl_pages_copyin(r[i], bufp);
		}
		tsize += tlen;
 next_ioreq:
		r[i] = MSL_BIORQ_COMPLETE;
	}
	/* Check for AIO in the fsrq prior to opening the fsrq for async
	 *    operation.  Otherwise a race condition is possible where the
	 *    async handler will unset the 'aio' flag, making this ioreq
	 *    look like a success.  The 'rc' is not used since more than
	 *    one BIORQ may be involved in this operation.
	 */
	if (msl_fsrqinfo_aioisset(q))
		rc = -SLERR_AIOWAIT;
	else
		rc = tsize;

	msl_fsrqinfo_readyset(q);

#if 0
	if (rw == SL_WRITE)
		fcmh_setlocalsize(f, off + size);
#endif

 out:
	if (bref)
		bmap_op_done_type(bref, BMAP_OPCNT_BIORQ);

	/* Note:  this is 'error' case.  For successful ops, 
	 *   msl_biorq_destroy called from msl_pages_copy[in|out]. 
	 */
	for (i = 0; i < nr; i++)
		if (r[i] && r[i] != MSL_BIORQ_COMPLETE) {
			r[i]->biorq_flags |= BIORQ_BMAPFAIL;
			msl_biorq_destroy(r[i]);
		}
	return (rc);
}
