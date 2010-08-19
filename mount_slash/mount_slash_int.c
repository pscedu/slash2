/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/pfl.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
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

/* async RPC pointers */
#define MSL_CB_POINTER_SLOT_BMPCES	0
#define MSL_CB_POINTER_SLOT_CSVC	1
#define MSL_CB_POINTER_SLOT_BIORQ	2
#define MSL_CB_POINTER_SLOT_BIORQS	3

/* Flushing fuse threads wait here for I/O completion. */
struct psc_waitq msl_fhent_flush_waitq = PSC_WAITQ_INIT;

struct timespec msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

__static void
msl_biorq_build(struct bmpc_ioreq **newreq, struct bmapc_memb *b,
		struct msl_fhent *mfh, uint32_t off, uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *bmpce, *bmpce_tmp;
	int i, npages=0, rbw=0;
	uint64_t bmpce_pagemap=0;
	off_t origoff=off;

	DEBUG_BMAP(PLL_TRACE, b, "adding req for (off=%u) (size=%u)",
		   off, len);

	psc_assert(len);
	psc_assert((off + len) <= SLASH_BMAP_SIZE);
	psc_assert(op == BIORQ_WRITE || op == BIORQ_READ);
	*newreq = r = PSCALLOC(sizeof(struct bmpc_ioreq));

	bmpc_ioreq_init(r, off, len, op, b, mfh);
	/* Take a ref on the bmap now so that it won't go away before
	 *   pndg IO's complete.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

	if (b->bcm_mode & BMAP_DIO) {
		/* The bmap is set to use directio, we may then skip
		 *   cache preparation.
		 */
		psc_assert(r->biorq_flags & BIORQ_DIO);
		goto out;
	}

	bmpc = bmap_2_bmpc(b);
	/* How many pages are needed to accommodate the request?
	 *   Determine and record whether RBW operations are needed on
	 *   the first or last pages.
	 */
	if (off & BMPC_BUFMASK) {
		/* Align the offset to the beginning of the first
		 *   buffer region and increase the len by that amount.
		 */
		len += off & BMPC_BUFMASK;
		off &= ~BMPC_BUFMASK;
		rbw = BIORQ_RBWFP;
	}
	npages = len / BMPC_BUFSZ;

	if (len % BMPC_BUFSZ) {
		npages++;
		if (npages == 1 && !rbw)
			rbw = BIORQ_RBWFP;
		else if (npages > 1)
			rbw |= BIORQ_RBWLP;
	}

	psc_assert(npages <= BMPC_IOMAXBLKS);

	bmpce_tmp = PSCALLOC(sizeof(*bmpce_tmp));
	/* Lock the bmap's page cache and try to locate cached pages
	 *   which correspond to this request.
	 */
	BMPC_LOCK(bmpc);
	for (i=0; i < npages; i++) {
		bmpce_tmp->bmpce_off = off + (i * BMPC_BUFSZ);
		bmpce = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
				   bmpce_tmp);
		if (bmpce) {
			BMPCE_LOCK(bmpce);
			/* Increment the ref cnt via the lru mgmt
			 *   function.
			 */
			bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
			if (!i && (bmpce->bmpce_flags & BMPCE_DATARDY) &&
			    (rbw & BIORQ_RBWFP))
				rbw &= ~BIORQ_RBWFP;
			else if (i == (npages - 1) &&
				 (bmpce->bmpce_flags & BMPCE_DATARDY) &&
				 (rbw & BIORQ_RBWLP))
				rbw &= ~BIORQ_RBWLP;

			BMPCE_ULOCK(bmpce);
			/* Mark this cache block as being already present
			 *   in the cache.
			 */
			bmpce_pagemap |= (1 << i);
			psc_dynarray_add(&r->biorq_pages, bmpce);
		}
	}
	BMPC_ULOCK(bmpc);
	/* Drop the lock and now try to allocate any needed bmpce's from
	 *    the pool.  Free the bmpce_tmp, it's no longer needed.
	 */
	PSCFREE(bmpce_tmp);
	/* Obtain any bmpce's which were not already present in the cache.
	 */
	for (i=0; i < npages; i++) {
		if (bmpce_pagemap & (1 << i))
			continue;

		bmpce = psc_pool_get(bmpcePoolMgr);
		bmpce_useprep(bmpce, r);
		bmpce->bmpce_off = off + (i * BMPC_BUFSZ);
		/* Atomically lookup and add the bmpce to the tree
		 *   if one of the same offset isn't already there.
		 */
		BMPC_LOCK(bmpc);
		bmpce_tmp = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
				       bmpce);
		if (!bmpce_tmp)
			SPLAY_INSERT(bmap_pagecachetree, &bmpc->bmpc_tree,
				     bmpce);
		else {
			bmpce_init(bmpcePoolMgr, bmpce);
			psc_pool_return(bmpcePoolMgr, bmpce);
			bmpce = bmpce_tmp;
		}
		BMPCE_LOCK(bmpce);
		bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
		BMPCE_ULOCK(bmpce);
		BMPC_ULOCK(bmpc);
		psc_dynarray_add(&r->biorq_pages, bmpce);
	}
	psc_assert(psc_dynarray_len(&r->biorq_pages) == npages);
	/* Sort the list by offset.
	 */
	if (npages > 1)
		psc_dynarray_sort(&r->biorq_pages, qsort, bmpce_sort_cmp);

	/* XXX Note if we moved to RD_DATARDY / WR_DATARDY then
	 *   we wouldn't have to fault in pages like this unless the
	 *   bmap was open in RW mode.
	 * XXX changeme.. this is causing unnecessary RBW rpc's!
	 *    key off the BMPCE flags
	 */
	if ((fcmh_getsize(b->bcm_fcmh) > origoff) &&
	    op == BIORQ_WRITE && rbw) {
		DEBUG_FCMH(PLL_NOTIFY, b->bcm_fcmh,
			   "setting RBW for biorq=%p", r);
		r->biorq_flags |= rbw;
	}

	/* Pass1: Retrieve memory pages from the cache on behalf of our pages
	 *   stuck in GETBUF.
	 */
	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		/* Only this thread may assign a buffer to the bmpce.
		 */
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    (bmpce->bmpce_flags & BMPCE_GETBUF)) {
			void *tmp;

			/* Increase the rdref cnt in preparation for any
			 *   RBW ops but only on new pages owned by this
			 *   page cache entry.  For now bypass
			 *   bmpce_handle_lru_locked() for this op.
			 *  XXX how is this ref affected by the cb's?
			 */
			if (bmpce_is_rbw_page(r, bmpce, i)) {
				bmpce->bmpce_flags |= BMPCE_RBWPAGE;
				psc_atomic16_inc(&bmpce->bmpce_rdref);
			}

			psc_assert(!bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_GETBUF);
			BMPCE_ULOCK(bmpce);

			tmp = bmpc_alloc();

			BMPCE_LOCK(bmpce);
			bmpce->bmpce_base = tmp;
			bmpce->bmpce_flags &= ~BMPCE_GETBUF;
			psc_waitq_wakeall(bmpce->bmpce_waitq);
			BMPCE_ULOCK(bmpce);

		} else {
			/* Don't bother prefetching blocks for unaligned
			 *   requests if another request is already
			 *   responsible for that block.
			 */
			if (!i && (r->biorq_flags & BIORQ_RBWFP))
				r->biorq_flags &= ~BIORQ_RBWFP;

			else if (i == (npages-1) &&
				 (r->biorq_flags & BIORQ_RBWLP))
				r->biorq_flags &= ~BIORQ_RBWLP;

			BMPCE_ULOCK(bmpce);
		}
	}

	/* Pass2: Sanity Check
	 */
	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);

		if (op == BIORQ_WRITE)
			psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
		else
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);
		if (biorq_is_my_bmpce(r, bmpce)) {
			/* The page is my reponsibility, ensure a cache
			 *   block has been assigned.
			 */
			psc_assert(bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
			bmpce->bmpce_flags &= ~BMPCE_INIT;

			if (op == BIORQ_READ)
				bmpce->bmpce_flags |= BMPCE_READPNDG;
		}
		BMPCE_ULOCK(bmpce);
	}
 out:
	DEBUG_BIORQ(PLL_NOTIFY, r, "new req");
	if (op == BIORQ_READ)
		pll_add(&bmap_2_bmpc(b)->bmpc_pndg_biorqs, r);
	else
		pll_add(&bmap_2_bmpc(b)->bmpc_new_biorqs, r);
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
	psc_assert(psclist_conjoint(&r->biorq_lentry));
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_WRITE && !(r->biorq_flags & BIORQ_DIO)) {
		atomic_dec(&bmpc->bmpc_pndgwr);
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) >= 0);

		if (atomic_read(&bmpc->bmpc_pndgwr))
			psc_assert(b->bcm_mode & BMAP_DIRTY);

		else {
			b->bcm_mode &= ~BMAP_DIRTY;
			/* Don't assert pll_empty(&bmpc->bmpc_pndg)
			 *   since read requests may be present.
			 */
			DEBUG_BMAP(PLL_INFO, b, "unset DIRTY nitems_pndg(%d)",
			   pll_nitems(&bmpc->bmpc_pndg_biorqs));
		}
	}

	if (!bmpc_queued_ios(bmpc) && !(b->bcm_mode & BMAP_CLI_FLUSHPROC) &&
	    !(b->bcm_mode & BMAP_REAPABLE)) {
		psc_assert(!atomic_read(&bmpc->bmpc_pndgwr));
		b->bcm_mode |= BMAP_REAPABLE;
		lc_addtail(&bmapTimeoutQ, bmap_2_msbd(b));
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
	struct bmap_pagecache *bmpc=bmap_2_bmpc(r->biorq_bmap);
	int i;

	psc_assert(r->biorq_flags & BIORQ_DESTROY);
	psc_assert(!(r->biorq_flags & BIORQ_INFL));

	BMPC_LOCK(bmpc);
	for (i=0; i < psc_dynarray_len(&r->biorq_pages); i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);

		bmpce_handle_lru_locked(bmpce, bmpc,
		 (r->biorq_flags & BIORQ_WRITE) ? BIORQ_WRITE : BIORQ_READ, 0);

		BMPCE_ULOCK(bmpce);
	}
	BMPC_ULOCK(bmpc);
}

__static void
msl_biorq_destroy(struct bmpc_ioreq *r)
{
	struct msl_fhent *f=r->biorq_fhent;
#if FHENT_EARLY_RELEASE
	int fhent=1;
#endif

	spinlock(&r->biorq_lock);

	/* Reads req's have their BIORQ_SCHED and BIORQ_INFL flags
	 *    cleared in msl_readio_cb to unblock waiting
	 *    threads at the earliest possible moment.
	 */
	if (!(r->biorq_flags & BIORQ_DIO)) {
		if (r->biorq_flags & BIORQ_WRITE) {
			psc_assert(r->biorq_flags & BIORQ_INFL);
			psc_assert(r->biorq_flags & BIORQ_SCHED);
			r->biorq_flags &= ~(BIORQ_INFL|BIORQ_SCHED);
		} else {
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			psc_assert(!(r->biorq_flags & BIORQ_SCHED));
		}
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
	while (atomic_read(&r->biorq_waitq.wq_nwaiters)) {
		psc_waitq_wakeall(&r->biorq_waitq);
		sched_yield();
	}

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
	spinlock(&f->mfh_lock);
	pll_remove(&f->mfh_biorqs, r);
	psc_waitq_wakeall(&msl_fhent_flush_waitq);
	freelock(&f->mfh_lock);
#endif

	psc_dynarray_free(&r->biorq_pages);

	if (r->biorq_rqset)
		pscrpc_set_destroy(r->biorq_rqset);

	psc_assert(!atomic_read(&r->biorq_waitq.wq_nwaiters));

	PSCFREE(r);
}

struct msl_fhent *
msl_fhent_new(struct fidc_membh *f)
{
	struct msl_fhent *mfh;

	fcmh_op_start_type(f, FCMH_OPCNT_OPEN);

	mfh = PSCALLOC(sizeof(*mfh));
	mfh->mfh_fcmh = f;
	LOCK_INIT(&mfh->mfh_lock);
	pll_init(&mfh->mfh_biorqs, struct bmpc_ioreq, biorq_mfh_lentry,
	    &mfh->mfh_lock);
	return (mfh);
}

/**
 * bmapc_memb_init - initialize a bmap structure.
 * @b: the bmap struct
 */
void
msl_bmap_init(struct bmapc_memb *b)
{
	struct bmap_cli_info *msbd;

	msbd = b->bcm_pri;
	msbd->msbd_bmap = b;
	bmpc_init(&msbd->msbd_bmpc);
}

void
bmap_biorq_expire(struct bmapc_memb *b)
{
	struct bmpc_ioreq *biorq;

	BMPC_LOCK(bmap_2_bmpc(b));
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_new_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_DEBUG, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}

	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_pndg_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_DEBUG, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}
	BMPC_ULOCK(bmap_2_bmpc(b));
}

__static void
bmap_biorq_waitempty(struct bmapc_memb *b)
{
	BMAP_LOCK(b);
	bcm_wait_locked(b, (!pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs) ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs)  ||
			    (b->bcm_mode & BMAP_CLI_FLUSHPROC)));

	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs));
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

	psc_assert(b->bcm_mode & BMAP_DIO);

	bmap_biorq_expire(b);
	bmap_biorq_waitempty(b);

	bmpc_lru_del(bmpc);

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}

void
msl_bmap_final_cleanup(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	bmap_biorq_waitempty(b);

	/* Mind lock ordering, remove from LRU first.
	 */
	bmpc_lru_del(bmpc);

	BMAP_LOCK(b);
	psc_assert(b->bcm_mode & BMAP_CLOSING);
	psc_assert(!(b->bcm_mode & BMAP_DIRTY));
	/* Assert that this bmap can no longer be scheduled by the
	 *   write back cache thread.
	 */
	psc_assert(psclist_disjoint(&bmap_2_msbd(b)->msbd_lentry));
	/* Assert that this thread cannot be seen by the page cache
	 *   reaper (it was lc_remove'd above by bmpc_lru_del()).
	 */
	psc_assert(psclist_disjoint(&bmpc->bmpc_lentry));
	BMAP_ULOCK(b);

	DEBUG_BMAP(PLL_INFO, b, "freeing");

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}


void
msl_bmap_reap_init(struct bmapc_memb *bmap, const struct srt_bmapdesc *sbd)
{
	struct bmap_cli_info *msbd=bmap->bcm_pri;
	int locked;

	locked = BMAP_RLOCK(bmap);

	msbd->msbd_sbd = *sbd;
	/* Record the start time,
	 *  XXX the directio status of the bmap needs to be returned by the
	 *     mds so we can set the proper expiration time.
	 */
	clock_gettime(CLOCK_REALTIME, &msbd->msbd_xtime);

	timespecadd(&msbd->msbd_xtime, &msl_bmap_timeo_inc,
	    &msbd->msbd_etime);
	timespecadd(&msbd->msbd_xtime, &msl_bmap_max_lease,
	    &msbd->msbd_xtime);

	/* Take the reaper ref cnt early and place the bmap
	 *    onto the reap list
	 */
	bmap->bcm_mode |= BMAP_REAPABLE;
	bmap_op_start_type(bmap, BMAP_OPCNT_REAPER);

	BMAP_URLOCK(bmap, locked);
	/* Add ourselves here, otherwise zero length files
	 *   will not be removed.
	 */
	lc_addtail(&bmapTimeoutQ, msbd);
}

/**
 * msl_bmap_retrieve - perform a blocking 'get' operation to retrieve
 *    one or more bmaps from the MDS.
 * @f: pointer to the fid cache structure to which this bmap belongs.
 * @b: the block id to retrieve (block size == SLASH_BMAP_SIZE).
 * @n: the number of bmaps to retrieve (serves as a simple read-ahead mechanism)
 */
int
msl_bmap_retrieve(struct bmapc_memb *bmap, enum rw rw)
{
	int rc, nretries = 0, getreptbl = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *msbd;
	struct srm_getbmap_req *mq;
	struct srm_getbmap_rep *mp;
	struct fcmh_cli_info *fci;
	struct fidc_membh *f;

	psc_assert(bmap->bcm_mode & BMAP_INIT);
	psc_assert(bmap->bcm_pri);
	psc_assert(bmap->bcm_fcmh);

	f = bmap->bcm_fcmh;
	fci = fcmh_2_fci(f);

 retry:
	FCMH_LOCK(f);
	if ((f->fcmh_state & (FCMH_CLI_HAVEREPLTBL |
	    FCMH_CLI_FETCHREPLTBL)) == 0) {
		f->fcmh_state |= FCMH_CLI_FETCHREPLTBL;
		getreptbl = 1;
	}
	FCMH_ULOCK(f);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_GETBMAP, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = f->fcmh_fg;
	mq->prefios = prefIOS; /* Tell MDS of our preferred ION */
	mq->bmapno = bmap->bcm_bmapno;
	mq->rw = rw;
	if (getreptbl)
		mq->flags |= SRM_GETBMAPF_GETREPLTBL;

	msbd = bmap->bcm_pri;

	DEBUG_FCMH(PLL_DEBUG, f, "retrieving bmap (bmapno=%u) (rw=%d)",
	    bmap->bcm_bmapno, rw);

	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0) {
		rc = mp->rc;
		if (mp->rc == SLERR_BMAP_DIOWAIT) {
			/* Retry for bmap to be DIO ready.
			 */
			DEBUG_BMAP(PLL_WARN, bmap, "SLERR_BMAP_DIOWAIT (rt=%d)",
				   nretries);
			rc = mp->rc;
			goto out;
		} else
			memcpy(&msbd->msbd_msbcr, &mp->bcw.crcstates,
			       sizeof(struct msbmap_crcrepl_states));
	} else
		goto out;

	FCMH_LOCK(f);

	msl_bmap_reap_init(bmap, &mp->sbd);

	if (getreptbl) {
		/* XXX don't forget that on write we need to invalidate
		 *   the local replication table..
		 */
		fci->fci_nrepls = mp->nrepls;
		memcpy(&fci->fci_reptbl, &mp->reptbl,
		       sizeof(sl_replica_t) * SL_MAX_REPLICAS);
		f->fcmh_state |= FCMH_CLI_HAVEREPLTBL;
		psc_waitq_wakeall(&f->fcmh_waitq);
	}

 out:
	FCMH_RLOCK(f);
	f->fcmh_state &= ~FCMH_CLI_FETCHREPLTBL;
	FCMH_ULOCK(f);
	if (rq) {
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc == SLERR_BMAP_DIOWAIT) {
		sleep(BMAP_CLI_DIOWAIT_SECS);
		if (nretries > (BMAP_CLI_MAX_LEASE * 2))
			return (-ETIMEDOUT);
		goto retry;
	}

	return (rc);
}

/**
 * msl_bmap_modeset - Set READ or WRITE as access mode on an open file
 *	block map.
 * @b: bmap.
 * @rw: access mode to set the bmap to.
 *
 * XXX have this take a bmapc_memb.
 *
 * Notes:  XXX I think this logic can be simplified when setting mode from
 *    WRONLY to RDWR.  In WRONLY this client already knows the address
 *    of the only ION from which this bmap can be read.  Therefore, it
 *    should be able to interface with that ION without intervention from
 *    the mds.
 */
__static int
msl_bmap_modeset(struct bmapc_memb *b, enum rw rw)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	int rc, nretries=0;

	psc_assert(rw == SL_WRITE || rw == SL_READ);
 retry:
	psc_assert(b->bcm_mode & BMAP_MDCHNG);

	if (b->bcm_mode & BMAP_WR)
		/* Write enabled bmaps are allowed to read with no
		 *   further action being taken.
		 */
		return (0);

	/* Add write mode to this bmap.
	 */
	psc_assert(rw == SL_WRITE && (b->bcm_mode & BMAP_RD));

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMC_VERSION,
	    SRMT_BMAPCHWRMODE, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(struct srt_bmapdesc));
	mq->prefios = prefIOS;
	rc = SL_RSX_WAITREP(rq, mp);
	if (rc == 0)
		rc = mp->rc;

	if (rc == 0)
		memcpy(bmap_2_sbd(b), &mp->sbd,
		    sizeof(struct srt_bmapdesc));

 out:
	if (rq) {
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc == SLERR_BMAP_DIOWAIT) {
		DEBUG_BMAP(PLL_WARN, b, "SLERR_BMAP_DIOWAIT rt=%d", nretries);
		sleep(BMAP_CLI_DIOWAIT_SECS);
		if (nretries > (BMAP_CLI_MAX_LEASE * 2))
			return (-ETIMEDOUT);
		goto retry;
	}

	return (rc);
}

struct bmapc_memb *
msl_bmap_load(struct msl_fhent *mfh, sl_bmapno_t n, enum rw rw)
{
	struct bmapc_memb *b;

	psc_assert(rw == SL_READ || rw == SL_WRITE);

	return (bmap_get(mfh->mfh_fcmh, n, rw, &b) ? NULL : b);
}

/**
 * msl_bmap_to_csvc - Given a bmap, perform a series of lookups to
 *	locate the ION csvc.  The ION was chosen by the mds and
 *	returned in the msl_bmap_retrieve routine. msl_bmap_to_csvc
 *	queries the configuration to find the ION's private info - this
 *	is where the import pointer is kept.  If no import has yet been
 *	allocated a new is made.
 * @b: the bmap
 * Notes: the bmap is locked to avoid race conditions with import checking.
 *        the bmap's refcnt must have been incremented so that it is not freed from under us.
 * XXX Dev Needed: If the bmap is a read-only then any replica may be
 *	accessed (so long as it is recent).  Therefore
 *	msl_bmap_to_csvc() should have logic to accommodate this.
 */
struct slashrpc_cservice *
msl_bmap_to_csvc(struct bmapc_memb *b, int exclusive)
{
	struct slashrpc_cservice *csvc;
	struct sl_resm *resm;
	int locked;

	/* Sanity check on the opcnt.
	 */
	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	locked = reqlock(&b->bcm_lock);
	resm = libsl_nid2resm(bmap_2_ion(b));
	psc_assert(resm->resm_nid == bmap_2_ion(b));
	ureqlock(&b->bcm_lock, locked);

	if (exclusive) {
		csvc = slc_geticsvc(resm);
		if (csvc)
			psc_assert(csvc->csvc_import->imp_connection->
			    c_peer.nid == bmap_2_ion(b));
	} else {
#if 0
		/* grab a random resm from any replica */
		for (n = 0; n < resm->resm_res->res_nnids; n++)
			for (j = 0; j < resm->resm_res->res_nnids; j++) {
				csvc = slc_geticsvc(resm);
				if (csvc)
					return (csvc->csvc_import);
			}
#endif
		csvc = slc_geticsvc(resm);
	}
	return (csvc);
}

struct slashrpc_cservice *
msl_try_get_replica_resm(struct bmapc_memb *bcm, int iosidx)
{
	struct slashrpc_cservice *csvc;
	struct bmap_cli_info *msbd;
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct sl_resm *resm;
	int j, rnd, nios;

	fci = fcmh_2_fci(bcm->bcm_fcmh);
	msbd = bcm->bcm_pri;

	if (SL_REPL_GET_BMAP_IOS_STAT(msbd->msbd_msbcr.msbcr_repls,
	    iosidx * SL_BITS_PER_REPLICA) != BREPLST_VALID)
		return (NULL);

	res = libsl_id2res(fci->fci_reptbl[iosidx].bs_id);

	nios = psc_dynarray_len(&res->res_members);
	rnd = psc_random32u(nios);
	for (j = 0; j < nios; j++, rnd++) {
		if (rnd >= nios)
			rnd = 0;
		resm = psc_dynarray_getpos(&res->res_members, rnd);
		csvc = slc_geticsvc(resm);
		if (csvc)
			return (csvc);
	}
	return (NULL);
}

struct slashrpc_cservice *
msl_bmap_choose_replica(struct bmapc_memb *b)
{
	struct slashrpc_cservice *csvc;
	struct fcmh_cli_info *fci;
	int n, rnd;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	fci = fcmh_get_pri(b->bcm_fcmh);

	/* first, try preferred IOS */
	rnd = psc_random32u(fci->fci_nrepls);
	for (n = 0; n < fci->fci_nrepls; n++, rnd++) {
		if (rnd >= fci->fci_nrepls)
			rnd = 0;

		if (fci->fci_reptbl[rnd].bs_id == prefIOS) {
			csvc = msl_try_get_replica_resm(b, rnd);
			if (csvc)
				return (csvc);
		}
	}

	/* rats, not available; try anyone available now */
	rnd = psc_random32u(fci->fci_nrepls);
	for (n = 0; n < fci->fci_nrepls; n++) {
		if (rnd >= fci->fci_nrepls)
			rnd = 0;

		csvc = msl_try_get_replica_resm(b, rnd);
		if (csvc)
			return (csvc);
	}
	return (NULL);
}

/**
 * msl_readio_cb - rpc callback used only for read or RBW operations.
 *    The primary purpose is to set the bmpce's to DATARDY so that other
 *    threads waiting for DATARDY may be unblocked.
 *  Note: Unref of the biorq will happen after the pages have been
 *     copied out to the applicaton buffers.
 */
int
msl_readio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct psc_dynarray *a = args->pointer_arg[MSL_CB_POINTER_SLOT_BMPCES];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CB_POINTER_SLOT_BIORQ];
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CB_POINTER_SLOT_CSVC];
	struct bmapc_memb *b;
	struct bmap_pagecache_entry *bmpce;
	int op=rq->rq_reqmsg->opc, i, rc;
	int clearpages=0;

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		goto out;

	b = r->biorq_bmap;
	psc_assert(b);

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);
	psc_assert(a);

	DEBUG_BMAP(PLL_INFO, b, "callback");
	DEBUG_BIORQ(PLL_INFO, r, "callback bmap=%p", b);

	if (rq->rq_status && rq->rq_status != -ENOENT) {
		if (rq->rq_status == -ENOENT)
			clearpages = 0;
		else {
			DEBUG_REQ(PLL_ERROR, rq, "non-zero status status %d",
				  rq->rq_status);
			psc_fatalx("Resolve issues surrounding this failure");
			// XXX Freeing of dynarray, bmpce's, etc
			rc = rq->rq_status;
			goto out;
		}
	}

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);

	/* Call the inflight CB only on the iov's in the dynarray -
	 *   not the iov's in the request since some of those may
	 *   have already been staged in.
	 */
	for (i=0; i < psc_dynarray_len(a); i++) {
		bmpce = psc_dynarray_getpos(a, i);
		BMPCE_LOCK(bmpce);

		DEBUG_BMPCE(PLL_INFO, bmpce, "DATARDY! i=%d len=%d",
			    i, psc_dynarray_len(a));

		psc_assert(bmpce->bmpce_waitq);
		psc_assert(biorq_is_my_bmpce(r, bmpce));

		bmpce->bmpce_flags |= BMPCE_DATARDY;
		if (bmpce->bmpce_flags & BMPCE_RBWPAGE) {
			/* The RBW stuff needs to be managed outside of
			 *   the LRU, this is not the best place but should
			 *   suffice for now.
			 */
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) == 1);
			psc_atomic16_dec(&bmpce->bmpce_rdref);
			bmpce->bmpce_flags &= ~BMPCE_RBWPAGE;
			DEBUG_BMPCE(PLL_INFO, bmpce, "infl dec for RBW");
		}

		if (clearpages) {
			DEBUG_BMPCE(PLL_WARN, bmpce, "clearing page");
			memset(bmpce->bmpce_base, 0, BMPC_BUFSZ);
		}

		DEBUG_BMPCE(PLL_INFO, bmpce, "datardy via readio_cb");

		/* Disown the bmpce by null'ing the waitq pointer.
		 */
		bmpce->bmpce_waitq = NULL;
		BMPCE_ULOCK(bmpce);
	}
	freelock(&r->biorq_lock);

	/* Free the dynarray which was allocated in msl_readio_rpc_create().
	 */
	psc_dynarray_free(a);
	PSCFREE(a);

 out:
	sl_csvc_decref(csvc);
	return (rc);
}

int
msl_io_rpcset_cb(__unusedx struct pscrpc_request_set *set, void *arg, int rc)
{
	struct psc_dynarray *biorqs = arg;
	struct bmpc_ioreq *r;
	int i;

	for (i=0; i < psc_dynarray_len(biorqs); i++) {
		r = psc_dynarray_getpos(biorqs, i);
		msl_biorq_destroy(r);
	}
	psc_dynarray_free(biorqs);
	PSCFREE(biorqs);

	return (rc);
}

int
msl_io_rpc_cb(__unusedx struct pscrpc_request *req, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[0];
	struct psc_dynarray *biorqs = args->pointer_arg[1];
	struct bmpc_ioreq *r;
	int i;

	DEBUG_REQ(PLL_INFO, req, "biorqs=%p len=%d",
		  biorqs, psc_dynarray_len(biorqs));

	DYNARRAY_FOREACH(r, i, biorqs)
		msl_biorq_destroy(r);
	psc_dynarray_free(biorqs);
	PSCFREE(biorqs);

	sl_csvc_decref(csvc);

	return (0);
}

int
msl_dio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct srm_io_req *mq;
	int rc, op=rq->rq_reqmsg->opc;

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		goto out;

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	psc_assert(mq);

	DEBUG_REQ(PLL_TRACE, rq, "completed dio req (op=%d) o=%u s=%u",
	    op, mq->offset, mq->size);

 out:
	return (rc);
}

__static void
msl_pages_dio_getput(struct bmpc_ioreq *r, char *b)
{
	struct slashrpc_cservice  *csvc;
	struct pscrpc_request     *req;
	struct pscrpc_bulk_desc   *desc;
	struct bmapc_memb	  *bcm;
	struct bmap_cli_info	  *msbd;
	struct iovec              *iovs;
	struct srm_io_req         *mq;
	struct srm_io_rep         *mp;

	size_t len, nbytes, size=r->biorq_len;
	int i, op, n=0, rc=1;

	psc_assert(r->biorq_flags & BIORQ_DIO);
	psc_assert(r->biorq_bmap);
	psc_assert(size);

	bcm = r->biorq_bmap;
	msbd = bcm->bcm_pri;

	DEBUG_BIORQ(PLL_TRACE, r, "dio req");

	op = r->biorq_flags & BIORQ_WRITE ?
		SRMT_WRITE : SRMT_READ;

	csvc = (op == SRMT_WRITE) ?
		msl_bmap_to_csvc(bcm, 1) :
		msl_bmap_choose_replica(bcm);

	r->biorq_rqset = pscrpc_prep_set();
	/* This buffer hasn't been segmented into LNET sized
	 *  chunks.  Set up buffers into 1MB chunks or smaller.
	 */
	n = (r->biorq_len / LNET_MTU) + ((r->biorq_len % LNET_MTU) ? 1 : 0);
	iovs = PSCALLOC(sizeof(*iovs) * n);

	for (i=0, nbytes=0; i < n; i++, nbytes += len) {
		len = MIN(LNET_MTU, (size-nbytes));

		rc = SL_RSX_NEWREQ(csvc->csvc_import, SRIC_VERSION, op, req, mq, mp);
		if (rc)
			psc_fatalx("SL_RSX_NEWREQ() failed %d", rc);

		req->rq_interpret_reply = msl_dio_cb;

		iovs[i].iov_base = b + nbytes;
		iovs[i].iov_len  = len;

		rc = rsx_bulkclient(req, &desc,
				    (op == SRMT_WRITE ?
				     BULK_GET_SOURCE : BULK_PUT_SINK),
				    SRIC_BULK_PORTAL, &iovs[i], 1);
		if (rc)
			psc_fatalx("rsx_bulkclient() failed %d", rc);

		mq->offset = r->biorq_off + nbytes;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		memcpy(&mq->sbd, &msbd->msbd_sbd, sizeof(mq->sbd));

		authbuf_sign(req, PSCRPC_MSG_REQUEST);
		pscrpc_set_add_new_req(r->biorq_rqset, req);
		if (pscrpc_push_req(req)) {
			DEBUG_REQ(PLL_ERROR, req, "pscrpc_push_req() failed");
			psc_fatalx("pscrpc_push_req(), no failover yet");
		}
	}
	/* Should be no need for a callback since this call is fully
	 *   blocking.
	 */
	psc_assert(nbytes == size);
	pscrpc_set_wait(r->biorq_rqset);
	pscrpc_set_destroy(r->biorq_rqset);
	r->biorq_rqset = NULL;
	PSCFREE(iovs);

	msl_biorq_destroy(r);

	sl_csvc_decref(csvc);
}

__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);
	/* This req must already be attached to the cache.
	 *   The BIORQ_FLUSHRDY bit prevents the request
	 *   from being processed prematurely.
	 */
	spinlock(&r->biorq_lock);
	r->biorq_flags |= BIORQ_FLUSHRDY;
	DEBUG_BIORQ(PLL_DEBUG, r, "BIORQ_FLUSHRDY");
	psc_assert(psclist_conjoint(&r->biorq_lentry));
	atomic_inc(&bmpc->bmpc_pndgwr);
	freelock(&r->biorq_lock);

	if (b->bcm_mode & BMAP_DIRTY) {
		/* If the bmap is already dirty then at least
		 *   one other writer must be present.
		 */
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 1);
		psc_assert((pll_nitems(&bmpc->bmpc_pndg_biorqs) +
			    pll_nitems(&bmpc->bmpc_new_biorqs)) > 1);

	} else {
		if (b->bcm_mode & BMAP_REAPABLE) {
			LIST_CACHE_LOCK(&bmapTimeoutQ);
			b->bcm_mode &= ~BMAP_REAPABLE;
			psc_assert(!(b->bcm_mode & BMAP_DIRTY));

			if (psclist_conjoint(&bmap_2_msbd(b)->msbd_lentry))
				lc_remove(&bmapTimeoutQ, bmap_2_msbd(b));
			LIST_CACHE_ULOCK(&bmapTimeoutQ);
		}

		b->bcm_mode |= BMAP_DIRTY;

		if (!(b->bcm_mode & BMAP_CLI_FLUSHPROC)) {
			/* Give control of the msdb_lentry to the bmap_flush
			 *   thread.
			 */
			b->bcm_mode |= BMAP_CLI_FLUSHPROC;
			psc_assert(psclist_disjoint(&bmap_2_msbd(b)->msbd_lentry));
			lc_addtail(&bmapFlushQ, bmap_2_msbd(b));
		}
	}

	DEBUG_BMAP(PLL_INFO, b, "biorq=%p list_empty(%d)",
		   r, pll_empty(&bmpc->bmpc_pndg_biorqs));
	BMPC_ULOCK(bmpc);
	BMAP_ULOCK(b);
}

__static void
msl_readio_rpc_create(struct bmpc_ioreq *r, int startpage, int npages)
{
	struct bmap_pagecache_entry *bmpce;
	struct slashrpc_cservice *csvc;
	struct pscrpc_request *req;
	struct pscrpc_bulk_desc *desc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	struct psc_dynarray *a;
	int rc, i;

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	BMAP_LOCK(r->biorq_bmap);
	csvc = (r->biorq_bmap->bcm_mode & BMAP_WR) ?
		msl_bmap_to_csvc(r->biorq_bmap, 1) :
		msl_bmap_choose_replica(r->biorq_bmap);
	BMAP_ULOCK(r->biorq_bmap);

	psc_assert(csvc);

	rc = SL_RSX_NEWREQ(csvc->csvc_import, SRIC_VERSION, SRMT_READ,
	    req, mq, mp);
	if (rc)
		psc_fatalx("SL_RSX_NEWREQ() failed %d", rc);

	iovs = PSCALLOC(sizeof(*iovs) * npages);
	a = PSCALLOC(sizeof(a));
	psc_dynarray_init(a);

	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i+startpage);

		BMPCE_LOCK(bmpce);
		/* Sanity checks.
		 */
		psc_assert(biorq_is_my_bmpce(r, bmpce));
		/* BMPCE_DATARDY should not be set, otherwise we wouldn't
		 *   be here.
		 */
		psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
		bmpce_usecheck(bmpce, BIORQ_READ,
			       biorq_getaligned_off(r, (i+startpage)));

		DEBUG_BMPCE(PLL_DEBUG, bmpce, "adding to rpc");

		BMPCE_ULOCK(bmpce);

		iovs[i].iov_base = bmpce->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			mq->offset = bmpce->bmpce_off;
		psc_dynarray_add(a, bmpce);
	}

	rc = rsx_bulkclient(req, &desc, BULK_PUT_SINK, SRIC_BULK_PORTAL,
			    iovs, npages);
	PSCFREE(iovs);
	if (rc)
		psc_fatalx("rsx_bulkclient() failed %d", rc);

	mq->size = npages * BMPC_BUFSZ;
	mq->op = SRMIOP_RD;
	memcpy(&mq->sbd, &bmap_2_msbd(r->biorq_bmap)->msbd_sbd,
	    sizeof(mq->sbd));

	r->biorq_flags |= BIORQ_INFL;

	DEBUG_BIORQ(PLL_NOTIFY, r, "launching read req");

	/* XXX Using a set for any type of read may be overkill.
	 */
	if (!r->biorq_rqset)
		r->biorq_rqset = pscrpc_prep_set();

	authbuf_sign(req, PSCRPC_MSG_REQUEST);
	pscrpc_set_add_new_req(r->biorq_rqset, req);
	/* Setup the callback, supplying the dynarray as a argument.
	 */
	req->rq_interpret_reply = msl_readio_cb;
	req->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_BMPCES] = a;
	req->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_BIORQ] = r;
	req->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_CSVC] = csvc;
	if (pscrpc_push_req(req)) {
		DEBUG_REQ(PLL_ERROR, req,
			  "pscrpc_push_req() failed");
		psc_fatalx("pscrpc_push_req, no failover yet");
	}
}

__static void
msl_pages_prefetch(struct bmpc_ioreq *r)
{
	int i, npages, sched=0;
	struct bmapc_memb *bcm;
	struct bmap_cli_info *msbd;
	struct bmap_pagecache_entry *bmpce;

	if (!((r->biorq_flags & BIORQ_READ)  ||
	      (r->biorq_flags & BIORQ_RBWFP) ||
	      (r->biorq_flags & BIORQ_RBWLP)) ||
	    (r->biorq_flags & BIORQ_DIO))
		return;

	bcm    = r->biorq_bmap;
	msbd   = bcm->bcm_pri;
	npages = psc_dynarray_len(&r->biorq_pages);

	r->biorq_flags |= BIORQ_SCHED;

	DEBUG_BIORQ(PLL_NOTIFY, r, "check prefetch");

	psc_assert(!r->biorq_rqset);

	/* Only read in the pages owned by this request.  To do this
	 *   the below loop marks only the iov slots which correspond
	 *   to page cache entries owned by this request as determined
	 *   by biorq_is_my_bmpce().
	 */
	if (r->biorq_flags & BIORQ_READ) {
		int j=-1;

		for (i=0; i < npages; i++) {
			bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
			BMPCE_LOCK(bmpce);

			bmpce_usecheck(bmpce, BIORQ_READ,
				       biorq_getaligned_off(r, i));

			if (biorq_is_my_bmpce(r, bmpce))
				psc_assert(!(bmpce->bmpce_flags &
					     BMPCE_DATARDY));

			BMPCE_ULOCK(bmpce);

			/* Try to set the tail bmpce if it's not yet
			 *   assigned.
			 */
			if (j < 0) {
				if (biorq_is_my_bmpce(r, bmpce))
					j = i;
			} else {
				if (!biorq_is_my_bmpce(r, bmpce)) {
					msl_readio_rpc_create(r, j, i-j);
					j = -1;
					sched = 1;

				} else if ((i-j) == BMPC_MAXBUFSRPC) {
					msl_readio_rpc_create(r, j, i-j);
					j = i;
					sched = 1;
				}
			}
		}

		if (j >= 0) {
			/* Catch any unsent frags at the end of the array.
			 */
			msl_readio_rpc_create(r, j, i-j);
			sched = 1;
		}

	} else {
		if (r->biorq_flags & BIORQ_RBWFP) {
			bmpce = psc_dynarray_getpos(&r->biorq_pages, 0);

			if (biorq_is_my_bmpce(r, bmpce)) {
				psc_assert(!(bmpce->bmpce_flags &
					     BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				msl_readio_rpc_create(r, 0, 1);
				sched = 1;
			}
		}

		if (r->biorq_flags & BIORQ_RBWLP) {
			bmpce = psc_dynarray_getpos(&r->biorq_pages,
						    psc_dynarray_len(&r->biorq_pages)-1);

			if (biorq_is_my_bmpce(r, bmpce)) {
				psc_assert(!(bmpce->bmpce_flags &
					     BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				msl_readio_rpc_create(r,
						      psc_dynarray_len(&r->biorq_pages)-1, 1);
				sched = 1;
			}
		}
	}

	if (!sched)
		r->biorq_flags &= ~BIORQ_SCHED;
}

/**
 * msl_pages_blocking_load - manage data prefetching activities.  This
 *	includes waiting on other thread to complete RPC for data in
 *	which we're interested.
 */
__static int
msl_pages_blocking_load(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	int rc=0, i, npages=psc_dynarray_len(&r->biorq_pages);

	if (r->biorq_rqset) {
		rc = pscrpc_set_wait(r->biorq_rqset);
		if (rc)
			// XXX need to cleanup properly
			psc_fatalx("pscrpc_set_wait rc=%d", rc);
		/* The set cb is not being used, msl_readio_cb() is
		 *   called for every rpc in the set.  This was causing
		 *   the biorq to have its flags mod'd in an incorrect
		 *   fashion.  For now, the following lines will be moved
		 *   here.
		 */
		spinlock(&r->biorq_lock);
		r->biorq_flags &= ~(BIORQ_RBWLP|BIORQ_RBWFP|
				    BIORQ_INFL|BIORQ_SCHED);
		DEBUG_BIORQ(PLL_INFO, r, "readio cb complete");
		psc_waitq_wakeall(&r->biorq_waitq);
		freelock(&r->biorq_lock);
		/* Destroy and cleanup the set now.
		 */
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;

	}

	for (i=0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);
		DEBUG_BMPCE(PLL_TRACE, bmpce, " ");

		if (!biorq_is_my_bmpce(r, bmpce))
			/* For pages not owned by this request,
			 *    wait for them to become DATARDY.
			 */
			while (!(bmpce->bmpce_flags & BMPCE_DATARDY)) {
				DEBUG_BMPCE(PLL_TRACE, bmpce, "waiting");
				psc_waitq_wait(bmpce->bmpce_waitq,
					       &bmpce->bmpce_lock);
				BMPCE_LOCK(bmpce);
			}

		if ((r->biorq_flags & BIORQ_READ) ||
		    !biorq_is_my_bmpce(r, bmpce)  ||
		    bmpce_is_rbw_page(r, bmpce, i))
			/* Read requests must have had their bmpce's
			 *   put into DATARDY by now (i.e. all RPCs
			 *   must have already been completed).
			 *   Unaligned writes must have been faulted in,
			 *   same as any page owned by another request.
			 */
			psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		BMPCE_ULOCK(bmpce);
	}
	return (rc);
}

/**
 * msl_pages_copyin - copy user pages into buffer cache and schedule the
 *    slabs to be sent to the IOS backend.
 * @r: array of request structs.
 * @buf: the source (application) buffer.
 */
__static void
msl_pages_copyin(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	uint32_t toff, tsize, nbytes;
	int i, npages;
	char *sink, *src;

	src    = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;
	npages = psc_dynarray_len(&r->biorq_pages);

	for (i=0; i < npages; i++) {
		/* All pages are involved, therefore tsize should have value.
		 */
		psc_assert(tsize);

		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		/* Re-check RBW sanity.  The waitq pointer within the bmpce
		 *   must still be valid in order for this check to work.
		 */
		if (bmpce_is_rbw_page(r, bmpce, i))
			psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		/* Set the starting buffer pointer into
		 *  our cache vector.
		 */
		sink = (char *)bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			/* The first cache buffer pointer may need
			 *    a bump if the request offset is unaligned.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
				       (toff & ~BMPC_BUFMASK));
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			sink += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else {
			bmpce_usecheck(bmpce, BIORQ_WRITE, toff);
			nbytes = MIN(BMPC_BUFSZ, tsize);
		}

		DEBUG_BMPCE(PLL_NOTIFY, bmpce, "tsize=%u nbytes=%u toff=%u",
			    tsize, nbytes, toff);
		/* Do the deed.
		 */
		memcpy(sink, src, nbytes);
		/* If the bmpce belongs to this request and is not yet
		 *   DATARDY (ie wasn't an RBW block) then set DATARDY
		 *   and wakeup anyone who was blocked.  Note the waitq
		 *   pointer will hang around until the request has completed.
		 * Note:  wrrefs are held until the
		 */
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    !(bmpce->bmpce_flags & BMPCE_DATARDY)) {
			/* This should never happen as RBW pages should
			 *   have BMPCE_DATARDY already set.
			 */
			psc_assert(!bmpce_is_rbw_page(r, bmpce, i));
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			psc_waitq_wakeall(bmpce->bmpce_waitq);
			bmpce->bmpce_waitq = NULL;
		}
		BMPCE_ULOCK(bmpce);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	/* Queue these iov's for send to IOS.
	 */
	msl_pages_schedflush(r);
}

/**
 * msl_pages_copyout - copy pages to the user application buffer.
 */
__static void
msl_pages_copyout(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	uint32_t toff, tsize;
	size_t nbytes;
	int i, npages;
	char *sink, *src;

	sink   = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;
	npages = psc_dynarray_len(&r->biorq_pages);

	psc_assert(npages);

	for (i=0; i < npages; i++) {
		psc_assert(tsize);

		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(bmpce);

		psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		bmpce_usecheck(bmpce, BIORQ_READ, biorq_getaligned_off(r, i));

		src = (char *)bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			src += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		DEBUG_BMPCE(PLL_DEBUG, bmpce, "tsize=%u nbytes=%zu toff=%u",
			    tsize, nbytes, toff);

		memcpy(sink, src, nbytes);
		BMPCE_ULOCK(bmpce);

		toff  += nbytes;
		sink  += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	msl_biorq_destroy(r);
}

/**
 * msl_io - I/O gateway routine which bridges FUSE and the slash2 client
 *	cache and backend.  msl_io() handles the creation of biorq's
 *	and the loading of bmaps (which are attached to the file's
 *	fcache_memb_handle and is ultimately responsible for data being
 *	prefetched (as needed), copied into or from the cache, and (on
 *	write) being pushed to the correct io server.
 * @fh: file handle structure passed to us by FUSE which contains the
 *	pointer to our fcache_memb_handle *.
 * @buf: the application source/dest buffer.
 * @size: size of buffer.
 * @off: file logical offset similar to pwrite().
 * @rw: the operation type (SL_READ or SL_WRITE).
 */
int
msl_io(struct msl_fhent *mfh, char *buf, size_t size, off_t off, enum rw rw)
{
#define MAX_BMAPS_REQ 4
	struct bmpc_ioreq *r[MAX_BMAPS_REQ];
	struct bmapc_memb *b[MAX_BMAPS_REQ];
	sl_bmapno_t s, e;
	size_t tlen, tsize=size;
	off_t roff;
	int nr, j, rc;
	char *p;

	psc_assert(mfh);
	psc_assert(mfh->mfh_fcmh);

	if (!size || (rw == SL_READ &&
	    (uint64_t)off >= fcmh_2_fsz(mfh->mfh_fcmh))) {
		rc = 0;
		goto out;
	}

	/* Are these bytes in the cache?
	 *  Get the start and end block regions from the input parameters.
	 */
	s = off / SLASH_BMAP_SIZE;
	e = ((off + size) - 1) / SLASH_BMAP_SIZE;

	if ((e - s) > MAX_BMAPS_REQ)
		return (-EINVAL);
	/* Relativize the length and offset (roff is not aligned).
	 */
	roff  = off - (s * SLASH_BMAP_SIZE);
	/* Length of the first bmap request.
	 */
	tlen  = MIN((size_t)(SLASH_BMAP_SIZE - roff), size);
	/* Foreach block range, get its bmap and make a request into its
	 *  page cache.  This first loop retrieves all the pages.
	 */
	for (nr=0; s <= e; s++, nr++) {
		DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
		    "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT
		    " rw=%d", tsize, tlen, off, roff, rw);

		psc_assert(tsize);
		/* Load up the bmap, if it's not available then we're out of
		 *  luck because we have no idea where the data is!
		 */
		b[nr] = msl_bmap_load(mfh, s, rw);
		if (!b[nr]) {
			rc = -EIO;
			DEBUG_FCMH(PLL_ERROR, mfh->mfh_fcmh,
			    "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT
			    " rw=%d rc=%d", tsize, tlen, off, roff, rw, rc);
			goto out;
		}

		msl_biorq_build(&r[nr], b[nr], mfh, (roff - (nr * SLASH_BMAP_SIZE)),
		    tlen, (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE);
		/* Start prefetching our cached buffers.
		 */
		msl_pages_prefetch(r[nr]);

		roff += tlen;
		tsize -= tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);

		BMAP_CLI_BUMP_TIMEO(b[nr]);
	}

	/* Note that the offsets used here are file-wise offsets not
	 *   offsets into the buffer.
	 */
	for (j=0, p=buf; j < nr; j++, p+=tlen) {
		/* Associate the biorq's with the mfh.
		 */
		pll_addtail(&mfh->mfh_biorqs, r[j]);

		if (r[j]->biorq_flags & BIORQ_DIO)
			msl_pages_dio_getput(r[j], p);

		else {
			/* Wait here for any pages to be faulted in from
			 *    the ION.
			 */
			if (rw == SL_READ ||
			    ((r[j]->biorq_flags & BIORQ_RBWFP) ||
			     (r[j]->biorq_flags & BIORQ_RBWLP)))
				if ((rc = msl_pages_blocking_load(r[j]))) {
					DEBUG_BIORQ(PLL_ERROR, r[j],
						    "msl_pages_blocking_load()"
						    " error=%d", rc);
					goto out;
				}

			(rw == SL_READ) ? msl_pages_copyout(r[j], p) :
					  msl_pages_copyin(r[j], p);
		}
		/* Unwind our reference from bmap_get().
		 */
		bmap_op_done_type(b[j], BMAP_OPCNT_LOOKUP);
	}

	if (rw == SL_WRITE) {
		fcmh_setlocalsize(mfh->mfh_fcmh, (size_t)(off + size));
		rc = size;
	} else {
		ssize_t fsz = fcmh_getsize(mfh->mfh_fcmh);
		/* The client cache is operating on pages (ie 32k) so
		 *   any short read must be caught here.
		 */
		if (fsz < (ssize_t)(size + off))
			rc = (size - (size + off - fsz));
		else
			rc = size;
	}
 out:
	return (rc);
}

struct bmap_ops bmap_ops = {
	msl_bmap_init,
	msl_bmap_retrieve,
	msl_bmap_modeset,
	msl_bmap_final_cleanup
};
