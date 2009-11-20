/* $Id$ */

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

#include "pfl/pfl.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "pfl/cdefs.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "buffer.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "bmpc.h"
#include "slashrpc.h"
#include "slconfig.h"

#define MSL_PAGES_GET 0
#define MSL_PAGES_PUT 1

extern struct psc_listcache bmapFlushQ;

__static SPLAY_GENERATE(fhbmap_cache, msl_fbr, mfbr_tentry, fhbmap_cache_cmp);


__static void
msl_biorq_build(struct bmpc_ioreq **newreq, struct bmapc_memb *b, uint32_t off,
		uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *bmpce, *bmpce_tmp;
	int i, npages=0, rbw=0;
	uint64_t bmpce_pagemap=0;

	DEBUG_BMAP(PLL_TRACE, b, "adding req for (off=%u) (size=%u)",
		   off, len);

	psc_assert((off + len) <= SLASH_BMAP_SIZE);
	psc_assert(op == BIORQ_WRITE || op == BIORQ_READ);
	*newreq = r = PSCALLOC(sizeof(struct bmpc_ioreq));

	bmpc_ioreq_init(r, off, len, op, b);

	if (b->bcm_mode & BMAP_DIO) {
		/* The bmap is set to use directio, we may then skip
		 *   cache preparation.
		 */
		psc_assert(r->biorq_flags & BIORQ_DIO);
		goto out;
	}

	bmpc = bmap_2_msbmpc(b);
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
		rbw |= BIORQ_RBWFP;
	}
	npages = len / BMPC_BUFSZ;

	if (len % BMPC_BUFSZ) {
		rbw |= BIORQ_RBWLP;
		npages++;
	}

	psc_assert(npages <= BMPC_IOMAXBLKS);

	bmpce_tmp = PSCALLOC(sizeof(*bmpce_tmp));
	/* Lock the bmap's page cache and try to locate cached pages
	 *   which correspond to this request.
	 */
	spinlock(&bmpc->bmpc_lock);
	for (i=0; i < npages; i++) {
		bmpce_tmp->bmpce_off = off + (i * BMPC_BUFSZ);
		bmpce = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
				   bmpce_tmp);
		if (bmpce) {
			spinlock(&bmpce->bmpce_lock);
			/* Increment the ref cnt via the lru mgmt
			 *   function.
			 */
			bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
			freelock(&bmpce->bmpce_lock);
			/* Mark this cache block as being already present
			 *   in the cache.
			 */
			bmpce_pagemap |= (1 << i);
			dynarray_add(&r->biorq_pages, bmpce);
		}
	}
	freelock(&bmpc->bmpc_lock);
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
		spinlock(&bmpc->bmpc_lock);
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
		spinlock(&bmpce->bmpce_lock);
		bmpce_handle_lru_locked(bmpce, bmpc, op, 1);
		freelock(&bmpce->bmpce_lock);
		freelock(&bmpc->bmpc_lock);
		dynarray_add(&r->biorq_pages, bmpce);
	}
	psc_assert(dynarray_len(&r->biorq_pages) == npages);
	/* Sort the list by offset.
	 */
	if (npages > 1)
		qsort(r->biorq_pages.da_items, r->biorq_pages.da_pos,
		      sizeof(void *), bmpce_sort_cmp);

	/* XXX Note if we were move to a RD_DATARDY / WR_DATARDY then
	 *   we wouldn't have to fault in pages like this unless the
	 *   bmap was open in RW mode.
	 */
	if ((fcmh_2_fsz(b->bcm_fcmh) > off) && op == BIORQ_WRITE && rbw)
		r->biorq_flags |= rbw;

	/* Pass1: Retrieve memory pages from the cache on behalf of our pages
	 *   stuck in GETBUF.
	 */
	for (i=0; i < npages; i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i);
		/* Only this thread may assign a buffer to the bmpce.
		 */
		spinlock(&bmpce->bmpce_lock);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    (bmpce->bmpce_flags & BMPCE_GETBUF)) {
			/* Increase the rdref cnt in preparation for any
			 *   RBW ops but only on new pages owned by this
			 *   page cache entry.  For now bypass
			 *   bmpce_handle_lru_locked() for this op.
			 *  XXX how is this ref affected by the cb's?
			 */
			if (bmpce_is_rbw_page(r, bmpce, i))
				psc_atomic16_inc(&bmpce->bmpce_rdref);

			psc_assert(!bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_GETBUF);
			freelock(&bmpce->bmpce_lock);

			bmpce->bmpce_base = bmpc_alloc();

			bmpce->bmpce_flags &= ~BMPCE_GETBUF;
			psc_waitq_wakeall(bmpce->bmpce_waitq);

		} else {
			/* Don't bother prefecthing blocks for unaligned
			 *   requests if another request is already
			 *   responsible for that block.
			 */
			if (!i && (r->biorq_flags & BIORQ_RBWFP))
				r->biorq_flags &= ~BIORQ_RBWFP;

			else if (i == (npages-1) &&
				 (r->biorq_flags & BIORQ_RBWLP))
				r->biorq_flags &= ~BIORQ_RBWLP;

			freelock(&bmpce->bmpce_lock);
		}
	}

	/* Pass2: Sanity Check
	 */
	for (i=0; i < npages; i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i);
		spinlock(&bmpce->bmpce_lock);

		if (op == BIORQ_WRITE)
			psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
		else
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);
		if (biorq_is_my_bmpce(r, bmpce)) {
			/* The page is my reponsibility, ensure a cache
			 *   block has been assigned.
			 */
			psc_assert(bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags == BMPCE_INIT);
			bmpce->bmpce_flags = 0;

			if (op == BIORQ_READ)
				bmpce->bmpce_flags |= BMPCE_READPNDG;
		}
		freelock(&bmpce->bmpce_lock);
	}
 out:
	DEBUG_BIORQ(PLL_TRACE, r, "new req");
	pll_add(bmap_2_msbmpc(b).bmpc_pndg, r);
}


__static void
bmap_biorq_del(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_msbmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);

	atomic_dec(&b->bcm_opcnt);

	DEBUG_BMAP(PLL_INFO, b, "remove biorq=%p nitems_pndg(%d)",
		   r, pll_nitems(&bmpc->bmpc_pndg));

	psc_assert(psclist_conjoint(&r->biorq_lentry));

	pll_remove(&bmpc->bmpc_pndg, r);
	if (r->biorq_flags & BIORQ_WRITE) {
		atomic_dec(&bmpc->bmpc_pndgwr);
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) >= 0);

	} else
		/* Nothing left to do for read biorq's since they don't
		 *   affect the dirty status of the bmap.
		 */
		goto out;

	if (atomic_read(&bmpc->bmpc_pndgwr)) {
		psc_assert(b->bcm_mode & BMAP_DIRTY);
		psc_assert(psclist_conjoint(&bmap_2_msbd(b)->msbd_lentry));

	} else {
		/* Set the special flag to prevent races with others which
		 *   can occur because we may drop the bmap lock before
		 *   removing from the bmapFlushQ listcache.
		 */
		b->bcm_mode |= BMAP_DIRTY2LRU;
		b->bcm_mode &= ~BMAP_DIRTY;

		if (LIST_CACHE_TRYLOCK(&bmapFlushQ))
			goto remove;

		else {
			BMPC_ULOCK(bmpc);
			BMAP_ULOCK(b);
			/* Retake the locks in the correct order else
			 *   deadlock.
			 */
			LIST_CACHE_LOCK(&bmapFlushQ);
			BMAP_LOCK(b);
			BMPC_LOCK(bmpc);
			/* Only this thread may unset BMAP_DIRTY2LRU.
			 */
			psc_assert(b->bcm_mode & BMAP_DIRTY2LRU);

			if (b->bcm_mode & BMAP_DIRTY) {
				psc_assert(psclist_conjoint(&bmap_2_msbd(b)->msbd_lentry));
				psc_assert(!pll_empty(&bmpc->bmpc_pndg));
				psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 0);

			} else {
			remove:
				psc_assert(pll_empty(&bmpc->bmpc_pndg));
				lc_remove(&bmapFlushQ, bmap_2_msbd(b));
			}
			LIST_CACHE_ULOCK(&bmapFlushQ);
			b->bcm_mode &= ~BMAP_DIRTY2LRU;
		}
	}

 out:
	psc_waitq_wakeall(&b->bcm_waitq);
	BMPC_ULOCK(bmpc);
	BMAP_ULOCK(b);
}

__static void
msl_biorq_unref(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmap_pagecache *bmpc=bmap_2_msbmpc(r->biorq_bmap);
	int i;

	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(!(r->biorq_flags & BIORQ_INFL));

	spinlock(&bmpc->bmpc_lock);
	for (i=0; i < dynarray_len(&r->biorq_pages); i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i);
		spinlock(&bmpce->bmpce_lock);

		bmpce_handle_lru_locked(bmpce, bmpc,
		 (r->biorq_flags & BIORQ_WRITE) ? BIORQ_WRITE : BIORQ_READ, 0);

		bmpce->bmpce_flags &= ~(BMPCE_INFL|BMPCE_IOSCHED);

		freelock(&bmpce->bmpce_lock);
	}
	freelock(&bmpc->bmpc_lock);
}

__static void
msl_biorq_destroy(struct bmpc_ioreq *r)
{
	/* One last shot to wakeup any blocked threads.
	 */
	psc_waitq_wakeall(&r->biorq_waitq);

	msl_biorq_unref(r);
	bmap_biorq_del(r);

	dynarray_free(&r->biorq_pages);

	if (r->biorq_rqset)
		pscrpc_set_destroy(r->biorq_rqset);

	PSCFREE(r);
}


struct msl_fhent *
msl_fhent_new(struct fidc_membh *f)
{
	struct msl_fhent *mfh;

	mfh = PSCALLOC(sizeof(*mfh));
	LOCK_INIT(&mfh->mfh_lock);
	SPLAY_INIT(&mfh->mfh_fhbmap_cache);
	mfh->mfh_fcmh = f;
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

/**
 * bmapc_memb_release - release a bmap structure and associated resources.
 * @b: the bmap.
 */
void
msl_bmap_release(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc=bmap_2_msbmpc(b);

	/* Mind lock ordering, remove from LRU first.
	 */
	bmpc_lru_del(bmpc);

	psc_assert(b->bcm_fcmh->fcmh_fcoo);

	BMAP_LOCK(b);
	psc_assert(b->bcm_mode & BMAP_CLOSING);
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

	spinlock(&bmpc->bmpc_lock);
	bmpc_freeall_locked(bmpc);
	freelock(&bmpc->bmpc_lock);

	bmap_remove(b);
}


__static void
bmap_biorq_waitempty(struct bmapc_memb *b)
{
	struct bmpc_ioreq *biorq;
	struct bmap_pagecache *bmpc=bmap_2_msbmpc(b);

	BMPC_LOCK(bmpc);
	PLL_FOREACH(biorq, bmap_2_msbmpc(b).bmpc_pndg) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		freelock(&biorq->biorq_lock);
	}
	BMPC_ULOCK(bmpc);

	BMAP_LOCK(b);
	while (!pll_empty(bmap_2_msbmpc(b).bmpc_pndg)) {
		psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
		BMAP_LOCK(b);
	}
	psc_assert(pll_empty(bmap_2_msbmpc(b).bmpc_pndg));
	BMAP_ULOCK(b);
}

void
msl_bmap_tryrelease(struct bmapc_memb *b)
{
	BMAP_LOCK(b);
	psc_assert(atomic_read(&b->bcm_wr_ref) >= 0);
	psc_assert(atomic_read(&b->bcm_rd_ref) >= 0);

	DEBUG_BMAP(PLL_INFO, b, "free me?");

	if (!atomic_read(&b->bcm_wr_ref) && !atomic_read(&b->bcm_rd_ref)) {
		b->bcm_mode |= BMAP_CLOSING;
		BMAP_ULOCK(b);
		/* Wait for any pending biorqs to clear.
		 */
		bmap_biorq_waitempty(b);
		msl_bmap_release(b);
	}
}

void
msl_fbr_free(struct msl_fbr *r)
{
	struct bmapc_memb *b = r->mfbr_bmap;

	psc_assert(b);
	psc_assert(SPLAY_ENTRY_DISJOINT(fhbmap_cache, r));

	msl_fbr_unref(r);
	PSCFREE(r);

	return (msl_bmap_tryrelease(b));
}

/**
 * msl_bmap_fetch - perform a blocking 'get' operation to retrieve
 *    one or more bmaps from the MDS.
 * @f: pointer to the fid cache structure to which this bmap belongs.
 * @b: the block id to retrieve (block size == SLASH_BMAP_SIZE).
 * @n: the number of bmaps to retrieve (serves as a simple read-ahead mechanism)
 */
__static int
msl_bmap_fetch(struct bmapc_memb *bmap, sl_blkno_t b, int rw)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_bmap_req *mq;
	struct srm_bmap_rep *mp;
	struct bmap_cli_info *msbd;
	struct fidc_membh *f;
	struct iovec iovs[3];
	int nblks, rc=-1;
	uint32_t i, getreptbl=0;
	struct msl_fcoo_data *mfd;

	psc_assert(bmap->bcm_mode & BMAP_INIT);
	psc_assert(bmap->bcm_pri);
	psc_assert(bmap->bcm_fcmh);

	f = bmap->bcm_fcmh;

	FCMH_LOCK(f);
	psc_assert(f->fcmh_fcoo);
	if (!f->fcmh_fcoo->fcoo_pri) {
		f->fcmh_fcoo->fcoo_pri = mfd = PSCALLOC(sizeof(*mfd));
		mfd->mfd_flags |= MFD_RETRREPTBL;
		getreptbl = 1;
	}
	FCMH_ULOCK(f);

	/* Build the new RPC request.
	 */
	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
			     SRMT_GETBMAP, rq, mq, mp)) != 0)
		return (rc);

	mq->sfdb  = *fcmh_2_fdb(f);
	mq->pios  = prefIOS; /* Tell MDS of our preferred ION */
	mq->blkno = b;
	mq->nblks = 1;
	mq->rw    = rw;
	mq->getreptbl = getreptbl ? 1 : 0;

	msbd = bmap->bcm_pri;
	bmap->bcm_mode |= (rw & SRIC_BMAP_WRITE) ? BMAP_WR : BMAP_RD;

	iovs[0].iov_base = &msbd->msbd_msbcr;
	iovs[0].iov_len  = sizeof(msbd->msbd_msbcr);

	iovs[1].iov_base = &msbd->msbd_bdb;
	iovs[1].iov_len  = sizeof(msbd->msbd_bdb);

	if (getreptbl) {
		/* This code only deals with SL_DEF_REPLICAS, not MAX.
		 */
		iovs[2].iov_base = &mfd->mfd_reptbl;
		iovs[2].iov_len  = sizeof(sl_replica_t) * INO_DEF_NREPLS;
	}

	DEBUG_FCMH(PLL_DEBUG, f, "retrieving bmap (s=%u) (rw=%d)", b, rw);

	nblks = 0;
	rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRMC_BULK_PORTAL, iovs,
		       2 + getreptbl);

	if ((rc = RSX_WAITREP(rq, mp)) == 0) {
		/* Verify the return.
		 */
		if (!mp->nblks) {
			psc_errorx("MDS returned 0 bmaps");
			return (-1);
		}
		/* Add the bmaps to the tree.
		 */
		spinlock(&f->fcmh_lock);
		for (i=0; i < mp->nblks; i++) {
			SPLAY_INSERT(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc,
				     bmap);
			//atomic_inc(&f->fcmh_fcoo->fcoo_bmapc_cnt);
			bmap_2_msion(bmap) = mp->ios_nid;
		}
		freelock(&f->fcmh_lock);
	}

	bmap->bcm_mode &= ~BMAP_INIT;

	if (getreptbl) {
		/* XXX don't forget that on write we need to invalidate the local replication
		 *   table..
		 */
		FCMH_LOCK(f);
		mfd->mfd_flags = MFD_HAVEREPTBL;
		psc_waitq_wakeall(&f->fcmh_waitq);
		FCMH_ULOCK(f);
	}

	return (rc);
}

/**
 * msl_bmap_modeset -
 * Notes:  XXX I think this logic can be simplified when setting mode from
 *    WRONLY to RDWR.  In WRONLY this client already knows the address
 *    of the only ION from which this bmap can be read.  Therefore, it
 *    should be able to interface with that ION without intervention from
 *    the mds.
 */
__static int
msl_bmap_modeset(struct fidc_membh *f, sl_blkno_t b, int rw)
{
	struct pscrpc_request *rq;
	struct srm_bmap_chmode_req *mq;
	struct srm_generic_rep *mp;
	int rc;

	psc_assert(rw == SRIC_BMAP_WRITE || rw == SRIC_BMAP_READ);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
			     SRMT_BMAPCHMODE, rq, mq, mp)) != 0)
		return (rc);

	mq->sfdb = *fcmh_2_fdb(f);
	mq->blkno = b;
	mq->rw = rw;

	if ((rc = RSX_WAITREP(rq, mp)) == 0) {
		if (mp->rc)
			psc_warn("msl_bmap_chmode() failed (f=%p) (b=%u)",
				 f, b);
	}
	return (rc);
}

void
msl_bmap_fhcache_clear(struct msl_fhent *mfh)
{
	struct msl_fbr *r, *n;

	spinlock(&mfh->mfh_lock);
	for (r = SPLAY_MIN(fhbmap_cache, &mfh->mfh_fhbmap_cache);
	    r; r = n) {
		n = SPLAY_NEXT(fhbmap_cache, &mfh->mfh_fhbmap_cache, r);
		psc_assert(SPLAY_REMOVE(fhbmap_cache,
		    &mfh->mfh_fhbmap_cache, r));
		msl_fbr_free(r);
	}
	freelock(&mfh->mfh_lock);
}

#define BML_NEW_BMAP  0
#define BML_HAVE_BMAP 1

__static void
msl_bmap_fhcache_ref(struct msl_fhent *mfh, struct bmapc_memb *b,
		     int mode, int rw)
{
	struct msl_fbr *r;

	/* Now handle the fhent's bmap cache, adding a new reference
	 *  if needed.
	 *
	 * A new bmap may not already exist in the file handle's
	 *  reference cache.  Lock around the fhcache_bmap_lookup()
	 *  test to prevent another thread from inserting before us.
	 */
	spinlock(&mfh->mfh_lock);
	r = fhcache_bmap_lookup(mfh, b);
	if (!r) {
		r = msl_fbr_new(b, (rw == SRIC_BMAP_WRITE ?
				    FHENT_WRITE : FHENT_READ));
		SPLAY_INSERT(fhbmap_cache, &mfh->mfh_fhbmap_cache, r);
	} else {
		/* Verify that the ref didn't not exist if the caller
		 *  specified BML_NEW_BMAP.
		 */
		psc_assert(mode != BML_NEW_BMAP);
		msl_fbr_ref(r, (rw == SRIC_BMAP_WRITE ?
				FHENT_WRITE : FHENT_READ));
	}
	freelock(&mfh->mfh_lock);
}

/**
 * msl_bmap_load - locate a bmap in fcache_memb_handle's splay tree or retrieve it from the MDS.
 * @f: the msl_fhent for the owning file.
 * @n: bmap number.
 * @prefetch: the number of subsequent bmaps to prefetch.
 * @rw: tell the mds if we plan to read or write.
 * Notes: XXX Need a way to detect bmap mode changes here (ie from read
 *	to rw) and take the neccessary actions to notify the mds, this
 *	detection will be done by looking at the refcnts on the bmap.
 * TODO:  XXX if bmap is already cached but is not in write mode (but
 *	rw==WRITE) then we must notify the mds of this.
 */
__static struct bmapc_memb *
msl_bmap_load(struct msl_fhent *mfh, sl_blkno_t n, uint32_t rw)
{
	struct fidc_membh *f = mfh->mfh_fcmh;
	struct bmapc_memb *b;

	int rc = 0, mode = BML_HAVE_BMAP;

	b = bmap_lookup_add(f, n, msl_bmap_init);
	psc_assert(b);

	if (b->bcm_mode & BMAP_INIT) {
		/* Retrieve the bmap from the sl_mds.
		 */
		rc = msl_bmap_fetch(b, n, rw);
		if (rc)
			return NULL;
		else {
			mode = BML_NEW_BMAP;
			psc_assert(!(b->bcm_mode & BMAP_INIT));
		}
		/* Verify that the mds has returned a 'write-enabled' bmap.
		 */
		if (rw == SRIC_BMAP_WRITE)
			psc_assert(b->bcm_mode & BMAP_WR);

		msl_bmap_fhcache_ref(mfh, b, mode, rw);
		return (b);
	}
	/* Else */
	/* Ref now, otherwise our bmap may get downgraded while we're
	 *  blocking on the waitq.
	 */
	msl_bmap_fhcache_ref(mfh, b, mode, rw);

	/* If our bmap is cached then we need to consider the current
	 *   caching policy and possibly notify the mds.  I.e. if our
	 *   bmap is read-only (but we'd like to write) then the mds
	 *   must be notified so that coherency amongst clients can
	 *   be maintained.
	 *  msl_io() has already verified that this file is writable.
	 *  XXX has it?
	 */
 retry:
	spinlock(&b->bcm_lock);
	if (rw != SRIC_BMAP_WRITE || (b->bcm_mode & BMAP_WR)) {
		/* Either we're in read-mode here or the bmap
		 *  has already been marked for writing therefore
		 *  the mds already knows we're writing.
		 */
		freelock(&b->bcm_lock);
		goto out;

	} else if (b->bcm_mode & BMAP_CLI_MCIP) {
		/* If some other thread has set BMAP_CLI_MCIP then HE
		 *  must set BMAP_CLI_MCC when he's done (at that time
		 *  he also must unset BMAP_CLI_MCIP and set BMAP_WR
		 */
		psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
		/* Done waiting, double check our refcnt, it should be at
		 *  least '1' (our ref).
		 * XXX Not sure if both checks are needed.
		 */
		psc_assert(atomic_read(&b->bcm_opcnt) > 0);
		psc_assert(atomic_read(&b->bcm_wr_ref) > 0);
		if (b->bcm_mode & BMAP_CLI_MCC) {
			/* Another thread has completed the upgrade
			 *  in mode change.  Verify that the bmap
			 *  is in the appropriate state.
			 *  Note: since our wr_ref has been set above,
			 *   the bmap MUST have BMAP_WR set here.
			 */
			psc_assert(!(b->bcm_mode & BMAP_CLI_MCIP));
			psc_assert((b->bcm_mode & BMAP_WR));
		} else
			/* We were woken up for a different
			 *  reason - try again.
			 */
			goto retry;

	} else { /* !BMAP_CLI_MCIP not set, we will set it and
		  *    proceed with the modechange operation.
		  */
		psc_assert(!(b->bcm_mode & BMAP_WR)   &&
			   !(b->bcm_mode & BMAP_CLI_MCIP) &&
			   !(b->bcm_mode & BMAP_CLI_MCC));

		b->bcm_mode |= BMAP_CLI_MCIP;
		freelock(&b->bcm_lock);
		/* An interesting fallout here is that the mds may callback
		 *  to us causing our pagecache to be purged :)
		 * Correction.. this is not true, since if there was another
		 *  writer then we would already be in directio mode.
		 */
		rc = msl_bmap_modeset(f, b->bcm_blkno, SRIC_BMAP_WRITE);
		psc_assert(!rc); /*  XXX for now.. */
		/* We're the only thread allowed here, these
		 *  bits could not have been set by another thread.
		 */
		spinlock(&b->bcm_lock);
		psc_assert(b->bcm_mode & BMAP_CLI_MCIP);
		psc_assert(!(b->bcm_mode & BMAP_CLI_MCC) &&
			   !(b->bcm_mode & BMAP_WR));
		b->bcm_mode &= ~BMAP_CLI_MCIP;
		b->bcm_mode |= (BMAP_WR | BMAP_CLI_MCC);
		freelock(&b->bcm_lock);
		psc_waitq_wakeall(&b->bcm_waitq);
	}
 out:
	return (b);
}

#define MIN_CONNECT_RETRY_SECS 30

__static int
msl_ion_connect(lnet_nid_t nid, struct cli_imp_ion *c)
{
	int rc;

	psc_dbg("Creating new import to %s",
		libcfs_nid2str(nid));

 retry:
	spinlock(&c->ci_lock);

	if (c->ci_flags & CION_CONNECTED) {
		psc_assert(c->ci_import);
		freelock(&c->ci_lock);
		return (0);

	} else if (c->ci_flags & CION_CONNECTING) {
		psc_waitq_wait(&c->ci_waitq, &c->ci_lock);
		goto retry;
	}

	if (c->ci_flags & CION_CONNECT_FAIL) {
		struct timespec ts;

		clock_gettime(CLOCK_REALTIME, &ts);
		if ((ts.tv_sec - c->ci_connect_time.tv_sec) <
		    MIN_CONNECT_RETRY_SECS)
			return (-EAGAIN);
	} else
		c->ci_flags &= ~CION_CONNECT_FAIL;


	psc_assert(!c->ci_import);
	psc_assert(!((c->ci_flags & CION_CONNECTED) ||
		     (c->ci_flags & CION_CONNECTING)));

	c->ci_flags |= CION_CONNECTING;
	freelock(&c->ci_lock);

	if ((c->ci_import = pscrpc_new_import()) == NULL)
		psc_fatalx("pscrpc_new_import");

	c->ci_import->imp_client->cli_request_portal = SRIC_REQ_PORTAL;
	c->ci_import->imp_client->cli_reply_portal = SRIC_REP_PORTAL;
	clock_gettime(CLOCK_REALTIME, &c->ci_connect_time);

	rc = rpc_issue_connect(nid, c->ci_import,
			       SRIC_MAGIC, SRIC_VERSION);

	spinlock(&c->ci_lock);
	if (rc) {
		psc_errorx("rpc_issue_connect() to %s", libcfs_nid2str(nid));
		pscrpc_import_put(c->ci_import);
		c->ci_import = NULL;
		c->ci_flags |= CION_CONNECT_FAIL;
	} else
		c->ci_flags |= CION_CONNECTED;

	c->ci_flags &= ~CION_CONNECTING;
	psc_waitq_wakeall(&c->ci_waitq);
	freelock(&c->ci_lock);

	return (rc);
}
/**
 * msl_bmap_to_import - Given a bmap, perform a series of lookups to
 *	locate the ION import.  The ION was chosen by the mds and
 *	returned in the msl_bmap_fetch routine. msl_bmap_to_import
 *	queries the configuration to find the ION's private info - this
 *	is where the import pointer is kept.  If no import has yet been
 *	allocated a new is made.
 * @b: the bmap
 * Notes: the bmap is locked to avoid race conditions with import checking.
 *        the bmap's refcnt must have been incremented so that it is not freed from under us.
 * XXX Dev Needed: If the bmap is a read-only then any replica may be
 *	accessed (so long as it is recent).  Therefore
 *	msl_bmap_to_import() should have logic to accommodate this.
 */
struct pscrpc_import *
msl_bmap_to_import(struct bmapc_memb *b, int add)
{
	struct cli_imp_ion *c;
	struct sl_resm *r;
	int locked;

	/* Sanity check on the opcnt.
	 */
	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	locked = reqlock(&b->bcm_lock);
	r = libsl_nid2resm(bmap_2_msion(b));
	if (!r)
		psc_fatalx("Failed to lookup %s, verify that the slash configs"
			   " are uniform across all servers",
			   libcfs_nid2str(bmap_2_msion(b)));

	c = r->resm_pri;
	ureqlock(&b->bcm_lock, locked);

	if (!c->ci_import && add)
		msl_ion_connect(bmap_2_msion(b), c);

	return (c->ci_import);
}


struct pscrpc_import *
msl_bmap_choose_replica(struct bmapc_memb *b)
{
	struct cli_imp_ion *c;
	struct resprof_cli_info *r;
	struct msl_fcoo_data *mfd;
	struct sl_resource *res;
	struct sl_resm *resm;
	lnet_nid_t repl_nid;
	int n;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);
	psc_assert(b->bcm_fcmh->fcmh_fcoo);

	mfd = b->bcm_fcmh->fcmh_fcoo->fcoo_pri;
	psc_assert(mfd);
	/*  XXX need a more intelligent algorithm here.
	 */
	res = libsl_id2res(mfd->mfd_reptbl[0].bs_id);
	if (!res)
		psc_fatalx("Failed to lookup iosid %u, verify that the slash "
			   "configs are uniform across all servers",
			   mfd->mfd_reptbl[0].bs_id);

	r = res->res_pri;
	spinlock(&r->rci_lock);
	n = r->rci_cnt++;
	if (r->rci_cnt >= (int)res->res_nnids)
		n = r->rci_cnt = 0;
	repl_nid = res->res_nids[n];
	freelock(&r->rci_lock);

	psc_trace("trying res(%s) ion(%s)",
		  res->res_name, libcfs_nid2str(repl_nid));

	resm = libsl_nid2resm(repl_nid);
	if (!resm)
		psc_fatalx("Failed to lookup %s, verify that the slash configs"
			   " are uniform across all servers",
			   libcfs_nid2str(repl_nid));

	c = resm->resm_pri;
	msl_ion_connect(repl_nid, c);

	return (c->ci_import);
}

/** msl_readio_cb - rpc callback used only for read or RBW operations.
 *    The primary purpose is to set the bmpce's to DATARDY so that other
 *    threads waiting for DATARDY may be unblocked.
 *  Note: Unref of the biorq will happen after the pages have been
 *     copied out to the applicaton buffers.
 */
int
msl_readio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct dynarray *a=args->pointer_arg[MSL_IO_CB_POINTER_SLOT];
	struct bmpc_ioreq *r=args->pointer_arg[MSL_OFTRQ_CB_POINTER_SLOT];
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache_entry *bmpce;
	int op=rq->rq_reqmsg->opc, i;

	b = r->biorq_bmap;
	psc_assert(b);

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);
	psc_assert(a);

	DEBUG_BMAP(PLL_INFO, b, "callback");
	DEBUG_BIORQ(PLL_INFO, r, "callback bmap=%p", b);

	if (rq->rq_status) {
		DEBUG_REQ(PLL_ERROR, rq, "non-zero status status %d",
		    rq->rq_status);
		psc_fatalx("Resolve issues surrounding this failure");
		// XXX Freeing of dynarray, bmpce's, etc
		return (rq->rq_status);
	}
	/* Call the inflight CB only on the iov's in the dynarray -
	 *   not the iov's in the request since some of those may
	 *   have already been staged in.
	 */
	for (i=0; i < dynarray_len(a); i++) {
		bmpce = dynarray_getpos(a, i);
		spinlock(&bmpce->bmpce_lock);

		psc_assert(bmpce->bmpce_waitq);
		psc_assert(biorq_is_my_bmpce(r, bmpce));
		psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
		psc_assert(bmpce->bmpce_flags & BMPCE_INFL);

		bmpce->bmpce_flags &= ~(BMPCE_INFL|BMPCE_IOSCHED);
		bmpce->bmpce_flags |= BMPCE_DATARDY;
		if (bmpce_is_rbw_page(r, bmpce, i)) {
			/* The RBW stuff needs to be managed outside of
			 *   the LRU, this is not the best place but should
			 *   suffice for now.
			 */
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref)
				   == 1);
			psc_atomic16_dec(&bmpce->bmpce_rdref);
		}
		DEBUG_BMPCE(PLL_INFO, bmpce, "datardy via readio_cb");

		/* Disown the bmpce by null'ing the waitq pointer.
		 */
		bmpce->bmpce_waitq = NULL;
		freelock(&bmpce->bmpce_lock);
	}
	psc_waitq_wakeall(&r->biorq_waitq);

	/* Free the dynarray which was allocated in msl_readio_rpc_create().
	 */
	PSCFREE(a);
	return (0);
}


int
msl_io_rpcset_cb_old(__unusedx struct pscrpc_request_set *set, void *arg, int rc)
{
	struct bmpc_ioreq *r=arg;

	msl_biorq_destroy(r);

	return (rc);
}


int
msl_io_rpcset_cb(__unusedx struct pscrpc_request_set *set, void *arg, int rc)
{
	struct dynarray *biorqs = arg;
	struct bmpc_ioreq *r;
	int i;

	for (i=0; i < dynarray_len(biorqs); i++) {
		r = dynarray_getpos(biorqs, i);

		spinlock(&r->biorq_lock);
		psc_assert(r->biorq_flags & BIORQ_INFL);
		psc_assert(r->biorq_flags & BIORQ_SCHED);
		r->biorq_flags &= ~BIORQ_INFL;
		freelock(&r->biorq_lock);

		msl_biorq_destroy(r);
	}
	dynarray_free(biorqs);
	PSCFREE(biorqs);

	return (rc);
}


int
msl_io_rpc_cb(__unusedx struct pscrpc_request *req, struct pscrpc_async_args *args)
{
	struct dynarray *biorqs;
	struct bmpc_ioreq *r;
	int i;

	biorqs = args->pointer_arg[0];

	DEBUG_REQ(PLL_INFO, req, "biorqs=%p", biorqs);

	for (i=0; i < dynarray_len(biorqs); i++) {
		r = dynarray_getpos(biorqs, i);

		spinlock(&r->biorq_lock);
		psc_assert(r->biorq_flags & BIORQ_INFL);
		psc_assert(r->biorq_flags & BIORQ_SCHED);
		r->biorq_flags &= ~BIORQ_INFL;
		freelock(&r->biorq_lock);

		msl_biorq_destroy(r);
	}
	dynarray_free(biorqs);
	PSCFREE(biorqs);

	return (0);
}


int
msl_dio_cb(struct pscrpc_request *rq, __unusedx struct pscrpc_async_args *args)
{
	struct srm_io_req *mq;
	int op=rq->rq_reqmsg->opc;

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	psc_assert(mq);

	DEBUG_REQ(PLL_TRACE, rq, "completed dio req (op=%d) o=%u s=%u",
	    op, mq->offset, mq->size);

	return (0);
}


__static void
msl_pages_dio_getput(struct bmpc_ioreq *r, char *b)
{
	struct pscrpc_import      *imp;
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

	imp = (op == SRMT_WRITE) ?
		msl_bmap_to_import(bcm, 1) :
		msl_bmap_choose_replica(bcm);

	r->biorq_rqset = pscrpc_prep_set();
	/* This buffer hasn't been segmented into LNET sized
	 *  chunks.  Set up buffers into 1MB chunks or smaller.
	 */
	n = (r->biorq_len / LNET_MTU) + ((r->biorq_len % LNET_MTU) ? 1 : 0);
	iovs = PSCALLOC(sizeof(*iovs) * n);

	for (i=0, nbytes=0; i < n; i++, nbytes += len) {
		len = MIN(LNET_MTU, (size-nbytes));

		rc = RSX_NEWREQ(imp, SRIC_VERSION, op, req, mq, mp);
		if (rc)
			psc_fatalx("RSX_NEWREQ() failed %d", rc);

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
		mq->op = (op == SRMT_WRITE ? SRMIO_WR : SRMIO_RD);
		memcpy(&mq->sbdb, &msbd->msbd_bdb, sizeof(mq->sbdb));

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
}


__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_msbmpc(b);
	int put_dirty=0;

	/* This req must already be attached to the cache.
	 */
	psc_assert(psclist_conjoint(&r->biorq_lentry));

	BMAP_LOCK(b);

	atomic_inc(&bmpc->bmpc_pndgwr);

	if (b->bcm_mode & BMAP_DIRTY) {
		/* If the bmap is already dirty then at least
		 *   one other writer must be present.
		 */
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 1);
		psc_assert(pll_nitems(&bmpc->bmpc_pndg) > 1);

	} else if (!(b->bcm_mode & BMAP_DIRTY2LRU)) {
		/* Deal with a race which can occur with bmap_oftrq_del()
		 *   due to his requirement to drop the bmap lock prior
		 *   to removing the bmap from the bmapFlushQ listcache.
		 *   By replacing the dirty flag bmap_biorq_del() will
		 *   leave the bmap on the bmap_biorq_del().
		 * Set it to dirty and hand it off to the bmap
		 *   flush thread.
		 */
		psc_assert(psclist_disjoint(&bmap_2_msbd(b)->msbd_lentry));
		b->bcm_mode |= BMAP_DIRTY;
		put_dirty = 1;
	}

	DEBUG_BMAP(PLL_INFO, b, "biorq=%p list_empty(%d)",
		   r, pll_empty(&bmpc->bmpc_pndg));

	BMAP_ULOCK(b);

	if (put_dirty)
		lc_addtail(&bmapFlushQ, bmap_2_msbd(b));
}

__static void
msl_readio_rpc_create(struct bmpc_ioreq *r, int startpage, int npages)
{
	struct bmap_pagecache_entry *bmpce;
	struct pscrpc_import *imp;
	struct pscrpc_request *req;
	struct pscrpc_bulk_desc *desc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	struct dynarray *a;
	int rc, i;

	ENTRY;

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	imp = msl_bmap_choose_replica(r->biorq_bmap);

	rc = RSX_NEWREQ(imp, SRIC_VERSION, SRMT_READ, req, mq, mp);
	if (rc)
		psc_fatalx("RSX_NEWREQ() failed %d", rc);

	iovs = PSCALLOC(sizeof(*iovs) * npages);
	a = PSCALLOC(sizeof(a));
	dynarray_init(a);

	for (i=0; i < npages; i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i+startpage);

		spinlock(&bmpce->bmpce_lock);
		/* Sanity checks.
		 */
		psc_assert(biorq_is_my_bmpce(r, bmpce));
		/* BMPCE_DATARDY should not be set, otherwise we should
		 *   not be here.
		 */
		psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
		psc_assert(bmpce->bmpce_flags & BMPCE_IOSCHED);
		bmpce_usecheck(bmpce, BIORQ_READ,
			       biorq_getaligned_off(r, (i+startpage)));
		bmpce->bmpce_flags |= BMPCE_INFL;
		DEBUG_BMPCE(PLL_TRACE, bmpce, "adding to rpc");

		freelock(&bmpce->bmpce_lock);

		iovs[i].iov_base = bmpce->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			mq->offset = bmpce->bmpce_off;
		dynarray_add(a, bmpce);
	}

	rc = rsx_bulkclient(req, &desc, BULK_PUT_SINK, SRIC_BULK_PORTAL,
			    iovs, npages);
	if (rc)
		psc_fatalx("rsx_bulkclient() failed %d", rc);

	mq->size = npages * BMPC_BUFSZ;
	mq->op = SRMIO_RD;
	memcpy(&mq->sbdb, &bmap_2_msbd(r->biorq_bmap)->msbd_bdb,
	       sizeof(mq->sbdb));

	pscrpc_set_add_new_req(r->biorq_rqset, req);
	if (pscrpc_push_req(req)) {
		DEBUG_REQ(PLL_ERROR, req,
			  "pscrpc_push_req() failed");
		psc_fatalx("pscrpc_push_req, no failover yet");
	}
	/* Setup the callback, supplying the dynarray as a argument.
	 */
	req->rq_interpret_reply = msl_readio_cb;
	req->rq_async_args.pointer_arg[MSL_IO_CB_POINTER_SLOT] = a;
	req->rq_async_args.pointer_arg[MSL_OFTRQ_CB_POINTER_SLOT] = r;
}

__static void
msl_pages_prefetch(struct bmpc_ioreq *r)
{
	int i, npages;
	struct bmapc_memb *bcm;
	struct bmap_cli_info *msbd;
	struct bmap_pagecache_entry *bmpce;

	if (!((r->biorq_flags & BIORQ_READ)  ||
	      (r->biorq_flags & BIORQ_RBWFP) ||
	      (r->biorq_flags & BIORQ_RBWLP)))
		return;

	bcm    = r->biorq_bmap;
	msbd   = bcm->bcm_pri;
	npages = dynarray_len(&r->biorq_pages);

	r->biorq_flags |= BIORQ_SCHED;

	psc_assert(!r->biorq_rqset);
	r->biorq_rqset = pscrpc_prep_set();
	/* Only read in the pages owned by this request.  To do this
	 *   the below loop marks only the iov slots which correspond
	 *   to page cache entries owned by this request as determined
	 *   by biorq_is_my_bmpce().
	 */
	if (r->biorq_flags & BIORQ_READ) {
		int j=-1;

		for (i=0; i < npages; i++) {
			bmpce = dynarray_getpos(&r->biorq_pages, i);
			spinlock(&bmpce->bmpce_lock);

			bmpce_usecheck(bmpce, BIORQ_READ,
				       biorq_getaligned_off(r, i));

			if (biorq_is_my_bmpce(r, bmpce)) {
				psc_assert(!(bmpce->bmpce_flags &
					     BMPCE_DATARDY));
				bmpce->bmpce_flags |= BMPCE_IOSCHED;
			}
			freelock(&bmpce->bmpce_lock);

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

				} else if ((i-j) == BMPC_MAXBUFSRPC) {
					msl_readio_rpc_create(r, j, i-j);
					j = i;
				}
			}
		}

		if (j >= 0)
			/* Catch any unsent frags at the end of the array.
			 */
			msl_readio_rpc_create(r, j, i-j);

	} else if (r->biorq_flags & BIORQ_RBWFP) {
		bmpce = dynarray_getpos(&r->biorq_pages, 0);

		if (biorq_is_my_bmpce(r, bmpce)) {
			psc_assert(!(bmpce->bmpce_flags &
				     BMPCE_DATARDY));
			bmpce->bmpce_flags |= BMPCE_IOSCHED;
			msl_readio_rpc_create(r, 0, 1);
		}

	} else if (r->biorq_flags & BIORQ_RBWLP) {
		bmpce = dynarray_getpos(&r->biorq_pages,
				dynarray_len(&r->biorq_pages)-1);

		if (biorq_is_my_bmpce(r, bmpce)) {
			psc_assert(!(bmpce->bmpce_flags &
				     BMPCE_DATARDY));
			bmpce->bmpce_flags |= BMPCE_IOSCHED;
			msl_readio_rpc_create(r,
			      dynarray_len(&r->biorq_pages)-1, 1);
		}
	}
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
	int rc, i, npages=dynarray_len(&r->biorq_pages);

		if (r->biorq_rqset) {
		rc = pscrpc_set_wait(r->biorq_rqset);
		if (rc)
			// XXX need to cleanup properly
			psc_fatalx("pscrpc_set_wait rc=%d", rc);
		/* Destroy and cleanup the set now.
		 */
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;
	}

	for (i=0; i < npages; i++) {
		bmpce = dynarray_getpos(&r->biorq_pages, i);
		spinlock(&bmpce->bmpce_lock);
		DEBUG_BMPCE(PLL_TRACE, bmpce, " ");

		if (!biorq_is_my_bmpce(r, bmpce))
			/* For pages not owned by this request,
			 *    wait for them to become DATARDY.
			 */
			while (!(bmpce->bmpce_flags & BMPCE_DATARDY)) {
				psc_waitq_wait(bmpce->bmpce_waitq,
					       &bmpce->bmpce_lock);
				spinlock(&bmpce->bmpce_lock);
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

		freelock(&bmpce->bmpce_lock);
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
	npages = dynarray_len(&r->biorq_pages);

	for (i=0; i < npages; i++) {
		/* All pages are involved, therefore tsize should have value.
		 */
		psc_assert(tsize);

		bmpce = dynarray_getpos(&r->biorq_pages, i);
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

		DEBUG_BMPCE(PLL_TRACE, bmpce, "tsize=%u nbytes=%u toff=%u",
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
		spinlock(&bmpce->bmpce_lock);
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
		freelock(&bmpce->bmpce_lock);

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
	npages = dynarray_len(&r->biorq_pages);

	psc_assert(npages);

	for (i=0; i < npages; i++) {
		psc_assert(tsize);

		bmpce = dynarray_getpos(&r->biorq_pages, i);

		spinlock(&bmpce->bmpce_lock);

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

		DEBUG_BMPCE(PLL_TRACE, bmpce, "tsize=%u nbytes=%zu toff=%u",
			    tsize, nbytes, toff);

		memcpy(sink, src, nbytes);
		freelock(&bmpce->bmpce_lock);

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
 * @op: the operation type (MSL_READ or MSL_WRITE).
 */
int
msl_io(struct msl_fhent *mfh, char *buf, size_t size, off_t off, int op)
{
#define MAX_BMAPS_REQ 4
	struct bmpc_ioreq *r[MAX_BMAPS_REQ];
	struct bmapc_memb *b;
	sl_blkno_t s, e;
	size_t tlen, tsize=size;
	off_t roff;
	int nr, j, rc;
	char *p;

	psc_assert(mfh);
	psc_assert(mfh->mfh_fcmh);

	if ((op == MSL_READ) && off >= (fcmh_2_fsz(mfh->mfh_fcmh))) {
		rc = 0;
		goto out;
	}
	/* Are these bytes in the cache?
	 *  Get the start and end block regions from the input parameters.
	 */
	//XXX beware, I think 'e' can be short by 1.
	s = off / mslfh_2_bmapsz(mfh);
	e = (off + size) / mslfh_2_bmapsz(mfh);

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
			   "sz=%zu tlen=%zu off=%"PSCPRIdOFF" roff=%"PSCPRIdOFF
			   " op=%d", tsize, tlen, off, roff, op);
		/* Load up the bmap, if it's not available then we're out of
		 *  luck because we have no idea where the data is!
		 */
		b = msl_bmap_load(mfh, s, (op == MSL_READ) ?
				  SRIC_BMAP_READ : SRIC_BMAP_WRITE);
		if (!b) {
			rc = -EIO;
			goto out;
		}

		msl_biorq_build(&r[nr], b, roff, tlen,
				(op == MSL_READ) ? BIORQ_READ : BIORQ_WRITE);
		/* Start prefetching our cached buffers.
		 */
		msl_pages_prefetch(r[nr]);

		roff += tlen;
		tsize -= tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);
	}

	/* Note that the offsets used here are file-wise offsets not
	 *   offsets into the buffer.
	 */
	for (j=0, p=buf; j < nr; j++, p+=tlen) {
		if (!r[j]->biorq_flags & BIORQ_DIO)
			msl_pages_dio_getput(r[j], p);

		else {
			/* Wait here for any pages to be faulted in from
			 *    the ION.
			 */
			if (op == MSL_READ ||
			    ((r[j]->biorq_flags & BIORQ_RBWFP) ||
			     (r[j]->biorq_flags & BIORQ_RBWLP)))
				if ((rc = msl_pages_blocking_load(r[j])))
					goto out;

			(op == MSL_READ) ?
				msl_pages_copyout(r[j], p) :
				msl_pages_copyin(r[j], p);
		}
	}
	rc = size;
 out:
	return (rc);
}
