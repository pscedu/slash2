/* $Id$ */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"
#include "offtree.h"

#include "fid.h"
#include "fidcache.h"

#define SL_FIDCACHE_LOW_THRESHOLD 80 // 80%

psc_spinlock_t	 fidcCacheLock;
list_cache_t	 fidcFreeList;
list_cache_t	 fidcDirtyList;
list_cache_t     fidcCleanList;


__static void
fidcache_put_locked(struct fidcache_memb_handle *, list_cache_t *);

/**
 * fcmh_clean_check - placeholder!  This function will ensure the validity of the passed in inode.
 */
__static int
fcmh_clean_check(struct fidcache_memb_handle *f)
{
	// XXX Write me
	f = NULL;
	return 0;
}

/**
 * bmap_cache_memb_init - initialize a bmap structure and create its offtree.
 * @b: the bmap struct
 * @f: the bmap's owner
 */
void
bmap_cache_memb_init(struct bmap_cache_memb *b, struct fidcache_memb_handle *f)
{
	memset(b, 0, sizeof(*b));
	atomic_set(&b->bcm_opcnt, 0);
	psc_waitq_init(&b->bcm_waitq);
	b->bcm_oftr = offtree_create(SLASH_BMAP_SIZE, SLASH_BMAP_BLKSZ,
				     SLASH_BMAP_WIDTH, SLASH_BMAP_DEPTH,
				     f, sl_buffer_alloc, sl_oftm_addref, 
				     sl_oftiov_pin_cb);
	psc_assert(b->bcm_oftr);
	f->fcmh_bmap_sz = SLASH_BMAP_SIZE;
}

/**
 * bmap_cache_cmp - bmap_cache splay tree comparator
 * @a: a bmap_cache_memb
 * @b: another bmap_cache_memb
 */
int
bmap_cache_cmp(const void *x, const void *y)
{
	const struct bmap_cache_memb *a = x, *b = y;

        if (a->bcm_blkno > b->bcm_blkno)
                return 1;
        else if (a->bcm_blkno < b->bcm_blkno)
                return -1;
        return 0;
}

__static SPLAY_GENERATE(bmap_cache, bmap_cache_memb,
			bcm_tentry, bmap_cache_cmp);


static inline int
fidcache_freelist_avail_check(void)
{
        psc_assert(fidcFreeList.lc_size > 0);

        if ((fidcFreeList.lc_nseen / SL_FIDCACHE_LOW_THRESHOLD) >
            (size_t)fidcFreeList.lc_size)
                return 0;
        return 1;
}

/**
 * zinocache_reap - get some inodes from the clean list and free them.
 */
__static void
fidcache_reap(void)
{
	struct fidcache_memb_handle *f;
	int locked;

	/* Try to reach the reserve threshold.
	 */
	do {
		f = lc_getnb(&fidcCleanList);
		if (f == NULL)
			return;

		locked = reqlock(&f->fcmh_lock);
		/* Make sure our clean list is 'clean' by
		 *  verifying the following conditions.
		 */
		if (!fcmh_clean_check(f)) {
			DEBUG_FCMH(PLL_ERROR, f, "Invalid fcmh state");
			psc_fatalx("Invalid state for clean list");
		}
		/*  Clean inodes may have non-zero refcnts, skip these
		 *   inodes.
		 */
		if (atomic_read(&f->fcmh_refcnt)) {
			DEBUG_FCMH(PLL_INFO, f, "restoring to clean list");
			lc_put(&fidcCleanList,
			       &f->fcmh_lentry);
			ureqlock(&f->fcmh_lock, locked);
			continue;
		}
		/* Free it..
		 */
		ATTR_SET(f->fcmh_state, FCM_CAC_FREEING);
                fidcache_put_locked(f, &fidcFreeList);
		ureqlock(&f->fcmh_lock, locked);

	} while(!fidcache_freelist_avail_check());
}

/**
 * fidcache_get - grab a clean fid from the cache.
 */
struct fidcache_memb_handle *
fidcache_get(list_cache_t *lc)
{
	struct fidcache_memb_handle *f;

	f = lc_getnb(lc);
        if (f == NULL) {
                fidcache_reap();        /* Try to free some fids */
                f = lc_getwait(lc);
        }
	if (lc == &fidcFreeList) {
		psc_assert(f->fcmh_state == FCM_CAC_FREE);
		psc_assert(fcmh_clean_check(f));
		fcmh_incref(f);
	} else
		psc_fatalx("It's unwise to get inodes from %p, it's not "
			   "fidcFreeList", lc);
	return (f);
}

/**
 * fidcache_put_locked - release an inode onto an inode list cache.
 *
 * This routine should be called when:
 * - moving a free inode to the dirty cache.
 * - moving a dirty inode to the free cache.
 * Notes: inode is already locked.  Extensive validity checks are
 *        performed within fidc_clean_check()
 */
__static void
fidcache_put_locked(struct fidcache_memb_handle *f, list_cache_t *lc)
{
	int clean;
	/* Check for uninitialized
	 */
	if (!f->fcmh_state) {
		DEBUG_FCMH(PLL_FATAL, f, "not initialized!");
                psc_fatalx("Tried to put an uninitialized inode");
	}
	if (f->fcmh_cache_owner == lc) {
		DEBUG_FCMH(PLL_FATAL, f, "invalid list move");
		psc_fatalx("Tried to move to the same list (%p)", lc);
	}
	/* Validate the inode and check if it has some dirty blocks
	 */
	clean = fcmh_clean_check(f);

	if (lc == &fidcFreeList) {
		struct bmap_cache_memb tbmp;

		/* Valid sources of this inode.
		 */
		if ((f->fcmh_cache_owner == &fidcFreeList) ||
		    (f->fcmh_cache_owner == &fidcCleanList)) {

		} else psc_fatalx("Bad inode fcmh_cache_owner %p",
			       f->fcmh_cache_owner);

		psc_assert(ATTR_TEST(f->fcmh_state, FCM_CAC_FREEING));
		/* All bmaps and cache buffers must have been
		 *  released prior to this.
		 */
		psc_assert(!SPLAY_NEXT(bmap_cache, &f->fcmh_bmap_cache, &tbmp));
		psc_assert(lc_empty(&f->fcmh_buffer_cache));
		psc_assert(!atomic_read(&f->fcmh_bmap_cache_cnt));
		/* Re-initialize it before placing onto the free list
		 */
		fidcache_handle_init(f);

	} else if (lc == &fidcCleanList) {
		psc_assert(
			   f->fcmh_cache_owner == &fidcFreeList ||
			   f->fcmh_cache_owner == &fidcDirtyList);
                psc_assert(ATTR_TEST(f->fcmh_state, FCM_CAC_CLEAN));
		psc_assert(clean);

	} else if (lc == &fidcDirtyList) {
		psc_assert(f->fcmh_cache_owner == &fidcCleanList);
                psc_assert(ATTR_TEST(f->fcmh_state, FCM_CAC_DIRTY));
		psc_assert(clean == 0);

	} else
		psc_fatalx("lc %p is a bogus list cache", lc);

	/* Place onto the respective list.
	 */
	FCMHCACHE_PUT(f, lc);
}

/**
 * sl_fcm_init - (slash_fidcache_member_init) init a fid cache member.
 */
void
fidcache_memb_init(struct fidcache_memb *fcm)
{
	memset(fcm, 0, (sizeof *fcm));
	fcm->fcm_fg.fg_fid = FID_ANY;
	fcm->fcm_fg.fg_gen = FID_ANY;
}

/**
 * sl_fcmh_init - (slash_fidcache_handle_init) init a fid cache handle.
 */
void
fidcache_handle_init(void *p)
{
	struct fidcache_memb_handle *f = p;

	memset(f, 0, (sizeof *f));
	f->fcmh_fd    = -1;
	INIT_PSCLIST_HEAD(&f->fcmh_lentry);
	atomic_set(&f->fcmh_refcnt, 0);
	atomic_set(&f->fcmh_bmap_cache_cnt, 0);
	LOCK_INIT(&f->fcmh_lock);
	//SPLAY_INIT(&f->fcm_lessees); // XXX Where did lessees go?
	SPLAY_INIT(&f->fcmh_bmap_cache);
	lc_init(&f->fcmh_buffer_cache, struct sl_buffer, slb_fcm_lentry);
	fidcache_memb_init(&f->fcmh_memb);
}

/**
 * sl_fidc_init - (slash_fidcache_init) initialize the fid cache.
 */
void
fidcache_init(void)
{
	int rc;

	LOCK_INIT(&fidcCacheLock);
	lc_reginit(&fidcFreeList,  struct fidcache_memb_handle, fcmh_lentry, "fcmfree");
	lc_reginit(&fidcDirtyList, struct fidcache_memb_handle, fcmh_lentry, "fcmdirty");
	lc_reginit(&fidcCleanList, struct fidcache_memb_handle, fcmh_lentry, "fcmclean");

	fidcFreeList.lc_max = MDS_FID_CACHE_MAXSZ;
	rc = lc_grow(&fidcFreeList, MDS_FID_CACHE_DEFSZ, fidcache_handle_init);

	psc_assert(rc == MDS_FID_CACHE_DEFSZ);
}
