/* $Id$ */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"
#include "fid.h"
#include "fidcache.h"

struct hash_table fidcHashTable;

/* Available fids */
list_cache_t	 fidcFreeList;

/* Dirty fids */
list_cache_t	 fidcDirtyList;

/* Cached fids avail for reaping */
list_cache_t     fidcCleanList;

size_t		 fidcAllocated;
psc_spinlock_t	 fidcCacheLock;

/*
 * bmap_cache_cmp - bmap_cache splay tree comparator
 * @a: a bmap_cache_memb
 * @b: another bmap_cache_memb
*/
int
bmap_cache_cmp(const void *x, const void *y)
{
	const struct bmap_cache_memb *a = x, *b = y;

        if (a->bcm_bmap.slbm_blkno > b->bcm_bmap.slbm_blkno)
                return 1;
        else if (a->bcm_bmap.slbm_blkno < b->bcm_bmap.slbm_blkno)
                return -1;
        return 0;
}

__static SPLAY_GENERATE(bmap_cache, bmap_cache_memb,
			bmap_cache_entry, bmap_cache_cmp);

/**
 * fidcache_reap - reclaim fids from the clean list and free them
 */
__static void
fidcache_reap(void)
{
	struct fcache_memb_handle *f;
	int locked;

	/* Try to reach the reserve threshold */
	do {
		f = lc_getnb(&fidcCleanList);
		if (f == NULL)
			return;

		locked = reqlock(&f->fcmh_lock);

		/*
		 * Make sure our clean list is 'clean' by
		 *  verifying the following conditions
		 */
		if (!fcmh_clean_check(f) ||
		    f->fcmh_state != FCMHS_CLEAN) {
			fcmh_dump(f);
			psc_fatalx("Invalid fid state for clean list");
		}

		/* Clean fids may have non-zero refcnts, skip these fids */
		if (atomic_read(&f->fcmh_refcnt)) {
			psc_info("fidc %p, has refcnt %d",
			      f, atomic_read(&f->fcmh_refcnt));
			//fcmh_dump(f);

			lc_put(&fidcCleanList,
			    &f->fcmh_cache_lentry);

			ureqlock(&f->fcmh_lock, locked);
			continue;
		}

		/* Free it */
		ATTR_SET(f->fcmh_state, FCMHS_FREEING);
		fidcache_put(f, &fidcFreeList);
		ureqlock(&f->fcmh_lock, locked);
	} while (!fcmh_freelist_avail_check());
}

/**
 * fidcache_get - grab a fid from a cache.
 */
__static struct fidcache_memb_handle *
fidcache_get(list_cache_t *lc, struct fcache_entry *f)
{
	struct fidcache_memb_handle *fc;

	f = lc_getnb(lc, 0);
	if (f == NULL) {
		fidcache_reap();	/* Try to free some fids */
		f = lc_getwait(lc);
	}

	if (lc == &fidcFreeList) {
		/*
		 * The fid should have nothing on its dirty lists
		 *  and have been initialized
		 */
		psc_assert(f->fcmh_state == FCMHS_FREE);
		psc_assert(fcmh_clean_check(f));
	} else
		psc_fatalx("tried to get fids from %p "
		    "instead of free list", lc);
	return (f);
}

/**
 * fidcache_put - release a fid onto a list cache.
 *
 * This routine should be called when:
 * - moving a free fid to the dirty cache.
 * - moving a dirty fid to the free cache.
 * Notes: fid should already be locked.  Extensive validity checks are
 *        performed within fcmh_clean_check()
 */
void
fidcache_put(struct fidcache_memb_handle *f, list_cache_t *lc)
{
	int rc, clean;

	/* Check for uninitialized */
	if (!f->fcmh_state) {
		fcmh_dump(f);
                psc_fatalx("Tried to put an uninitialized fid");
	}

	if (f->fcmh_cache_owner == lc) {
		fcmh_dump(f);
		psc_fatalx("fid tried to move to the same list");
	}

	/* Validate the fid and check if it has some dirty blocks */
	clean = FMDH_CHECK(f);

	/*
	 * If we are releasing a fid onto the free list,
	 * remove the corresponding hash table entry for it.
	 */
	if (lc == &fidcFreeList) {
		/* Valid sources of this fid */
		if (f->fcmh_cache_owner == FIDC_CACHE_INIT ||
		    f->fcmh_cache_owner == &fidcFreeList ||
		    f->fcmh_cache_owner == &fidcCleanList) {
		} else
			psc_fatalx("Bad fcmh_cache_owner %p",
			    f->fcmh_cache_owner);

		psc_assert(clean);

		psc_assert(ATTR_TEST(f->fcmh_state, FCMHS_FREEING));

		rc = del_hash_entry(&fidcHashTable,
		    f->fcmh_fid.zfid_inum);

#if 0
		/* Remove RPC export links. */
		psclist_for_each_entry_safe(f, next, &f->fcmh_eils) {
			zeil = psclist_entry(e, struct zeil, zeil_ino_entry);
			zeil_unregister(zeil);
			psclist_del(&zeil->zeil_ino_entry);
			free(zeil);
		}
#endif

		if (ATTR_TEST(f->fcmh_state, FCMHS_FREE))
			/* make sure free inodes weren't on the hash table */
			psc_assert(rc == -1);

		/* fid is free now. */
		fcmh_init(f);
	} else if (lc == &fidcCleanList) {
		psc_assert(
		    f->fcmh_cache_owner == &fidcFreeList ||
		    f->fcmh_cache_owner == &fidcDirtyList);
		psc_assert(ATTR_TEST(f->fcmh_state, FCMHS_CLEAN));
		psc_assert(clean);
	} else if (lc == &fidcDirtyList) {
		psc_assert(f->fcmh_cache_owner == &fidcCleanList);
		psc_assert(ATTR_TEST(f->fcmh_state, FCMHS_DIRTY));
		psc_assert(clean == 0);
	} else
		psc_fatalx("lc %p is a bogus list cache", lc);

	FIDCACHE_PUT(f, lc);
}

/**
 * fidcache_lookup - locate or create a hash table entry indexed by fid.
 * @fid: file ID to hash on.
 */
struct fidcache_memb_handle *
fidcache_lookup(struct slash_fid *fid)
{
	struct fidcache_memb_handle *f;
	struct hash_entry *e;
	int try_create = 0;

	psc_assert(fid->zfid_inum);

 restart:
	/*
	 * Check if the fid already exists in another
	 * connection/file descriptor.
	 *
	 * If try_create, the hash table is locked
	 */
	e = get_hash_entry(&fidcHashTable, fid->fid_inum, NULL);
	if (e) {
		/*
		 * Did we bounce here from the 'else' stmt?
		 *  This means that someone slipped in and made the inode
		 *  before us.  At this point just release the lock and
		 *  proceed as usual.
		 */
		if (try_create) {
			freelock(&fidcHashTable.htable_lock);
			/* release the pre-allocated inode */
			ATTR_SET(f->fcmh_state, FCMHS_FREEING);
			fidcache_put(f, &fidcFreeList);
		}

		f = e->private;

		spinlock(&f->fcmh_lock);

		fcmh_clean_check(f);

		/* by this point the inode is valid but is it ours?  */
		if (fid->fid_inum != f->fcmh_fid.zfid_inum) {
			freelock(&f->fcmh_lock);
			goto restart;
		}
		/*
		 * inc ref before releasing lock, otherwise the inode may
		 * be reaped from under us
		 */
		fcmh_incref(f);
		freelock(&f->fcmh_lock);
	} else {
		/*
		 * lock the entire cache here to avoid duplicate
		 *  entries
		 */
		if (!try_create) {
			/* have to get this.. we don't want to
			 *  block here while holding the hash lock!!
			 */
			f = fidcache_get(&fidcFreeList, fid);
			spinlock(&fidcHashTable.htable_lock);
			try_create = 1;
			goto restart;
		}

		/* We have the htable lock and just double-checked
		 *  for dups and none were found.  Go ahead and
		 *  create the inode
		 */
		psc_assert(f->fcmh_state == FCMHS_FREE);
		fcmh_clean_check(f);

		/* Init the inode */
		COPYFID(&f->fcmh_fid, fid);

		/* no need to lock here, we're the only thread
		 *  accessing this inode
		 * XXX unless someone has a dangling reference to it
		 */
		fcmh_incref(f);

		ATTR_UNSET(f->fcms_state, FCMHS_FREE);
		ATTR_SET(f->fcmh_state, FCMHS_CLEAN);

		/* Place it on the clean list */
		fidcache_put(f, &fidcCleanList);

		/* XXX fill out fcmh_stb a little with inum. */
		init_hash_entry(&f->fcmh_hentry,
				&f->fcmh_fid.zfid_inum, ino);

		/* make the inode viewable by all */
		add_hash_entry(&fidcHashTable, &ino->fcmh_hentry);

		/* ok release the hash table lock */
		freelock(&fidcHashTable.htable_lock);
	}
	return (ino);
}

void
fcmh_init(struct fidcache_memb_handle *f)
{
	f->fcmh_cache_owner = &fidcFreeList;
	ATTR_SET(f->fcmh_state, FCMHS_FREEING);
	f->fcmh_gen = 0;
	LOCK_INIT(&f->fcmh_lock);
	ZINOCACHE_PUT(f, &fidcFreeList);
}

/*
 * fidcache_init - initialize the various inode caches.
 */
void
fidcache_init(void)
{
	struct fidcache_memb_handle *f;

	LOCK_INIT(&fidcCacheLock);
	lc_reginit(&fidcFreeList, "inofree");
	lc_reginit(&fidcDirtyList, "inodirty");
	lc_reginit(&fidcCleanList, "inoclean");

	fidcFreeList.lc_max = FIDCACHE_ALLOC_MAX;

	init_hash_table(&fidcHashTable, FIDCACHE_HASHTABLE_SIZE, "inode");

	f = PSCALLOC(FIDCACHE_ALLOC_DEF * sizeof(*f));
	for (i = 0; i < FIDCACHE_ALLOC_DEF; i++, f++)
		fcmh_init(f);
}
