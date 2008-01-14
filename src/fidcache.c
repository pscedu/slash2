/* $Id: zestInodeCache.c 2430 2008-01-04 21:08:57Z yanovich $ */

#include <stdio.h>

#include "psc_util/atomic.h"
#include "fid.h"
#include "fidcache.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/cdefs.h"

/*
 * Available inodes
 */
list_cache_t	 fidcFreeList;
/*
 * Dirty inodes which have no completed parity groups
 */
list_cache_t	 fidcDirtyList;
/*
 * Cached inodes avail for reaping, inodes here have no dirty
 *  blocks
 */
list_cache_t     fidcCleanList;

size_t		 fidcAllocated;
zest_spinlock_t	 fidcCacheLock;

/*  
 * bmap_cache_cmp - bmap_cache splay tree comparator
 * @a: a bmap_cache_memb
 * @b: another bmap_cache_memb
*/
int
bmap_cache_cmp(struct bmap_cache_memb *a, struct bmap_cache_memb *b)
{
        if (a->bcm_bmap.slbm_blkno > b->bcm_bmap.slbm_blkno)
                return 1;
        else if (a->bcm_bmap.slbm_blkno < b->bcm_bmap.slbm_blkno)
                return -1;

        return 0;
}

__static SPLAY_GENERATE(bmap_cache, bmap_cache_memb, 
			bmap_cache_entry, bmap_cache_cmp);

/**
 * zinocache_reap - get some inodes from the clean list and free them
 */
__static void
zinocache_reap(void)
{
	struct psclist_head *e;
	fcache_entry_t      *fce;
	int locked;
	/*
	 * Try to reach the reserve threshold
	 */
	do {
		e = lc_get(&fidcCleanList, 0);
		if (e == NULL)
			/* didn't get anything */
			return;

		ino = psclist_entry(e, fidc_t, fidc_cache_lentry);
		locked = reqlock(&ino->fidc_lock);
		/*
		 * Make sure our clean list is 'clean' by
		 *  verifying the following conditions
		 */
		if (!fidc_clean_check(ino) ||
		    ino->fidc_state != FIDC_CLEAN) {
			fidc_dump(ino);
			psc_fatalx("Invalid inode state for clean list");
		}
		/*
		 * Clean inodes may have non-zero refcnts, skip these
		 *   inodes.
		 */
		if (atomic_read(&ino->fidc_refcnt)) {
			psc_info("fidc %p, has refcnt %d",
			      ino, atomic_read(&ino->fidc_refcnt));
			//fidc_dump(ino);

			lc_put(&fidcCleanList,
			    &ino->fidc_cache_lentry);

			ureqlock(&ino->fidc_lock, locked);
			continue;
		}
		/*
		 * Free it
		 */
		ATTR_SET(ino->fidc_state, FIDC_FREEING);
		zinocache_put(ino, &fidcFreeList);
		ureqlock(&ino->fidc_lock, locked);

	} while(!fidc_freelist_avail_check());
}

/**
 * zinocache_get - grab an inode from an inode list cache.
 */
__static fidc_t *
zinocache_get(list_cache_t *zlc, struct zest_fid *fid)
{
	struct psclist_head *e;
	fidc_t *ino;

	e = lc_get(zlc, 0);
	if (e == NULL) {
		zinocache_reap(); /* Try to free some inodes */
		e = lc_get(zlc, 1); /* try again, blocking here */
	}
	ino = psclist_entry(e, fidc_t, fidc_cache_lentry);

	if (zlc == &fidcFreeList) {
		/*
		 * The inode should have nothing on its dirty lists
		 *  and have been initialized
		 */
		psc_assert(ino->fidc_state == FIDC_FREE);
		psc_assert(fidc_clean_check(ino));

		lc_register(&ino->fidc_incoming,
		    "ino-"FIDFMT"-inc", FIDFMTARGS(fid));
		lc_register(&ino->fidc_unprotected,
		    "ino-"FIDFMT"-unprot", FIDFMTARGS(fid));
		lc_register(&ino->fidc_sync_error,
		    "ino-"FIDFMT"-syncerr", FIDFMTARGS(fid));
	} else
		psc_fatalx("It's unwise to get inodes from %p, it's not "
		       "fidcFreeList", zlc);
	return (ino);
}

/**
 * zinocache_put - release an inode onto an inode list cache.
 *
 * This routine should be called when:
 * - moving a free inode to the dirty cache.
 * - moving a dirty inode to the free cache.
 * Notes: inode is already locked.  Extensive validity checks are
 *        performed within fidc_clean_check()
 */
void
zinocache_put(fidc_t *ino, list_cache_t *zlc)
{
	int rc, clean;
	/*
	 * Check for uninitialized
	 */
	if (!ino->fidc_state) {
		fidc_dump(ino);
                psc_fatalx("Tried to put an uninitialized inode");
	}

	if (ino->fidc_cache_owner == zlc) {
		fidc_dump(ino);
		psc_fatalx("Inode tried to move to the same list");
	}
	/*
	 * Validate the inode and check if it has some dirty blocks
	 */
	clean = FIDC_LIST_CHECK(ino);
	/*
	 * If we are releasing an inode onto the free list,
	 * remove the corresponding hash table entry for it.
	 */
	if (zlc == &fidcFreeList) {
		struct psclist_head *e, *next;
		struct zeil       *zeil;
		/*
		 * Valid sources of this inode
		 */
		if ((ino->fidc_cache_owner == FIDC_CACHE_INIT) ||
		    (ino->fidc_cache_owner == &fidcFreeList) ||
		    (ino->fidc_cache_owner == &fidcCleanList)) {

		} else psc_fatalx("Bad inode fidc_cache_owner %p",
			       ino->fidc_cache_owner);

		psc_assert(clean);

		psc_assert(ATTR_TEST(ino->fidc_state, FIDC_FREEING));

		lc_unregister(&ino->fidc_incoming);
		lc_unregister(&ino->fidc_unprotected);
		lc_unregister(&ino->fidc_sync_error);

		rc = del_hash_entry(&fidcHashTable,
				    ino->fidc_fid.zfid_inum);

		/* Remove RPC export links. */
		psclist_for_each_safe(e, next, &ino->fidc_zeils) {
			zeil = psclist_entry(e, struct zeil, zeil_ino_entry);
			zeil_unregister(zeil);
			psclist_del(&zeil->zeil_ino_entry);
			free(zeil);
		}

		if (ATTR_TEST(ino->fidc_state, FIDC_FREE))
			/* make sure free inodes weren't on the hash table */
			psc_assert(rc == -1);

		/* Inode is "free" now. */
		fidc_init(ino);

	} else if (zlc == &fidcCleanList) {
		psc_assert(
		    ino->fidc_cache_owner == &fidcFreeList ||
		    ino->fidc_cache_owner == &fidcDirtyList);
		psc_assert(ATTR_TEST(ino->fidc_state, FIDC_CLEAN));
		psc_assert(clean);

	} else if (zlc == &fidcDirtyList) {
		psc_assert(ino->fidc_cache_owner == &fidcCleanList);
		psc_assert(ATTR_TEST(ino->fidc_state, FIDC_DIRTY));
		psc_assert(clean == 0);

	} else
		psc_fatalx("zlc %p is a bogus list cache", zlc);

	ZINOCACHE_PUT(ino, zlc);
}

/**
 * fidc_lookup - locate or create an inode hash table entry indexed by fid.
 * @fid: file ID to hash on.
 */
fidc_t *
zinocache_lookup(zest_fid_t *fid)
{
	struct hash_entry *e;
	fidc_t          *ino;
	int                try_create = 0;

	psc_assert(fid->zfid_inum);

 restart:
	/*
	 * Check if an inode for the file already exists
	 * in another connection/file descriptor.
	 *
	 * If try_create, the hash table is locked
	 */
	e = get_hash_entry(&fidcHashTable, fid->zfid_inum, NULL);
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
			ATTR_SET(ino->fidc_state, FIDC_FREEING);
			zinocache_put(ino, &fidcFreeList);
		}

		ino = e->private;

		spinlock(&ino->fidc_lock);

		(int)fidc_clean_check(ino);
		/*
		 * by this point the inode is valid but is it ours?
		 */
		if (fid->zfid_inum != ino->fidc_fid.zfid_inum) {
			freelock(&ino->fidc_lock);
			goto restart;
		}
		/*
		 *inc ref before releasing lock, otherwise the inode may
		 * be reaped from under us
		 */
		fidc_incref(ino);

		freelock(&ino->fidc_lock);
	} else {
		/*
		 * lock the entire cache here to avoid duplicate
		 *  entries
		 */
		if (!try_create) {
			/* have to get this.. we don't want to
			 *  block here while holding the hash lock!!
			 */
			ino        = zinocache_get(&fidcFreeList, fid);
			spinlock(&fidcHashTable.htable_lock);
			try_create = 1;
			goto restart;
		}
		/* We have the htable lock and just double-checked
		 *  for dups and none were found.  Go ahead and
		 *  create the inode
		 */
		psc_assert(ino->fidc_state == FIDC_FREE);
		(int)fidc_clean_check(ino);

		/* Init the inode */
		COPYFID(&ino->fidc_fid, fid);

		/* no need to lock here, we're the only thread
		 *  accessing this inode
		 * XXX unless someone has a dangling reference to it
		 */
		fidc_incref(ino);

		ATTR_UNSET(ino->fidc_state, FIDC_FREE);
		ATTR_SET(ino->fidc_state, FIDC_CLEAN);

		/* Place it on the clean list */
		zinocache_put(ino, &fidcCleanList);

		/* XXX fill out fidc_stb a little with inum. */
		init_hash_entry(&ino->fidc_hentry,
				&ino->fidc_fid.zfid_inum, ino);

		/* make the inode viewable by all */
		add_hash_entry(&fidcHashTable, &ino->fidc_hentry);

		/* ok release the hash table lock */
		freelock(&fidcHashTable.htable_lock);
	}
	return (ino);
}

/*
 * zinocache_alloc - create new inodes and add to free inode list.
 * @n: number of inodes to add.
 */
int
zinocache_alloc(size_t n)
{
	fidc_t *ino, *inodes;
	size_t i;
	int rc;

	rc = 0;
	spinlock(&fidcCacheLock);
	if (fidcAllocated + n > ZINOCACHE_ALLOC_MAX) {
		rc = ENOMEM;
		goto done;
	}
	fidcAllocated += n;

	ino = inodes = PSCALLOC(FIDCSZ * n);
	for (i = 0; i < n; i++, ino++) {
		ino->fidc_cache_owner = &fidcFreeList;
		ATTR_SET(ino->fidc_state, FIDC_FREEING);
		fidc_init(ino);
		ino->fidc_gen = 0;
		LOCK_INIT(&ino->fidc_lock);
		ZINOCACHE_PUT(ino, &fidcFreeList);
	}
 done:
	freelock(&fidcCacheLock);
	return (rc);
}

/*
 * zinocache_init - initialize the various inode caches.
 */
void
zinocache_init(void)
{
	int rc;

	LOCK_INIT(&fidcCacheLock);
	lc_reginit(&fidcFreeList, "inofree");
	lc_reginit(&fidcDirtyList, "inodirty");
	lc_reginit(&fidcCleanList, "inoclean");

	fidcFreeList.lc_max = ZINOCACHE_ALLOC_MAX;

	init_hash_table(&fidcHashTable, ZINOCACHE_HASHTABLE_SIZE, "inode");

	rc = zinocache_alloc(ZINOCACHE_ALLOC_DEF);
	psc_assert(rc == 0);
}
