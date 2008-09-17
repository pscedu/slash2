/* $Id$ */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"

#include "cache_params.h"
#include "offtree.h"
#include "fid.h"
#include "fidcache.h"

#define SL_FIDCACHE_LOW_THRESHOLD 80 // 80%

struct psc_poolmgr fidcFreePool;
#define fidcFreeList fidcFreePool.ppm_lc
list_cache_t	 fidcDirtyList;
list_cache_t     fidcCleanList;

struct hash_table fidcHtable;

struct sl_fsops *slFsops=NULL;

void
fidc_put_locked(struct fidc_memb_handle *, list_cache_t *);

/**
 * fcmh_clean_check - placeholder!  This function will ensure the validity of the passed in inode.
 */
__static int
fcmh_clean_check(struct fidc_memb_handle *f)
{
	spinlock(&f->fcmh_lock);

	freelock(&f->fcmh_lock);
	// XXX Write me
	f = NULL;
	return 0;
}

/**
 * bmapc_memb_init - initialize a bmap structure and create its offtree.
 * @b: the bmap struct
 * @f: the bmap's owner
 */
void
bmapc_memb_init(struct bmapc_memb *b, struct fidc_memb_handle *f)
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
 * bmapc_cmp - bmap_cache splay tree comparator
 * @a: a bmapc_memb
 * @b: another bmapc_memb
 */
int
bmapc_cmp(const void *x, const void *y)
{
	const struct bmapc_memb *a = x, *b = y;

        if (a->bcm_blkno > b->bcm_blkno)
                return (1);
        else if (a->bcm_blkno < b->bcm_blkno)
                return (-1);
        return (0);
}

__static SPLAY_GENERATE(bmap_cache, bmapc_memb,
			bcm_tentry, bmapc_cmp);


static inline int
fidc_freelist_avail_check(void)
{
        psc_assert(fidcFreePool.ppm_lc.lc_size > 0);

        if ((fidcFreePool.ppm_lc.lc_nseen / SL_FIDCACHE_LOW_THRESHOLD) >
            (size_t)fidcFreePool.ppm_lc.lc_size)
                return 0;
        return 1;
}

/**
 * zinocache_reap - get some inodes from the clean list and free them.
 */
__static void
fidc_reap(void)
{
	struct fidc_memb_handle *f;
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
                fidc_put_locked(f, &fidcFreeList);
		ureqlock(&f->fcmh_lock, locked);

	} while(!fidc_freelist_avail_check());
}

/**
 * fidc_get - grab a clean fid from the cache.
 */
struct fidc_memb_handle *
fidc_get(list_cache_t *lc)
{
	struct fidc_memb_handle *f;

	f = lc_getnb(lc);
        if (f == NULL) {
                fidc_reap();        /* Try to free some fids */
                f = lc_getwait(lc);
        }
	if (lc == &fidcFreePool.ppm_lc) {
		psc_assert(f->fcmh_state == FCM_CAC_FREE);
		psc_assert(fcmh_clean_check(f));
		fcmh_incref(f);
	} else
		psc_fatalx("It's unwise to get inodes from %p, it's not "
			   "fidcFreePool.ppm_lc", lc);
	return (f);
}

/**
 * fidc_put_locked - release an inode onto an inode list cache.
 *
 * This routine should be called when:
 * - moving a free inode to the dirty cache.
 * - moving a dirty inode to the free cache.
 * Notes: inode is already locked.  Extensive validity checks are
 *        performed within fidc_clean_check()
 */
void
fidc_put_locked(struct fidc_memb_handle *f, list_cache_t *lc)
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

	if (lc == &fidcFreePool.ppm_lc) {
		struct bmapc_memb tbmp;

		/* Valid sources of this inode.
		 */
		if ((f->fcmh_cache_owner == &fidcFreePool.ppm_lc) ||
		    (f->fcmh_cache_owner == &fidcCleanList)) {

		} else psc_fatalx("Bad inode fcmh_cache_owner %p",
			       f->fcmh_cache_owner);

		psc_assert(ATTR_TEST(f->fcmh_state, FCM_CAC_FREEING));
		/* All bmaps and cache buffers must have been
		 *  released prior to this.
		 */
		psc_assert(!SPLAY_NEXT(bmap_cache,
				       &f->fcmh_bmapc, &tbmp));
		psc_assert(lc_empty(&f->fcmh_buffer_cache));
		psc_assert(!atomic_read(&f->fcmh_bmapc_cnt));
		/* Re-initialize it before placing onto the free list
		 */
		fidc_memb_handle_init(f);

	} else if (lc == &fidcCleanList) {
		psc_assert(
			   f->fcmh_cache_owner == &fidcFreePool.ppm_lc ||
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
 * fidc_lookup_simple - perform a simple lookup of a fid in the cache.  If the fid is found, its refcnt is incremented and it is returned.
 */
struct fidc_memb_handle *
fidc_lookup_simple (slfid_t f)
{
	struct hash_entry *e;

	e = get_hash_entry(&fidcHtable, (u64)f, NULL, NULL);
	if (e) {
		struct fidc_memb_handle *fcmh = e->private;

		psc_assert(fcmh);
		atomic_inc(&fcmh->fcmh_refcnt);
		return (fcmh);
	}
	return (NULL);
}

struct fidc_memb_handle *
fidc_lookup_inode (sl_inode_t *i)
{
	int     try_create=0;
	struct  fidc_memb_handle *fcmh;

 restart:
	fcmh = fidc_lookup_simple(i->ino_fg.fg_fid);
	if (fcmh) {
		if (try_create) {
			freelock(&fidcHtable.htable_lock);
			fidc_put_locked(fcmh, &fidcFreeList);
		}
		fcmh_clean_check(fcmh);

		if (i->ino_fg.fg_fid != fcmh_2_fid(fcmh))
                        goto restart;
	} else {
		if (!try_create) {
                        fcmh = fidc_get(&fidcFreeList);
                        spinlock(&fidcHtable.htable_lock);
                        try_create = 1;
                        goto restart;
                }
		fcmh_clean_check(fcmh);
		COPY_INODE_2_FCMH(i, fcmh);
		fcmh->fcmh_state &= ~(FCM_CAC_FREE);
		fcmh->fcmh_state |= FCM_CAC_CLEAN;
		atomic_inc(&fcmh->fcmh_refcnt);
		fidc_put_locked(fcmh, &fidcCleanList);
		init_hash_entry(&fcmh->fcmh_hashe, &(fcmh_2_fid(fcmh)), fcmh);
                add_hash_entry(&fidcHtable, &fcmh->fcmh_hashe);
		freelock(&fidcHtable.htable_lock);
	}
	return (fcmh);
}

__static int
fidc_xattr_load(slfid_t fid, sl_inodeh_t *inoh)
{
	char fidfn[FID_MAX_PATH];
	ssize_t sz=sizeof(sl_inode_t);
	psc_crc_t crc;
	int rc;

	fid_makepath(fid, fidfn);

	rc = fid_getxattr(fidfn, SFX_INODE,  &inoh->inoh_ino, sz);
	if (rc)
		return (rc);

	PSC_CRC_CALC(crc, &inoh->inoh_ino, sz);
	if (crc != inoh->inoh_ino.ino_crc) {
                psc_warnx("Crc failure on inode");
                errno = EIO;
                return -1;
        }
	/* XXX move me
	if (inoh->inoh_ino.ino_nrepls) {
                sz = sizeof(sl_replica_t) * inoh->inoh_ino.ino_nrepls;
                inoh->inoh_replicas = PSCALLOC(sz);
		rc = fid_getxattr(fidfn, SFX_INODE,  inoh->inoh_replicas, sz);

                PSC_CRC_CALC(&crc, inoh->inoh_replicas, sz);
                if (crc != inoh->inoh_ino.ino_rs_crc) {
                        psc_warnx("Crc failure on replicas");
                        errno = EIO;
                        return -1;
                }
        }
	*/
	return (0);
}

struct fidc_memb_handle *
fidc_lookup_immns (slfid_t f)
{
	struct fidc_memb_handle *fcmh;

	if ((fcmh = fidc_lookup_simple(f)) != NULL)
		return (fcmh);
	else {
		sl_inodeh_t inoh;

		if (fidc_xattr_load(f, &inoh))
			return (NULL);

		fcmh = fidc_lookup_inode(&inoh.inoh_ino);
		psc_assert(fcmh);
	}
	return (fcmh);
}

/**
 * fidc_memb_init - init a fidcache member.
 */
void
fidc_memb_init(struct fidc_memb *fcm)
{
	memset(fcm, 0, sizeof(*fcm));
	fcm->fcm_inodeh.inoh_ino.ino_fg.fg_fid = FID_ANY;
	fcm->fcm_inodeh.inoh_ino.ino_fg.fg_gen = FID_ANY;
}

/**
 * fidc_memb_handle_init - init a fidcache member handle.
 */
int
fidc_memb_handle_init(struct fidc_memb_handle *f)
{
	memset(f, 0, sizeof(*f));
	f->fcmh_fd = -1;
	INIT_PSCLIST_HEAD(&f->fcmh_lentry);
	atomic_set(&f->fcmh_refcnt, 0);
	atomic_set(&f->fcmh_bmapc_cnt, 0);
	LOCK_INIT(&f->fcmh_lock);
	SPLAY_INIT(&f->fcmh_bmapc);
	lc_init(&f->fcmh_buffer_cache, struct sl_buffer, slb_fcm_lentry);
	fidc_memb_init(&f->fcmh_memb);
	return (0);
}

/**
 * fidcache_init - initialize the fid cache.
 */
void
fidcache_init(enum fid_cache_users t, void (*fcm_init)(void *))
{
	int rc, htsz;
	ssize_t	fcdsz, fcmsz;

	switch (t) {
	case FIDC_USER_CLI:
		htsz  = FIDC_CLI_HASH_SZ;
		fcdsz = FIDC_CLI_DEFSZ;
		fcmsz = FIDC_CLI_MAXSZ;
		break;
	case FIDC_ION_HASH_SZ:
		htsz  = FIDC_ION_HASH_SZ;
		fcdsz = FIDC_ION_DEFSZ;
		fcmsz = FIDC_ION_MAXSZ;
		break;
	case FIDC_MDS_HASH_SZ:
		htsz  = FIDC_MDS_HASH_SZ;
		fcdsz = FIDC_MDS_DEFSZ;
		fcmsz = FIDC_MDS_MAXSZ;
		break;
	default:
		psc_fatalx("Invalid fidcache user type");
	}

	rc = psc_pool_init(&fidcFreePool, struct fidc_memb_handle,
	    fcmh_lentry, 0, fcdsz, fidc_memb_handle_init,
	    "fcmfreepool");
	fidcFreePool.ppm_max = fcmsz;

	lc_reginit(&fidcDirtyList, struct fidc_memb_handle,
		   fcmh_lentry, "fcmdirty");
	lc_reginit(&fidcCleanList, struct fidc_memb_handle,
		   fcmh_lentry, "fcmclean");

	init_hash_table(&fidcHtable, htsz, "fidcHtable");
}
