#ifndef __FIDC_COMMON_H__
#define __FIDC_COMMON_H__ 1

#include "fid.h"
#include "fidcache.h"
#include "psc_ds/pool.h"
#include "psc_ds/hash.h"
#include "cache_params.h"

#define FCOO_START   0
#define FCOO_NOSTART 1

extern struct sl_fsops *slFsops;
extern struct hash_table fidcHtable;

extern struct fidc_membh *
fidc_lookup_fg(const struct slash_fidgen *);

extern struct fidc_membh *
fidc_lookup_simple (slfid_t);

extern struct fidc_membh *
__fidc_lookup_inode (const struct slash_fidgen *, int, 
		     const struct fidc_memb *, 
		     const struct slash_creds *);

enum fidc_lookup_flags {
	FIDC_LOOKUP_CREATE    = (1 << 0), /* Create if not present         */
	FIDC_LOOKUP_EXCL      = (1 << 1), /* Fail if fcmh is present       */
	FIDC_LOOKUP_COPY      = (1 << 2), /* Create from existing attrs    */
	FIDC_LOOKUP_LOAD      = (1 << 3), /* Create, get attrs from mds    */
	FIDC_LOOKUP_REFRESH   = (1 << 3), /* load and refresh are the same */
	FIDC_LOOKUP_FCOOSTART = (1 << 4), /* start the fcoo before exposing
					   *  the cache entry.              */
	FIDC_LOOKUP_NOREF     = (1 << 5)
};

/* Perform a simple fidcache lookup, returning NULL if DNE.
 */
#define fidc_lookup_inode(f) fidc_lookup_simple(f)

/* Create the inode from existing attributes.
 */
#define fidc_lookup_copy_inode(f, fcm, creds)				\
	__fidc_lookup_inode((f), (FIDC_LOOKUP_CREATE |			\
				  FIDC_LOOKUP_COPY   |			\
				  FIDC_LOOKUP_REFRESH),			\
			    (fcm), (creds))

/* Create the inode from existing attributes but don't ref it.
 *  This used for preloading the inode cache.
 */
#define fidc_lookup_copy_inode_noref(f, fcm, creds)			\
	__fidc_lookup_inode((f), (FIDC_LOOKUP_CREATE |			\
				  FIDC_LOOKUP_COPY   |			\
				  FIDC_LOOKUP_NOREF  |			\
				  FIDC_LOOKUP_REFRESH),			\
			    (fcm), (creds))

/* Create the inode if it doesn't exist loading its attributes from the network.
 */
#define fidc_lookup_load_inode(f, creds)				\
	({								\
		struct slash_fidgen __t = {f, FID_ANY};			\
		struct fidc_membh *__ret;				\
		__ret = __fidc_lookup_inode(&__t, (FIDC_LOOKUP_CREATE |	\
						   FIDC_LOOKUP_LOAD),	\
					    NULL, (creds));		\
		__ret;							\
	})

/* Create the inode from existing attributes only if one by the same id does not
 *  already exist.  Once it's created call fidc_fcoo_start_locked() so that only
 *  this thread may execute an open on the inode.
 * NOTE: This is needed for fuse create which does a create and open atomically.
 */
#define fidc_lookup_createopen_inode(f, fcm, creds)			\
	__fidc_lookup_inode(f, (FIDC_LOOKUP_CREATE |			\
				FIDC_LOOKUP_EXCL   |			\
				FIDC_LOOKUP_COPY   |			\
				FIDC_LOOKUP_FCOOSTART),			\
			    (fcm), (creds))

/* Increment an fcmh reference, fcmh_refcnt is used by the fidcache
 *  to determine which fcmh's may be reclaimed.
 */
#define fidc_membh_incref(f)						\
	do {								\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);	\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));		\
		atomic_inc(&(f)->fcmh_refcnt);				\
		DEBUG_FCMH(PLL_NOTIFY, (f), "incref");			\
	} while (0)

/* Drop an fcmh reference.
 */
#define fidc_membh_dropref(f)						\
	do {								\
		atomic_dec(&(f)->fcmh_refcnt);				\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));		\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);	\
		DEBUG_FCMH(PLL_NOTIFY, (f), "dropref");			\
	} while (0)

extern void 
fidc_fcm_setattr(struct fidc_membh *, const struct stat *);

extern int
fidc_membh_init(__unusedx struct psc_poolmgr *, void *);

extern void
fidc_memb_init(struct fidc_memb *fcm, slfid_t f);

extern void
fidc_fcoo_init(struct fidc_open_obj *f);

extern void
fidcache_init(enum fid_cache_users t, int (*fcm_reap_cb)(struct fidc_membh *));

extern int
fidc_fid2cfd(slfid_t f, u64 *cfd, struct fidc_membh **fcmh);

extern int
fidc_fcmh2cfd(struct fidc_membh *fcmh, u64 *cfd);

#endif
