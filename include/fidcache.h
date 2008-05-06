/* $Id$ */

#ifndef __FIDCACHE_H__
#define __FIDCACHE_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include "psc_types.h"
#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/tree.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"

#include "buffer.h"
#include "fid.h"
#include "inode.h"

#define FIDCACHE_HTABLE_SZ	32767

extern struct hash_table fidcHashTable;

extern list_cache_t	fidcFreeList;
extern list_cache_t	fidcDirtyList;
extern list_cache_t	fidcCleanList;
extern psc_spinlock_t	fidcCacheLock;

/* sl_finfo - hold stats and lamport clock */
struct sl_finfo {
	struct timeval	slf_opentime;		/* when we received client OPEN  */
	struct timeval	slf_closetime;		/* when we received client CLOSE */
	struct timeval	slf_lattr_update;	/* last attribute update         */
	u64		slf_opcnt;		/* count attr updates            */
	size_t		slf_readb;		/* num bytes read                */
	size_t		slf_writeb;		/* num bytes written             */
};

struct sl_uid {
//	u64	sluid_guid;	/* Stubs for global uids */
//	u64	sluid_ggid;
	uid_t	sluid_suid;	/* site uid */
	gid_t	sluid_sgid;	/* site gid */
};

struct bmap_refresh {
	slash_fid_t	bmrfr_fid;
	sl_blkno_t	bmrfr_blk;
	u8		bmrfr_flags;
};

/*
 * bmap_cli_info - for each block in the fidcache, associate the set of
 * possible I/O servers and the store the CRC of the block.
 */
struct bmap_info {
	sl_blkno_t	bmapi_blkno;
	sl_ios_id_t	bmapi_ios[SL_DEF_REPLICAS];
	sl_gcrc_t	bmapi_gencrc;
};

/*
 * bmap_info_handle - holder for bmap_info (which is a wire struct).
 */
//struct bmap_info_handle {
//	struct bmap_info	bmapih_info;
//	struct psc_list_head	bmapih_lentry;
//};

/*
 * bmap_cache_memb - central structure for block map caching used in
 * all slash service contexts (mds, ios, client).
 *
 * bmap_cache_memb sits in the middle of the GFC stratum.
 */
struct bmap_cache_memb {
	struct timeval		 bcm_ts;
	struct bmap_info	 bcm_bmap_info;
	atomic_t		 bcm_refcnt;		/* one ref per client (mds) */
	void			*bcm_info_pri;
	struct psclist_head	 bcm_lentry;		/* lru chain */
	struct psclist_head	 bcm_buffers;		/* track our buffers */
	SPLAY_ENTRY(bmap_cache_memb) bcm_tentry;	/* fcm tree entry */
};

int
bmap_cache_cmp(const void *, const void *);

SPLAY_HEAD(bmap_cache, bmap_cache_memb);
SPLAY_PROTOTYPE(bmap_cache, bmap_cache_memb, bcm_tentry, bmap_cache_cmp);

/*
 * fidcache_memb - holds inode filesystem related data
 */
struct fidcache_memb {
	struct slash_fid	fcm_fid;
	struct stat		fcm_stb;
	struct sl_finfo		fcm_slfinfo;
	struct sl_uid		fcm_uid;
};

/*
 * fidcache_memb_handle - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidcache_memb tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps (fcm_lessees).
 */
typedef struct fidcache_memb_handle {
	struct fidcache_memb	*fcmh_memb;
	struct hash_entry	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct timeval		 fcmh_access;
	list_cache_t		*fcmh_cache_owner;
	int			 fcmh_fd;
	u32			 fcmh_state;
	atomic_t		 fcmh_refcnt;
	void			*fcmh_objm;		/* mmap region for filemap md */
	size_t			 fcmh_objm_sz;		/* nbytes mapped for objm */
	void			*fcmh_info_pri;
	struct bmap_cache	 fcmh_bmap_cache;	/* splay tree of bmap cache */
	list_cache_t		 fcmh_buffer_cache;	/* list of data buffers (slb)*/
	psc_spinlock_t		 fcmh_lock;
};

/* fidcache member handle states */
#define FCMHS_CLEAN	(1 << 0)
#define FCMHS_DIRTY	(1 << 1)
#define FCMHS_FREEING	(1 << 2)

static inline void
fchm_init(struct fidcache_memb_handle *f)
{
	LOCK_INIT(&f->fcmh_lock);
	lc_init(&f->fcmh_buffer_cache, struct sl_buffer, slb_fcm_lentry);
	atomic_set(&f->fcmh_refcnt, 0);
	f->fcmh_cache_owner = NULL;
}

struct sl_fsops {
	/* sl_getattr - used for stat and open, loads the objects
	 *  via the pathname.
	 */
	int (*slfsop_getattr)(const char *path, struct fidcache_memb *fcm);
	/* sl_fgetattr - used for stat and open via fid, grabs all the
	 * attributes of the object and updates the fcm
	 */
	int (*slfsop_fgetattr)(struct fidcache_memb *fcm);
	/* sl_setattr - used to update the objects attrs, for now
	 * all attrs will be updated.  later we may refine the granularity
	 */
	int (*slfsop_setattr)(struct fidcache_memb *fcm);
	/* sl_write - vectorized object write.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_write)(struct fidcache_memb *fcm,
			    const struct iovec *vector, int count);
	/* sl_read - vectorized object read.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_read)(struct fidcache_memb *fcm,
			   const struct iovec *vector, int count);
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*slfsop_getmap)(struct fidcache_memb  *fcm,
			     struct bmap_cache_memb *bcms, int count);

	int (*slfsop_invmap)(struct fidcache_memb *fcm, struct bmap_refresh *bmr);
};

#define FCMH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_FCMH_FCMH_FLAGS(fcmh)					\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCM_CLEAN), "C"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCM_DIRTY), "D"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCM_FREEING), "F")

#define REQ_FCMH_FLAGS_FMT "%s%s%s"

static inline const char *
fcmh_lc_2_string(list_cache_t *lc)
{
	if (lc == &fidcCleanList)
		return "Clean";
	else if (lc == &fidcFreeList)
		return "Free";
	else if (lc == &fidcDirtyList)
		return "Dirty";
	psc_fatalx("bad fidc list cache %p", lc);
}

#define DEBUG_FCMH(level, fcmh, fmt, ...)			\
	_psclog(__FILE__, __func__, __LINE__,			\
		PSS_OTHER, (level), 0,				\
		" fcmh@%p inum+gen:"LPX64"+"LPX64" state:"	\
		REQ_FCMH_FLAGS_FMT" cowner:%s fd:%d :: "fmt,	\
		(fcmh), (fcmh)->fcmh_memb.fcm_fid.fid_inum,	\
		(fcmh)->fcmh_memb.fcm_fid.fid_gen,		\
		DEBUG_FCMH_FCMH_FLAGS(fcmh), (fcmh)->fcmh_fd,	\
		atomic_read(&(fcmh)->fcmh_refcnt),		\
		## __VA_ARGS__)					\

#endif /* __FIDCACHE_H__ */
