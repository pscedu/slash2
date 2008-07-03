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

#include "slconfig.h"
#include "buffer.h"
#include "fid.h"
#include "inode.h"
#include "offtree.h"

/* Hand computed */
#define MDS_FID_CACHE_DEFSZ 1024   /* Number of fcmh's to allocate by default */
#define MDS_FID_CACHE_MAXSZ 131072 /* Max fcmh's */

#define SLASH_BMAP_SIZE  134217728
#define SLASH_BMAP_WIDTH 8
#define SLASH_BMAP_DEPTH 5
#define SLASH_BMAP_SHIFT 11
/* End hand computed */

#define SLASH_BMAP_BLKSZ (SLASH_BMAP_SIZE / power((size_t)SLASH_BMAP_WIDTH, \
						  (size_t)(SLASH_BMAP_DEPTH-1)))
#define SLASH_BMAP_BLKMASK ~(SLASH_BMAP_BLKSZ-1)

#define SLASH_MAXBLKS_PER_REQ (LNET_MTU / SLASH_BMAP_BLKSZ)

#define BMAP_MAX_GET 63

#define FCMH_LOCK(h)  spinlock(&(h)->fcmh_lock)
#define FCMH_ULOCK(h) freelock(&(h)->fcmh_lock)

#define FCMHCACHE_PUT(fcmh, list)					\
        do {                                                            \
                (fcmh)->fcmh_cache_owner = (list);			\
                lc_put((list), &(fcmh)->fcmh_lentry);			\
        } while (0)

extern list_cache_t	fidcFreeList;
extern list_cache_t	fidcDirtyList;
extern list_cache_t	fidcCleanList;
extern psc_spinlock_t	fidcCacheLock;

/* sl_finfo - hold stats and lamport clock */
struct sl_finfo {
	//struct timespec slf_opentime;	   /* when we received client OPEN  */
	//struct timespec slf_closetime;   /* when we received client CLOSE */
	struct timespec	slf_lattr_update;  /* last attribute update         */
	u64		slf_opcnt;	   /* count attr updates            */
	size_t		slf_readb;	   /* num bytes read                */
	size_t		slf_writeb;	   /* num bytes written             */
};

/*
 * fidcache_memb - holds inode filesystem related data
 */
struct fidcache_memb {
	struct slash_fidgen	fcm_fg;
	struct stat		fcm_stb;
	struct sl_finfo		fcm_slfinfo;
	//struct sl_uid		fcm_uid;
};

struct sl_uid {
//	u64	sluid_guid;	/* Stubs for global uids */
//	u64	sluid_ggid;
	uid_t	sluid_suid;	/* site uid */
	gid_t	sluid_sgid;	/* site gid */
};

struct bmap_refresh {
	struct slash_fidgen	bmrfr_fg;
	sl_blkno_t		bmrfr_blk;
	u8			bmrfr_flags;
};

/*
 * bmap_info_cli - hangs from the void * pointer in the sl_resm_t struct.
 *  It's tasked with holding the import to the correct ION.
 */
struct bmap_info_cli {
	struct pscrpc_import *bmic_import;
};

#define BMAP_AUTH_SZ 8
/*
 * bmap_info - for each block in the fidcache, associate the set of
 * possible I/O servers and the store the CRC of the block.
 */
struct bmap_info {
	lnet_nid_t      bmapi_ion;                   /* MDS chosen io node  */
	sl_ios_id_t	bmapi_ios[SL_DEF_REPLICAS];  /* Replica store       */
	unsigned char   bmapi_auth[BMAP_AUTH_SZ];    /* Our write key       */
};

/*
 * bmap_cache_memb - central structure for block map caching used in
 *    all slash service contexts (mds, ios, client).
 *
 * bmap_cache_memb sits in the middle of the GFC stratum.
 */
struct bmap_cache_memb {
	sl_blkno_t	        bcm_blkno;       /* Bmap blk number */
	struct timespec		bcm_ts;
	struct bmap_info	bcm_bmapih;
	atomic_t		bcm_refcnt;	 /* one ref per client (mds) */
	void		       *bcm_info_pri;    /* point to private data    */
	struct fidcache_memb   *bcm_fcm;         /* pointer to fid info      */
	struct offtree_root    *bcm_oftr;
	psc_spinlock_t          bcm_lock;
	u32                     bcm_flags;
	SPLAY_ENTRY(bmap_cache_memb) bcm_tentry; /* fcm tree entry */
};

int
bmap_cache_cmp(const void *, const void *);

SPLAY_HEAD(bmap_cache, bmap_cache_memb);
SPLAY_PROTOTYPE(bmap_cache, bmap_cache_memb, bcm_tentry, bmap_cache_cmp);

/*
 * fidcache_memb_handle - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidcache_memb tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps (fcm_lessees).
 */
struct fidcache_memb_handle {
	struct fidcache_memb	 fcmh_memb;
	struct psclist_head	 fcmh_lentry;
	struct timespec		 fcmh_access;
	list_cache_t		*fcmh_cache_owner;
	int			 fcmh_fd;
	u64                      fcmh_fh;
	u32			 fcmh_state;
	atomic_t		 fcmh_refcnt;
	void			*fcmh_info_pri;
	atomic_t                 fcmh_bmap_cache_cnt;
	struct bmap_cache	 fcmh_bmap_cache;    /* splay tree of bmap cache */
	list_cache_t		 fcmh_buffer_cache;  /* list of data buffers (slb)*/
	psc_spinlock_t		 fcmh_lock;
};

#define fcm_set_accesstime(f) {					    \
		clock_gettime(CLOCK_REALTIME, &(f)->fcmh_access);   \
	}

enum fcmh_states {
	FCM_CAC_CLEAN   = (1 << 0),
	FCM_CAC_DIRTY   = (1 << 1),
	FCM_CAC_FREEING = (1 << 2),
	FCM_CAC_FREE    = (1 << 3),
	FCM_ATTR_FID    = (1 << 4),  /* Have fidcache memb */
	FCM_ATTR_SIZE   = (1 << 5),
	FCM_ATTR_STAT   = (1 << 6)
};

struct sl_fsops {
	/* sl_getattr - used for stat and open, loads the objects
	 *  via the pathname.
	 */
	int (*slfsop_getattr)(const char *path,
			      struct fidcache_memb_handle *fcm);
	/* sl_fgetattr - used for stat and open via fid, grabs all the
	 * attributes of the object and updates the fcm
	 */
	int (*slfsop_fgetattr)(struct fidcache_memb_handle *fcm);
	/* sl_setattr - used to update the objects attrs, for now
	 * all attrs will be updated.  later we may refine the granularity
	 */
	int (*slfsop_setattr)(struct fidcache_memb_handle *fcm);
	/* sl_write - Object write.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_write)(struct fidcache_memb_handle *fcm,
			    const void *buf, int count, off_t offset);
	/* sl_read - Object read.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_read)(struct fidcache_memb_handle *fcm,
			   const void *buf, int count, off_t offset);
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*slfsop_getmap)(struct fidcache_memb_handle  *fcm,
			     struct bmap_cache_memb *bcms, int count);

	int (*slfsop_invmap)(struct fidcache_memb_handle *fcm,
			     struct bmap_refresh *bmr);
};

#define FCMH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_FCMH_FCMH_FLAGS(fcmh)				      \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_CAC_CLEAN),   "C"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_CAC_DIRTY),   "D"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_CAC_FREEING), "F"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_CAC_FREE),    "f"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_ATTR_FID),    "f"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_ATTR_SIZE),   "s"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_ATTR_STAT),   "S")

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s%s"

static inline const char *
fcmh_lc_2_string(list_cache_t *lc)
{
	if (lc == &fidcCleanList)
		return "Clean";
	else if (lc == &fidcFreeList)
		return "Free";
	else if (lc == &fidcDirtyList)
		return "Dirty";
	else if (lc == NULL)
		return "Null";
	psc_fatalx("bad fidc list cache %p", lc);
}

#define DEBUG_FCMH(level, fcmh, fmt, ...)				\
	_psclog(__FILE__, __func__, __LINE__,				\
		PSS_OTHER, (level), 0,					\
		" fcmh@%p i+g:"LPX64"+"LPX64" s:"			\
		REQ_FCMH_FLAGS_FMT" lc:%s fd:%d r:%d:: "fmt,		\
		(fcmh),							\
		(fcmh)->fcmh_memb.fcm_fg.fg_fid,			\
		(fcmh)->fcmh_memb.fcm_fg.fg_gen,			\
		DEBUG_FCMH_FCMH_FLAGS(fcmh),				\
		fcmh_lc_2_string((fcmh)->fcmh_cache_owner),		\
		(fcmh)->fcmh_fd,					\
		atomic_read(&(fcmh)->fcmh_refcnt),			\
		## __VA_ARGS__)


static inline void
fcmh_incref(struct fidcache_memb_handle *fch)
{
	fcm_set_accesstime(fch);
        atomic_inc(&fch->fcmh_refcnt);
	DEBUG_FCMH(PLL_TRACE, fch, "fcmh_incref");
}

static inline void
fcmh_decref(struct fidcache_memb_handle *fch)
{
        atomic_dec(&fch->fcmh_refcnt);
        psc_assert(atomic_read(&fch->fcmh_refcnt) >= 0);
	DEBUG_FCMH(PLL_TRACE, fch, "fcmh_decref");
}

void fidcache_handle_init(void *p);
void fidcache_init(void);

void bmap_cache_memb_init(struct bmap_cache_memb *b,
	struct fidcache_memb_handle *f);

#endif /* __FIDCACHE_H__ */
