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
#include "psc_ds/pool.h"
#include "psc_ds/tree.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_ds/lockedlist.h"

#include "cache_params.h"
#include "slconfig.h"
#include "buffer.h"
#include "jflush.h"
#include "fid.h"
#include "inode.h"
#include "offtree.h"

extern struct hash_table fidcHtable;

#define FCMH_LOCK(f)  spinlock(&(f)->fcmh_lock)
#define FCMH_ULOCK(f) freelock(&(f)->fcmh_lock)
#define FCMH_LOCK_ENSURE(f) LOCK_ENSURE(&(f)->fcmh_lock)
#define BMAP_LOCK_ENSURE(b) LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_LOCK(b)  spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b) freelock(&(b)->bcm_lock)

#define FCMHCACHE_PUT(fcmh, list)					\
        do {                                                            \
		(fcmh)->fcmh_cache_owner = (list);			\
		if (list == &fidcFreePool.ppm_lc)			\
			psc_pool_return(&fidcFreePool, (fcmh));		\
		else							\
        	        lc_put((list), &(fcmh)->fcmh_lentry);		\
        } while (0)

extern struct psc_poolmgr fidcFreePool;
extern list_cache_t	fidcDirtyList;
extern list_cache_t	fidcCleanList;

/* sl_finfo - hold stats and lamport clock */
struct sl_finfo {
	//struct timespec slf_opentime;	   /* when we received client OPEN  */
	//struct timespec slf_closetime;   /* when we received client CLOSE */
	struct timespec	slf_lattr_update;  /* last attribute update         */
	u64		slf_opcnt;	   /* count attr updates            */
	size_t		slf_readb;	   /* num bytes read                */
	size_t		slf_writeb;	   /* num bytes written             */
};


struct sl_uid {
	u64	sluid_guid;	/* Stubs for global uids */
	u64	sluid_ggid;
	uid_t	sluid_suid;	/* site uid */
	gid_t	sluid_sgid;	/* site gid */
};

/*
 * fidc_memb - holds inode filesystem related data
 */
struct fidc_memb {
	sl_inodeh_t             fcm_inodeh;
	struct stat		fcm_stb;
	struct sl_finfo		fcm_slfinfo;
	struct sl_uid		fcm_uid;
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
 * possible I/O servers.  
 * XXX needs work to fit with the new structures for replication bitmaps.
 */
struct bmap_info {
	lnet_nid_t      bmapi_ion;                   /* MDS chosen io node  */
	u32             bmapi_mode;                  /* MDS tells cache pol */
	unsigned char   bmapi_auth[BMAP_AUTH_SZ];    /* Our write key       */
	struct slash_block_handle *bmapi_data;
};

// XXX should bmapi_mode be stored in bmap_info?
enum bmap_cli_modes {
        BMAP_CLI_RD   = (1<<0),  /* bmap has read creds       */
        BMAP_CLI_WR   = (1<<1),  /* write creds               */
	BMAP_CLI_DIO  = (1<<2),  /* bmap is in dio mode       */
	BMAP_CLI_MCIP = (1<<3),  /* "mode change in progress" */
	BMAP_CLI_MCC  = (1<<4)   /* "mode change complete"    */	
};

enum bmap_mds_modes {
	BMAP_MDS_WR     = (1<<0), 
	BMAP_MDS_RD     = (1<<1),
	BMAP_MDS_DIO    = (1<<2), /* directio */
	BMAP_MDS_FAILED = (1<<3), /* crc failure */
	BMAP_MDS_EMPTY  = (1<<4)  /* new bmap, not yet committed to disk*/
};

/*
 * bmapc_memb - central structure for block map caching used in
 *    all slash service contexts (mds, ios, client).
 *
 * bmapc_memb sits in the middle of the GFC stratum.
 * XXX some of these elements may need to be moved into the bcm_info_pri
 *     area (as part of new structures?) so save space on the mds.
 */
struct fidc_memb_handle;

struct bmapc_memb {
	sl_blkno_t	            bcm_blkno;    /* Bmap blk number */
	struct fidc_memb_handle    *bcm_fcmh;   /* pointer to fid info   */
	struct bmap_info	    bcm_bmapih;
	atomic_t		    bcm_rd_ref;	  /* one ref per write fd  */
	atomic_t		    bcm_wr_ref;	  /* one ref per read fd   */
	struct timespec		    bcm_ts;
	atomic_t                    bcm_opcnt;    /* pending opcnt         */
	u64                         bcm_holes[2]; /* one bit SLASH_BMAP_SIZE */
	union {
		void		       *bmt_mds_pri;
		struct offtree_root    *bmt_cli_oftr;
	} bmap_type;
	psc_spinlock_t              bcm_lock;
	struct psc_wait_queue       bcm_waitq;
	struct jflush_item          bcm_jfi;
	SPLAY_ENTRY(bmapc_memb) bcm_tentry; /* fcm tree entry        */
#define bcm_mds_pri bmap_type.bmt_mds_pri
#define bcm_oftr    bmap_type.bmt_cli_oftr				
};

#define bmap_set_accesstime(b) {				    \
		clock_gettime(CLOCK_REALTIME, &(b)->bcm_ts);	    \
	}


int
bmapc_cmp(const void *, const void *);

SPLAY_HEAD(bmap_cache, bmapc_memb);
SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

/*
 * fidc_memb_handle - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidc_memb tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps (fcm_lessees).
 */
struct fidc_memb_handle {
	struct fidc_memb	 fcmh_memb;
	struct psclist_head	 fcmh_lentry;
	struct timespec		 fcmh_access;
	list_cache_t		*fcmh_cache_owner;
	int			 fcmh_fd;
	u64                      fcmh_fh;
	u32			 fcmh_state;
	atomic_t		 fcmh_refcnt;
	void			*fcmh_pri;
	atomic_t                 fcmh_bmapc_cnt;
	struct bmap_cache	 fcmh_bmapc;         /* bmap cache splay */
	list_cache_t		 fcmh_buffer_cache;  /* chain our slbs   */
	psc_spinlock_t		 fcmh_lock;
	size_t                   fcmh_bmap_sz;
	struct hash_entry        fcmh_hashe;
	struct jflush_item       fcmh_jfi;
#define fcmh_cfd fcmh_fh
};

#define fcmh_2_fid(f)	(f)->fcmh_memb.fcm_inodeh.inoh_ino.ino_fg.fg_fid
#define fcmh_2_fgp(f)	(&(f)->fcmh_memb.fcm_inodeh.inoh_ino.ino_fg)
#define fcmh_2_fsz(f)	(f)->fcmh_memb.fcm_stb.st_size
#define fcmh_2_inoh(f)	(&(f)->fcmh_memb.fcm_inodeh)

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
			      struct slash_creds *creds,
			      struct fidc_memb_handle **fcm);
	/* sl_fgetattr - used for stat and open via fid, grabs all the
	 * attributes of the object and updates the fcm
	 */
	int (*slfsop_fgetattr)(struct fidc_memb_handle *fcm);
	/* sl_setattr - used to update the objects attrs, for now
	 * all attrs will be updated.  later we may refine the granularity
	 */
	int (*slfsop_bmap_load)(struct fidc_memb_handle *fcm, size_t num);     
	int (*slfsop_fsetattr)(struct fidc_memb_handle *fcm);
	/* sl_write - Object write.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_write)(struct fidc_memb_handle *fcm,
			    const void *buf, int count, off_t offset);
	/* sl_read - Object read.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_read)(struct fidc_memb_handle *fcm,
			   const void *buf, int count, off_t offset);
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*slfsop_getmap)(struct fidc_memb_handle  *fcm,
			     struct bmapc_memb *bcms, int count);

	int (*slfsop_invmap)(struct fidc_memb_handle *fcm,
			     struct bmap_refresh *bmr);
};

extern struct sl_fsops *slFsops;

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
	else if (lc == &fidcFreePool.ppm_lc)
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
		" fcmh@%p i+g:%"_P_U64"x+%"_P_U64"x s:"			\
		REQ_FCMH_FLAGS_FMT" lc:%s fd:%d r:%d:: "fmt,		\
		(fcmh),							\
		fcmh_2_fgp(fcmh)->fg_fid,				\
		fcmh_2_fgp(fcmh)->fg_gen,				\
		DEBUG_FCMH_FCMH_FLAGS(fcmh),				\
		fcmh_lc_2_string((fcmh)->fcmh_cache_owner),		\
		(fcmh)->fcmh_fd,					\
		atomic_read(&(fcmh)->fcmh_refcnt),			\
		## __VA_ARGS__)


#define DEBUG_BMAP(level, b, fmt, ...)					\
	_psclog(__FILE__, __func__, __LINE__,				\
		PSS_OTHER, (level), 0,					\
		" bmap@%p b:%u m:%u ion=(%s) i:%"_P_U64"x"		\
		"rref=%u wref=%u opcnt=%u "fmt,				\
		(b), (b)->bcm_blkno, (b)->bcm_bmapih.bmapi_mode,	\
		((b)->bcm_bmapih.bmapi_ion != LNET_NID_ANY) ?		\
		nid2str((b)->bcm_bmapih.bmapi_ion) : NULL,		\
		(b)->bcm_fcmh ? fcmh2fid((b)->bcm_fcmh) : NULL,		\
		atomic_read(&(b)->bcm_rd_ref),				\
		atomic_read(&(b)->bcm_wr_ref),				\
		atomic_read(&(b)->bcm_opcnt),				\
		## __VA_ARGS__)


static inline void
fcmh_incref(struct fidc_memb_handle *fch)
{
	fcm_set_accesstime(fch);
        atomic_inc(&fch->fcmh_refcnt);
	DEBUG_FCMH(PLL_TRACE, fch, "fcmh_incref");
}

static inline void
fcmh_decref(struct fidc_memb_handle *fch)
{
        atomic_dec(&fch->fcmh_refcnt);
        psc_assert(atomic_read(&fch->fcmh_refcnt) >= 0);
	DEBUG_FCMH(PLL_TRACE, fch, "fcmh_decref");
}

static inline struct bmapc_memb *
fcmh_bmap_lookup(struct fidc_memb_handle *fch, sl_blkno_t n)
{
	struct bmapc_memb lb, *b;
	int locked;

	lb.bcm_blkno=n;
	locked = reqlock(&fch->fcmh_lock);	
	b = SPLAY_FIND(bmap_cache, &fch->fcmh_bmapc, &lb);
	if (b)
		atomic_inc(&b->bcm_opcnt);
	ureqlock(&fch->fcmh_lock, locked);

	return (b);
}

void fidc_handle_init(struct fidc_memb_handle *);
void fidc_init(enum fid_cache_users, void (*)(void *));

struct fidc_memb_handle * fidc_get(list_cache_t *lc);

void bmapc_memb_init(struct bmapc_memb *b,
	struct fidc_memb_handle *f);

#endif /* __FIDC_H__ */
