#ifndef FID_CACHE_H
#define FID_CACHE_H 1

#include "psc_types.h"
#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_ds/tree.h"
#include "psc_ds/listcache.h"
#include "psc_util/atomic.h"
#include "fid.h"

#define MDS_FID_CACHE_SZ 131071
#define IOS_FID_CACHE_SZ 131071
#define CLI_FID_CACHE_SZ 1023

#define FID_CACHE_HTABLE_SZ 32767

#define FCH_LOCK(h)  spinlock(&(h)->fch_lock)
#define FCH_ULOCK(h) freelock(&(h)->fch_lock)

extern struct hash_table fidCache;

#define FCM_CLEAN   (1 << 0)
#define FCM_DIRTY   (1 << 1)
#define FCM_FREEING (1 << 2)

/* sl_finfo - hold stats and lamport clock */
struct sl_finfo {
        struct timeval  slf_opentime;      /* when we received client OPEN  */
        struct timeval  slf_closetime;     /* when we received client CLOSE */
	struct timeval  slf_lattr_update;  /* last attribute update         */
	u64             slf_opcnt;         /* count attr updates            */
        size_t          slf_readb;         /* num bytes read                */
        size_t          slf_writeb;        /* num bytes written             */ 
};

struct sl_uid {
        //u64   sluid_guid;  /* Stubs for global uids */
        //u64   sluid_ggid;
        uid_t sluid_suid; /* site uid */
        gid_t sluid_sgid; /* site gid */
};

//struct bmap_refresh {
//	slash_fid_t bmrfr_fid;
//	sl_blkno_t  bmrfr_blk;
//	u8          bmrfr_flags;
//};

struct mexpbcm;
/*
 * bmexpcr (bmap_export_cache_reference) - maintain back references to the exports which reference this bmap via their export bmap caches.  This reference allows for simple lookup and access to the export-specific bmap tree which represents the client's bmap cache.  Usage scenarios include single block invalidations to multiple clients.
 *
 * bmexpcr are members of the bmap export tree - the lower tier in the GFC.
 */
struct bmexpcr {
	struct mexpbcm      *bmexpcr_ref;	
	SPLAY_ENTRY(bmexpcr) bmexpcr_tentry;
};

/* Tree of bmexpcr's held by bmap_mds_info. */
SPLAY_HEAD(bmap_exports, bmexpcr);
SPLAY_PROTOTYPE(bmap_exports, bmexpcr, bmexpcr_tentry, bmap_cache_cmp);

/* 
 * bmap_mds_info - associate the fcache block to its respective export bmap caches.
 */
struct bmap_mds_info {
	atomic_t            bmdsi_refcnt;  /* count our references */
	struct bmap_exports bmdsi_exports; /* point to our exports */
};

/* 
 * bmap_cli_info - for each block in the fcache, associate the set of possible i/o servers and the store the crc of the block.
 */
struct bmap_info {
	sl_blkno_t   bmapi_blkno;
	sl_ios_id_t  bmapi_ios[SL_DEF_REPLICAS];
	sl_gcrc_t    bmapi_gencrc;
};

/* 
 * bmap_info_handle - holder for bmap_info (which is a wire struct).
 */
//struct bmap_info_handle {
//	struct bmap_info      bmapih_info;
//	struct psc_list_head  bmapih_lentry;	
//};

/*
 * bmap_cache_memb - central structure for block map caching used in all slash service contexts (mds, ios, client).  
 *
 * bmap_cache_memb sits in the middle of the GFC stratum.
 */
struct bmap_cache_memb {
	struct timeval   bcm_ts;	
	struct bmap_info bcm_bmap_info;
	atomic_t         bcm_refcnt;  /* one ref per client (mds) */
	union bcm_data {
		struct bmap_cli_info *cli_info;
		struct bmap_mds_info *mds_info;
		void                 *ios_info;
	};
	struct psclist_head          bcm_lentry;  /* lru chain         */
	struct psclist_head          bcm_buffers; /* track our buffers */
	SPLAY_ENTRY(bmap_cache_memb) bcm_tentry;  /* fcm tree entry    */
};

/* Tree definition for fcm tracking of clients (via their exports) */
struct mexpfcm;
SPLAY_HEAD(bmap_lessees, mexpfcm);
SPLAY_PROTOTYPE(bmap_lessees, mexpfcm, mecm_fcm_tentry, bmap_cache_cmp);

/* Tree definition for the fcm to enable tracking of leased bmaps */
SPLAY_HEAD(bmap_cache, bmap_cache_memb);
SPLAY_PROTOTYPE(bmap_cache, bmap_cache_memb, bcm_tentry, bmap_cache_cmp);

/*
 * fid_cache_memb - holds inode filesystem related data
 */
typedef struct fid_cache_memb {
	slash_fid_t          fcm_fid;
	struct stat          fcm_stb; 
	struct sl_finfo      fcm_slfinfo;
	struct sl_uid        fcm_uid;
} fcache_memb_t;

/* fid_cache_memb_handle - the primary inode cache structure, all updates and lookups into the inode are done through here.  fid_cache_memb tracks cached bmaps (bmap_cache) and clients (via their exports) which hold cached bmaps (fcm_lessees).
 *
 */
typedef struct fid_cache_memb_handle {
	fcache_memb_t        fcmh_memb; 
	struct hash_entry    fcmh_hentry;
	struct psclist_entry fcmh_lentry;
	struct timeval       fcmh_access;
	list_cache_t        *fcmh_cache_owner;	
	int                  fcmh_fd;
	u32                  fcmh_state;
	atomic_t             fcmh_refcnt;
	void                *fcmh_objm;     /* mmap region for filemap md    */
	size_t               fcmh_objm_sz;  /* nbytes mapped for objm        */
	struct bmap_lessees  fcmh_lessees;  /* mds only, client leases array
					     * of bmap_mds_export_cache      */
	struct bmap_cache    fcmh_bmap_cache; /* splay tree of bmap cache    */
	list_cache_t         fcmh_buffer_cache; /* list of data buffers      */
	psc_spinlock_t       fcmh_lock;
} fcache_mhandle_t;

struct sl_fsops {	
	/* sl_getattr - used for stat and open, loads the objects 
	 *  via the pathname.
	 */
	int (*sl_getattr(const char *path, struct fid_cache_memb *fcm)); 
	/* sl_fgetattr - used for stat and open via fid, grabs all the 
	 * attributes of the object and updates the fcm 
	 */
	int (*sl_fgetattr(struct fid_cache_memb *fcm));	
	/* sl_setattr - used to update the objects attrs, for now
	 * all attrs will be updated.  later we may refine the granularity
	 */
	int (*sl_setattr(struct fid_cache_memb *fcm));
	/* sl_write - vectorized object write.  Either within mds or ios 
	 *  context.
	 */
	int (*sl_write(struct fid_cache_memb *fcm, 
		       const struct iovec *vector, int count));
	/* sl_read - vectorized object read.  Either within mds or ios 
	 *  context.
	 */
	int (*sl_read(struct fid_cache_memb *fcm, 
 	              const struct iovec *vector, int count));
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*sl_getmap(struct fid_cache_memb  *fcm,
			struct bmap_cache_memb *bcms, int count));

	int (*sl_invmap(struct fid_cache_memb *fcm, struct bmap_refresh *bmr));
};

struct fid_cache {	
	struct hash_table fc_htable;	
	list_cache_t      fc_free;
	list_cache_t      fc_clean;
	list_cache_t      fc_dirty;
	struct sl_fsops  *fc_fsops;
};


#define FCMH_FLAG(field, str) (field ? str : "")
#define DEBUG_FCMH_FCMH_FLAGS(fcmh)					\
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_CLEAN), "C"),		\
		FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_DIRTY), "D"),	\
		FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCM_FREEING), "F")

#define REQ_FCMH_FLAGS_FMT "%s%s%s"

static inline const char *
fcmh_lc_2_string(list_cache_t *lc)
{
        switch (lc) {
	case (NULL):
		return "Null";
	case (fidcCleanList):
		return "Clean";
        case (fidcFreeList):
                return "Free";
        case (fidcDirtyList):
                return "Dirty";
        default:
                return "?Unknown?";
        }
}

#define DEBUG_FCMH(level, fcmh, fmt, ...)				\
	do {								\
		_psclog(__FILE__, __func__, __LINE__,			\
			PSS_OTHER, level, 0,				\
			" fcmh@%p inum+gen:"LPX64"+"LPX64" state:"	\
			REQ_FCMH_FLAGS_FMT" cowner:%s fd:%d :: "fmt,	\
			fcmh, fcmh->fcmh_memb.fcm_fid.fid_inum,		\
			fcmh->fcmh_memb.fcm_fid.fid_gen,		\
			DEBUG_FCMH_FCMH_FLAGS(fcmh), fcmh->fcmh_fd,	\
			atomic_read(&fcmh->fcmh_refcnt),		\
			## __VA_ARGS__);				\
	} while(0)


#endif
