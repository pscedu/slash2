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

#define FCH_LOCK(h)  spinlock(&(h)->fch_lock)
#define FCH_ULOCK(h) freelock(&(h)->fch_lock)

#define FCH_LOCK(m)  spinlock(&(m)->fcm_lock)
#define FCH_ULOCK(m) freelock(&(m)->fcm_lock)

extern struct hash_table fidCache;

/* File info holder */
struct sl_finfo {
        struct timeval  slf_opentime;      /* when we received client OPEN  */
        struct timeval  slf_closetime;     /* when we received client CLOSE */
	struct timeval  slf_lattr_update;  /* last attribute update         */
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
 */
struct bmexpcr {
	struct mexpbcm *bmexpcr_ref;	
	SPLAY_ENTRY(bmexpcr);
};

/* Tree of bmexpcr's held by bmap_mds_info. */
SPLAY_HEAD(bmap_exports, bmap_expcp);
SPLAY_PROTOTYPE(bmap_exports, bmap_exp_memb, mecm_tentry, bmap_cache_cmp);

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
struct bmap_info_handle {
	struct bmap_info      bmapih_info;
	struct psc_list_head  bmapih_lentry;	
};

/*
 * bmap_cache_memb - central structure for block map caching used in all slash service contexts (mds, ios, client).  
 */
struct bmap_cache_memb {
	struct timeval          bcm_ts;	
	struct bmap_info_handle bcm_bmap_info;
	atomic_t                bcm_refcnt;  /* one ref per client (mds) */
	union bcm_data {
		struct bmap_cli_info *cli_info;
		struct bmap_mds_info *mds_info;
		void                 *ios_info;
	};
	struct psclist_head          bcm_lentry; /* lru chain      */
	SPLAY_ENTRY(bmap_cache_memb) bcm_tentry; /* fcm tree entry */
};

/* Tree definition for fcm tracking of clients (via their exports) */
struct mexpfcm;
SPLAY_HEAD(bmap_lessees, mexpfcm);
SPLAY_PROTOTYPE(bmap_lessees, mexpfcm, mecm_fcm_tentry, bmap_cache_cmp);

/* Tree definition for the fcm to enable tracking of leased bmaps */
SPLAY_HEAD(bmap_cache, bmap_cache_memb);
SPLAY_PROTOTYPE(bmap_cache, bmap_cache_memb, bcm_tentry, bmap_cache_cmp);

typedef struct fid_cache_memb {
	slash_fid_t          fcm_fid;
	struct stat          fcm_stb; 
	struct sl_info       fcm_slfinfo;
	struct sl_uid        fcm_uid;
	int                  fcm_fd;
	void                *fcm_objm;     /* mmap region for filemap md    */
	size_t               fcm_objm_sz;  /* nbytes mapped for objm        */
	psc_spinlock         fcm_lock;
	struct sl_fsops     *fcm_fsops;
	struct bmap_lessees  fcm_lessees; /* mds only, client leases array
					   * of bmap_mds_export_cache
					   */
	struct bmap_cache    fcm_bmap_cache;  /* splay tree of bmap cache  */
} fcache_memb_t;

/*
 * fid_cache_memb_handle - container for the fid cache member.
 */
typedef struct fid_cache_memb_handle {
	struct hash_entry    fch_hentry;
	struct psclist_entry fch_lentry;
	struct timeval       fch_access;
	psc_spinlock_t       fch_lock;       
	fcache_memb_t       *fch_memb;	
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
	 * On the client 
	 */
	int (*sl_getmap(struct fid_cache_memb *fcm,
			const struct iovec *vector, int count));

	int (*sl_invmap(struct fid_cache_memb *fcm, struct bmap_refresh *bmr));
};

struct fid_cache {	
	struct hash_table fc_htable;	
	list_cache_t      fc_free;
	list_cache_t      fc_clean;
	list_cache_t      fc_dirty;
	struct sl_fsops  *fc_fsops;
};

#endif
