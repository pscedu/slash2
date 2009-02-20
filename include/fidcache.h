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

#define FCMH_ATTR_TIMEO 5

#define FCMHCACHE_PUT(fcmh, list)					\
        do {                                                            \
		(fcmh)->fcmh_cache_owner = (list);			\
		if (list == &fidcFreePool->ppm_lc)			\
			psc_pool_return(fidcFreePool, (fcmh));	\
		else							\
        	        lc_put((list), &(fcmh)->fcmh_lentry);		\
        } while (0)


static inline double 
fidc_gettime(void) { 
	struct timespec ts;

	clock_gettime(CLOCK_REALTIME, &ts);
	return (ts.tv_nsec/1000000000.0 + ts.tv_sec);
}

extern struct psc_poolmgr *fidcFreePool;
extern struct psc_listcache fidcDirtyList;
extern struct psc_listcache fidcCleanList;

struct sl_uid {
	u64	sluid_guid;	/* Stubs for global uids */
	u64	sluid_ggid;
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
	BMAP_MDS_EMPTY  = (1<<4), /* new bmap, not yet committed to disk*/
	BMAP_MDS_CRC_UP = (1<<5), /* crc update in progress */
	BMAP_MDS_INIT   = (1<<6)
};

/*
 * bmapc_memb - central structure for block map caching used in
 *    all slash service contexts (mds, ios, client).
 *
 * bmapc_memb sits in the middle of the GFC stratum.
 * XXX some of these elements may need to be moved into the bcm_info_pri
 *     area (as part of new structures?) so save space on the mds.
 */
struct fidc_membh;

struct bmapc_memb {
	sl_blkno_t	             bcm_blkno;   /* Bmap blk number        */
	struct fidc_membh           *bcm_fcmh;    /* pointer to fid info    */
	struct bmap_info	     bcm_bmapih;
	atomic_t		     bcm_rd_ref;  /* one ref per write fd    */
	atomic_t		     bcm_wr_ref;  /* one ref per read fd     */
	struct timespec		     bcm_ts;
	atomic_t                     bcm_opcnt;   /* pending opcnt           */
	u64                          bcm_holes[2];/* one bit SLASH_BMAP_SIZE */
	union {
		void		    *bmt_mds_pri;
		struct offtree_root *bmt_cli_oftr;
	} bmap_type;
	psc_spinlock_t               bcm_lock;
	psc_waitq_t                  bcm_waitq;
	struct jflush_item           bcm_jfi;
	SPLAY_ENTRY(bmapc_memb) bcm_tentry;       /* fcm tree entry    */
#define bcm_mds_pri bmap_type.bmt_mds_pri
#define bcm_oftr    bmap_type.bmt_cli_oftr				
#define bcm_dirty   bcm_holes                     /* change context for ION */
};

#define bmap_set_accesstime(b) {				    \
		clock_gettime(CLOCK_REALTIME, &(b)->bcm_ts);	    \
	}


int
bmapc_cmp(const void *, const void *);

SPLAY_HEAD(bmap_cache, bmapc_memb);
SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

struct sl_finfo {
	u64		slf_opcnt;	   /* count attr updates            */
	size_t		slf_readb;	   /* num bytes read                */
	size_t		slf_writeb;	   /* num bytes written             */
	double          slf_age;
};

struct fidc_open_obj {
	u64                      fcoo_cfd;
	int                      fcoo_oref_rw[2];    /* open cnt for r & w */ 
	atomic_t                 fcoo_bmapc_cnt;
	list_cache_t		 fcoo_buffer_cache;  /* chain our slbs   */
	struct bmap_cache	 fcoo_bmapc;         /* bmap cache splay */
	size_t                   fcoo_bmap_sz;
	struct jflush_item       fcoo_jfi;
	void			*fcoo_pri;           /* mds, client, ion */
};

/*
 * fidc_memb - holds inode filesystem related data
 */
struct fidc_memb {
	struct slash_fidgen      fcm_fg;
	struct sl_finfo		 fcm_slfinfo;
	struct stat	         fcm_stb;
};

#define FCM_CLEAR(fcm) memset((fcm), 0, sizeof(struct fidc_memb))

#define FCM_FROM_FG_ATTR(fcm, fg, a)					\
	do {								\
		memcpy(&(fcm)->fcm_stb, (a), sizeof((fcm)->fcm_stb));	\
		memcpy(&(fcm)->fcm_fg, (fg), sizeof((fcm)->fcm_fg));	\
	} while (0)


struct fidc_membh;


struct sl_fsops;
/*
 * fidc_membh - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidc_memb tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps (fcm_lessees).
 */
struct fidc_membh {
	struct fidc_memb        *fcmh_fcm;
	struct fidc_open_obj    *fcmh_fcoo;
	int                      fcmh_state;
	psc_spinlock_t		 fcmh_lock;
	atomic_t		 fcmh_refcnt;
	struct hash_entry        fcmh_hashe;
	struct psclist_head	 fcmh_lentry;
	list_cache_t		*fcmh_cache_owner;
	psc_waitq_t              fcmh_waitq;
	struct sl_fsops         *fcmh_fsops;
	void                    *fcmh_pri;
	union {
		struct psclist_head children;
	} fcmh_data;
#define fcmh_children fcmh_data.children
};

enum fcmh_states {
	FCMH_CAC_CLEAN     = (1 << 0),
	FCMH_CAC_DIRTY     = (1 << 1),
	FCMH_CAC_FREEING   = (1 << 2),
	FCMH_CAC_FREE      = (1 << 3),
	FCMH_HAVE_FCM      = (1 << 4),
	FCMH_ISDIR         = (1 << 5),
	FCMH_FCOO_STARTING = (1 << 6),
	FCMH_FCOO_ATTACH   = (1 << 7),
	FCMH_FCOO_CLOSING  = (1 << 8),
	FCMH_FCOO_FAILED   = (1 << 9),
	FCMH_HAVE_ATTRS    = (1 << 10),
	FCMH_GETTING_ATTRS = (1 << 11)
};

#define fcmh_2_fid(f)	(f)->fcmh_fcm->fcm_fg.fg_fid
#define fcmh_2_gen(f)	(f)->fcmh_fcm->fcm_fg.fg_gen
#define fcmh_2_fgp(f)	(&(f)->fcmh_fcm->fcm_fg)
#define fcmh_2_fsz(f)	(size_t)(f)->fcmh_fcm->fcm_stb.st_size
#define fcmh_2_attrp(f)	(&(f)->fcmh_fcm->fcm_stb)

#define fcmh_2_age(f)	((f)->fcmh_fcm->fcm_slfinfo.slf_age)
#define fcmh_2_stb(f)	(&(f)->fcmh_fcm->fcm_stb)
#define fcmh_2_isdir(f) (S_ISDIR((f)->fcmh_fcm->fcm_stb.st_mode))

#define fcm_2_fid(f)	(f)->fcm_fg.fg_fid
#define fcm_2_gen(f)	(f)->fcm_fg.fg_gen
#define fcm_2_fgp(f)	(&(f)->fcm_fg)
#define fcm_2_fsz(f)	(f)->fcm_stb.st_size
//#define fcm_2_inoh(f)	(&(f)->fcm_inodeh)

#define fcm_set_accesstime(f) {					    \
		clock_gettime(CLOCK_REALTIME, &(f)->fcmh_access);   \
	}

static inline void
fcm_dump_stb_(const struct stat *stb, int level)
{
	psc_logs(level, PSS_OTHER, 
		"stb (%p) dev:%lu inode:%"_P_U64"d mode:0%o nlink:%lu "
		"uid:%u gid:%u rdev:%lu sz:%"_P_U64"d "
		"blk:%lu blkcnt:%zd atime:%lu mtime:%lu ctime:%lu", 
		stb,
		stb->st_dev, stb->st_ino, stb->st_mode, 
		stb->st_nlink, stb->st_uid, stb->st_gid, 
		stb->st_rdev, stb->st_size, stb->st_blksize, 
		stb->st_blocks, stb->st_atime, stb->st_mtime, 
		stb->st_mtime);	
}

#define fcm_dump_stb(stb, level)				\
{								\
	psc_logs(level, PSS_OTHER,				\
		 "stb (%p) dev:%lu inode:%"_P_U64"d mode:0%o nlink:%lu " \
		 "uid:%u gid:%u rdev:%lu sz:%"_P_U64"d "	\
		 "blk:%lu blkcnt:%zd atime:%lu mtime:%lu ctime:%lu", \
		 (stb),						\
		 (stb)->st_dev, (stb)->st_ino, (stb)->st_mode,	\
		 (stb)->st_nlink, (stb)->st_uid, (stb)->st_gid,	\
		 (stb)->st_rdev, (stb)->st_size, (stb)->st_blksize, \
		 (stb)->st_blocks, (stb)->st_atime, (stb)->st_mtime, \
		 (stb)->st_mtime);				\
}

#define FCMH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_FCMH_FCMH_FLAGS(fcmh)					\
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_CAC_CLEAN),   "C"),	\
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_CAC_DIRTY),   "D"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_CAC_FREEING), "R"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_CAC_FREE),    "F"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_HAVE_FCM),    "f"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_ISDIR),       "d"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_FCOO_STARTING), "S"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_FCOO_ATTACH), "a"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_FCOO_CLOSING), "c"),	\
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_FCOO_FAILED), "f"),	\
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_HAVE_ATTRS), "A"), \
	FCMH_FLAG(ATTR_TEST(fcmh->fcmh_state, FCMH_GETTING_ATTRS), "G")

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s%s%s%s%s%s%s"

static inline const char *
fcmh_lc_2_string(struct psc_listcache *lc)
{
	if (lc == &fidcCleanList)
		return "Clean";
	else if (lc == &fidcFreePool->ppm_lc)
		return "Free";
	else if (lc == &fidcDirtyList)
		return "Dirty";
	else if (lc == NULL)
		return "Null";
	psc_fatalx("bad fidc list cache %p", lc);
}

#define DEBUG_FCMH(level, fcmh, fmt, ...)				\
do {								        \
	int dbg_fcmh_locked=reqlock(&(fcmh)->fcmh_lock);		\
	psc_logs((level), PSS_OTHER,					\
		 " fcmh@%p fcm@%p fcoo@%p fcooref(%d:%d) i+g:%"_P_U64"d+%" \
		 _P_U64"d s: "REQ_FCMH_FLAGS_FMT" lc:%s r:%d :: "fmt,	\
		 (fcmh), (fcmh)->fcmh_fcm, (fcmh)->fcmh_fcoo,		\
		 (int)(((fcmh)->fcmh_fcoo &&				\
			(fcmh)->fcmh_fcoo != (struct fidc_open_obj *)0x01) ? \
		       (fcmh)->fcmh_fcoo->fcoo_oref_rw[0] : -66),	\
		 (int)(((fcmh)->fcmh_fcoo &&				\
			(fcmh)->fcmh_fcoo != (struct fidc_open_obj *)0x01) ? \
		       (fcmh)->fcmh_fcoo->fcoo_oref_rw[1] : -66),	\
		 (u64)(((fcmh)->fcmh_fcm) ? fcmh_2_fid((fcmh)) : FID_ANY), \
		 (u64)(((fcmh)->fcmh_fcm) ? fcmh_2_gen((fcmh)) : FID_ANY), \
		 DEBUG_FCMH_FCMH_FLAGS(fcmh),				\
		 fcmh_lc_2_string((fcmh)->fcmh_cache_owner),		\
		 atomic_read(&(fcmh)->fcmh_refcnt),			\
		 ## __VA_ARGS__);					\
	ureqlock(&(fcmh)->fcmh_lock, dbg_fcmh_locked);			\
} while (0)

void
fidc_fcoo_init(struct fidc_open_obj *f);

void
fidc_put_locked(struct fidc_membh *f, list_cache_t *lc);

static inline void
fidc_fcoo_check_locked(struct fidc_membh *h)
{
	struct fidc_open_obj *o = h->fcmh_fcoo;

	DEBUG_FCMH(PLL_DEBUG, h, "check locked");

	psc_assert(o);	
	psc_assert(h->fcmh_state & FCMH_FCOO_ATTACH);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_CLOSING));
	psc_assert(o->fcoo_oref_rw[0] || o->fcoo_oref_rw[1]);
}



static inline void
fidc_fcoo_start_locked(struct fidc_membh *h) 
{
	psc_assert(!h->fcmh_fcoo);

	h->fcmh_state |= FCMH_FCOO_STARTING;

	if (h->fcmh_state & FCMH_FCOO_FAILED) {
		DEBUG_FCMH(PLL_WARN, h, 
			   "trying to start a formerly failed fcmh");
		h->fcmh_state &= ~FCMH_FCOO_FAILED;
	}

	h->fcmh_fcoo = PSCALLOC(sizeof(*h->fcmh_fcoo));
	fidc_fcoo_init(h->fcmh_fcoo);

	DEBUG_FCMH(PLL_DEBUG, h, "start locked");
}

static inline void
fidc_fcoo_remove(struct fidc_membh *h)
{
	struct fidc_open_obj *o=h->fcmh_fcoo;
	int l=reqlock(&h->fcmh_lock);
	
	psc_assert(!(o->fcoo_oref_rw[0] & o->fcoo_oref_rw[1]));
	psc_assert(h->fcmh_cache_owner == &fidcDirtyList);
	psc_assert(!o->fcoo_pri);

	h->fcmh_state &= ~FCMH_FCOO_ATTACH;
	h->fcmh_fcoo = NULL;
	PSCFREE(o);

	h->fcmh_state &= ~FCMH_CAC_DIRTY;
	h->fcmh_state |= FCMH_CAC_CLEAN;
	lc_remove(&fidcDirtyList, (void *)h);

	DEBUG_FCMH(PLL_INFO, h, "fidc_fcoo_remove");
	h->fcmh_state &= ~FCMH_FCOO_CLOSING;
	fidc_put_locked(h, &fidcCleanList);

	ureqlock(&h->fcmh_lock, l);

	psc_waitq_wakeall(&h->fcmh_waitq);
}

/**
 * fidc_fcoo_startwait_locked - if the fcoo is in 'STARTING' state, wait for it to complete.  Otherwise return 1 if ATTACHED and zero otherwise.
 * @h: the fcmh.
 * Notes:  always return locked.
 */
static inline int
fidc_fcoo_wait_locked(struct fidc_membh *h, int nostart)
{
	psc_assert(h->fcmh_fcoo);

	DEBUG_FCMH(PLL_DEBUG, h, "wait locked, nostart=%d", nostart);

 retry_closing:
	if (h->fcmh_state & FCMH_FCOO_CLOSING) {
		/* The fcoo exists but it's on its way out.
		 */
		psc_waitq_wait(&h->fcmh_waitq, &h->fcmh_lock);
		spinlock(&h->fcmh_lock);
		if (!h->fcmh_fcoo) {
			if (!nostart)
				fidc_fcoo_start_locked(h);
			return (1);
		} else
			goto retry_closing;
	}
	
 retry_starting:
	if (h->fcmh_state & FCMH_FCOO_STARTING) {
		/* Only perform one fcoo start operation.
		 */
		psc_waitq_wait(&h->fcmh_waitq, &h->fcmh_lock);
		spinlock(&h->fcmh_lock);
		goto retry_starting;

	} else if (h->fcmh_state & FCMH_FCOO_ATTACH) {
		fidc_fcoo_check_locked(h);
		return (0);

	} else if (h->fcmh_state & FCMH_FCOO_FAILED)
		return (-1);
	
	else {
		DEBUG_FCMH(PLL_FATAL, h, "invalid fcmh_state (%d)", 
			   h->fcmh_state);
		abort();
	}
}

static inline void
fidc_fcoo_startdone(struct fidc_membh *h) 
{
	psc_assert(h->fcmh_fcoo);

	spinlock(&h->fcmh_lock);
	psc_assert(h->fcmh_state & FCMH_FCOO_STARTING);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_ATTACH));

	h->fcmh_state &= ~(FCMH_FCOO_STARTING | FCMH_CAC_CLEAN);
	h->fcmh_state |= (FCMH_FCOO_ATTACH | FCMH_CAC_DIRTY);
	/* Move the inode to the dirty list so that it's not
	 *  considered for reaping.
	 */	
	DEBUG_FCMH(PLL_INFO, h, "fidc_fcoo_startdone");

	lc_remove(h->fcmh_cache_owner, h);
	fidc_put_locked(h, &fidcDirtyList);

	freelock(&h->fcmh_lock);
	psc_waitq_wakeall(&h->fcmh_waitq);
}

static inline void
fidc_fcoo_startfailed(struct fidc_membh *h)
{
	psc_assert(h->fcmh_fcoo);
		
	spinlock(&h->fcmh_lock);
	psc_assert(h->fcmh_state & FCMH_FCOO_STARTING);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_ATTACH));

	h->fcmh_state &= ~(FCMH_FCOO_STARTING | FCMH_CAC_CLEAN);
	h->fcmh_state |= (FCMH_FCOO_FAILED);

	DEBUG_FCMH(PLL_WARN, h, "fidc_fcoo_failed");

	PSCFREE(h->fcmh_fcoo);
	h->fcmh_fcoo = NULL;

	freelock(&h->fcmh_lock);
	psc_waitq_wakeall(&h->fcmh_waitq);
}


struct sl_fsops {
	int (*slfsop_open)(struct fidc_membh *fcmh, 
			   const struct slash_creds *creds);

	int (*slfsop_getattr)(struct fidc_membh *fcm,
			      const struct slash_creds *creds);

	int (*slfsop_setattr)(struct fidc_membh *fcm, int toset,
			       struct slash_creds *creds);

	int (*slfsop_bmap_load)(struct fidc_membh *fcm, size_t num);     
	/* sl_write - Object write.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_write)(struct fidc_membh *fcm,
			    const void *buf, int count, off_t offset);
	/* sl_read - Object read.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_read)(struct fidc_membh *fcm,
			   const void *buf, int count, off_t offset);
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*slfsop_getmap)(struct fidc_membh  *fcm,
			     struct bmapc_memb *bcms, int count);

	int (*slfsop_invmap)(struct fidc_membh *fcm,
			     struct bmap_refresh *bmr);
};

extern struct sl_fsops *slFsops;


#define DEBUG_BMAP(level, b, fmt, ...)					\
	do {								\
		char __nidstr[PSC_NIDSTR_SIZE];				\
		psc_nid2str((b)->bcm_bmapih.bmapi_ion, __nidstr);	\
		psc_logs((level), PSS_OTHER,				\
			" bmap@%p b:%u m:%u ion=(%s) i:%"_P_U64"x"	\
			"rref=%u wref=%u opcnt=%u "fmt,			\
			(b), (b)->bcm_blkno,				\
			(b)->bcm_bmapih.bmapi_mode,			\
			(((b)->bcm_bmapih.bmapi_ion != LNET_NID_ANY) ?	\
			 __nidstr : "<?>"),				\
			((b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0), \
			atomic_read(&(b)->bcm_rd_ref),			\
			atomic_read(&(b)->bcm_wr_ref),			\
			atomic_read(&(b)->bcm_opcnt),			\
			## __VA_ARGS__);				\
	} while (0)


static inline struct bmapc_memb *
bmap_lookup_locked(struct fidc_open_obj *fcoo, sl_blkno_t n)
{
	struct bmapc_memb lb, *b;

	lb.bcm_blkno=n;
	b = SPLAY_FIND(bmap_cache, &fcoo->fcoo_bmapc, &lb);
	if (b)
		atomic_inc(&b->bcm_opcnt);

	return (b);
}

static inline struct bmapc_memb * 
bmap_lookup(struct fidc_membh *f, sl_blkno_t n)
{
	int l = reqlock(&f->fcmh_lock);
	struct bmapc_memb *b = bmap_lookup_locked(f->fcmh_fcoo, n);
	
	ureqlock(&f->fcmh_lock, l);
	return (b);
}

static inline void
bmap_op_done(struct bmapc_memb *b)
{
	atomic_dec(&b->bcm_opcnt);
}

struct fidc_membh * fidc_lookup_simple(slfid_t);
struct fidc_membh * fidc_get(list_cache_t *);

void bmapc_memb_init(struct bmapc_memb *b, struct fidc_membh *f);

#endif /* __FIDC_H__ */
