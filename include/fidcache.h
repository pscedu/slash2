/* $Id$ */

#ifndef _FIDCACHE_H_
#define _FIDCACHE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include "psc_types.h"
#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/lockedlist.h"
#include "psc_ds/pool.h"
#include "psc_ds/tree.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"

#include "cache_params.h"
#include "slconfig.h"
#include "buffer.h"
#include "jflush.h"
#include "fid.h"
#include "inode.h"

struct bmap_refresh;
struct fidc_membh;
struct sl_fsops;

struct sl_uid {
	u64			 sluid_guid;	/* Stubs for global uids */
	u64			 sluid_ggid;
	uid_t			 sluid_suid;	/* site uid */
	gid_t			 sluid_sgid;	/* site gid */
};

struct sl_finfo {
	u64			 slf_opcnt;     /* count attr updates       */
	size_t			 slf_readb;	/* num bytes read           */
	size_t			 slf_writeb;	/* num bytes written        */
	double			 slf_age;
};

SPLAY_HEAD(bmap_cache, bmapc_memb);

struct fidc_open_obj {
	struct srt_fd_buf	 fcoo_fdb;
	int			 fcoo_oref_rw[2];    /* open cnt for r & w */
	atomic_t		 fcoo_bmapc_cnt;
	list_cache_t		 fcoo_buffer_cache;  /* chain our slbs   */
	struct bmap_cache	 fcoo_bmapc;         /* bmap cache splay */
	size_t			 fcoo_bmap_sz;
	void			*fcoo_pri;           /* mds, client, ion */
};

#define fidc_settimeo(age)						\
	do {								\
		(age) = fidc_gettime() + FCMH_ATTR_TIMEO;		\
	} while (0)

/*
 * fidc_memb - holds inode filesystem related data
 */
struct fidc_memb {
	struct slash_fidgen	 fcm_fg;
	struct sl_finfo		 fcm_slfinfo;
	struct stat		 fcm_stb;
};

#define fcm_2_fid(f)	(f)->fcm_fg.fg_fid
#define fcm_2_gen(f)	(f)->fcm_fg.fg_gen
#define fcm_2_fgp(f)	(&(f)->fcm_fg)
#define fcm_2_fsz(f)	(f)->fcm_stb.st_size
#define fcm_2_age(f)    (f)->fcm_slfinfo.slf_age
//#define fcm_2_inoh(f)	(&(f)->fcm_inodeh)

#define FCM_CLEAR(fcm) memset((fcm), 0, sizeof(struct fidc_memb))

#define FCM_FROM_FG_ATTR(fcm, fg, a)					\
	do {								\
		memcpy(&(fcm)->fcm_stb, (a), sizeof((fcm)->fcm_stb));	\
		memcpy(&(fcm)->fcm_fg, (fg), sizeof((fcm)->fcm_fg));	\
	} while (0)

#define fcm_set_accesstime(f)						\
	clock_gettime(CLOCK_REALTIME, &(f)->fcmh_access)

#define fcm_dump_stb(stb, level)					\
	psc_logs(level, PSS_OTHER,					\
	    "stb (%p) dev:%lu inode:%"_P_U64"d mode:0%o "		\
	    "nlink:%lu uid:%u gid:%u rdev:%lu sz:%"_P_U64"d "		\
	    "blk:%lu blkcnt:%zd atime:%lu mtime:%lu ctime:%lu", 	\
	    (stb),							\
	    (stb)->st_dev, (stb)->st_ino, (stb)->st_mode,		\
	    (stb)->st_nlink, (stb)->st_uid, (stb)->st_gid,		\
	    (stb)->st_rdev, (stb)->st_size, (stb)->st_blksize,		\
	    (stb)->st_blocks, (stb)->st_atime, (stb)->st_mtime,		\
	    (stb)->st_mtime)

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

#define fcmh_fid fcmh_fcm->fcm_fg.fg_fid
#define fcmh_gen fcmh_fcm->fcm_fg.fg_gen
#define fcmh_fg  fcmh_fcm->fcm_fg.fg_fg

#define fcmh_2_fid(f)	(f)->fcmh_fcm->fcm_fg.fg_fid
#define fcmh_2_gen(f)	(f)->fcmh_fcm->fcm_fg.fg_gen
#define fcmh_2_fgp(f)	(&(f)->fcmh_fcm->fcm_fg)
#define fcmh_2_fsz(f)	(size_t)(f)->fcmh_fcm->fcm_stb.st_size
#define fcmh_2_attrp(f)	(&(f)->fcmh_fcm->fcm_stb)

#define fcmh_2_age(f)	((f)->fcmh_fcm->fcm_slfinfo.slf_age)
#define fcmh_2_stb(f)	(&(f)->fcmh_fcm->fcm_stb)
#define fcmh_2_isdir(f) (S_ISDIR((f)->fcmh_fcm->fcm_stb.st_mode))

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

#define FCMH_ATTR_TIMEO 5

#define FCMH_LOCK(f)		spinlock(&(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock(&(f)->fcmh_lock)
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)

#define FCMH_FLAG(field, str) ((field) ? (str) : "")
#define DEBUG_FCMH_FCMH_FLAGS(fcmh)						\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_CLEAN),   "C"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_DIRTY),   "D"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREEING), "R"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREE),    "F"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_FCM),    "f"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_ISDIR),       "d"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_STARTING), "S"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_ATTACH), "a"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_CLOSING), "c"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_FAILED), "f"),	\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_ATTRS), "A"),		\
	FCMH_FLAG(ATTR_TEST((fcmh)->fcmh_state, FCMH_GETTING_ATTRS), "G")

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s%s%s%s%s%s%s"

#define DEBUG_FCMH(level, fcmh, fmt, ...)				\
do {								        \
	int dbg_fcmh_locked=reqlock(&(fcmh)->fcmh_lock);		\
	psc_logs((level), PSS_OTHER,					\
		 " fcmh@%p fcm@%p fcoo@%p fcooref(%d:%d) i+g:%"_P_U64"d+%" \
		 _P_U64"d s: "REQ_FCMH_FLAGS_FMT" pri:%p lc:%s r:%d :: "fmt,	\
		 (fcmh), (fcmh)->fcmh_fcm, (fcmh)->fcmh_fcoo,		\
		 (int)(((fcmh)->fcmh_fcoo &&				\
			(fcmh)->fcmh_fcoo != (struct fidc_open_obj *)0x01) ? \
		       (fcmh)->fcmh_fcoo->fcoo_oref_rw[0] : -66),	\
		 (int)(((fcmh)->fcmh_fcoo &&				\
			(fcmh)->fcmh_fcoo != (struct fidc_open_obj *)0x01) ? \
		       (fcmh)->fcmh_fcoo->fcoo_oref_rw[1] : -66),	\
		 (u64)(((fcmh)->fcmh_fcm) ? fcmh_2_fid((fcmh)) : FID_ANY), \
		 (u64)(((fcmh)->fcmh_fcm) ? fcmh_2_gen((fcmh)) : FID_ANY), \
		 DEBUG_FCMH_FCMH_FLAGS(fcmh), (fcmh)->fcmh_pri,		\
		 fcmh_lc_2_string((fcmh)->fcmh_cache_owner),		\
		 atomic_read(&(fcmh)->fcmh_refcnt),			\
		 ## __VA_ARGS__);					\
	ureqlock(&(fcmh)->fcmh_lock, dbg_fcmh_locked);			\
} while (0)

#define FCMHCACHE_PUT(fcmh, list)					\
	do {								\
		(fcmh)->fcmh_cache_owner = (list);			\
		if (list == &fidcFreePool->ppm_lc)			\
			psc_pool_return(fidcFreePool, (fcmh));		\
		else							\
			lc_put((list), &(fcmh)->fcmh_lentry);		\
	} while (0)

//RB_HEAD(fcmh_childrbtree, fidc_membh);
//RB_PROTOTYPE(fcmh_childrbtree, fidc_membh, fcmh_children, bmapc_cmp);

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

void fidc_fcoo_init(struct fidc_open_obj *f);
void fidc_put(struct fidc_membh *f, list_cache_t *lc);

struct fidc_membh * fidc_lookup_simple(slfid_t);
struct fidc_membh * fidc_get(void);

extern struct sl_fsops *slFsops;
extern struct hash_table fidcHtable;
extern struct psc_poolmgr *fidcFreePool;
extern struct psc_listcache fidcDirtyList;
extern struct psc_listcache fidcCleanList;

static inline double
fidc_gettime(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_REALTIME, &ts);
	return (ts.tv_nsec/1000000000.0 + ts.tv_sec);
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

static inline void
dump_fcmh(struct fidc_membh *f)
{
	DEBUG_FCMH(PLL_ERROR, f, "");
}

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
	struct fidc_open_obj *o;

	lc_remove(&fidcDirtyList, (void *)h);

	spinlock(&h->fcmh_lock);
	o = h->fcmh_fcoo;
	psc_assert(h->fcmh_cache_owner == &fidcDirtyList);
	psc_assert(!(o->fcoo_oref_rw[0] || o->fcoo_oref_rw[1]));
	psc_assert(!o->fcoo_pri);

	h->fcmh_state &= ~FCMH_FCOO_ATTACH;
	h->fcmh_fcoo = NULL;
	PSCFREE(o);

	h->fcmh_state &= ~FCMH_CAC_DIRTY;
	h->fcmh_state |= FCMH_CAC_CLEAN;
	freelock(&h->fcmh_lock);

	fidc_put(h, &fidcCleanList);

	spinlock(&h->fcmh_lock);
	h->fcmh_state &= ~FCMH_FCOO_CLOSING;
	DEBUG_FCMH(PLL_DEBUG, h, "fidc_fcoo_remove");
	freelock(&h->fcmh_lock);
	psc_waitq_wakeall(&h->fcmh_waitq);
}

/**
 * fidc_fcoo_startwait_locked - if the fcoo is in 'STARTING' state, wait
 *	for it to complete.  Otherwise return 1 if ATTACHED and zero
 *	otherwise.
 * @h: the fcmh.
 * Notes:  always return locked.
 */
static inline int
fidc_fcoo_wait_locked(struct fidc_membh *h, int nostart)
{
	psc_assert(h->fcmh_fcoo || (h->fcmh_state & FCMH_FCOO_CLOSING));

	DEBUG_FCMH(PLL_DEBUG, h, "wait locked, nostart=%d", nostart);

 retry:
	if ((h->fcmh_state & FCMH_FCOO_CLOSING) || !h->fcmh_fcoo) {
		/* The fcoo exists but it's on its way out.
		 */
		psc_waitq_wait(&h->fcmh_waitq, &h->fcmh_lock);
		spinlock(&h->fcmh_lock);
		if (!h->fcmh_fcoo) {
			if (!nostart)
				fidc_fcoo_start_locked(h);
			return (1);
		} else
			goto retry;

	} else if (h->fcmh_state & FCMH_FCOO_STARTING) {
		/* Only perform one fcoo start operation.
		 */
		psc_waitq_wait(&h->fcmh_waitq, &h->fcmh_lock);
		spinlock(&h->fcmh_lock);
		goto retry;

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
	lc_remove(&fidcCleanList, h);

	spinlock(&h->fcmh_lock);
	psc_assert(h->fcmh_fcoo);
	psc_assert(h->fcmh_cache_owner == &fidcCleanList);
	psc_assert(h->fcmh_state & FCMH_FCOO_STARTING);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_ATTACH));
	h->fcmh_state &= ~(FCMH_FCOO_STARTING | FCMH_CAC_CLEAN);
	h->fcmh_state |= (FCMH_FCOO_ATTACH | FCMH_CAC_DIRTY);
	DEBUG_FCMH(PLL_INFO, h, "fidc_fcoo_startdone");
	freelock(&h->fcmh_lock);
	/* Move the inode to the dirty list so that it's not
	 *  considered for reaping.
	 */

	//fidc_put_locked(h, &fidcDirtyList);
	fidc_put(h, &fidcDirtyList);

	//freelock(&h->fcmh_lock);
	psc_waitq_wakeall(&h->fcmh_waitq);
}

static inline void
fidc_fcoo_startfailed(struct fidc_membh *h)
{
	psc_assert(h->fcmh_fcoo);

	spinlock(&h->fcmh_lock);
	psc_assert(h->fcmh_state & FCMH_FCOO_STARTING);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_ATTACH));

	h->fcmh_state &= ~FCMH_FCOO_STARTING;
	h->fcmh_state |= FCMH_FCOO_FAILED;

	DEBUG_FCMH(PLL_WARN, h, "fidc_fcoo_failed");

	PSCFREE(h->fcmh_fcoo);
	h->fcmh_fcoo = NULL;

	freelock(&h->fcmh_lock);
	psc_waitq_wakeall(&h->fcmh_waitq);
}

#endif /* _FIDCACHE_H_ */
