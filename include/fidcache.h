/* $Id$ */

#ifndef _SL_FIDCACHE_H_
#define _SL_FIDCACHE_H_

#include "psc_ds/pool.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "psc_util/time.h"

#include "slashrpc.h"

struct bmap_refresh;
struct bmapc_memb;
struct fidc_memb;
struct fidc_membh;
struct fidc_open_obj;

struct sl_fsops {
	int (*slfsop_open)(struct fidc_membh *, const struct slash_creds *);
	int (*slfsop_getattr)(struct fidc_membh *, const struct slash_creds *);
	int (*slfsop_setattr)(struct fidc_membh *, int, struct slash_creds *);
	int (*slfsop_bmap_load)(struct fidc_membh *, size_t);
	/* sl_write - Object write.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_write)(struct fidc_membh *, const void *, int, off_t);
	/* sl_read - Object read.  Either within mds or ios
	 *  context.
	 */
	int (*slfsop_read)(struct fidc_membh *, const void *, int, off_t);
	/* sl_getmap - load the data placement map for a given file.
	 * On the client, the blk no's are determined by calculating the
	 * request offset with the block size.
	 */
	int (*slfsop_getmap)(struct fidc_membh *, struct bmapc_memb *, int);
	int (*slfsop_invmap)(struct fidc_membh *, struct bmap_refresh *);
};

/*
 * fidc_membh - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidc_memb tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps (fcm_lessees).
 */
struct fidc_membh {
	struct fidc_memb	*fcmh_fcm;
	struct fidc_open_obj	*fcmh_fcoo;
	int			 fcmh_state;
	psc_spinlock_t		 fcmh_lock;
	atomic_t		 fcmh_refcnt;
	struct hash_entry	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct psc_listcache	*fcmh_cache_owner;
	struct psc_waitq	 fcmh_waitq;
	struct sl_fsops		*fcmh_fsops;
	struct fidc_private	*fcmh_pri;
	struct psclist_head	 fcmh_children;
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

#define FCMH_ATTR_TIMEO		8 /* number of seconds in which attribute times out */

#define FCMH_LOCK(f)		spinlock(&(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock(&(f)->fcmh_lock)
#define FCMH_RLOCK(f)		reqlock(&(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock(&(f)->fcmh_lock, (lk))
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)

#define fcmh_2_fid(f)		(f)->fcmh_fcm->fcm_fg.fg_fid
#define fcmh_2_gen(f)		(f)->fcmh_fcm->fcm_fg.fg_gen
#define fcmh_2_fgp(f)		(&(f)->fcmh_fcm->fcm_fg)
#define fcmh_2_fsz(f)		(f)->fcmh_fcm->fcm_stb.st_size
#define fcmh_2_attrp(f)		(&(f)->fcmh_fcm->fcm_stb)
#define fcmh_2_nbmaps(f)	((sl_bmapno_t)howmany(fcmh_2_fsz(f), SLASH_BMAP_SIZE))

#define fcmh_2_age(f)		(&(f)->fcmh_fcm->fcm_age)
#define fcmh_2_stb(f)		(&(f)->fcmh_fcm->fcm_stb)
#define fcmh_2_isdir(f)		(S_ISDIR((f)->fcmh_fcm->fcm_stb.st_mode))

#define DEBUG_FCMH_FLAGS(fcmh)							\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_CLEAN)		? "C" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_DIRTY)		? "D" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREEING)		? "R" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREE)		? "F" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_FCM)		? "f" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_ISDIR)		? "d" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_STARTING)	? "S" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_ATTACH)		? "a" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_CLOSING)	? "c" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_FAILED)		? "f" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_ATTRS)		? "A" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_GETTING_ATTRS)	? "G" : ""

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s%s%s%s%s%s%s"

#define DEBUG_FCMH(level, fcmh, fmt, ...)					\
do {										\
	int _dbg_fcmh_locked = reqlock(&(fcmh)->fcmh_lock);			\
										\
	psc_logs((level), PSS_GEN,						\
		 " fcmh@%p fcm@%p fcoo@%p fcooref(%d:%d) i+g:%"PRId64"+"	\
		 "%"PRId64" s: "REQ_FCMH_FLAGS_FMT" pri:%p lc:%s r:%d :: "fmt,	\
		 (fcmh), (fcmh)->fcmh_fcm, (fcmh)->fcmh_fcoo,			\
		 (fcmh)->fcmh_fcoo == NULL ||					\
		    (fcmh)->fcmh_fcoo == FCOO_STARTING ? -66 :			\
		    (fcmh)->fcmh_fcoo->fcoo_oref_rd,				\
		 (fcmh)->fcmh_fcoo == NULL ||					\
		    (fcmh)->fcmh_fcoo == FCOO_STARTING ? -66 :			\
		    (fcmh)->fcmh_fcoo->fcoo_oref_wr,				\
		 (fcmh)->fcmh_fcm ? fcmh_2_fid(fcmh) : FID_ANY,			\
		 (fcmh)->fcmh_fcm ? fcmh_2_gen(fcmh) : FIDGEN_ANY,		\
		 DEBUG_FCMH_FLAGS(fcmh), (fcmh)->fcmh_pri,			\
		 fcmh_lc_2_string((fcmh)->fcmh_cache_owner),			\
		 atomic_read(&(fcmh)->fcmh_refcnt),				\
		 ## __VA_ARGS__);						\
	ureqlock(&(fcmh)->fcmh_lock, _dbg_fcmh_locked);				\
} while (0)

#define FCMHCACHE_PUT(fcmh, list)						\
	do {									\
		(fcmh)->fcmh_cache_owner = (list);				\
		if ((list) == &fidcPool->ppm_lc)				\
			psc_pool_return(fidcPool, (fcmh));			\
		else								\
			lc_put((list), &(fcmh)->fcmh_lentry);			\
	} while (0)

/* Increment an fcmh reference, fcmh_refcnt is used by the fidcache
 *  to determine which fcmh's may be reclaimed.
 */
#define fidc_membh_incref(f)							\
	do {									\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);		\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));			\
		atomic_inc(&(f)->fcmh_refcnt);					\
		DEBUG_FCMH(PLL_TRACE, (f), "incref");				\
	} while (0)

/* Drop an fcmh reference.
 */
#define fidc_membh_dropref(f)							\
	do {									\
		atomic_dec(&(f)->fcmh_refcnt);					\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));			\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);		\
		DEBUG_FCMH(PLL_TRACE, (f), "dropref");				\
	} while (0)

/*
 * fidc_memb - holds inode filesystem related data
 */
struct fidc_memb {
	struct slash_fidgen	fcm_fg;		/* identity of the file */
	struct timespec		fcm_age;	/* age of the attributes */
	struct stat		fcm_stb;	/* file attributes */
};

#define fcm_2_fid(f)	(f)->fcm_fg.fg_fid
#define fcm_2_gen(f)	(f)->fcm_fg.fg_gen
#define fcm_2_fgp(f)	(&(f)->fcm_fg)
#define fcm_2_fsz(f)	(f)->fcm_stb.st_size
#define fcm_2_age(f)	(&(f)->fcm_age)

#define FCM_CLEAR(fcm)	memset((fcm), 0, sizeof(struct fidc_memb))

#define FCM_FROM_FG_ATTR(fcm, fg, a)						\
	do {									\
		memcpy(&(fcm)->fcm_stb, (a), sizeof((fcm)->fcm_stb));		\
		memcpy(&(fcm)->fcm_fg, (fg), sizeof((fcm)->fcm_fg));		\
	} while (0)

#define fcm_set_accesstime(f)							\
	clock_gettime(CLOCK_REALTIME, &(f)->fcmh_access)

SPLAY_HEAD(bmap_cache, bmapc_memb);

struct fidc_open_obj {
	struct srt_fd_buf	 fcoo_fdb;
	int			 fcoo_oref_rw[2];	/* open cnt for r & w */
	int                      fcoo_fd;
	struct bmap_cache	 fcoo_bmapc;		/* bmap cache splay */
	size_t			 fcoo_bmap_sz;
	void			*fcoo_pri;		/* msl_fcoo_data or fidc_mds_info */
#define fcoo_oref_rd		 fcoo_oref_rw[0]
#define fcoo_oref_wr		 fcoo_oref_rw[1]
};

#define FCOO_STARTING		((struct fidc_open_obj *)0x01)

enum fidc_lookup_flags {
	FIDC_LOOKUP_CREATE    = (1 << 0), /* Create if not present         */
	FIDC_LOOKUP_EXCL      = (1 << 1), /* Fail if fcmh is present       */
	FIDC_LOOKUP_COPY      = (1 << 2), /* Create from existing attrs    */
	FIDC_LOOKUP_LOAD      = (1 << 3), /* Create, get attrs from mds    */
	FIDC_LOOKUP_REFRESH   = (1 << 3), /* load and refresh are the same */
	FIDC_LOOKUP_FCOOSTART = (1 << 4), /* start the fcoo before exposing
					   *  the cache entry.             */
	FIDC_LOOKUP_NOREF     = (1 << 5)
};

/* Perform a simple fidcache lookup, returning NULL if DNE.
 */
#define fidc_lookup_inode(fg)	fidc_lookup_simple(fg)
#define fidc_lookup_fg(fg)	_fidc_lookup_fg((fg), 0)

/* Create the inode from existing attributes.
 */
#define fidc_lookup_copy_inode(fg, fcm, creds, fcmhp)				\
	fidc_lookup((fg), FIDC_LOOKUP_CREATE | FIDC_LOOKUP_COPY |		\
	    FIDC_LOOKUP_REFRESH, (fcm), (creds), (fcmhp))

/* Create the inode from existing attributes but don't ref it.
 *  This used for preloading the inode cache.
 */
#define fidc_lookup_copy_inode_noref(fg, fcm, creds, fcmhp)			\
	fidc_lookup((fg), FIDC_LOOKUP_CREATE | FIDC_LOOKUP_COPY |		\
	    FIDC_LOOKUP_NOREF | FIDC_LOOKUP_REFRESH, (fcm), (creds),		\
	    (fcmhp))

#define fidc_lookup_load_fg(fg, creds, fcmhp)					\
	fidc_lookup((fg), FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD, NULL,		\
	    (creds), (fcmhp))

/* Create the inode from existing attributes only if one by the same id does not
 *  already exist.  Once it's created call fidc_fcoo_start_locked() so that only
 *  this thread may execute an open on the inode.
 * NOTE: This is needed for fuse create which does a create and open atomically.
 */
#define fidc_lookup_createopen_inode(f, fcm, creds, fcmhp)			\
	fidc_lookup(f, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_EXCL |			\
	    FIDC_LOOKUP_COPY | FIDC_LOOKUP_FCOOSTART, (fcm), (creds),		\
	    (fcmhp))

#define fidc_settimeo(age)							\
	do {									\
		*(age) = fidc_gettime() + FCMH_ATTR_TIMEO;			\
	} while (0)

int			 fidc_membh_init(struct psc_poolmgr *, void *);
void			 fidc_membh_setattr(struct fidc_membh *, const struct stat *);

struct fidc_open_obj	*fidc_fcoo_init(void);
void			 fidc_memb_init(struct fidc_memb *, slfid_t);

struct fidc_membh	*fidc_get(void);
void			 fidc_put(struct fidc_membh *, struct psc_listcache *);
int			 fidc_fcmh2fdb(struct fidc_membh *, struct srt_fd_buf *);
void			 fidcache_init(enum fid_cache_users, int (*)(struct fidc_membh *));

struct fidc_membh	*fidc_lookup_simple(slfid_t);
struct fidc_membh	*_fidc_lookup_fg(const struct slash_fidgen *, int);

int			 fidc_lookup(const struct slash_fidgen *, int,
			    const struct stat *, const struct slash_creds *,
			    struct fidc_membh **);

void                     fidc_fcm_size_update(struct fidc_membh *, size_t);

extern struct sl_fsops		*slFsops;
extern struct hash_table	 fidcHtable;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_listcache	 fidcDirtyList;
extern struct psc_listcache	 fidcCleanList;

#define DEBUG_STATBUF(stb, level)						\
	psc_logs((level), PSS_GEN,						\
	    "stb (%p) dev:%lu inode:%"PRId64" mode:0%o "			\
	    "nlink:%lu uid:%u gid:%u rdev:%lu sz:%"PRId64" "			\
	    "blk:%lu blkcnt:%zd atime:%lu mtime:%lu ctime:%lu",			\
	    (stb),								\
	    (stb)->st_dev, (stb)->st_ino, (stb)->st_mode,			\
	    (stb)->st_nlink, (stb)->st_uid, (stb)->st_gid,			\
	    (stb)->st_rdev, (stb)->st_size, (stb)->st_blksize,			\
	    (stb)->st_blocks, (stb)->st_atime, (stb)->st_mtime,			\
	    (stb)->st_mtime)

static __inline void
dump_statbuf(struct stat *stb, int level)
{
	DEBUG_STATBUF(stb, level);
}

//RB_PROTOTYPE(fcmh_childrbtree, fidc_membh, fcmh_children, bmapc_cmp);

extern void (*initFcooCb)(struct fidc_open_obj *);

static __inline void
fidc_gettime(struct timespec *ts)
{
	struct timespec tmp = { FCMH_ATTR_TIMEO, 0 };

	clock_gettime(CLOCK_REALTIME, ts);
	timespecadd(ts, &tmp, ts);
}

static __inline const char *
fcmh_lc_2_string(struct psc_listcache *lc)
{
	if (lc == &fidcCleanList)
		return "Clean";
	else if (lc == &fidcPool->ppm_lc)
		return "Free";
	else if (lc == &fidcDirtyList)
		return "Dirty";
	else if (lc == NULL)
		return "Null";
	psc_fatalx("bad fidc list cache %p", lc);
}

static __inline void
dump_fcmh(struct fidc_membh *f)
{
	DEBUG_FCMH(PLL_ERROR, f, "");
}

/* Create the inode if it doesn't exist loading its attributes from the network.
 */
static __inline int
fidc_lookup_load_inode(slfid_t fid, const struct slash_creds *cr,
    struct fidc_membh **fp)
{
	struct slash_fidgen fg = { fid, FIDGEN_ANY };

	return (fidc_lookup(&fg, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD,
	    NULL, cr, fp));
}

/**
 * fcmh_clean_check - verify the validity of the fcmh.
 */
static __inline int
fcmh_clean_check(struct fidc_membh *f)
{
	int locked, clean = 0;

	locked = reqlock(&f->fcmh_lock);
	DEBUG_FCMH(PLL_INFO, f, "clean_check");
	if (f->fcmh_state & FCMH_CAC_CLEAN) {
		if (f->fcmh_fcoo) {
			psc_assert(f->fcmh_state & FCMH_FCOO_STARTING);
			psc_assert(atomic_read(&f->fcmh_refcnt) > 0);
		}
		psc_assert(!(f->fcmh_state &
			     (FCMH_CAC_DIRTY | FCMH_CAC_FREE | FCMH_FCOO_ATTACH)));
		clean = 1;
	}
	ureqlock(&f->fcmh_lock, locked);
	return (clean);
}

static __inline void
fidc_fcoo_check_locked(struct fidc_membh *h)
{
	struct fidc_open_obj *o = h->fcmh_fcoo;

	DEBUG_FCMH(PLL_DEBUG, h, "check locked");

	psc_assert(o);
	psc_assert(h->fcmh_state & FCMH_FCOO_ATTACH);
	psc_assert(!(h->fcmh_state & FCMH_FCOO_CLOSING));
	psc_assert(o->fcoo_oref_rd || o->fcoo_oref_wr);
}

static __inline void
fidc_fcoo_start_locked(struct fidc_membh *h)
{
	psc_assert(!h->fcmh_fcoo);

	h->fcmh_state |= FCMH_FCOO_STARTING;

	if (h->fcmh_state & FCMH_FCOO_FAILED) {
		DEBUG_FCMH(PLL_WARN, h,
			   "trying to start a formerly failed fcmh");
		h->fcmh_state &= ~FCMH_FCOO_FAILED;
	}
	h->fcmh_fcoo = fidc_fcoo_init();
	DEBUG_FCMH(PLL_DEBUG, h, "start locked");
}

static __inline void
fidc_fcoo_remove(struct fidc_membh *h)
{
	struct fidc_open_obj *o;

	lc_remove(&fidcDirtyList, h);

	spinlock(&h->fcmh_lock);
	o = h->fcmh_fcoo;
	psc_assert(h->fcmh_cache_owner == &fidcDirtyList);
	psc_assert(!(o->fcoo_oref_rd || o->fcoo_oref_wr));
	psc_assert(!o->fcoo_pri);
	psc_assert(SPLAY_EMPTY(&o->fcoo_bmapc));

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

/* fcoo wait flags */
#define FCOO_START   0
#define FCOO_NOSTART 1

/**
 * fidc_fcoo_wait_locked - if the fcoo is in 'STARTING' state, wait
 *	for it to complete.  Otherwise return 1 if ATTACHED and zero
 *	otherwise.
 * @h: the fcmh.
 * Notes:  always return locked.
 */
static __inline int
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

static __inline void
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

static __inline void
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

#endif /* _SL_FIDCACHE_H_ */
