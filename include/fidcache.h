/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _SL_FIDCACHE_H_
#define _SL_FIDCACHE_H_

#include "psc_ds/hash2.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"
#include "psc_util/time.h"

#include "cache_params.h"
#include "slashrpc.h"

#ifndef __LP64__
#define DEMOTED_INUM_WIDTHS
#endif

struct bmap_refresh;
struct bmapc_memb;
struct fidc_membh;
struct fidc_nameinfo;
struct fidc_open_obj;

struct sl_fcmh_ops {
	int	(*sfop_getattr)(struct fidc_membh *, const struct slash_creds *);
	int	(*sfop_grow)(void);
	void	(*sfop_shrink)(void);
};

/*
 * fidc_membh - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidc_membh tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps.
 */
struct fidc_membh {
	struct slash_fidgen	 fcmh_fg;		/* identity of the file */
#ifdef DEMOTED_INUM_WIDTHS
	struct slash_fidgen	 fcmh_smallfg;		/* integer-demoted fg_fid for hashing */
#endif
	struct timeval		 fcmh_age;		/* age of this entry */
	struct srt_stat		 fcmh_sstb;
	struct fidc_open_obj	*fcmh_fcoo;
	int			 fcmh_state;
	int			 fcmh_lasterror;
	psc_spinlock_t		 fcmh_lock;
	atomic_t		 fcmh_refcnt;
	struct psc_hashent	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct psc_listcache	*fcmh_cache_owner;
	struct psc_waitq	 fcmh_waitq;
};

/* fcmh_flags */
#define	FCMH_CAC_CLEAN		0x0001		/* (1 << 0) in clean cache */
#define	FCMH_CAC_DIRTY		0x0002		/* (1 << 1) in dirty cache */
#define	FCMH_CAC_FREEING	0x0004		/* (1 << 2) on clean cache */
#define	FCMH_CAC_FREE		0x0008		/* (1 << 3) in free pool */
#define	FCMH_ISDIR		0x0010		/* (1 << 4) is a directory */
#define	FCMH_FCOO_STARTING	0x0020		/* (1 << 5) open obj is initializing */
#define	FCMH_FCOO_STARTWAIT	0x0040		/* (1 << 6) someone is initializing */
#define	FCMH_FCOO_ATTACH	0x0080		/* (1 << 7) open obj present */
#define	FCMH_FCOO_CLOSING	0x0100		/* (1 << 8) open obj going away */
#define	FCMH_FCOO_FAILED	0x0200		/* (1 << 9) open obj didn't load */
#define	FCMH_HAVE_ATTRS		0x0400		/* (1 << 10) has valid stat info */
#define	FCMH_GETTING_ATTRS	0x0800		/* (1 << 11) fetching stat info */
#define	_FCMH_FLGSHFT		0x1000		/* (1 << 12) */

/*
 * If fuse_ino_t, declared 'unsigned long', is 4 bytes, inums will get
 * integer demoted, so we must store two: the original inum, used when
 * communicating information about the actual fcmh, as well as the
 * demoted value, used in hash table lookups from FUSE syscall handlers.
 */
#ifdef DEMOTED_INUM_WIDTHS
# define FCMH_HASH_FIELD	fcmh_smallfg
#else
# define FCMH_HASH_FIELD	fcmh_fg
#endif

#define FCMH_ATTR_TIMEO		8 /* number of seconds in which attribute times out */

#define FCMH_LOCK(f)		spinlock(&(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock(&(f)->fcmh_lock)
#define FCMH_RLOCK(f)		reqlock(&(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock(&(f)->fcmh_lock, (lk))
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)

#define fcmh_2_fid(f)		(f)->fcmh_fg.fg_fid
#define fcmh_2_gen(f)		(f)->fcmh_fg.fg_gen
#define fcmh_2_fsz(f)		(f)->fcmh_sstb.sst_size
#define fcmh_2_nbmaps(f)	((sl_bmapno_t)howmany(fcmh_2_fsz(f), SLASH_BMAP_SIZE))
#define fcmh_2_ptruncgen(f)	(f)->fcmh_sstb.sst_ptruncgen

#define fcmh_isdir(f)		S_ISDIR((f)->fcmh_sstb.sst_mode)

#define DEBUG_FCMH_FLAGS(fcmh)							\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_CLEAN)		? "C" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_DIRTY)		? "D" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREEING)		? "R" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREE)		? "F" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_ISDIR)		? "d" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_STARTING)	? "S" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_ATTACH)		? "a" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_CLOSING)	? "c" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_FCOO_FAILED)		? "f" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_ATTRS)		? "A" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_GETTING_ATTRS)	? "G" : ""

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s%s%s%s%s%s"

#define DEBUG_FCMH(level, fcmh, fmt, ...)					\
	do {									\
		int _dbg_fcmh_locked = reqlock(&(fcmh)->fcmh_lock);		\
										\
		psc_logs((level), PSS_GEN,					\
		    "fcmh@%p fcoo@%p fcooref(%d:%d) i+g:%"PRId64"+"		\
		    "%"PRId64" s:"REQ_FCMH_FLAGS_FMT" pri:%p lc:%s "		\
		    "ref:%d :: "fmt,						\
		    (fcmh), (fcmh)->fcmh_fcoo,					\
		    (fcmh)->fcmh_fcoo == NULL ||				\
		    (fcmh)->fcmh_fcoo == FCOO_STARTING ? -66 :			\
		    (fcmh)->fcmh_fcoo->fcoo_oref_rd,				\
		    (fcmh)->fcmh_fcoo == NULL ||				\
		    (fcmh)->fcmh_fcoo == FCOO_STARTING ? -66 :			\
		    (fcmh)->fcmh_fcoo->fcoo_oref_wr,				\
		    fcmh_2_fid(fcmh),						\
		    fcmh_2_gen(fcmh),						\
		    DEBUG_FCMH_FLAGS(fcmh), (fcmh)->fcmh_name,			\
		    fcmh_lc_2_string((fcmh)->fcmh_cache_owner),			\
		    atomic_read(&(fcmh)->fcmh_refcnt),				\
		    ## __VA_ARGS__);						\
		ureqlock(&(fcmh)->fcmh_lock, _dbg_fcmh_locked);			\
	} while (0)

#define FCMHCACHE_PUT(fcmh, list)						\
	do {									\
		(fcmh)->fcmh_cache_owner = (list);				\
		if ((list) == &fidcPool->ppm_lc)				\
			psc_pool_return(fidcPool, (fcmh));			\
		else								\
			lc_add((list), (fcmh));					\
	} while (0)

/* Increment an fcmh reference, fcmh_refcnt is used by the fidcache
 *  to determine which fcmh's may be reclaimed.
 */
#define fcmh_incref(f)								\
	do {									\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);		\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));			\
		atomic_inc(&(f)->fcmh_refcnt);					\
		DEBUG_FCMH(PLL_TRACE, (f), "incref");				\
	} while (0)

/* Drop an fcmh reference.
 */
#define fcmh_dropref(f)								\
	do {									\
		atomic_dec(&(f)->fcmh_refcnt);					\
		psc_assert(!((f)->fcmh_state & FCMH_CAC_FREE));			\
		psc_assert(atomic_read(&(f)->fcmh_refcnt) >= 0);		\
		DEBUG_FCMH(PLL_TRACE, (f), "dropref");				\
	} while (0)

static __inline void *
fcmh_get_pri(struct fidc_membh *fcmh)
{
	return (fcmh + 1);
}

SPLAY_HEAD(bmap_cache, bmapc_memb);

struct fidc_open_obj {
	struct srt_fd_buf	 fcoo_fdb;
	int			 fcoo_oref_rd;
	int			 fcoo_oref_wr;
	struct bmap_cache	 fcoo_bmapc;		/* bmap cache splay */
	size_t			 fcoo_bmap_sz;
};

#define FCOO_STARTING		((struct fidc_open_obj *)0x01)

static __inline void *
fcoo_get_pri(struct fidc_open_obj *fcoo)
{
	return (fcoo + 1);
}

/* fidc_lookup() flags */
enum {
	FIDC_LOOKUP_CREATE	= (1 << 0),		/* Create if not present         */
	FIDC_LOOKUP_EXCL	= (1 << 1),		/* Fail if fcmh is present       */
	FIDC_LOOKUP_COPY	= (1 << 2),		/* Create from existing attrs    */
	FIDC_LOOKUP_LOAD	= (1 << 3),		/* Use external fetching mechanism */
	FIDC_LOOKUP_FCOOSTART	= (1 << 4)		/* Start fcoo before exposing fcmh */
};

#define FIDC_LOOKUP_REFRESH	FIDC_LOOKUP_LOAD	/* load and refresh are the same */

/* fcmh_setattr() flags */
#define FCMH_SETATTRF_NONE	0
#define FCMH_SETATTRF_SAVESIZE	(1 << 0)

#define fidc_lookup_fg(fg)	_fidc_lookup_fg((fg), 0)

void			 fcmh_dtor(void *);
struct fidc_membh	*fcmh_get(void);
int			 fcmh_getfdbuf(struct fidc_membh *, struct srt_fd_buf *);
void			 fcmh_setfdbuf(struct fidc_membh *, const struct srt_fd_buf *);
int			 fcmh_init(struct psc_poolmgr *, void *);
void			 fcmh_setattr(struct fidc_membh *, const struct srt_stat *, int);

struct fidc_open_obj	*fidc_fcoo_init(void);

void			 fidc_put(struct fidc_membh *, struct psc_listcache *);
void			 fidc_init(int, int, int, int (*)(struct fidc_membh *));
struct fidc_membh	*fidc_lookup_simple(slfid_t);
struct fidc_membh	*_fidc_lookup_fg(const struct slash_fidgen *, int);

int			 fidc_lookup(const struct slash_fidgen *, int,
			    const struct srt_stat *, int, const struct slash_creds *,
			    struct fidc_membh **);

void			 dump_fcmh(struct fidc_membh *);
void			 dump_fcmh_flags(int);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_listcache	 fidcDirtyList;
extern struct psc_listcache	 fidcCleanList;
extern int			 fcoo_priv_size;

static __inline void
fcmh_refresh_age(struct fidc_membh *fcmh)
{
	struct timeval tmp = { FCMH_ATTR_TIMEO, 0 };

	PFL_GETTIME(&fcmh->fcmh_age);
	timeradd(&fcmh->fcmh_age, &tmp, &fcmh->fcmh_age);
}

static __inline const char *
fcmh_lc_2_string(struct psc_listcache *lc)
{
	if (lc == &fidcCleanList)
		return "Clean";
	if (lc == &fidcPool->ppm_lc)
		return "Free";
	if (lc == &fidcDirtyList)
		return "Dirty";
	if (lc == NULL)
		return "Null";
	psc_fatalx("invalid fidcache list cache %p", lc);
}

/**
 * fidc_lookup_load_inode - Create the inode if it doesn't exist loading
 *	its attributes from the network.
 */
static __inline int
fidc_lookup_load_inode(slfid_t fid, const struct slash_creds *cr,
    struct fidc_membh **fp)
{
	struct slash_fidgen fg = { fid, FIDGEN_ANY };

	return (fidc_lookup(&fg, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD,
	    NULL, FCMH_SETATTRF_NONE, cr, fp));
}

/**
 * fcmh_clean_check - Verify the validity of a fid cache member.
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

/*
 * Used to open a file after fidc_membh has been locked down.  When done,
 * the caller should call fidc_fcoo_startdone() or fidc_fcoo_startfailed()
 * to wake up any waiters.
 */
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
	psc_waitq_wakeall(&h->fcmh_waitq);
	freelock(&h->fcmh_lock);
}

/* fcoo wait flags */
#define FCOO_START   0
#define FCOO_NOSTART 1

/**
 * fidc_fcoo_wait_locked - Wait for an open object to becomd ready.
 * @h: the fcmh.
 * @nostart: whether to kick start it if there isn't an open object.
 * Returns:
 *	-1	open object couldn't load.
 *	 0	open object is available.
 *	 1	open object was freshly initialized.
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

	DEBUG_FCMH(PLL_FATAL, h, "invalid fcmh_state (%d)",
	    h->fcmh_state);
	abort();
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

	psc_waitq_wakeall(&h->fcmh_waitq);
	/* XXX we should hold be holding the lock while we wake */
//	freelock(&h->fcmh_lock);
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
