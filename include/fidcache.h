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

/* XXX move to bmap.h */
SPLAY_HEAD(bmap_cache, bmapc_memb);

struct sl_fcmh_ops {
	int	(*sfop_ctor)(struct fidc_membh *);
	void	(*sfop_dtor)(struct fidc_membh *);
	int	(*sfop_getattr)(struct fidc_membh *);
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
	int			 fcmh_state;
	psc_spinlock_t		 fcmh_lock;
	int			 fcmh_refcnt;
	struct psc_hashent	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct psc_listcache	*fcmh_cache_owner;
	struct psc_waitq	 fcmh_waitq;
	struct bmap_cache	 fcmh_bmaptree;		/* bmap cache splay */
};

/* fcmh_flags */

#define	FCMH_CAC_CLEAN		0x0001		/* (1 << 0) in clean cache */
#define	FCMH_CAC_DIRTY		0x0002		/* (1 << 1) in dirty cache, "dirty" means not reapable */

#define	FCMH_CAC_FREEING	0x0004		/* (1 << 2) this item is being freed */
#define	FCMH_CAC_INITING	0x0008		/* (1 << 4) this item is being initialized */
#define	FCMH_CAC_WAITING	0x0010		/* (1 << 8) this item is being waited on */

#define	FCMH_CAC_FREE		0x0010		/* (1 << 9) in free pool */
#define	FCMH_HAVE_ATTRS		0x0020		/* (1 << 10) has valid stat info */
#define	FCMH_GETTING_ATTRS	0x0040		/* (1 << 11) fetching stat info */
#define	FCMH_WAITING_ATTRS	0x0080		/* (1 << 12) someone is waiting */

#define	_FCMH_FLGSHFT		0x0100		/* (1 << 13) */

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
	ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_ATTRS)		? "A" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_GETTING_ATTRS)	? "G" : ""

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s"

#define FIDFMT			"%"PRId64":%"PRId64
#define FIDFMTARGS(fg)		(fg)->fg_fid, (fg)->fg_gen

#define DEBUG_FCMH(level, fcmh, fmt, ...)					\
	do {									\
		int _dbg_fcmh_locked = reqlock(&(fcmh)->fcmh_lock);		\
										\
		psc_logs((level), PSS_GEN,					\
		    "fcmh@%p fg:"FIDFMT" s:"REQ_FCMH_FLAGS_FMT" "		\
		    "lc:%s ref:%d :: "fmt,					\
		    (fcmh), FIDFMTARGS(&fcmh->fcmh_fg), DEBUG_FCMH_FLAGS(fcmh),	\
		    fcmh_lc_2_string((fcmh)->fcmh_cache_owner),			\
		    (fcmh)->fcmh_refcnt,					\
		    ## __VA_ARGS__);						\
		ureqlock(&(fcmh)->fcmh_lock, _dbg_fcmh_locked);			\
	} while (0)

#define FCMHCACHE_PUT(fcmh, list)						\
	do {									\
		if ((list) == &fidcFreeList)					\
			fcmh_destroy(fcmh);					\
		else {								\
			psc_assert((fcmh)->fcmh_cache_owner == NULL);		\
			(fcmh)->fcmh_cache_owner = (list);			\
			lc_add((list), (fcmh));					\
		}								\
	} while (0)


/* debugging aid: spit out the reason for the reference count taking/dropping */
enum fcmh_opcnt_types {
	FCMH_OPCNT_LOOKUP_FIDC,   //0
	FCMH_OPCNT_LOOKUP_PARENT, //1
	FCMH_OPCNT_OPEN,          //2
	FCMH_OPCNT_BMAP,          //3
	FCMH_OPCNT_CHILD,         //4
	FCMH_OPCNT_NEW,           //5
	FCMH_OPCNT_WAIT           //6
};

static __inline void *
fcmh_get_pri(struct fidc_membh *fcmh)
{
	return (fcmh + 1);
}

/* fcmh_setattr() flags */
#define FCMH_SETATTRF_NONE	0
#define FCMH_SETATTRF_SAVESIZE	(1 << 0)

void			 fcmh_dtor(void *);
struct fidc_membh	*fcmh_get(void);
void			 fcmh_setattr(struct fidc_membh *, const struct srt_stat *, int);

void			 fidc_put(struct fidc_membh *, struct psc_listcache *);
void			 fidc_init(int, int, int, int (*)(struct fidc_membh *));

/* fidc_lookup() flags */
enum {
	FIDC_LOOKUP_CREATE	= (1 << 0),		/* Create if not present         */
	FIDC_LOOKUP_EXCL	= (1 << 1),		/* Fail if fcmh is present       */
	FIDC_LOOKUP_LOAD	= (1 << 2),		/* Use external fetching mechanism */
	FIDC_LOOKUP_REMOVE	= (1 << 3)		/* remove the fcmh from the hash table */
};

int			 fidc_lookup(const struct slash_fidgen *, int,
			    const struct srt_stat *, int, const struct slash_creds *,
			    struct fidc_membh **);

/* two wrappers of fidc_lookup(), they are used to do simple lookups without any special flags */
struct fidc_membh	*fidc_lookup_fid(slfid_t);
struct fidc_membh	*fidc_lookup_fg(const struct slash_fidgen *);

void                     fcmh_op_start_type(struct fidc_membh *, enum fcmh_opcnt_types);

void                     fcmh_op_done_type(struct fidc_membh *, enum fcmh_opcnt_types);

void			 dump_fcmh(struct fidc_membh *);
void			 dump_fcmh_flags(int);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_listcache	 fidcDirtyList;
extern struct psc_listcache	 fidcCleanList;

#define fidcFreeList		fidcPool->ppm_lc

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
 * fcmh_clean_check - Verify the validity of a fid cache member.
 */
static __inline int
fcmh_clean_check(struct fidc_membh *f)
{
	int locked, clean = 0;

	locked = reqlock(&f->fcmh_lock);
	DEBUG_FCMH(PLL_INFO, f, "clean_check");
	if (f->fcmh_state & FCMH_CAC_CLEAN) {
		psc_assert(!(f->fcmh_state &
		     (FCMH_CAC_DIRTY | FCMH_CAC_FREE)));
		clean = 1;
	}
	ureqlock(&f->fcmh_lock, locked);
	return (clean);
}

static __inline int
fcmh_getload(const struct slash_fidgen *fgp, const struct slash_creds *crp,
    struct fidc_membh **fcmhp)
{
	return (fidc_lookup(fgp, FIDC_LOOKUP_CREATE,
	    NULL, 0, crp, fcmhp));
}

#endif /* _SL_FIDCACHE_H_ */
