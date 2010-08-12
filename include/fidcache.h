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

/*
 * The fidcache manages a pool of handles in memory representing files
 * resident in a SLASH network.  Entries in this pool are thusly
 * fidcache member handles (fcmh).
 */

#ifndef _SL_FIDCACHE_H_
#define _SL_FIDCACHE_H_

#include "pfl/hashtbl.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"
#include "psc_util/time.h"

#include "cache_params.h"
#include "fid.h"

#ifndef __LP64__
#define DEMOTED_INUM_WIDTHS
#endif

struct bmapc_memb;
struct fidc_membh;

/* XXX move to bmap.h */
SPLAY_HEAD(bmap_cache, bmapc_memb);

struct sl_fcmh_ops {
	int	(*sfop_ctor)(struct fidc_membh *);
	void	(*sfop_dtor)(struct fidc_membh *);
	int	(*sfop_getattr)(struct fidc_membh *);
	void	(*sfop_postsetattr)(struct fidc_membh *);
};

/*
 * fidc_membh - the primary inode cache structure, all
 * updates and lookups into the inode are done through here.
 *
 * fidc_membh tracks cached bmaps (bmap_cache) and clients
 * (via their exports) which hold cached bmaps.
 *
 * Service specific private structures (i.e., fcmh_mds_info,
 * fcmh_cli_info, and fcmh_iod_info) are allocated along with
 * the fidc_membh structure.  They can be accessed by calling
 * fcmh_get_pri() defined below.
 */
struct fidc_membh {
	struct slash_fidgen	 fcmh_fg;		/* identity of the file */
#ifdef DEMOTED_INUM_WIDTHS
	struct slash_fidgen	 fcmh_smallfg;		/* integer-demoted fg_fid for hashing */
#endif
	struct srt_stat		 fcmh_sstb;		/* higher-level stat(2) buffer */
	int			 fcmh_flags;		/* see FCMH_* below */
	psc_spinlock_t		 fcmh_lock;
	int			 fcmh_refcnt;
	struct psc_hashent	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct psc_waitq	 fcmh_waitq;
	struct bmap_cache	 fcmh_bmaptree;		/* bmap cache splay */
#define fcmh_state fcmh_flags
};

/* fcmh_flags */
#define	FCMH_CAC_FREE		(1 <<  0)	/* totally free item */
#define	FCMH_CAC_CLEAN		(1 <<  1)	/* in clean cache */
#define	FCMH_CAC_DIRTY		(1 <<  2)	/* dirty, not reapable */
#define	FCMH_CAC_INITING	(1 <<  3)	/* initializing */
#define	FCMH_CAC_WAITING	(1 <<  4)	/* being waited on */
#define	FCMH_CAC_TOFREE		(1 <<  5)	/* been deprecated */
#define	FCMH_CAC_REAPED		(1 <<  6)	/* has been reaped */
#define	FCMH_HAVE_ATTRS		(1 <<  7)	/* has valid stat info */
#define	FCMH_GETTING_ATTRS	(1 <<  8)	/* fetching stat info */
#define	FCMH_CTOR_FAILED	(1 <<  9)	/* constructor fn failed */
#define	FCMH_CTOR_SUCCESS	(1 << 10)	/* constructor success */
#define	_FCMH_FLGSHFT		(1 << 11)

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

/* number of seconds in which attribute times out */
#define FCMH_ATTR_TIMEO		8

#define FCMH_LOCK(f)		spinlock(&(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock(&(f)->fcmh_lock)
#define FCMH_TRYLOCK(f)		trylock(&(f)->fcmh_lock)
#define FCMH_RLOCK(f)		reqlock(&(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock(&(f)->fcmh_lock, (lk))
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)

#define fcmh_2_fid(f)		(f)->fcmh_fg.fg_fid
#define fcmh_2_gen(f)		(f)->fcmh_sstb.sst_gen
#define fcmh_2_fsz(f)		(f)->fcmh_sstb.sst_size
#define fcmh_2_fg(f)            (f)->fcmh_fg
#define fcmh_2_nbmaps(f)	((sl_bmapno_t)howmany(fcmh_2_fsz(f), SLASH_BMAP_SIZE))
#define fcmh_2_ptruncgen(f)	(f)->fcmh_sstb.sst_ptruncgen
#define fcmh_2_utimgen(f)	(f)->fcmh_sstb.sst_utimgen

#define fcmh_isdir(f)		S_ISDIR((f)->fcmh_sstb.sst_mode)
#define fcmh_isreg(f)		S_ISREG((f)->fcmh_sstb.sst_mode)

#define fcmh_wait_locked(f, cond)					\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		while (cond) {						\
			psc_waitq_wait(&(f)->fcmh_waitq,		\
				       &(f)->fcmh_lock);		\
			FCMH_LOCK(f);					\
		}							\
	} while (0)

#define fcmh_wait_nocond_locked(f)					\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		psc_waitq_wait(&(f)->fcmh_waitq,			\
			       &(f)->fcmh_lock);			\
		FCMH_LOCK(f);						\
	} while (0)

#define fcmh_wake_locked(f)						\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		if (psc_waitq_nwaiters(&(f)->fcmh_waitq))		\
			psc_waitq_wakeall(&(f)->fcmh_waitq);		\
	} while (0)

#define DEBUG_FCMH_FLAGS(fcmh)						\
	(fcmh)->fcmh_state & FCMH_CAC_FREE		? "F" : "",	\
	(fcmh)->fcmh_state & FCMH_CAC_CLEAN		? "C" : "",	\
	(fcmh)->fcmh_state & FCMH_CAC_DIRTY		? "D" : "",	\
	(fcmh)->fcmh_state & FCMH_CAC_TOFREE		? "T" : "",	\
	(fcmh)->fcmh_state & FCMH_HAVE_ATTRS		? "A" : "",	\
	(fcmh)->fcmh_state & FCMH_GETTING_ATTRS		? "G" : "",	\
	(fcmh)->fcmh_state & FCMH_CAC_INITING		? "I" : "",	\
	(fcmh)->fcmh_state & FCMH_CAC_WAITING		? "W" : "",	\
	(fcmh)->fcmh_state & FCMH_CTOR_FAILED		? "f" : "",	\
	fcmh_isdir(fcmh)				? "d" : ""

#define REQ_FCMH_FLAGS_FMT	"%s%s%s%s%s%s%s%s%s%s"

#define DEBUG_FCMH(level, fcmh, fmt, ...)				\
	psc_logs((level), PSS_GEN,					\
	   "fcmh@%p fg:"FIDFMT" "REQ_FCMH_FLAGS_FMT" "			\
	   "ref:%d sz=%"PRId64" :: "fmt,				\
	   (fcmh), FIDFMTARGS(&(fcmh)->fcmh_fg),			\
	   DEBUG_FCMH_FLAGS(fcmh),					\
	   (fcmh)->fcmh_refcnt, fcmh_2_fsz(fcmh),			\
	   ## __VA_ARGS__)

/* debugging aid: spit out the reason for the reference count taking/dropping */
enum fcmh_opcnt_types {
	FCMH_OPCNT_LOOKUP_FIDC,		/* 0 */
	FCMH_OPCNT_OPEN,		/* 1 */
	FCMH_OPCNT_BMAP,		/* 2 */
	FCMH_OPCNT_DIRENTBUF,		/* 3 */
	FCMH_OPCNT_NEW,			/* 4 */
	FCMH_OPCNT_WAIT,		/* 5 */
	FCMH_OPCNT_UPSCHED		/* 6 */
};

/* fcmh_setattr() flags */
#define FCMH_SETATTRF_NONE		0
#define FCMH_SETATTRF_SAVELOCAL		(1 << 0)	/* save local updates (file size, etc) */
#define FCMH_SETATTRF_HAVELOCK		(1 << 1)

void	fidc_init(int, int, int, int (*)(struct fidc_membh *));
void	fcmh_setattr(struct fidc_membh *, const struct srt_stat *, int);

/* fidc_lookup() flags */
#define FIDC_LOOKUP_NONE		0
#define FIDC_LOOKUP_CREATE		(1 << 0)	/* Create if not present         */
#define FIDC_LOOKUP_EXCL		(1 << 1)	/* Fail if fcmh is present       */
#define FIDC_LOOKUP_LOAD		(1 << 2)	/* Use external fetching mechanism */

#define fidc_lookup(fgp, lkfl, sstb, safl, fcmhp)			\
	_fidc_lookup((fgp), (lkfl), (sstb), (safl), (fcmhp),		\
	    __FILE__, __func__, __LINE__)

#define fidc_lookup_fid(fid)						\
	_fidc_lookup_fid((fid), __FILE__, __func__, __LINE__)

#define fidc_lookup_fg(fgp)						\
	_fidc_lookup_fg((fgp), __FILE__, __func__, __LINE__)

int			 _fidc_lookup(const struct slash_fidgen *, int,
			    const struct srt_stat *, int, struct fidc_membh **,
			    const char *, const char *, int);

/* these fidc_lookup() wrappers are used for simple lookups (no flags) */
struct fidc_membh	*_fidc_lookup_fid(slfid_t, const char *, const char *, int);
struct fidc_membh	*_fidc_lookup_fg(const struct slash_fidgen *, const char *, const char *, int);
ssize_t			 fcmh_getsize(struct fidc_membh *);

void			 fcmh_op_start_type(struct fidc_membh *, enum fcmh_opcnt_types);
void			 fcmh_op_done_type(struct fidc_membh *, enum fcmh_opcnt_types);

void			 dump_fidcache(void);
void			 dump_fcmh(struct fidc_membh *);
void			 dump_fcmh_flags(int);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_listcache	 fidcDirtyList;
extern struct psc_listcache	 fidcCleanList;
extern struct psc_hashtbl	 fidcHtable;

#define fidcFreeList		fidcPool->ppm_lc

static __inline void *
fcmh_get_pri(struct fidc_membh *fcmh)
{
	return (fcmh + 1);
}

#endif /* _SL_FIDCACHE_H_ */
