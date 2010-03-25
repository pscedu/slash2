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
	struct timeval		 fcmh_age;		/* age of this entry */
	struct srt_stat		 fcmh_sstb;
	int			 fcmh_state;
	psc_spinlock_t		 fcmh_lock;
	int			 fcmh_refcnt;
	struct psc_hashent	 fcmh_hentry;
	struct psclist_head	 fcmh_lentry;
	struct psc_waitq	 fcmh_waitq;
	struct bmap_cache	 fcmh_bmaptree;		/* bmap cache splay */
};

static __inline void *
fcmh_get_pri(struct fidc_membh *fcmh)
{
	return (fcmh + 1);
}

/* fcmh_flags */

#define	FCMH_CAC_FREE		0x0001		/* (1 << 0) totally free item */
#define	FCMH_CAC_CLEAN		0x0002		/* (1 << 1) in clean cache */
#define	FCMH_CAC_DIRTY		0x0004		/* (1 << 2) in dirty cache, "dirty" means not reapable */

#define	FCMH_CAC_FREEING	0x0008		/* (1 << 3) this item is being freed */
#define	FCMH_CAC_INITING	0x0010		/* (1 << 4) this item is being initialized */
#define	FCMH_CAC_WAITING	0x0020		/* (1 << 5) this item is being waited on */

#define	FCMH_HAVE_ATTRS		0x0040		/* (1 << 6) has valid stat info */
#define	FCMH_GETTING_ATTRS	0x0080		/* (1 << 7) fetching stat info */
#define	FCMH_WAITING_ATTRS	0x0100		/* (1 << 8) someone is waiting */

#define	_FCMH_FLGSHFT		0x0200		/* (1 << 9) */

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
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREE)		? "F" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_CLEAN)		? "C" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_DIRTY)		? "D" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_CAC_FREEING)		? "R" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_HAVE_ATTRS)		? "A" : "",	\
	ATTR_TEST((fcmh)->fcmh_state, FCMH_GETTING_ATTRS)	? "G" : ""

#define REQ_FCMH_FLAGS_FMT "%s%s%s%s%s%s"

#define FIDFMT			"%"PRId64":%"PRId64
#define FIDFMTARGS(fg)		(fg)->fg_fid, (fg)->fg_gen

#define DEBUG_FCMH(level, fcmh, fmt, ...)					\
	do {									\
		psc_logs((level), PSS_GEN,					\
		    "fcmh@%p fg:"FIDFMT" "REQ_FCMH_FLAGS_FMT" "			\
		    "ref:%d :: "fmt,					\
		    (fcmh), FIDFMTARGS(&fcmh->fcmh_fg), DEBUG_FCMH_FLAGS(fcmh),	\
		    (fcmh)->fcmh_refcnt,					\
		    ## __VA_ARGS__);						\
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

/* fcmh_setattr() flags */
#define FCMH_SETATTRF_NONE		(0 << 0)
#define FCMH_SETATTRF_SAVESIZE		(1 << 0)
#define FCMH_SETATTRF_HAVELOCK		(1 << 1)

/* users of FIDC cache */
#define	FIDC_MDS			1
#define FIDC_IOD			2
#define	FIDC_CLIENT			3

void	fidc_init(int, int, int, int (*)(struct fidc_membh *), int);
void	fcmh_setattr(struct fidc_membh *, const struct srt_stat *, int);


/* fidc_lookup() flags */
enum {
	FIDC_LOOKUP_NONE	= (0 << 0),		/* no flag */
	FIDC_LOOKUP_CREATE	= (1 << 0),		/* Create if not present         */
	FIDC_LOOKUP_EXCL	= (1 << 1),		/* Fail if fcmh is present       */
	FIDC_LOOKUP_LOAD	= (1 << 2)		/* Use external fetching mechanism */
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

static __inline int
fcmh_getload(const struct slash_fidgen *fgp, const struct slash_creds *crp,
    struct fidc_membh **fcmhp)
{
	return (fidc_lookup(fgp, FIDC_LOOKUP_CREATE,
	    NULL, 0, crp, fcmhp));
}

#endif /* _SL_FIDCACHE_H_ */
