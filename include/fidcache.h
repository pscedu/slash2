/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/time.h"
#include "psc_ds/tree.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"

#include "bmap.h"
#include "cache_params.h"
#include "fid.h"
#include "slashrpc.h"
#include "slsubsys.h"

struct fidc_membh;

struct sl_fcmh_ops {
	int	(*sfop_ctor)(struct fidc_membh *);
	void	(*sfop_dtor)(struct fidc_membh *);
	int	(*sfop_getattr)(struct fidc_membh *, void *);
	void	(*sfop_postsetattr)(struct fidc_membh *);
	int	(*sfop_modify)(struct fidc_membh *, const struct slash_fidgen *);
};

/**
 * fidc_membh - The primary cached file structure; all updates and
 * lookups into the inode are done through here.
 *
 * fidc_membh tracks cached bmaps (bmap_cache) and clients (via their
 * exports) which hold cached bmaps.
 *
 * Service specific private structures (i.e. fcmh_mds_info,
 * fcmh_iod_info, and fcmh_cli_info) are allocated along with the
 * fidc_membh structure.  They can be accessed by calling
 * fcmh_get_pri() defined below.
 */
struct fidc_membh {
	struct srt_stat		 fcmh_sstb;	/* higher-level stat(2) buffer */
	int			 fcmh_flags;	/* see FCMH_* below */
	psc_spinlock_t		 fcmh_lock;
	int			 fcmh_refcnt;	/* threads referencing us */
	struct psc_hashent	 fcmh_hentry;	/* hash table membership for lookups */
	struct psclist_head	 fcmh_lentry;	/* busy or idle list */
	struct psc_waitq	 fcmh_waitq;	/* wait here for operations */
	struct bmap_cache	 fcmh_bmaptree;	/* bmap cache splay */
};

/* fcmh_flags (cache) */
#define	FCMH_CAC_FREE		(1 <<  0)	/* totally free item */
#define	FCMH_CAC_IDLE		(1 <<  1)	/* not being used, in clean cache */
#define	FCMH_CAC_BUSY		(1 <<  2)	/* being used, not reapable */
#define	FCMH_CAC_INITING	(1 <<  3)	/* initializing */
#define	FCMH_CAC_WAITING	(1 <<  4)	/* being waited on */
#define	FCMH_CAC_TOFREE		(1 <<  5)	/* ctor failure or memory pressure */
#define	FCMH_CAC_REAPED		(1 <<  6)	/* has been reaped */
#define	FCMH_CAC_RLSBMAP	(1 <<  7)	/* lookup due to releasing bmap */
#define	FCMH_HAVE_ATTRS		(1 <<  8)	/* has valid stat(2) info */
#define	FCMH_GETTING_ATTRS	(1 <<  9)	/* fetching stat(2) info */
#define	FCMH_CTOR_FAILED	(1 << 10)	/* constructor func failed */
#define	FCMH_NO_BACKFILE	(1 << 11)	/* fcmh does not have a backing file */
#define FCMH_IN_SETATTR		(1 << 12)	/* setattr in progress */
#define	_FCMH_FLGSHFT		(1 << 13)

/* number of seconds in which attribute times out */
#define FCMH_ATTR_TIMEO		8
#define FCMH_ATTR_TIMEO_SHORT	4

#define FCMH_LOCK(f)		spinlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_TRYLOCK(f)		trylock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_TRYREQLOCK(f, lk)	tryreqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock, (lk))
#define FCMH_RLOCK(f)		reqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock, (lk))
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)

#define fcmh_fg			fcmh_sstb.sst_fg
#define fcmh_2_fid(f)		(f)->fcmh_fg.fg_fid
#define fcmh_2_gen(f)		(f)->fcmh_fg.fg_gen
#define fcmh_2_fsz(f)		(f)->fcmh_sstb.sst_size
#define fcmh_2_mode(f)		(f)->fcmh_sstb.sst_mode
#define fcmh_2_nbmaps(f)	((sl_bmapno_t)howmany(fcmh_getsize(f), SLASH_BMAP_SIZE))
#define fcmh_2_ptruncgen(f)	(f)->fcmh_sstb.sst_ptruncgen
#define fcmh_2_utimgen(f)	(f)->fcmh_sstb.sst_utimgen
#define fcmh_2_nblks(f)		(f)->fcmh_sstb.sst_blocks

#define fcmh_isdir(f)		S_ISDIR((f)->fcmh_sstb.sst_mode)
#define fcmh_isreg(f)		S_ISREG((f)->fcmh_sstb.sst_mode)

#define fcmh_wait_locked(f, cond)					\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		while (cond) {						\
			psc_waitq_wait(&(f)->fcmh_waitq,		\
			    &(f)->fcmh_lock);				\
			FCMH_LOCK(f);					\
		}							\
	} while (0)

#define fcmh_wait_nocond_locked(f)					\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		psc_waitq_wait(&(f)->fcmh_waitq, &(f)->fcmh_lock);	\
		FCMH_LOCK(f);						\
	} while (0)

#define fcmh_wake_locked(f)						\
	do {								\
		FCMH_LOCK_ENSURE(f);					\
		if (psc_waitq_nwaiters(&(f)->fcmh_waitq))		\
			psc_waitq_wakeall(&(f)->fcmh_waitq);		\
	} while (0)

#ifdef _SLASH_MDS
# define DEBUG_FCMH_BLKSIZE_LABEL "msz"
#else
# define DEBUG_FCMH_BLKSIZE_LABEL "bsz"
#endif

#define DEBUG_FCMH(level, f, fmt, ...)					\
	psclogs((level), SLSS_FCMH,					\
	    "fcmh@%p f+g="SLPRI_FG" flg=%#x:%s%s%s%s%s%s%s%s%s%s%s%s "	\
	    "ref=%d sz=%"PRId64" "DEBUG_FCMH_BLKSIZE_LABEL"=%"PRId64" "	\
	    "mode=%#o : "fmt,						\
	    (f), SLPRI_FG_ARGS(&(f)->fcmh_fg), (f)->fcmh_flags,		\
	    (f)->fcmh_flags & FCMH_CAC_FREE		? "F" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_IDLE		? "i" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_BUSY		? "B" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_INITING		? "I" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_WAITING		? "W" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_TOFREE		? "T" : "",	\
	    (f)->fcmh_flags & FCMH_CAC_REAPED		? "R" : "",	\
	    (f)->fcmh_flags & FCMH_HAVE_ATTRS		? "A" : "",	\
	    (f)->fcmh_flags & FCMH_GETTING_ATTRS	? "G" : "",	\
	    (f)->fcmh_flags & FCMH_CTOR_FAILED		? "f" : "",	\
	    (f)->fcmh_flags & FCMH_NO_BACKFILE		? "N" : "",	\
	    (f)->fcmh_flags & ~(_FCMH_FLGSHFT - 1)	? "+" : "",	\
	    (f)->fcmh_refcnt, fcmh_2_fsz(f), (f)->fcmh_sstb.sst_blksize,\
	    (f)->fcmh_sstb.sst_mode, ## __VA_ARGS__)

/* debugging aid: spit out the reason for the reference count taking/dropping */
enum fcmh_opcnt_types {
/* 0 */	FCMH_OPCNT_BMAP,		/* bcm_fcmh */
/* 1 */	FCMH_OPCNT_DIRENTBUF,
/* 2 */	FCMH_OPCNT_LOOKUP_FIDC,		/* fidc_lookup() */
/* 3 */	FCMH_OPCNT_NEW,
/* 4 */	FCMH_OPCNT_OPEN,		/* mount_slash pscfs file info */
/* 5 */	FCMH_OPCNT_UPSCHED,		/* MDS uswi_fcmh */
/* 6 */	FCMH_OPCNT_WAIT,
/* 7 */	FCMH_OPCNT_WORKER,		/* MDS worker */
/* 8 */	FCMH_OPCNT_DIRTY_QUEUE
};

/* fcmh_setattr() flags */
#define FCMH_SETATTRF_NONE		0
#define FCMH_SETATTRF_SAVELOCAL		(1 << 0)	/* save local updates (file size, etc) */
#define FCMH_SETATTRF_HAVELOCK		(1 << 1)

void	 fidc_init(int, int);
void	 fcmh_setattrf(struct fidc_membh *, struct srt_stat *, int);
void	_fcmh_decref(const struct pfl_callerinfo *, struct fidc_membh *, enum fcmh_opcnt_types);

#define fcmh_decref(f, type)		_fcmh_decref(PFL_CALLERINFOSS(SLSS_FCMH), (f), (type))

#define fcmh_setattr(f, sstb)		fcmh_setattrf((f), (sstb), 0)
#define fcmh_setattr_locked(f, sstb)	fcmh_setattrf((f), (sstb), FCMH_SETATTRF_HAVELOCK)

/* fidc_lookup() flags */
#define FIDC_LOOKUP_NONE		0
#define FIDC_LOOKUP_CREATE		(1 << 0)	/* Create if not present		*/
#define FIDC_LOOKUP_EXCL		(1 << 1)	/* Fail if fcmh is present		*/
#define FIDC_LOOKUP_LOAD		(1 << 2)	/* Use external fetching mechanism	*/
#define FIDC_LOOKUP_RLSBMAP		(1 << 3)	/* Release bmap on sliod		*/

#define fidc_lookup(fgp, lkfl, sstb, safl, fcmhp)			\
	_fidc_lookup(PFL_CALLERINFOSS(SLSS_FCMH), (fgp), (lkfl),	\
	    (sstb), (safl), (fcmhp), NULL)

#define fidc_lookup_fid(fid)		_fidc_lookup_fid(PFL_CALLERINFOSS(SLSS_FCMH), (fid))
#define fidc_lookup_fg(fgp)		_fidc_lookup_fg(PFL_CALLERINFOSS(SLSS_FCMH), (fgp))

int	 _fidc_lookup(const struct pfl_callerinfo *,
	    const struct slash_fidgen *, int, struct srt_stat *, int,
	    struct fidc_membh **, void *);

/* these fidc_lookup() wrappers are used for simple lookups (no flags) */
struct fidc_membh	*_fidc_lookup_fid(const struct pfl_callerinfo *, slfid_t);
struct fidc_membh	*_fidc_lookup_fg(const struct pfl_callerinfo *, const struct slash_fidgen *);

ssize_t	 fcmh_getsize(struct fidc_membh *);

void	_fcmh_op_start_type(const struct pfl_callerinfo *, struct fidc_membh *, enum fcmh_opcnt_types);
void	_fcmh_op_done_type(const struct pfl_callerinfo *, struct fidc_membh *, enum fcmh_opcnt_types);

#define fcmh_op_start_type(fcmh, type)					\
	_fcmh_op_start_type(PFL_CALLERINFOSS(SLSS_FCMH), (fcmh), (type))

#define fcmh_op_done_type(fcmh, type)					\
	_fcmh_op_done_type(PFL_CALLERINFOSS(SLSS_FCMH), (fcmh), (type))

#define fcmh_op_done(fcmh)	fcmh_op_done_type((fcmh), FCMH_OPCNT_LOOKUP_FIDC)

void	 _dump_fcmh_flags_common(int *, int *);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_hashtbl	 fidcHtable;

static __inline void *
fcmh_get_pri(struct fidc_membh *fcmh)
{
	return (fcmh + 1);
}

static __inline struct fidc_membh *
fcmh_from_pri(void *p)
{
	struct fidc_membh *f = p;

	return (f - 1);
}

#endif /* _SL_FIDCACHE_H_ */
