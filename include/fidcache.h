/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * The fidcache manages a pool of handles in memory representing files
 * resident in a SLASH2 network.  Entries in this pool are thusly
 * fidcache member handles (fcmh).
 */

#ifndef _SL_FIDCACHE_H_
#define _SL_FIDCACHE_H_

#include "pfl/hashtbl.h"
#include "pfl/lock.h"
#include "pfl/pool.h"
#include "pfl/time.h"
#include "pfl/tree.h"

#include "bmap.h"
#include "cache_params.h"
#include "fid.h"
#include "slashrpc.h"
#include "slsubsys.h"

#define MAX_FCMH_LIFETIME	(60 * 5)

struct fidc_membh;

struct sl_fcmh_ops {
	int	(*sfop_ctor)(struct fidc_membh *, int);
	void	(*sfop_dtor)(struct fidc_membh *);
	int	(*sfop_getattr)(struct fidc_membh *, void *);
	void	(*sfop_postsetattr)(struct fidc_membh *);
	int	(*sfop_modify)(struct fidc_membh *, const struct sl_fidgen *);
};

/**
 * fidc_membh - The primary cached file structure; all updates and
 * lookups into the inode are done through here.
 *
 * fidc_membh tracks cached bmaps (bmaptree) and clients (via their
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
	int			 fcmh_refcnt;	/* threads referencing us */
	psc_spinlock_t		 fcmh_lock;
	pthread_t		 fcmh_owner;	/* holds BUSY */
	const char		*fcmh_fn;
	int			 fcmh_lineno;
	struct pfl_hashentry	 fcmh_hentry;	/* hash table membership for lookups */
	struct psclist_head	 fcmh_lentry;	/* busy or idle list */
	struct psc_waitq	 fcmh_waitq;	/* wait here for operations */
	struct timespec		 fcmh_etime;	/* current expire time */
	struct bmaptree		 fcmh_bmaptree;	/* bmap cache splay */
	struct pfl_rwlock	 fcmh_rwlock;
};

/* fcmh_flags */
#define FCMH_FREE		(1 <<  0)	/* totally free item */
#define FCMH_IDLE		(1 <<  1)	/* on idle list */
#define FCMH_INITING		(1 <<  2)	/* initializing */
#define FCMH_WAITING		(1 <<  3)	/* being waited on */
#define FCMH_TOFREE		(1 <<  4)	/* ctor failure or memory pressure */
#define FCMH_HAVE_ATTRS		(1 <<  5)	/* has valid stat(2) info */
#define FCMH_GETTING_ATTRS	(1 <<  6)	/* fetching stat(2) info */
#define FCMH_CTOR_FAILED	(1 <<  7)	/* constructor func failed */
#define FCMH_BUSY		(1 <<  8)	/* fcmh being processed */
#define FCMH_DELETED		(1 <<  9)	/* fcmh has been deleted */
#define _FCMH_FLGSHFT		(1 << 10)

/* number of seconds in which attribute times out */
#define FCMH_ATTR_TIMEO		8

#define FCMH_LOCK(f)		spinlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_TRYLOCK(f)		trylock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_TRYRLOCK(f, lk)	tryreqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock, (lk))
#define FCMH_RLOCK(f)		reqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock_pci(PFL_CALLERINFOSS(SLSS_FCMH), &(f)->fcmh_lock, (lk))
#define FCMH_LOCK_ENSURE(f)	LOCK_ENSURE(&(f)->fcmh_lock)
#define FCMH_HAS_LOCK(f)	psc_spin_haslock(&(f)->fcmh_lock)

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

#define FCMH_TRYBUSY(f)							\
	_PFL_RVSTART {							\
		pthread_t _pthr = pthread_self();			\
		int _waslocked, _got = 0;				\
									\
		_waslocked = FCMH_RLOCK(f);				\
		if ((f)->fcmh_flags & FCMH_BUSY) {			\
			if ((f)->fcmh_owner == _pthr)			\
				DEBUG_FCMH(PLL_FATAL, (f),		\
				    "TRYBUSY: already holding");	\
		} else {						\
			(f)->fcmh_flags |= FCMH_BUSY;			\
			(f)->fcmh_owner = _pthr;			\
			(f)->fcmh_lineno = __LINE__;			\
			(f)->fcmh_fn = __FILE__;			\
			DEBUG_FCMH(PLL_DEBUG, (f), "set BUSY");		\
			_got = 1;					\
		}							\
		FCMH_URLOCK(f, _waslocked);				\
		(_got);							\
	} _PFL_RVEND

#define FCMH_REQ_BUSY(f, waslocked)					\
	_PFL_RVSTART {							\
		pthread_t _pthr = pthread_self();			\
		int _wasbusy = 0;					\
									\
		*(waslocked) = FCMH_RLOCK(f);				\
		if (((f)->fcmh_flags & FCMH_BUSY) &&			\
		    (f)->fcmh_owner == _pthr) {				\
			 _wasbusy = 1;					\
			DEBUG_FCMH(PLL_DEBUG, (f), "require BUSY");	\
		} else {						\
			fcmh_wait_locked((f),				\
			    (f)->fcmh_flags & FCMH_BUSY);		\
			(f)->fcmh_flags |= FCMH_BUSY;			\
			(f)->fcmh_owner = _pthr;			\
			(f)->fcmh_lineno = __LINE__;			\
			(f)->fcmh_fn = __FILE__;			\
			DEBUG_FCMH(PLL_DEBUG, (f), "set BUSY");		\
		}							\
		(_wasbusy);						\
	} _PFL_RVEND

#define FCMH_UREQ_BUSY(f, wasbusy, waslocked)				\
	do {								\
		(void)FCMH_RLOCK(f);					\
		FCMH_BUSY_ENSURE(f);					\
		if (wasbusy) {						\
			DEBUG_FCMH(PLL_DEBUG, (f), "unrequire BUSY");	\
		} else {						\
			(f)->fcmh_owner = 0;				\
			(f)->fcmh_flags &= ~FCMH_BUSY;			\
			DEBUG_FCMH(PLL_DEBUG, (f), "cleared BUSY");	\
			fcmh_wake_locked(f);				\
		}							\
		FCMH_URLOCK((f), (waslocked));				\
	} while (0)

#define FCMH_WAIT_BUSY(f)						\
	do {								\
		int _wb, _waslocked;					\
									\
		_wb = FCMH_REQ_BUSY((f), &_waslocked);			\
		psc_assert(_wb == 0);					\
	} while (0)

#define FCMH_UNBUSY(f)		FCMH_UREQ_BUSY((f), 0, PSLRV_WASNOTLOCKED)

#define FCMH_HAS_BUSY(f)						\
	(((f)->fcmh_flags & FCMH_BUSY) &&				\
	 (f)->fcmh_owner == pthread_self())

#define FCMH_BUSY_ENSURE(f)						\
	psc_assert(FCMH_HAS_BUSY(f))

#ifdef _SLASH_MDS
# define DEBUG_FCMH_BLKSIZE_LABEL "msz"
#else
# define DEBUG_FCMH_BLKSIZE_LABEL "bsz"
#endif

#define DEBUG_FCMH(level, f, fmt, ...)					\
	psclogs((level), SLSS_FCMH,					\
	    "fcmh@%p f+g="SLPRI_FG" "					\
	    "flg=%#x:%s%s%s%s%s%s%s%s%s%s%s "				\
	    "ref=%d sz=%"PRId64" "DEBUG_FCMH_BLKSIZE_LABEL"=%"PRId64" "	\
	    "mode=%#o : "fmt,						\
	    (f), SLPRI_FG_ARGS(&(f)->fcmh_fg), (f)->fcmh_flags,		\
	    (f)->fcmh_flags & FCMH_FREE			? "F" : "",	\
	    (f)->fcmh_flags & FCMH_IDLE			? "i" : "",	\
	    (f)->fcmh_flags & FCMH_INITING		? "I" : "",	\
	    (f)->fcmh_flags & FCMH_WAITING		? "W" : "",	\
	    (f)->fcmh_flags & FCMH_TOFREE		? "T" : "",	\
	    (f)->fcmh_flags & FCMH_HAVE_ATTRS		? "A" : "",	\
	    (f)->fcmh_flags & FCMH_GETTING_ATTRS	? "G" : "",	\
	    (f)->fcmh_flags & FCMH_CTOR_FAILED		? "f" : "",	\
	    (f)->fcmh_flags & FCMH_BUSY			? "S" : "",	\
	    (f)->fcmh_flags & FCMH_DELETED		? "D" : "",	\
	    (f)->fcmh_flags & ~(_FCMH_FLGSHFT - 1)	? "+" : "",	\
	    (f)->fcmh_refcnt, fcmh_2_fsz(f), (f)->fcmh_sstb.sst_blksize,\
	    (f)->fcmh_sstb.sst_mode, ## __VA_ARGS__)

/* types of references */
#define FCMH_OPCNT_BMAP			 0	/* bcm_fcmh backpointer */
#define FCMH_OPCNT_LOOKUP_FIDC		 1	/* fidc_lookup() */
#define FCMH_OPCNT_NEW			 2
#define FCMH_OPCNT_OPEN			 3	/* CLI: pscfs file info */
#define FCMH_OPCNT_WAIT			 4	/* dup ref during initialization */
#define FCMH_OPCNT_WORKER		 5	/* MDS: generic worker */
#define FCMH_OPCNT_DIRTY_QUEUE		 6	/* CLI: attribute flushing */
#define FCMH_OPCNT_UPSCH		 7	/* MDS: temporarily held by upsch engine */
#define FCMH_OPCNT_READDIR		 8	/* CLI: readahead readdir RPC */
#define FCMH_OPCNT_READAHEAD		 9	/* IOD/CLI: readahead */
#define FCMH_OPCNT_DIRCACHE		10	/* CLI: async dircache */
#define FCMH_OPCNT_MAXTYPE		11

void	fidc_init(int);

/* fidc_lookup() flags */
#define FIDC_LOOKUP_NONE		0
#define FIDC_LOOKUP_CREATE		(1 << 0)	/* Create if not present		*/
#define FIDC_LOOKUP_EXCL		(1 << 1)	/* Fail if fcmh is present		*/
#define FIDC_LOOKUP_LOAD		(1 << 2)	/* Use external fetching mechanism	*/

int	fidc_reap(int, int);

void	sl_freapthr_spawn(int, const char *);

int	_fidc_lookup(const struct pfl_callerinfo *,
	    const struct sl_fidgen *, int,
	    struct fidc_membh **, void *);

#define fidc_lookup(fgp, lkfl, fp)					\
	_fidc_lookup(PFL_CALLERINFOSS(SLSS_FCMH), (fgp), (lkfl), (fp),	\
	    NULL)

#define fidc_lookup_fg(fgp, fp)		fidc_lookup((fgp), 0, (fp))

#define fidc_lookup_fid(fid, fp)					\
	_PFL_RVSTART {							\
		struct sl_fidgen _fg = { (fid), FGEN_ANY };		\
									\
		fidc_lookup(&_fg, 0, (fp));				\
	} _PFL_RVEND

ssize_t	 fcmh_getsize(struct fidc_membh *);

void	_fcmh_op_start_type(const struct pfl_callerinfo *, struct fidc_membh *, int);
void	_fcmh_op_done_type(const struct pfl_callerinfo *, struct fidc_membh *, int);

#define fcmh_op_start_type(f, type)					\
	_fcmh_op_start_type(PFL_CALLERINFOSS(SLSS_FCMH), (f), (type))

#define fcmh_op_done_type(f, type)					\
	_fcmh_op_done_type(PFL_CALLERINFOSS(SLSS_FCMH), (f), (type))

#define fcmh_op_done(f)		fcmh_op_done_type((f), FCMH_OPCNT_LOOKUP_FIDC)

void	_dump_fcmh_flags_common(int *, int *);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_poolmgr	*fidcPool;
extern struct psc_hashtbl	 fidcHtable;

static __inline void *
fcmh_get_pri(struct fidc_membh *f)
{
	psc_assert(f);
	return (f + 1);
}

static __inline struct fidc_membh *
fcmh_from_pri(void *p)
{
	struct fidc_membh *f = p;

	psc_assert(f);
	return (f - 1);
}

static __inline const struct fidc_membh *
fcmh_from_pri_const(const void *p)
{
	const struct fidc_membh *f = p;

	psc_assert(f);
	return (f - 1);
}

#endif /* _SL_FIDCACHE_H_ */
