/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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

struct fidc_membh;

struct sl_fcmh_ops {
	int	(*sfop_ctor)(struct fidc_membh *, int);
	void	(*sfop_dtor)(struct fidc_membh *);
	int	(*sfop_getattr)(struct fidc_membh *, void *);
	int	(*sfop_reopen)(struct fidc_membh *, slfgen_t);
};

/*
 * The primary in-memory file structure; all updates and lookups into the
 * inode are done through here.
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
	struct psclist_head	 fcmh_lentry;	/* idle or free list */
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
#define FCMH_BUSY		(1 <<  7)	/* fcmh being processed */
#define FCMH_DELETED		(1 <<  8)	/* fcmh has been deleted */
#define _FCMH_FLGSHFT		(1 <<  9)

/* number of seconds in which attribute times out */
#define FCMH_ATTR_TIMEO		30

#define FCMH_PCI		PFL_CALLERINFOSS(SLSS_FCMH)

#define FCMH_LOCK(f)		spinlock_pci(FCMH_PCI, &(f)->fcmh_lock)
#define FCMH_ULOCK(f)		freelock_pci(FCMH_PCI, &(f)->fcmh_lock)
#define FCMH_TRYLOCK(f)		trylock_pci(FCMH_PCI, &(f)->fcmh_lock)
#define FCMH_TRYRLOCK(f, lk)	tryreqlock_pci(FCMH_PCI, &(f)->fcmh_lock, (lk))
#define FCMH_RLOCK(f)		reqlock_pci(FCMH_PCI, &(f)->fcmh_lock)
#define FCMH_URLOCK(f, lk)	ureqlock_pci(FCMH_PCI, &(f)->fcmh_lock, (lk))
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

#define FCMH_WAIT_BUSY(f, unlock)					\
	do {								\
		pthread_t _pthr = pthread_self();			\
		FCMH_LOCK_ENSURE((f));					\
		psc_assert(!FCMH_HAS_BUSY((f)));			\
		fcmh_wait_locked((f), (f)->fcmh_flags & FCMH_BUSY);	\
		(f)->fcmh_flags |= FCMH_BUSY;				\
		(f)->fcmh_owner = _pthr;				\
		(f)->fcmh_lineno = __LINE__;				\
		(f)->fcmh_fn = __FILE__;				\
		DEBUG_FCMH(PLL_DIAG, (f), "set BUSY");			\
		if (unlock)						\
			 FCMH_ULOCK((f));				\
	} while (0)

#define FCMH_UNBUSY(f, lock)						\
	do {								\
		if (lock)						\
			FCMH_LOCK((f));					\
		else							\
			FCMH_LOCK_ENSURE((f));				\
		psc_assert(FCMH_HAS_BUSY(f));				\
		(f)->fcmh_owner = 0;					\
		(f)->fcmh_flags &= ~FCMH_BUSY;				\
		DEBUG_FCMH(PLL_DIAG, (f), "clear BUSY");		\
		fcmh_wake_locked((f));					\
		if (lock)						\
			FCMH_ULOCK((f));				\
	} while (0)

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
	    "flg=%#x:%s%s%s%s%s%s%s%s%s%s "				\
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
	    (f)->fcmh_flags & FCMH_BUSY			? "S" : "",	\
	    (f)->fcmh_flags & FCMH_DELETED		? "D" : "",	\
	    (f)->fcmh_flags & ~(_FCMH_FLGSHFT - 1)	? "+" : "",	\
	    (f)->fcmh_refcnt, fcmh_2_fsz(f), (f)->fcmh_sstb.sst_blksize,\
	    (f)->fcmh_sstb.sst_mode, ## __VA_ARGS__)

/* types of references */
#define FCMH_OPCNT_BMAP			 0	/* all: bcm_fcmh backpointer */
#define FCMH_OPCNT_LOOKUP_FIDC		 1	/* all: fidc_lookup() */
#define FCMH_OPCNT_NEW			 2	/* all: early initialization */
#define FCMH_OPCNT_OPEN			 3	/* CLI: pscfs file info */
#define FCMH_OPCNT_WAIT			 4	/* all: dup ref during initialization */
#define FCMH_OPCNT_WORKER		 5	/* MDS: generic worker */
#define FCMH_OPCNT_DIRTY_QUEUE		 6	/* CLI: attribute flushing */
#define FCMH_OPCNT_UPSCH		 7	/* MDS: temporarily held by upsch engine */
#define FCMH_OPCNT_READDIR		 8	/* CLI: readahead readdir RPC */
#define FCMH_OPCNT_READAHEAD		 9	/* IOD/CLI: readahead */
#define FCMH_OPCNT_DIRCACHE		10	/* CLI: async dircache */
#define FCMH_OPCNT_SYNC_AHEAD		11	/* IOD: sync ahead */
#define FCMH_OPCNT_UPDATE		12	/* IOD: update file */
#define FCMH_OPCNT_MAXTYPE		13

void	fidc_init(int);
void	fidc_destroy(void);
int	fidc_reap(int, int);

#define MAX_FCMH_LIFETIME		60

#define FCMH_MAX_REAP			32

#define SL_FIDC_REAPF_EXPIRED		(1 << 0)
#define SL_FIDC_REAPF_ROOT		(1 << 1)

void	sl_freapthr_spawn(int, const char *);

/* fidc_lookup() flags */
#define FIDC_LOOKUP_CREATE		(1 << 0)	/* create if not present */
#define FIDC_LOOKUP_LOAD		(1 << 1)	/* use external fetching mechanism */
#define FIDC_LOOKUP_EXCL		(1 << 2)	/* ensure that this call creates */

int	_fidc_lookup(const struct pfl_callerinfo *, slfid_t, slfgen_t,
	    int, struct fidc_membh **, void *);

#define sl_fcmh_lookup(fid, fgen, flags, fp, arg)			\
	_fidc_lookup(FCMH_PCI, (fid), (fgen), (flags), (fp), (arg))

#define sl_fcmh_lookup_fg(fg, flags, fp)				\
	sl_fcmh_lookup((fg)->fg_fid, (fg)->fg_gen, (flags), (fp), NULL)

#define sl_fcmh_get_fg(fgp, fp)						\
	sl_fcmh_lookup_fg((fgp), FIDC_LOOKUP_CREATE, (fp))

#define sl_fcmh_load(fid, fgen, fp)					\
	sl_fcmh_lookup((fid), (fgen), FIDC_LOOKUP_CREATE |		\
	    FIDC_LOOKUP_LOAD, (fp), NULL)
#define sl_fcmh_load_fg(fg, fp)						\
	sl_fcmh_load((fg)->fg_fid, (fg)->fg_gen, (fp))
#define sl_fcmh_load_fid(fid, fp)					\
	sl_fcmh_load((fid), FGEN_ANY, (fp))

#define sl_fcmh_peek(fid, fgen, fp)					\
	sl_fcmh_lookup((fid), (fgen), 0, (fp), NULL)
#define sl_fcmh_peek_fg(fg, fp)						\
	sl_fcmh_peek((fg)->fg_fid, (fg)->fg_gen, (fp))
#define sl_fcmh_peek_fid(fid, fp)					\
	sl_fcmh_peek((fid), FGEN_ANY, (fp))

ssize_t	 fcmh_getsize(struct fidc_membh *);

void	fcmh_op_start_type(struct fidc_membh *, int);
void	fcmh_op_done_type(struct fidc_membh *, int);

#define fcmh_op_done(f)							\
    fcmh_op_done_type((f), FCMH_OPCNT_LOOKUP_FIDC)

void	_dump_fcmh_flags_common(int *, int *);

extern struct sl_fcmh_ops	 sl_fcmh_ops;
extern struct psc_hashtbl	 sl_fcmh_hashtbl;
extern struct psc_listcache	 sl_fcmh_idle;
extern struct psc_poolmgr	*sl_fcmh_pool;
extern struct psc_thread	*sl_freapthr;
extern struct psc_waitq		 sl_freap_waitq;

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
