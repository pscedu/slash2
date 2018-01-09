/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
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
 * The bmap (block map) interface divides the space of a file in a SLASH2
 * deployment into manageable units.  bmaps are ordered sequentially from
 * the beginning of the file space and are the fundamental elements of
 * I/O node file region ownership and in replication management.
 *
 * bmaps store information such as the state on each replicated instance
 * and are themselves subdivided into slivers which track cyclic redundancy
 * checksums for integrity and such.
 */

#ifndef _BMAP_H_
#define _BMAP_H_

#include <inttypes.h>
#include <time.h>

#include "pfl/atomic.h"
#include "pfl/crc.h"
#include "pfl/list.h"
#include "pfl/lock.h"
#include "pfl/pool.h"
#include "pfl/tree.h"

#include "cache_params.h"
#include "fid.h"
#include "slsubsys.h"
#include "sltypes.h"

struct fidc_membh;
struct srt_bmapdesc;

#define	MSL_BMAP_COUNT		64
#define	SLI_BMAP_COUNT		1024
#define	MDS_BMAP_COUNT		4096

/*
 * Basic information about bmaps shared by all MDS, IOS, and CLI.
 * @bcs_crcstates: bits describing the state of each sliver
 *	(BMAP_SLVR_DATA, etc).
 * @bcs_repls: bitmap used for tracking the replication status of this
 *	bmap (BREPLST_INVALID, etc).
 *
 * This structure must be 64-bit aligned and padded.
 */
struct bmap_core_state {
	uint8_t			bcs_crcstates[SLASH_SLVRS_PER_BMAP];
	uint8_t			bcs_repls[SL_REPLICA_NBYTES];
};

#define BMAP_SEQLOG_FACTOR	100

/**
 * bmap_extra_state - Additional fields needed by MDS.
 * @bes_crcs: the CRC table, one 8-byte CRC per sliver.
 * @bes_gen: current generation number.
 * @bes_replpol: replication policy.
 *
 * This structure must be 64-bit aligned and padded.
 */
struct bmap_extra_state {
	uint64_t		bes_crcs[SLASH_SLVRS_PER_BMAP];
	sl_bmapgen_t		bes_gen;
	uint32_t		bes_replpol;
};

/**
 * bmap_ondisk - Bmap over-wire/on-disk structure.  This structure maps
 *	the persistent state of the bmap within the inode's metafile.
 *
 *	This structure is followed by a 64-bit CRC on disk. 1184 bytes
 *	in all.
 */
struct bmap_ondisk {
	struct bmap_core_state	bod_corestate;
	struct bmap_extra_state	bod_extrastate;

/* used by SL_REPL_GET_BMAP_IOS_STAT() */

/* Some of the following are used by dumpfid.c */

#define bod_repls	bod_corestate.bcs_repls
#define bod_crcstates	bod_corestate.bcs_crcstates
#define bod_crcs	bod_extrastate.bes_crcs
#define bod_replpol	bod_extrastate.bes_replpol
#define bod_gen		bod_extrastate.bes_gen
};

/**
 * bmap - Central structure for block map caching used in all SLASH2
 *	service contexts (mds, ios, client).  The pool for this
 *	structure and its private area for each service is initialized
 *	in bmap_cache_init().
 *
 * bmap sits in the middle of the GFC stratum.
 */
struct bmap {
	sl_bmapno_t		 bcm_bmapno;	/* bmap index number */
	uint32_t		 bcm_flags;	/* see BMAP_* below */
	struct fidc_membh	*bcm_fcmh;	/* pointer to fid info */
	psc_atomic32_t		 bcm_opcnt;	/* pending opcnt (# refs) */
	psc_spinlock_t		 bcm_lock;
	RB_ENTRY(bmap)		 bcm_tentry;	/* entry in fcmh's bmap tree */
	struct psc_listentry	 bcm_lentry;	/* free pool and flush queue */
	pthread_t		 bcm_owner;	/* temporary processor */
};

#define bmapc_memb bmap

/* shared bmap_flags */
#define BMAPF_RD		(1 <<  0)	/* data is read-only */
#define BMAPF_WR		(1 <<  1)	/* data is read-write accessible */
#define BMAPF_LOADED		(1 <<  2)	/* contents are loaded */
#define BMAPF_LOADING		(1 <<  3)	/* retrieval RPC is inflight */
#define BMAPF_DIO		(1 <<  4)	/* direct I/O; no client caching allowed */
#define BMAPF_TOFREE		(1 <<  5)	/* refcnt dropped to zero, removing */
#define BMAPF_MODECHNG		(1 <<  6)	/* op mode changing (e.g. READ -> WRITE) */
#define BMAPF_WAITERS		(1 <<  7)	/* has bcm_fcmh waiters */
#define BMAPF_BUSY		(1 <<  8)	/* temporary processing lock */
#define _BMAPF_SHIFT		(1 <<  9)

#define BMAP_RW_MASK		(BMAPF_RD | BMAPF_WR)

#define bmap_2_fid(b)		fcmh_2_fid((b)->bcm_fcmh)

#define SL_MAX_IOSREASSIGN	16

/*
 * This will retry for > 20 hours.
 */
#define SL_MAX_BMAPFLSH_DELAY	10
#define SL_MAX_BMAPFLSH_RETRIES	8192

#define BMAP_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_HASLOCK(b)		psc_spin_haslock(&(b)->bcm_lock)
#define BMAP_LOCK(b)		spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b)		freelock(&(b)->bcm_lock)
#define BMAP_RLOCK(b)		reqlock(&(b)->bcm_lock)
#define BMAP_URLOCK(b, lk)	ureqlock(&(b)->bcm_lock, (lk))
#define BMAP_TRYLOCK(b)		trylock(&(b)->bcm_lock)

#define _DEBUG_BMAP_FMT		"bmap@%p bno:%u flg:%#x:"		\
				"%s%s%s%s%s%s%s%s%s%s "			\
				"fid:"SLPRI_FID" opcnt=%d : "

#define _DEBUG_BMAP_FMTARGS(b)						\
	(b), (b)->bcm_bmapno, (b)->bcm_flags,				\
	(b)->bcm_flags & BMAPF_RD	? "R" : "",			\
	(b)->bcm_flags & BMAPF_WR	? "W" : "",			\
	(b)->bcm_flags & BMAPF_LOADED	? "L" : "",			\
	(b)->bcm_flags & BMAPF_LOADING	? "l" : "",			\
	(b)->bcm_flags & BMAPF_DIO	? "D" : "",			\
	(b)->bcm_flags & BMAPF_TOFREE	? "F" : "",			\
	(b)->bcm_flags & BMAPF_MODECHNG	? "G" : "",			\
	(b)->bcm_flags & BMAPF_WAITERS	? "w" : "",			\
	(b)->bcm_flags & BMAPF_BUSY	? "B" : "",			\
	(b)->bcm_flags & ~(_BMAPF_SHIFT - 1) ? "+" : "",		\
	(b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : FID_ANY,		\
	psc_atomic32_read(&(b)->bcm_opcnt)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP, _DEBUG_BMAP_FMT fmt,		\
	    _DEBUG_BMAP_FMTARGS(b), ## __VA_ARGS__)

#define _DEBUG_BMAP(pci, level, b, fmt, ...)				\
	_psclog_pci((pci), (level), 0, _DEBUG_BMAP_FMT fmt,		\
	    _DEBUG_BMAP_FMTARGS(b), ## __VA_ARGS__)

#define bmap_wait_locked(b, cond)					\
	do {								\
		BMAP_LOCK_ENSURE(b);					\
		while (cond) {						\
			(b)->bcm_flags |= BMAPF_WAITERS;		\
			psc_waitq_wait(&(b)->bcm_fcmh->fcmh_waitq,	\
			    &(b)->bcm_lock);				\
			BMAP_LOCK(b);					\
		}							\
	} while (0)

#define bmap_wake_locked(b)						\
	do {								\
		BMAP_LOCK_ENSURE(b);					\
		if ((b)->bcm_flags & BMAPF_WAITERS) {			\
			psc_waitq_wakeall(&(b)->bcm_fcmh->fcmh_waitq);	\
			(b)->bcm_flags &= ~BMAPF_WAITERS;		\
		}							\
	} while (0)

#define BMAP_WAIT_BUSY(b)						\
	do {								\
		pthread_t _pthr = pthread_self();			\
									\
		(void)BMAP_RLOCK(b);					\
		bmap_wait_locked((b),					\
		    ((b)->bcm_flags & BMAPF_BUSY) &&			\
		    (b)->bcm_owner != _pthr);				\
		(b)->bcm_flags |= BMAPF_BUSY;				\
		(b)->bcm_owner = _pthr;					\
		DEBUG_BMAP(PLL_DEBUG, (b), "set BUSY");			\
	} while (0)

#define BMAP_UNBUSY(b)							\
	do {								\
		(void)BMAP_RLOCK(b);					\
		BMAP_BUSY_ENSURE(b);					\
		(b)->bcm_owner = 0;					\
		(b)->bcm_flags &= ~BMAPF_BUSY;				\
		DEBUG_BMAP(PLL_DEBUG, (b), "cleared BUSY");		\
		bmap_wake_locked(b);					\
		BMAP_ULOCK(b);						\
	} while (0)

#define BMAP_BUSY_ENSURE(b)						\
	do {								\
		psc_assert((b)->bcm_flags & BMAPF_BUSY);		\
		psc_assert((b)->bcm_owner == pthread_self());		\
	} while (0)

/*
 * TODO: Convert all callers to lock the bmap before start or done type.
 */
#define bmap_op_start_type(b, type)					\
	do {								\
		DEBUG_BMAP(PLL_DEBUG, (b),				\
		    "took reference (type=%u)", (type));		\
		psc_atomic32_inc(&(b)->bcm_opcnt);			\
	} while (0)

#define bmap_op_done_type(b, type)					\
	do {								\
		(void)BMAP_RLOCK(b);					\
		DEBUG_BMAP(PLL_DEBUG, (b),				\
		    "drop reference (type=%u)", (type));		\
		psc_assert(psc_atomic32_read(&(b)->bcm_opcnt) > 0);	\
		psc_atomic32_dec(&(b)->bcm_opcnt);			\
		_bmap_op_done(PFL_CALLERINFOSS(SLSS_BMAP), (b),		\
		    _DEBUG_BMAP_FMT "released reference (type=%u)",	\
		    _DEBUG_BMAP_FMTARGS(b), (type));			\
	} while (0)

#define bmap_op_done(b)		bmap_op_done_type((b), BMAP_OPCNT_LOOKUP)

#define bmap_foff(b)		((b)->bcm_bmapno * SLASH_BMAP_SIZE)

/*
 * bmap per-replica states. Note that these values have been baked into
 * the on-disk data structures. So do not change them.
 *
 * See DUMP_BMAP_REPLS() for the corresponding symbols of these flags.
 */
#define BREPLST_INVALID		0	/* no data present (zeros) */
#define BREPLST_REPL_SCHED	1	/* replica is being made */
#define BREPLST_REPL_QUEUED	2	/* replica needs to be made */
#define BREPLST_VALID		3	/* replica is active */
#define BREPLST_TRUNC_QUEUED	4	/* partial truncation in bmap */
#define BREPLST_TRUNC_SCHED	5	/* ptrunc resolving CRCs recomp */
#define BREPLST_GARBAGE_QUEUED	6	/* marked for reclamation, note */
					/* a backend might support it */
#define BREPLST_GARBAGE_SCHED	7	/* being reclaimed */
#define NBREPLST		8

/* CRC of a zeroed sliver */
#define BMAP_NULL_CRC		UINT64_C(0x436f5d7c450ed606)

#define BMAP_OD_CRCSZ		sizeof(struct bmap_ondisk)
#define BMAP_OD_SZ		(BMAP_OD_CRCSZ + sizeof(uint64_t))

/* bcs_crcstates flags */
#define BMAP_SLVR_DATA		(1 << 0)	/* Data present, otherwise slvr is hole */
#define BMAP_SLVR_CRC		(1 << 1)	/* Has valid CRC */
#define BMAP_SLVR_CRCABSENT	(1 << 3)
#define _BMAP_SLVR_FLSHFT	(1 << 4)

/*
 * Routines to get and fetch a bmap replica's status.
 * This code assumes NBREPLST is < 256
 */
#define SL_REPL_GET_BMAP_IOS_STAT(data, off)				\
	(SL_REPLICA_MASK &						\
	    (((data)[(off) / NBBY] >> ((off) % NBBY)) |			\
	    ((off) % NBBY + SL_BITS_PER_REPLICA > NBBY ?		\
	     (data)[(off) / NBBY + 1] << (SL_BITS_PER_REPLICA -		\
	      ((off) % NBBY + SL_BITS_PER_REPLICA - NBBY)) : 0)))

#define SL_REPL_SET_BMAP_IOS_STAT(data, off, val)			\
	do {								\
		int _j;							\
									\
		(data)[(off) / NBBY] = ((data)[(off) / NBBY] &		\
		    ~(SL_REPLICA_MASK << ((off) % NBBY))) |		\
		    ((val) << ((off) % NBBY));				\
		_j = (off) % NBBY + SL_BITS_PER_REPLICA - NBBY;		\
		if (_j > 0) {						\
			_j = SL_BITS_PER_REPLICA - _j;			\
			(data)[(off) / NBBY + 1] =			\
			    ((data)[(off) / NBBY + 1] &			\
			    ~(SL_REPLICA_MASK >> _j)) | ((val) >> _j);	\
		}							\
	} while (0)

/* bmap replication policies */
#define BRPOL_ONETIME		0
#define BRPOL_PERSIST		1
#define NBRPOL			2

#define DEBUG_BMAPOD(level, bmap, fmt, ...)				\
	_dump_bmapod(PFL_CALLERINFOSS(SLSS_BMAP), (level), (bmap),	\
	    (fmt), ## __VA_ARGS__)

#define DEBUG_BMAPODV(level, bmap, fmt, ap)				\
	_dump_bmapodv(PFL_CALLERINFOSS(SLSS_BMAP), (level),		\
	    (bmap), (fmt), (ap))

/* bmap_get flags */
#define BMAPGETF_NONBLOCK	(1 << 0)	/* load lease in the background */
#define BMAPGETF_CREATE		(1 << 1)	/* create a new bmap if not in cache */
#define BMAPGETF_NORETRIEVE	(1 << 2)	/* when loading, do not invoke retrievef */
#define BMAPGETF_NOAUTOINST	(1 << 3)	/* do not autoinstantiate */
#define BMAPGETF_NODISKREAD	(1 << 4)	/* do not read from disk - nothing there */
#define BMAPGETF_NODIO		(1 << 5)	/* cancel lease request if it would conjure DIO */

int	 bmap_cmp(const void *, const void *);
void	 bmap_cache_init(size_t, int, int (*)(struct psc_poolmgr *));
void	 bmap_cache_destroy(void);
void	 bmap_free_all_locked(struct fidc_membh *);
void	 bmap_biorq_waitempty(struct bmap *);
void	_bmap_op_done(const struct pfl_callerinfo *,
	    struct bmap *, const char *, ...);
int	_bmap_get(const struct pfl_callerinfo *, struct fidc_membh *,
	    sl_bmapno_t, enum rw, int, struct bmap **);
struct bmap *
	 bmap_lookup_cache(struct fidc_membh *, sl_bmapno_t, int, int *);

int	 bmapdesc_access_check(struct srt_bmapdesc *, enum rw, sl_ios_id_t);

void	 dump_bmap_repls(uint8_t *);

void	_dump_bmap_flags_common(uint32_t *, int *);

void	_dump_bmapodv(const struct pfl_callerinfo *, int,
	    struct bmap *, const char *, va_list);
void	_dump_bmapod(const struct pfl_callerinfo *, int,
	    struct bmap *, const char *, ...);

#define bmap_getf(f, n, rw, fl, bp)	_bmap_get(PFL_CALLERINFOSS(SLSS_BMAP), \
					(f), (n), (rw), (fl), (bp))

#define bmap_lookup(f, n, bp)		_bmap_get(PFL_CALLERINFOSS(SLSS_BMAP), \
					(f), (n), 0, 0, (bp))

#define bmap_get(f, n, rw, bp)		_bmap_get(PFL_CALLERINFOSS(SLSS_BMAP), \
					(f), (n), (rw), BMAPGETF_CREATE, (bp))

enum bmap_opcnt_types {
	BMAP_OPCNT_ASYNC,		/* all: asynchronous callback */
	BMAP_OPCNT_BCRSCHED,		/* all: bmap CRC update list */
	BMAP_OPCNT_BIORQ,		/* all: IO request */
	BMAP_OPCNT_BMPCE,		/* CLI: page */
	BMAP_OPCNT_FLUSH,		/* CLI: flusher queue */
	BMAP_OPCNT_LEASE,		/* MDS: bmap_lease */
	BMAP_OPCNT_LOOKUP,		/* all: bmap_get */
	BMAP_OPCNT_REAPER,		/* all: client bmap timeout */
	BMAP_OPCNT_RELEASER,		/* IOD: bmap lease relinquisher */
	BMAP_OPCNT_REPLWK,		/* IOD: repl work */
	BMAP_OPCNT_SLVR,		/* all: IOD sliver */
	BMAP_OPCNT_TRUNCWAIT,		/* CLI: waiting for ptrunc to resolve */
	BMAP_OPCNT_UPSCH,		/* all: peer update scheduler */
	BMAP_OPCNT_WORK			/* all: generic worker thread */
};

RB_HEAD(bmaptree, bmap);
RB_PROTOTYPE(bmaptree, bmap, bcm_tentry, bmap_cmp);

struct bmap_ops {
	void	(*bmo_init_privatef)(struct bmap *);
	int	(*bmo_retrievef)(struct bmap *, int);
	int	(*bmo_mode_chngf)(struct bmap *, enum rw, int);
	void	(*bmo_final_cleanupf)(struct bmap *);
};

extern struct bmap_ops sl_bmap_ops;

static __inline void *
bmap_get_pri(struct bmap *b)
{
	psc_assert(b);
	return (b + 1);
}

static __inline const void *
bmap_get_pri_const(const struct bmap *b)
{
	psc_assert(b);
	return (b + 1);
}

static __inline void
brepls_init(int *ar, int val)
{
	int i;

	for (i = 0; i < NBREPLST; i++)
		ar[i] = val;
}

static __inline void
brepls_init_idx(int *ar)
{
	int i;

	for (i = 0; i < NBREPLST; i++)
		ar[i] = i;
}

#endif /* _BMAP_H_ */
