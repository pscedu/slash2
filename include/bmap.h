/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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
 * The bmap (block map) interface divides the space of a file in a SLASH
 * network into manageable units.  bmaps are ordered sequentially from
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

#include "psc_ds/tree.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"

#include "cache_params.h"
#include "fid.h"
#include "slsubsys.h"
#include "sltypes.h"

struct fidc_membh;

/**
 * bmap_core_state
 * @bcs_crcstates: bits describing the state of each sliver.
 * @bcs_repls: bitmap used for tracking the replication status of this bmap.
 *
 * This structure must be 64-bit aligned and padded.
 */
struct bmap_core_state {
	uint8_t			bcs_crcstates[SLASH_CRCS_PER_BMAP];
	uint8_t			bcs_repls[SL_REPLICA_NBYTES];
};

/**
 * bmap_extra_state
 * @bes_crcs: the CRC table, one 8-byte CRC per sliver.
 * @bes_gen: current generation number.
 * @bes_replpol: replication policy.
 *
 * This structure must be 64-bit aligned and padded.
 */
struct bmap_extra_state {
	uint64_t		bes_crcs[SLASH_CRCS_PER_BMAP];
	sl_bmapgen_t		bes_gen;
	uint32_t		bes_replpol;
};

/**
 * bmap_ondisk - slash bmap over-wire/on-disk structure.  This
 *	structure maps the persistent state of the bmap within the
 *	inode's metafile.
 * @bod_bhcrc: on-disk checksum.
*/
struct bmap_ondisk {
	struct bmap_core_state	bod_corestate;
	struct bmap_extra_state	bod_extrastate;
	uint64_t		bod_crc;
#define bod_repls	bod_corestate.bcs_repls
#define bod_crcstates	bod_corestate.bcs_crcstates
#define bod_crcs	bod_extrastate.bes_crcs
#define bod_replpol	bod_extrastate.bes_replpol
};

/**
 * bmapc_memb - central structure for block map caching used in
 *    all slash service contexts (mds, ios, client).  The pool
 *    for this structure and its private area for each service
 *    is initialized in bmap_cache_init().
 *
 * bmapc_memb sits in the middle of the GFC stratum.
 * XXX some of these elements may need to be moved into the bcm_info_pri
 *     area (as part of new structures?) so save space on the mds.
 */
struct bmapc_memb {
	sl_bmapno_t		 bcm_bmapno;	/* bmap index number */
	struct fidc_membh	*bcm_fcmh;	/* pointer to fid info */
	psc_atomic32_t		 bcm_opcnt;	/* pending opcnt */
	uint32_t		 bcm_flags;	/* see BMAP_* below */
	psc_spinlock_t		 bcm_lock;
	SPLAY_ENTRY(bmapc_memb)	 bcm_tentry;	/* bmap_cache splay tree entry */
	struct psc_listentry	 bcm_lentry;	/* free pool */

	/*
	 * This must start on a 64-bit boundary, and must lay at the end
	 * of this structure as the bmap_{mds,iod}_info begin with the
	 * next segment of the bmap_ondisk, which must lay contiguous in
	 * memory for I/O over the network and with ZFS.
	 */
	struct bmap_core_state	 bcm_corestate;

#define bcm_crcstates	bcm_corestate.bcs_crcstates
#define bcm_repls	bcm_corestate.bcs_repls
};

/* shared bmap_flags */
#define BMAP_RD			(1 << 0)	/* XXX use enum rw */
#define BMAP_WR			(1 << 1)	/* XXX use enum rw */
#define BMAP_INIT		(1 << 2)	/* initializing from disk/network */
#define BMAP_DIO		(1 << 3)
#define BMAP_DIORQ		(1 << 4)
#define BMAP_CLOSING		(1 << 5)
#define BMAP_DIRTY		(1 << 6)
#define BMAP_MEMRLS		(1 << 7)
#define BMAP_DIRTY2LRU		(1 << 8)
#define BMAP_TIMEOQ		(1 << 9)
#define BMAP_IONASSIGN		(1 << 10)
#define BMAP_MDCHNG		(1 << 11)
#define BMAP_WAITERS		(1 << 12)	/* has bcm_fcmh waiters */
#define BMAP_ORPHAN		(1 << 13)	/* removed from fcmh_bmaptree */
#define _BMAP_FLSHFT		(1 << 14)

#define bmap_2_ondisk(b)	((struct bmap_ondisk *)&(b)->bcm_corestate)

#define BMAP_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_LOCK(b)		spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b)		freelock(&(b)->bcm_lock)
#define BMAP_RLOCK(b)		reqlock(&(b)->bcm_lock)
#define BMAP_URLOCK(b, lk)	ureqlock(&(b)->bcm_lock, (lk))
#define BMAP_TRYLOCK(b)		trylock(&(b)->bcm_lock)

#define BMAP_SETATTR(b, fl)	SETATTR_LOCKED(&(b)->bcm_lock, &(b)->bcm_flags, (fl))
#define BMAP_CLEARATTR(b, fl)	CLEARATTR_LOCKED(&(b)->bcm_lock, &(b)->bcm_flags, (fl))

#define _DEBUG_BMAP_FMT		"bmap@%p b:%x flg:%u fid:"SLPRI_FID" opcnt=%u "
#define _DEBUG_BMAP_FMTARGS(b)	(b), (b)->bcm_bmapno, (b)->bcm_flags,	\
				(b)->bcm_fcmh ?				\
				    fcmh_2_fid((b)->bcm_fcmh) : 0,	\
				psc_atomic32_read(&(b)->bcm_opcnt)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP, _DEBUG_BMAP_FMT fmt,		\
	    _DEBUG_BMAP_FMTARGS(b), ## __VA_ARGS__)

#define _DEBUG_BMAP(pci, level, b, fmt, ...)				\
	_psclog_pci((pci), (level), 0, _DEBUG_BMAP_FMT fmt,		\
	    _DEBUG_BMAP_FMTARGS(b), ## __VA_ARGS__)

#define bcm_wait_locked(b, cond)					\
	do {								\
		BMAP_LOCK_ENSURE(b);					\
		while (cond) {						\
			(b)->bcm_flags |= BMAP_WAITERS;			\
			psc_waitq_wait(&(b)->bcm_fcmh->fcmh_waitq,	\
				       &(b)->bcm_lock);			\
			BMAP_LOCK(b);					\
		}							\
	} while (0)

#define bcm_wake_locked(b)						\
	do {								\
		BMAP_LOCK_ENSURE(b);					\
		if ((b)->bcm_flags & BMAP_WAITERS) {			\
			psc_waitq_wakeall(&(b)->bcm_fcmh->fcmh_waitq);	\
			(b)->bcm_flags &= ~BMAP_WAITERS;		\
		}							\
	} while (0)

#define bmap_op_start_type(b, type)					\
	do {								\
		psc_atomic32_inc(&(b)->bcm_opcnt);			\
		DEBUG_BMAP(PLL_DEBUG, (b),				\
		    "took reference (type=%d)", (type));		\
	} while (0)

#define bmap_op_done_type(b, type)					\
	_bmap_op_done(PFL_CALLERINFOSS(SLSS_BMAP), (b),			\
	    _DEBUG_BMAP_FMT "removing reference (type=%d)",		\
	    _DEBUG_BMAP_FMTARGS(b), (type))

#define bmap_foff(b)		((b)->bcm_bmapno * SLASH_BMAP_SIZE)

/* bmap per-replica states */
#define BREPLST_INVALID		0	/* no data present */
#define BREPLST_REPL_SCHED	1	/* replica is being made */
#define BREPLST_REPL_QUEUED	2	/* replica needs to be made */
#define BREPLST_VALID		3	/* replica is active */
#define BREPLST_TRUNCPNDG	4	/* partial truncation in bmap */
#define BREPLST_TRUNCPNDG_SCHED	5	/* checksum error */
#define BREPLST_GARBAGE		6	/* marked for deletion */
#define BREPLST_GARBAGE_SCHED	7	/* being deleted */
#define NBREPLST		8

#define BMAP_NULL_CRC		UINT64_C(0x436f5d7c450ed606)

#define	BMAP_OD_SZ		(sizeof(struct bmap_ondisk))
#define	BMAP_OD_CRCSZ		(BMAP_OD_SZ - (sizeof(uint64_t)))

/* bcs_crcstates flags */
#define BMAP_SLVR_DATA		(1 << 0)	/* Data present, otherwise slvr is hole */
#define BMAP_SLVR_CRC		(1 << 1)	/* Has valid CRC */
#define BMAP_SLVR_CRCDIRTY	(1 << 2)
#define BMAP_SLVR_WANTREPL	(1 << 3)	/* Queued for replication */
#define _BMAP_SLVR_LAST		4

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
#define BRP_ONETIME		0
#define BRP_PERSIST		1
#define NBRP			2

#define DEBUG_BMAPOD(level, bmap, fmt, ...)				\
	_log_debug_bmapod(PFL_CALLERINFOSS(SLSS_BMAP), (level), (bmap),	\
	    (fmt), ## __VA_ARGS__)

#define DEBUG_BMAPODV(level, bmap, fmt, ap)				\
	_log_debug_bmapodv(PFL_CALLERINFOSS(SLSS_BMAP), (level),	\
	    (bmap), (fmt), (ap))

/* bmap_get flags */
#define BMAPGETF_LOAD		(1 << 0)	/* allow loading if not in cache */
#define BMAPGETF_NORETRIEVE	(1 << 1)	/* when loading, do not invoke retrievef */

int	 bmap_cmp(const void *, const void *);
void	 bmap_cache_init(size_t);
void	 bmap_orphan(struct bmapc_memb *);
void	 bmap_biorq_waitempty(struct bmapc_memb *);
void	_bmap_op_done(const struct pfl_callerinfo *,
	    struct bmapc_memb *, const char *, ...);
int	 bmap_getf(struct fidc_membh *, sl_bmapno_t, enum rw, int,
	    struct bmapc_memb **);

void	_log_debug_bmapodv(const struct pfl_callerinfo *, int,
	    struct bmapc_memb *, const char *, va_list);
void	_log_debug_bmapod(const struct pfl_callerinfo *, int,
	    struct bmapc_memb *, const char *, ...);

#define bmap_lookup(f, n, bp)		bmap_getf((f), (n), 0, 0, (bp))
#define bmap_get(f, n, rw, bp)		bmap_getf((f), (n), (rw),	\
					    BMAPGETF_LOAD, (bp))

#define bmap_get_noretr(f, n, rw, bp)	bmap_getf((f), (n), (rw),	\
					  BMAPGETF_LOAD | BMAPGETF_NORETRIEVE, (bp))

enum bmap_opcnt_types {
/*  0 */ BMAP_OPCNT_LOOKUP,
/*  1 */ BMAP_OPCNT_IONASSIGN,
/*  2 */ BMAP_OPCNT_LEASE,
/*  3 */ BMAP_OPCNT_MDSLOG,
/*  4 */ BMAP_OPCNT_BIORQ,
/*  5 */ BMAP_OPCNT_REPLWK,		/* ION */
/*  6 */ BMAP_OPCNT_REAPER,		/* Client bmap timeout */
/*  7 */ BMAP_OPCNT_COHCB,		/* MDS coherency callback */
/*  8 */ BMAP_OPCNT_SLVR,
/*  9 */ BMAP_OPCNT_BCRSCHED,
/* 10 */ BMAP_OPCNT_RLSSCHED,
/* 11 */ BMAP_OPCNT_TRUNCWAIT
};

SPLAY_HEAD(bmap_cache, bmapc_memb);
SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

struct bmap_ops {
	void	(*bmo_init_privatef)(struct bmapc_memb *);
	int	(*bmo_retrievef)(struct bmapc_memb *, enum rw);
	int	(*bmo_mode_chngf)(struct bmapc_memb *, enum rw);
	void	(*bmo_final_cleanupf)(struct bmapc_memb *);
};

extern struct bmap_ops bmap_ops;

static __inline void *
bmap_get_pri(struct bmapc_memb *bcm)
{
	return (bcm + 1);
}

static __inline const void *
bmap_get_pri_const(const struct bmapc_memb *bcm)
{
	return (bcm + 1);
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

static __inline void
_dump_bmap_flags(uint32_t *flags, int *seq)
{
	PFL_PRFLAG(BMAP_RD, flags, seq);
	PFL_PRFLAG(BMAP_WR, flags, seq);
	PFL_PRFLAG(BMAP_INIT, flags, seq);
	PFL_PRFLAG(BMAP_DIO, flags, seq);
	PFL_PRFLAG(BMAP_DIORQ, flags, seq);
	PFL_PRFLAG(BMAP_CLOSING, flags, seq);
	PFL_PRFLAG(BMAP_DIRTY, flags, seq);
	PFL_PRFLAG(BMAP_MEMRLS, flags, seq);
	PFL_PRFLAG(BMAP_DIRTY2LRU, flags, seq);
	PFL_PRFLAG(BMAP_TIMEOQ, flags, seq);
	PFL_PRFLAG(BMAP_IONASSIGN, flags, seq);
	PFL_PRFLAG(BMAP_MDCHNG, flags, seq);
	PFL_PRFLAG(BMAP_WAITERS, flags, seq);
	PFL_PRFLAG(BMAP_ORPHAN, flags, seq);
}

#endif /* _BMAP_H_ */
