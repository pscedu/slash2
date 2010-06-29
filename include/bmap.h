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
#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"
#include "psc_util/waitq.h"

#include "cache_params.h"
#include "slashrpc.h"
#include "fid.h"
#include "fidcache.h"

struct fidc_membh;

/*
 * bmapc_memb - central structure for block map caching used in
 *    all slash service contexts (mds, ios, client).
 *
 * bmapc_memb sits in the middle of the GFC stratum.
 * XXX some of these elements may need to be moved into the bcm_info_pri
 *     area (as part of new structures?) so save space on the mds.
 */
struct bmapc_memb {
	sl_bmapno_t		 bcm_bmapno;	/* bmap index number        */
	struct fidc_membh	*bcm_fcmh;	/* pointer to fid info    */
	psc_atomic32_t		 bcm_opcnt;	/* pending opcnt           */
	uint32_t		 bcm_mode;	/* see flags below */
	psc_spinlock_t		 bcm_lock;
	SPLAY_ENTRY(bmapc_memb)	 bcm_tentry;	/* bmap_cache splay tree entry    */
	struct psclist_head	 bcm_lentry;	/* free pool */
	struct srt_bmap_wire	*bcm_od;	/* on-disk representation */
	void			*bcm_pri;	/* bmap_mds_info, bmap_cli_info, or bmap_iod_info */
#define bcm_blkno bcm_bmapno
};

/* common bmap modes */
#define BMAP_RD			(1 << 0)	/* XXX rename this to diassociate SL_READ */
#define BMAP_WR			(1 << 1)	/* XXX rename this to diassociate SL_WRITE */
#define BMAP_INIT		(1 << 2)	/* initializing from disk/network */
#define BMAP_DIO		(1 << 3)
#define BMAP_CLOSING		(1 << 4)
#define BMAP_DIRTY		(1 << 5)
#define BMAP_MEMRLS		(1 << 6)
#define BMAP_DIRTY2LRU		(1 << 7)
#define BMAP_REAPABLE		(1 << 8)
#define BMAP_IONASSIGN		(1 << 9)
#define BMAP_MDCHNG             (1 << 10)
#define _BMAP_FLSHFT		(1 << 11)

#define BMAP_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_LOCK(b)		spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b)		freelock(&(b)->bcm_lock)
#define BMAP_RLOCK(b)		reqlock(&(b)->bcm_lock)
#define BMAP_URLOCK(b, lk)	ureqlock(&(b)->bcm_lock, (lk))

#define bcm_wait_locked(b, cond)					\
	do {								\
		BMAP_LOCK_ENSURE(b);					\
		while (cond) {						\
			psc_waitq_wait(&(b)->bcm_fcmh->fcmh_waitq,	\
				       &(b)->bcm_lock);			\
			BMAP_LOCK(b);					\
		}							\
	} while (0)

#define bcm_wake_locked(b) do {						\
		BMAP_LOCK_ENSURE(b);					\
		psc_waitq_wakeall(&(b)->bcm_fcmh->fcmh_waitq);		\
	} while (0)

#define _DEBUG_BMAP(file, func, line, level, b, fmt, ...)		\
	psclog((file), (func), (line), PSS_GEN, (level), 0,		\
	       "bmap@%p b:%x m:%u i:%"PRId64" opcnt=%u "fmt,		\
	    (b), (b)->bcm_blkno, (b)->bcm_mode,				\
	    (b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0,		\
	    atomic_read(&(b)->bcm_opcnt),				\
	    ## __VA_ARGS__)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	_DEBUG_BMAP(__FILE__, __func__, __LINE__, (level), (b), fmt,	\
	    ## __VA_ARGS__)


/* per-replica states */
#define SL_REPLST_INACTIVE	0
#define SL_REPLST_SCHED		1
#define SL_REPLST_OLD		2
#define SL_REPLST_ACTIVE	3
#define SL_REPLST_TRUNCPNDG	4
#define SL_REPLST_GARBAGE	5
#define SL_REPLST_GARBAGE_SCHED	6
#define SL_NREPLST		7

#define slash_bmap_od		srt_bmap_wire

#define	BMAP_OD_SZ		(sizeof(struct slash_bmap_od))
#define	BMAP_OD_CRCSZ		(BMAP_OD_SZ - (sizeof(psc_crc64_t)))

/* Currently, 8 bits are available for flags. */
enum {
	BMAP_SLVR_DATA		= (1 << 0),	/* Data present, otherwise slvr is hole */
	BMAP_SLVR_CRC		= (1 << 1),	/* Valid CRC */
	BMAP_SLVR_WANTREPL	= (1 << 2)	/* Queued for replication */
};

/*
 * Routines to get and fetch a bmap replica's status.
 * This code assumes SL_NREPLST is < 256 !
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

static __inline void
_log_debug_bmapodv(const char *file, const char *func, int lineno,
    int level, struct bmapc_memb *bmap, const char *fmt, va_list ap)
{
	unsigned char *b = bmap->bcm_od->bh_repls;
	char mbuf[LINE_MAX], rbuf[SL_MAX_REPLICAS + 1];
	int off, k, ch[SL_NREPLST];

	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);

	ch[SL_REPLST_INACTIVE] = '-';
	ch[SL_REPLST_SCHED] = 's';
	ch[SL_REPLST_OLD] = 'o';
	ch[SL_REPLST_ACTIVE] = '+';
	ch[SL_REPLST_TRUNCPNDG] = 't';
	ch[SL_REPLST_GARBAGE] = 'g';

	for (k = 0, off = 0; k < SL_MAX_REPLICAS;
	    k++, off += SL_BITS_PER_REPLICA)
		rbuf[k] = ch[SL_REPL_GET_BMAP_IOS_STAT(b, off)];
	rbuf[k] = '\0';

	_DEBUG_BMAP(file, func, lineno, level, bmap, "pol %d repls=[%s] %s",
	    bmap->bcm_od->bh_repl_policy, rbuf, mbuf);
}

static __inline void
_log_debug_bmapod(const char *file, const char *func, int lineno,
    int level, struct bmapc_memb *bmap, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	_log_debug_bmapodv(file, func, lineno, level, bmap, fmt, ap);
	va_end(ap);
}

#define DEBUG_BMAPOD(level, bmap, fmt, ...)				\
	_log_debug_bmapod(__FILE__, __func__, __LINE__, (level),	\
	    (bmap), (fmt), ## __VA_ARGS__)

#define DEBUG_BMAPODV(level, bmap, fmt, ap)				\
	_log_debug_bmapodv(__FILE__, __func__, __LINE__, (level),	\
	    (bmap), (fmt), (ap))

/* bmap_get flags */
#define BMAPGETF_LOAD		(1 << 0)	/* allow loading if not in cache */
#define BMAPGETF_NORETRIEVE	(1 << 1)	/* when loading, do not invoke retrievef */

int	 bmap_cmp(const void *, const void *);
void	 bmap_cache_init(size_t);
void	_bmap_op_done(struct bmapc_memb *);
int	 bmap_getf(struct fidc_membh *, sl_bmapno_t, enum rw, int,
	    struct bmapc_memb **);

int	 bmapdesc_access_check(struct srt_bmapdesc *, enum rw,
	    sl_ios_id_t, lnet_nid_t);

#define bmap_lookup(f, n, bp)		bmap_getf((f), (n), 0, 0, (bp))
#define bmap_get(f, n, rw, bp)		bmap_getf((f), (n), (rw),	\
					    BMAPGETF_LOAD, (bp))

#define bmap_get_noretr(f, n, rw, bp)	bmap_getf((f), (n), (rw),	\
					  BMAPGETF_LOAD | BMAPGETF_NORETRIEVE, (bp))

#define bmap_op_start_type(b, type)					\
	do {								\
		atomic_inc(&(b)->bcm_opcnt);				\
		DEBUG_BMAP(PLL_NOTIFY, (b),				\
		    "took reference (type=%d)", (type));		\
	} while (0)

#define bmap_op_start(b)						\
	do {								\
		atomic_inc(&(b)->bcm_opcnt);				\
		DEBUG_BMAP(PLL_NOTIFY, (b), "took reference");		\
	} while (0)

#define bmap_op_done(b)							\
	do {								\
		DEBUG_BMAP(PLL_NOTIFY, (b), "removing reference");	\
		_bmap_op_done(b);					\
	} while (0)

#define bmap_op_done_type(b, type)					\
	do {								\
		DEBUG_BMAP(PLL_NOTIFY, (b),				\
		    "removing reference (type=%d)", (type));		\
		_bmap_op_done(b);					\
	} while (0)

#define bmap_op_done_type_simple(b, type)				\
	do {								\
		psc_assert(atomic_read(&(b)->bcm_opcnt) > 1);		\
		atomic_dec(&(b)->bcm_opcnt);				\
		DEBUG_BMAP(PLL_NOTIFY, (b),				\
		    "removed reference (type=%d)", (type));		\
	} while (0)

enum bmap_opcnt_types {
	/* 0 */ BMAP_OPCNT_LOOKUP,
	/* 1 */ BMAP_OPCNT_IONASSIGN,
	/* 2 */ BMAP_OPCNT_LEASE,
	/* 3 */ BMAP_OPCNT_MDSLOG,
	/* 4 */ BMAP_OPCNT_BIORQ,
	/* 5 */ BMAP_OPCNT_REPLWK,		/* ION */
	/* 6 */ BMAP_OPCNT_REAPER,		/* Client bmap timeout */
	/* 7 */ BMAP_OPCNT_COHCB,		/* MDS coherency callback */
	/* 8 */ BMAP_OPCNT_SLVR,
	/* 9 */ BMAP_OPCNT_BCRSCHED,
	/* 10*/ BMAP_OPCNT_RLSSCHED
};

SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

struct bmap_ops {
	void	(*bmo_init_privatef)(struct bmapc_memb *);
	int	(*bmo_retrievef)(struct bmapc_memb *, enum rw);
	int	(*bmo_mode_chngf)(struct bmapc_memb *, enum rw);
	void	(*bmo_final_cleanupf)(struct bmapc_memb *);
};

extern struct bmap_ops bmap_ops;

#endif /* _BMAP_H_ */
