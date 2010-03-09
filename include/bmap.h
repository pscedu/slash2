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

#include "fid.h"
#include "fidcache.h"

struct fidc_membh;
struct fidc_open_obj;

struct bmap_refresh {
	struct slash_fidgen	bmrfr_fg;
	sl_blkno_t		bmrfr_blk;
	uint8_t			bmrfr_flags;
};

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
	atomic_t		 bcm_rd_ref;	/* one ref per write fd    */
	atomic_t		 bcm_wr_ref;	/* one ref per read fd     */
	atomic_t		 bcm_opcnt;	/* pending opcnt           */
	uint32_t		 bcm_mode;	/* see flags below */
	psc_spinlock_t		 bcm_lock;
	struct psc_waitq	 bcm_waitq;     /* XXX think about replacing
						   me with bcm_fcmh->fcmh_waitq
						*/
	SPLAY_ENTRY(bmapc_memb)	 bcm_tentry;	/* bmap_cache splay tree entry    */
	struct psclist_head	 bcm_lentry;	/* free pool */
	struct slash_bmap_od	*bcm_od;	/* on-disk representation */
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
#define _BMAP_FLSHFT		(1 << 8)

#define BMAP_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_LOCK(b)		spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b)		freelock(&(b)->bcm_lock)
#define BMAP_RLOCK(b)		reqlock(&(b)->bcm_lock)
#define BMAP_URLOCK(b, lk)	ureqlock(&(b)->bcm_lock, (lk))

#define _DEBUG_BMAP(file, func, line, level, b, fmt, ...)		\
	psclog((file), (func), (line), PSS_GEN, (level), 0,		\
	    "bmap@%p b:%x m:%u i:%"PRIx64				\
	    " rref=%u wref=%u opcnt=%u "fmt,				\
	    (b), (b)->bcm_blkno, (b)->bcm_mode,				\
	    (b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0,		\
	    atomic_read(&(b)->bcm_rd_ref),				\
	    atomic_read(&(b)->bcm_wr_ref),				\
	    atomic_read(&(b)->bcm_opcnt),				\
	    ## __VA_ARGS__)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	_DEBUG_BMAP(__FILE__, __func__, __LINE__, (level), (b), fmt,	\
	    ## __VA_ARGS__)

/*
 *   Each bmapod uses a char array as a bitmap to track which
 *   stores the bmap is replicated to.
 */
#define SL_BITS_PER_REPLICA	3
#define SL_REPLICA_MASK		((uint8_t)((1 << SL_BITS_PER_REPLICA) - 1))

/* must be 64-bit aligned */
#define SL_REPLICA_NBYTES	((SL_MAX_REPLICAS * SL_BITS_PER_REPLICA) / NBBY)

#define SL_BMAP_SIZE		SLASH_BMAP_SIZE
#define SL_BMAP_CRCSIZE		(1024 * 1024)
#define SL_CRCS_PER_BMAP	(SL_BMAP_SIZE / SL_BMAP_CRCSIZE)	/* must be 64-bit aligned in bytes */

/* per-replica states */
#define SL_REPLST_INACTIVE	0
#define SL_REPLST_SCHED		1
#define SL_REPLST_OLD		2
#define SL_REPLST_ACTIVE	3
#define SL_REPLST_TRUNCPNDG	4
#define SL_NREPLST		5

/*
 * Associate a CRC with a generation ID for a block.
 */
typedef struct slash_gencrc {
	psc_crc64_t		gc_crc;
} sl_gcrc_t;

struct slash_bmap_cli_wire {
	uint8_t			bw_crcstates[SL_CRCS_PER_BMAP];
	uint8_t			bw_repls[SL_REPLICA_NBYTES];
} __packed;

/**
 * slash_bmap_od - slash bmap over-wire/on-disk structure.  This
 *	structure maps the persistent state of the bmap within the
 *	inode's metafile.
 * @bh_gen: current generation number.
 * @bh_crcs: the crc table, one 8 byte crc per sliver.
 * @bh_crcstates: some bits for describing the state of a sliver.
 * @bh_repls: bitmap used for tracking the replication status of this bmap.
 * @bh_bhcrc: on-disk checksum.
 */
#define slash_bmap_wire slash_bmap_od
struct slash_bmap_od {
	sl_gcrc_t		bh_crcs[SL_CRCS_PER_BMAP];
	uint8_t			bh_crcstates[SL_CRCS_PER_BMAP];
	uint8_t			bh_repls[SL_REPLICA_NBYTES];
	sl_blkgen_t		bh_gen;
	uint32_t		bh_repl_policy;

	/* the CRC must be at the end */
	psc_crc64_t		bh_bhcrc;
};

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
#define BMAPGETF_LOAD	(1 << 0)		/* allow loading if not in cache */

int	 bmap_cmp(const void *, const void *);
void	 bmap_cache_init(size_t);
void	_bmap_op_done(struct bmapc_memb *);
int	_bmap_get(struct fidc_membh *, sl_blkno_t, enum rw, int,
	    struct bmapc_memb **, void *);

#define bmap_lookup(f, n, bp)		_bmap_get((f), (n), 0, 0, (bp), NULL)
#define bmap_get(f, n, rw, bp, arg)	_bmap_get((f), (n), (rw),	\
					    BMAPGETF_LOAD, (bp), (arg))

#define bmap_op_start_type(b, type)					\
	do {								\
		atomic_inc(&(b)->bcm_opcnt);				\
		DEBUG_BMAP(PLL_NOTIFY, (b), "took reference (type=%d)", type); \
	} while (0)

#define bmap_op_start(b)						\
	do {								\
		atomic_inc(&(b)->bcm_opcnt);				\
		DEBUG_BMAP(PLL_NOTIFY, (b), "took reference");		\
	} while (0)

#define bmap_op_done(b)							\
	do {								\
		DEBUG_BMAP(PLL_NOTIFY, (b), "removing reference");	\
		_bmap_op_done((b));					\
	} while (0)

#define bmap_op_done_type(b, type)					\
	do {								\
		DEBUG_BMAP(PLL_NOTIFY, (b), "removing reference (type=%d)", type); \
		_bmap_op_done((b));					\
	} while (0)

enum bmap_opcnt_types {
	BMAP_OPCNT_LOOKUP,
	BMAP_OPCNT_IONASSIGN,
	BMAP_OPCNT_BREF,
	BMAP_OPCNT_MDSLOG,
	BMAP_OPCNT_BIORQ,
	BMAP_OPCNT_REPLWK			/* ION */
};

SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

extern void	(*bmap_init_privatef)(struct bmapc_memb *);
extern int	(*bmap_retrievef)(struct bmapc_memb *, enum rw, void *);
extern void	(*bmap_final_cleanupf)(struct bmapc_memb *);

#endif /* _BMAP_H_ */
