/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/lock.h"
#include "psc_util/waitq.h"

#include "fidcache.h"
#include "fid.h"

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
	sl_blkgen_t	bh_gen;
	sl_gcrc_t	bh_crcs[SL_CRCS_PER_BMAP];
	uint8_t		bh_crcstates[SL_CRCS_PER_BMAP];
	uint8_t		bh_repls[SL_REPLICA_NBYTES];

	/* the CRC must be at the end */
	psc_crc64_t	bh_bhcrc;
};

#define	BMAP_OD_SZ	(sizeof(struct slash_bmap_od))
#define	BMAP_OD_CRCSZ	(BMAP_OD_SZ - (sizeof(psc_crc64_t)))

enum slash_bmap_slv_states {
	BMAP_SLVR_DATA = (1<<0), /* Data present, otherwise slvr is hole */
	BMAP_SLVR_CRC  = (1<<1)  /* Valid CRC */
	//XXX ATM, 6 bits are left
};

/* get a replica's bmap replication status */
#define SL_REPL_GET_BMAP_IOS_STAT(data, off)				\
	((((data)[(off) / NBBY] >> ((off) % NBBY)) |			\
	  ((off) % NBBY + SL_BITS_PER_REPLICA > NBBY ? 0 :		\
	   (data)[(off) / NBBY + 1] <<					\
	    (NBBY - (off) % NBBY))) & SL_REPLICA_MASK)

/* set a replica's bmap replication status */
#define SL_REPL_SET_BMAP_IOS_STAT(data, off, val)			\
	((data)[(off) / NBBY] = ((data)[(off) / NBBY] &			\
	    ~(SL_REPLICA_MASK << ((off) % NBBY))) |			\
	    ((val) << ((off) % NBBY)))

/* bmap replication policies */
#define BRP_ONETIME		0
#define BRP_PERSIST		1
#define NBRP			2

#define _DEBUG_BMAP(file, func, line, level, b, fmt, ...)		\
	psclog((file), (func), (line), PSS_GEN, (level), 0,		\
	    "bmap@%p b:%x m:%u i:%"PRIx64				\
	    " rref=%u wref=%u opcnt=%u "fmt,				\
	    (b), (b)->bcm_blkno,					\
	    (b)->bcm_mode,						\
	    (b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0,		\
	    atomic_read(&(b)->bcm_rd_ref),				\
	    atomic_read(&(b)->bcm_wr_ref),				\
	    atomic_read(&(b)->bcm_opcnt),				\
	    ## __VA_ARGS__)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	_DEBUG_BMAP(__FILE__, __func__, __LINE__, (level), (b), fmt,	\
	    ## __VA_ARGS__)

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
	BMAP_OPCNT_BIORQ
};

SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

extern void	(*bmap_init_privatef)(struct bmapc_memb *);
extern int	(*bmap_retrievef)(struct bmapc_memb *, enum rw, void *);
extern void	(*bmap_final_cleanupf)(struct bmapc_memb *);

#endif /* _BMAP_H_ */
