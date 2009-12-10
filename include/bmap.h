/* $Id$ */

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
 *    all slash service contexts (mds, ios, client). It is allocated
 *    by bmap_lookup_add().
 *
 * bmapc_memb sits in the middle of the GFC stratum.
 * XXX some of these elements may need to be moved into the bcm_info_pri
 *     area (as part of new structures?) so save space on the mds.
 */
struct bmapc_memb {
	sl_blkno_t		 bcm_blkno;	/* Bmap blk number        */
	struct fidc_membh	*bcm_fcmh;	/* pointer to fid info    */
	atomic_t		 bcm_rd_ref;	/* one ref per write fd    */
	atomic_t		 bcm_wr_ref;	/* one ref per read fd     */
	atomic_t		 bcm_opcnt;	/* pending opcnt           */
	uint32_t		 bcm_mode;
	psc_spinlock_t		 bcm_lock;
	struct psc_waitq	 bcm_waitq;     /* XXX think about replacing
						   me with bcm_fcmh->fcmh_waitq
						*/
	SPLAY_ENTRY(bmapc_memb)	 bcm_tentry;	/* bmap_cache splay tree entry    */
	struct psclist_head	 bcm_lentry;	/* free pool */
	void			*bcm_pri;	/* bmap_mds_info, bmap_cli_info, or bmap_iod_info */
};

/* common bmap modes */
#define BMAP_RD			(1 << 0)
#define BMAP_WR			(1 << 1)
#define BMAP_INIT		(1 << 2)	/* contents need to be loaded from disk or over the network */
#define BMAP_INFLIGHT		(1 << 3)	/* contents are being loaded, please wait */
#define BMAP_DIO		(1 << 4)
#define BMAP_CLOSING		(1 << 5)
#define BMAP_DIRTY		(1 << 6)
#define BMAP_MEMRLS		(1 << 7)
#define BMAP_DIRTY2LRU		(1 << 8)
#define BMAP_LOAD_FAIL		(1 << 9)
#define _BMAP_FLSHFT		(1 << 10)

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

#define DEBUG_BMAP(level, b, fmt, ...)					\
	psc_logs((level), PSS_GEN,					\
		 " bmap@%p b:%x m:%u i:%"PRIx64				\
		 " rref=%u wref=%u opcnt=%u "fmt,			\
		 (b), (b)->bcm_blkno,					\
		 (b)->bcm_mode,						\
		 (b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0,		\
		 atomic_read(&(b)->bcm_rd_ref),				\
		 atomic_read(&(b)->bcm_wr_ref),				\
		 atomic_read(&(b)->bcm_opcnt),				\
		 ## __VA_ARGS__)

int  bmapc_cmp(const void *, const void *);
void bmap_op_done(struct bmapc_memb *);
void bmap_remove(struct bmapc_memb *);
int  bmap_try_release_locked(struct bmapc_memb *);
void bmap_try_release(struct bmapc_memb *);
struct bmapc_memb * bmap_lookup_locked(struct fidc_open_obj *, sl_blkno_t);
struct bmapc_memb * bmap_lookup(struct fidc_membh *, sl_blkno_t);
struct bmapc_memb * bmap_lookup_add(struct fidc_membh *, sl_blkno_t,
				    void (*)(struct bmapc_memb *));

SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

extern struct psc_poolmaster	 bmap_poolmaster;
extern struct psc_poolmgr	*bmap_pool;

#endif /* _BMAP_H_ */
