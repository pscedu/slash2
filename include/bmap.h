/* $Id$ */

#ifndef _BMAP_H_
#define _BMAP_H_

#include <inttypes.h>
#include <time.h>

#include "psc_types.h"
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
	u8			bmrfr_flags;
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
	sl_blkno_t		 bcm_blkno;	/* Bmap blk number        */
	struct fidc_membh	*bcm_fcmh;	/* pointer to fid info    */
	atomic_t		 bcm_rd_ref;	/* one ref per write fd    */
	atomic_t		 bcm_wr_ref;	/* one ref per read fd     */
	struct timespec		 bcm_ts;
	atomic_t		 bcm_opcnt;	/* pending opcnt           */
	u32			 bcm_mode;
	psc_spinlock_t		 bcm_lock;
	psc_waitq_t		 bcm_waitq;     /* XXX think about replacing 
						   me with bcm_fcmh->fcmh_waitq
						*/
	SPLAY_ENTRY(bmapc_memb)	 bcm_tentry;	/* fcm tree entry    */
	void			*bcm_pri;
};

enum bmap_common_modes {
	BMAP_RD      = (1 << 0),
	BMAP_WR      = (1 << 1),
	BMAP_INIT    = (1 << 2),
	BMAP_DIO     = (1 << 3),
	BMAP_CLOSING = (1 << 4)
};

#define BMAP_RSVRD_MODES 5 /* highest bmap_common_mode + 1 */

#define BMAP_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bcm_lock)
#define BMAP_LOCK(b)		spinlock(&(b)->bcm_lock)
#define BMAP_ULOCK(b)		freelock(&(b)->bcm_lock)
#define BMAP_RLOCK(b)		reqlock(&(b)->bcm_lock)
#define BMAP_URLOCK(b, lk)	ureqlock(&(b)->bcm_lock, lk)

#define DEBUG_BMAP(level, b, fmt, ...)					\
	psc_logs((level), PSS_OTHER,					\
		 " bmap@%p b:%x m:%u i:%"PRIx64				\
		 " rref=%u wref=%u opcnt=%u "fmt,			\
		 (b), (b)->bcm_blkno,					\
		 (b)->bcm_mode,						\
		 (b)->bcm_fcmh ? fcmh_2_fid((b)->bcm_fcmh) : 0,		\
		 atomic_read(&(b)->bcm_rd_ref),				\
		 atomic_read(&(b)->bcm_wr_ref),				\
		 atomic_read(&(b)->bcm_opcnt),				\
		 ## __VA_ARGS__)

#define bmap_set_accesstime(b)						\
	clock_gettime(CLOCK_REALTIME, &(b)->bcm_ts)

extern int
bmapc_cmp(const void *, const void *);

extern struct bmapc_memb *
bmap_lookup_locked(struct fidc_open_obj *, sl_blkno_t);

extern struct bmapc_memb * 
bmap_lookup(struct fidc_membh *, sl_blkno_t);

extern struct bmapc_memb * 
bmap_lookup_add(struct fidc_membh *f, sl_blkno_t n, 
		void (*bmap_init_fn)(struct bmapc_memb *, struct fidc_membh *, sl_blkno_t));

SPLAY_PROTOTYPE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

static inline void
bmap_op_done(struct bmapc_memb *b)
{
	atomic_dec(&b->bcm_opcnt);
}

#endif /* _BMAP_H_ */
