/* $Id$ */

#ifndef _SLASH_IOD_BMAP_H_
#define _SLASH_IOD_BMAP_H_

#include <sys/time.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "fidc_iod.h"
#include "slvr.h"
#include "slashrpc.h"

extern struct psc_listcache iodBmapLru;

/* For now only one of these structures is needed.  In the future
 *   we'll need one per MDS.
 */
struct biod_infl_crcs {
	psc_spinlock_t		binfcrcs_lock;
	atomic_t                binfcrcs_nbcrs;
	struct psc_lockedlist   binfcrcs_hold;
	struct psc_lockedlist   binfcrcs_ready;
	struct psc_lockedlist   binfcrcs_infl;
};

struct bmap_iod_info;

struct biod_crcup_ref {
	uint64_t			 bcr_xid;
	uint16_t			 bcr_flags;
	struct timespec			 bcr_age;
	struct bmap_iod_info            *bcr_biodi;
	struct psclist_head              bcr_lentry;
	struct srm_bmap_crcup		 bcr_crcup;
};

#define	BCR_NONE			 0x00
#define BCR_SCHEDULED                    0x01

#define DEBUG_BCR(level, b, fmt, ...)					\
        psc_logs((level), PSS_GEN,                                      \
                 "bcr@%p fid="FIDFMT" xid=%"PRIu64" nups=%d fl=%d age=%lu" \
                 " bmap@%p:%u :: "fmt,					\
                 (b), FIDFMTARGS(&(b)->bcr_crcup.fg), (b)->bcr_xid,	\
		 (b)->bcr_crcup.nups, (b)->bcr_flags, (b)->bcr_age.tv_sec, \
		 (b)->bcr_biodi->biod_bmap,				\
		 (b)->bcr_biodi->biod_bmap->bcm_blkno,			\
		 ## __VA_ARGS__)

SPLAY_HEAD(biod_slvrtree, slvr_ref);
SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

struct bmap_iod_info {
	psc_spinlock_t          biod_lock;
	struct bmapc_memb      *biod_bmap;
	struct biod_crcup_ref  *biod_bcr;
	struct biod_slvrtree    biod_slvrs;
	struct slash_bmap_wire *biod_bmap_wire;
	struct psclist_head     biod_lentry;
	struct timespec         biod_age;
	uint64_t                biod_bcr_xid;
	uint64_t                biod_bcr_xid_last;
	uint32_t                biod_inflight;
};



static inline void
bcr_hold_2_ready(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	int locked;

	LOCK_ENSURE(&bcr->bcr_biodi->biod_lock);	
	psc_assert(bcr->bcr_biodi->biod_bcr == bcr);

	locked = reqlock(&inf->binfcrcs_lock);	
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	pll_remove(&inf->binfcrcs_hold, bcr);
	pll_addtail(&inf->binfcrcs_ready, bcr);
	ureqlock(&inf->binfcrcs_lock, locked);

	bcr->bcr_biodi->biod_bcr = NULL;
}

static inline void
bcr_hold_add(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	psc_assert(psclist_disjoint(&bcr->bcr_lentry));
	pll_addtail(&inf->binfcrcs_hold, bcr);
	atomic_inc(&inf->binfcrcs_nbcrs);
}

static inline void
bcr_hold_requeue(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	int locked;

	locked = reqlock(&inf->binfcrcs_lock);
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	pll_remove(&inf->binfcrcs_hold, bcr);
	pll_addtail(&inf->binfcrcs_hold, bcr);
	ureqlock(&inf->binfcrcs_lock, locked);
}

static inline void
bcr_xid_check(struct biod_crcup_ref *bcr)
{
	int locked;

	locked = reqlock(&bcr->bcr_biodi->biod_lock);
	psc_assert(bcr->bcr_xid < bcr->bcr_biodi->biod_bcr_xid);
	psc_assert(bcr->bcr_xid == bcr->bcr_biodi->biod_bcr_xid_last);
	ureqlock(&bcr->bcr_biodi->biod_lock, locked);
}

static inline void
bcr_xid_last_bump(struct biod_crcup_ref *bcr)
{
	int locked;

        locked = reqlock(&bcr->bcr_biodi->biod_lock);
	bcr_xid_check(bcr);
	bcr->bcr_biodi->biod_bcr_xid_last++;
	ureqlock(&bcr->bcr_biodi->biod_lock, locked);
}

static inline void
bcr_ready_remove(struct biod_infl_crcs *inf, struct biod_crcup_ref *bcr)
{
	spinlock(&inf->binfcrcs_lock);
	psc_assert(psclist_conjoint(&bcr->bcr_lentry));
	psc_assert(bcr->bcr_flags & BCR_SCHEDULED);
	pll_remove(&inf->binfcrcs_hold, bcr);
	freelock(&inf->binfcrcs_lock);

	atomic_dec(&inf->binfcrcs_nbcrs);
	bcr_xid_last_bump(bcr);
	PSCFREE(bcr);
}


#if 0
static inline int
bcr_cmp(const void *x, const void *y)
{
	const struct biod_crcup_ref *a = x, *b = y;

	if (a->bcr_xid > b->bcr_xid)
		return (1);
	if (a->bcr_xid < b->bcr_xid)
		return (-1);
	return (0);
}
#endif

#define bmap_2_biodi(b)		((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b)	bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b)	bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_slvrs(b)	(&bmap_2_biodi(b)->biod_slvrs)
#define bmap_2_biodi_wire(b)	bmap_2_biodi(b)->biod_bmap_wire

/* bmap iod modes */
#define BMAP_IOD_RELEASING	(_BMAP_FLSHFT << 0)
#define BMAP_IOD_RETRFAIL	(_BMAP_FLSHFT << 1)

#define BIOD_CRCUP_MAX_AGE		 2		/* in seconds */

enum slash_bmap_slv_states {
	BMAP_SLVR_DATA = (1<<0), /* Data present, otherwise slvr is hole */
	BMAP_SLVR_CRC  = (1<<1)  /* Valid CRC */
	//XXX ATM, 6 bits are left
};

#define slvr_2_biod(s)		((struct bmap_iod_info *)(s)->slvr_pri)
#define slvr_2_bmap(s)		slvr_2_biod(s)->biod_bmap
#define slvr_2_fcmh(s)		slvr_2_biod(s)->biod_bmap->bcm_fcmh
#define slvr_2_fd(s)		slvr_2_fcmh(s)->fcmh_fcoo->fcoo_fd
#define slvr_2_biodi_wire(s)	slvr_2_biod(s)->biod_bmap_wire

#define slvr_2_buf(s, blk)						\
	(void *)(((s)->slvr_slab->slb_base) + ((blk) * SLASH_SLVR_BLKSZ))

#define slvr_2_fileoff(s, blk)						\
	(off_t)((slvr_2_bmap(s)->bcm_blkno * SLASH_BMAP_SIZE) +		\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		((blk) * SLASH_SLVR_BLKSZ))

#define slvr_2_crcbits(s)						\
	slvr_2_biodi_wire((s))->bh_crcstates[(s)->slvr_num]

#define slvr_2_crc(s)							\
	slvr_2_biodi_wire((s))->bh_crcs[(s)->slvr_num].gc_crc

#define SLVR_GETLOCK(s)		(&(slvr_2_biod(s))->biod_lock)
#define SLVR_LOCK(s)		spinlock(SLVR_GETLOCK(s))
#define SLVR_ULOCK(s)		freelock(SLVR_GETLOCK(s))
#define SLVR_RLOCK(s)		reqlock(SLVR_GETLOCK(s))
#define SLVR_URLOCK(s, lk)	ureqlock(SLVR_GETLOCK(s), (lk))
#define SLVR_LOCK_ENSURE(s)	LOCK_ENSURE(SLVR_GETLOCK(s))
#define SLVR_TRYLOCK(s)		trylock(SLVR_GETLOCK(s))
#define SLVR_TRYREQLOCK(s, lk)	tryreqlock(SLVR_GETLOCK(s), lk)

#define SLVR_WAKEUP(s)							\
	psc_waitq_wakeall(&(slvr_2_bmap((s)))->bcm_waitq)

#define SLVR_WAIT(s)							\
	do {								\
		DEBUG_SLVR(PLL_NOTIFY, (s), "SLVR_WAIT");		\
		while (!((s)->slvr_flags & SLVR_DATARDY)) {		\
			psc_waitq_wait(&(slvr_2_bmap((s)))->bcm_waitq,	\
				       &(slvr_2_biod((s)))->biod_lock);	\
			SLVR_LOCK((s));					\
		}							\
	} while (0)

static inline void
slvr_lru_pin_check(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab && psclist_conjoint(&s->slvr_lentry));
	psc_assert(s->slvr_flags == (SLVR_LRU|SLVR_PINNED));
}

static inline int
slvr_lru_tryunpin_locked(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab);
	if (s->slvr_pndgwrts || s->slvr_pndgreads ||
	    s->slvr_flags & SLVR_CRCDIRTY)
		return (0);

	psc_assert(s->slvr_flags & SLVR_LRU);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	psc_assert(!(s->slvr_flags &
		     (SLVR_NEW|SLVR_FAULTING|
		      SLVR_GETSLAB|SLVR_CRCDIRTY)));

	s->slvr_flags &= ~SLVR_PINNED;
	return (1);
}

static inline int
slvr_lru_slab_freeable(struct slvr_ref *s)	
{
	int freeable = 1;
	SLVR_LOCK_ENSURE(s);

	psc_assert(s->slvr_flags & SLVR_LRU);

	if (s->slvr_flags & SLVR_DATARDY)
		psc_assert(!(s->slvr_flags &
			     (SLVR_NEW|SLVR_FAULTING|SLVR_GETSLAB)));

	if (!s->slvr_slab)
		psc_assert(!(s->slvr_flags & 
			     (SLVR_NEW|SLVR_FAULTING|
			      SLVR_GETSLAB|SLVR_DATARDY)));		
	
	if (s->slvr_flags & SLVR_PINNED)
		freeable = 0;

	DEBUG_SLVR(PLL_INFO, s, "freeable=%d", freeable);

	return (freeable);
}

static inline int
slvr_lru_freeable(struct slvr_ref *s)
{
	int freeable=0;

	if (s->slvr_slab ||
	    s->slvr_flags & SLVR_PINNED   ||
	    s->slvr_flags & SLVR_CRCDIRTY)
		goto out;

	psc_assert(slvr_lru_slab_freeable(s));

	freeable = 1;
 out:
	return (freeable);
}

#endif /* _SLASH_IOD_BMAP_H_ */
