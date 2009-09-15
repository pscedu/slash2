/* $Id$ */

#ifndef _SLASH_IOD_BMAP_H_
#define _SLASH_IOD_BMAP_H_

#include <sys/time.h>

#include "psc_rpc/rpc.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "fidc_iod.h"
#include "slvr.h"
#include "slashrpc.h"

extern struct psc_listcache iodBmapLru;

#define BIOD_CRCUP_MAX_AGE 2

struct biod_crcup_ref {
	uint64_t                    bcr_id;
	struct timespec             bcr_age;
	struct slvr_ref            *bcr_slvr;
	SPLAY_ENTRY(biod_crcup_ref) bcr_tentry;
	struct srm_bmap_crcup       bcr_crcup;
};

static inline int
bcr_cmp(const void *x, const void *y)
{
        const struct biod_crcup_ref *a = x, *b = y;

        if (a->bcr_id > b->bcr_id)
                return (1);
        if (a->bcr_id < b->bcr_id)
                return (-1);
        return (0);
}

SPLAY_HEAD(crcup_reftree, biod_crcup_ref);
SPLAY_PROTOTYPE(crcup_reftree, biod_crcup_ref, bcr_tentry, bcr_cmp);

/* For now only one of these structures is needed.  In the future
 *   we'll need one per MDS.
 */
struct biod_infslvr_tree {
	uint64_t             binfst_counter;
	psc_spinlock_t       binfst_lock;
	struct crcup_reftree binfst_tree;
};

SPLAY_HEAD(biod_slvrtree, slvr_ref);
SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

struct bmap_iod_info {
	psc_spinlock_t          biod_lock;
	struct bmapc_memb      *biod_bmap;
	struct biod_slvrtree    biod_slvrs;     
	struct slash_bmap_wire *biod_bmap_wire;
	struct psclist_head     biod_lentry;
	struct timespec         biod_age;
	uint32_t                biod_bcr_id;
};

#define bmap_2_biodi(b)		((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b)	bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b)	bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_slvrs(b)	(&bmap_2_biodi(b)->biod_slvrs)
#define bmap_2_biodi_wire(b)	bmap_2_biodi(b)->biod_bmap_wire

enum iod_bmap_modes {
	BMAP_IOD_RETRIEVE  = (1 << (0 + BMAP_RSVRD_MODES)),
	BMAP_IOD_RELEASING = (1 << (1 + BMAP_RSVRD_MODES)),
	BMAP_IOD_RETRFAIL  = (1 << (2 + BMAP_RSVRD_MODES))
};

#define slvr_2_biod(s)		((struct bmap_iod_info *)(s)->slvr_pri)
#define slvr_2_bmap(s)		slvr_2_biod(s)->biod_bmap
#define slvr_2_fcmh(s)		slvr_2_biod(s)->biod_bmap->bcm_fcmh
#define slvr_2_fd(s)		slvr_2_fcmh(s)->fcmh_fcoo->fcoo_fd
#define slvr_2_biodi_wire(s)	slvr_2_biod(s)->biod_bmap_wire

#define slvr_2_buf(s, blk)						\
	(void *)(((s)->slvr_slab->slb_base) + (blk * SLASH_SLVR_BLKSZ))

#define slvr_2_fileoff(s, blk)						\
	(off_t)((slvr_2_bmap(s)->bcm_blkno * SLASH_BMAP_SIZE) +	\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		(blk * SLASH_SLVR_BLKSZ))

#define slvr_2_crcbits(s)			\
	slvr_2_biodi_wire((s))->bh_crcstates[(s)->slvr_num]

#define slvr_2_crc(s)				\
	slvr_2_biodi_wire((s))->bh_crcs[(s)->slvr_num].gc_crc

#define SLVR_GETLOCK(s)		(&(slvr_2_biod(s))->biod_lock)
#define SLVR_LOCK(s)		spinlock(SLVR_GETLOCK(s))
#define SLVR_ULOCK(s)		freelock(SLVR_GETLOCK(s))
#define SLVR_RLOCK(s)		reqlock(SLVR_GETLOCK(s))
#define SLVR_URLOCK(s, lk)	ureqlock(SLVR_GETLOCK(s), (lk))
#define SLVR_LOCK_ENSURE(s)	LOCK_ENSURE(SLVR_GETLOCK(s))
#define SLVR_TRYLOCK(s)         trylock(SLVR_GETLOCK(s))

#define SLVR_WAKEUP(s)					\
	psc_waitq_wakeall(&(slvr_2_bmap(s))->bcm_waitq)

#define SLVR_WAIT(s)						\
	do {							\
	slvr_wait_retry:					\
		psc_waitq_wait(&(slvr_2_bmap(s))->bcm_waitq,	\
			       &(slvr_2_biod(s))->biod_lock);	\
		SLVR_LOCK(s);					\
		if (!(s->slvr_flags & SLVR_DATARDY))		\
			goto slvr_wait_retry;			\
	} while (0)

static inline void
slvr_lru_pin_check(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
        psc_assert(s->slvr_slab && psclist_conjoint(&s->slvr_lentry));
	psc_assert(s->slvr_flags == (SLVR_LRU|SLVR_PINNED));
}

static inline void
slvr_lru_unpin(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
        psc_assert(s->slvr_slab);
	psc_assert(!psc_atomic16_read(&s->slvr_pndgreads));
	psc_assert(!psc_atomic16_read(&s->slvr_pndgwrts));

	psc_assert(s->slvr_flags & SLVR_LRU);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);
	
	psc_assert(!(s->slvr_flags & 
		     (SLVR_NEW|SLVR_CRCING|SLVR_FAULTING|SLVR_INFLIGHT|
		      SLVR_GETSLAB|SLVR_CRCDIRTY)));

	s->slvr_flags &= ~SLVR_PINNED;
}

static inline int
slvr_lru_slab_freeable(struct slvr_ref *s)
{
	int freeable = 1;
	SLVR_LOCK_ENSURE(s);

	psc_assert(s->slvr_flags & SLVR_LRU);

	if (s->slvr_flags & SLVR_PINNED) {
		psc_assert(psc_atomic16_read(&s->slvr_pndgwrts) ||
			   psc_atomic16_read(&s->slvr_pndgreads));
		freeable = 0;
	} 

	if (s->slvr_flags & SLVR_DATARDY)
		psc_assert(!(s->slvr_flags &
			     (SLVR_NEW|SLVR_CRCING|SLVR_FAULTING|SLVR_INFLIGHT|
			      SLVR_GETSLAB)));
	else
		freeable = 0;
	
	return (freeable);
}

static inline int
slvr_lru_freeable(struct slvr_ref *s)
{
	int locked, freeable=0;

	locked = SLVR_RLOCK(s);
	if (!slvr_lru_slab_freeable(s))
		goto out;
	      
	if (s->slvr_slab)
		goto out;

	if (s->slvr_flags & SLVR_CRCDIRTY)
		goto out;

	freeable = 1;
 out:
	SLVR_URLOCK(s, locked);
	return (freeable);
}

#endif /* _SLASH_IOD_BMAP_H_ */
