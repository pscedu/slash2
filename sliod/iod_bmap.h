/* $Id$ */

#ifndef _SLASH_IOD_BMAP_H_
#define _SLASH_IOD_BMAP_H_

#include <sys/time.h>

#include "psc_types.h"

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "fidc_iod.h"
#include "slvr.h"

extern struct psc_listcache iodBmapLru;

SPLAY_HEAD(biod_slvrtree, slvr_ref);
SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

struct bmap_iod_info {
	psc_spinlock_t          biod_lock;
	struct bmapc_memb      *biod_bmap;
	struct biod_slvrtree    biod_slvrs;     
	struct slash_bmap_wire *biod_bmap_wire;
	struct psclist_head     biod_lentry;
	struct timespec         biod_age;
	uint32_t                biod_crcup_id;
};


struct bmap_crcup_ref {
	uint32_t bcr_id;

};

#define bmap_2_biodi(b) ((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b) bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b) bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_oftr(b)			\
	bmap_2_biodi(b)->biod_oftr
//	(((struct iodbmap_data *)((b)->bcm_pri))->biod_oftr)
#define bmap_2_biodi_wire(b)			\
	bmap_2_biodi(b)->biod_bmap_wire


enum iod_bmap_modes {
	BMAP_IOD_RETRIEVE  = (1 << (0 + BMAP_RSVRD_MODES)),
	BMAP_IOD_RELEASING = (1 << (1 + BMAP_RSVRD_MODES))
};

#define slvr_2_biod(s) ((struct bmap_iod_info *)(s)->slvr_iobd)
#define slvr_2_bmap(s) slvr_2_biod(s)->biod_bmap
#define slvr_2_fcmh(s) slvr_2_biod(s)->biod_bmap->bcm_fcmh
#define slvr_2_fd(s) fcmh_2_fiodi(slvr_2_fcmh(s))->fiodi_fd
#define slvr_2_biodi_wire(s) slvr_2_biod(s)->biod_bmap_wire

#define slvr_2_buf(s, blk)						\
	(void *)(((s)->slvr_slab->slb_base) + (blk * SLASH_SLVR_BLKSZ))

#define slvr_2_fileoff(s, blk)						\
	(off_t)((slvr_2_bmap(s)->bcm_bmapno * SLASH_BMAP_SIZE) +	\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		(blk * SLASH_SLVR_BLKSZ))

#define slvr_2_crcbits(s)			\
	(u8)(slvr_2_biodi_wire((s))->bh_crcstates[(s)->slvr_num])

#define slvr_2_crc(s)				\
	(psc_crc_t)(slvr_2_biodi_wire((s))->bh_crc[(s)->slvr_num])

#define SLVR_LOCK(s)				\
	spinlock(&(slvr_2_biod(s))->biod_lock)

#define SLVR_ULOCK(s)				\
	freelock(&(slvr_2_biod(s))->biod_lock)

#define SLVR_LOCK_ENSURE(s)				\
	LOCK_ENSURE(&(slvr_2_biod(s))->biod_lock)

#define SLVR_WAKEUP(s)					\
	psc_waitq_wakeall(&(slvr_2_bmap(s))->bcm_waitq)

#define SLVR_WAIT(s)						\
	{							\
		psc_waitq_wait(&(slvr_2_bmap(s))->bcm_waitq,	\
			       &(slvr_2_biod(s))->biod_lock);	\
		SLVR_LOCK(s);					\
	}

static inline void
slvr_lru_pin(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
        psc_assert(s->slvr_slab && psclist_conjoint(&s->slvr_lentry));

        bitflag_sorc(&s->slvr_flags, NULL, SLVR_LRU,
                     (SLVR_DIRTY|SLVR_NEW|SLVR_CRCING|SLVR_FAULTING|
		      SLVR_INFLIGHT|SLVR_GETSLAB|SLVR_PINNED|
                      SLVR_DATARDY|SLVR_DIRTY|SLVR_CRCDIRTY),
                     SLVR_PINNED, 0, (BIT_STRICT|BIT_ABORT));
}

static inline void
slvr_lru_unpin(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
        psc_assert(s->slvr_slab && psclist_conjoint(&s->slvr_lentry));
	psc_assert(!atomic_read(&s->slvr_pndgreads));
	psc_assert(!atomic_read(&s->slvr_pndgwrts));

        bitflag_sorc(&s->slvr_flags, NULL, (SLVR_LRU|SLVR_PINNED|SLVR_DATARDY),
                     (SLVR_DIRTY|SLVR_NEW|SLVR_CRCING|SLVR_FAULTING|
		      SLVR_INFLIGHT|SLVR_GETSLAB|SLVR_DIRTY|SLVR_CRCDIRTY),
                     0, SLVR_PINNED, (BIT_STRICT|BIT_ABORT));
}


#endif /* _SLASH_IOD_BMAP_H_ */
