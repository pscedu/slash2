/* $Id$ */

#ifndef _SLASH_SLVR_H_
#define _SLASH_SLVR_H_

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_ds/listcache.h"
#include "psc_util/assert.h"

#include "bmap.h"
#include "offtree.h"

extern struct list_cache dirtySlvrs;

/**
 * slvr_ref - sliver reference used for scheduling dirty slivers
 *   to be crc'd and sent to the mds.
 * @slvr_num: sliver number (0 - SL_CRCS_PER_BMAP)
 * @slvr_updates: update counter used to detect modifications to the
 *   sliver.  Note that atomic is not used here since the lock must be 
 *   held to avoid race conditions.
 * @slvr_pri: private pointer used for backpointer to iodbmap_data.
 * @slvr_ts: timestamp since last access.
 * @slvr_lentry: dirty queue.
 * @slvr_tentry: bmap tree entry.
 * Note: slivers are locked through their iodbmap_data lock.
 */
struct slvr_ref {
	uint16_t              slvr_num;
	uint16_t              slvr_flags;
	uint32_t              slvr_updates; 
	void                 *slvr_pri;
	struct timespec       slvr_ts;
	struct psclist_head   slvr_lentry;
	SPLAY_ENTRY(slvr_ref) slvr_tentry;
};

#define SLVR_LOCK(s)				\
	spinlock(&(SLVR_2_IOBD(s))->iobd_lock)

#define SLVR_ULOCK(s)				\
	freelock(&(SLVR_2_IOBD(s))->iobd_lock)

#define SLVR_LOCK_ENSURE(s)				\
	LOCK_ENSURE(&(SLVR_2_IOBD(s))->iobd_lock)

#define SLVR_2_BLK(s) ((s)->slvr_num * (SLASH_BMAP_SIZE/SLASH_BMAP_BLKSZ))

enum slvr_states {
	SLVR_NEW       = (1<<0),
	SLVR_CRCING    = (1<<1),
	SLVR_SCHEDULED = (1<<2),
	SLVR_INFLIGHT  = (1<<3),
};

static inline void
slvr_init(struct slvr_ref *s, uint16_t num, void *pri)
{
	s->slvr_num = num;
	s->slvr_flags = SLVR_NEW;
	s->slvr_pri = pri;
	s->slvr_updates = 0;
	INIT_PSCLIST_ENTRY(&s->slvr_lentry);
}

static inline int
slvr_cmp(const void *x, const void *y)
{
        const struct slvr_ref *a = x, *b = y;

        if (a->slvr_num > b->slvr_num)
                return (1);
        if (a->slvr_num < b->slvr_num)
                return (-1);
        return (0);
}

extern void
slvr_release(struct slvr_ref *);

//extern struct slvr_ref * 
//slvr_get(uint16_t, struct iodbmap_data *, int);

extern void
slvr_update(struct slvr_ref *);

extern void
slvr_cache_init(void);

#endif
