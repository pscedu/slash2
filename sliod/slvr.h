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

#ifndef _SLASH_SLVR_H_
#define _SLASH_SLVR_H_

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/tree.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "bmap_iod.h"
#include "buffer.h"
#include "slashrpc.h"

struct bmap_iod_info;

extern struct psc_listcache lruSlvrs;
extern struct psc_listcache crcqSlvrs;

/**
 * slvr_ref - sliver reference used for scheduling dirty slivers to be crc'd and sent to the mds.
 * Note: slivers are locked through their bmap_iod_info lock.
 */
struct slvr_ref {
	uint16_t		 slvr_num;		/* sliver index in the block map */
	uint16_t		 slvr_flags;
	uint16_t		 slvr_pndgwrts;		/* # of writes in progess */
	uint16_t		 slvr_pndgreads;	/* # of reads in progress */
	psc_crc64_t		 slvr_crc;		/* used if there's no bmap_wire present, only is valid if !SLVR_CRCDIRTY */
	void			*slvr_pri;		/* private pointer used for backpointer to bmap_iod_info */
	struct sl_buffer	*slvr_slab;
	struct psclist_head	 slvr_lentry;		/* dirty queue */
	SPLAY_ENTRY(slvr_ref)	 slvr_tentry;		/* bmap tree entry */
};

enum {
	SLVR_NEW	= (1 <<  0),	/* newly initialized */
	SLVR_SPLAYTREE	= (1 <<  1),	/* registered in the splay tree */
	SLVR_UNUSED1	= (1 <<  2),	/* unused1 */
	SLVR_FAULTING	= (1 <<  3),	/* contents being filled in from the disk or over the network */
	SLVR_GETSLAB	= (1 <<  4),	/* assigning memory buffer to slvr */
	SLVR_PINNED	= (1 <<  5),	/* slab cannot be removed from the cache */
	SLVR_DATARDY	= (1 <<  6),	/* ready for read / write activity */
	SLVR_DATAERR    = (1 <<  7),
	SLVR_LRU	= (1 <<  8),	/* cached but not dirty */
	SLVR_CRCDIRTY	= (1 <<  9),	/* crc does not match cached buffer */
	SLVR_CRCING     = (1 << 10),
	SLVR_FREEING	= (1 << 11),	/* sliver is being reaped */
	SLVR_SLBFREEING	= (1 << 12),	/* slab of the sliver is being reaped */
	SLVR_REPLSRC	= (1 << 13),    /* slvr is replication source */
	SLVR_REPLDST	= (1 << 14)     /* slvr is replication destination */
};

#define SLVR_2_BLK(s)		((s)->slvr_num * (SLASH_BMAP_SIZE / SLASH_BMAP_BLKSZ))

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

#define SLVR_WAIT(s, cond)						\
	do {								\
		DEBUG_SLVR(PLL_NOTIFY, (s), "SLVR_WAIT");		\
		while (!(cond)) {					\
			psc_waitq_wait(&(slvr_2_bmap((s)))->bcm_waitq,	\
				       &(slvr_2_biod((s)))->biod_lock);	\
			SLVR_LOCK((s));					\
		}							\
	} while (0)

#define slvr_2_biod(s)		((struct bmap_iod_info *)(s)->slvr_pri)
#define slvr_2_bmap(s)		slvr_2_biod(s)->biod_bmap
#define slvr_2_fcmh(s)		slvr_2_biod(s)->biod_bmap->bcm_fcmh
#define slvr_2_fii(s)		fcoo_2_fii(slvr_2_fcmh(s)->fcmh_fcoo)
#define slvr_2_fd(s)		slvr_2_fii(s)->fii_fd
#define slvr_2_biodi_wire(s)	slvr_2_biod(s)->biod_bmap_wire

#define slvr_2_buf(s, blk)						\
	((void *)(((s)->slvr_slab->slb_base) + ((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_fileoff(s, blk)						\
	((off_t)((slvr_2_bmap(s)->bcm_blkno * SLASH_BMAP_SIZE) +	\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_crcbits(s)						\
	slvr_2_biodi_wire((s))->bh_crcstates[(s)->slvr_num]

#define slvr_2_crc(s)							\
	slvr_2_biodi_wire((s))->bh_crcs[(s)->slvr_num].gc_crc

#define slvr_io_done(s, rw)						\
	((rw) == SL_WRITE ? slvr_wio_done(s) : slvr_rio_done(s))

#define SLVR_FLAGS_FMT "%s%s%s%s%s%s%s%s%s%s%s%s"
#define DEBUG_SLVR_FLAGS(s)						\
	(s)->slvr_flags & SLVR_NEW		? "n" : "-",		\
	(s)->slvr_flags & SLVR_FAULTING		? "f" : "-",		\
	(s)->slvr_flags & SLVR_GETSLAB		? "G" : "-",		\
	(s)->slvr_flags & SLVR_PINNED		? "p" : "-",		\
	(s)->slvr_flags & SLVR_CRCDIRTY		? "D" : "-",		\
	(s)->slvr_flags & SLVR_DATARDY		? "d" : "-",		\
	(s)->slvr_flags & SLVR_DATAERR		? "E" : "-",		\
	(s)->slvr_flags & SLVR_LRU		? "l" : "-",		\
	(s)->slvr_flags & SLVR_FREEING		? "F" : "-",		\
	(s)->slvr_flags & SLVR_SLBFREEING	? "b" : "-",		\
	(s)->slvr_flags & SLVR_REPLSRC		? "S" : "-",		\
	(s)->slvr_flags & SLVR_REPLDST		? "T" : "-"		\

#define DEBUG_SLVR(level, s, fmt, ...)					\
	psc_logs((level), PSS_GEN,					\
		 "slvr@%p num=%hu pw=%hu pr=%hu pri@%p slab@%p flgs:"	\
		 SLVR_FLAGS_FMT" :: "fmt,				\
		 (s), (s)->slvr_num,					\
		 (s)->slvr_pndgwrts,					\
		 (s)->slvr_pndgreads,					\
		 (s)->slvr_pri, (s)->slvr_slab, DEBUG_SLVR_FLAGS(s),	\
		 ## __VA_ARGS__)

struct slvr_ref *
	slvr_lookup(uint32_t, struct bmap_iod_info *, enum rw);
void	slvr_cache_init(void);
void	slvr_clear_inuse(struct slvr_ref *, int, uint32_t);
int	slvr_do_crc(struct slvr_ref *);
int	slvr_fsbytes_wio(struct slvr_ref *, uint32_t, uint32_t);
int	slvr_io_prep(struct slvr_ref *, uint32_t, uint32_t, enum rw);
void	slvr_repl_prep(struct slvr_ref *, int);
void	slvr_rio_done(struct slvr_ref *);
void	slvr_schedule_crc(struct slvr_ref *);
void	slvr_slab_prep(struct slvr_ref *, enum rw);
void	slvr_wio_done(struct slvr_ref *);
void	slvr_worker_init(void);

static __inline int
slvr_cmp(const void *x, const void *y)
{
	const struct slvr_ref *a = x, *b = y;

	return (CMP(a->slvr_num, b->slvr_num));
}

SPLAY_PROTOTYPE(biod_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

static __inline void
slvr_lru_pin_check(struct slvr_ref *s)
{
	SLVR_LOCK_ENSURE(s);
	psc_assert(s->slvr_slab && psclist_conjoint(&s->slvr_lentry));
	psc_assert(s->slvr_flags == (SLVR_LRU|SLVR_PINNED));
}

static __inline int
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

static __inline int
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

static __inline int
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

#endif
