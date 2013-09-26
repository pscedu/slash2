/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Block map page cache definitions - for managing the in-memory store
 * of file regions (bmaps).
 */

#ifndef _SL_BMPC_H_
#define _SL_BMPC_H_

#include <time.h>

#include "pfl/time.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lockedlist.h"
#include "pfl/tree.h"
#include "pfl/vbitmap.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"
#include "psc_util/waitq.h"

#include "cache_params.h"
#include "bmap.h"

struct msl_fhent;
struct msl_fsrqinfo;

#define BMPC_BUFSZ		SLASH_SLVR_BLKSZ
#define BMPC_BLKSZ		BMPC_BUFSZ
#define BMPC_BUFMASK		(BMPC_BLKSZ - 1)
#define BMPC_IOMAXBLKS		64
#define BMPC_MAXBUFSRPC		(1024 * 1024 / BMPC_BUFSZ)	/* same as LNET_MTU / BMPC_BUFSZ */

/* plus one because the offset in the first request might not be page aligned */
#define BMPC_COALESCE_MAX_IOV	(BMPC_MAXBUFSRPC + 1)

struct timespec			bmapFlushDefMaxAge;

struct bmap_pagecache_entry {
	psc_atomic32_t		 bmpce_ref;	/* biorq and readahead refs	*/
	uint32_t		 bmpce_flags;	/* BMPCE_* flag bits		*/
	uint32_t		 bmpce_off;	/* filewise, bmap relative	*/
	psc_spinlock_t		 bmpce_lock;	/* serialize			*/
	void			*bmpce_base;	/* base pointer from slb	*/
	void			*bmpce_owner;
	struct psc_waitq	*bmpce_waitq;	/* others block here on I/O	*/
	struct timespec		 bmpce_laccess;	/* last page access		*/
	struct psc_listentry	 bmpce_ralentry;/* queue read ahead		*/
	struct psc_lockedlist	 bmpce_pndgaios;
	SPLAY_ENTRY(bmap_pagecache_entry) bmpce_tentry;
	struct psc_listentry	 bmpce_lentry;	/* chain on bmap LRU		*/
};

#define BMPCE_INIT		(1 <<  0)
#define BMPCE_DATARDY		(1 <<  1)
#define BMPCE_LRU		(1 <<  2)
#define BMPCE_TOFREE		(1 <<  3)
#define BMPCE_EIO		(1 <<  4)	/* I/O error */
#define BMPCE_READA		(1 <<  5)	/* read-ahead */
#define BMPCE_AIOWAIT		(1 <<  6)	/* wait on async read */
#define BMPCE_DISCARD		(1 <<  7)	/* don't cache after I/O */

#define BMPCE_LOCK(b)		spinlock(&(b)->bmpce_lock)
#define BMPCE_ULOCK(b)		freelock(&(b)->bmpce_lock)
#define BMPCE_RLOCK(b)		reqlock(&(b)->bmpce_lock)
#define BMPCE_TRYLOCK(b)	trylock(&(b)->bmpce_lock)
#define BMPCE_URLOCK(b, lk)	ureqlock(&(b)->bmpce_lock, (lk))
#define BMPCE_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bmpce_lock)

#define BMPCE_WAIT(b)		psc_waitq_wait((b)->bmpce_waitq, &(b)->bmpce_lock)

/* introduce a flag to avoid unconditional wakeup */

#define BMPCE_WAKE(e)							\
	do {								\
		if ((e)->bmpce_waitq) {					\
			psc_waitq_wakeall((e)->bmpce_waitq);		\
			DEBUG_BMPCE(PLL_DEBUG, (e), "wakeup");		\
		} else							\
			DEBUG_BMPCE(PLL_INFO, (e), "NULL bmpce_waitq");	\
	} while (0)

#define BMPCE_SETATTR(e, fl, ...)					\
	do {								\
		int _locked;						\
									\
		_locked = BMPCE_RLOCK(e);				\
		(e)->bmpce_flags |= (fl);				\
		DEBUG_BMPCE(PLL_INFO, (e), ##__VA_ARGS__);		\
		BMPCE_URLOCK((e), _locked);				\
	} while (0)

#define DEBUG_BMPCE(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP,					\
	    "bmpce@%p fl=%u:%s%s%s%s%s%s%s "				\
	    "o=%#x b=%p "						\
	    "ts="PSCPRI_TIMESPEC" "					\
	    "ref=%u "							\
	    "owner=%p : " fmt,						\
	    (b), (b)->bmpce_flags,					\
	    (b)->bmpce_flags & BMPCE_DATARDY		? "d" : "",	\
	    (b)->bmpce_flags & BMPCE_LRU		? "l" : "",	\
	    (b)->bmpce_flags & BMPCE_TOFREE		? "T" : "",	\
	    (b)->bmpce_flags & BMPCE_INIT		? "i" : "",	\
	    (b)->bmpce_flags & BMPCE_EIO		? "E" : "",	\
	    (b)->bmpce_flags & BMPCE_READA		? "a" : "",	\
	    (b)->bmpce_flags & BMPCE_AIOWAIT		? "w" : "",	\
	    (b)->bmpce_off, (b)->bmpce_base,				\
	    PSCPRI_TIMESPEC_ARGS(&(b)->bmpce_laccess),			\
	    psc_atomic32_read(&(b)->bmpce_ref),				\
	    (b)->bmpce_owner, ## __VA_ARGS__)

static __inline int
bmpce_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache_entry *a = x, *b = y;

	return (CMP(a->bmpce_off, b->bmpce_off));
}

SPLAY_HEAD(bmap_pagecachetree, bmap_pagecache_entry);
SPLAY_PROTOTYPE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
		bmpce_cmp)

struct bmpc_ioreq {
	char				*biorq_buf;
	int32_t				 biorq_ref;
	/*
	 * Note that a request may fall somewhere within a bmap.  It
	 * might be not page aligned.
	 */
	uint32_t			 biorq_off;	/* filewise, bmap relative	*/
	uint32_t			 biorq_len;	/* non-aligned, real length	*/
	uint32_t			 biorq_flags;	/* state and op type bits	*/
	uint32_t			 biorq_retries;	/* dirty data flush retries	*/
	sl_ios_id_t			 biorq_last_sliod;
	psc_spinlock_t			 biorq_lock;
	struct timespec			 biorq_expire;
	struct psc_dynarray		 biorq_pages;	/* array of bmpce		*/
	struct psclist_head		 biorq_lentry;	/* chain on bmpc_pndg_biorqs	*/
	struct psclist_head		 biorq_mfh_lentry; /* chain on file handle	*/
	struct psclist_head		 biorq_bwc_lentry;
	struct psclist_head		 biorq_png_lentry;
	SPLAY_ENTRY(bmpc_ioreq)		 biorq_tentry;
	struct bmapc_memb		*biorq_bmap;	/* backpointer to our bmap	*/
	struct pscrpc_request_set	*biorq_rqset;
	struct psc_waitq		 biorq_waitq;	/* used by a bmpce */
	struct msl_fhent		*biorq_mfh;	/* back pointer to msl_fhent */
	struct msl_fsrqinfo		*biorq_fsrqi;
};

#define	BIORQ_READ			(1 <<  0)
#define	BIORQ_WRITE			(1 <<  1)
#define	BIORQ_RBWFP			(1 <<  2)	/* read before write - first page */
#define	BIORQ_RBWLP			(1 <<  3)	/* read before write - last page */
#define	BIORQ_SCHED			(1 <<  4)	/* guard race between threads and callback */
#define	BIORQ_INFL			(1 <<  5)
#define	BIORQ_DIO			(1 <<  6)
#define	BIORQ_FORCE_EXPIRE		(1 <<  7)
#define	BIORQ_DESTROY			(1 <<  8)
#define	BIORQ_FLUSHRDY			(1 <<  9)
#define	BIORQ_NOFHENT			(1 << 10)	/* release a file handle before flush is complete */
#define BIORQ_AIOWAIT			(1 << 11)
#define BIORQ_PENDING			(1 << 12)
#define BIORQ_WAIT			(1 << 13)
#define BIORQ_MFHLIST			(1 << 14)

#define BIORQ_LOCK(r)			spinlock(&(r)->biorq_lock)
#define BIORQ_ULOCK(r)			freelock(&(r)->biorq_lock)
#define BIORQ_RLOCK(r)			reqlock(&(r)->biorq_lock)
#define BIORQ_URLOCK(r)			ureqlock(&(r)->biorq_lock)
#define BIORQ_LOCK_ENSURE(r)		LOCK_ENSURE(&(r)->biorq_lock)

#define BIORQ_SETATTR(r, fl)		SETATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))
#define BIORQ_CLEARATTR(r, fl)		CLEARATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))

#define DEBUG_BIORQ(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP, "biorq@%p flg=%#x:"			\
	    "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s "				\
	    "ref=%d off=%u len=%u "					\
	    "retry=%u buf=%p rqi=%p "					\
	    "sliod=%x np=%d "						\
	    "b=%p ex="PSCPRI_TIMESPEC" : "fmt,				\
	    (b), (b)->biorq_flags,					\
	    (b)->biorq_flags & BIORQ_READ		? "r" : "",	\
	    (b)->biorq_flags & BIORQ_WRITE		? "w" : "",	\
	    (b)->biorq_flags & BIORQ_RBWFP		? "f" : "",	\
	    (b)->biorq_flags & BIORQ_RBWLP		? "l" : "",	\
	    (b)->biorq_flags & BIORQ_SCHED		? "s" : "",	\
	    (b)->biorq_flags & BIORQ_INFL		? "i" : "",	\
	    (b)->biorq_flags & BIORQ_DIO		? "d" : "",	\
	    (b)->biorq_flags & BIORQ_FORCE_EXPIRE	? "x" : "",	\
	    (b)->biorq_flags & BIORQ_DESTROY		? "D" : "",	\
	    (b)->biorq_flags & BIORQ_FLUSHRDY		? "L" : "",	\
	    (b)->biorq_flags & BIORQ_NOFHENT		? "n" : "",	\
	    (b)->biorq_flags & BIORQ_AIOWAIT		? "S" : "",	\
	    (b)->biorq_flags & BIORQ_PENDING		? "p" : "",	\
	    (b)->biorq_flags & BIORQ_WAIT		? "W" : "",	\
	    (b)->biorq_flags & BIORQ_MFHLIST		? "m" : "",	\
	    (b)->biorq_ref, (b)->biorq_off, (b)->biorq_len,		\
	    (b)->biorq_retries, (b)->biorq_buf, (b)->biorq_fsrqi,	\
	    (b)->biorq_last_sliod, psc_dynarray_len(&(b)->biorq_pages),	\
	    (b)->biorq_bmap, PSCPRI_TIMESPEC_ARGS(&(b)->biorq_expire), ## __VA_ARGS__)

static __inline int
bmpc_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq *a = x, *b = y;
	uint32_t rc;

	rc = CMP(a->biorq_off, b->biorq_off);
	if (rc)
		return (rc);

	rc = CMP(a->biorq_len, b->biorq_len);
	if (rc)
		return (rc);

	return (CMP(a, b));
}

SPLAY_HEAD(bmpc_biorq_tree, bmpc_ioreq);
SPLAY_PROTOTYPE(bmpc_biorq_tree, bmpc_ioreq, biorq_tentry, bmpc_biorq_cmp)

struct bmap_pagecache {
	struct bmap_pagecachetree	 bmpc_tree;		/* tree of entries */
	struct timespec			 bmpc_oldest;		/* LRU's oldest item */
	struct psc_lockedlist		 bmpc_lru;		/* cleancnt can be kept here  */
	/*
	 * List for new requests minus BIORQ_READ and BIORQ_DIO.  All
	 * requests are sorted based on their starting offsets to
	 * facilitate write coalescing.
	 */
	struct bmpc_biorq_tree		 bmpc_new_biorqs;
	int				 bmpc_new_nbiorqs;
	struct psc_lockedlist		 bmpc_pndg_biorqs;	/* chain pending I/O requests */
	struct psc_lockedlist		 bmpc_pndg_ra;		/* RA bmpce's pending comp */
	int				 bmpc_pndgwr;		/* # pending wr req */
	psc_spinlock_t			 bmpc_lock;		/* serialize access */
	struct psclist_head		 bmpc_lentry;		/* chain to global LRU lc */
};

/*
 * The following four macros are equivalent to PLL_xxx counterparts
 * because of the way we initialize the locked lists in bmap_pagecache.
 */
#define BMPC_LOCK(b)		spinlock(&(b)->bmpc_lock)
#define BMPC_ULOCK(b)		freelock(&(b)->bmpc_lock)
#define BMPC_RLOCK(b)		reqlock(&(b)->bmpc_lock)
#define BMPC_URLOCK(b, lk)	ureqlock(&(b)->bmpc_lock, (lk))

static __inline int
bmpc_queued_ios(struct bmap_pagecache *bmpc)
{
	int locked, rc;

	locked = BMPC_RLOCK(bmpc);
	rc = pll_nitems(&bmpc->bmpc_pndg_biorqs) +
	    pll_nitems(&bmpc->bmpc_pndg_ra) +
	    bmpc->bmpc_new_nbiorqs;
	BMPC_URLOCK(bmpc, locked);
	return (rc);
}

struct bmpc_write_coalescer {
	struct psc_lockedlist		 bwc_pll;
	size_t				 bwc_size;
	off_t				 bwc_soff;
	struct iovec			 bwc_iovs[BMPC_COALESCE_MAX_IOV];
	struct bmap_pagecache_entry	*bwc_bmpces[BMPC_COALESCE_MAX_IOV];
	int				 bwc_niovs;
	int				 bwc_nbmpces;
	struct psclist_head		 bwc_lentry;
};

static __inline void
bmpce_useprep(struct bmap_pagecache_entry *bmpce,
    struct bmpc_ioreq *biorq, struct psc_waitq *wq)
{
	psc_assert(!bmpce->bmpce_waitq);
	psc_assert(bmpce->bmpce_flags == BMPCE_INIT);

	bmpce->bmpce_flags &= ~BMPCE_INIT;
	psc_atomic32_set(&bmpce->bmpce_ref, 1);
	bmpce->bmpce_owner = biorq;

	/*
	 * We put the entry back to the splay tree before it is fully
	 * allocated, so we need this field to remember who owns it.
	 * Alternatively, we could use locking.
	 */
	if (!wq)
		bmpce->bmpce_waitq = &biorq->biorq_waitq;
	else {
		bmpce->bmpce_flags |= BMPCE_READA;
		bmpce->bmpce_waitq = wq;
	}
}

static __inline void
bmpce_usecheck(struct bmap_pagecache_entry *bmpce, int op, uint32_t off)
{
	int locked;

	locked = reqlock(&bmpce->bmpce_lock);

	DEBUG_BMPCE(PLL_DEBUG, bmpce, "op=%d off=%u", op, off);

	psc_assert(psc_atomic32_read(&bmpce->bmpce_ref) > 0);

	psc_assert(bmpce->bmpce_off == off);
	ureqlock(&bmpce->bmpce_lock, locked);
}

/**
 * biorq_is_my_bmpce - Informs the caller that biorq, r, owns the the
 *	page cache entry, b.  This state implies that the thread
 *	processing 'r' is responsible for allocating a memory page and
 *	possible faulting in that page from the ION.
 */
#define biorq_is_my_bmpce(r, b)	((r) == (b)->bmpce_owner)

#define biorq_getaligned_off(r, nbmpce)					\
	(((r)->biorq_off & ~BMPC_BUFMASK) + ((nbmpce) * BMPC_BUFSZ))

#define biorq_voff_get(r)	((r)->biorq_off + (r)->biorq_len)

void	 bmpc_global_init(void);
void	 bmpc_freeall_locked(struct bmap_pagecache *);
void	 bmpc_biorqs_fail(struct bmap_pagecache *, int);
void	 bmpc_biorqs_destroy(struct bmapc_memb *, int);

struct bmpc_ioreq *
	 bmpc_biorq_new(struct msl_fsrqinfo *, struct bmapc_memb *,
	    char *, int, uint32_t, uint32_t, int);

int	 bmpce_init(struct psc_poolmgr *, void *);
struct bmap_pagecache_entry *
	 bmpce_lookup_locked(struct bmapc_memb *, struct bmpc_ioreq *,
	    uint32_t, struct psc_waitq *);

void	 bmpce_release_locked(struct bmap_pagecache_entry *,
	    struct bmap_pagecache *);

void	 bwc_release(struct bmpc_write_coalescer *);

extern struct psc_poolmgr	*bmpcePoolMgr;
extern struct psc_poolmgr	*bwcPoolMgr;
extern struct psc_listcache	 bmpcLru;

static __inline void
bmpc_lru_del(struct bmap_pagecache *bmpc)
{
	lc_remove(&bmpcLru, bmpc);
}

static __inline void
bmpc_init(struct bmap_pagecache *bmpc)
{
	memset(bmpc, 0, sizeof(*bmpc));
	INIT_PSC_LISTENTRY(&bmpc->bmpc_lentry);
	INIT_SPINLOCK(&bmpc->bmpc_lock);

	/* Double check the exclusivity of these lists... */

	pll_init(&bmpc->bmpc_lru, struct bmap_pagecache_entry,
	    bmpce_lentry, NULL);

	pll_init(&bmpc->bmpc_pndg_ra, struct bmap_pagecache_entry,
	    bmpce_lentry, NULL);

	pll_init(&bmpc->bmpc_pndg_biorqs, struct bmpc_ioreq,
	    biorq_lentry, NULL);

	SPLAY_INIT(&bmpc->bmpc_new_biorqs);

	/*
	 * Add the bmpc to the tail of LRU where it will stay until it's
	 * freed.
	 */
	lc_add(&bmpcLru, bmpc);
}

static __inline int
bmpc_lru_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache * const *pa = x, *a = *pa;
	const struct bmap_pagecache * const *pb = y, *b = *pb;

	if (timespeccmp(&a->bmpc_oldest, &b->bmpc_oldest, <))
		return (-1);

	if (timespeccmp(&a->bmpc_oldest, &b->bmpc_oldest, >))
		return (1);

	return (0);
}

#endif /* _SL_BMPC_H_ */
