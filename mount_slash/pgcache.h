/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/atomic.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/lockedlist.h"
#include "pfl/pool.h"
#include "pfl/time.h"
#include "pfl/tree.h"
#include "pfl/vbitmap.h"
#include "pfl/waitq.h"

#include "bmap.h"
#include "cache_params.h"
#include "slconn.h"

struct msl_fhent;
struct msl_fsrqinfo;

#define BMPC_BUFSZ		SLASH_SLVR_BLKSZ
#define BMPC_BUFMASK		(BMPC_BUFSZ - 1)
#define BMPC_MAXBUFSRPC		(LNET_MTU / BMPC_BUFSZ)

/* plus one because the offset in the first request might not be page aligned */
#define BMPC_COALESCE_MAX_IOV	(BMPC_MAXBUFSRPC + 1)

struct bmap_pagecache_entry {
	int			 bmpce_rc;
	psc_atomic32_t		 bmpce_ref;	/* biorq and readahead refs	*/
	uint32_t		 bmpce_flags;	/* BMPCE_* flag bits		*/
	uint32_t		 bmpce_off;	/* relative to inside bmap	*/
	uint32_t		 bmpce_start;	/* region where data are valid  */
	uint32_t		 bmpce_len;
	psc_spinlock_t		 bmpce_lock;	/* serialize			*/
	void			*bmpce_base;	/* statically allocated pg contents */
	struct psc_waitq	*bmpce_waitq;	/* others block here on I/O	*/
	struct pfl_timespec	 bmpce_laccess;	/* last page access		*/
	struct psc_lockedlist	 bmpce_pndgaios;
	SPLAY_ENTRY(bmap_pagecache_entry) bmpce_tentry;
	struct psc_listentry	 bmpce_lentry;	/* chain on bmap LRU		*/
};

/* bmpce_flags */
#define BMPCE_DATARDY		(1 <<  0)
#define BMPCE_FAULTING		(1 <<  1)
#define BMPCE_LRU		(1 <<  2)
#define BMPCE_TOFREE		(1 <<  3)
#define BMPCE_EIO		(1 <<  4)	/* I/O error */
#define BMPCE_READA		(1 <<  5)	/* brought in by readahead logic */
#define BMPCE_AIOWAIT		(1 <<  6)	/* wait on async read */
#define BMPCE_DISCARD		(1 <<  7)	/* don't cache after I/O */
#define BMPCE_PINNED		(1 <<  8)	/* do not modify */

#define BMPCE_LOCK(e)		spinlock(&(e)->bmpce_lock)
#define BMPCE_ULOCK(e)		freelock(&(e)->bmpce_lock)
#define BMPCE_RLOCK(e)		reqlock(&(e)->bmpce_lock)
#define BMPCE_TRYLOCK(e)	trylock(&(e)->bmpce_lock)
#define BMPCE_URLOCK(e, lk)	ureqlock(&(e)->bmpce_lock, (lk))
#define BMPCE_LOCK_ENSURE(e)	LOCK_ENSURE(&(e)->bmpce_lock)

#define BMPCE_WAIT(e)		psc_waitq_wait((e)->bmpce_waitq, &(e)->bmpce_lock)

/* XXX: introduce a flag to avoid unconditional wakeup */
#define BMPCE_WAKE(e)							\
	do {								\
		if ((e)->bmpce_waitq) {					\
			psc_waitq_wakeall((e)->bmpce_waitq);		\
			DEBUG_BMPCE(PLL_DEBUG, (e), "wakeup");		\
		} else							\
			DEBUG_BMPCE(PLL_DIAG, (e), "NULL bmpce_waitq");	\
	} while (0)

#define BMPCE_SETATTR(e, fl, ...)					\
	do {								\
		int _locked;						\
									\
		_locked = BMPCE_RLOCK(e);				\
		(e)->bmpce_flags |= (fl);				\
		DEBUG_BMPCE(PLL_DIAG, (e), ##__VA_ARGS__);		\
		BMPCE_URLOCK((e), _locked);				\
	} while (0)

#define DEBUG_BMPCE(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP,					\
	    "bmpce@%p fl=%u:%s%s%s%s%s%s%s%s "				\
	    "o=%#x b=%p "						\
	    "ts="PSCPRI_PTIMESPEC" "					\
	    "ref=%u : " fmt,						\
	    (b), (b)->bmpce_flags,					\
	    (b)->bmpce_flags & BMPCE_DATARDY		? "d" : "",	\
	    (b)->bmpce_flags & BMPCE_FAULTING		? "f" : "",	\
	    (b)->bmpce_flags & BMPCE_LRU		? "l" : "",	\
	    (b)->bmpce_flags & BMPCE_TOFREE		? "T" : "",	\
	    (b)->bmpce_flags & BMPCE_EIO		? "E" : "",	\
	    (b)->bmpce_flags & BMPCE_READA		? "a" : "",	\
	    (b)->bmpce_flags & BMPCE_AIOWAIT		? "w" : "",	\
	    (b)->bmpce_flags & BMPCE_DISCARD		? "D" : "",	\
	    (b)->bmpce_off, (b)->bmpce_base,				\
	    PSCPRI_TIMESPEC_ARGS(&(b)->bmpce_laccess),			\
	    psc_atomic32_read(&(b)->bmpce_ref),				\
	    ## __VA_ARGS__)

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
	char			*biorq_buf;
	int32_t			 biorq_ref;
	/*
	 * Note that a request may fall somewhere within a bmap.  It
	 * might be not page aligned.
	 */
	uint32_t		 biorq_off;	/* filewise, bmap relative	*/
	uint32_t		 biorq_len;	/* length of the original req	*/
	uint32_t		 biorq_flags;	/* state and op type bits	*/
	uint32_t		 biorq_retries;	/* dirty data flush retries	*/
	sl_ios_id_t		 biorq_last_sliod;
	psc_spinlock_t		 biorq_lock;
	struct timespec		 biorq_expire;
	struct psc_dynarray	 biorq_pages;	/* array of bmpce		*/
	struct psclist_head	 biorq_lentry;	/* chain on bmpc_pndg_biorqs	*/
	struct psclist_head	 biorq_png_lentry;
	RB_ENTRY(bmpc_ioreq)	 biorq_tentry;	/* redblack tree membership     */
	struct bmapc_memb	*biorq_bmap;	/* backpointer to our bmap	*/
	struct msl_fsrqinfo	*biorq_fsrqi;	/* NULL for internal read-ahead */
};

#define	BIORQ_READ		(1 <<  0)
#define	BIORQ_WRITE		(1 <<  1)
#define	BIORQ_SCHED		(1 <<  2)	/* flush in progress, don't clear unless retry happen */
#define	BIORQ_DIO		(1 <<  3)
#define	BIORQ_EXPIRE		(1 <<  4)
#define	BIORQ_DESTROY		(1 <<  5)
#define	BIORQ_FLUSHRDY		(1 <<  6)
#define BIORQ_FREEBUF		(1 <<  7)	/* DIO READ needs a buffer */
#define BIORQ_WAIT		(1 <<  8)
#define BIORQ_READAHEAD		(1 <<  9)	/* performed by readahead */

#define BIORQ_LOCK(r)		spinlock(&(r)->biorq_lock)
#define BIORQ_ULOCK(r)		freelock(&(r)->biorq_lock)
#define BIORQ_RLOCK(r)		reqlock(&(r)->biorq_lock)
#define BIORQ_URLOCK(r, lk)	ureqlock(&(r)->biorq_lock, (lk))
#define BIORQ_LOCK_ENSURE(r)	LOCK_ENSURE(&(r)->biorq_lock)

#define BIORQ_SETATTR(r, fl)	SETATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))
#define BIORQ_CLEARATTR(r, fl)	CLEARATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))

#define DEBUGS_BIORQ(level, ss, r, fmt, ...)				\
	psclogs((level), (ss), "biorq@%p flg=%#x:%s%s%s%s%s%s%s%s "	\
	    "ref=%d off=%u len=%u "					\
	    "retry=%u buf=%p rqi=%p pfr=%p "				\
	    "sliod=%x npages=%d "					\
	    "bmap=%p exp="PSCPRI_TIMESPEC" : "fmt,			\
	    (r), (r)->biorq_flags,					\
	    (r)->biorq_flags & BIORQ_READ		? "r" : "",	\
	    (r)->biorq_flags & BIORQ_WRITE		? "w" : "",	\
	    (r)->biorq_flags & BIORQ_SCHED		? "s" : "",	\
	    (r)->biorq_flags & BIORQ_DIO		? "d" : "",	\
	    (r)->biorq_flags & BIORQ_EXPIRE		? "x" : "",	\
	    (r)->biorq_flags & BIORQ_DESTROY		? "D" : "",	\
	    (r)->biorq_flags & BIORQ_FLUSHRDY		? "L" : "",	\
	    (r)->biorq_flags & BIORQ_WAIT		? "W" : "",	\
	    (r)->biorq_ref, (r)->biorq_off, (r)->biorq_len,		\
	    (r)->biorq_retries, (r)->biorq_buf, (r)->biorq_fsrqi,	\
	    (r)->biorq_fsrqi ? mfsrq_2_pfr((r)->biorq_fsrqi) : NULL,	\
	    (r)->biorq_last_sliod, psc_dynarray_len(&(r)->biorq_pages),	\
	    (r)->biorq_bmap, PSCPRI_TIMESPEC_ARGS(&(r)->biorq_expire),	\
	    ## __VA_ARGS__)

#define DEBUG_BIORQ(level, r, fmt, ...)					\
	DEBUGS_BIORQ((level), SLSS_BMAP, (r), fmt, ## __VA_ARGS__)

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

RB_HEAD(bmpc_biorq_tree, bmpc_ioreq);
RB_PROTOTYPE(bmpc_biorq_tree, bmpc_ioreq, biorq_tentry, bmpc_biorq_cmp)

struct bmap_pagecache {
	struct bmap_pagecachetree	 bmpc_tree;		/* tree of entries */
	struct timespec			 bmpc_oldest;		/* LRU's oldest item */
	struct psc_lockedlist		 bmpc_lru;		/* cleancnt can be kept here  */
	struct psc_waitq		 bmpc_waitq;

	/*
	 * List for new requests minus BIORQ_READ and BIORQ_DIO.  All
	 * requests are sorted based on their starting offsets to
	 * facilitate write coalescing.
	 *
	 * The tree is protected by the bmap lock.
	 */
	struct bmpc_biorq_tree		 bmpc_new_biorqs;
	struct psc_lockedlist		 bmpc_pndg_biorqs;	/* chain pending I/O requests */
	struct psclist_head		 bmpc_lentry;		/* chain to global LRU lc */
};

struct bmpc_write_coalescer {
	size_t				 bwc_size;
	off_t				 bwc_soff;
	struct psc_dynarray		 bwc_biorqs;
	struct iovec			 bwc_iovs[BMPC_COALESCE_MAX_IOV];
	struct bmap_pagecache_entry	*bwc_bmpces[BMPC_COALESCE_MAX_IOV];
	int				 bwc_niovs;
	int				 bwc_nbmpces;
	struct psclist_head		 bwc_lentry;
};

static __inline void
bmpce_usecheck(struct bmap_pagecache_entry *bmpce, __unusedx int op, uint32_t off)
{
	psc_assert(psc_atomic32_read(&bmpce->bmpce_ref) > 0);
	psc_assert(bmpce->bmpce_off == off);
}

#define biorq_getaligned_off(r, nbmpce)					\
	(((r)->biorq_off & ~BMPC_BUFMASK) + ((nbmpce) * BMPC_BUFSZ))

#define biorq_voff_get(r)	((r)->biorq_off + (r)->biorq_len)

void	 bmpc_global_init(void);
void	 bmpc_freeall_locked(struct bmap_pagecache *);
void	 bmpc_biorqs_flush(struct bmapc_memb *, int);
void	 bmpc_biorqs_destroy_locked(struct bmapc_memb *, int);

struct bmpc_ioreq *
	 bmpc_biorq_new(struct msl_fsrqinfo *, struct bmapc_memb *,
	    char *, int, uint32_t, uint32_t, int);

int	 bmpce_init(struct psc_poolmgr *, void *);
struct bmap_pagecache_entry *
	 bmpce_lookup_locked(struct bmapc_memb *, uint32_t, struct psc_waitq *);

void	 bmpce_release_locked(struct bmap_pagecache_entry *,
	    struct bmap_pagecache *);

void	 bwc_release(struct bmpc_write_coalescer *);

extern struct psc_poolmgr	*bmpce_pool;
extern struct psc_poolmgr	*bwc_pool;
extern struct psc_listcache	 bmpcLru;

extern struct timespec		bmapFlushDefMaxAge;

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

	/* Double check the exclusivity of these lists... */

	pll_init(&bmpc->bmpc_lru, struct bmap_pagecache_entry,
	    bmpce_lentry, NULL);

	pll_init(&bmpc->bmpc_pndg_biorqs, struct bmpc_ioreq,
	    biorq_lentry, NULL);

	psc_waitq_init(&bmpc->bmpc_waitq);

	RB_INIT(&bmpc->bmpc_new_biorqs);

	PFL_GETTIMESPEC(&bmpc->bmpc_oldest);
	lc_addtail(&bmpcLru, bmpc);
}

#endif /* _SL_BMPC_H_ */
