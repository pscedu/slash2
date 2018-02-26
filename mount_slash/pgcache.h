/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * Block map page cache definitions - for managing the in-memory store
 * of file regions (bmaps).
 */

#ifndef _MSL_PGCACHE_H_
#define _MSL_PGCACHE_H_

#include <time.h>

#include "pfl/atomic.h"
#include "pfl/fs.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/lockedlist.h"
#include "pfl/pool.h"
#include "pfl/time.h"
#include "pfl/tree.h"
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
	struct bmap		*bmpce_bmap;
	int16_t			 bmpce_rc;	/* unused now, will purge */
	int16_t			 bmpce_ref;	/* reference count */
	uint16_t		 bmpce_flags;	/* BMPCEF_* flag bits */
#if BMPC_BUFSZ > 65536
#error bump bmpce_len
#endif
	uint16_t		 bmpce_len;
	uint32_t		 bmpce_off;	/* relative to inside bmap */
	uint32_t		 bmpce_start;	/* region where data are valid */
	 int16_t		 bmpce_pins;	/* page contents are read-only */
	psc_spinlock_t		 bmpce_lock;
	struct bmap_page_entry	*bmpce_entry;	/* statically allocated pg contents */
	struct psc_waitq	*bmpce_waitq;	/* others block here on I/O */
	struct psc_lockedlist	 bmpce_pndgaios;
	RB_ENTRY(bmap_pagecache_entry) bmpce_tentry;
	struct psc_listentry	 bmpce_lentry;	/* chain on bmap LRU */
};

#define	PAGE_CANFREE		0x01
#define	PAGE_MADVISE		0x02

struct bmap_page_entry {
	struct psc_listentry	 page_lentry;
	int			 page_flag;
	void			*page_buf;
};

/* bmpce_flags */
#define BMPCEF_DATARDY		(1 <<  0)	/* data loaded in memory */
#define BMPCEF_FAULTING		(1 <<  1)	/* loading via RPC */
#define BMPCEF_TOFREE		(1 <<  2)	/* eviction in progress */
#define BMPCEF_EIO		(1 <<  3)	/* I/O error */
#define BMPCEF_LRU              (1 <<  4)       /* on LRU list */
#define BMPCEF_AIOWAIT		(1 <<  5)	/* wait on async read */
#define BMPCEF_DISCARD		(1 <<  6)	/* don't cache after I/O is done */
#define BMPCEF_READAHEAD	(1 <<  7)	/* populated from readahead */
#define BMPCEF_ACCESSED		(1 <<  8)	/* bmpce was used before reap (readahead) */
#define BMPCEF_IDLE		(1 <<  9)	/* on idle_pages listcache */
#define BMPCEF_REAPED		(1 << 10)	/* reaper has removed us from LRU listcache */
#define BMPCEF_READALC		(1 << 11)	/* on readahead_pages listcache */

#define BMPCE_LOCK(e)		spinlock(&(e)->bmpce_lock)
#define BMPCE_ULOCK(e)		freelock(&(e)->bmpce_lock)
#define BMPCE_RLOCK(e)		reqlock(&(e)->bmpce_lock)
#define BMPCE_TRYLOCK(e)	trylock(&(e)->bmpce_lock)
#define BMPCE_URLOCK(e, lk)	ureqlock(&(e)->bmpce_lock, (lk))
#define BMPCE_LOCK_ENSURE(e)	LOCK_ENSURE(&(e)->bmpce_lock)

#define BMPCE_WAIT(e)		psc_waitq_wait((e)->bmpce_waitq, &(e)->bmpce_lock)

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

#define DEBUG_BMPCE(level, pg, fmt, ...)				\
	psclogs((level), SLSS_BMAP,					\
	    "bmpce@%p fcmh=%p fid="SLPRI_FID" "				\
	    "fl=%#x:%s%s%s%s%s%s%s%s%s%s%s "				\
	    "off=%#09x entry=%p ref=%u : " fmt,				\
	    (pg), (pg)->bmpce_bmap->bcm_fcmh,				\
	    fcmh_2_fid((pg)->bmpce_bmap->bcm_fcmh), (pg)->bmpce_flags,	\
	    (pg)->bmpce_flags & BMPCEF_DATARDY		? "d" : "",	\
	    (pg)->bmpce_flags & BMPCEF_FAULTING		? "f" : "",	\
	    (pg)->bmpce_flags & BMPCEF_TOFREE		? "t" : "",	\
	    (pg)->bmpce_flags & BMPCEF_EIO		? "e" : "",	\
	    (pg)->bmpce_flags & BMPCEF_AIOWAIT		? "w" : "",	\
	    (pg)->bmpce_flags & BMPCEF_DISCARD		? "D" : "",	\
	    (pg)->bmpce_flags & BMPCEF_READAHEAD	? "r" : "",	\
	    (pg)->bmpce_flags & BMPCEF_ACCESSED		? "a" : "",	\
	    (pg)->bmpce_flags & BMPCEF_IDLE		? "i" : "",	\
	    (pg)->bmpce_flags & BMPCEF_REAPED		? "X" : "",	\
	    (pg)->bmpce_flags & BMPCEF_READALC		? "R" : "",	\
	    (pg)->bmpce_off, (pg)->bmpce_entry,				\
	    (pg)->bmpce_ref, ## __VA_ARGS__)

static __inline int
bmpce_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache_entry *a = x, *b = y;

	return (CMP(a->bmpce_off, b->bmpce_off));
}

RB_HEAD(bmap_pagecachetree, bmap_pagecache_entry);
RB_PROTOTYPE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
    bmpce_cmp)

struct bmpc_ioreq {
	char			*biorq_buf;
	int32_t			 biorq_ref;
	/*
	 * Note that a request may fall somewhere within a bmap.  It
	 * might be not page aligned.
	 */
	uint32_t		 biorq_off;	/* bmap relative */
	uint32_t		 biorq_len;	/* length of the original req */
	uint32_t		 biorq_flags;	/* state and op type bits */
	int		 	 biorq_retries;	/* dirty data flush retries */
	sl_ios_id_t		 biorq_last_sliod;
	psc_spinlock_t		 biorq_lock;
	struct timespec		 biorq_expire;
	struct psc_dynarray	 biorq_pages;	/* array of bmpce */
	struct psc_listentry	 biorq_lentry;	/* chain on bmpc_pndg_biorqs */
	struct psc_listentry	 biorq_exp_lentry;/* chain on bmpc_biorqs_exp */
	struct psc_listentry	 biorq_aio_lentry;/* chain on bmpce's aios */
	RB_ENTRY(bmpc_ioreq)	 biorq_tentry;	/* indexed on offset for write coalescing */
	struct bmap		*biorq_bmap;	/* backpointer to our bmap */
	struct msl_fsrqinfo	*biorq_fsrqi;	/* NULL for internal read-ahead */
};

#define BIORQ_READ		(1 <<  0)	/* request is for an application read(2) */
#define BIORQ_WRITE		(1 <<  1)	/* request is for an application write(2) */
#define BIORQ_DIO		(1 <<  2)	/* direct I/O (no buffering) */
#define BIORQ_EXPIRE		(1 <<  3)	/* buffered write should be flushed now */
#define BIORQ_DESTROY		(1 <<  4)	/* debug: returned to system */
#define BIORQ_FLUSHRDY		(1 <<  5)	/* write buffer is filled */
#define BIORQ_FREEBUF		(1 <<  6)	/* DIO READ needs a buffer */
#define BIORQ_WAIT		(1 <<  7)	/* at least one waiter for aio */
#define BIORQ_ONTREE		(1 <<  8)	/* on bmpc_biorqs rbtree */
#define BIORQ_READAHEAD		(1 <<  9)	/* performed by readahead */
#define BIORQ_AIOWAKE		(1 << 10)	/* aio needs wakeup */

#define BIORQ_LOCK(r)		spinlock(&(r)->biorq_lock)
#define BIORQ_ULOCK(r)		freelock(&(r)->biorq_lock)
#define BIORQ_RLOCK(r)		reqlock(&(r)->biorq_lock)
#define BIORQ_URLOCK(r, lk)	ureqlock(&(r)->biorq_lock, (lk))
#define BIORQ_LOCK_ENSURE(r)	LOCK_ENSURE(&(r)->biorq_lock)

#define BIORQ_SETATTR(r, fl)	SETATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))
#define BIORQ_CLEARATTR(r, fl)	CLEARATTR_LOCKED(&(r)->biorq_lock, &(r)->biorq_flags, (fl))

#define DEBUGS_BIORQ(level, ss, r, fmt, ...)				\
	psclogs((level), (ss), "biorq@%p "				\
	    "flg=%#x:%s%s%s%s%s%s%s%s%s%s%s "				\
	    "ref=%d off=%u len=%u "					\
	    "retry=%u buf=%p rqi=%p pfr=%p "				\
	    "sliod=%x npages=%d "					\
	    "bmap=%p exp="PSCPRI_TIMESPEC" : "fmt,			\
	    (r), (r)->biorq_flags,					\
	    (r)->biorq_flags & BIORQ_READ		? "r" : "",	\
	    (r)->biorq_flags & BIORQ_WRITE		? "w" : "",	\
	    (r)->biorq_flags & BIORQ_DIO		? "d" : "",	\
	    (r)->biorq_flags & BIORQ_EXPIRE		? "x" : "",	\
	    (r)->biorq_flags & BIORQ_DESTROY		? "D" : "",	\
	    (r)->biorq_flags & BIORQ_FLUSHRDY		? "l" : "",	\
	    (r)->biorq_flags & BIORQ_FREEBUF		? "f" : "",	\
	    (r)->biorq_flags & BIORQ_WAIT		? "W" : "",	\
	    (r)->biorq_flags & BIORQ_ONTREE		? "t" : "",	\
	    (r)->biorq_flags & BIORQ_READAHEAD		? "a" : "",	\
	    (r)->biorq_flags & BIORQ_AIOWAKE		? "k" : "",	\
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
	struct psc_lockedlist		 bmpc_lru;

	/*
	 * List for new requests minus BIORQ_READ and BIORQ_DIO.  All
	 * requests are sorted based on their starting offsets to
	 * facilitate write coalescing.
	 *
	 * The tree is protected by the bmap lock.
	 */
	int				 bmpc_pndg_writes;
	struct bmpc_biorq_tree		 bmpc_biorqs;
	struct psc_lockedlist		 bmpc_biorqs_exp;	/* flush/expire */
	struct psc_lockedlist		 bmpc_pndg_biorqs;	/* all requests */
	struct psc_listentry		 bmpc_lentry;		/* chain to global LRU lc */
};

struct bmpc_write_coalescer {
	size_t				 bwc_size;
	off_t				 bwc_soff;
	struct psc_dynarray		 bwc_biorqs;
	struct iovec			 bwc_iovs[BMPC_COALESCE_MAX_IOV];
	struct bmap_pagecache_entry	*bwc_bmpces[BMPC_COALESCE_MAX_IOV];
	int				 bwc_niovs;
	int				 bwc_nbmpces;
	struct psc_listentry		 bwc_lentry;
};

static __inline void
bmpce_usecheck(struct bmap_pagecache_entry *bmpce, __unusedx int op,
    uint32_t off)
{
	psc_assert(bmpce->bmpce_ref > 0);
	psc_assert(bmpce->bmpce_off == off);
}

#define biorq_getaligned_off(r, nbmpce)					\
	(((r)->biorq_off & ~BMPC_BUFMASK) + ((nbmpce) * BMPC_BUFSZ))

#define biorq_voff_get(r)	((r)->biorq_off + (r)->biorq_len)

void	 bmap_pagecache_destroy(void);

void	 bmpc_global_init(void);
void	 bmpc_freeall(struct bmap *);
void	 bmpc_biorqs_flush(struct bmap *);

void	bmpc_expire_biorqs(struct bmap_pagecache *);

struct bmpc_ioreq *
	 bmpc_biorq_new(struct msl_fsrqinfo *, struct bmap *,
	    char *, uint32_t, uint32_t, int);

int      bmpce_lookup(struct bmpc_ioreq *,
             struct bmap *, int, uint32_t, struct psc_waitq *);

void	 bmpce_init(struct bmap_pagecache_entry *);
void     bmpce_release_locked(struct bmap_pagecache_entry *,
            struct bmap_pagecache *);

struct bmpc_write_coalescer *	 bwc_alloc(void);
void				 bwc_free(struct bmpc_write_coalescer *);

void	bmpce_free(struct bmap_pagecache_entry *, struct bmap_pagecache *);

extern struct psc_poolmgr	*bmpce_pool;
extern struct psc_poolmgr	*bwc_pool;

extern struct timespec		 msl_bflush_maxage;
extern struct psc_listcache	 msl_idle_pages;
extern struct psc_listcache	 msl_readahead_pages;

extern struct psc_listcache	 bmpcLru;

static __inline void
bmpc_init(struct bmap_pagecache *bmpc)
{
	memset(bmpc, 0, sizeof(*bmpc));

	INIT_PSC_LISTENTRY(&bmpc->bmpc_lentry);

	pll_init(&bmpc->bmpc_lru, struct bmap_pagecache_entry,
	    bmpce_lentry, NULL);

	/* Double check the exclusivity of these lists... */
	pll_init(&bmpc->bmpc_pndg_biorqs, struct bmpc_ioreq,
	    biorq_lentry, NULL);
	pll_init(&bmpc->bmpc_biorqs_exp, struct bmpc_ioreq,
	    biorq_exp_lentry, NULL);

	RB_INIT(&bmpc->bmpc_biorqs);

	lc_addtail(&bmpcLru, bmpc);
}

#endif /* _MSL_PGCACHE_H_ */
