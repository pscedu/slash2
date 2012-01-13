/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Block map page cache definitions - for managing the in-memory store
 * of file regions (bmaps).
 */

#ifndef _SL_BMPC_H_
#define _SL_BMPC_H_

#include <time.h>

#include "pfl/time.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/lockedlist.h"
#include "psc_ds/tree.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"
#include "psc_util/waitq.h"

#include "cache_params.h"
#include "bmap.h"
#include "buffer.h"

struct msl_fhent;
struct msl_fsrqinfo;

#define BMPC_BUFSZ		SLASH_SLVR_BLKSZ
#define BMPC_BLKSZ		BMPC_BUFSZ
#define BMPC_DEFSLBS		1
#define BMPC_MAXSLBS		32
#define BMPC_BUFMASK		(BMPC_BLKSZ - 1)
#define BMPC_IOMAXBLKS		64
#define BMPC_MAXBUFSRPC		(1024 * 1024 / BMPC_BUFSZ)

#define BMPC_DEF_MINAGE		{ 0, 600000000 } /* seconds, nanoseconds */
#define BMPC_INTERVAL		{ 0, 200000000 }

struct timespec			bmapFlushDefMaxAge;

struct bmap_pagecache_entry {
	psc_atomic16_t		 bmpce_wrref;	/* pending write ops		*/
	psc_atomic16_t		 bmpce_rdref;	/* pending read ops		*/
	uint64_t		 bmpce_syncxid;	/* xid associated with sync op  */
	uint32_t		 bmpce_flags;	/* BMPCE_* flag bits		*/
	uint32_t		 bmpce_off;	/* filewise, bmap relative	*/
	psc_spinlock_t		 bmpce_lock;	/* serialize			*/
	void			*bmpce_base;	/* base pointer from slb	*/
	void			*bmpce_owner;
	struct psc_waitq	*bmpce_waitq;	/* others block here on I/O	*/
	struct timespec		 bmpce_laccess;	/* last page access		*/
	struct psc_listentry	 bmpce_ralentry; /* queue read ahead		*/
	struct psc_lockedlist	 bmpce_pndgaios;
	SPLAY_ENTRY(bmap_pagecache_entry) bmpce_tentry;
	struct psc_listentry	 bmpce_lentry;	/* chain on bmap lru		*/
};

#define	BMPCE_NEW		(1 << 0)	/* 0x00001 */
#define	BMPCE_GETBUF		(1 << 1)	/* 0x00002 */
#define	BMPCE_DATARDY		(1 << 2)	/* 0x00004 */
#define	BMPCE_DIRTY2LRU		(1 << 3)	/* 0x00008 */
#define	BMPCE_LRU		(1 << 4)	/* 0x00010 */
#define	BMPCE_FREE		(1 << 5)	/* 0x00020 */
#define	BMPCE_FREEING		(1 << 6)	/* 0x00040 */
#define	BMPCE_INIT		(1 << 7)	/* 0x00080 */
#define	BMPCE_READPNDG		(1 << 8)	/* 0x00100: Pending read */
#define	BMPCE_RBWPAGE		(1 << 9)	/* 0x00200 */
#define	BMPCE_RBWRDY		(1 << 10)	/* 0x00400 */
#define	BMPCE_INFLIGHT		(1 << 11)	/* 0x00800: I/O in progress */
#define	BMPCE_EIO		(1 << 12)	/* 0x01000: I/O error */
#define BMPCE_READA		(1 << 13)	/* 0x02000: read-ahead */
#define BMPCE_AIOWAIT		(1 << 14)	/* 0x04000: wait on async read */
#define BMPCE_SYNCWAIT		(1 << 15)	/* 0x10000: wait on sliod sync */

#define BMPCE_LOCK(b)		spinlock(&(b)->bmpce_lock)
#define BMPCE_ULOCK(b)		freelock(&(b)->bmpce_lock)
#define BMPCE_RLOCK(b)		reqlock(&(b)->bmpce_lock)
#define BMPCE_URLOCK(b, lk)	ureqlock(&(b)->bmpce_lock, (lk))
#define BMPCE_LOCK_ENSURE(b)	LOCK_ENSURE(&(b)->bmpce_lock)

#define BMPCE_WAIT(b)		psc_waitq_wait((b)->bmpce_waitq, &(b)->bmpce_lock)
#define BMPCE_WAKE(b)							\
	do {								\
		if ((b)->bmpce_waitq)					\
			psc_waitq_wakeall((b)->bmpce_waitq);		\
		else							\
			DEBUG_BMPCE(PLL_MAX, b, "NULL bmpce_waitq");	\
	} while (0)

#define BMPCE_2_BIORQ(b)						\
	((b)->bmpce_waitq ? (char *)(b)->bmpce_waitq -			\
	 offsetof(struct bmpc_ioreq, biorq_waitq) : NULL)

#define BMPCE_SETATTR(bmpce, fl, ...)					\
	do {								\
		int _locked;						\
									\
		_locked = BMPCE_RLOCK(bmpce);				\
		(bmpce)->bmpce_flags |= (fl);				\
		DEBUG_BMPCE(PLL_INFO, (bmpce), ##__VA_ARGS__);		\
		BMPCE_URLOCK((bmpce), _locked);				\
	} while (0)

#define BMPCE_FLAGS_FORMAT "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s"
#define DEBUG_BMPCE_FLAGS(b)						\
	(b)->bmpce_flags & BMPCE_NEW			? "n" : "",	\
	(b)->bmpce_flags & BMPCE_DATARDY		? "d" : "",	\
	(b)->bmpce_flags & BMPCE_DIRTY2LRU		? "D" : "",	\
	(b)->bmpce_flags & BMPCE_LRU			? "l" : "",	\
	(b)->bmpce_flags & BMPCE_FREE			? "f" : "",	\
	(b)->bmpce_flags & BMPCE_FREEING		? "F" : "",	\
	(b)->bmpce_flags & BMPCE_INIT			? "i" : "",	\
	(b)->bmpce_flags & BMPCE_READPNDG		? "r" : "",	\
	(b)->bmpce_flags & BMPCE_RBWPAGE		? "B" : "",	\
	(b)->bmpce_flags & BMPCE_RBWRDY			? "R" : "",	\
	(b)->bmpce_flags & BMPCE_INFLIGHT		? "L" : "",	\
	(b)->bmpce_flags & BMPCE_EIO			? "E" : "",	\
	(b)->bmpce_flags & BMPCE_READA			? "a" : "",	\
	(b)->bmpce_flags & BMPCE_AIOWAIT		? "w" : "",	\
	(b)->bmpce_flags & BMPCE_SYNCWAIT		? "S" : ""

#define DEBUG_BMPCE(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP,					\
	    "bmpce@%p fl=%u:"BMPCE_FLAGS_FORMAT" "			\
	    "o=%#x b=%p "						\
	    "ts="PSCPRI_TIMESPEC" "					\
	    "wr=%hu rd=%hu "						\
	    "owner=%p : " fmt,						\
	    (b), (b)->bmpce_flags, DEBUG_BMPCE_FLAGS(b),		\
	    (b)->bmpce_off, (b)->bmpce_base,				\
	    PSCPRI_TIMESPEC_ARGS(&(b)->bmpce_laccess),			\
	    psc_atomic16_read(&(b)->bmpce_wrref),			\
	    psc_atomic16_read(&(b)->bmpce_rdref),			\
	    (b)->bmpce_owner, ## __VA_ARGS__)

static __inline int
bmpce_lrusort_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache_entry * const *pa = x, *a = *pa;
	const struct bmap_pagecache_entry * const *pb = y, *b = *pb;

	if (timespeccmp(&a->bmpce_laccess, &b->bmpce_laccess, <))
		return (-1);

	if (timespeccmp(&a->bmpce_laccess, &b->bmpce_laccess, >))
		return (1);

	return (0);
}

static __inline int
bmpce_lrusort_cmp1(const void *x, const void *y)
{
	const struct bmap_pagecache_entry *a = x;
	const struct bmap_pagecache_entry *b = y;

	if (timespeccmp(&a->bmpce_laccess, &b->bmpce_laccess, >))
		return (-1);

	if (timespeccmp(&a->bmpce_laccess, &b->bmpce_laccess, <))
		return (1);

	return (0);
}

static __inline int
bmpce_cmp(const void *x, const void *y)
{
	const struct bmap_pagecache_entry *a = x, *b = y;

	return (CMP(a->bmpce_off, b->bmpce_off));
}

SPLAY_HEAD(bmap_pagecachetree, bmap_pagecache_entry);
SPLAY_PROTOTYPE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry,
		bmpce_cmp);

struct bmap_pagecache {
	struct bmap_pagecachetree	 bmpc_tree;		/* tree of cbuf_handle */
	struct timespec			 bmpc_oldest;		/* LRU's oldest item */
	struct psc_lockedlist		 bmpc_lru;		/* cleancnt can be kept here  */
	struct psc_lockedlist		 bmpc_new_biorqs;
	struct psc_lockedlist		 bmpc_pndg_biorqs;	/* chain pending I/O requests */
	struct psc_lockedlist		 bmpc_pndg_ra;		/* RA bmpce's pending comp */
	atomic_t			 bmpc_pndgwr;		/* # pending wr req */
	int                              bmpc_compwr;           /* # of completed writes */
	psc_spinlock_t			 bmpc_lock;		/* serialize access to splay tree and locked lists  */
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
bmpc_queued_writes(struct bmap_pagecache *bmpc)
{
	int nwrites = atomic_read(&bmpc->bmpc_pndgwr);

	psc_assert(nwrites >= 0);

	if (nwrites > 0)
		psc_assert((pll_nitems(&bmpc->bmpc_pndg_biorqs) > 0) ||
			   (pll_nitems(&bmpc->bmpc_new_biorqs) > 0));

	return (nwrites);
}

static __inline int
bmpc_queued_ios(struct bmap_pagecache *bmpc)
{
	return (pll_nitems(&bmpc->bmpc_pndg_biorqs) +
		pll_nitems(&bmpc->bmpc_pndg_ra) +
		pll_nitems(&bmpc->bmpc_new_biorqs));
}

struct bmpc_ioreq {
	uint32_t			 biorq_off;	/* filewise, bmap relative	*/
	uint32_t			 biorq_len;	/* non-aligned, real length	*/
	uint32_t			 biorq_flags;	/* state and op type bits	*/
	uint32_t                         biorq_retries;
	sl_ios_id_t                      biorq_last_sliod;
	psc_spinlock_t			 biorq_lock;
	struct timespec			 biorq_issue;	/* time to initiate I/O		*/
	struct timespec			 biorq_expire;
	struct psc_dynarray		 biorq_pages;	/* array of bmpce		*/
	struct psclist_head		 biorq_lentry;	/* chain on bmpc_pndg_biorqs	*/
	struct psclist_head		 biorq_mfh_lentry; /* chain on file handle	*/
	struct psclist_head		 biorq_bwc_lentry;
	struct bmapc_memb		*biorq_bmap;	/* backpointer to our bmap	*/
	struct pscrpc_request_set	*biorq_rqset;
	struct psc_waitq		 biorq_waitq;
	struct msl_fhent		*biorq_fhent;	/* back pointer to msl_fhent */
	struct msl_fsrqinfo		*biorq_fsrqi;
};

#define	BIORQ_READ			(1 <<  0)
#define	BIORQ_WRITE			(1 <<  1)
#define	BIORQ_RBWFP			(1 <<  2)	/* read before write - first page */
#define	BIORQ_RBWLP			(1 <<  3)	/* read before write - last page */
#define	BIORQ_SCHED			(1 <<  4)
#define	BIORQ_INFL			(1 <<  5)
#define	BIORQ_DIO			(1 <<  6)
#define	BIORQ_FORCE_EXPIRE		(1 <<  7)
#define	BIORQ_DESTROY			(1 <<  8)
#define	BIORQ_FLUSHRDY			(1 <<  9)
#define	BIORQ_NOFHENT			(1 << 10)	/* release a file handle before flush is complete */
#define BIORQ_APPEND			(1 << 11)
#define BIORQ_READAHEAD			(1 << 12)
#define BIORQ_RBWFAIL			(1 << 13)
#define BIORQ_AIOWAIT			(1 << 14)
#define BIORQ_RESCHED			(1 << 15)
#define BIORQ_ARCHIVER			(1 << 16)
#define BIORQ_FLUSHABORT		(1 << 17)
#define BIORQ_EXPIREDLEASE		(1 << 18)
#define BIORQ_MAXRETRIES		(1 << 19)	/* too many retries.. */
#define BIORQ_BMAPFAIL			(1 << 20)
#define BIORQ_READFAIL			(1 << 21)

#define BIORQ_LOCK(r)			spinlock(&(r)->biorq_lock)
#define BIORQ_ULOCK(r)			freelock(&(r)->biorq_lock)
#define BIORQ_RLOCK(r)			reqlock(&(r)->biorq_lock)
#define BIORQ_URLOCK(r)			ureqlock(&(r)->biorq_lock)
#define BIORQ_LOCK_ENSURE(r)		LOCK_ENSURE(&(r)->biorq_lock)

#define DEBUG_BIORQ(level, b, fmt, ...)					\
	psclogs((level), SLSS_BMAP, "biorq@%p "				\
	    "fl=%#x:%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s "	\
	    "o=%u l=%u r=%u sliod=%x "					\
	    "np=%d b=%p "						\
	    "ex="PSCPRI_TIMESPEC" : "fmt,				\
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
	    (b)->biorq_flags & BIORQ_FLUSHRDY		? "r" : "",	\
	    (b)->biorq_flags & BIORQ_NOFHENT		? "n" : "",	\
	    (b)->biorq_flags & BIORQ_APPEND		? "A" : "",	\
	    (b)->biorq_flags & BIORQ_READAHEAD		? "a" : "",	\
	    (b)->biorq_flags & BIORQ_RBWFAIL		? "F" : "",	\
	    (b)->biorq_flags & BIORQ_AIOWAIT		? "W" : "",	\
	    (b)->biorq_flags & BIORQ_RESCHED		? "R" : "",	\
	    (b)->biorq_flags & BIORQ_ARCHIVER		? "c" : "",	\
	    (b)->biorq_flags & BIORQ_FLUSHABORT		? "B" : "",	\
	    (b)->biorq_flags & BIORQ_EXPIREDLEASE	? "X" : "",	\
	    (b)->biorq_flags & BIORQ_MAXRETRIES		? "x" : "",	\
	    (b)->biorq_flags & BIORQ_BMAPFAIL		? "b" : "",	\
	    (b)->biorq_flags & BIORQ_READFAIL		? "E" : "",	\
	    (b)->biorq_off, (b)->biorq_len, (b)->biorq_retries,		\
	    (b)->biorq_last_sliod,					\
	    psc_dynarray_len(&(b)->biorq_pages), (b)->biorq_bmap,	\
	    PSCPRI_TIMESPEC_ARGS(&(b)->biorq_expire), ## __VA_ARGS__)

#define biorq_wait_locked(r, cond)					\
	do {								\
		BIORQ_LOCK_ENSURE(r);					\
		while (cond) {						\
			psc_waitq_wait(&(r)->biorq_waitq, &(r)->biorq_lock); \
			BIORQ_LOCK(r);					\
		}							\
	} while (0)

#define biorq_wake_locked(r) psc_waitq_wakeall(&(r)->biorq_waitq))

int	_msl_offline_retry(struct bmpc_ioreq *, int);
int	msl_fd_offline_retry(struct msl_fhent *);

#define msl_offline_retry(r)		_msl_offline_retry((r), 0)
#define msl_offline_retry_ignexpire(r)	_msl_offline_retry((r), 1)

struct bmpc_write_coalescer {
	struct psc_lockedlist		 bwc_pll;
	size_t				 bwc_size;
	off_t				 bwc_soff;
	struct iovec			 bwc_iovs[34];  //XXX need define for this: (MIN_COALESCE_RPC_SZ/BMPC_BUFSZ)+2
	struct bmap_pagecache_entry	*bwc_bmpces[34];
	int				 bwc_niovs;
	int				 bwc_nbmpces;
	struct psclist_head		 bwc_lentry;
};

static __inline void
bmpce_freeprep(struct bmap_pagecache_entry *bmpce)
{
	LOCK_ENSURE(&bmpce->bmpce_lock);

	psc_assert(!(bmpce->bmpce_flags & (BMPCE_FREEING | BMPCE_NEW)));
	psc_assert(bmpce->bmpce_flags & (BMPCE_DATARDY | BMPCE_EIO));

	psc_assert(!psc_atomic16_read(&bmpce->bmpce_rdref));
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));

	bmpce->bmpce_flags = BMPCE_FREEING;
}

static __inline void
bmpce_useprep(struct bmap_pagecache_entry *bmpce,
    struct bmpc_ioreq *biorq, struct psc_waitq *wq)
{
	psc_assert(!bmpce->bmpce_waitq);
	psc_assert(psclist_disjoint(&bmpce->bmpce_lentry));
	psc_assert(psclist_disjoint(&bmpce->bmpce_ralentry));
	psc_assert(bmpce->bmpce_flags == BMPCE_NEW);
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_rdref));

	bmpce->bmpce_flags = BMPCE_INIT;
	bmpce->bmpce_owner = biorq;
	/*
	 * We put the entry back to the splay tree before it
	 * is fully allocated, so we need this field to remember
	 * who owns it.  Alternatively, we could use locking.
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

	if (op == BIORQ_READ)
		psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);
	else
		psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);

	psc_assert(bmpce->bmpce_off == off);
	ureqlock(&bmpce->bmpce_lock, locked);
}

/**
 * biorq_is_my_bmpce - Informs the caller that biorq, r, owns the
 *    the page cache entry, b.  This state implies that the thread
 *    processing 'r' is responsible for allocating a memory page
 *    and possible faulting in that page from the ION.
 */
#define biorq_is_my_bmpce(r, b)	((r) == (b)->bmpce_owner)

#define biorq_getaligned_off(r, nbmpce)					\
	(((r)->biorq_off & ~BMPC_BUFMASK) + ((nbmpce) * BMPC_BUFSZ))

#define biorq_voff_get(r)	((r)->biorq_off + (r)->biorq_len)

#define bmpce_is_rbw_page(r, b, pos)					\
	(biorq_is_my_bmpce((r), (b)) &&					\
	 ((!(pos) && ((r)->biorq_flags & BIORQ_RBWFP)) ||		\
	  (((pos) == (psc_dynarray_len(&(r)->biorq_pages) - 1) &&	\
	    ((r)->biorq_flags & BIORQ_RBWLP)))))

void  bmpc_global_init(void);
int   bmpc_grow(int);
void *bmpc_alloc(void);
void  bmpc_free(void *);
void  bmpc_freeall_locked(struct bmap_pagecache *);
int   bmpc_biorq_cmp(const void *, const void *);
void  bmpc_biorqs_fail(struct bmap_pagecache *, int);
int   bmpce_init(struct psc_poolmgr *, void *);
void  bmpce_getbuf(struct bmap_pagecache_entry *);
struct bmap_pagecache_entry *
      bmpce_lookup_locked(struct bmap_pagecache *, struct bmpc_ioreq *,
	  uint32_t, struct psc_waitq *);
void  bmpce_handle_lru_locked(struct bmap_pagecache_entry *,
	    struct bmap_pagecache *, int, int);
void  bwc_release(struct bmpc_write_coalescer *);

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

	pll_init(&bmpc->bmpc_lru, struct bmap_pagecache_entry,
		 bmpce_lentry, &bmpc->bmpc_lock);

	pll_init(&bmpc->bmpc_pndg_ra, struct bmap_pagecache_entry,
		 bmpce_lentry, &bmpc->bmpc_lock);

	pll_init(&bmpc->bmpc_pndg_biorqs, struct bmpc_ioreq,
		 biorq_lentry, &bmpc->bmpc_lock);

	pll_init(&bmpc->bmpc_new_biorqs, struct bmpc_ioreq,
		 biorq_lentry, &bmpc->bmpc_lock);
	/*
	 * Add the bmpc to the tail of LRU where it will stay until it's
	 * freed.
	 */
	lc_add(&bmpcLru, bmpc);
}

static __inline void
bmpc_ioreq_init(struct bmpc_ioreq *ioreq, uint32_t off, uint32_t len,
    int op, struct bmapc_memb *bmap, struct msl_fhent *fhent,
    struct msl_fsrqinfo *q)
{
	memset(ioreq, 0, sizeof(*ioreq));
	psc_waitq_init(&ioreq->biorq_waitq);
	INIT_PSC_LISTENTRY(&ioreq->biorq_lentry);
	INIT_PSC_LISTENTRY(&ioreq->biorq_mfh_lentry);
	INIT_PSC_LISTENTRY(&ioreq->biorq_bwc_lentry);
	INIT_SPINLOCK(&ioreq->biorq_lock);

	PFL_GETTIMESPEC(&ioreq->biorq_issue);
	timespecadd(&ioreq->biorq_issue, &bmapFlushDefMaxAge,
	    &ioreq->biorq_expire);

	ioreq->biorq_off  = off;
	ioreq->biorq_len  = len;
	ioreq->biorq_bmap = bmap;
	ioreq->biorq_flags = op;
	ioreq->biorq_fhent = fhent;
	ioreq->biorq_fsrqi = q;
	ioreq->biorq_last_sliod = IOS_ID_ANY;

	BMAP_LOCK(bmap);
	if (bmap->bcm_flags & BMAP_DIO)
		ioreq->biorq_flags |= BIORQ_DIO;
	if (bmap->bcm_flags & BMAP_ARCHIVER)
		ioreq->biorq_flags |= BIORQ_ARCHIVER;
	BMAP_ULOCK(bmap);
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

#if 0
static __inline void
bmpc_decrease_minage(void)
{
	struct timespec ts = BMPC_INTERVAL;

	BMPCSLABS_LOCK();

	timespecsub(&bmpcSlabs.bmms_minage, &ts,
		    &bmpcSlabs.bmms_minage);

	if (bmpcSlabs.bmms_minage.tv_sec < 0)
		timespecclear(&bmpcSlabs.bmms_minage);

	BMPCSLABS_ULOCK();
}

static __inline void
bmpc_increase_minage(void)
{
	struct timespec ts = BMPC_INTERVAL;

	BMPCSLABS_LOCK();

	timespecadd(&bmpcSlabs.bmms_minage, &ts,
		    &bmpcSlabs.bmms_minage);

	BMPCSLABS_ULOCK();
}
#endif

#endif /* _SL_BMPC_H_ */
