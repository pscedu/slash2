/* $Id$ */

#ifndef _SL_BMPC_H_
#define _SL_BMPC_H_

#include <time.h>

#include "psc_ds/tree.h"
#include "psc_ds/list.h"
#include "psc_ds/lockedlist.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "psc_util/waitq.h"
#include "psc_util/spinlock.h"
#include "psc_ds/vbitmap.h"

#include "bmap.h"
#include "buffer.h"

extern struct psc_poolmaster bmpcePoolMaster;
extern struct psc_poolmgr *bmpcePoolMgr;

#define BMPC_BUFSZ      32768
#define BMPC_BLKSZ      BMPC_BUFSZ
#define BMPC_SLB_NBLKS  256  /* 8MB slab */
#define BMPC_DEFSLBS    8
#define BMPC_MAXSLBS    32
#define BMPC_BUFMASK    (BMPC_BLKSZ-1)
#define BMPC_SLBBUFMASK ((BMPC_BLKSZ * BMPC_SLB_NBLKS)-1)
#define BMPC_IOMAXBLKS  64
#define BMPC_MAXBUFSRPC (1048576/BMPC_BUFSZ)

#define BMPC_DEF_MINAGE {2, 0} /* seconds, nanoseconds */
#define BMPC_INTERVAL   {0, 200000000}

uint32_t bmpcDefSlbs = BMPC_DEFSLBS;
uint32_t bmpcMaxSlbs = BMPC_MAXSLBS; /* Adjust to some % of system mem? */

struct bmpc_mem_slbs {
	atomic_t              bmms_waiters;
	uint16_t              bmms_reap;
	struct timespec       bmms_minage;
	psc_spinlock_t        bmms_lock;
	struct psc_lockedlist bmms_slbs;
	struct psc_waitq      bmms_waitq;
};

extern struct bmpc_mem_slbs bmpcSlabs;

#define lockBmpcSlabs  spinlock(&bmpcSlabs.bmms_lock)
#define ulockBmpcSlabs freelock(&bmpcSlabs.bmms_lock)

struct bmap_pagecache_entry {	
	psc_atomic16_t            bmpce_wrref;   /* pending write ops        */
	psc_atomic16_t            bmpce_rdref;   /* pending read ops         */
	uint32_t                  bmpce_flags;   /* state bits               */
	uint32_t                  bmpce_off;     /* filewise, bmap relative  */
	psc_spinlock_t            bmpce_lock;    /* serialize                */
	void                     *bmpce_base;    /* base pointer from slb    */
	struct psc_waitq         *bmpce_waitq;   /* others block here on I/O */
	struct psclist_head       bmpce_lentry;  /* chain on bmap lru        */
	struct timespec           bmpce_laccess; /* last page access         */
	SPLAY_ENTRY(bmap_pagecache_entry) bmpce_tentry;
};

enum BMPCE_STATES {
	BMPCE_NEW       = (1<<0),
	BMPCE_GETBUF    = (1<<1),
	BMPCE_INFLGHT   = (1<<2),
        BMPCE_IOSCHED   = (1<<3),	
	BMPCE_DATARDY   = (1<<4),
	BMPCE_DIRTY2LRU = (1<<5),
	BMPCE_LRU       = (1<<6),
	BMPCE_FREE      = (1<<7),
	BMPCE_FREEING   = (1<<8)
};

#define DEBUG_BMPCE(level, b, fmt, ...)					\
	psc_logs((level), PSS_GEN,					\
		 " bmpce@%p fl=%x o=%x b=%p ts=%u:%u wr=%hu rd=%hu: "fmt, \
		 (b), (b)->bmpce_flags, (b)->bmpce_off, (b)->bmpce_base, \
		 (b)->bmpce_laccess.tv_sec, (b)->bmpce_laccess.tv_nsec, \
		 psc_atomic16_read(&(b)->bmpce_wrref),			\
		 psc_atomic16_read(&(b)->bmpce_rdref),			\
		 ## __VA_ARGS__)


static inline void
bmpce_init(struct bmap_pagecache_entry *bmpce)
{
	memset(bmpce, 0, sizeof(*bmpce));
	LOCK_INIT(&bmpce->bmpce_lock);
	bmpce->bmpce_flags = BMPCE_NEW;
}

static inline void
bmpce_freeprep(struct bmap_pagecache_entry *bmpce)
{
	psc_assert(LOCK_ENSURE(&bmpce->bmpce_lock));
	psc_assert(!(bmpce->flags & 
		     (BMPCE_FREEING|BMPCE_IOSCHED|BMPCE_INFLGHT|
		      BMPCE_GETBUF|BMPCE_NEW)));
	psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

	psc_assert(!psc_atomic16_read(&bmpce->bmpce_rdref));
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));
	
	bmpce->flags = BMPCE_FREEING;
}

static inline void
bmpce_useprep(struct bmap_pagecache_entry *bmpce, struct bmap_pagecache *bmpc)
{
	psc_assert(!bmpce->bmpce_base);
	psc_assert(!bmpce->bmpce_waitq);
	psc_assert(psclist_disjoint(&bmpce->bmpce_lentry));
	psc_assert(bmpce->bmpce_flags == BMPCE_NEW);
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_wrref));
	psc_assert(!psc_atomic16_read(&bmpce->bmpce_rdref));

	bmpce->bmpce_flags = BMPCE_GETBUF;
	bmpce->bmpce_waitq = &bmpc->bmpc_waitq;
}

static inline void
bmpce_usecheck(struct bmap_pagecache_entry *bmpce, int op, uint32_t off)
{
	int locked;
	
	locked = reqlock(&bmpce->bmpce_lock);

	DEBUG_BMPCE(PLL_TRACE, bmpce, "op=%d off=%u", op, off);
	psc_assert(bmpce->bmpce_base);
	psc_assert(!(bmpce->bmpce_flags & BMPC_GETBUF));

	if (op == BIORQ_READ)
		psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref));
	else
		psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref));

	psc_assert(bmpce->bmpce_off == off);
	ureqlock(&bmpce->bmpce_lock, locked);
}

static inline int
bmpce_cmp(const void *x, const void *y)
{
	if (((struct bmap_pagecache_entry *)x)->bmpce_off <
	    ((struct bmap_pagecache_entry *)y)->bmpce_off)
		return (-1);

	if (((struct bmap_pagecache_entry *)x)->bmpce_off > 
	    ((struct bmap_pagecache_entry *)y)->bmpce_off)
		return (1);

	return (0);
}


SPLAY_HEAD(bmap_pagecachetree, bmap_pagecache_entry);
SPLAY_PROTOTYPE(bmap_pagecachetree, bmap_pagecache_entry, bmpce_tentry, 
		bmpce_cmp);

struct bmap_pagecache {
	struct bmap_pagecachetree bmpc_tree;   /* tree of cbuf_handle        */
	struct timespec           bmpc_oldest; /* LRU's oldest item          */
	struct psc_lockedlist     bmpc_lru;    /* cleancnt can be kept here  */
	struct psc_lockedlist     bmpc_pndg;   /* chain pending I/O requests */
	atomic_t                  bmpc_pndgwr; /* # pending wr req           */
	psc_spinlock_t            bmpc_lock;   /* serialize tree and pll     */
	struct psclist_head       bmpc_lentry; /* chain to global LRU lc     */
};

#define BMPC_LOCK(b)  spinlock(&(b)->bmpc_lock
#define BMPC_ULOCK(b) freelock(&(b)->bmpc_lock

#define BMPC_WAIT  psc_waitq_wait(&bmpcSlabs.bmms_waitq, &bmpcSlabs.bmms_lock)
#define BMPC_WAKE  psc_waitq_wakeall(&bmpcSlabs.bmms_waitq)

struct bmpc_ioreq {	
	uint32_t                   biorq_off;   /* filewise, bmap relative   */
	uint32_t                   biorq_len;   /* non-aligned, real length  */
	uint32_t                   biorq_flags; /* state and op type bits    */
	psc_spinlock_t             biorq_lock;
	struct timespec            biorq_start; /* issue time                */
	struct psc_dynarray        biorq_pages; /* array of bmpce            */
	struct psclist_head        biorq_lentry;/* chain on bmpc_pndg        */
	struct bmapc_memb         *biorq_bmap;  /* backpointer to our bmap   */
	struct pscrpc_request_set *biorq_rqset;
};


enum BMPC_IOREQ_FLAGS {
	BIORQ_READ  = (1<<0),
	BIORQ_WRITE = (1<<1),
	BIORQ_RBWFP = (1<<2),
	BIORQ_RBWLP = (1<<3),
	BIORQ_SCHED = (1<<4),
	BIORQ_INFL  = (1<<5),
	BIORQ_DIO   = (1<<6)
};

#define BIORQ_FLAGS_FORMAT "%s%s%s%s%s%s%s"
#define BIORQ_FLAG(field, str) ((field) ? (str) : "-")
#define DEBUG_BIORQ_FLAGS(s)				        \
        BIORQ_FLAG(((b)->biorq_flags & BIORQ_READ), "r"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_WRITE), "w"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_RBWFP), "f"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_RBWLP), "l"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_SCHED), "s"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_INFL), "i"),	\
	BIORQ_FLAG(((b)->biorq_flags & BIORQ_DIO), "d")

#define DEBUG_BIORQ(level, b, fmt, ...)					\
	psc_logs((level), PSS_GEN,					\
		 " biorq@%p fl=%x o=%x np=%d b=%p ts=%u:%u: "		\
		 BIORQ_FLAGS_FORMAT" "fmt,				\
		 (b), (b)->biorq_flags, (b)->biorq_off,			\
		 dynarray_len(&(b)->biorq_pages), (b)->biorq_bmap,	\
		 (b)->bmpce_laccess.tv_sec, (b)->bmpce_laccess.tv_nsec, \
		 DEBUG_BIORQ_FLAGS(b), ## __VA_ARGS__)

/* biorq_is_my_bmpce - informs the caller that biorq, r, owns the 
 *    the page cache entry, b.  This state implies that the thread 
 *    processing 'r' is responsible for allocating a memory page 
 *    and possible faulting in that page from the ION.
 */
#define biorq_is_my_bmpce(r, b)	(&(r)->biorq_waitq == (b)->bmpce_waitq)

#define biorq_getaligned_off(r, nbmpce) ((&(r)->biorq_off & ~BMPC_BUFMASK) + \
					 (nbmpce * BMPC_BUFSZ))

#define bmpce_is_rbw_page(r, b, pos)					\
	(biorq_is_my_bmpce(r, b) &&					\
	 ((!pos && ((r)->biorq_flags & BIORQ_RBWFP)) ||			\
	  ((pos == (dynarray_len(&(r)->biorq_pages)-1) &&		\
	    ((r)->biorq_flags & BIORQ_RBWLP)))))

static inline void
bmpc_init(struct bmap_pagecache *bmpc) {
	memset(bmpc, 0, sizeof(*bmpc));

	LOCK_INIT(&bmpc->bmpc_lock);

	pll_init(&bmpc->bmpc_lru, struct bmap_pagecache_entry,
                 bmpce_lentry, &bmpc->bmpc_lock);

	pll_init(&bmpc->bmpc_pndg, struct bmpc_ioreq,
                 biorq_lentry, &bmpc->bmpc_lock);
}

static inline void
bmpc_ioreq_init(struct bmpc_ioreq *ioreq, uint32_t off, uint32_t len, int op,
		const struct bmapc_memb *bmap)
{
	memset(ioreq, 0, sizeof(*ioreq));
	psc_waitq_init(&ioreq->biorq_waitq);
	LOCK_INIT(&ioreq->biorq_lock);
	clock_gettime(CLOCK_REALTIME, &ioreq->biorq_start);

	ioreq->biorq_off  = off;
	ioreq->biorq_len  = len;
	ioreq->biorq_bmap = bmap;
	ioreq->biorq_flags = op;
	if (b->bcm_mode & BMAP_DIO)
                r->biorq_flags |= BIORQ_DIO;
}

static inline int
bmpc_lru_cmp(const void *x, const void *y)
{
	struct bmap_pagecache *a=x, *b=y;

	if (timespeccmp(&a->bmpc_oldest, &b->bmpce_oldest, <))
		return (-1);
	
	if (timespeccmp(&a->bmpc_oldest, &b->bmpce_oldest, >))
		return (1);

	return (0);
}

static inline void
bmpc_decrease_minage(void)
{
	struct timespec ts = BMPC_INTERVAL;
	int tmp;

	lockBmpcSlabs;

	timespecsub(&bmpcSlabs.bmms_minage, &ts, 
		    &bmpcSlabs.bmms_minage);
	
	if (bmpcSlabs.bmms_minage.tv_sec) < 0)
		timespecclear(&bmpcSlabs.bmms_minage);

	ulockBmpcSlabs;
}

static inline void
bmpc_increase_minage(void)
{
	struct timespec ts = BMPC_INTERVAL;
	int tmp;

	lockBmpcSlabs;

	timespecadd(&bmpcSlabs.bmms_minage, &ts, 
		    &bmpcSlabs.bmms_minage);

	ulockBmpcSlabs;
}


void  bmpc_init(void);
void  bmpc_grow(void);
void *bmpc_alloc(void);
void  bmpc_free(void *);
void  bmpc_freeall_locked(struct bmap_pagecache *);


