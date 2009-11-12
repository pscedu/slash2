/* $Id: buffer.h 8571 2009-10-23 17:37:21Z yanovich $ */

#ifndef _SL_BUFFER_H_
#define _SL_BUFFER_H_

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "pfl/cdefs.h"
#include "psc_util/lock.h"

#include "offtree.h"

struct dynarray;
struct vbitmap;

#define SLB_SIZE (slCacheBlkSz * slCacheNblks)

enum slb_states {
	SLB_DIRTY    = 0x01, /* have dirty data          */
	SLB_INFLIGHT = 0x02, /* data faulting in or out  */
	SLB_FREEING  = 0x04,
	SLB_PINNED   = 0x08, /* not freeable             */
	SLB_LRU      = 0x10, /* on the lru, nothing pinned or dirty */
	SLB_FREE     = 0x20,
	SLB_INIT     = 0x40,
	SLB_FRESH    = 0x80,
};

#define SLB_FULL(slb) (!vbitmap_nfree((slb)->slb_inuse))

#define SLB_IOV2EBASE(iov, slb)						\
	(((iov)->oftiov_base + ((iov)->oftiov_nblks * (slb)->slb_blksz)) - 1)

#define SLB_REF2EBASE(ref, slb)						\
	(((ref)->slbir_base + ((ref)->slbir_nblks * (slb)->slb_blksz)) - 1)

#define SLB_SLB2EBASE(slb)						\
	(((slb)->slb_base + ((slb)->slb_nblks * (slb)->slb_blksz)) - 1)

/* sl_buffer - slash_buffer, is used for both read caching and write
 *   aggregation.  The buffer is split into N subsections where N is
 *   the size of the vbitmap structure.
 *  Slb_ref is maintained for every offtree entry which accesses this
 *     buffer.
 *  Slb_iov_list is used to hold a sorted list of offtree_memb pointers
 *    (sorted by floff).  This is used when the LRU tells us to free
 *    our segments.
 *  Slb_mgmt_lentry is used for the global free list, global lru, and
 *    the dirty list.
 *
 *  The slb_bmap list entry attaches to the bmap so that it may be freed
 *    with the bmap or have its regions allocated from within the bmap.
 */
struct sl_buffer {
	struct vbitmap *slb_inuse;  /* track which segments are busy   */
	int             slb_nblks;  /* num blocks                      */
	uint32_t        slb_blksz;  /* blocksize                       */
	void           *slb_base;   /* point to the data buffer        */
	atomic_t        slb_ref;
	atomic_t        slb_unmapd_ref;
	atomic_t        slb_inflight;
	atomic_t        slb_inflpndg;
	psc_spinlock_t  slb_lock;
	uint32_t        slb_flags;
	struct psc_listcache  *slb_lc_owner;
	struct psc_lockedlist *slb_lc_fcm;
	struct psclist_head slb_iov_list;    /* list iovref backpointers */
	struct psclist_head slb_mgmt_lentry; /* chain lru or outgoing q  */
	struct psclist_head slb_fcm_lentry;  /* chain to fidcm entry     */
};

#define SLB_FLAG(field, str) (field ? str : "")
#define DEBUG_SLB_FLAGS(slb)					  \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_DIRTY),    "d"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_INFLIGHT), "I"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_FREEING),  "F"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_PINNED),   "P"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_LRU),      "L"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_FREE),     "f"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_INIT),     "i"), \
	SLB_FLAG(ATTR_TEST((slb)->slb_flags, SLB_FRESH),    "r")

#define SLB_FLAGS_FMT "%s%s%s%s%s%s%s%s"

#define DEBUG_SLB(level, slb, fmt, ...)					\
	psc_logs((level), PSS_GEN, 					\
		" slb@%p b:%p sz(%d/%d) bsz:%u"				\
		" ref:%d umref:%d inf:%d infp:%d fl:"SLB_FLAGS_FMT	\
		" fcm:%p lco:%p "fmt,					\
		(slb), (slb)->slb_base, (slb)->slb_nblks,		\
		vbitmap_nfree((slb)->slb_inuse),			\
		(slb)->slb_blksz,					\
		atomic_read(&(slb)->slb_ref),				\
		atomic_read(&(slb)->slb_unmapd_ref),			\
		atomic_read(&(slb)->slb_inflight),			\
		atomic_read(&(slb)->slb_inflpndg),			\
		DEBUG_SLB_FLAGS(slb),					\
		(slb)->slb_lc_fcm, (slb)->slb_lc_owner,			\
		## __VA_ARGS__)

#define DUMP_SLB(level, slb, fmt, ...)					\
        do {                                                            \
		int __l;						\
		struct sl_buffer_iovref *__r;				\
									\
		DEBUG_SLB((level), (slb), fmt, ## __VA_ARGS__);	\
		__l = reqlock(&slb->slb_lock);				\
		psclist_for_each_entry(__r, &(slb)->slb_iov_list,	\
				       slbir_lentry) {			\
			if (__r->slbir_pri) {				\
				struct offtree_memb *__m;		\
									\
				__m = __r->slbir_pri;			\
				DEBUG_OFT((level), __m,			\
				    "SLB ref %p memb", __r);		\
				DEBUG_OFFTIOV(level,			\
					     __m->oft_norl.oft_iov,	\
					     "iov of memb %p", __m);	\
			} else						\
				psc_logs((level), PSS_GEN, 		\
					"--> Unmapped SLB ref %p memb " \
					fmt, __r, ## __VA_ARGS__);	\
		}							\
		ureqlock(&(slb)->slb_lock, __l);			\
	} while (0)

struct sl_buffer_iovref {
	void  *slbir_base;                /* base pointer val (within slb) */
	size_t slbir_nblks;               /* allocation size               */
	void  *slbir_pri;                 /* backpointer to oftmemb        */
	void  *slbir_pri_bmap;
	int    slbir_flags;
	struct psclist_head slbir_lentry; /* chain to slb                  */
};

enum slb_ref_flags {
	SLBREF_MAPPED = (1 << 0),      /* Backpointer to oftm in place */
	SLBREF_REAP   = (1 << 1)       /* Freeing                      */
};

/* Should have been done earlier
 * have to add ref's before adding to pin list
 */
#define slb_fresh_2_pinned(slb) do {				\
		ATTR_UNSET((slb)->slb_flags, SLB_FRESH);	\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);		\
		(slb)->slb_lc_owner = NULL;			\
	} while (0)

#define slb_lru_2_pinned(slb) do {				\
		sl_buffer_lru_assertions((slb));		\
		ATTR_UNSET((slb)->slb_flags, SLB_LRU);		\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);		\
	} while (0)

#define slb_pinned_2_lru(slb) do {					\
		sl_buffer_pin_2_lru_assertions((slb));			\
		ATTR_UNSET((slb)->slb_flags, SLB_PINNED);		\
		ATTR_SET((slb)->slb_flags, SLB_LRU);			\
		(slb)->slb_lc_owner = NULL;				\
	} while (0)

#define SLB_TIMEOUT_SECS  5
#define SLB_TIMEOUT_NSECS 0

#define slb_set_alloctimer(t) do {				\
		clock_gettime(CLOCK_REALTIME, (t));		\
		(t)->tv_sec  += SLB_TIMEOUT_SECS;		\
		(t)->tv_nsec += SLB_TIMEOUT_NSECS;		\
	} while (0)

#define SLB_RP_TIMEOUT_SECS  0
#define SLB_RP_TIMEOUT_NSECS 200000

#define slb_set_readpndg_timer(t) do {				\
		clock_gettime(CLOCK_REALTIME, (t));		\
		(t)->tv_sec  += SLB_RP_TIMEOUT_SECS;		\
		(t)->tv_nsec += SLB_RP_TIMEOUT_NSECS;		\
	} while (0)

#define slb_inflight_cb(iov, op)			\
	do {						\
		if (slInflightCb)			\
			(*slInflightCb)(iov, op);	\
	} while (0)

#define slb_pin_cb(iov, op)				\
	do {						\
		if (bufSlPinCb)				\
			(*bufSlPinCb)(iov, op);		\
	} while (0)

#define SL_INFLIGHT_INC 0
#define SL_INFLIGHT_DEC 1

int  sl_buffer_init(struct psc_poolmgr *, void *);
void sl_buffer_destroy(void *);
int  sl_buffer_alloc(size_t, off_t, struct dynarray *, void *);
void sl_buffer_cache_init(void);
void sl_buffer_fresh_assertions(const struct sl_buffer *);
void sl_oftiov_inflight_cb(struct offtree_iov *, int);
void sl_oftiov_pin_cb(struct offtree_iov *, int);
void sl_oftm_addref(struct offtree_memb *, void *);

typedef void (*sl_oftiov_inflight_callback)(struct offtree_iov *, int);
extern sl_oftiov_inflight_callback slInflightCb;

typedef void (*sl_oftiov_pin_callback)(struct offtree_iov *, int);
extern sl_oftiov_pin_callback bufSlPinCb;

typedef int (*sl_iov_try_memrls)(void *);
typedef void (*sl_iov_memrls_ulock)(void *);

extern sl_iov_try_memrls   slMemRlsTrylock;
extern sl_iov_memrls_ulock slMemRlsUlock;

extern struct psc_poolmaster	 slBufsPoolMaster;
extern struct psc_poolmgr	*slBufsPool;
extern struct psc_listcache	 slBufsFree;
extern struct psc_listcache	 slBufsLru;
extern struct psc_listcache	 slBufsPin;
extern int			 slCacheBlkSz;
extern int			 slCacheNblks;
extern uint32_t			 slbFreeDef;
extern uint32_t			 slbFreeMax;

#endif /* _SL_BUFFER_H_ */
