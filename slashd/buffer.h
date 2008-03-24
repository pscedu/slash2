#ifndef SL_BUFFER_H
#define SL_BUFFER_H 1

#include "psc_types.h"
#include "psc_util/atomic.h"
#include "psc_ds/list.h"
#include "psc_util/cdefs.h"
#include "psc_ds/vbitmap.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"
#include "psc_rpc/rpc.h"

enum slb_states {
	SLB_DIRTY    = 0x01, /* have dirty data          */
	SLB_INFLIGHT = 0x02, /* dirty data is being sent */
	SLB_FREEING  = 0x04,
	SLB_PINNED   = 0x08, /* not freeable             */
	SLB_LRU      = 0x10, /* on the lru, nothing pinned or dirty */
	SLB_FREE     = 0x20,
	SLB_INIT     = 0x40,
	SLB_FRESH    = 0x80
};

#define SLB_FULL(slb) (!vbitmap_nfree((slb)->slb_inuse))
	
#define SLB_IOV2EBASE(iov, slb)						\
	(((iov)->oftiov_base + ((iov)->oftiov_nblks * (slb)->slb_blksz)) - 1)
	
#define SLB_REF2EBASE(ref, slb)						\
	(((ref)->slbir_base + ((ref)->slbir_nblks * (slb)->slb_blksz)) - 1)
	
#define SLB_SLB2EBASE(slb)						\
	(((slb)->slb_base + ((slb)->slb_nblks * (slb)->slb_blksz)) - 1)
	
/* sl_buffer - slash_buffer, is used for both read caching and write aggregation.  The buffer is split into N subsections where N is the size of the vbitmap structure.  

Slb_ref is maintained for every offtree entry which accesses this buffer.

Slb_iov_list is used to hold a sorted list of offtree_memb pointers (sorted by floff).  This is used when the LRU tells us to free our segments.

Slb_mgmt_lentry is used for the global free list, global lru, and the dirty list.

The slb_bmap list entry attaches to the bmap so that it may be freed with the bmap or have its regions allocated from within the bmap.
 */
struct sl_buffer {
	struct vbitmap *slb_inuse;  /* track which segments are busy   */
	//struct vbitmap *slb_dirty;  /* track which segments are dirty  */
	int             slb_nblks;  /* num blocks                      */
	u32             slb_blksz;  /* blocksize                       */
	void           *slb_base;   /* point to the data buffer        */
	atomic_t        slb_ref;
	atomic_t        slb_unmapd_ref;
	psc_spinlock_t  slb_lock;
	u32             slb_flags;
	list_cache_t   *slb_lc_owner;
	list_cache_t   *slb_lc_fcm;
	struct psclist_head slb_iov_list;    /* list of iov backpointers    */ 
	struct psclist_head slb_mgmt_lentry; /* attach to lru or outgoing q */ 
	struct psclist_head slb_fcm_lentry;  /* chain to fidcm entry        */
};

#define SLB_FLAG(field, str) (field ? str : "")
#define DEBUG_SLB_FLAGS(iov)					\
        SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_DIRTY),  "d"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_INFLIGHT), "I"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_FREEING),"F"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_PINNED),  "P"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_LRU),   "L"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_FREE),   "f"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_INIT),   "i"),	\
	SLB_FLAG(ATTR_TEST(slb->slb_flags, SLB_FRESH),   "r")	\
		
	
#define SLB_FLAGS_FMT "%s%s%s%s%s%s%s%s"

#define DEBUG_SLB(level, slb, fmt, ...)					\
        do {                                                            \
                _psclog(__FILE__, __func__, __LINE__,                   \
                        PSS_OTHER, level, 0,                            \
                        " slb@%p b:%p sz(%d/"LPD64") bsz:"LPD64	\
                        " ref:%d umref:%d fl:"SLB_FLAGS_FMT		\
			" fcm:%p lco:%p "fmt,				\
                        slb, slb->slb_base, slb->slb_nblks,		\
			vbitmap_nfree(slb->slb_inuse),			\
			slb->slb_blksz,					\
			atomic_read(&slb->slb_ref),			\
			atomic_read(&slb->slb_unmapd_ref),		\
			DEBUG_SLB_FLAGS(slb),				\
			slb->slb_lc_fcm, slb->slb_lc_owner,		\
			## __VA_ARGS__);				\
	} while(0)

	
#define DUMP_SLB(level, slb, fmt, ...)					\
        do {                                                            \
		int __l, __c;						\
		struct sl_buffer_iovref *__r;				\
									\
		DEBUG_SLB(level, slb, fmt, ## __VA_ARGS__);		\
		__l = reqlock(&slb->slb_lock);				\
		psclist_for_each_entry(__r, &slb->slb_iov_list,		\
				       slbir_lentry) {			\
			DEBUG_OFT(level,(struct offtree_memb *) __r->slbir_pri, \
				  "SLB %p memb", slb);			\
		}							\
		ureqlock(&slb->slb_lock, __l);				\
	} while (0)
		

struct sl_buffer_iovref {
	void  *slbir_base;                /* base pointer val (within slb) */
	size_t slbir_nblks;               /* allocation size               */
	void  *slbir_pri;                 /* backpointer to oftmemb        */
	int    slbir_flags;
	struct psclist_head slbir_lentry; /* chain to slb                  */
};

enum slb_ref_flags {
	SLBREF_MAPPED = (1 << 0),      /* Backpointer to oftm in place */
	SLBREF_REAP   = (1 << 1)       /* Freeing                      */
};

#define slb_fresh_2_pinned(slb) {				\
		sl_buffer_fresh_assertions((slb));		\
		ATTR_UNSET((slb)->slb_flags, SLB_FRESH);	\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);		\
	}

#define slb_lru_2_pinned(slb) {					\
		sl_buffer_lru_assertions((slb));		\
		ATTR_UNSET((slb)->slb_flags, SLB_FRESH);	\
		ATTR_SET((slb)->slb_flags, SLB_PINNED);		\
	}

#define SLB_TIMEOUT_SECS  5
#define SLB_TIMEOUT_NSECS 0

#define slb_set_alloctimer(t) {				\
		clock_gettime(CLOCK_REALTIME, (t));	\
		(t)->tv_sec  += SLB_TIMEOUT_SECS;	\
		(t)->tv_nsec += SLB_TIMEOUT_NSECS;	\
	}

#define SLB_RP_TIMEOUT_SECS  0
#define SLB_RP_TIMEOUT_NSECS 200000

#define slb_set_readpndg_timer(t) {			\
		clock_gettime(CLOCK_REALTIME, (t));	\
		(t)->tv_sec  += SLB_RP_TIMEOUT_SECS;	\
		(t)->tv_nsec += SLB_RP_TIMEOUT_NSECS;	\
	}

#endif
