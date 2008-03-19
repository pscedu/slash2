#ifndef SL_BUFFER_H
#define SL_BUFFER_H 1

#include "psc_types.h"
#include "psc_util/atomic.h"
#include "psc_ds/list.h"
#include "psc_util/cdefs.h"
#include "psc_ds/vbitmap.h"
#include "psc_ds/listcache.h"

#define SLB_DIRTY    0x01 /* have dirty data          */
#define SLB_INFLIGHT 0x02 /* dirty data is being sent */
#define SLB_FREEING  0x04
#define SLB_PINNED   0x08 /* not freeable             */
#define SLB_LRU      0x10 /* on the lru, nothing pinned or dirty */
#define SLB_FREE     0x20
#define SLB_INIT     0x40
#deifne SLB_FRESH    0x80

#define SLB_FULL(b) (!vbitmap_nfree((b)->slb_inuse))

#define SLB_IOV2EBASE(iov, slb)						\
	(((iov)->oftiov_base + ((iov)->oftiov_nblks * slb->slb_blksz)) - 1)

#define SLB_REF2EBASE(ref, slb)						\
	(((ref)->slbir_base + ((ref)->slbir_nblks * slb->slb_blksz)) - 1)

#define SLB_SLB2EBASE(slb)						\
	(((slb)->slb_base + ((slb)->slb_nblks * slb->slb_blksz)) - 1)

/* sl_buffer - slash_buffer, is used for both read caching and write aggregation.  The buffer is split into N subsections where N is the size of the vbitmap structure.  

Slb_ref is maintained for every offtree entry which accesses this buffer.

Slb_iov_list is used to hold a sorted list of offtree_memb pointers (sorted by floff).  This is used when the LRU tells us to free our segments.

Slb_mgmt_lentry is used for the global free list, global lru, and the dirty list.

The slb_bmap list entry attaches to the bmap so that it may be freed with the bmap or have its regions allocated from within the bmap.
 */
struct sl_buffer {
	struct vbitmap *slb_inuse;  /* track which segments are busy   */
	//struct vbitmap *slb_dirty;  /* track which segments are dirty  */
	u32             slb_nblks;  /* num blocks                      */
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

#endif
