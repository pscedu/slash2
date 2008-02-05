#ifndef SL_BUFFER_H
#define SL_BUFFER_H 1

#include "psc_util/types.h"
#include "psc_util/atomic.h"
#include "psc_ds/list.h"
#include "psc_util/cdefs.h"
#include "psc_ds/vbitmap.h"

#define SLB_DIRTY    0x01 /* have dirty data          */
#define SLB_INFLIGHT 0x02 /* dirty data is being sent */

/* sl_buffer - slash_buffer, is used for both read caching and write aggregation.  The buffer is split into N subsections where N is the size of the vbitmap structure.  

Slb_ref is maintained for every offtree entry which accesses this buffer.

Slb_mgmt_lentry is used for the global free list, global lru, and the dirty list.

The slb_bmap list entry attaches to the bmap so that it may be freed with the bmap or have its regions allocated from within the bmap.
 */
struct sl_buffer {
	struct vbitmap *slb_bitmap;
	u32             slb_len;    /* allocated size                  */
	void           *slb_base;   /* point to the data buffer        */
	atomic_t        slb_ref;
	psc_spinlock_t  slb_lock;
	u32             slb_flags;
	struct psclist_head slb_mgmt_lentry; /* attach to lru or outgoing q */ 
	struct psclist_head slb_bmap_lentry; /* chain to bmap entry         */
};

#endif
