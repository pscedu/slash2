#ifndef OFFTREE_H
#define OFFTREE_H 1

#include "psc_util/types.h"
#include "psc_util/atomic.h"
#include "psc_ds/list.h"
#include "psc_util/cdefs.h"
#include "psc_util/waitq.h"

#define OFT_NODE       0x01
#define OFT_LEAF       0x02
#define OFT_READPNDG   0x04
#define OFT_WRITEPNDG  0x08
#define OFT_ALLOCPNDG  0x10
#define OFT_REAP       0x20
#define OFT_FREEING    0x40

static inline size_t
power(size_t base, size_t exp)
{
	size_t i, p;
	
	for (i=1, p=1; i <= exp; i++)
		p = p * base;
	return p;
}

#define OFT_STARTOFF(root, d, w) (((root)->oftr_mapsz / (power((root)->oftr_width, d))) * w) 
#define OFT_ENDOFF(root, d, w)   ((((root)->oftr_mapsz / (power((root)->oftr_width, d))) * (w+1)) - 1)
#define OFT_REGIONSZ(root, d)    (OFT_ENDOFF(root, d+1, 0) + 1)
#define OFT_REGIONBLKS(root, d)  ((OFT_REGIONSZ(root, d) / (root)->oftr_minsz)


#ifndef MIN
# define MIN(a,b) (((a)<(b)) ? (a): (b))
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b)) ? (a): (b))
#endif


struct offtree_iov {
	void   *oftiov_base;  /* point to our data buffer  */
	size_t  oftiov_len;   /* length of respective data */
	off_t   oftiov_floff; /* file-logical offset       */
	ssize_t oftiov_fllen; /* file-logical len          */
	void   *oftiov_pri;   /* private data, in slash's case, point at the 
				sl_buffer structure which manages the 
				region containing our base */
};

#define OFT_MEMB_INIT(m) {					\
		psc_waitq_init(&(m)->oft_waitq);		\
		LOCK_INIT(&(m)->oft_lock);			\
		atomic_set(&(m)->oft_ref, 0);			\
	}

struct offtree_memb {
	struct psc_wait_queue oft_waitq; /* block here on OFT_GETPNDG */
	psc_spinlock_t        oft_lock;
	u32                   oft_flags;
	atomic_t              oft_ref;
	struct offtree_memb  *oft_parent;
	union norl {
		struct offtree_iov   *oft_iov;
		struct offtree_memb **oft_children;
	};
};

/*
 * offtree_alloc_fn - allocate memory from slabs managed within the bmap.
 *  @size_t: number of blocks to allocate (void * pri) knows the block size
 *  @void *: opaque pointer (probably to the bmap)
b *  @iovs *: array of iovecs allocated to handle the allocation (returned)
 *  Return: the number of blocks allocated (and hence the number of iovec's in the array.
 */
typedef int (*offtree_alloc_fn)(size_t, struct offtree_iov **, int *, void *);

struct offtree_root {
	psc_spinlock_t oftr_lock;
	u32            oftr_width;
	u32            oftr_maxdepth;
	size_t         oftr_mapsz; /* map size, how many bytes are tracked? */
	size_t         oftr_minsz; /* minimum chunk size                    */
	void          *oftr_pri;   /* opaque backpointer (use bmap for now) */
	offtree_alloc_fn    oftr_alloc;
	struct offtree_memb oftr_memb; /* root member                       */
};

struct offtree_req {
	struct offtree_root *oftrq_root;
	struct offtree_memb *oftrq_memb;
	struct dynarray      oftrq_darray;
	u8                   oftrq_op;
};



#endif 
