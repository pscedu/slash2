#ifndef OFFTREE_H
#define OFFTREE_H 1

#include <sys/types.h>

#include "psc_types.h"
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

#define OFT_REGIONSZ(root, d)					\
	((root)->oftr_mapsz / (power((root)->oftr_width, d)))

#define OFT_REGIONBLKS(root, d)  \
	(OFT_REGIONSZ(root, d) / (root)->oftr_minsz)

/* abs_width is the global width position of the spoke, not the 
 *  position relative to the parent
 */
#define OFT_STARTOFF(root, d, abs_width)	\
	(OFT_REGIONSZ(root, d) * abs_width)

#define OFT_ENDOFF(root, d, abs_width)				\
	(OFT_REGIONSZ(root, d) * (abs_width + 1) - 1)

#ifndef MIN
# define MIN(a,b) (((a)<(b)) ? (a): (b))
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b)) ? (a): (b))
#end

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
	struct psclist_head   oft_lentry; /* chain in the slb    */
	union norl {
		struct offtree_iov   *oft_iov;
		struct offtree_memb **oft_children;
	}		      oft_norl;
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
	struct dynarray     *oftrq_darray;
	off_t                oftrq_floff; /* file-logical offset       */
	ssize_t              oftrq_fllen; /* file-logical len          */
	u8                   oftrq_op;
	u8                   oftrq_depth;
	u8                   oftrq_width;	
};

#define OFT_STARTCHILD(r, d, o)  (o / OFT_REGIONSZ(r, d))
#define OFT_ENDCHILD(r, d, o, l) ((((o+l) / OFT_REGIONSZ(r, d)) +	\
				  (((o+l) % OFT_REGIONSZ(r, d)) ? 1:0)) - 1)

static inline int 
oft_schild_get(off_t o, struct offtree_root *r, int d, int abs_width)
{
	off_t soff = OFT_STARTOFF(r, d, abs_width);
	
	if (!((o >= soff) && (o < OFT_ENDOFF(r, d, abs_width))))
		return (-1);
		
	return ((o - soff) / OFT_REGIONSZ(r, d));
}

static inline int 
oft_echild_get(off_t o, size_t l, struct offtree_root *r, int d, int abs_width)
{
	off_t soff = OFT_STARTOFF(r, d, abs_width);
	
	if (!((o >= soff) && (o <= OFT_ENDOFF(r, d, abs_width))))
		return (-1);
		
	return ((((o + l) - soff) / OFT_REGIONSZ(r, (d+1))) + 
		(((o + l) % OFT_REGIONSZ(r, (d+1))) ? 1:0) - 1)
}



#endif 
