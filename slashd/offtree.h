#ifndef OFFTREE_H
#define OFFTREE_H 1

#include <sys/types.h>

#include "psc_types.h"
#include "psc_util/atomic.h"
#include "psc_ds/list.h"
#include "psc_util/cdefs.h"
#include "psc_util/waitq.h"

enum oft_attributes {
	OFT_NODE      = (1 << 0), /* Node not a leaf          */
	OFT_LEAF      = (1 << 1), /* Leaf not a node          */
	OFT_READPNDG  = (1 << 2), /* Read is about to occur   */
	OFT_WRITEPNDG = (1 << 3), /* Write is about to occur  */
	OFT_ALLOCPNDG = (1 << 4), /* Alloc is about to occur  */
	OFT_REQPNDG   = (1 << 5), 
	OFT_REAP      = (1 << 6), /* Process of being removed */
	OFT_FREEING   = (1 << 7), /* Different from Reap?     */
	OFT_SPLITTING = (1 << 8), /* Leaf becoming a parent   */
	OFT_INVMEM    = (1 << 9)  /* Pages have not been 'filled in' yet */
};

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

#define OFT_ENDOFF(root, d, abs_width)			\
	(OFT_REGIONSZ(root, d) * (abs_width + 1) - 1)

/* Verify minsz is a power of 2 */
#define OFT_BLKMASK_UPPER(root, o) ((r->oftr_minsz - 1) & o)
#define OFT_BLKMASK_LOWER(root, o) ((~(r->oftr_minsz - 1)) & o)

#ifndef MIN
# define MIN(a,b) (((a)<(b)) ? (a): (b))
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b)) ? (a): (b))
#end

enum oft_iov_flags {
	OFTIOV_DATARDY   = (1 << 0), /* Buffer contains no data       */
	OFTIOV_FAULTING  = (1 << 1), /* Buffer is being retrieved    */
	OFTIOV_COLLISION = (1 << 2), /* Collision ops must take place */
	OFTIOV_IOPNDG    = (1 << 3)  /* About to do I/o */
};

struct offtree_iov {
	int     oftiov_flags;
	void   *oftiov_base;  /* point to our data buffer  */
	off_t   oftiov_off;   /* data offset               */
	size_t  oftiov_blksz;
	size_t  oftiov_nblks; /* length of respective data */	
	void   *oftiov_pri;   /* private data, in slash's case, point at the 
				sl_buffer structure which manages the 
				region containing our base */
};

#define OFT_MEMB_INIT(m, p) {					\
		psc_waitq_init(&(m)->oft_waitq);		\
		LOCK_INIT(&(m)->oft_lock);			\
		atomic_set(&(m)->oft_ref, 0);			\
                (m)->oft_parent = p;                            \
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

#define OFTM_FLAG(field, str) (field ? str : "")
#define DEBUG_OFTM_FLAGS(oft)						\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_NODE), "N"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_LEAF), "L"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_READPNDG), "R"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_WRITEPNDG), "W"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_ALLOCPNDG), "A"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_REQPNDG), "r"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_REAP), "p"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_FREEING), "F"),		\
	OFTM_FLAG(ATTR_TEST(oft->flags, OFT_SPLITTING), "S")

#define REQ_OFTM_FLAGS_FMT "%s%s%s%s%s%s%s%s%s"

#define DEBUG_OFT(level, oft, fmt, ...)					\
	do {								\
		if (ATTR_TEST((oft)->oft_flags, OFT_LEAF)) {		\
			_psclog(__FILE__, __func__, __LINE__,		\
				PSS_OTHER, level, 0,			\
				" oft@%p p:%p ref:%d"			\
				" fl:"REQ_OFTM_FLAGS_FMT" "fmt,		\
				oft,					\
				(oft)->oft_parent,			\
				atomic_read(&(oft)->oft_ref), 		\
				DEBUG_OFTM_FLAGS(oft),			\
				## __VA_ARGS__);			\
		} else {						\
			_psclog(__FILE__, __func__, __LINE__,		\
				PSS_OTHER, level, 0,			\
				" oft@%p p:%p r:%d "			\
				"fl:"REQ_OFTM_FLAGS_FMT" "fmt,		\
				oft, (oft)->oft_parent,			\
				atomic_read(&(oft)->oft_ref),		\
				DEBUG_OFTM_FLAGS(oft),			\
				## __VA_ARGS__);			\
		}							\
	} while(0)


/*
 * offtree_alloc_fn - allocate memory from slabs managed within the bmap.
 *  @size_t: number of blocks to allocate (void * pri) knows the block size
 *  @void *: opaque pointer (probably to the bmap)
 *  @iovs *: array of iovecs allocated to handle the allocation (returned)
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
	off_t                oftrq_off;   /* aligned, file-logical offset   */
	ssize_t              oftrq_nblks; /* number of blocks requested     */
	u8                   oftrq_op;
	u8                   oftrq_depth;
	u8                   oftrq_width;	
};

static inline int 
oft_child_get(off_t o, struct offtree_root *r, int d, int abs_width)
{
	off_t soff = OFT_STARTOFF(r, d, abs_width);
	
	/* ensure that the parent is responsible for the given range */
	if (!((o >= soff) && (o < OFT_ENDOFF(r, d, abs_width))))
		abort();
	
	return ((o - soff) / OFT_REGIONSZ(r, (d+1)));
}

#define DEBUG_OFFTREQ(level, oftr, fmt, ...)				\
	do {								\
		_psclog(__FILE__, __func__, __LINE__,			\
			PSS_OTHER, level, 0,				\
			" oftr@%p o:"LPX64" l:"LPX64" node:%p darray:%p"\
			" root:%p op:%hh d:%hh w:%hh "fmt,	        \
			oftr, oftr->oftrq_off, oftr->oftrq_nblks,	\
			oftr->oftrq_memb, oftr->oftrq_darray,	        \
			oftr->oftrq_root, oftr->oftrq_op,               \
			oftr->oftrq_depth, oftr->oftrq_width,           \
			## __VA_ARGS__);  \
	} while(0)


#define OFFTIOV_FLAG(field, str) (field ? str : "")
#define DEBUG_OFFTIOV_FLAGS(iov)					\
	OFFTIOV_FLAG(ATTR_TEST(iov->flags, OFTIOV_DATARDY),  "d"),	\
	OFFTIOV_FLAG(ATTR_TEST(iov->flags, OFTIOV_FAULTING), "f"),	\
	OFFTIOV_FLAG(ATTR_TEST(iov->flags, OFTIOV_COLLISION),"p"),     \
	OFFTIOV_FLAG(ATTR_TEST(iov->flags, OFTIOV_IOPNDG),   "i")

#define OFFTIOV_FLAGS_FMT "%s%s%s%s"

#define DEBUG_OFFTIOV(level, iov, fmt, ...)				\
	do {								\
		_psclog(__FILE__, __func__, __LINE__,			\
			PSS_OTHER, level, 0,				\
			" oftiov@%p b:%p o:"LPX64" l:"LPX64 "bsz:"LPX64 \
			"priv:%p fl:"OFFTIOV_FLAGS_FMT" "fmt,		\
			iov, iov->oftiov_base, iov->oftiov_off,		\
			iov->oftiov_nblks, iov->oftiov_blksz,	        \
			iov->oftiov_pri, DEBUG_OFFTIOV_FLAGS(iov),[5~	\
			## __VA_ARGS__);				\
	} while(0)

#endif 
