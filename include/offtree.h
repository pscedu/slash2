/* $Id$ */

#ifndef _OFFTREE_H_
#define _OFFTREE_H_

#include <sys/types.h>

#include <inttypes.h>

#include "psc_types.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"
#include "psc_util/log.h"
#include "psc_util/waitq.h"

#include "slashrpc.h"
#include "cache_params.h"

#define OFTIOV_CB_SINGLE_PTR_SLOT 1
#define OFTIOV_CB_COALESCED_PTR_SLOT 2

#define SL_BUFFER_PIN 0
#define SL_BUFFER_UNPIN 1

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

#define OFT_REGIONBLKS(root, d)				\
	(OFT_REGIONSZ(root, d) / (root)->oftr_minsz)

/* abs_width is the global width position of the spoke, not the
 *  position relative to the parent
 */
#define OFT_STARTOFF(root, d, abs_width)	\
	(OFT_REGIONSZ(root, d) * abs_width)

#define OFT_ENDOFF(root, d, abs_width)			\
	((off_t)(OFT_REGIONSZ(root, d) * (abs_width + 1) - 1))

/* Confusing name, it means given an oft_request, find the
 *   starting offset for the region not the request.
 */
#define OFT_REQ_STARTOFF(req)						\
	((off_t)(OFT_REGIONSZ((req)->oftrq_root, (req)->oftrq_depth) *	\
		 (req)->oftrq_width))

#define OFT_REQ_ENDOFF(req)				       \
	((off_t)(OFT_REGIONSZ((req)->oftrq_root, (req)->oftrq_depth) *	\
		 ((req)->oftrq_width + 1) - 1))

#define OFT_REQ_ABSWIDTH_GET(req, pos)					\
	(((req)->oftrq_width * (req)->oftrq_root->oftr_width) + pos)


#define OFT_REQ_REGIONBLKS(req)				       \
	((OFT_REGIONSZ((req)->oftrq_root, (req)->oftrq_depth) - \
	  ((req)->oftrq_off - OFT_REQ_STARTOFF(req))) /		\
	 (req)->oftrq_root->oftr_minsz)


#define OFT_VERIFY_REQ_SE(req, s, e) {					\
		psc_trace("OFT_VERIFY_REQ_SE NR (%"PRIx64"/%"PRIx64")",	\
			  s, e);					\
		psc_trace("OFT_VERIFY_REQ_SE RG (%"PRIx64"/%"PRIx64")",	\
			  OFT_REQ_STARTOFF(req), OFT_REQ_ENDOFF(req));	\
		psc_assert((s >= OFT_REQ_STARTOFF(req)) &&		\
			   (e <= OFT_REQ_ENDOFF(req)));			\
	}

#define OFT_REQ2SE_OFFS(req, s, e) {					\
		int TTt;						\
									\
		s = (req)->oftrq_off;					\
		e = (s + ((req)->oftrq_nblks *				\
			  (req)->oftrq_root->oftr_minsz)) - 1;		\
		TTt = s % (req)->oftrq_root->oftr_minsz;		\
		psc_assert(!TTt);					\
		TTt = (e+1) % (req)->oftrq_root->oftr_minsz;		\
		psc_assert(!TTt);					\
		psc_trace("OFT_REQ2SE_OFFS (%"PRIx64"/%"PRIx64")", s, e);	\
        }

#define OFT_REQ2E_OFF_(req)						\
	(off_t)(((req)->oftrq_off + ((req)->oftrq_nblks *		\
				     (req)->oftrq_root->oftr_minsz) - 1))

#define OFT_IOV2E_OFF(iov, e) {						\
		e = (((iov)->oftiov_off + ((iov)->oftiov_nblks *	\
					   (iov)->oftiov_blksz)) - 1);	\
	}

#define OFT_IOV2E_OFF_(iov)						\
	(off_t)((((iov)->oftiov_off + ((iov)->oftiov_nblks *		\
				       (iov)->oftiov_blksz)) - 1))

#define OFT_IOV2SE_OFFS(iov, s, e) {					\
		s = (iov)->oftiov_off;					\
		OFT_IOV2E_OFF(iov, e);					\
	}

#define OFT_IOVSZ(iov) (ssize_t)((iov)->oftiov_nblks * ((iov)->oftiov_blksz))

#define OFT_REQ2BLKSZ(req) ((req)->oftrq_root->oftr_minsz)

/* Verify minsz is a power of 2 */
#define OFT_BLKMASK_UPPER(r, o) (((r)->oftr_minsz - 1) & o)
#define OFT_BLKMASK_LOWER(r, o) ((~((r)->oftr_minsz - 1)) & o)

/*
 * Stash assertion routines here
 */

#define oftm_leaf_verify(m) {						\
		if (!ATTR_TEST((m)->oft_flags, OFT_ROOT)) {		\
			if (!(ATTR_TEST((m)->oft_flags, OFT_ALLOCPNDG) || \
			      ATTR_TEST((m)->oft_flags, OFT_UNINIT))) {	\
				psc_assert((m)->oft_norl.oft_iov);	\
				psc_assert(atomic_read(&(m)->oft_ref) == 1); \
			}						\
		}							\
		if (ATTR_TEST((m)->oft_flags, OFT_ALLOCPNDG) ||		\
		    ATTR_TEST((m)->oft_flags, OFT_ROOT))		\
			psc_assert(!ATTR_TEST((m)->oft_flags, OFT_UNINIT)); \
		if (ATTR_TEST((m)->oft_flags, OFT_UNINIT))		\
			psc_assert(!(m)->oft_norl.oft_iov);		\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_LEAF));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_NODE));	\
		if ((m)->oft_norl.oft_iov)				\
			psc_assert((m) == (m)->oft_norl.oft_iov->oftiov_memb); \
	}								\

#define oftm_splitting_leaf_verify(m) {					\
		psc_assert(!(m)->oft_norl.oft_iov);			\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_LEAF));	\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_SPLITTING));	\
		psc_assert(!atomic_read(&(m)->oft_ref));		\
	}								\

#define oftm_node_verify(m) {						\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_NODE));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_LEAF));	\
		psc_assert(atomic_read(&(m)->oft_ref) ||		\
			   ATTR_TEST((m)->oft_flags, OFT_SPLITTING));	\
									\
	}								\

#define oftm_unrelease_verify(m) {					\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_LEAF) ||	\
			   ATTR_TEST((m)->oft_flags, OFT_NODE));	\
		psc_assert(atomic_read(&(m)->oft_ref) ||		\
			   ATTR_TEST((m)->oft_flags, OFT_ALLOCPNDG));	\
	}

#define oftm_root_verify(m) {						\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_ROOT));	\
	}								\

#define oftm_freeleaf_verify(m) {					\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_FREEING));	\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_LEAF));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_NODE));	\
		psc_assert(!atomic_read(&(m)->oft_rdop_ref));		\
		psc_assert(!atomic_read(&(m)->oft_wrop_ref));		\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_ALLOCPNDG));	\
		psc_assert(!(m)->oft_norl.oft_iov);			\
	}

#define oftm_reuse_verify(m) {					\
		psc_assert((m)->oft_flags == OFT_RELEASE);	\
		psc_assert(!atomic_read(&(m)->oft_ref));	\
		psc_assert(!(m)->oft_norl.oft_iov);		\
	}

/* List the conditions for a tree leaf which is prepared for a read.
 */
#define oftm_io_prepped_verify(m) {					\
		psc_assert(ATTR_TEST((m)->oft_flags, OFT_LEAF));	\
		psc_assert((atomic_read(&(m)->oft_rdop_ref) > 0) ||	\
			   (atomic_read(&(m)->oft_wrop_ref) > 0));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_ALLOCPNDG));  \
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_NODE));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_FREEING));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_SPLITTING));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_RELEASE));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_MCHLDGROW));	\
		psc_assert(!ATTR_TEST((m)->oft_flags, OFT_UNINIT));	\
		psc_assert((m) == (m)->oft_norl.oft_iov->oftiov_memb);	\
}

#define oftm_read_prepped_verify(m) {					\
		oftm_io_prepped_verify(m);				\
		psc_assert(atomic_read(&(m)->oft_rdop_ref) > 0);	\
}

#define oftm_write_prepped_verify(m) {					\
		oftm_io_prepped_verify(m);				\
		psc_assert(atomic_read(&(m)->oft_wrop_ref) > 0);	\
}

#ifndef MIN
# define MIN(a,b) (((a)<(b)) ? (a): (b))
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b)) ? (a): (b))
#endif

struct offtree_memb;
struct dynarray;

struct offtree_iov {
	int     oftiov_flags;
	void   *oftiov_base;  /* point to our data buffer  */
	off_t   oftiov_off;   /* data offset               */
	size_t  oftiov_blksz;
	size_t  oftiov_nblks; /* length of respective data */
	void   *oftiov_pri;   /* private data, in slash's case, point at the
	 			  sl_buffer structure which manages the
				  region containing our base */
	struct offtree_memb *oftiov_memb;  /* backpointer to our
					      offtree node */
};

enum oft_iov_flags {
	OFTIOV_DATARDY    = (1 << 0), /* Buffer contains readable data  */
	OFTIOV_FAULTING   = (1 << 1), /* Buffer is being retrieved      */
	OFTIOV_FAULTPNDG  = (1 << 2), /* Our iov is scheduled           */
	OFTIOV_COLLISION  = (1 << 3), /* Collision ops must take place  */
	OFTIOV_FREEING    = (1 << 4), /* Collision ops must take place  */
	OFTIOV_MAPPED     = (1 << 5), /* Mapped to a tree node          */
	OFTIOV_REMAPPING  = (1 << 6), /* Remap to a another tree node   */
	OFTIOV_REMAP_SRC  = (1 << 7), /* IOV is the remap source buffer */
	OFTIOV_REMAP_END  = (1 << 8), /* IOV is the last to remap       */
	OFTIOV_PUSHING    = (1 << 9), /* IOV is being written to IOS    */
	OFTIOV_PUSHPNDG   = (1 << 10) /* IOV is being written to IOS    */
};

#define OFFTIOV_FLAG(field, str) (field ? str : "")
#define DEBUG_OFFTIOV_FLAGS(iov)					  \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_DATARDY),  "d"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_FAULTING), "f"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_FAULTING), "P"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_COLLISION),"p"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_FREEING),  "F"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_MAPPED),   "m"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_REMAPPING),"r"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_REMAP_SRC),"R"), \
	OFFTIOV_FLAG(ATTR_TEST(iov->oftiov_flags, OFTIOV_REMAP_END),"E")
#define OFFTIOV_FLAGS_FMT "%s%s%s%s%s%s%s%s%s"

#define DEBUG_OFFTIOV(level, iov, fmt, ...)				\
	psc_logs((level), PSS_OTHER, 					\
		" oftiov@%p b:%p o:%"PRIx64" l:%"PRId64			\
		" bsz:%"PRId64" pri:%p fl:"OFFTIOV_FLAGS_FMT		\
		" m:%p "fmt,						\
		(iov), (iov)->oftiov_base, (iov)->oftiov_off,		\
		(iov)->oftiov_nblks, (iov)->oftiov_blksz,		\
		(iov)->oftiov_pri, DEBUG_OFFTIOV_FLAGS(iov),		\
		(iov)->oftiov_memb,					\
		## __VA_ARGS__)

#define OFT_MEMB_INIT(m, p)						\
	do {								\
		psc_waitq_init(&(m)->oft_waitq);			\
		LOCK_INIT(&(m)->oft_lock);				\
		atomic_set(&(m)->oft_ref, 0);				\
		atomic_set(&(m)->oft_rdop_ref, 0);			\
		atomic_set(&(m)->oft_wrop_ref, 0);			\
                (m)->oft_parent = p;                            	\
		ATTR_SET((m)->oft_flags, OFT_LEAF);			\
	} while (0)

struct offtree_memb {
	psc_waitq_t           oft_waitq;  /* block here on OFT_GETPNDG */
	psc_spinlock_t        oft_lock;
	u32                   oft_flags;
	atomic_t              oft_ref;    /* hb or nchildren     */
	atomic_t              oft_wrop_ref; /* pending write operations  */
	atomic_t              oft_rdop_ref; /* pending read operations  */
#define oft_op_ref oft_rdop_ref
	struct offtree_memb  *oft_parent;
	u8                    oft_pos;
	u8                    oft_depth;
	u16                   oft_width;
	//struct psclist_head   oft_lentry; /* chain in the slb    */
	union norl {
		struct offtree_iov   *oft_iov;
		struct offtree_memb **oft_children;
	}		      oft_norl;
};

enum oft_attributes {
	OFT_NODE      = (1 << 0), /* Node not a leaf          */
	OFT_LEAF      = (1 << 1), /* Leaf not a node          */
	//	OFT_READPNDG  = (1 << 2), /* Read is about to occur   */
	//OFT_WRITEPNDG = (1 << 3), /* Write is about to occur  */
	OFT_ALLOCPNDG = (1 << 4), /* Alloc is about to occur  */
	OFT_ROOT      = (1 << 6), /* Tree root                */
	OFT_FREEING   = (1 << 7), /* Different from Reap?     */
	OFT_SPLITTING = (1 << 8), /* Leaf becoming a parent   */
	OFT_RELEASE   = (1 << 9), /* Reclaim empty parent     */
	OFT_MCHLDGROW = (1 << 10),/* Multichild grow          */
	OFT_UNINIT    = (1 << 11) /* Uninitialized leaf       */
};

#define OFTM_FLAG(field, str) (field ? str : "")
#define DEBUG_OFTM_FLAGS(oft)					    \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_NODE),      "N"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_LEAF),      "L"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_ALLOCPNDG), "A"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_ROOT),      "r"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_FREEING),   "F"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_SPLITTING), "S"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_RELEASE),   "e"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_UNINIT),    "u"), \
	OFTM_FLAG(ATTR_TEST((oft)->oft_flags, OFT_MCHLDGROW), "g")

#define REQ_OFTM_FLAGS_FMT "%s%s%s%s%s%s%s%s%s"

#define DEBUG_OFT(level, oft, fmt, ...)					\
	do {								\
		if (ATTR_TEST((oft)->oft_flags, OFT_LEAF)) {		\
			psc_logs((level), PSS_OTHER, 			\
				" oft@%p pos:%hhu d:%hhu w:%hu p:%p "	\
				"ref:%d rref:%d wref:%d"		\
				" fl:"REQ_OFTM_FLAGS_FMT" "fmt,		\
				(oft), (oft)->oft_pos,			\
				(oft)->oft_depth,			\
				(oft)->oft_width, (oft)->oft_parent,	\
				atomic_read(&(oft)->oft_ref), 		\
				atomic_read(&(oft)->oft_rdop_ref),	\
				atomic_read(&(oft)->oft_wrop_ref),	\
				DEBUG_OFTM_FLAGS(oft),			\
				## __VA_ARGS__);			\
		} else {						\
			psc_logs((level), PSS_OTHER, 			\
				" oft@%p pos:%hhu d:%hhu w:%hu p:%p "	\
				"ref:%d rref:%d wref:%d"		\
				" fl:"REQ_OFTM_FLAGS_FMT" "fmt,		\
				(oft), (oft)->oft_pos,			\
				(oft)->oft_depth,			\
				(oft)->oft_width, (oft)->oft_parent,	\
				atomic_read(&(oft)->oft_ref),		\
				atomic_read(&(oft)->oft_rdop_ref),	\
				atomic_read(&(oft)->oft_wrop_ref),	\
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
typedef int  (*offtree_alloc_fn)(size_t, off_t, struct dynarray *, void *);
typedef void (*offtree_putnode_cb)(struct offtree_memb *);
typedef void (*offtree_slbpin_cb)(struct offtree_iov *, int);

struct offtree_root {
	psc_spinlock_t oftr_lock;
	u32            oftr_width;
	u32            oftr_maxdepth;
	size_t         oftr_mapsz; /* map size, how many bytes are tracked? */
	size_t         oftr_minsz; /* minimum chunk size                    */
	void          *oftr_pri;   /* opaque backpointer (use bmap for now) */
	struct offtree_memb oftr_memb; /* root member                       */
	offtree_alloc_fn    oftr_alloc;
	offtree_putnode_cb  oftr_putnode_cb;
	offtree_slbpin_cb   oftr_slbpin_cb;
};

struct offtree_fill {
	struct pscrpc_request_set *oftfill_reqset; /* Our request set */
	struct dynarray           *oftfill_inprog; /* already in progress   */
	psc_spinlock_t             oftfill_lock;
};

struct offtree_req {
	struct offtree_root  *oftrq_root;
	struct offtree_memb  *oftrq_memb;   /* pointer to request node head */
	struct dynarray      *oftrq_darray; /* sorted array of buffer iov's */
	off_t                 oftrq_off;    /* aligned, file-logical offset  */
	size_t                oftrq_nblks;  /* number of blocks requested */
	u8                    oftrq_op;
	u8                    oftrq_depth;
	u16                   oftrq_width;
	off_t                 oftrq_darray_off;
	struct offtree_fill   oftrq_fill;
	void                 *oftrq_bmap;
#define oftrq_len oftrq_nblks               /* reuse oftrq_nblks in dio mode */
	struct psclist_head   oftrq_lentry; /* chain onto bmap */
};

enum offtree_req_op_types {
	OFTREQ_OP_NOOP  = (1<<0),
	OFTREQ_OP_READ  = (1<<1),
	OFTREQ_OP_WRITE = (1<<2),
	OFTREQ_OP_PRFFP = (1<<3),
	OFTREQ_OP_PRFLP = (1<<4),
	OFTREQ_OP_DIO   = (1<<5)
};

static inline size_t
oftrq_size_get(const struct offtree_req *r)
{
	if (r->oftrq_op & OFTREQ_OP_DIO)
		return r->oftrq_len;
	else
		return (size_t)(r->oftrq_nblks * SLASH_BMAP_BLKSZ);
}

#define REQ_OFTRQ_FLAGS_FMT "%s%s%s%s%s"

#define DEBUG_OFTRQ_FLAGS(oftrq)					\
	OFTM_FLAG(ATTR_TEST((oftrq)->oftrq_op, OFTREQ_OP_NOOP),  "N"),	\
	OFTM_FLAG(ATTR_TEST((oftrq)->oftrq_op, OFTREQ_OP_READ),  "R"),	\
	OFTM_FLAG(ATTR_TEST((oftrq)->oftrq_op, OFTREQ_OP_WRITE), "W"),	\
	OFTM_FLAG(ATTR_TEST((oftrq)->oftrq_op, OFTREQ_OP_PRFFP), "p"),	\
	OFTM_FLAG(ATTR_TEST((oftrq)->oftrq_op, OFTREQ_OP_PRFLP), "l")

#define DEBUG_OFFTREQ(level, oftr, fmt, ...)				\
	psc_logs((level), PSS_OTHER, 					\
		" oftr@%p o:%"PRIx64" l:%"PRId64" node:%p darray:%p"	\
		" root:%p op:%hhu d:%hhu w:%hu "			\
		REQ_OFTRQ_FLAGS_FMT" "fmt,				\
		(oftr), (oftr)->oftrq_off, (oftr)->oftrq_nblks,		\
		(oftr)->oftrq_memb, (oftr)->oftrq_darray,		\
		(oftr)->oftrq_root, (oftr)->oftrq_op,			\
		(oftr)->oftrq_depth, (oftr)->oftrq_width,		\
		DEBUG_OFTRQ_FLAGS(oftr),				\
		## __VA_ARGS__)

static inline int
oft_child_get(off_t o, struct offtree_root *r, int d, int abs_width)
{
	off_t soff = OFT_STARTOFF(r, d, abs_width);

	/* ensure that the parent is responsible for the given range */
	if (!((o >= soff) && (o < OFT_ENDOFF(r, d, abs_width))))
		abort();

	return ((o - soff) / OFT_REGIONSZ(r, (d+1)));
}

static inline int
oft_child_req_get(off_t o, struct offtree_req *req)
{
	off_t soff = OFT_REQ_STARTOFF(req);

	/* Ensure that the parent is responsible for the given range
	 */
	if (!((o >= soff) && (o <= OFT_REQ_ENDOFF(req))))
		abort();

	return ((o - soff) / OFT_REGIONSZ(req->oftrq_root,
					  (req->oftrq_depth+1)));
}

#define OFT_REF_DEC 0
#define OFT_REF_INC 1

#define oft_refcnt_dec(r, m) oft_adjust_refcnt((r), (m), OFT_REF_DEC)
#define oft_refcnt_inc(r, m) oft_adjust_refcnt((r), (m), OFT_REF_INC)

static inline void
oft_adjust_refcnt(struct offtree_req *req, struct offtree_memb *m, int op)
{
	if (op == OFT_REF_DEC) {
		if (req->oftrq_op == OFTREQ_OP_WRITE)
			atomic_dec(&m->oft_wrop_ref);

		if (req->oftrq_op == OFTREQ_OP_READ)
			atomic_dec(&m->oft_rdop_ref);
	}
	if (op == OFT_REF_INC) {
		if (req->oftrq_op == OFTREQ_OP_WRITE)
			atomic_inc(&m->oft_wrop_ref);

		if (req->oftrq_op == OFTREQ_OP_READ)
			atomic_inc(&m->oft_rdop_ref);
	}
}


static inline void
oftiov_2_iov(const struct offtree_iov *v, struct iovec *i)
{
	i->iov_base = v->oftiov_base;
	i->iov_len  = OFT_IOVSZ(v);
}


struct offtree_root *
offtree_create(size_t, size_t, u32, u32, void *, offtree_alloc_fn,
	       offtree_putnode_cb, offtree_slbpin_cb);

void offtree_destroy(struct offtree_root *);
int offtree_region_preprw(struct offtree_req *);
void offtree_iovs_check(const struct offtree_iov *, const int);
void offtree_freeleaf_locked(struct offtree_memb *);

#endif /* _OFFTREE_H_ */
