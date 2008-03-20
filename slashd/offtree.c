#include "offtree.h"
#include "psc_util/alloc.h"

static void
offtree_node_reuse_locked(struct offtree_memb *m)
{
	/* Reuse, don't release
	 */
	psc_assert(m->oft_flags == OFT_RELEASE);
	psc_assert(!atomic_read(m->oft_ref));
	psc_assert(!m->norl.oft_iov);
	m->oft_flags = OFT_LEAF | OFT_ALLOCPNDG;
}

static void
offtree_node_release(struct offtree_memb *oftm)
{
	struct offtree_memb *parent = oftm->oft_parent;

	spinlock(&parent->oft_lock);
	spinlock(&oftm->oft_lock);

	if (!ATTR_TEST(oftm->oft_flags, OFT_RELEASE)) {
		psc_assert(ATTR_TEST(oftm->oft_flags, OFT_LEAF) ||
			   ATTR_TEST(oftm->oft_flags, OFT_NODE));
		psc_assert(atomic_read(oftm->oft_ref));
		DEBUG_OFT(PLL_INFO, oftm, "was reclaimed quickly");	
		goto out;
	}
	DEBUG_OFT(PLL_INFO, oftm, "releasing child .. ");
	DEBUG_OFT(PLL_INFO, parent, ".. from parent");

	freelock(&oftm->oft_lock);
	PSCFREE(oftm);
	parent->norl.oft_children[oftm->oft_pos] = NULL;

	if (atomic_dec_and_test(&parent->oft_ref)) {
		ATTR_SET(parent->oft_flags, OFT_COLLAPSE);
		freelock(&parent->oft_ref);
		offtree_node_release(parent);
	} else {
	out:
		freelock(&oftm->oft_lock);
		freelock(&parent->oft_lock);
	}
}

/**
 * offtree_leaf2node - must be called locked.
 * @oftm:  tree member to promote.
 */
static void
offtree_leaf2node_locked(struct offtree_memb *oftm)
{	
	ATTR_SET(m->oft_flags, OFT_SPLITTING);
	m->norl.oft_children = PSC_ALLOC(sizeof(struct offtree_memb **) * 
					 r->oftr_width);	
	ATTR_UNSET(m->oft_flags, OFT_LEAF);
	ATTR_SET(m->oft_flags, OFT_NODE);
	ATTR_UNSET(m->oft_flags, OFT_SPLITTING);	
}

struct offtree_root *
offtree_create(size_t mapsz, size_t minsz, u32 width, u32 depth,
	       void *private, offtree_alloc_fn alloc_fn)
{
	struct offtree_root *t = PSCALLOC(sizeof(struct offtree_root));
	
	LOCK_INIT(&t->oftr_lock);
	t->oftr_width    = width;
	t->oftr_mapsz    = minsz;
	t->oftr_minsz    = mapsz;
	t->oftr_maxdepth = depth;
	t->oftr_alloc    = alloc_fn;
	t->oftr_pri      = private;  /* our bmap handle */
	ATTR_SET(t->oftr_flags, OFT_ROOT);
	
	return (t);
}

void
offtree_freeleaf_locked(struct offtree_memb *oftm)
{
	struct offtree_memb *parent = oftm->oft_parent;
	int root=0;

	if (!parent) {
		psc_assert(ATTR_TEST(oftm->oft_flags, OFT_ROOT));
		root = 1;
	} else
		psc_assert(ATTR_TEST(parent->oft_flags, OFT_NODE));

	/* Only leafs have pages */
	psc_assert(ATTR_TEST(oftm->oft_flags, OFT_FREEING));
	psc_assert(ATTR_TEST(oftm->oft_flags, OFT_LEAF));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_NODE));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_REQPNDG));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_READPNDG));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_WRITEPNDG));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_ALLOCPNDG));
	psc_assert(!oftm->norl.oft_iov);
	
	if (root) {
		/* Reset the tree root
		 */
		oftm->oft_flags = OFT_ROOT;
		freelock(&oftm->oft_lock);
		return;
	}

	psc_assert(parent->norl.oft_children[oftm->oft_pos] == otfm);
	freelock(&oftm->oft_lock);
	
	PSCFREE(oftm);
	parent->norl.oft_children[oftm->oft_pos] = NULL;
	if (atomic_dec_and_test(&parent->oft_ref)) {
		ATTR_SET(parent->oft_flags, OFT_RELEASE);
		freelock(&parent->oft_ref);
		offtree_node_release(paren);
	}
}

static void
offtree_iovs_check(struct offtree_iov *iovs, int niovs) 
{
	int   i, j;
	off_t e;

	for (i=0; i < niovs; i++) {
		/* No empty iovs */		
		psc_assert(iovs[i]->oftiov_nblks);
		
		if (i)
			psc_assert(iovs[i]->oftiov_off == (e + 1));
		
		OFT_IOV2E_OFF(iovs[i], e);
		
		for (j=0; j < niovs; j++) {
			if (i == j) continue;
			//check for overlapping bases?
		}
	}
}

static size_t
offtree_calc_nblks_hb_int(struct offtree_req *r, struct offtree_iov *v, 
			  size_t *front, size_t *back)
{
	size_t nblks = r->oftrq_nblks;
	off_t  nr_soffa, nr_eoffa, hb_soffa, hb_eoffa;

	psc_assert(v && front && back);

	*front = *back = 0;

	OFT_REQ2SE_OFFS(r, nr_soffa, nr_eoffa);
	OFT_IOV2SE_OFFS(v, hb_soffa, hb_eoffa);	
	DEBUG_OFFTREQ(PLL_TRACE, r, "req eoffa="LPX64, nr_eoffa);
	DEBUG_OFFTIOV(PLL_TRACE, v, "hb  eoffa="LPX64, hb_eoffa);       
	/* Otherwise we shouldn't be here 
	 */
	psc_assert((nr_soffa < hb_soffa) || (nr_eoffa > hb_eoffa));
	
	if (nr_soffa < hb_soffa) {
		if (nr_eoffa < hb_soffa) {
			/* Regions do not overlap */
			*front = nblks;
			goto out;
		} else
			/* Frontal overlap, also cover the case where
			 *   the have_buffer is completely enveloped.
			 */
			*front = (hb_soffa - nr_soffa) % m;
	}	
	if (nr_eoffa > hb_eoffa) {
		if (nr_soffa > hb_eoffa)
			/* Regions do not overlap */
			*back = nblks;
			goto out;
		else 
			*back = (nr_eoffa - hb_eoffa) % m;
	}
 out:	
	return (*back + *front);
}


/*
 * offtree_blks_get - allocate memory blocks for request 'req'.  Take into account an existing memory buffer and manage the allocation array.  On successful completion, the req->oftrq_darray will contain an array of iov's which may be used for I/O.  If the 'have_buffer' exists, then it will be placed at the correct logical location within the iov array.  Since our blocks are aligned this is possible.
 * @req: the allocation request (which also holds the array).
 * @hb_iov: if !NULL, represents the currently held buffer.
 * Returns '0' on success.
 */
static ssize_t
offtree_blks_get(struct offtree_req *req, struct offtree_iov *hb_iov)
{
	ssize_t tblks=0, rc=0;
	size_t  niovs=0;
	struct  offtree_iov  *miovs, 
	struct  offtree_root *r = req->oftrq_root;

	tblks = req->oftrq_nblks;

	/* Determine nblks taking into account overlap */
	if (!hb_iov) {		
		rc = (r->oftr_alloc)(tblks, &miovs, &niovs, r);
		if (rc != nblks) {
			if (rc < tblks) {
				rc = -1;
				goto done;
			} else
				psc_warnx("Wanted "LPX64" got "LPX64, 
					  tblks, rc);
		}		       
		psc_assert(niovs);

		for (j=0; j < niovs; j++)
			dynarray_add(req->oftrq_darray, 
				     (const void *)miovs[j]);

	} else {
		size_t front=0, back=0, nblks;
		/* The 'have_buffer' needs to be placed at the correct
		 *  offset in the oftrq_darray, so determine the number
		 *  of blocks which are required ahead-of and behind the 
		 *  have_buffer.  Add the iov's to the array in the 
		 *  correct order.
		 */
		nblks = offtree_calc_nblks_hb_int(req, iov, &front, &back);
		psc_assert(front || back);
		/* Allocate 'front' blocks and add their iovs to the front
		 *  of the queue..
		 */
		if (front) {
			rc = (r->oftr_alloc)(front, &miovs, &niovs, r);
			if (rc != front) {
				if (rc < front) {
					rc = -1;
					goto done;
				} else
					psc_fatalx("Wanted "LPX64" got "LPX64, 
						   front, rc);
			}
			psc_assert(niovs);
			/* Place them at the beginning of the array */
			for (j=0; j < niovs; j++)
				dynarray_add(req->oftrq_darray, 
					     (const void *)miovs[j]);
		}
		/* Add the 'have_buffer' iov to the array here in the middle */
		dynarray_add(req->oftrq_darray, iov);
		
		if (back) {
			/* Allocate 'back' blocks */
			rc = (r->oftr_alloc)(back, &miovs, &niovs, r);
			if (rc != back) {
				if (rc < back) {
					rc = -1;
					goto done;
				} else
					psc_fatalx("Wanted "LPX64" got "LPX64, 
						  back, rc);
			}		       
			psc_assert(niovs);
			for (j=0; j < niovs; j++)
				/* Allocate 'rear' blocks and add their
				 *  to the end of the queue..
				 */
				dynarray_add(req->oftrq_darray, 
					     (const void *)miovs[j]);
		}
	}
 done:
	if (rc < 0)
		return (rc);

	return (tblks);
}

#define NEW_PARTIAL_IOV(n, o, off, nblks)				\
	do {								\
		(n) = PSCALLOC(sizeof(struct offtree_req));		\
		(n)->oftiov_base  = (o)->oftiov_base +			\
			(off * (o)->oftiov_blksz);			\
		(n)->oftiov_blksz = (o)->oftiov_blksz;			\
		(n)->oftiov_pri   = (o)->oftiov_pri;			\
		(n)->oftiov_nblks = nblks;				\
	} while (0)		       

/*
 * offtree_putleaf - apply buffers to a leafh
 *
 offtree_putnode(req, iovoff, tiov_cnt, sblkoff);	
 * Notes:  don't modify source iov's directly.  Copy them using NEW_PARTIAL_IOV.
 *  Putnode doesn't have to discover hb's, they've already been accounted for in the iov array.
 */
static void
offtree_putnode(struct offtree_req *req, int iovoff, int iovcnt, int blkoff)
{
	struct offtree_iov *iov, *tiov;

	DEBUG_OFFTREQ(PLL_INFO, req, "o:%d, c:%d, bo:%d"
		      iovoff, iovcnt, blkoff);

	psc_assert(req->oftrq_darray);
	psc_assert(req->oftrq_root);
	psc_assert(req->oftrq_memb);
	psc_assert(req->oftrq_depth < req->oftrq_root->oftr_maxdepth);
	psc_assert(iovcnt > 0);

	if (iovcnt == 1) {
		iov = dynarray_getpos(req->oftrq_darray, iovoff);
		psc_assert((iov->oftiov_nblks - blkoff) >= req->oftrq_nblks);
		
		DEBUG_OFFTIOV(PLL_INFO, iov, "hb");

		if (!blkoff) {
			req->oftr_oftm->norl.oft_iov = iov;
			psc_assert(!iovoff && !blkoff);
			sl_oftm_addref(req->oftr_oftm);
		} else {
			/* Only modref if the request doesn't use the 
			 *   entire iov otherwise the existing reference
			 *   will suffice.  sl_oftiov_modref handles
			 *   assertions.
			 */
			NEW_PARTIAL_IOV(tiov, iov, blkoff, req->oftrq_nblks);
			req->oftr_oftm->norl.oft_iov = tiov
			sl_oftm_addref(req->oftr_oftm);
		}
		goto out;
		
	} else {
		/* Can't directly map more than 1 iov so recursion is needed.
		 */
		struct offtree_req myreq;
		off_t  nr_soffa, nr_eoffa;
		off_t  rg_soff = OFT_REQ_STARTOFF(req);
		off_t  rg_eoff = OFT_REQ_ENDOFF(req);
		off_t  ioffa;
		int    tchild, schild, echild;
		int    j=0, b=0, tiov_cnt=1;
		int    nblks=req->oftrq_nblks;
		/* 
		 * ***Promote to parent node***
		 *   Manage creation of children and preservation 
		 *     of attached buffer (if any) - the messy case
		 *   This case isn't so bad because there are no nodes 
		 *    below us, only leafs.
		 */
		offtree_leaf2node_locked(req->oftrq_memb);	
		/* Determine affected children 
		 */
		OFT_REQ2SE_OFFS(req, nr_soffa, nr_eoffa);

		schild = oft_child_req_get(nr_soffa, req);
		echild = oft_child_req_get(nr_eoffa, req);
		
		psc_assert((schild >= 0) && 
			   (echild >= 0) && 
			   (schild <= echild));
		/* 
		 * Iterate over affected subregions, alloc'ing leaf nodes
		 *  and placing buffers.
		 * How many blocks go to each child? Middle children must
		 *   be complete.
		 */
		memcpy(&myreq, req, (sizeof(*req)));
		myreq.oftr_depth++;
		myreq.oftr_width = OFT_REQ_ABSWIDTH_GET(&myreq, schild);

		for (j=0, b=0, i_offa=nr_soffa, tchild=schild; 
		     tchild <= echild; 
		     j++, tchild++, myreq.oftr_width++) {

			rg_soff = OFT_REQ_STARTOFF(&myreq);
			rg_eoff = OFT_REQ_ENDOFF(&myreq);		
			/* This should always be true */
			psc_assert(nr_soffa <= rg_soff); 	

			if (tchild > schild)
				psc_assert(i_offa == rg_soff);
			/* How many blocks fit within this range?
			 *  Push offset iterator i_offa
			 */
			i_offa += myreq.oftr_nblks = 
				(MIN(rg_eoff, nr_eoffa) + 1) - i_offa;

			psc_assert(!(myreq.oftr_nblks % OFT_REQ2BLKSZ(req)));

			myreq.oftrq_nblks /= OFT_REQ2BLKSZ(&myreq);  
			myreq.oft_memb     = offtree_newleaf(req->oftrq_memb, 
							     tchild);
			myreq.oftrq_off    = MAX(OFT_REQ_STARTOFF(&myreq), 
						 req->oftrq_off);
			nblks             -= myreq.oftrq_nblks;
			/* More middle child sanity (middle children must
			 *  consume their entire region).
			 */
			if (j < echild)
				psc_assert((i_offa - 1) == rg_eoff);
			/* How many iovs are needed to fill the child? 
			 *  Inspect our array of iov's. 
			 *  @iovoff: is the 'persisent' iterator
			 *  @tiov_cnt: used to inform the child of how many
			 *    iov's are present.
			 */
			tiov = dynarray_getpos(req->oftrq_darray,
					       (iovoff + (tiov_cnt-1)));
			/* Factor in partially used iov's */
			b = (tiov->oftiov_nblks - blkoff);
			psc_assert(b > 0);

			while (b < myreq.oftrq_nblks) {
				tiov_cnt++; /* persistent count */
				psc_assert(tiov_cnt <= iovcnt);
				tiov = dynarray_getpos(req->oftrq_darray, 
						       (iovoff + (tiov_cnt-1)));
				b += tiov->oftiov_nblks;
			}
			offtree_putnode(req, iovoff, tiov_cnt, blkoff);
			/* Bump iovoff, subtract one if the current
			 *   iov in underfilled.
			 */
			iovoff += tiov_cnt - ((b > myreq.oftrq_nblks) ? 1 : 0);
			/* At which block in the iov do we start? */
			if (b)
				blkoff = tiov->oftiov_nblks - 
					(b - myreq.oftrq_nblks);
			else 
				blkoff = 0;
			
			if (tchild == echild) 
				psc_assert(!nblks && !blkoff);
		}
	}		
 out:
}

/*
 *
 * New children may have to deal with old have_buffers.  Old buffers and new buffers are the same..

 */
struct offtree_memb *
offtree_newleaf(struct offtree_memb *parent, int pos)
{
	struct offtree_memb *new;
	
	psc_assert(!parent->norl.oft_children[pos]);
	
	new = PSC_ALLOC(sizeof(struct offtree_memb));
	OFT_MEMB_INIT(new);
	ATTR_SET(new->oft_flags, OFT_REQPNDG);
	ATTR_SET(new->oft_flags, OFT_ALLOCPNDG);
	new->oft_pos = pos;
	parent->norl.oft_children[pos] = new;
	atomic_inc(&parent->oft_ref);

	return new;
}

/*
 * offtree_region_preprw_leaf_locked - the stage 2 call into the tree, it does not recurse but rather handles the allocation step taking into account existing buffers. 
 */
int
offtree_region_preprw_leaf_locked(struct offtree_req *req)
{
	struct  offtree_memb *m = req->oftrq_memb;
	struct  offtree_iov  *iov = NULL;	
	off_t   nr_soffa, nr_eoffa, hb_soffa=0, hb_eoffa=0;
	ssize_t nblks;

	psc_assert(ATTR_TEST(m->oft_flags, OFT_LEAF));
	psc_assert(ATTR_TEST(m->oft_flags, OFT_REQPNDG));

	DEBUG_OFFTREQ(PLL_INFO, req, "new req");

	OFT_REQ2SE_OFFS(req, nr_soffa, nf_eoffa);

	if (!ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG)) {
		iov = m->norl.oft_iov;
		/* Intention to allocate must be determined ahead of time.
		 */
		psc_assert(iov && iov->oftiov_base && iov->oftiov_nblks);

		OFT_IOV2SE_OFFS(iov, hb_soffa, hb_eaoffa);
		DEBUG_OFFTIOV(PLL_INFO, iov, "hb");	

		if ((nr_soffa >= hb_soffa) && (nr_eoffa <= hb_eoffa)) {
			DEBUG_OFFTREQ(PLL_TRACE, req, 
				      "req fulfilled by existing hb %p", iov);
			goto done;
		} else 
			/* Existing allocation is not sufficient, prep
			 *  for adding memory.
			 */
			ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);
	}	
	/* Allocate the blocks taking into accout a currently 
	 *   held buffer (have_buffer) in 'iov'.  
	 */
	if ((nblks = offtree_blks_get(req, iov)) < 0)
		goto error;
	/* How many iovs did we get back?
	 */
	niovs = dynarray_len(req->oftrq_darray);
	psc_assert(niovs > 0);

	if (niovs == 1) {
		 /* Should only have 1 new buffer and no exisiting buffers.
		  */
		psc_assert(!m->norl.oft_iov);
		m->norl.oft_iov = dynarray_get(req->oftrq_darray);
		DEBUG_OFFTREQ(PLL_TRACE, req, 
			      "req fulfilled by a new buffer");
		DEBUG_OFFTIOV(PLL_INFO, m->norl.oft_iov, "new hb");
		offtree_putnode(req, 0, 1, 0);
		goto done;

	} else {
		struct offtree_req myreq;
		off_t  rg_soff = OFT_REQ_STARTOFF(req);
		off_t  rg_eoff = OFT_REQ_ENDOFF(req);
		off_t  ioffa;
		int    tchild, schild, echild;
		int    j=0, iovoff=0, sblkoff=0;
		/* 
		 * ***Promote to parent node***
		 *   Manage creation of children and preservation 
		 *     of attached buffer (if any) - the messy case
		 *   This case isn't so bad because there are no nodes 
		 *    below us, only leafs.
		 */
		offtree_leaf2node_locked(m);	
		/* Determine affected children 
		 */
		schild = oft_child_req_get(nr_soffa, req);
		echild = oft_child_req_get(nr_eoffa, req);
		
		psc_assert((schild >= 0) && 
			   (echild >= 0) && 
			   (schild <= echild));
		/* 
		 * Iterate over affected subregions, alloc'ing leaf nodes
		 *  and placing buffers.
		 *
		 * How many blocks go to each child? Middle children must
		 *   be complete.
		 */
		myreq.oftr_depth++;

		for (j=0, i_offa=nr_soffa, tchild=schild; tchild <= echild; 
		     j++, tchild++) {
			int tiov_cnt=1, b=0, nchldblks=0;
			struct offtree_iov  *tiov;
			struct offtree_memb *tmemb;
			
			memcpy(&myreq, req, (sizeof(*req)));
			
			rg_soff = OFT_REQ_STARTOFF(req);
			rg_eoff = OFT_REQ_ENDOFF(req);
			/* This should always be true */
			psc_assert(nr_soffa <= rg_soff); 	
			/* Middle child sanity (middle children must
			 *   be completely used).
			 */
			if (tchild > schild)
				psc_assert(i_offa == rg_soff);
			/* How many blocks fit within this range?
			 *  Push offset iterator i_offa
			 */
			i_offa += myreq.oftr_nblks = 
				(MIN(rg_eoff, nr_eoffa) + 1) - i_offa;

			psc_assert(!(nchldblks % OFT_REQ2BLKSZ(req)));
			/* More middle child sanity (middle children must
			 *  consume their entire region).
			 */
			if (j < echild)
				psc_assert((i_offa - 1) == rg_eoff);
			/* How many iovs are needed to fill the child? 
			 *  Inspect our array of iov's. 
			 *  @iovoff: is the 'persisent' iterator
			 *  @tiov_cnt: used to inform the child of how many
			 *    iov's are present.
			 */
			tiov = dynarray_getpos(req->oftrq_darray,
					       (iovoff + tiov_cnt));
			/* Factor in partially used iov's */
			b = (tiov->oftiov_nblks - sblkoff);
			while (b < myreq.oftrq_nblks) {
				tiov_cnt++;
				tiov = dynarray_getpos(req->oftrq_darray, 
						       (iovoff + tiov_cnt));
				b += tiov->oftiov_nblks;
			}
			/* Make the child... */
			myreq.oft_memb     = offtree_newleaf(m, tchild);
			myreq.oftrq_width  = OFT_REQ_ABSWIDTH_GET(req, tchild);
			myreq.oftrq_off    = MAX(OFT_REQ_STARTOFF(&myreq), 
						 req->oftrq_off);
			myreq.oftrq_nblks /= OFT_REQ2BLKSZ(req);
			nblks             -= myreq.oftrq_nblks;
			offtree_putnode(req, iovoff, tiov_cnt, sblkoff);
			/* Bump iovoff */
			iovoff += tiov_cnt - ((b > myreq.oftrq_nblks) ? 1 : 0);
			/* At which block in the iov do we start? */
			if (b)
				sblkoff = tiov->oftiov_nblks - 
					(b - myreq.oftrq_nblks);
			else 
				sblkoff = 0;
			
			if (tchild == echild)
				psc_assert(!sblkoff);
		}
	}
 done:	
}

/**
 * offtree_region_preprw - Given a memory region request (offtree_req), ensure that the proper memory allocation is performed to satisfy the request.  Return an array of iovec buffers in req->oftrq_darray.  This function is the first stage of a two-stage recursive process.  Recursion in stage1 is descent based, where the process recurses until the request's root node is located.  Several factors are taken into consideration including existing tree nodes and buffers.  The procedure is complicated by the fact that offtree makes use of sparseness where possible.
 * @req: offtree request which contains root and member pointers.  
 * Notes: returns the offtree_memb which is the head of the request.  The offtree_memb is tagged with OFT_REQPNDG so that it will not be freed.  Request offset must be aligned. 
 * Notes1: called before processing a system read or write routine to prepare the file-wise cache for the operation.
 * Return: '0' for success, '-1' failure.  On success the darray pointer contained in the request will contain the src/dst iovectors, in their respective order.
 */
int 
offtree_region_preprw(struct offtree_req *req)
{
	struct offtree_memb *m = req->oftrq_memb;
	off_t  nr_soffa, nr_eoffa;
	int    scnt=0;

	psc_assert(req->oftrq_darray);
	psc_assert(req->oftrq_root);
	psc_assert(req->oftrq_memb);	

	OFT_REQ2SE_OFFS(req, nr_soffa, nr_eoffa);
	OFT_VERIFY_REQ_SE(req, nr_soffa, nr_eoffa);

 wakeup_retry:
	DEBUG_OFFTREQ(PLL_TRACE, req, "soff=%"ZLPX64" eoff=%"ZLPX64" scnt=%d",
		      nr_soffa, nr_eoffa, scnt);	

	spinlock(&m->oft_lock);

	DEBUG_OFT(PLL_TRACE, m, "member");
	/* Verify tree node state
	 */
	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		psc_assert(!ATTR_TEST(m->oft_flags, OFT_NODE));		
		if (ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG)) {
			/* Block for completion of allocation 
			 */
			DEBUG_OFT(PLL_TRACE, m, 
				  "block on OFT_ALLOCPNDG req:%p", req);
			psc_waitq_wait(&m->oft_waitq, &m->oft_lock);
			scnt++;
			goto wakeup_retry;
		}
		/* Found a leaf, drop into stage2 
		 */
	runleaf:
		ATTR_SET(m->oft_flags, OFT_REQPNDG);
		return(offtree_region_preprw_leaf_locked(req));

	} else if (m->oft_flags == OFT_RELEASE) {
		offtree_node_reuse_locked(m);
		goto runleaf;

	} else if (ATTR_TEST(m->oft_flags, OFT_NODE)) {
		int schild, echild;

		psc_assert(!ATTR_TEST(m->oft_flags, OFT_LEAF));
		if (ATTR_TEST(m->oft_flags, OFT_MCHLDGROW)) {
			DEBUG_OFT(PLL_TRACE, m, 
				  "block on OFT_MCHLDGROW req:%p", req);
			psc_waitq_wait(&m->oft_waitq, &m->oft_lock);
			scnt++;
			goto wakeup_retry;
		}
		/* Am I the root or is it one of my children? 
		 */
		schild = oft_child_req_get(nr_soffa, req);
		echild = oft_child_req_get(nr_eoffa, req);
		
		if (schild == echild) {
			struct offtree_memb *new;

			new = m->norl.oft_children[schild];		
			if (!new) 
				new = offtree_newleaf(m, schild);
			
			req->oftrq_memb = new;			
			/* Release lock after assigning child
			 */
			freelock(&m->oft_lock);
			/* Request can be handled by our child, recurse 
			 */
			req->oftrq_depth++;
			req->oftrq_width = OFT_REQ_ABSWIDTH_GET(req, schild);

			return (offtree_region_preprw(req));

		} else {
			struct offtree_req myreq;
			size_t nblks = req->oftrq_nblks;
			int    tchild;

			psc_assert(echild > schild);
			
			ATTR_SET(m->oft_flags, OFT_MCHLDGROW);
			freelock(&m->oft_lock);
			memcpy(&myreq, req, (sizeof(*req)));
			/* Requested range straddles multiple children so 
			 *  my node (m) is the request root.  Format individual
			 *  requests for all affected children.  The darray 
			 *  pointer contained in req should remain intact and 
			 *  contain the iovecs on completion.
			 */
			myreq.oftrq_depth++;
			for (tchild=schild; tchild <= echild; tchild++) {
				myreq.oftrq_width = OFT_REQ_ABSWIDTH_GET(req, tchild);
				myreq.oftrq_off   = MAX(OFT_REQ_STARTOFF(&myreq), req->oftrq_off);
				myreq.oftrq_nblks = MIN(OFT_REQ_REGIONBLKS(&myreq), nblks);
				myreq.oftrq_memb  = m->norl.oft_children[tchild];
				nblks            -= myreq.oftrq_nblks;
				if (myreq.oftrq_memb) {
					DEBUG_OFFTREQ(PLL_TRACE, &myreq, "existing child");
					spinlock(&myreq.oftrq_memb->oft_lock);
					if (ATTR_TEST(myreq.oftrq_memb->oft_flags, OFT_LEAF)) {
						offtree_region_preprw_leaf_locked(&myreq);
						freelock(&myreq.oftrq_memb->oft_lock);
					} else {
						freelock(&myreq.oftrq_memb->oft_lock);
						offtree_region_preprw(&myreq);
					}
				} else {
					myreq.oftrq_memb = offtree_newleaf(m, tchild);
					DEBUG_OFFTREQ(PLL_TRACE, &myreq, "new child");
					spinlock(&myreq.oftrq_memb->oft_lock);
					offtree_region_preprw_leaf_locked(&myreq);
					freelock(&myreq.oftrq_memb->oft_lock);
				}
			}
		}
	} else
		psc_fatalx("Invalid offtree node state %d", m->oft_flags);

 done:
	ATTR_UNSET(m->oft_flags, OFT_REQPNDG);
	freelock(&m->oft_lock);
	return (m);
 error:
	/* do the right node mgmt here.. */
	return (-ENOMEM);
}
