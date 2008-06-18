#include "offtree.h"
#include "psc_util/alloc.h"


static void
offtree_iov_array_dump(const struct dynarray *a)
{
	int i, n=dynarray_len(a);
	struct offtree_iov *v;
	
	for (i=0; i < n; i++) {
		v = dynarray_getpos(a, i);
		DEBUG_OFFTIOV(PLL_WARN, v, "offtree_iov_array_dump");
	}
}


struct offtree_root *
offtree_create(size_t mapsz, size_t minsz, u32 width, u32 depth, 
	       void *private, offtree_alloc_fn alloc_fn, 
	       offtree_putnode_cb putnode_cb_fn, 
	       offtree_slbpin_cb  slbpin_cb_fn)
{
	struct offtree_root *t = PSCALLOC(sizeof(struct offtree_root));
	
	LOCK_INIT(&t->oftr_lock);
	t->oftr_width    = width;
	t->oftr_mapsz    = mapsz;
	t->oftr_minsz    = minsz;
	t->oftr_maxdepth = depth;
	t->oftr_alloc    = alloc_fn;
	t->oftr_pri      = private;  /* our fcache handle */
	t->oftr_putnode_cb = putnode_cb_fn;
	t->oftr_slbpin_cb  = slbpin_cb_fn;

	OFT_MEMB_INIT(&t->oftr_memb, NULL);
	//atomic_set(&t->oftr_memb.oft_op_ref, 1);
	ATTR_SET(t->oftr_memb.oft_flags, OFT_ROOT);
	
	return (t);
}

/**
 * offtree_iovs_check - iov array verifier.
 * @iovs: array of iovs
 * @niovs: number of iovs in array
 */
void
offtree_iovs_check(const struct offtree_iov *iovs, int niovs) 
{
	int   i, j;
	off_t e;

	for (i=0; i < niovs; i++, iovs++) {
		/* No empty iovs */		
		psc_assert(iovs->oftiov_nblks);
		
		if (i)
			psc_assert(iovs->oftiov_off == (e + 1));
		
		OFT_IOV2E_OFF(iovs, e);
		
		for (j=0; j < niovs; j++) {
			if (i == j) continue;
			//check for overlapping bases?
		}
	}
}

/**
 * offtree_newleaf - assign a new leaf node to the parent.
 * @parent: the parent pointer
 * @pos: position the child will occupy
 * Notes:  Assumes that the new leaf will be allocated to.
 * Notes:  Parent must be locked.
 */
static struct offtree_memb *
offtree_newleaf_locked(struct offtree_memb *parent, int pos)
{
	struct offtree_memb *new;
	
	psc_assert(!parent->oft_norl.oft_children[pos]);
	
	new = PSCALLOC(sizeof(struct offtree_memb));
	OFT_MEMB_INIT(new, parent);
	ATTR_SET(new->oft_flags, OFT_UNINIT);
	new->oft_pos = pos;
	parent->oft_norl.oft_children[pos] = new;
	atomic_inc(&parent->oft_ref);

	return new;
}

static void
offtree_leaf2node_prep(struct offtree_memb *oftm) {
	/* Leaf2node caller is responsible for saving 
	 *  the nodes' buffers.  Dereference the oft.
	 */
	oftm_leaf_verify(oftm);
	atomic_set(&oftm->oft_ref, 0);
	oftm->oft_norl.oft_iov = NULL;	       
	ATTR_SET(oftm->oft_flags, OFT_SPLITTING);
}


/**
 * offtree_leaf2node - transform a leaf into a parent node.
 * @oftm:  tree member to promote.
 * Notes: tree member must be locked.  Caller must manage OFT_SPLITTING which must be set anytime the refcnt is 0. See oftm_splitting_leaf_verify() macro for more details.
 */
static void
offtree_leaf2node_locked(struct offtree_memb *oftm, struct offtree_root *r)
{	
	DEBUG_OFT(PLL_TRACE, oftm, "promote");
	oftm_splitting_leaf_verify(oftm);
	oftm->oft_norl.oft_children = PSCALLOC(sizeof(struct offtree_memb **) * 
					       r->oftr_width);
	ATTR_UNSET(oftm->oft_flags, OFT_LEAF);
	ATTR_SET(oftm->oft_flags, OFT_NODE);
	ATTR_UNSET(oftm->oft_flags, OFT_SPLITTING);
	/* No need to wake up others, they would be blocked on 
	 *  the spinlock anyway.
	 */
	//psc_waitq_wakeup(&oftm->oft_waitq);
}


/**
 * offtree_node_reuse_locked - retain a tree node which is in the process of being released.  The releasing thread checks for state change and when detected will leave the node in place.
 * @m: the tree node. 
 */
static void
offtree_node_reuse_locked(struct offtree_memb *m)
{
	/* Reuse, don't release
	 */
	oftm_reuse_verify(m);
	m->oft_flags = (OFT_LEAF | OFT_ALLOCPNDG);
}

#define oft_child_free(__c, __p) {					\
		DEBUG_OFT(PLL_INFO, (__c), "releasing child .. ");	\
		DEBUG_OFT(PLL_INFO, (__p), ".. from parent");		\
		psc_assert((__p)->oft_norl.oft_children[(__c)->oft_pos]	\
			   == (__c));					\
		(__p)->oft_norl.oft_children[(__c)->oft_pos] = NULL;	\
		freelock(&(__c)->oft_lock);				\
		PSCFREE((__c));						\
	}

/**
 * offtree_node_release - free node which has a zero refcnt.  Handle cases: parent is the root, parent also needs to be released (recursive), and where another thread has requested to keep the node.
 * Notes: locking order (1-parent, 2-node)
 */
static void
offtree_node_release(struct offtree_memb *oftm)
{
	struct offtree_memb *parent = oftm->oft_parent;

	if (!parent || ATTR_TEST(oftm->oft_flags, OFT_ROOT)) {
		DEBUG_OFT(PLL_INFO, oftm, "i am the root!");
		psc_fatalx("Attempted to release the root");
	}

	oftm_node_verify(oftm);       
	spinlock(&parent->oft_lock);
	spinlock(&oftm->oft_lock);

	if (!ATTR_TEST(oftm->oft_flags, OFT_RELEASE)) {
		/* offtree_node_reuse_locked() got here, verify a few things
		 *  and then give up.
		 */		
		oftm_unrelease_verify(oftm);
		DEBUG_OFT(PLL_INFO, oftm, "was reclaimed quickly");	
		goto out;
	}
	
	oft_child_free(oftm, parent);

	if (atomic_dec_and_test(&parent->oft_ref)) {
		if (!parent->oft_parent) {
			/* Our parent is the root */
			oftm_root_verify(parent->oft_parent);
			goto out;
		}
		parent->oft_flags = OFT_RELEASE;
		freelock(&parent->oft_lock);
		offtree_node_release(parent); 
	} else {
	out:
		freelock(&oftm->oft_lock);
		freelock(&parent->oft_lock);
	}
}

void
offtree_freeleaf_locked(struct offtree_memb *oftm)
{
	struct offtree_memb *parent = oftm->oft_parent;
	int root=0;

	DEBUG_OFT(PLL_TRACE, oftm, "freeing");

	if (!parent) {
		oftm_root_verify(oftm);
		root = 1;
	} else
		oftm_node_verify(parent);	

	oftm_freeleaf_verify(oftm);

	if (root) {
		/* Reset the tree root (remove OFT_FREEING and OFT_LEAF flags)
		 */
		oftm->oft_flags = OFT_ROOT;
		freelock(&oftm->oft_lock);
	} else {
		oft_child_free(oftm, parent);
		/* Test parent for releasing
		 */
		if (atomic_dec_and_test(&parent->oft_ref)) {
			ATTR_SET(parent->oft_flags, OFT_RELEASE);
			freelock(&parent->oft_lock);
			offtree_node_release(parent);
		}
	}
}

/**
 * offtree_calc_nblks_hb_int - calculate number of blocks needed to statisfy request taking into account the existing mapped buffer 'v'.  Handles the case where 'v' is overlapping (or not) or encompassed.
 * @r: request
 * @v: existing buffer
 * @front: nblks needed ahead of 'v'.
 * @back:  nblks needed behind 'v'.
 * Returns: the total number of blocks needed.  *front and *back contain the number of blocks needed before and after the existing buffer.
 */
static size_t
offtree_calc_nblks_hb_int(const struct offtree_req *r, 
			  const struct offtree_iov *v, 
			  ssize_t *front, ssize_t *back)
{	
	size_t nblks = r->oftrq_nblks;
	off_t  nr_soffa, nr_eoffa, hb_soffa, hb_eoffa;

	psc_assert(v && front && back);
	psc_assert(r->oftrq_root->oftr_minsz == v->oftiov_blksz);

	*front = *back = 0;

	OFT_REQ2SE_OFFS(r, nr_soffa, nr_eoffa);
	OFT_IOV2SE_OFFS(v, hb_soffa, hb_eoffa);

	DEBUG_OFFTREQ(PLL_TRACE, r, "req eoffa="LPX64, nr_eoffa);
	DEBUG_OFFTIOV(PLL_TRACE, v, "hb  eoffa="LPX64, hb_eoffa);       
	/* Otherwise we shouldn't be here.
	 */
	psc_assert((nr_soffa < hb_soffa) || (nr_eoffa > hb_eoffa));
	
	if (nr_soffa < hb_soffa) {
		if (nr_eoffa < hb_soffa) {
			/* Regions do not overlap.
			 */
			*front = nblks;
			goto out;
		} else
			/* Frontal overlap.
			 */
			*front = (hb_soffa - nr_soffa) / 
				v->oftiov_blksz;
	}	
	if (nr_eoffa > hb_eoffa) {
		if (nr_soffa > hb_eoffa) {
			/* Regions do not overlap.
			 */
			*back = nblks;
			goto out;
		} else 
			*back = ((nr_eoffa+1) - (hb_eoffa+1)) / 
				v->oftiov_blksz;
	}
 out:	
	return (*back + *front);
}


/*
 * offtree_blks_get - allocate memory blocks for request 'req'.  Take into account an existing memory buffer and manage the allocation array.  On successful completion, the req->oftrq_darray will contain an array of iov's which may be used for I/O.  If the 'have_buffer' exists, then it will be placed at the correct logical location within the iov array.  Since our blocks are aligned this is possible.
 * @req: the allocation request (which also holds the array).
 * @hb_iov: if !NULL, represents the currently held buffer.
 * Returns 'nblks' on success.
 */
static ssize_t
offtree_blks_get(struct offtree_req *req, struct offtree_iov *hb_iov)
{
	ssize_t tblks=0, rc=0;
	int     oniovs=0;
	struct  offtree_root *r = req->oftrq_root;

	tblks = req->oftrq_nblks;

	DEBUG_OFFTREQ(PLL_TRACE, req, "req");	
	DEBUG_OFT(PLL_TRACE, req->oftrq_memb, "memb");
	psc_assert(ATTR_TEST(req->oftrq_memb->oft_flags, OFT_ALLOCPNDG));
	/* Determine nblks taking into account overlap 
	 */	
	if (!hb_iov) {		
		oniovs = dynarray_len(req->oftrq_darray);
		/* No existing buffer, make full allocation
		 */
		rc = (r->oftr_alloc)(tblks, req->oftrq_off, 
				     req->oftrq_darray, r);
		if (rc != tblks) {
			if (rc < tblks)
				goto done;
			else
				psc_fatalx("Wanted %zd, got %zd", 
					   tblks, rc);
		}		       
		psc_assert((dynarray_len(req->oftrq_darray) - oniovs) > 0);

	} else {
		ssize_t front=0, back=0;
		size_t nblks;
		/* The 'have_buffer' needs to be placed at the correct
		 *  offset in the oftrq_darray, so determine the number
		 *  of blocks which are required ahead-of and behind the 
		 *  have_buffer.  Add the iov's to the array in the 
		 *  correct order.
		 */
		nblks = offtree_calc_nblks_hb_int(req, hb_iov, &front, &back);
		psc_assert(front || back);

		ATTR_SET(hb_iov->oftiov_flags, OFTIOV_REMAPPING);

		DEBUG_OFFTIOV(PLL_TRACE, hb_iov, "hb f=%zd, b=%zd", 
			      front, back);
		/* Allocate 'front' blocks and add their iovs to the front
		 *  of the queue..
		 */		
		if (front) {
			oniovs = dynarray_len(req->oftrq_darray);

			rc = (r->oftr_alloc)(front, req->oftrq_off, 
					     req->oftrq_darray, r);
			if (rc != front) {
				if (rc < front) {
					psc_errorx("Wanted "LPX64" got "LPX64, 
						   front, rc);
					rc = -1;
					goto done;
				} else
					psc_fatalx("Wanted "LPX64" got "LPX64, 
						   front, rc);
			}
			psc_assert((dynarray_len(req->oftrq_darray) - 
				    oniovs) > 0);
		}
		/* Add the 'have_buffer' iov to the array here in the middle 
		 *  request that it is remapped by the slab manager.
		 */
		dynarray_add(req->oftrq_darray, hb_iov);
		
		if (back) {
			/* Push the iov offset to the beginning of the back
			 *   segment.
			 */
			//off_t toff=(req->oftrq_off +=
			off_t toff=(req->oftrq_off + 
				    (req->oftrq_nblks - back) * r->oftr_minsz);

			oniovs = dynarray_len(req->oftrq_darray);
			/* Allocate 'back' blocks.
			 */
			rc = (r->oftr_alloc)(back, toff, 
					     req->oftrq_darray, r);
			if (rc != back) {
				if (rc < back) { 
					psc_errorx("Wanted %zd got %zd", 
						   back, rc);
					rc = -1;
					goto done;
				} else
					psc_fatalx("Wanted %zd got %zd", 
						  back, rc);
			}		       
			psc_assert((dynarray_len(req->oftrq_darray) - 
				    oniovs) > 0);
		}
	}
 done:
	if (rc < 0) {
		DEBUG_OFFTREQ(PLL_ERROR, req, "error rc=%zd", rc);
		return (rc);
	}

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
 * offtree_putleaf - apply buffers to a leaf
 *
 offtree_putnode(req, iovoff, tiov_cnt, sblkoff);	
 * Notes:  don't modify source iov's directly.  Copy them using NEW_PARTIAL_IOV.
 *  Putnode doesn't have to discover hb's, they've already been accounted for in the iov array.
 */
static void
offtree_putnode(struct offtree_req *req, int iovoff, int iovcnt, int blkoff)
{
	struct offtree_iov *iov, *tiov;

	ENTRY;
	DEBUG_OFFTREQ(PLL_TRACE, req, "o:%d c:%d bo:%d",
		      iovoff, iovcnt, blkoff);

	psc_assert(req->oftrq_darray);
	psc_assert(req->oftrq_root);
	psc_assert(req->oftrq_memb);
	psc_assert(req->oftrq_depth < req->oftrq_root->oftr_maxdepth);
	psc_assert(iovcnt > 0);

	if (iovcnt == 1) {
		iov = dynarray_getpos(req->oftrq_darray, iovoff);

		psc_assert((u32)(iov->oftiov_nblks - blkoff) >= 
			   req->oftrq_nblks);

		/* Debugging info
		 */
		req->oftrq_memb->oft_width = req->oftrq_width;
		req->oftrq_memb->oft_depth = req->oftrq_depth;

		DEBUG_OFT(PLL_INFO, req->oftrq_memb, "placing buffer here");
		DEBUG_OFFTIOV(PLL_INFO, iov, "hb");
		/* Paranioa, verify that the refcnt is zero.
		 */
		psc_assert(!atomic_read(&req->oftrq_memb->oft_ref));
		/* Now bump it to 1.
		 */
		atomic_set(&req->oftrq_memb->oft_ref, 1);
		/* Notify the allocator if this is a remap.
		 */
		if (ATTR_TEST(iov->oftiov_flags, OFTIOV_MAPPED)) { 
			if (req->oftrq_nblks == iov->oftiov_nblks) {
				psc_assert(ATTR_TEST(iov->oftiov_flags, 
						     OFTIOV_REMAP_SRC));

				iov->oftiov_memb = req->oftrq_memb;
				req->oftrq_memb->oft_norl.oft_iov = iov;
				ATTR_SET(iov->oftiov_flags, OFTIOV_REMAP_END);
			} else {
				struct offtree_iov *niov;
				
				niov = PSCALLOC(sizeof(struct offtree_iov));
				ATTR_SET(niov->oftiov_flags, OFTIOV_REMAPPING);
				niov->oftiov_nblks = req->oftrq_nblks;
				niov->oftiov_off   = req->oftrq_off;
				niov->oftiov_pri   = iov->oftiov_pri;
				niov->oftiov_blksz = iov->oftiov_blksz;
				niov->oftiov_base  = iov->oftiov_base + 
					(req->oftrq_off - iov->oftiov_off);
				/* Ensure that the new partial iov doesn't overrun
				 *  the REMAP_SRC iov.
				 */
				psc_trace("blks=%zu reqeoff="LPX64" reqeoffa="LPX64
					  " ioveoff="LPX64" ioveoffa="LPX64, 
					  niov->oftiov_nblks,
					  (req->oftrq_off+(req->oftrq_nblks*iov->oftiov_blksz)), 
					  OFT_REQ2E_OFF_(req),
					  (iov->oftiov_off+(iov->oftiov_nblks * iov->oftiov_blksz)), 
					  OFT_IOV2E_OFF_(iov));

				psc_assert(OFT_REQ2E_OFF_(req) <= OFT_IOV2E_OFF_(iov));
				
				if (OFT_REQ2E_OFF_(req) == OFT_IOV2E_OFF_(iov)) 
					ATTR_SET(niov->oftiov_flags, OFTIOV_REMAP_END);

				niov->oftiov_memb = req->oftrq_memb;
				req->oftrq_memb->oft_norl.oft_iov = niov;
				DEBUG_OFFTIOV(PLL_INFO, niov, "remap (niov)");
			}
			DEBUG_OFFTIOV(PLL_INFO, iov,  "remapsrc (iov)");

		} else {
			if (req->oftrq_nblks < iov->oftiov_nblks) {
				struct offtree_iov *niov;
				niov = PSCALLOC(sizeof(*niov));
				/* Prep the new iov.
				 */
				ATTR_SET(niov->oftiov_flags, OFTIOV_REMAPPING);
				niov->oftiov_nblks = req->oftrq_nblks;
				niov->oftiov_off   = req->oftrq_off;
				niov->oftiov_pri   = iov->oftiov_pri;
				niov->oftiov_blksz = iov->oftiov_blksz;
				niov->oftiov_base  = iov->oftiov_base + 
					(req->oftrq_off - iov->oftiov_off);
				/* Ensure that the new partial iov doesn't overrun
				 *  the REMAP_SRC iov.
				 */
				psc_trace("blks=%zu reqeoff="LPX64" reqeoffa="LPX64
					  " ioveoff="LPX64" ioveoffa="LPX64, 
					  niov->oftiov_nblks,
					  (req->oftrq_off+(req->oftrq_nblks*iov->oftiov_blksz)), 
					  OFT_REQ2E_OFF_(req),
					  (iov->oftiov_off+(iov->oftiov_nblks * iov->oftiov_blksz)), 
					  OFT_IOV2E_OFF_(iov));

				psc_assert(OFT_REQ2E_OFF_(req) <= OFT_IOV2E_OFF_(iov));
				
				if (OFT_REQ2E_OFF_(req) == OFT_IOV2E_OFF_(iov)) 
					ATTR_SET(niov->oftiov_flags, OFTIOV_REMAP_END);

				req->oftrq_memb->oft_norl.oft_iov = niov;
				ATTR_SET(iov->oftiov_flags, OFTIOV_REMAP_SRC);
				DEBUG_OFFTIOV(PLL_INFO, niov, "short remap (niov)");
			} else {
				iov->oftiov_memb = req->oftrq_memb;
				req->oftrq_memb->oft_norl.oft_iov = iov;
			}
		}

		DEBUG_OFFTIOV(PLL_INFO, req->oftrq_memb->oft_norl.oft_iov, 
			      "final iov");

		if (req->oftrq_root->oftr_putnode_cb)
			(req->oftrq_root->oftr_putnode_cb)(req->oftrq_memb);

		ATTR_UNSET(req->oftrq_memb->oft_flags, OFT_ALLOCPNDG);
		
		goto out;
		
	} else {
		/* Can't directly map more than 1 iov so recursion is needed.
		 */
		struct offtree_req myreq;
		off_t  nr_soffa, nr_eoffa;
		off_t  rg_soff = OFT_REQ_STARTOFF(req);
		off_t  rg_eoff = OFT_REQ_ENDOFF(req);
		off_t  i_offa; /* offset iterator */
		int    tchild, schild, echild;
		int    j=0, b=0, tiov_cnt, t=0;
		int    nblks=req->oftrq_nblks;
		/* 
		 * ***Promote to parent node***
		 *   Manage creation of children and preservation 
		 *     of attached buffer (if any) - the messy case
		 *   This case isn't so bad because there are no nodes 
		 *    below us, only leafs.
		 */
		//req->oftrq_memb->oft_norl.oft_iov = NULL;
		//ATTR_SET(req->oftrq_memb->oft_flags, OFT_SPLITTING);
		offtree_leaf2node_prep(req->oftrq_memb);
		offtree_leaf2node_locked(req->oftrq_memb, req->oftrq_root);
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
		myreq.oftrq_depth++;
		myreq.oftrq_width = OFT_REQ_ABSWIDTH_GET(&myreq, schild);
		/* XXX this loop is essentially identical to the one in 
		 *   offtree_region_preprw_leaf_locked(), this whole
		 *   segment should be moved into a function.
		 */
		for (j=0, b=0, tiov_cnt=1, i_offa=nr_soffa, tchild=schild; 
		     tchild <= echild; 
		     j++, tchild++, myreq.oftrq_width++, tiov_cnt=1) {
			/* Region values increase with myreq.oftr_width
			 */
			//printf ("WIDTH = %hhu\n", myreq.oftrq_width);
			rg_soff = OFT_REQ_STARTOFF(&myreq);
			rg_eoff = OFT_REQ_ENDOFF(&myreq);		
			/* This should always be true 
			 */
			psc_trace("i_soffa="LPX64", rg_soff="LPX64, 
				  i_offa, rg_soff);
			psc_assert(i_offa >= rg_soff);

			if (tchild > schild)
				psc_assert(i_offa == rg_soff);
			/* How many blocks fit within this range?
			 *  Push offset iterator i_offa
			 */
			i_offa += myreq.oftrq_nblks = 
				(MIN(rg_eoff, nr_eoffa) + 1) - i_offa;

			t = (myreq.oftrq_nblks % OFT_REQ2BLKSZ(req));
			psc_assert(!t);

			myreq.oftrq_nblks /= OFT_REQ2BLKSZ(&myreq);  
			myreq.oftrq_memb   = offtree_newleaf_locked(req->oftrq_memb, 
								    tchild);
			oft_refcnt_inc(&myreq, myreq.oftrq_memb);
			ATTR_UNSET(myreq.oftrq_memb->oft_flags, OFT_UNINIT);
			ATTR_SET(myreq.oftrq_memb->oft_flags, OFT_ALLOCPNDG);

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
					       iovoff);
			DEBUG_OFFTIOV(PLL_TRACE, tiov, "sblkoff debug(0)");

			psc_trace("i_offa="LPX64" tiov->oftiov_off="LPX64
				  " nblks=%zd sblkoff=%d", 
                                  i_offa, tiov->oftiov_off + (blkoff * tiov->oftiov_blksz),
                                  myreq.oftrq_nblks, blkoff);

			psc_assert(tiov->oftiov_off + (blkoff * tiov->oftiov_blksz) 
                                   == myreq.oftrq_off);

			/* Factor in partially used iov's 
			 */
			b = (tiov->oftiov_nblks - blkoff);
			psc_assert(b > 0);

			while (b < myreq.oftrq_nblks) {
				struct offtree_iov *piov = tiov;
				tiov = dynarray_getpos(req->oftrq_darray, 
						       iovoff + tiov_cnt);
				DEBUG_OFFTIOV(PLL_TRACE, tiov, "sblkoff debug(%d)", tiov_cnt);
				tiov_cnt++; /* tmp count */
				psc_assert(tiov_cnt <= iovcnt);
				psc_assert(tiov->oftiov_off == OFT_IOV2E_OFF_(piov) + 1);
				b += tiov->oftiov_nblks;
			}
			offtree_putnode(&myreq, iovoff, tiov_cnt, blkoff);
			/* Bump iovoff, subtract one if the current
			 *   iov in underfilled.
			 */
			iovoff += tiov_cnt - ((b > myreq.oftrq_nblks) ? 1 : 0);
			/* At which block in the iov do we start? 
			 */
			b -= myreq.oftrq_nblks;
			if (b)
				blkoff = tiov->oftiov_nblks - b;
			else 
				blkoff = 0;
			/* This check fails when the iov requires mapping
			 *   across multiple tree nodes.
			 *  ./offtree_test -i 2 -B 9 
			 */
			//if (tchild == echild) 
			//	psc_assert(!nblks && !blkoff);

		}
	}	
 out:
	EXIT;
}

/*
 * offtree_region_preprw_leaf_locked - the stage 2 call into the tree, it does not recurse but rather handles the allocation step taking into account existing buffers. 
 */
static int
offtree_region_preprw_leaf_locked(struct offtree_req *req)
{
	ENTRY;
	struct  offtree_memb *m = req->oftrq_memb;
	struct  offtree_iov  *iov = NULL;	
	off_t   nr_soffa, nr_eoffa, hb_soffa=0, hb_eoffa=0;
	ssize_t nblks;
	int     niovs;

	psc_assert(ATTR_TEST(m->oft_flags, OFT_LEAF));

	if (req->oftrq_op == OFTREQ_OP_WRITE)
		psc_assert(atomic_read(&m->oft_wrop_ref) > 0);

	if (req->oftrq_op == OFTREQ_OP_READ)
		psc_assert(atomic_read(&m->oft_rdop_ref) > 0);

	DEBUG_OFFTREQ(PLL_INFO, req, "new req");
	DEBUG_OFT(PLL_INFO, m, "new req's memb");

	OFT_REQ2SE_OFFS(req, nr_soffa, nr_eoffa);

	if (!ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG)) {
		iov = m->oft_norl.oft_iov;
		/* Intent to allocate must be determined ahead of time.
		 */
		psc_assert(iov && iov->oftiov_base && iov->oftiov_nblks);
		/* Tell the underlying cache subsystem to pin this guy.
		 */
		(req->oftrq_root->oftr_slbpin_cb)(iov);
		/* Get the start and end offsets.
		 */
		OFT_IOV2SE_OFFS(iov, hb_soffa, hb_eoffa);

		DEBUG_OFFTIOV(PLL_INFO, iov, "hb");
		/* Check to see if the existing allocation can fulfill
		 *   this request.
		 */
		if ((nr_soffa >= hb_soffa) && (nr_eoffa <= hb_eoffa)) {
			DEBUG_OFFTREQ(PLL_TRACE, req, 
				      "req fulfilled by existing hb %p", iov);

			dynarray_add(req->oftrq_darray, iov);
			req->oftrq_darray_off++;
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
	nblks = offtree_blks_get(req, iov);
	if (nblks < 0) {
		DEBUG_OFFTREQ(PLL_ERROR, req, 
			      "offtree_blks_get() error nblks=%zd", nblks);
		ATTR_UNSET(m->oft_flags, OFT_ALLOCPNDG);
		goto error;
	}
	/* How many iovs did we get back?
	 */
	niovs = dynarray_len(req->oftrq_darray);
	psc_assert(niovs > 0);

	if ((niovs - req->oftrq_darray_off) == 1) {
		/* Should only have 1 new buffer and no exisiting buffers.
		 */
		psc_assert(!m->oft_norl.oft_iov);
		/* XXX Putnode does not use the m->oft_norl.oft_iov
		 *  pointer in this context, it calls dynarray_getpos().
		 *  Keep it for debugging.
		 */
		m->oft_norl.oft_iov = dynarray_getpos(req->oftrq_darray,
						      req->oftrq_darray_off);
		DEBUG_OFFTREQ(PLL_TRACE, req, 
			      "req fulfilled by a new buffer");
		DEBUG_OFFTIOV(PLL_INFO, m->oft_norl.oft_iov, "new hb");	  

		offtree_putnode(req, req->oftrq_darray_off, 1, 0);
		ATTR_UNSET(m->oft_flags, OFT_ALLOCPNDG);
		psc_waitq_wakeall(&m->oft_waitq);
		goto done;

	} else {
		struct offtree_req myreq;
		off_t  rg_soff = OFT_REQ_STARTOFF(req);
		off_t  rg_eoff = OFT_REQ_ENDOFF(req);
		off_t  crg_eoff, crg_soff;
		off_t  i_offa;
		int    tchild, schild, echild, tiov_cnt;
		int    j=0, iovoff=0, sblkoff=0;
		/* 
		 * ***Promote to parent node***
		 *   Manage creation of children and preservation 
		 *     of attached buffer (if any) - the messy case
		 *   This case isn't so bad because there are no nodes 
		 *    below us, only leafs.
		 */
		offtree_leaf2node_prep(m);
		offtree_leaf2node_locked(m, req->oftrq_root);
		
		DEBUG_OFT(PLL_TRACE, m, "promote to node (niovs=%d)", niovs);
		/* Capture the new starting offset, it may be different
		 *   if the existing buffer's offset was < that of the 
		 *   new request.  offtree_blks_get() handed back a sorted
		 *   iov array so the first item has the lowest offset.
		 * This segment is not accessed recursively so there is no need
		 *   for a blkoff into the iov.
		 */
		iov = dynarray_getpos(req->oftrq_darray, req->oftrq_darray_off);
		DEBUG_OFFTIOV(PLL_TRACE, iov, "iov 0 (doff=%zu)", 
			      req->oftrq_darray_off);
		nr_soffa = i_offa = iov->oftiov_off;
		
		iov = dynarray_getpos(req->oftrq_darray, (niovs-1));
		DEBUG_OFFTIOV(PLL_TRACE, iov, "iov n");
		nr_eoffa = (iov->oftiov_off + (iov->oftiov_blksz *
					       iov->oftiov_nblks)) - 1;
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
		for (j=0, tchild=schild, tiov_cnt=1; 
		     tchild <= echild; 
		     j++, tchild++, tiov_cnt=1) {
			int b=0, t;
			struct offtree_iov  *tiov;
			/* Set the s and e offsets for our children.
			 */
			crg_soff = OFT_STARTOFF(req->oftrq_root, 
						(req->oftrq_depth+1), 
						((req->oftrq_width * 
						  req->oftrq_root->oftr_width) + tchild));
			
			crg_eoff = OFT_ENDOFF(req->oftrq_root,
					      (req->oftrq_depth+1),
					      ((req->oftrq_width * 
						req->oftrq_root->oftr_width) + tchild));
			
			/* Middle child sanity (middle children must
			 *   be completely used).
			 */
			if (tchild > schild)
				psc_assert(i_offa == crg_soff);
			
			memcpy(&myreq, req, (sizeof(*req)));
			/* Set the req offset to the iterative offset value.
			 */
			myreq.oftrq_off = i_offa;
			/* This should always be true.
			 */
			psc_assert_msg(myreq.oftrq_off >= crg_soff && 
				       myreq.oftrq_off <  crg_eoff, 
				       "myreq.oftrq_off="LPX64" crg_soff="LPX64
				       " crg_eoff="LPX64, 
				       myreq.oftrq_off, crg_soff, crg_eoff);

			/* Manage the depth and width of the request.
			 */
			myreq.oftrq_depth++;
			myreq.oftrq_width = (req->oftrq_width *
					     req->oftrq_root->oftr_width) + tchild;
			/* How many blocks fit within this range?
			 *  Push offset iterator i_offa. 
			 *  Verify that the offset is aligned.
			 */ 
			myreq.oftrq_nblks = (MIN(crg_eoff, nr_eoffa) + 1) - i_offa;
			t = myreq.oftrq_nblks % OFT_REQ2BLKSZ(req);
			psc_assert(!t);
			/* Compute the actual number of blks.
			 */
			myreq.oftrq_nblks /= OFT_REQ2BLKSZ(req);

			/* How many iovs are needed to fill the child? 
			 *  Inspect our array of iov's. 
			 *  @iovoff: is the 'persisent' iterator
			 *  @tiov_cnt: used to inform the child of how many
			 *    iov's are present.
			 */
			tiov = dynarray_getpos(req->oftrq_darray,
					       req->oftrq_darray_off + iovoff);

			psc_trace("i_offa="LPX64" crg_soff="LPX64 
				  " tiov->oftiov_off="LPX64" nblks=%zd sblkoff=%d", 
				  i_offa, crg_soff, tiov->oftiov_off + (sblkoff * tiov->oftiov_blksz),
				  myreq.oftrq_nblks, sblkoff);

			/* Verify that the iov popped from the dynarray
			 *   has the expected offset value.  Here this should always be true. 
			 */
			psc_assert(tiov->oftiov_off + (sblkoff * tiov->oftiov_blksz) 
				   == i_offa);

			ATTR_SET(tiov->oftiov_flags, OFTIOV_REMAP_SRC);
			DEBUG_OFFTIOV(PLL_TRACE, tiov, "sblkoff=%d debug(0)", sblkoff);

			/* Factor in partially used iov's.
			 */			
			b = (tiov->oftiov_nblks - sblkoff);
			psc_assert(b > 0);
			/* 
			 */
			for (t=0; (b < myreq.oftrq_nblks) && (t < niovs); t++) {
				struct offtree_iov *piov = tiov;			       
				tiov = dynarray_getpos(req->oftrq_darray, 
						       req->oftrq_darray_off + iovoff + tiov_cnt);
				DEBUG_OFFTIOV(PLL_TRACE, tiov, "sblkoff debug(%d)", tiov_cnt);
				psc_assert((iovoff + tiov_cnt) < niovs);
				psc_assert(tiov->oftiov_off == OFT_IOV2E_OFF_(piov) + 1);
				tiov_cnt++;
				b += tiov->oftiov_nblks;
			}
			if (b < myreq.oftrq_nblks) {
				psc_errorx("accumulated blocks (%d) < "
					   "myreq.oftrq_nblks (%zu)", 
					   b, myreq.oftrq_nblks);
				goto error;
			}
				
			psc_trace("myreq.oftrq_nblks=%zd b=%d, tiov->oftiov_nblks=%zu tiov_cnt=%d", 
				  myreq.oftrq_nblks, b, tiov->oftiov_nblks, tiov_cnt);
			/* Make the child...
			 */
			myreq.oftrq_memb = offtree_newleaf_locked(m, tchild);
			oft_refcnt_inc(&myreq, myreq.oftrq_memb);

			ATTR_UNSET(myreq.oftrq_memb->oft_flags, OFT_UNINIT);
			ATTR_SET(myreq.oftrq_memb->oft_flags, OFT_ALLOCPNDG);
			/* Take the soffa from the first iov returned
			 *  by the allocator.
			 */
			//myreq.oftrq_off    = nr_soffa;
			nblks -= myreq.oftrq_nblks;

			offtree_putnode(&myreq, iovoff + req->oftrq_darray_off, 
					tiov_cnt, sblkoff);
			/* Bump iovoff. 
			 */
			iovoff += tiov_cnt - ((b > myreq.oftrq_nblks) ? 1 : 0);
			/* At which block in the iov do we start? 
			 */
			b -= myreq.oftrq_nblks;
			if (b)
				sblkoff = tiov->oftiov_nblks - b;
			else 
				sblkoff = 0;

			if (tchild == echild)				
				/* Ensure that all blocks have been exhausted.
				 */ 
				psc_assert(!sblkoff);
			else 
				/* Bump iterator for the next time through.
				 */
				i_offa += (myreq.oftrq_nblks * OFT_REQ2BLKSZ(req));
		}
	}
 done:	
	RETURN (0);
 error:
	RETURN (-1);
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
	ENTRY;
	struct offtree_memb *m = req->oftrq_memb;
	off_t  nr_soffa, nr_eoffa;
	int    scnt=0, rc;

	psc_assert(req->oftrq_darray);
	psc_assert(req->oftrq_root);
	psc_assert(req->oftrq_memb);	

 	OFT_REQ2SE_OFFS(req, nr_soffa, nr_eoffa);

 wakeup_retry:

	DEBUG_OFFTREQ(PLL_TRACE, req, "eoff="LPX64" scnt=%d",
		      nr_eoffa, scnt);	
	OFT_VERIFY_REQ_SE(req, nr_soffa, nr_eoffa);

	spinlock(&m->oft_lock);

	DEBUG_OFT(PLL_TRACE, m, "member");
	/* Verify tree node state
	 */
	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		oftm_leaf_verify(m);
		if (ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG)) {
			/* Block for completion of allocation 
			 */
			DEBUG_OFT(PLL_WARN, m, 
				  "block on OFT_ALLOCPNDG req:%p", req);
			/* Lock release
			 */
			psc_waitq_wait(&m->oft_waitq, &m->oft_lock);
			scnt++;
			goto wakeup_retry;
		}
		/* Root leaf does not get OFT_ALLOCPNDG by default.
		 */
		if (ATTR_TEST(m->oft_flags, OFT_ROOT) && !m->oft_norl.oft_iov)
			ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);

		if (ATTR_TEST(m->oft_flags, OFT_UNINIT)) {
			/* The leaf is now initialized.
			 */
			ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);
			ATTR_UNSET(m->oft_flags, OFT_UNINIT);
		} else 
			/* Newly allocated leafs have op_ref 
			 *   already set to 1.
			 */
			oft_refcnt_inc(req, m);
	runleaf:
		rc = offtree_region_preprw_leaf_locked(req);
		/* Free the memb lock and return.
		 */
		if (rc && !req->oftrq_memb->oft_norl.oft_iov) {
			oft_refcnt_dec(req, m);
			ATTR_SET(req->oftrq_memb->oft_flags, OFT_FREEING);
			offtree_freeleaf_locked(req->oftrq_memb);
		} else 
			freelock(&m->oft_lock);
		return(rc);

	} else if (m->oft_flags == OFT_RELEASE) {
		offtree_node_reuse_locked(m);
		goto runleaf;

	} else if (ATTR_TEST(m->oft_flags, OFT_NODE)) {
		int schild, echild;

		oftm_node_verify(m);
		if (ATTR_TEST(m->oft_flags, OFT_MCHLDGROW)) {
			DEBUG_OFT(PLL_WARN, m, 
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

			new = m->oft_norl.oft_children[schild];		
			if (!new) 
				new = offtree_newleaf_locked(m, schild);

			oft_refcnt_inc(req, new);

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
			/* Requested range straddles multiple children so 
			 *  my node (m) is the request root.  Format individual
			 *  requests for all affected children.  The darray 
			 *  pointer contained in req should remain intact and 
			 *  contain the iovecs on completion.
			 */
			struct offtree_req myreq;
			size_t nblks = req->oftrq_nblks;
			int    tchild, tdepth=0;

			psc_assert(echild > schild);
			DEBUG_OFFTREQ(PLL_TRACE, req, "req spans multiple children");
		
			ATTR_SET(m->oft_flags, OFT_MCHLDGROW);
			/* Maybe this will save some CPU, waiters will sleep 
			 *  on the waitq.
			 */
			freelock(&m->oft_lock);
			memcpy(&myreq, req, (sizeof(*req)));

			myreq.oftrq_depth++;
			for (tchild=schild, tdepth=myreq.oftrq_depth; 
			     tchild <= echild; tchild++, myreq.oftrq_depth=tdepth) {
				myreq.oftrq_width = OFT_REQ_ABSWIDTH_GET(req, tchild);
				myreq.oftrq_off   = MAX(OFT_REQ_STARTOFF(&myreq), req->oftrq_off);
				myreq.oftrq_nblks = MIN(OFT_REQ_REGIONBLKS(&myreq), nblks);
				myreq.oftrq_memb  = m->oft_norl.oft_children[tchild];
				nblks            -= myreq.oftrq_nblks;
				myreq.oftrq_darray_off = dynarray_len(req->oftrq_darray);	
			     
				if (myreq.oftrq_memb) {
					DEBUG_OFFTREQ(PLL_TRACE, &myreq, "existing child");
					spinlock(&myreq.oftrq_memb->oft_lock);
					if (ATTR_TEST(myreq.oftrq_memb->oft_flags, OFT_LEAF)) {
						rc = offtree_region_preprw_leaf_locked(&myreq);	
						freelock(&myreq.oftrq_memb->oft_lock);
						if (rc < 0)
							RETURN (rc);
					} else {
						freelock(&myreq.oftrq_memb->oft_lock);
						psc_trace("recurse into offtree_region_preprw()");
						rc = offtree_region_preprw(&myreq);
						if (rc < 0)
							RETURN (rc);
					}
				} else {
					myreq.oftrq_memb = offtree_newleaf_locked(m, tchild);
					DEBUG_OFFTREQ(PLL_TRACE, &myreq, "new child");
					oft_refcnt_inc(&myreq, myreq.oftrq_memb);
					spinlock(&myreq.oftrq_memb->oft_lock);
					ATTR_SET(myreq.oftrq_memb->oft_flags, OFT_ALLOCPNDG);
					ATTR_UNSET(myreq.oftrq_memb->oft_flags, OFT_UNINIT);
					rc = offtree_region_preprw_leaf_locked(&myreq);
					freelock(&myreq.oftrq_memb->oft_lock);
					if (rc < 0)
						RETURN (rc);
				}
			}
			ATTR_UNSET(m->oft_flags, OFT_MCHLDGROW);
			/* I'm pretty sure this is the right behavior...
			 */
			psc_waitq_wakeall(&m->oft_waitq);
		}
	} else
		psc_fatalx("Invalid offtree node state %d", m->oft_flags);

	RETURN (0);
}
