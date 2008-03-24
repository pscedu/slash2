#include "offtree.h"
#include "psc_util/alloc.h"


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

/**
 * offtree_leaf2node - transform a leaf into a parent node.
 * @oftm:  tree member to promote.
 * Notes: tree member must be locked.  Caller must manage OFT_SPLITTING which must be set anytime the refcnt is 0. See oftm_splitting_leaf_verify() macro for more details.
 */
static void
offtree_leaf2node_locked(struct offtree_memb *oftm, struct offtree_root *r)
{	
	oftm_splitting_leaf_verify(oftm);
	oftm->norl.oft_children = PSC_ALLOC(sizeof(struct offtree_memb **) * 
					    r->oftr_width);
	ATTR_UNSET(oftm->oft_flags, OFT_LEAF);
	ATTR_SET(oftm->oft_flags, OFT_NODE);
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
		psc_assert((__p)->norl.oft_children[(__c)->oft_pos]	\
			   == (__c));					\
		(__p)->norl.oft_children[(__c)->oft_pos] = NULL;	\
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

	if (!parent || ATTR_TEST(oftm, OFT_ROOT)) {
		DEBUG_OFT(PLL_INFO, oftm, "i am the root!");
		psc_fatalx("Attempted to release the root");
	}

	oftm_node_verify(m);       
	spinlock(&parent->oft_lock);
	spinlock(&oftm->oft_lock);

	if (!ATTR_TEST(oftm->oft_flags, OFT_RELEASE)) {
		/* offtree_node_reuse_locked() got here, verify a few things
		 *  and then give up.
		 */		
		oftm_unrelease_verify(m);
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
		freelock(&parent->oft_ref);
		offtree_node_release(parent); 
	} else {
	out:
		freelock(&oftm->oft_lock);
		freelock(&parent->oft_lock);
	}
}

static void
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
			freelock(&parent->oft_ref);
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
 * Returns 'nblks' on success.
 */
static ssize_t
offtree_blks_get(struct offtree_req *req, const struct offtree_iov *hb_iov)
{
	ssize_t tblks=0, rc=0;
	size_t  niovs=0;
	struct  offtree_iov  *miovs, 
	struct  offtree_root *r = req->oftrq_root;

	tblks = req->oftrq_nblks;

	DEBUG_OFFTREQ(PLL_TRACE, req, "req");	
	
	/* Determine nblks taking into account overlap 
	 */
	if (!hb_iov) {		
		/* No existing buffer, make full allocation
		 */
		rc = (r->oftr_alloc)(tblks, &miovs, &niovs, r);
		if (rc != nblks) {
			if (rc < tblks)
				goto done;
			else
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
		DEBUG_OFFTIOV(PLL_TRACE, hb_iov, "hb");

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
			/* Use the existing reference.
			 */
			req->oftr_oftm->norl.oft_iov = iov;
			psc_assert(!iovoff && !blkoff);
		} else {
			/* Only modref if the request doesn't use the 
			 *   entire iov otherwise the existing reference
			 *   will suffice.  sl_oftiov_modref handles
			 *   assertions.
			 */
			NEW_PARTIAL_IOV(tiov, iov, blkoff, req->oftrq_nblks);
			req->oftr_oftm->norl.oft_iov = tiov
		}

		sl_oftm_addref(req->oftr_oftm);			
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
		myreq.oftr_depth++;
		myreq.oftr_width = OFT_REQ_ABSWIDTH_GET(&myreq, schild);

		for (j=0, b=0, i_offa=nr_soffa, tchild=schild; 
		     tchild <= echild; 
		     j++, tchild++, myreq.oftr_width++) {
			/* Region values increase with myreq.oftr_width
			 */
			rg_soff = OFT_REQ_STARTOFF(&myreq);
			rg_eoff = OFT_REQ_ENDOFF(&myreq);		
			/* This should always be true 
			 */
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
 * offtree_region_preprw_leaf_locked - the stage 2 call into the tree, it does not recurse but rather handles the allocation step taking into account existing buffers. 
 */
static int
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
		/* Check to see if the existing allocation can fulfill
		 *   this request.
		 */
		if ((nr_soffa >= hb_soffa) && (nr_eoffa <= hb_eoffa)) {
			DEBUG_OFFTREQ(PLL_TRACE, req, 
				      "req fulfilled by existing hb %p", iov);

			dynarray_add(req->oftrq_darray, iov);
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
	DEBUG_OFFTREQ(PLL_TRACE, req, "eoff=%"ZLPX64" scnt=%d",
		      nr_eoffa, scnt);	

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

			new = m->norl.oft_children[schild];		
			if (!new) 
				new = offtree_newleaf(m, schild);
x			
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
						psc_trace("recurse into offtree_region_preprw()");
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
			ATTR_UNSET(m->oft_flags, OFT_MCHLDGROW);
			/* I'm pretty sure this is the right behavior...
			 */
			psc_waitq_wakeall(&m->oftm_waitq);
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
