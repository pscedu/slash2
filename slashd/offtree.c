#include "offtree.h"
#include "psc_util/alloc.h"

static void
offtree_node2leaf(struct offtree_memb *oftm)
{


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
	
	return (t);
}

void
offtree_freeleaf(struct offtree_memb *oftm)
{
	spinlock(&oftm->oft_lock);
	/* Only leafs have pages */
	psc_assert(ATTR_TEST(oftm->oft_flags, OFT_LEAF));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_NODE));
	/* Allocate pages first, mark oftm second
	 *  otherwise the oftm will think is owns pages
	 *  which are in fact being reclaimed..
	 *  the pages' slb must have been pinned before 
	 *  the oftm can claim OFT_WRITEPNDG || READPNDG
	 */
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_READPNDG));
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_WRITEPNDG));
	/* This state would mean that we're freeing pages 
	 *  that do not exist here.. surely this is bad.
	 */
	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_ALLOCPNDG));
	
	freelock(&oftm->oft_lock);
}

static void
offtree_iovs_check(struct offtree_iov *iovs, int niovs) 
{
	int i;
	off_t  prevfloff = 0;
	size_t prevlen   = 0;

	for (i=0; i < niovs; i++) { 	
		/* No empty iovs */
		psc_assert(iovs[i]->oftiov_nblks);		
		/* Ensure that floffs are increasing and non-overlapping */
		if (iovs[i]->oftiov_floff)
			psc_assert(iovs[i]->oftiov_floff >=
				   (prevfloff + prevlen));
		
		prevfloff = iovs[i]->oftiov_floff;
		//prevlen   = iovs[i]->oftiov_nblks * //XXX FIXME;	
	}
}

static size_t
offtree_calc_nblks_int(struct offtree_req *r)
{
	off_t  nr_soffa = req->oftrq_floff;
	size_t l = req->oftrq_fllen;	
	size_t m = req->oftrq_root->oftr_minsz;
	/* Align our offset with the minimum size, move
	 *   the length back to the aligned offset
	 */
	nr_soffa -= (nr_soffa % m);
	l += (nr_soffa % m);

	return((l / m) + ((l % m) ? 1:0));
}

static size_t
offtree_calc_nblks_hb_int(struct offtree_req *r, struct offtree_iov *v, 
			  size_t *front, size_t *back)
{
	size_t nblks=0,tblks=0;
	off_t  nr_soffa=req->oftrq_floff, nr_eoffa, hb_soffa, hb_eoffa;
	size_t l = req->oftrq_fllen;	
	size_t m = req->oftrq_root->oftr_minsz;

	psc_assert(v && front && back);

	*front = *back = 0;

	/* Align our offset with the minimum size, move
	 *   the length back to the aligned offset
	 */
	nr_soffa -= (nr_soffa % m);
	l += (nr_soffa % m);

	nblks = (l / m) + ((l % m) ? 1:0);

	/* Calculate overlap (there may be none) */
	/* Align the soff to block boundary */
	hb_soffa = v->oftiov_floff - (v->oftiov_floff % m);
	hb_eoffa = (hb_soffa + (v->oftiov_nblks * m) - 1);
	nr_eoffa = (o + (nblks * m) - 1);
	
	/* Sanity check */
	psc_assert(!(hb_soffa % m) && !(hb_eoffa % m));
	/* Otherwise we shouldn't be here */
	psc_assert((nr_soffa < hb_soffa) || (nr_eoffa > hb_eoffa));
	/* Unlikely, catch off-by-one */
	psc_assert((nr_eoffa != hb_soffa) && (nr_soffa != hb_eoffa));
	
	if (nr_soffa < hb_soffa) {
		/* Check my math - probably not needed */
		psc_assert(!((hb_soffa - nr_soffa) % m));
		
		if (nr_eoffa < hb_soffa) {
			/* Regions do not overlap */
			*front = nblks;
			goto out;
		} else
			/* Frontal overlap, also cover the case where
			 *   the have_buffer is completely enveloped.
			 */
			*front = (nr_soffa - hb_soffa) % m;

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
	struct  offtree_iov  *moivs, 
	struct  offtree_root *r = req->oftrq_root;

	tblks = offtree_calc_nblks_int(req);

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
		size_t front, back, nblks;
		/* The 'have_buffer' needs to be placed at the correct
		 *  offset in the oftrq_darray, so determine the number
		 *  of blocks which are required ahead-of and behind the 
		 *  have_buffer.  Add the iov's to the array in the 
		 *  correct order.
		 */
		nblks = offtree_calc_nblks_hb_int(req, iov, &front, &back);
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
					psc_warnx("Wanted "LPX64" got "LPX64, 
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
					psc_warnx("Wanted "LPX64" got "LPX64, 
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

#define NEW_PARTIAL_IOV(o, n, off, nblks)				\
	do {								\
		(n) = PSCALLOC(sizeof(*tiov));				\
		(n)->oftiov_base  = (o)->oftiov_base +			\
			(off * (o)->oftiov_blksz);			\
		(n)->oftiov_blksz = (o)->oftiov_blksz;			\
		(n)->oftiov_pri   = (o)->oftiov_pri;			\
		(n)->oftiov_nblks = nblks;				\
	} while (0)		       
/*

offtree_newchild(m, nchldblks, 
                 req->oftrq_darray, 
                 iovoff, tiov_cnt, sblkoff);
*/
/*
 * offtree_putleaf - apply buffers to a leafh
 *
 */
void
offtree_putnode(struct offtree_memb *child, int depth, size_t nblks, 
		struct dynarray *bufs, int iovoff, int iovcnt, int blkoff)
{
	struct offtree_iov *iov, *tiov;
	int i=0, fblks=0;

	for (i=0; i < iovcnt; i++) {
		iov = dynarray_getpos(bufs, (iovoff + i));
		if (!i && iovoff)
			/* First time, handle iovoff */
			NEW_PARTIAL_IOV(tiov, iov, iovoff, 
					(iov->oftiov_nblks - iovoff));
		
		else if ((i == (iovcnt - 1)) && 
			   (fblks + iov->oftiov_nblks != nblks)) {
			/* The last iteration must complete the request */
			psc_assert((fblks + iov->oftiov_nblks) >= nblks);
			NEW_PARTIAL_IOV(tiov, iov, 0, (nblks - fblks));	
		} else
			tiov = iov;

		fblks += tiov->oftiov_nblks;		
	}
	psc_assert(fblks == nblks);
	
	sl_oftiov_modref(tiov, iov);
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
	ATTR_SET(new->oft_flags, OFT_ALLOCPNDG);
	
	parent->norl.oft_children[pos] = new;
	atomic_inc(&parent->oft_ref);

	return new;
}

/*
 * offtree_region_preprw_leaf_locked - the final allocation call into the tree, it may recurse if the allocation is fragmented or there was an existing buffer that needs to be remapped (or both).  What's currently here needs to be modified  so that the allocation is handled by the caller not in here.
 *
 */
int
offtree_region_preprw_leaf_locked(struct offtree_req *req, int d, int w)
{
	struct offtree_root  *r = req->oftrq_root;
	struct offtree_memb  *m = req->oftrq_memb;
	struct offtree_iov   *iov = NULL;
	off_t  rg_soff  = OFT_STARTOFF(r, d, w);
	off_t  rg_eoff  = OFT_ENDOFF(r, d, w);
	off_t  nr_soffa = req->oftrq_off;
	off_t  nr_eoffa = (nr_soffa + (req->oftrq_nblks * minsz)) - 1;
	off_t  i_offa;
	int    j=0, iovoff=0, sblk=0;
	size_t tblks, nblks, front, back, niovs=0;
	struct offtree_iov *miovs;

	int tchild, schild, echild;

	psc_assert(m->norl.oft_iov);
	iov = m->norl.oft_iov;
	psc_assert(iov->oftiov_base && iov->oftiov_nblks);	
	psc_assert(ATTR_TEST(m->oft_flags, OFT_LEAF));
	psc_assert(ATTR_TEST(m->oft_flags, OFT_REQPNDG));
	/*
	 * Existing buffers must be preserved through 
	 *  our offspring. For now, no empty leaf nodes!
	 */
	hb_soffa = iov->oftiov_off;
	hb_eoffa = hb_soffa + (iov->oftiov_nblks * minsz);

	DEBUB_OFFTREQ(PLL_INFO, req, "w:%d d:%d", w, d);
	DEBUG_OFFTIOV(PLL_INFO, iov, "hb");
	
	if ((nr_soffa >= hb_soffa) && (nr_eoffa <= hb_eoffa)) {
		DEBUG_OFFTREQ(PLL_TRACE, req, 
			      "req fulfilled by existing hb %p", iov);
		goto done;
	}
	/* Block others from allocating into this node / leaf
	 *  until we are done.
	 */
	ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);	
	/* Allocate the blocks taking into accout a currently 
	 *   held buffer (have_buffer) in 'iov'.  
	 */
	if ((tblks = offtree_blks_get(req, iov)) < 0)
		goto error;
	/* How many iovs did we get back?
	 */
	niovs = dynarray_len(req->oftrq_darray);
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
	schild = oft_child_get(nr_soffa, r, d, w);
	echild = oft_child_get(nr_eoffa, r, d, w);
	
	psc_assert((schild >= 0) && (echild >= 0) && (schild <= echild));
	/* 
	 * Iterate over affected subregions, alloc'ing leaf nodes
	 *  and placing buffers.
	 *
	 * How many blocks go to each child?  Middle childred must
	 *   be complete.
	 */
	for (j=0, i_offa=nr_soffa, tchild=schild; tchild <= echild; 
	     j++, tchild++) {
		int tiov_cnt=1, b=0, nchldblks=0;
		struct offtree_iov *tiov;
		
		absw = ((w * r->oftr_width) + schild);
		
		rg_soff = OFT_STARTOFF(r, d+1, absw);
		rg_eoff = OFT_ENDOFF(r, d+1, absw);		
		/* This should always be true */
		psc_assert(nr_soffa <= rg_soff); 	
		/* Middle child sanity (middle children must
		 *   be completely used).
		 */
		if (j > schild)
			psc_assert(i_offa == rg_soff);
		/* How many blocks fit within this range?
		 *  Push offset iterator i_offa
		 */
		i_offa += nchldblks = (MIN(rg_eoff, nr_eoffa) + 1) - i_offa;
		
		psc_assert(!(nchldblks % minsz));
		/* More middle child sanity (middle children must
		 *  consume their entire region).
		 */
		if (j < echild)
			psc_assert((i_offa - 1) == rg_eoff);
		/* Ok, now we know how many blks (tmp) must be granted 
		 *  to this child.
		 */
		nchldblks /= minsz;			
		
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
		while (b < nchldblks) {
			tiov_cnt++;
			tiov = dynarray_getpos(req->oftrq_darray, 
					       (iovoff + tiov_cnt));
			b += tiov->oftiov_nblks;
		}
		/* Make the child... */
		offtree_newleaf(m, tchild);
		/*
		  nchldblks, 
		  req->oftrq_darray, 
		  iovoff, tiov_cnt, sblkoff);
		*/
		/* Bump iovoff */
		iovoff += tiov_cnt - ((b > nchldblks) ? 1 : 0);
		/* At which block in the iov do we start? */
		if (b)
			sblkoff = tiov->oftiov_nblks - (b - nchldblks);
		else 
			sblkoff = 0;
		
		if (tchild == echild)
			psc_assert(!sblkoff);
	}
 done:
}

/**
 * offtree_region_preprw - return the tree node which is the uppermost node capable of handing the given request.  The request range must fit into address space covered by this region of the tree.
 * @req: offtree request which contains root and member pointers.  
 * @d: tree depth 
 * @w: 'global' width (or horizontal position)
 * Notes: returns the offtree_memb which is the head of the request.  The offtree_memb is tagged with OFT_REQPNDG so that it will not be freed.  Request offset must be aligned.
 */
int 
offtree_region_preprw(struct offtree_req *req, int d, int w)
{
	struct offtree_root  *r = req->oftrq_root;
	struct offtree_memb  *m = req->oftrq_memb;
	struct offtree_iov   *iov = NULL;
	struct offtree_memb **c;
	size_t rc, minsz=req->oftrq_root->oftr_minsz, tblks=0;
	int    schild, echild, tchild, absw, j;

	off_t  rg_soff  = OFT_STARTOFF(r, d, w);
	off_t  rg_eoff  = OFT_ENDOFF(r, d, w);
	off_t  nr_soffa = req->oftrq_off;
	off_t  nr_eoffa = (nr_soffa + (req->oftrq_nblks * minsz)) - 1;

	psc_assert(!(nr_soffa % minsz));
	psc_assert(!((nr_eoffa + 1) % minsz));

	DEBUG_OFFTREQ(PLL_TRACE, req, 
		      "depth=%d width=%d, soff=%"ZLPX64" eoff=%"ZLPX64,
		      d, w, OFT_STARTOFF(r, d, w), OFT_ENDOFF(r, d, w));
	/* Verify this request is for me?
	 */
	psc_assert((nr_soffa >= rg_soff) && (nr_eoffa <= rg_eoff));
	
	spinlock(&m->oft_lock);

	if (!(ATTR_TEST(m->oft_flags, OFT_LEAF) ||
	      ATTR_TEST(m->oft_flags, OFT_NODE)))
		psc_fatalx("Invalid offtree node state %d", m->oft_flags);

	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		ATTR_SET(m->oft_flags, OFT_REQPNDG);	
		offtree_region_preprw_leaf_locked(req, d, w);		
	} 
	
	//else if (ATTR_TEST(m->oft_flags, OFT_NODE)) {
	/* am I the root or is it one of my children? */
	schild = oft_schild_get(o, r, d, w);
	echild = oft_echild_get(o, l, r, d, w);
	
	if (schild == echild) {
		struct offtree_memb *new = m->norl.oft_children[schild];
		
		if (!new) 
			/* guaranteed to execute the "if OFT_LEAF"
			 *  block in the next recursion 
			 */
			new = offtree_newleaf(m, schild);
		
		req->oftrq_memb = new;
		
		// XXX THIS may need to be done..if so other things will 
		//  have to change above.
		//spinlock(new->oft_lock);
		freelock(&m->oft_lock);
		/* request can be handled by one tree node so recurse. */
		return (offtree_region_preprw(req, d+1, 
					      ((w * r->oftr_width) + schild)));
			
	} else {
		struct offtree_req   myreq;
		struct offtree_memb *tmemb;
		
		memcpy(&myreq, req, (sizeof(*req)));
		/* the requested range straddles multiple children so 
		 *  my node (m) is the request root 
		 */
		psc_assert(echild > schild);
		while (schild <= echild) {
			/* Ensure all necessary children have been allocated
			 */
			if (!(tmemb = m->norl.oft_children[schild])) {
				tmemb = PSCALLOC(sizeof(*tmemb));
				OFT_MEMB_INIT(tmemb);
				/* increment for each child reference */
				atomic_inc(&m->oft_ref);
				//ATTR_SET(tmemb->oftm_flags, OFT_ALLOCPNDG);
				m->norl.oft_children[schild] = tmemb;
			}
			//spinlock(&tmemb->oft_lock);
			ATTR_SET(tmemb->oftm_flags, OFT_REQPNDG);	
			offtree_region_preprw(req, d+1, ((w * r->oftr_width) + schild));
		}
		goto done;
	}		

	
 done:
	// The owning slabs will have to be pinned!
	ATTR_UNSET(m->oft_flags, OFT_REQPNDG);
	freelock(&m->oft_lock);
	req->oftrq_depth = d;
	req->oftrq_width = w;
	return (m);
 error:
	/* do the right node mgmt here.. */
	return (-ENOMEM);
}

struct offtree_req *
offtree_region_alloc(struct offtree_root *r, off_t o, size_t l)
{
	struct offtree_req *req = PSC_ALLOC(sizeof(*req));
	struct dynarray    a;
	
	req->oftrq_root  = r;
	req->oftrq_memb  = &r->oftr_memb;
	req->oftrq_floff = o;
	req->oftrq_fllen = l;
	req->oftrq_darry = &a;
	
	req->oftrq_memb  = offtree_requestroot_get(&req, 0, 0);
	
}

/**
 * offtree_preprw_internal - allocate buffer pages to the appropriate location(s) in the offtree.  
 * @v: region which needs to be backed with buffers (floff and fllen)
 * @m: the tree node in question
 * @r: the head/root of the tree
 * @a: array structure to hold the ordered queue of tree leaves which envelope the requested region.
 * @d: tree depth
 * @w: tree width - d and w are used to track location during recursive processing
 * @rw: operation type (read or write)
 * Notes: First order of operation is to find the tree node which corresponds to the floffset/len pair in 'v'. 
 */
static void
offtree_preprw_internal(const struct offtree_iov  *v, 
			struct offtree_memb *m, 
			const struct offtree_root *r, 
			struct dynarray *a,
			u16 d, u16 w, u8 rw) 
{
  
      size_t rlen = v->oftiov_len;
#if 0
      size_t sblk  = t->oftiov_floff / r->oftr_minsz;	
      size_t tlen  = v->oftiov_len - (v->oftiov_floff % r->oftr_minsz);
      size_t nblks = 1 + (tlen / r->oftr_minsz) +
	((tlen % r->oftr_minsz) ? 1 : 0);
#endif	
  
      /* ensure the request is within the range of this node */
      // XXX how will request iov's be normalized? assume it's before
      //  we get into here..

       
      // goto the correct depth in the tree, this depends on the 
      //  length and the offset of the request.   Offsets which span nodes
      //   must be accommodated.
      //if (OFT_REGIONSZ(r, d+1) > (v->oftiov_fllen)
      /* take the lock, then perform node checks */
      spinlock(&m->oft_lock);
      psc_assert(!((ATTR_TEST(m->oft_flags, OFT_LEAF)) &&
		   (ATTR_TEST(m->oft_flags, OFT_NODE))));
      
      if (ATTR_TEST(m->oft_flags, OFT_NODE)) {
		/* We are a parent node (therefore have no data pointers)
		 *  Go to the correct child pointer (the pointer array must
		 *  have been allocated).
		 */
		u16 tmpw = (OFT_REGIONSZ(r, d) / v->oftiov_floff);

		psc_assert(m->norl.oft_children);
		/* Lock the tree member while checking its child pointers
		 */
		if (!m->norl.oft_children[tmpw]) {
			/* allocate a child */
			struct offtree_memb *tmemb = PSCALLOC(sizeof(*tmemb));

			OFT_MEMB_INIT(tmemb);
			ATTR_SET(tmemb->oft_flags, OFT_LEAF);

			/* don't let others touch this yet */
			ATTR_SET(tmemb->oft_flags, OFT_ALLOCPNDG);
			atomic_inc(&tmemb->oft_ref);
			m->norl.oft_children[tmpw] = tmemb;
		}
		freelock(&m->oft_lock);

		offtree_preprw_internal(r, m->norl.oft_children[tmpw], 
					a, v, d+1, tmpw, rw);
	} else {
		/* Leaf node, must have a data pointer and some memory */
		
		/* if we're at maxdepth then we have to have an allocation */

		/* x = allocate(n)  where n is either iov_len or OFT_ENDOFF (which ever is smaller.  If x==n then N stays a leaf otherwise its allocation goes to a (newly created) child node and N becomes a Parent. 
		 */

		/* 
		 * Handle a newly created tree node, at this point it is a 
		 *  leaf but it may turn into a parent depending on the outcome
		 *  of the allocation.
		 */
		  if (ATTR_TEST(m->oft_flags, OFT_ALLOCPNDG)) {
			int    i, x=-1, niovs=0;
			struct offtree_iovec *iovs = NULL;

			/* have to round up for block alignment */
			size_t nblks = ((v->oftiov_len / r->oftr_minsz) + 
					((v->oftiov_floff % r->oftr_minsz) ? 1: 0));
			
			/* find the lesser of nblks or REGIONSZ */
			nblks = MIN(nblks, OFT_REGIONBLKS(r, d));
			
			if (d == r->oftr_maxdepth)
				/* only 1 block allowed at lowest depth */
				psc_assert(nblks == 1);

			/* allocate nblks */
			x = (r->oftr_alloc)(nblks, &iovs, &niovs, (void *)r);
			
			if (x > 0)
				psc_assert(niovs);
			else {
				// handle < case
			}

			for (i=0; i < niovs; i++) {
				psc_assert(iovs[i]);
				psc_assert(!(iovs[i].iov_len % r->oftr_minsz));
			}

			if (d == r->oftr_maxdepth)
				/* only 1 block allowed at lowest depth */
				psc_assert(niovs == x == 1);

			if (x == nblks) {
				if (niovs == 1) {
					/* got a single contiguous region, remain a leaf */
					m->norl.oft_iov = iovs[0];
					spinlock(&m->oft_lock);
					ATTR_UNSET(OFT_ALLOCPNDG, m->oft_flags);
					if (rw == OFT_READOP) {
						// prepare for read sink
						ATTR_SET(OFT_READPNDG, m->oft_flags);
					} else 
						// prepare for data copy
					  ATTR_SET(OFT_WRITEPNDG, m->oft_flags);	
					
					/* invalidate the file logical region w/ -1 */
					/* done for us in malloc */
					//m->norl.oft_iov.oftiov_fllen = -1;
					freelock(&m->oft_lock);
				}				
			}
		  }
		  
	return;
}


/*
 * offtree_pin - prepare regions for I/O (read or write).  This could involve performing allocations (r->oftr_alloc) or fetching pages for read.
 */
void
offtree_preprw(struct offtree_root *r, struct offtree_iov *iovs, 
	       int niovs, int op)
{
	int    i;
	struct dynarray oft_membs;
	struct offtree_iov *myiovs, tmpiov;
	//size_t sblk, nblks, tmp, tlen;

	offtree_check_iovs(iovs, niovs);

	dynarray_init(&oft_membs);

	for (i=0; i < niovs; i++) {
		memcpy(&tmpiov, iovs[i], sizeof(struct offtree_iov)); 
		offtree_preprw_internal(r, &r->oftr_memb, sblk, nblks);
	}
}

/*
 * offtree_insert - copy the regions described by *iovs into their respective offtree members handling sl_buffer management along the way. 
 *   This should overwirte whatever blocks are already cached..
 */
void
offtree_insert(struct offtree_root *r, struct offtree_iov *iovs, int niovs)
{
	int    i;
	size_t nblks;
	size_t tlen;
	struct offtree_iov *myiovs;

	for (i=0, nblks=0; i < niovs; i++) { 

		offtree_check_iovs(iovs, niovs);

		if (iovs[i]->oftiov_floff % r->oftr_minsz) {
			/* The floff is in mid-block, account for underflow */
			tlen -= (iovs[i]->oftiov_floff % r->oftr_minsz);
			nblks++;
		}
		if (tlen) {
			/* There's more. */
			nblks += (int)(iovs[i]->oftiov_len / r->oftr_minsz);
			if (iovs[i]->oftiov_len % r->oftr_minsz)
				/* anything trailing at the end? */
				nblks++;
		}			
	}

	/* Do the allocation - this operation must manage the allocation and 
	 *  refcnt'ing of the sl_buffers.  The sl_buffer pointers are returned
	 *  in the offtree_iov->pri pointer.
	 */
	psc_assert((r->oftr_alloc)(tsz, r->oftr_pri, &myiovs, &i));
	psc_assert(i > 0);
	
	
	return;
}
