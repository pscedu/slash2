#include "offtree.h"
#include "psc_util/alloc.h"

static void
offtree_node2leaf(struct offtree_memb *oftm)
{


}

/**
 * offtree_leaf2node - must be called locked.
 *
 */
static void
offtree_leaf2node(struct offtree_memb *oftm)
{	
	ATTR_SET(m->oft_flags, OFT_SPLITTING);	
	m->norl.oft_children = PSC_ALLOC(sizeof(struct offtree_memb **) * r->oftr_width);	
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
	/* Only leaves have pages */
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
offtree_calc_nblks(struct offtree_req *r, struct offtree_iov *v)
{
	size_t nblks;
	off_t  o = req->oftrq_floff;
	size_t l = req->oftrq_fllen;	
	size_t m = req->oftrq_root->oftr_minsz;

	/* Align our offset with the minimum size, move
	 *   the length back to the aligned offset
	 */
	o -= (o % m);
	l += (o % m);

	nblks = (l / m) + ((l % m) ? 1:0);

	if (!v)
		return (nblks);
	
	/* Calculate the overlap */
	else {
		/* Align the soff to block boundary */
		off_t  soff  = v->oftiov_floff - (v->oftiov_floff % m);
		off_t  eoff  = v->oftiov_nblks * m;
		size_t tblks = 0;

		/* Otherwise we shouldn't be here */
		psc_assert((o < soff) || (l > eoff));
		
		if (o < soff) {
			/* Check my math */
			psc_assert(!((soff - o) % m));
			tblks = (soff - o) / m;
		}
		
		if (l > eoff)
			tblks += ((l - eoff) / m) + ((l % eoff) ? 1:0);
		
		psc_assert(tblks < nblks);
		nblks = tblks;
	}
	return (nblks);
}

void
offtree_putchild()
{


}

void
offtree_newchild(struct offtree_memb *parent, int pos, off_t off, int nblks,
		 int d, int absw, struct offtree_iov *src_iov)
{
	struct offtree_memb *new;

	psc_assert(!parent->norl.oft_children[pos]);
	psc_assert(src_iov);

	new = PSC_ALLOC(sizeof(struct offtree_memb));
	OFT_MEMB_INIT(new);
	ATTR_SET(new->oft_flags, OFT_ALLOCPNDG);

	parent->norl.oft_children[pos] = new;
	atomic_inc(&parent->oft_ref);
	
	if (!src_iov->oftiov_fllen) {
		/* Caller applies file logical attrs afterwards */
		psc_assert(!src_iov->oftiov_floff);
		sl_oftiov_addref(src_iov);
		
	} else {
		struct offtree_iov *new_iov = PSC_ALLOC(sizeof(*new_iov));

		sl_oftiov_addref(new_iov, src_iov, sblk, .nblks);
	}
}
		 

/**
 * offtree_requestroot_get - return the tree node which is the uppermost node capable of handing the given request.
 * @req: offtree request which contains root and member pointers.  
 * @d: tree depth 
 * @w: 'global' width (or horizontal position)
 * Notes: returns the offtree_memb which is the head of the request.  The offtree_memb is tagged with OFT_REQPNDG so that it will not be freed.
 */
int 
offtree_region_preprw(struct offtree_req *req, int d, int w)
{
	struct offtree_root  *r = req->oftrq_root;
	struct offtree_memb  *m = req->oftrq_memb;
	struct offtree_iov   *iov = NULL;
	struct offtree_memb **c;
	off_t  o = req->oftrq_floff;
	size_t l = req->oftrq_fllen;
	size_t rc;
	size_t minsz = req->oftrq_root->oftr_minsz;
	off_t  hb_soffa=0, hb_eoff=0, rg_soff, rg_eoff, nr_soffa, nr_eoffa;	
	int    schild, echild, absw;
	size_t tblks = 0;

	DEBUG_OFFTREQ(PLL_TRACE, req, 
		      "depth=%d width=%d, soff=%"ZLPX64" eoff=%"ZLPX64,
		      d, w, OFT_STARTOFF(r, d, w), OFT_ENDOFF(r, d, w));
       
	psc_assert(OFT_REGIONSZ(r, d) >= (o + l));
	/* Compute aligned offsets for have_buffer and 
	 *  the acutal request.
	 */			
	nr_soffa = o - (o % minsz);
	nr_eoffa = ((o + l) + ((o + l) % minsz));
	
	spinlock(&m->oft_lock);
	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		int    have_buffer=0;
		size_t nblks;
		size_t niovs=0, iovcnt=0;
		struct offtree_iov *miovs;

		psc_assert((o >= OFT_STARTOFF(r, d, w)) &&
			   ((o + l) <= OFT_ENDOFF(r, d, w))); 

		/* XXX not sure if both ALLOCPNDG and oft_ref are needed.. */
		/* fence the uninitialized leaf */
		ATTR_SET(m->oft_flags, OFT_REQPNDG);

		iov = m->norl.oft_iov;
		if (iov) {
			/* Do some simple sanity checks on the iov */
			psc_assert((iov->oftiov_base && iov->oftiov_nblks) && 
				   (iov->oftiov_fllen > 0));

			/* Our previous iov must be remapped properly, 
			 *  align the offset.
			 */
			hb_soffa = iov->oftiov_floff - (iov->oftiov_floff % minsz);
			hb_eoffa = iov->oftiov_nblks * minsz;

			/* Already have the requested offset in the buffer */
			if ((o >= iov->oftiov_floff) &&
			    (l <= iov->oftiov_fllen)) {
				//no use keeping refcnts here because 
				//  io can be ongoing during split operations
				//  and we'd have no way to push the ref
				//  to the correct child node :(
				// The owning slab will have to be pinned!
				//atomic_inc(&m->oft_ref);
				goto done;
			} 
		}
		/* Block others from allocating into this node / leaf
		 *  until we are done.
		 */
		ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);		
		/* Determine nblks taking into account overlap */
		nblks = offtree_calc_nblks(req, (iov ? iov : NULL));
		/* Allocate blocks */
		rc = (r->oftr_alloc)(nblks, &miovs, &niovs, r);
		if (rc != nblks) {
			if (rc < nblks)
				goto error;
			else
				psc_warnx("Wanted "LPX64" got "LPX64, 
					  nblks, rc);
		}
		psc_assert(niovs);

		if ((niovs == 1) && !have_buffer) {			
			/* Only 1 new buffer and no previous buffers to 
			 *  deal with.  Store the iov pointer, base 
			 *  and nblks are already filled, oftrq_darray
			 *  was populated by alloc().
			 */
			m->norl.oft_iov = miovs;
			/* Save the file logical info   */
			m->norl.oft_iov->oftiov_floff = req->oftrq_floff;
			m->norl.oft_iov->oftiov_fllen = req->oftrq_fllen;
			DEBUG_OFT(PLL_INFO, m, "Assigning single buffer %p",
				  miovs);
			goto done;
		}
		/* Else .. (niovs > 1 || have_buffer)
		 * Manage creation of children and preservation 
		 *   of attached buffer (if any) - the messy case
		 */
		/* Promote to parent node */
		offtree_leaf2node(m);		
		/* Determine affected children */
		schild = oft_schild_get(o, r, d, w);
		echild = oft_echild_get(o, l, r, d, w);
		psc_assert((schild >= 0) && (echild >= 0));		
		/* Iterate over affected subregions, alloc'ing leaf nodes
		 *  and placing buffers.
		 */
		while (schild <= echild) {
			absw = ((w * r->oftr_width) + schild);
			
			rg_soff = OFT_STARTOFF(r, d+1, absw);
			rg_eoff = OFT_ENDOFF(r, d+1, absw);

			psc_assert(nr_soffa <= rg_soff);			

			while (((rg_eoff +1) > rg_soff) &&
			       ((nr_eoffa+1) > nr_soffa)) {
				/* Deal with the 'have_buffer'? */
				if (((hb_eoffa - hb_soffa) > 0) && 
				    (hb_soffa <= nr_soffa)) {
					psc_assert(!(((hb_eoffa+1) - hb_soffa) % minsz));
					psc_assert(!(((rg_eoff+1)  - hb_soffa) % minsz));
					psc_assert(hb_soffa >= rg_soff);
					/* How many blocks are needed for this leaf */
					int blks = MIN((((hb_eoffa+1) - hb_soffa) / minsz),  
						       (((rg_eoff +1) - hb_soffa) / minsz));

					/* Remember the iov from above?  Take a bite from it*/
					if (!m->norl.oft_children[schild])
						offtree_newchild(m, schild, hb_soffa, blks, d+1, absw, iov);
					else
						offtree_putchild(m, schild, hb_soffa, blks, d+1, absw, iov);

					/* Increment new_req & region start offsets */
					nr_soffa += blks * minsz;
					psc_assert(nr_soffa <= (nr_eoffa+1));

					rg_soff  += blks * minsz;
					psc_assert(rg_soff <= (rg_eoff+1));
				} else {
					
					
				}
			}
			
			schild++;
		}
		
	} else if (ATTR_TEST(m->oft_flags, OFT_NODE)) {
		/* am I the root or is it one of my children? */
		int schild = oft_schild_get(o, r, d, w);
		int echild = oft_echild_get(o, l, r, d, w);
		
		if (schild == echild) {
			if (!m->norl.oft_children[schild]) {
				/* allocate a child */
				struct offtree_memb *tmemb;

				tmemb = PSCALLOC(sizeof(*tmemb));
				OFT_MEMB_INIT(tmemb);
				/* fence the uninitialized leaf */
				ATTR_SET(tmemb->oftm_flags, OFT_ALLOCPNDG);
				m->norl.oft_children[schild] = tmemb;
			}
			/* increment for each child reference */
			atomic_inc(&m->oft_ref);
			freelock(&m->oft_lock);
			/* request can be handled by one tree node */
			req->oftrq_memb = m->norl.oft_children[schild];

			return (offtree_region_preprw(req, d+1, ((w * r->oftr_width) + schild));
			
		} else {
			struct offtree_req   myreq;
			struct offtree_memb *tmemb;

			memcpy(&myreq, req, (sizeof(*req)));

			psc_assert(echild > schild);
			/* the requested range straddles multiple children so 
			 *  this node (m) is the request root 
			 */
			while (schild <= echild) {
				if (m->norl.oft_children[schild]) {
					tmemb = m->norl.oft_children[schild];
					spinlock(&tmemb->oft_lock);
					ATTR_SET(tmemb->oftm_flags, OFT_REQPNDG);
					continue;
				}
				tmemb = PSCALLOC(sizeof(*tmemb));
				OFT_MEMB_INIT(tmemb);
				/* increment for each child reference */
				atomic_inc(&m->oft_ref);
				ATTR_SET(tmemb->oftm_flags, OFT_ALLOCPNDG);
				m->norl.oft_children[schild] = tmemb;
				
				offtree_region_preprw(req, d+1, ((w * r->oftr_width) + schild));
			}
			goto done;
		}
		
	} else 
		psc_fatalx("Invalid offtree node state %d", m->oft_flags);
	
 done:
		// The owning slabs will have to be pinned!
		ATTR_SET(m->oft_flags, OFT_REQPNDG);
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
