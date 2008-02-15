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
[	psc_assert(!ATTR_TEST(oftm->oft_flags, OFT_WRITEPNDG));
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

	/* Align our offset with the minimum size */
	o -= (o % m);
	l += (o % m);

	nblks = (l / m) + ((l % m) ? 1:0)

	if (!v)
		return (nblks);
	
	/* Calculate the overlap */
	else {
		/* Here's the 'physical' address of region start */
		off_t  soff  = v->oftiov_floff - (v->oftiov_floff % m);
		off_t  eoff  = v->oftiov_nblks * m;
		size_t tblks = 0;

		/* Otherwise we shouldn't be here */
		psc_assert((o < soff) || (l > eoff));
		
		if (o < soff) {
			/* Check my math */
			psc_assert(!(soff - o) % m);
			tblks = (soff - o) / m;
		}
		
		if (l > eoff)
			tblks += ((l - eoff) / m) + ((l % eoff) ? 1:0);
		
		psc_assert(tblks < nblks);
		nblks = tblks;
	}

	return (nblks);
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

	DEBUG_OFFTREQ(PLL_TRACE, req, 
		      "depth=%d width=%d, soff=%"ZLPX64" eoff=%"ZLPX64,
		      d, w, OFT_STARTOFF(r, d, w), OFT_ENDOFF(r, d, w));
       
	psc_assert(OFT_REGIONSZ(r, d) >= req->oftrq_fllen + req->oftrq_floff);
	
	spinlock(&m->oft_lock);
	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		int    have_buffer=0;
		size_t nblks;
		size_t niovs=0;
		struct offtree_iov *miovs;

		psc_assert((o >= OFT_STARTOFF(r, d, w)) &&
			   (o + l >= OFT_ENDOFF(r, d, w))); 
		/* XXX not sure if both ALLOCPNDG and oft_ref are needed.. */
		/* fence the uninitialized leaf */
		ATTR_SET(m->oft_flags, OFT_REQPNDG);
		iov = m->norl.oft_iov;

		if (iov) {
			/* do some simple sanity checks on the iov */
			psc_assert((iov->oftiov_base && iov->oftiov_nblks) && 
				   (iov->oftiov_fllen > 0));

			have_buffer=1;

			/* Already have the requested offset in the buffer */
			if ((o >= iov->oftiov_floff) &&
			    (l <= iov->oftiov_fllen)) {
				//no use keeping refcnts here because 
				//  io can be ongoing during split operations
				//  and we'd have no way to push the ref
				//  to the correct child node :(
				//atomic_inc(&m->oft_ref);
				goto done;
			} 
			c = m->norl.oft_children;    
		}
		/* hmm if have_buffer==1 then we must block other's 
		 *  from allocating into this node / leaf
		 */
		ATTR_SET(m->oft_flags, OFT_ALLOCPNDG);
		nblks = offtree_calc_nblks(req, (iov ? iov : NULL));

		if ((r->oftr_alloc)(nblks, &miovs, &niovs, r) != nblks)
			goto error;

		if ((niovs + have_buffer) > 1) {
			/* Manage creation of children and preservation 
			 * of attached buffer (if any) 
			 */
			ATTR_SET(m->oft_flags, OFT_SPLITTING);
			
			c = PSC_ALLOC(sizeof(struct offtree_memb **) * 
				      r->oftr_width);
			
			ATTR_UNSET(m->oft_flags, OFT_LEAF);
			ATTR_SET(m->oft_flags, OFT_NODE);
			/* Based on the iov's len and width, create the 
			 *  necessary children recursively if needed 
			 */
			// make children here..
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

			return (offtree_region_prep(req, d+1, 
						    (w * r->oftr_width) + schild));
			
		} else {
			struct offtree_memb *tmemb;
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
			}
			goto done;
		}
		
	} else 
		psc_fatalx("Invalid offtree node state %d", m->oft_flags);
	
 done:
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
