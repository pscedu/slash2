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
		psc_assert(iovs[i]->oftiov_len);		
		/* Ensure that floffs are increasing and non-overlapping */
		if (iovs[i]->oftiov_floff)
			psc_assert(iovs[i]->oftiov_floff >=
				   (prevfloff + prevlen));
		
		prevfloff = iovs[i]->oftiov_floff;
		prevlen   = iovs[i]->oftiov_len;	
	}
}


static void
offtree_preprw_internal(struct offtree_root *r, 
			struct offtree_memb *m, 
			struct dynarray     *a, 
			struct offtree_iov  *v, 
			u16 d, u16 w, u8 rw) 
{
#if 0
	size_t sblk  = t->oftiov_floff / r->oftr_minsz;	
	size_t tlen  = v->oftiov_len - (v->oftiov_floff % r->oftr_minsz);
	size_t nblks = 1 + (tlen / r->oftr_minsz) +
		((tlen % r->oftr_minsz) ? 1 : 0);
#endif	
	/* check this node first */
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
		spinlock(&m->oft_lock);
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


		
		if (v->oftiov_floff >= OFT_STARTOFF(r, d, w) &&
		    v->oftiov_floff <= OFT_STARTOFF(r, d, w) &&
		    
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
