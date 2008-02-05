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

static inline void
offtree_iovs_check(struct offtree_iov *iovs, int niovs) {
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

static inline void
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
	if (ATTR_TEST(m->oft_flags, OFT_LEAF)) {
		if (v->oftiov_floff
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

