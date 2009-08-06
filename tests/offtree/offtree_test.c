#include <sys/param.h>
#include <sys/uio.h>

#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

#include "buffer.h"
#include "offtree.h"
#include "fidcache.h"

const char *progname;

size_t mapSize     = (1024*1024*128);
size_t nblksPerReq = 1;
size_t iterScheme  = 0;
size_t overLapBlks = 1;
size_t iterations  = 2;
size_t treeDepth   = 5;
size_t treeWidth   = 8;
off_t  soffa=0;

__dead void
usage(void)
{
        fprintf(stderr, "usage: %s [-b slCacheBlkSz] [-d slbFreeDef]"
		"[-m slbFreeMax] [-n slCacheNblks] [-z mapSize (MB)]"
		"[-i iterScheme] [-B nblksPerReq]\n",
		progname);
        exit(1);
}

void
mygetopt(int argc, char **argv)
{
	char c;
	while ((c = getopt(argc, argv, "b:d:m:n:z:hB:I:i:W:D:O:")) != -1) 
		switch (c) {
		case 'b':
			slCacheBlkSz = atoi(optarg);
			break;
		case 'd':
			slbFreeDef = atoi(optarg);
			break;
		case 'm':
			slbFreeMax = atoi(optarg);
			break;
		case 'n':
			slCacheNblks = atoi(optarg);
			break;
		case 'z':
			mapSize = atoi(optarg);
			mapSize *= 1048576;
			break;
		case 'B':
			nblksPerReq = atoi(optarg);
			break;
		case 'I':
			iterScheme = atoi(optarg);
			break;
		case 'O':
			overLapBlks = atoi(optarg);
			break;
		case 'i':
			iterations = atoi(optarg);
			break;
		case 's':
			soffa = atoi(optarg);
                        break;			
		case 'W':
			treeWidth = atoi(optarg);
			break;
		case 'D':
			treeDepth = atoi(optarg);
			break;
		case 'h':
		default:
			usage();
		}
}

off_t
calc_offset(off_t o)
{
	switch (iterScheme) {
	case 0: /* Full range increment */
		o += (nblksPerReq * slCacheBlkSz);
		break;
	case 1: /* Partial range increment */
		o += ((nblksPerReq - overLapBlks) * slCacheBlkSz);
		break;
	}
	
	fprintf(stderr, "\tCalcOffset "LPX64"\n", o);

	return o;
}

int
process_blk(void *b, off_t o, int rw)
{
	size_t i, nwords = slCacheBlkSz / sizeof(off_t);
	off_t  bnum      = o / slCacheBlkSz;
	off_t *p = (off_t *)b;

	for (i=0; i < nwords; i++) {
#if 0
		if (!rw)
			fprintf(stderr, "R p[%zu]=%p bnum=%llu cbs=%zu o=%zu\n", 
				i, &p[i], bnum, slCacheBlkSz, o);

		else 
			fprintf(stderr, "W p[%zu]=%p bnum=%llu\n", 
				i, &p[i], bnum);
#endif
		if (rw)
			p[i] = bnum;
		else {
			if (p[i] != bnum) {
				fprintf(stderr, "Err bnum="LPX64" but p[%zu]\n", 
					bnum, p[i]);
				return 1;
			}
		}
	}
	return 0;
}


int
process_blks(struct offtree_req *req, int rw)
{
	struct dynarray    *a=req->oftrq_darray;
	struct offtree_iov *v;
	unsigned int i, j, n;
	off_t   o=req->oftrq_off;
	size_t  niovs=dynarray_len(a);
	ssize_t nbufs=0;
	int     rc=0;
		
	DEBUG_OFFTREQ(PLL_WARN, req, "");

	for (i=0; i < niovs; i++, n=0) { 
		v = dynarray_getpos(a, i);
		DEBUG_OFFTIOV(PLL_WARN, v, "iov%d rq_off=%zu OFT_IOV2E_OFF_(%zu)", 
			      i, req->oftrq_off,  OFT_IOV2E_OFF_(v));
		/* The dynarray may contain iov's that we don't need.
		 */
		if (o > OFT_IOV2E_OFF_(v))
			continue;

		for (j=0; j < v->oftiov_nblks; j++) {
			if (o > (v->oftiov_off + (j*slCacheBlkSz)))
				continue;
			else {
				if (OFT_REQ_ENDOFF(req) < (v->oftiov_off + (j*slCacheBlkSz)))
					goto out;
				else {
					fprintf(stderr, "iov=%p buf=%p voff="LPX64" roff="LPX64
						" v->oftiov_base=%p v->oftiov_nblks=%zu\n", 
						v, (void *)((v->oftiov_base + (j*slCacheBlkSz))), 
						(off_t)(v->oftiov_off + (j*slCacheBlkSz)),
						(off_t)(o + (nbufs*slCacheBlkSz)),
						v->oftiov_base, v->oftiov_nblks);
					
					rc += process_blk((void *)((v->oftiov_base + (j*slCacheBlkSz))), 
							  (off_t)(o + (nbufs*slCacheBlkSz)), rw);
					
					nbufs++;
				}
			}
		}
	}
 out:
	if (nbufs != req->oftrq_nblks) {
		fprintf(stderr, "nbufs=%zu != oftrq_nblks=%zu\n", 
			nbufs, req->oftrq_nblks);
		abort();
	}
	return rc;
}

int main(int argc, char **argv)
{
	struct fidc_memb fcm;
	struct offtree_root  *oftr;
	struct offtree_req    req;
	unsigned int          i;
	slfid_t fid;
	int      rc=0;

	memset(&fcm, 0, sizeof(fcm));
	memset(&req, 0, sizeof(req));

	mygetopt(argc, argv);
	
	sl_buffer_cache_init();

	fidc_memb_init(&fcm, &fid);	

	oftr = offtree_create(mapSize, slCacheBlkSz, 
			      treeWidth, treeDepth, &fcm, 
			      sl_buffer_alloc, sl_oftm_addref, sl_oftiov_pin_cb);

	req.oftrq_darray = PSCALLOC(sizeof(struct dynarray));

	for (req.oftrq_off=soffa, i=0; i < iterations; i++) {	
	
		fprintf(stderr, "Offset "LPX64"\n", req.oftrq_off);

		req.oftrq_root   = oftr;
		req.oftrq_memb   = &oftr->oftr_memb;
		req.oftrq_nblks  = nblksPerReq;	
		req.oftrq_width  = req.oftrq_depth = 0;
		req.oftrq_op     = OFTREQ_OP_READ;
		dynarray_init(req.oftrq_darray);
		memset(req.oftrq_darray, 0, sizeof(struct dynarray));
		offtree_region_preprw(&req);
		rc = process_blks(&req, 1);
		if (rc)
			abort();
		req.oftrq_off    = calc_offset(req.oftrq_off);
	}

	for (req.oftrq_off=soffa, i=0; i < iterations; i++) {
		fprintf(stderr, "Read Offset "LPX64"\n", req.oftrq_off);
		req.oftrq_root   = oftr;
                req.oftrq_memb   = &oftr->oftr_memb;
                req.oftrq_nblks  = nblksPerReq;
                req.oftrq_width  = req.oftrq_depth = 0;
		req.oftrq_op     = OFTREQ_OP_READ;
                dynarray_init(req.oftrq_darray);
                memset(req.oftrq_darray, 0, sizeof(struct dynarray));
                offtree_region_preprw(&req);
                rc = process_blks(&req, 0);
		if (rc)
			abort();
                req.oftrq_off    = calc_offset(req.oftrq_off);
        }

	return (0);
};

#if 0
__static void
msl_oftrq_build(struct offtree_req *r, struct bmap_cache_memb *b, 
		u64 cfd, off_t off, size_t len, int op)
{
	/* Ensure the offset fits within the range and mask off the
	 *  lower bits to align with the offtree's page size.
	 */
	psc_assert((off + len) <= SLASH_BMAP_SIZE);
	psc_assert(op == OFTREQ_OP_WRITE || 
		   op == OFTREQ_OP_READ);

	r->oftrq_darray = PSCALLOC(sizeof(struct dynarray));
	r->oftrq_root   = &b->bcm_oftree;
	r->oftrq_memb   = &b->bcm_oftree.oftr_memb;
	r->oftrq_cfd    = cfd;
	r->oftrq_width  = r->oftrq_depth = 0;
	r->oftrq_off    = off & SLASH_BMAP_BLKMASK;
	r->oftrq_op     = op;
	r->oftrq_off    = off & SLASH_BMAP_BLKMASK;
	/* Add the bits which were masked above.
	 */
	len += off & (~SLASH_BMAP_BLKMASK);

	//r->oftrq_nblks  = ((r->oftrq_off + len) / SLASH_BMAP_BLKSZ) +
	//	(len & (~SLASH_BMAP_BLKMASK) ? 1 : 0);

	r->oftrq_nblks  = (len << SLASH_BMAP_SHIFT) +
		(len & (~SLASH_BMAP_BLKMASK) ? 1 : 0);	
	       
	if (op == OFTREQ_OP_WRITE) {
		/* Determine is 'read before write' is needed.
		 */
		if (off & (~SLASH_BMAP_BLKMASK))
			r->oftrq_op |= OFTREQ_OP_PRFFP; 

		if (r->oftrq_nblks > 1)
			if (len & (~SLASH_BMAP_BLKMASK))
				r->oftrq_op |= OFTREQ_OP_PRFLP;

	DEBUG_OFFTREQ(PLL_TRACE, r, "newly built request");
}

#endif
