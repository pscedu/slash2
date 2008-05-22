#include <sys/param.h>
#include <sys/uio.h>

#include <stdlib.h>
#include <strings.h>
#include <unistd.h>

#include "buffer.h"
#include "offtree.h"
#include "fidcache.h"

const char *progname;

size_t mapSize     = (1024*1024*512);
size_t nblksPerReq = 1;
size_t iterScheme  = 0;
size_t overLapBlks = 1;
size_t iterations  = 2;
size_t treeDepth   = 6;
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
				fprintf(stderr, "Err bnum=%llu but p[%zu]\n", 
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
					fprintf(stderr, "iov=%p buf=%p voff=%llx roff=%llx "
						"v->oftiov_base=%p v->oftiov_nblks=%zu\n", 
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
	struct fidcache_memb_handle fcm;
	struct offtree_root  *oftr;
	struct offtree_req    req;
	unsigned int          i;
	int      rc=0;

	memset(&fcm, 0, sizeof(fcm));
	memset(&req, 0, sizeof(req));

	mygetopt(argc, argv);
	
	sl_buffer_cache_init();

	fidcache_handle_init(&fcm);	

	oftr = offtree_create(mapSize, slCacheBlkSz, 
			      treeWidth, treeDepth, &fcm, 
			      sl_buffer_alloc, sl_oftm_addref);

	req.oftrq_darray = PSCALLOC(sizeof(struct dynarray));

	for (req.oftrq_off=soffa, i=0; i < iterations; i++) {	
	
		fprintf(stderr, "Offset "LPX64"\n", req.oftrq_off);

		req.oftrq_root   = oftr;
		req.oftrq_memb   = &oftr->oftr_memb;
		req.oftrq_nblks  = nblksPerReq;	
		req.oftrq_width  = req.oftrq_depth = 0;
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
