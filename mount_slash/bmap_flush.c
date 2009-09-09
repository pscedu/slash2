#include <sys/time.h>
#include <sys/types.h>
#include <stdlib.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "buffer.h"
#include "cli_bmap.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "offtree.h"
#include "slashrpc.h"
#include "slconfig.h"

struct timespec bmapFlushDefMaxAge = {0, 1000000000L};
struct timespec bmapFlushDefSleep =  {0, 100000000L};

struct psclist_cache bmapFlushQ;
struct pscrpc_nbreqset pndgReqs;
struct dynarray pndgReqSets;

#define MIN_COALESCE_RPC_SZ LNET_MTU /* Try for big RPC's */

extern int slCacheBlkSz;

__static int
bmap_flush_oftrq_expired(const struct offtree_req *a)
{
	struct timespec ts;

	clock_gettime(CLOCK_REALTIME, &ts);

	if ((a->oftrq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) > ts.tv_sec)
		return (1);

	else if ((a->oftrq_start.tv_sec + bmapFlushDefMaxAge.tv_sec) < 
		 ts.tv_sec)
		return (0);

	if ((a->oftrq_start.tv_nsec + bmapFlushDefMaxAge.tv_nsec) >=
	    ts.tv_nsec)
		return (1);
	
	return (0);
}

__static size_t 
bmap_flush_coalesce_size(const struct dynarray *oftrqs)
{
	struct offtree_req *r;
	size_t size;

	r = dynarray_getpos(oftrqs, dynarray_len(oftrqs) - 1);
	size = r->oftrq_off + oftrq_size_get(r);

	r = dynarray_getpos(oftrqs, 0);
	size -= r->oftrq_off;
	
	return (size);
}

__static struct pscrpc_request *
bmap_flush_create_rpc(struct pscrpc_import *imp, struct iovec *iovs, int niovs)
{
	struct offtree_req *r;
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *req;
	struct srm_io_req *mq;
        struct srm_io_rep *mp;
	int rc;

	if ((rc = RSX_NEWREQ(imp, SRIC_VERSION, SRMT_WRITE, req, mq, mp))) {
                errno = -rc;
                psc_fatalx("RSX_NEWREQ() bad time to fail :( rc=%d", -rc);
        }

	rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
                            iovs, niovs);
        if (rc)
                psc_fatalx("rsx_bulkclient() failed with %d", rc);


	return (req);
}

__static void
bmap_flush_send_rpcs(const struct dynarray *oftrqs, struct iovec *iovs, 
		     int niovs)
{
	struct pscrpc_request *req;
	
	r = dynarray_get(oftrqs);
	imp = msl_bmap_to_import(r->bcm_pri);

	for (i=1; i < dynarray_len(oftrqs); i++) {
		/* All oftrqs should have the same import, otherwise
		 *   there is a major problem.
		 */
		r = dynarray_getpos(oftrqs, i);
		psc_assert(imp == msl_bmap_to_import(r->bcm_pri));
	}

	if (bmap_flush_coalesce_size(oftrqs) <= LNET_MTU) {
		/* Single rpc case.  Set the appropriate cb handler
		 *   and attach to the nb request set.
		 */
		req = bmap_flush_create_rpc(imp, iovs, niovs);
		req->rq_interpret_reply = msl_io_rpc_cb;
		req->rq_async_args.pointer_arg[0] = oftrqs;
		nbreqset_add(&pndgReqs, req);

	} else {
		/* Deal with a multiple rpc operation 
		 */
		struct pscrpc_request_set *set;
		struct iovec *tiov;
		size_t size;
		int n, rc;

		set = pscrpc_prep_set();
		set->set_interpret = msl_io_rpcset_cb;
		set->set_arg = oftrqs;
		
		for (n=1, size=0, tiov=iovs; n <= niovs; n++) {
			if ((size + iovs[n-1].iov_len) > LNET_MTU) {
				req = bmap_flush_create_rpc(imp, tiov, n);
				pscrpc_set_add_new_req(set, req);
				if (pscrpc_push_req(req)) {
					DEBUG_REQ(PLL_ERROR, req, 
						  "pscrpc_push_req() failed");
					psc_fatalx("no failover yet");
				}
				size = iovs[n-1].iov_len;
				tiov = iovs[n-1];
			} else
				size += iovs[n-1].iov_len;
		}

		
	}	
}

__static int 
bmap_flush_oftrq_cmp(const void *x, const void *y)
{
	struct offtree_req *a = *(const struct offtree_req *)x;
	struct offtree_req *b = *(const struct offtree_req *)y;

	if (a->oftrq_off < b->oftrq_off)
		return (-1);

	else if	(a->oftrq_off > b->oftrq_off)
		return (1);

	else {
		/* Larger requests with the same start offset
		 *  should have ordering priority.
		 */
		if (a->oftrq_nblks > b->oftrq_nblks)
			return (-1);

		else if (a->oftrq_nblks < b->oftrq_nblks)
			return (1);
	}		
	return (0);
}

__static int
bmap_flush_coalesce_map(const struct dynarray *oftrqs, struct iovec **iovs)
{
	struct offtree_req *r;
	off_t off;
	int j, niovs=1;

	psc_assert(!*iovs);
	psc_assert(dynarray_len(oftrqs) > 0);

	*iovs = PSCALLOC(sizeof(struct iovec));
	
	r = dynarray_get(oftrqs);
	oftiov_2_iov(r, *iovs[0]);

	for (j=1, off = oftrq_size_get(r); j < dynarray_len(oftrqs); j++) {
		r = dynarray_getpos(oftrqs, j);
		if (oftrq_size_get(r) <= off) 
			continue;

		psc_assert(!((off - r->oftrq_off) % slCacheBlkSz));

		niovs++;
		*iovs = psc_realloc(sizeof(struct iovec) * niovs);	
		oftiov_2_iov(r, *iovs[niovs]);
		/* Increase the base pointer past the overlapping area.
		 */
		*iovs[niovs].base += (off - r->oftrq_off);
		off = oftrq_size_get(r);
	}

	return (niovs);
}

__static struct dynarray * 
bmap_flush_trycoalesce(const struct dynarray *oftrqs, int *offset)
{
	int i, off, expired=0;
	struct offtree_req *r=NULL, *t;
	struct dynarray b=DYNARRAY_INIT, *a=NULL;

	psc_assert(dynarray_len(oftrqs) > *offset);

	for (off=0; (off + *offset) < dynarray_len(oftrqs); off++) {
		t = dynarray_getpos(oftrqs, off + *offset);
		if (r)
			psc_assert(t->oftrq_off >= r->oftrq_off);
		/* If any member is expired then we'll push everything out.
		 */
		if (!expired)
			expired = bmap_flush_oftrq_expired(r);

		DEBUG_OFTRQ(PLL_INFO, r, "1st oftrq compare to..");
		DEBUG_OFTRQ(PLL_INFO, t, "2nd oftrq (expired=%d)", expired);

		if (!r || t->oftrq_off <= (r->oftrq_off + oftrq_size_get(r))) {
			dynarray_add(&b, t);
			r = t;
		} else {
			if ((bmap_flush_coalesce_size(&b) >= 
			     MIN_COALESCE_RPC_SZ) || expired)
				goto make_coalesce;
			else {
				/* Start over.
				 */
				dynarray_reset(&b);
				dynarray_add(&b, t);
				r = t;
			}
		}			
	}
	if (expired) {
	make_coalesce:
		a = PSCALLOC(sizeof(*a));
		for (i=0; i < dynarray_len(&b); i++)
			dynarray_add(a, dynarray_getpos(&b, i));
	}

	*offset += off;
	dynarray_free(&b);

	return (a);
}

void *
bmap_flush_thr(__unusedx void *arg)
{
	struct bmapc_memb *b;
	struct dynarray a = DYNARRAY_INIT, *oftrqs;
	struct offtree_req *r, *tr;
	struct psclist_head *h;
	struct iovec *iovs;
	int i, niovs;

	while (1) { 
		b = lc_getwait(&bmapFlushQ);

		BMAP_LOCK(b);		
		DEBUG_BMAP(PLL_INFO, b, "try flush");
		
		h = &bmap_2_msbd(b)->msbd_oftrqs;

		if (b->bcm_mode & BMAP_DIRTY)
			psc_assert(!psclist_empty(h));
		else {
			DEBUG_BMAP(PLL_INFO, b, "is clean, descheduling..");
			psc_assert(psclist_empty(h))
			BMAP_ULOCK(b);
			continue;
		}
		/* Ok, have something to do.
		 */
		dynarray_reset(&a);

		psclist_for_each_entry(r, h, oftrq_lentry)
			dynarray_add(&a, r);

		BMAP_ULOCK(b);
		/* Sort the items by their offsets.
		 */
		qsort(a.da_items, sizeof(void *), dynarray_len(&a),  
		      bmap_flush_oftrq_cmp);
		
		while (i < dynarray_len(&a)) {
			oftrqs = bmap_flush_trycoalesce(&a, &i);
			if (!oftrqs)
				/* Didn't get any, keep looping through
				 *   until the end of the dynarray is 
				 *   reached.
				 */
				continue;

			niovs = bmap_flush_coalesce_map(oftrqs, &iovs);
			psc_assert(niovs);
			/* Have a set of iov's now.  Let's create an rpc
			 *   or rpc set and send it out.
			 */
			bmap_flush_send_rpcs(oftrqs, iovs, niovs);
			PSCFREE(iovs);
		}
	}
}

