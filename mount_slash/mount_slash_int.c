/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26

#include <fuse.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/log.h"
#include "psc_mount/dhfh.h"

#include "slconfig.h"
#include "mount_slash.h"
#include "slashrpc.h"
#include "offtree.h"
#include "fidcache.h"

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

__static void
msl_oftrq_destroy(struct offtree_req *r) 
{
	struct bmap_cache_memb *b = r->oftrq_root->oftr_pri;
	
	psc_assert(b);
	psc_assert(r->oftrq_darray);

	PSCFREE(r->oftrq_darray);
	atomic_dec(&b->bcm_opcnt);
	psc_assert(atomic_read(&b->bcm_opcnt) >= 0);
	
	if (r->oftrq_fill.oftfill_reqset)
		pscrpc_set_destroy(r->oftrq_fill.oftfill_reqset);
}


__static void
msl_fcm_get(struct fhent *fh)
	struct fcache_memb_handle *f = sl_fidc_get(&fidcFreeList);

	fidcache_memb_init(f);
	/* Incref so that this inode is not immediately considered for reaping.
	 */
	fcmh_incref(fcmh);
	/* Cross-associate the fcmh and fhent structures.
	 */
	fcmh->fcmh_fh = fh->fh;	
	fh->fh_pri    = fcmh;

	return (fcmh);
}

/**
 * msl_fdreg_cb - (file des registration callback) This is the callback handler issued from fh_register().  Its primary duty is to allocate the fidcache member handle structure and attach it to the file des structure.
 * @fh: the file handle.
 * @op: op type (FD_REG_NEW, FD_REG_EXISTS are valid).
 * @args: array of pointer arguments (not used here).
 */
void
msl_fdreg_cb(struct fhent *fh, int op, __unusedx void *args[])
{
	psc_assert(op == FD_REG_NEW ||
		   op == FD_REG_EXISTS);

	spinlock(&fh->lock);
	if (op == FD_REG_NEW) {
		if (fh->fd != FD_REG_INIT) 
			goto exists;

		if (fh->fd == FD_REG_INIT) {
			psc_assert(fh->fh_pri == NULL);
			psc_assert(!atomic_read(&fh->refcnt));
 			/* msl_fcm_get() may block for an fcmh, 
			 *  hopefully that doesn't hurt us here since
			 *  the fh is spinlocked.
			 */
			msl_fcm_get(fh);
			fh->fd = FD_REG_READY;
		}
		
	} else if (op == FD_REG_EXISTS) {
	exists:
		psc_assert(fh->fd == FD_REG_READY);		
	}
	atomic_inc(&fh->refcnt);
	freelock(&fh->lock);
}

/**
 * msl_bmap_fetch - perform a blocking 'get' operation to retrieve one or more bmaps from the MDS.
 * @f: pointer to the fid cache structure to which this bmap belongs.
 * @b: the block id to retrieve (block size == SLASH_BMAP_SIZE).
 * @n: the number of bmaps to retrieve (serves as a simple read-ahead mechanism)
 */
__static int 
msl_bmap_fetch(struct fidcache_memb_handle *f, sl_blkno_t b, size_t n, int rw) 
{
	struct pscrpc_bulk_desc *desc;
        struct pscrpc_request *rq;
        struct srm_bmap_req *mq;
        struct srm_bmap_rep *mp;
	struct bmap_cache_memb **bmaps;
        struct iovec *iovs;
        int rc=-1, i;

	psc_assert(n < BMAP_MAX_GET);

	/* Build the new rpc request.
	 */
	if ((rc = RSX_NEWREQ(mds_import, SRM_VERSION,
			     SRMT_GETBMAP, rq, mq, mp)) != 0)
		return (rc);

	mq->blkno = b;
	mq->nblks = n; 
	mq->fid   = FID_ANY; /* The MDS interpolates the fid from his cfd */
	mq->rw    = rw;

	iovs  = PSCALLOC(sizeof(*iov) * n);
	/* Init the bmap handles and setup the iovs.
	 */
	for (i=0; i < n; i++) {
		bmaps[i] = PSCALLOC(sizeof(bmap_cache_memb));
		bmap_cache_memb_init(bmaps[i], f);
		iovs[i].iov_base = &bmaps[i].bcm_bmapi;
		iovs[i].iov_len  = sizeof(struct bmap_info);
	}
	DEBUG_FCMH(PLL_DEBUG, f, "retrieving bmaps (s=%u, n=%zu)", b, n);

	rsx_bulkclient(rq, &desc, BULK_PUT_SINK, SRM_BULK_PORTAL, iovs, n);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		/* Verify the return.
		 */
		if (!mp->nblks) {
			psc_errorx("MDS returned 0 bmaps");
			rc = -1;
			goto fail;
		}
		if (mp->nblks > n) {
			psc_errorx("MDS returned more bmaps than expected! "
				   "mp->nblks(%zu) > n(%zu)", 
				   mp->nblks, n);
			rc = -1;
			mp->nblks = 0;
			goto fail;
		}
		/* Add the bmaps to the tree.
		 */
		spinlock(&f->fcmh_lock);
		for (i=0; i < mp->nblks ; i++) {
			SPLAY_INSERT(bmap_cache, &f->fcmh_bmap_cache, 
				     bmap_cache_memb);
			atomic_inc(&f->fcmh_bmap_cache_cnt);
		}
		freelock(&f->fcmh_lock);
	} else
		/* Something went wrong, free all bmaps.
		 */
		mp->nblks = 0;
 fail:	
	/* Free any slack.
	 */
	for (i=mp->nblks; i < n; i++)
		PSCFREE(bmaps[i]);

	PSCFREE(iovs);
	return (rc);
}

/**
 * msl_bmap_load - locate a bmap in fcache_memb_handle's splay tree or retrieve it from the MDS.
 * @f: the fcache_memb_handle for the owning file.
 * @n: bmap number.
 * @prefetch: the number of subsequent bmaps to prefetch.
 * @rw: tell the mds if we plan to read or write.
 */
__static struct bmap_cache_memb *
msl_bmap_load(struct fcache_memb_handle *f, sl_blkno_t n, int prefetch, u32 rw)
{
	struct bmap_cache_memb t, *b;
	int rc=0;

	t.bcm_blkno=n;

	b = SPLAY_FIND(bmap_cache, &f->fcmh_bmap_cache, &t);
	if (!b) {
		/* Retrieve the bmap from the sl_mds.
		 */			
		rc = msl_bmap_fetch(f, t.bcm_blkno, prefetch, rw);
		if (rc)
			return NULL;
	}
	b = SPLAY_FIND(bmap_cache, &f->fcmh_bmap_cache, &t);
	psc_assert(b);	
	atomic_inc(&b->bcm_opcnt);
	return (b);
}

/**
 * msl_bmap_to_import - Given a bmap, perform a series of lookups to locate the ION import.  The ION was chosen by the mds and returned in the msl_bmap_fetch routine. msl_bmap_to_import queries the configuration to find the ION's private info - this is where the import pointer is kept.  If no import has yet been allocated a new is made.
 * @b: the bmap
 * Notes: the bmap is locked to avoid race conditions with import checking.
 *        the bmap's refcnt must have been incremented so that it is not freed from under us.
 */
__static struct pscrpc_import *
msl_bmap_to_import(struct bmap_cache_memb *b)
{
	struct bmap_info_cli *c;
	sl_resm_t *r;
	int locked;
	
	/* Sanity check on the refcnt.
	 */
	psc_assert(atomic_read(&b->bcm_refcnt) > 0);

	locked = reqlock(&b->bcm_lock);
	r = libsl_nid2resm(b->bcm_bmapih.bmapi_ion);
	if (!r)
		psc_fatalx("Failed to lookup %s, verify that the slash configs"
			   " are uniform across all servers", 
			   libcfs_nid2str(b->bcm_bmapih.bmapi_ion));

	c = (struct bmap_info_cli *)r->resm_pri;
	if (!c->bmic_import) {
		psc_dbg("Creating new import to %s", 
			libcfs_nid2str(b->bcm_bmapih.bmapi_ion));

		if ((c->bmic_import = new_import()) == NULL)
			psc_fatalx("new_import");

		c->bmic_import->imp_client =
			PSCALLOC(sizeof(struct pscrpc_client));
		psc_assert(c->bmic_import->imp_client);

		c->bmic_import->imp_client->cli_request_portal = 
			SRCI_REQ_PORTAL;
		c->bmic_import->imp_client->cli_reply_portal =
			SRCI_REP_PORTAL;
	}
	ureqlock(&b->bcm_lock, locked);	
	return (c->bmic_import);
}

/**
 * msl_io_cb - this function is called from the pscrpc layer as the rpc request completes.  Its task is to set various state machine flags and call into the slb layer to decref the inflight counter.  On SRMT_WRITE the IOV's owning pin ref is decremented.
 * @rq: the rpc request.
 * @arg: opaque reference to pscrpc_async_args. pscrpc_async_args is where we stash pointers of use to the callback.
 * @status: return code from rpc.
 */
int
msl_io_cb(struct pscrpc_request *rq, void *arg, int status)
{
	struct pscrpc_async_args *args = arg;
	struct dynarray     *a = args->pointer_arg[MSL_IO_CB_POINTER_SLOT];
	struct offtree_iov  *v;
	struct offtree_memb *m;
	struct sl_buffer    *s;
	int i, n=dynarray_len(a);
	int op=rq->rq_reqmsg->opc;

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	if (status) {
                DEBUG_REQ(PLL_ERROR, rq, "non-zero status status %d, "
                          "rq_status %d", status, rq->rq_status);
		psc_fatalx("Resolve issues surrounding this failure");
		// XXX Freeing of dynarray, offtree state, etc
                return (status);
        }

	for (i=0; i < n; i++) {
		v = dynarray_getpos(i);

		/* Decrement the inflight counter in the slb.
		 */
		slb_inflight_cb(v, SL_INFLIGHT_DEC);

		m = (struct offtree_memb *)v->oftiov_memb;
		/* Take the lock and do some sanity checks.
		 *  XXX waitq wake here (oftm)
		 */
		spinlock(&m->oft_lock);
		switch (op) {
		case (SRMT_READ):
			oftm_read_prepped_verify(m);
			psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG) &&
				   ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING));
			ATTR_UNSET(v->oftiov_flags, OFTIOV_FAULTING);
			ATTR_UNSET(v->oftiov_flags, OFTIOV_FAULTPNDG);
			/* Set datardy but leave OFT_READPNDG (in the oftm) until 
			 *   the memcpy -> application buffer has taken place.
			 */ 
			ATTR_SET(v->oftiov_flags, OFTIOV_DATARDY);
			DEBUG_OFFTIOV(PLL_TRACE, v, "OFTIOV_DATARDY");
			
			break;

		case (SRMT_WRITE):
			struct offtree_root *oftr;

			oftm_write_prepped_verify(m);
			oftr = args->pointer_arg[MSL_IO_CB_POINTER_SLOT];
			/* Manage the offtree leaf and decrement the slb.
			 */
			(oftr->oftr_slbpin_cb)(v, SL_BUFFER_UNPIN);
			atomic_dec(&m->oft_wrop_ref);
			/* DATARDY must always be true here since the buffer
			 *  has already been copied to with valid data.
			 */
			psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_PUSHPNDG) &&
                                   ATTR_TEST(v->oftiov_flags, OFTIOV_PUSHING)  &&
				   ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
                        ATTR_UNSET(v->oftiov_flags, OFTIOV_PUSHING);
                        ATTR_UNSET(v->oftiov_flags, OFTIOV_PUSHPNDG);
			DEBUG_OFFTIOV(PLL_TRACE, v, "PUSH DONE");
			break;

		default:
			DEBUG_REQ(PLL_FATAL, rq, "bad opcode");
			psc_fatalx("How did this opcode(%d) happen?", op);
		}
		freelock(&m->oft_lock);			
	}
	/* Free the dynarray which was allocated in msl_pages_prefetch().
	 */
	PSCFREE(a);
	pscrpc_req_finished(rq);
	return (0);
}

/**
 * msl_pagereq_finalize - this function is the intersection point of many slash subssytems (pscrpc, sl_config, and bmaps).  Its job is to prepare a reqset of read or write requests and ship them to the correct io server.
 * @r:  the offtree request.
 * @a:  the array of iov's involved.
 * @op: GET or PUT.
 */
__static void
msl_pagereq_finalize(struct offtree_req *r, struct dynarray *a, int op)
{
	struct pscrpc_import      *imp;
	struct pscrpc_request_set *rqset;
	struct pscrpc_request     *req;
        struct pscrpc_bulk_desc   *desc;
	struct bmap_cache_memb    *bcm;
	struct iovec              *iovs;
	struct offtree_iov        *v;
        struct srm_io_req         *mq;
        struct srm_io_rep         *mp;
	int    i, n=dynarray_len(a), tblks=0;
	
	psc_assert(n);	
	psc_assert(op == MSL_PAGES_PUT || op == MSL_PAGES_GET);

	if (op == MSL_PAGES_GET) {
		psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG));
		ATTR_SET(v->oftiov_flags, OFTIOV_FAULTING);
	} else {
		psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_PUSHPNDG));
		ATTR_SET(v->oftiov_flags, OFTIOV_PUSHING);
	}
	/* Get a new request set if one doesn't already exist.
	 */	
	if (!r->oftrq_fill.oftfill_reqset)
		r->oftrq_fill.oftfill_reqset = pscrpc_prep_set();
	
	rqset = r->oftrq_fill.oftfill_reqset;
	psc_assert(rqset);
	/* Point to our bmap handle, it has the import information needed
	 *  for the rpc request.  (Fid and ios id's)
	 */
	bcm = (struct bmap_cache_memb *)r->oftrq_root->oftr_pri;
	imp = msl_bmap_to_import(bcm);

	if ((rc = rsx_newreq(imp, SRCI_VERSION, 
			     (op == MSL_PAGES_PUT ? SRMT_WRITE : SRMT_READ),
			     sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0) {
                errno = -rc;
		psc_fatalx("rsx_newreq() bad time to fail :(");
        }
	/* Setup the callback, supplying the dynarray as a argument.
	 */	
	req->rq_interpret_reply = msl_io_cb;
        req->rq_async_args.pointer_arg[MSL_IO_CB_POINTER_SLOT] = a;
	if (op == MSL_PAGES_PUT)
		/* On write, stash the offtree root for the callback
		 *  so that the buffer can be unpinned.
		 */
		req->rq_async_args.pointer_arg[MSL_WRITE_CB_POINTER_SLOT] = 
			r->oftrq_root;

	/* Prep the iovs and bulk descriptor
	 */
	iovs = PSCALLOC(sizeof(*iovs) * n);
	for (i=0; i < n; i++) {
		v = dynarray_getpos(i);
		if (!i)
			mq->offset = v->oftiov_off;

		tblks += v->oftiov_nblks;
		/* Make an iov for lnet.
		 */
		oftiov_2_iov(v, iovs[i]);
		/* Tell the slb layer that this offtree_iov is going 
		 *   for a ride.
		 */
		slb_inflight_cb(v, SL_INFLIGHT_INC);
	}
	mq->size = v->oftiov_blksz * tblks;
	mq->op = (op == MSL_PAGES_PUT ? SRMIO_WR : SRMIO_RD);
	mq->cfd = r->oftrq_cfd;

	/* Seems counter-intuitive, but it's right.  MSL_PAGES_GET is a 
	 * 'PUT' to the client, MSL_PAGES_PUSH is a server get.
	 */
	rsx_bulkclient(req, &desc, 
		       (op == MSL_PAGES_GET ? BULK_PUT_SINK : BULK_GET_SOURCE),
		       SRM_BULK_PORTAL, iovs, n);
	/* The bulk descriptor copies these iovs so it's OK to free them.
	 */
	PSCFREE(iovs);
	/* Push onto our request set and send it out the door.
	 */
	pscrpc_set_add_new_req(rqset, req);
	if (pscrpc_push_req(req)) {
                DEBUG_REQ(PLL_ERROR, req, "pscrpc_push_req() failed");
		psc_fatalx("pscrpc_push_req(), no failover yet");
	}
}

/**
 * msl_pages_track_pending - called when an IOV is being faulted in by another thread.  msl_pages_track_pending() saves a reference to the ongoing RPC so that later (msl_pages_blocking_load()) it may be checked for completion. 
 * @r: offtree_req to which the iov is attached.
 * @v: the iov being tracked.
 * Notes:  At this time no retry mechanism is in place.
 */
__static void
msl_pages_track_pending(struct offtree_req *r,
                        const struct offtree_iov *v)
{
        struct offtree_memb *m = (struct offtree_memb *)v->oftiov_memb;

        /* This would be a problem..
         */
        psc_assert(!ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
        /* There must be at least 2 pending operations
         *  (ours plus the one who set OFTIOV_FAULT*)
         */
        psc_assert(atomic_read(m->oft_rdop_ref) > 1);
        /* Allocate a new 'in progress' dynarray if
         *  one does not already exist.
         */
        if (!r->oftrq_fill->oftfill_inprog)
                r->oftrq_fill->oftfill_inprog =
                        PSCALLOC(sizeof(struct dynarray));
        /* This iov is being loaded in by another
         *  thread, place it in our 'fill' structure
         *  and check on it later.
         */
        dynarray_add(r->oftrq_fill->oftfill_inprog, v);
}

/**
 * msl_pages_getput - called to stage pages in or out of the client-side cache.  msl_pages_getput() does the proper coalescing for puts and gets, managing the semantics and dynarray allocation for each.  Dynarray's allocated here are freed in the msl_io_cb.  On MSL_PAGES_GET, msl_pages_getput() checks the cache for valid pages, pages being loaded by other threads are tracked via msl_pages_track_pending() and blocked-on for completion.  IOV's which are already in 'DATARDY' state are protected by oft_rdop_ref which was inc'd in offtree_region_preprw().  msl_pages_copyout() does the decref on oft_rdop_ref.
 * @r: the request.
 * @op: used by the below macros.
 * Notes: both MSL_PAGES_PUT and MSL_PAGES_GET call msl_pagereq_finalize() to issue the necessary RPC requests (which are gathered within the rpcset attached to the oftrq.
 */
__static void
msl_pages_getput(struct offtree_req *r, int op)
{	
	struct offtree_iov  *v;
	struct offtree_memb *m;
	struct dynarray     *a=r->oftrq_darray, *coalesce=NULL;
	off_t                o=r->oftrq_off, toff=0;
	size_t               niovs=dynarray_len(a);
	u32                  i, j, n;
	int                  rc=0, nc_blks=0, t=0;

#define launch_cb {					\
		psc_assert(coalesce);			\
		msl_pagereq_finalize(r, coalesce, op);	\
	        coalesce = NULL;			\
		nc_bufs  = 0;				\
	}

#define new_cb {					\
		psc_assert(!nc_blks);			\
		psc_assert(!coalesce);			\
		coalesce = PSCALLOC(sizeof(*coalesce)); \
	}

	for (i=0; i < niovs; i++, n=0) {
		v = dynarray_getpos(a, i);
		DEBUG_OFFTIOV(PLL_TRACE, v, 
			      "iov%d rq_off=%zu OFT_IOV2E_OFF_(%zu)",
			      i, req->oftrq_off,  OFT_IOV2E_OFF_(v));

		/* Assert that oft iovs are contiguous.
		 */
		if (i)
			psc_assert(toff == v->oftiov_off);

		toff = OFT_IOV2E_OFF_(v) + 1;
		/* The dynarray may contain iov's that we don't need, 
		 *  skip them.
		 */
		if (o > OFT_IOV2E_OFF_(v))
			continue;

                /* On read-before-write, only check first and last iov's
                 *  if they have been specified for retrieval.
                 * 't' is used to denote the first valid iov.
                 */
		if ((op == MSL_PAGES_GET) && 
		    (ATTR_TEST(r->oftrq_op, OFTREQ_OP_WRITE))) {
			if (!((t == 0 && 
			       ATTR_TEST(r->oftrq_op, OFTREQ_OP_PREFFP)) || 
			      (i == niovs - 1) && 
			      ATTR_TEST(r->oftrq_op, OFTREQ_OP_PREFLP)))
				continue;
			else 
				if (!t)
					t = 1;
		}
		/* Map the offtree leaf holding this iov.
		 */
		m = (struct offtree_memb *)v->oftiov_memb;
		DEBUG_OFFTIOV(PLL_INFO, v, "processing..");
		DEBUG_OFT(PLL_INFO, m, "..processing");

		spinlock(&m->oft_lock);
		/* Ensure the offtree leaf is sane.
		 */
		if (op == MSL_PAGES_GET)
			oftm_read_prepped_verify(m);		
		else
			oftm_write_prepped_verify(m);

		nblks += v->oftiov_nblks;
		/* oftiov_nblks cannot be bigger than slCacheNblks!
		 */
		psc_assert(v->oftiov_nblks <= slCacheNblks);
		psc_assert(nc_blks < slCacheNblks);
		/* Iov's cannot be split across requests but they may be 
		 *  coalesced into one request.  We're assured that the 
		 *  largest contiguous piece of memory is <= to LNET_MTU.
		 * For MSL_PAGES_PUT, the entire array is always sent.
		 */
		if (op == MSL_PAGES_PUT || 
		    ((op == MSL_PAGES_GET) && 
		     (!ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY) &&
		      !ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG)))) {
			/* Mark it.
			 */
			if (op == MSL_PAGES_GET)
				ATTR_SET(v->oftiov_flags, OFTIOV_FAULTPNDG);
			else
				ATTR_SET(v->oftiov_flags, OFTIOV_PUSHPNDG);

   			if ((v->oftiov_nblks + nc_blks) < slCacheNblks) {
				/* It fits, coalesce.
				 */
				if (!coalesce)
					new_cb;

				dynarray_add(coalesce, v);
				nc_blks += v->oftiov_nblks;
				/* Just to be sure..
				 */
				psc_assert(nc_blks < slCacheNblks);
				
			} else if ((v->oftiov_nblks + nc_blks) == 
				   slCacheNblks) {
				/* A perfect fit, send this one out.
				 */
				dynarray_add(coalesce, v);
				nc_blks += v->oftiov_nblks;
				launch_cb;

			} else {
				/* The new iov won't fit, send out the cb as 
				 *  is.  Launch the current coalesce buffer 
				 *  (which must exist). 
				 */				
				launch_cb;
				/* Make a new cb and add the new iov.
				 */
				new_cb;
				dynarray_add(coalesce, v); 
				/* The new one may constitute an entire I/O, 
				 *  if so send it on its merry way.
				 */
				if (v->oftiov_nblks == slCacheNblks)
					launch_cb;
				else
					nc_blks = v->oftiov_nblks;	
			}
		} else { 
			/* This iov is being or already has been handled which 
			 *  means that any existing coalesce buffer must be 
			 *  pushed.
			 */
			psc_assert(op == MSL_PAGES_GET);

			if (coalesce)
				launch_cb;

			if (ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG))
				msl_pages_track_pending(r, v);
			else
				psc_assert(ATTR_TEST(v->oftiov_flags, 
						     OFTIOV_DATARDY));
		}
		/* Finished testing / setting attrs, release the lock.
		 */
	end_loop:
		freelock(&m->oft_lock);	
	}
	/* There may be a cb lingering.
	 */
	if (coalesce)
		lauch_cb;
}
#define msl_pages_prefetch(r) msl_pages_getput(r, MSL_PAGES_GET)
#define msl_pages_flush(r)    msl_pages_getput(r, MSL_PAGES_PUT)

/**
 * msl_pages_copyin - copy user pages into buffer cache and schedule the slabs to be sent to the IOS backend.
 * @r: array of request structs.
 * @n: the number of the oftrq's involved.
 * @buf: the source (application) buffer.
 * @size: the size of the buffer.
 * @off: logical file offset.
 * Notes:  the iov's (contained in the offtree_req dynarray) are unpinned via msl_io_cb.
 */
__static size_t
msl_pages_copyin(struct offtree_req *r, int n, char *buf, 
		 size_t size, off_t off)
{
	struct dynarray     *a;
	struct offtree_iov  *v;
	struct offtree_memb *m;
	struct iovev        *iov;
	int    l, i, j, x=0, niovs=0, nc_bufs=0;
	off_t  o, t;
	size_t nbytes;
	ssize_t tsize;
	char  *b, *p;
	int    rbw=0;

	psc_assert(r[0].oftrq_off <= off);
	/* Relative offset into the buffer.
	 */
	t = off - r[0].oftrq_off;
	
	for (i=0, p=buf, tsize=size; i < n; i++) {
		a = r[i].oftrq_darray;
		l = dynarray_len(a);

		for (j=0; j < l; j++) {
			v = dynarray_getpos(j);
			m = (struct offtree_memb *)v->oftiov_memb;

			DEBUG_OFFTIOV(PLL_TRACE, v, "iov%d rq_off=%zu "
				      "OFT_IOV2E_OFF_(%zu) bufp=%p sz=%zu "
				      "tsz=%zd nbytes=%zu",
				      i, req->oftrq_off,  OFT_IOV2E_OFF_(v), 
				      p, size, tsize, nbytes);
			if (!tsize)
				goto out;

			/* Set the starting buffer pointer into 
			 *  our cache vector.
			 */
			b  = (char *)v->oftiov_base;

			m = (struct offtree_memb *)v->oftiov_memb;
			spinlock(&m->oft_lock);
			psc_assert(oftm_write_prepped_verify(m));
			if (!x) {
				/* The first req structure is always needed, 
				 *  but not necessarily its first iov(s).
				 */
				if (r[0].oftrq_off > OFT_IOV2E_OFF_(v)) {
					freelock(&m->oft_lock);
                                        continue;
				}
				/* 't' informs us that the start of the write
				 *   is unaligned, the latter half of this
				 *   stmt checks the end of the write.
				 */
				if (t) {
					/* Partial iov writes must have been
					 *  pre-faulted (ie. read).
					 */
					oftm_read_prepped_verify(m);
					/* Verify that the iov is DATARDY.
					 */
					psc_assert(ATTR_TEST(v->oftiov_flags, 
							     OFTIOV_DATARDY));
					/* Set the read-before-write notifier
					 *  (first iov).
					 */
					rbw = 1;			
					/* Bump our cache vector destination
					 *  pointer.
					 */
					b += t;
				}
				x = 1;
				nbytes = MIN(OFT_IOVSZ(v) - t, tsize);

			} else if (i == (n-1) && j == (l-1)) {
				/* Last iov, if the write size is smaller than
				 *  the iov the data must have been faulted in.
				 * Note:  This does not need to be run in
				 *   addition to the above 'if' since all that
				 *   really needs to happen here is to check 
				 *   for OFTIOV_DATARDY and set rbw=1.
				 */
				if (tsize < OFT_IOVSZ(v)) {
					oftm_read_prepped_verify(m);
                                        psc_assert(ATTR_TEST(v->oftiov_flags, 
							     OFTIOV_DATARDY));
					/* Set the read-before-write notifier.
					 *  (last iov).
					 */
					rbw = 1;
                                }
				nbytes = MIN(OFT_IOVSZ(v), tsize);

			} else {
				psc_assert(tsize >= OFT_IOVSZ(v));
				nbytes = MIN(OFT_IOVSZ(v), tsize);
			}
			/* I'm thinking that we don't need to lock
			 *  the memcpy..
			 */
			freelock(&m->oft_lock);
			/* Do the deed.
			 */
			memcpy(b, p, nbytes);
			/* Notify others that this buffer is now valid for
			 *  reads or unaligned writes.  Note that the buffer
			 *  must still be pinned for it has not been sent yet.
			 */
			if (rbw) {
				psc_assert(ATTR_TEST(v->oftiov_flags, 
						     OFTIOV_DATARDY));
				/* Drop the rbw reference.
				 */
				atomic_dec(&m->oft_rdop_ref);
				rbw = 0;
			} else {
				spinlock(&v->oftiov_lock);
				ATTR_SET(v->oftiov_flags, OFTIOV_DATARDY);
				freelock(&v->oftiov_lock);
			}
			p += nbytes
			tsize -= nbytes;
			/* XXX the details of this wakeup may need to be 
			 *  sorted out.
			 */
			psc_waitq_wake(&m->oft_waitq);
		}				
		/* Queue these iov's for send to IOS.
		 */
		msl_pages_flush(r);
	}
 out:
	psc_assert(!tsize);
	return (size);
}


/**
 * msl_pages_copyout - copy pages to the user application buffer, release rdop references held in the offtree_memb, and unpin the iov's owning slabs.  Also sets the IOV to DATARDY so that other threads may access the data cached there.
 * @r: the offtree req array.
 * @n: number of oftrq's
 * @buf: application source buffer.
 * @size: the number of bytes involved.
 * @off: file-logical offset.
 */
__static size_t
msl_pages_copyout(struct offtree_req *r, int n, char *buf, 
		  size_t size, off_t off)
{
	struct dynarray     *a;
	struct offtree_iov  *v;
	struct offtree_memb *m;
	int    l, i, j, x=0;
	off_t  o, t;
	size_t nbytes;
	ssize_t tsize;
	char  *b, *p;

	psc_assert(r[0].oftrq_off <= off);
	/* Relative offset into the buffer.
	 */
	t = off - r[0].oftrq_off;
	
	for (i=0, p=buf, tsize=size; i < n; i++) {
		a = r[i].oftrq_darray;
		l = dynarray_len(a);
		
		for (j=0; j < l; j++) {
			v=dynarray_getpos(j);
			DEBUG_OFFTIOV(PLL_TRACE, v, "iov%d rq_off=%zu "
				      "OFT_IOV2E_OFF_(%zu) bufp=%p sz=%zu "
				      "tsz=%zd nbytes=%zu",
				      i, req->oftrq_off,  OFT_IOV2E_OFF_(v), 
				      p, size, tsize, nbytes);
			if (!x) {
				/* These pages aren't involved, skip.
				 */
				if (r[x].oftrq_off > OFT_IOV2E_OFF_(v))
					continue;
				x  = 1;
				b += t;				
				nbytes = MIN(OFT_IOVSZ(v) - t, tsize);
			} else
				nbytes = MIN(OFT_IOVSZ(v), tsize);

			m = (struct offtree_memb *)v->oftiov_memb;
			spinlock(&m->oft_lock);
			oftm_read_prepped_verify(m); 			
			psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
			freelock(&m->oft_lock);

			b  = (char *)v->oftiov_base;
			memcpy(p, b, nbytes);
			p += nbytes;			
			tsize -= nbytes;
			/* As far as we're concerned, we're done with all vital
			 *  operations on this iov.  It can go away if needed.
			 * Manage the offtree leaf and decrement the slb.
			 */
			(r->oftrq_root->oftr_slbpin_cb)(v, SL_BUFFER_UNPIN);
			atomic_dec(&m->oft_rdop_ref);
			/* XXX the details of this wakeup may need to be
			 *  sorted out.  The main thing to know is that there
			 *  are several sleepers / wakers accessing this q
			 *  for various reasons.
			 */
			psc_waitq_wake(&m->oft_waitq);
			if (!tsize)
				goto out;
		}				
	}	 
 out:
	psc_assert(!tsize);
	return (size);
}

/**
 * msl_pages_blocking_load - manage data prefetching activities.  This includes waiting on other thread to complete RPC for data in which we're interested.
 * @r: array of offtree requests.
 * @n: number of offtree reqs.
 */
__static int
msl_pages_blocking_load(struct offtree_req *r, int n)
{
	int i, rc=0;

	for (i=0; i < n; i++)
		msl_pages_prefetch(r[i]);
	/* Iterate again, this time checking / waiting for all page-ins to 
	 *   complete.  Note that msl_read_cb sets OFTIOV_DATARDY.
	 */
	for (i=0; i < n; i++) {
		struct offtree_fill *f = r[i].oftrq_fill;
		psc_waitq_t *w;
		int          k;
		/* If a request set is present then block on its completion.
		 */
		if (f->oftfill_reqset) {
			if ((rc = pscrpc_set_wait(f->oftfill_reqset))) {
				psc_error("pscrpc_set_wait rc=%d", rc);
				return (rc);
			}
		}
		/* Wait for our friends to finish their page-ins which we
		 *  are also blocking on.
		 */
		for (k=0; k < dynarray_len(f->oftfill_inprog); k++) {
			struct offtree_iov  *v = dynarray_getpos(k);
			struct offtree_memb *m = v->oftiov_memb;
			
			w = &m->oft_waitq;
		retry:
			spinlock(&m->oft_lock);
			/* Has the other thread finished working on this?
			 */
			if (!ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY)) {
				psc_waitq_wait(w, &m->oft_lock);
				goto retry;
			} else
				freelock(&m->oft_lock);
		}
	}
	return (rc);
}

/**
 * msl_io - I/O gateway routine which bridges FUSE and the slash2 client cache and backend.  msl_io() handles the creation of offtree_req's and the loading of bmaps (which are attached to the file's fcache_memb_handle and is ultimately responsible for data being prefetched (as needed), copied into or from the cache, and (on write) being pushed to the correct io server.
 * @fh: file handle structure passed to us by FUSE which contains the pointer to our fcache_memb_handle *.
 * @buf: the application source/dest buffer.
 * @size: size of buffer.
 * @off: file logical offset similar to pwrite().
 * @op: the operation type (MSL_READ or MSL_WRITE).
 */
int
msl_io(struct fhent *fh, char *buf, size_t size, off_t off, int op)
{
	struct fcache_memb_handle *f;
	struct offtree_req        *r=NULL;
	struct bmap_cache_memb     t, *b;
	sl_blkno_t                 s, e;
	size_t                     tlen;
	off_t                      roff;
	int i,j, rc,n=0;

	f = fh->fh_pri;	
	psc_assert(f);
	/* Are these bytes in the cache?
	 *  Get the start and end block regions from the input parameters.
	 */
	s = off / f->fcmh_bmap_sz;
	e = (off + size) / f->fcmh_bmap_sz;	
	/* Relativize the length and offset.
	 */
	roff  = off - (s * SLASH_BMAP_SIZE);
	tlen  = MIN((SLASH_BMAP_SIZE - roff), size);
	/* Foreach block range, get its bmap and make a request into its 
	 *  offtree.  This first loop retrieves all the pages.
	 */
	for (i=0; s < e; s++, i++) {
		/* Load up the bmap, if it's not available then we're out of 
		 *  luck because we have no idea where the data is!
		 */
		b = msl_bmap_load(f, s, (i ? 0 : (e-s)), (op == MSL_READ) ?
				  SRCI_BMAP_READ : SRCI_BMAP_WRITE);
		if (!b)
			return -EIO;
		/* Malloc offtree request and pass to the initializer.
		 */
		r = realloc(r, (sizeof(*r)) * i);

		msl_oftrq_build(r[i], b, fh->id, roff, tlen, (op == MSL_READ) ?
                                OFTREQ_OP_READ : OFTREQ_OP_WRITE);

		/* Retrieve offtree region.
		 */
		if ((rc = offtree_region_preprw(r))) {
			psc_error("offtree_region_preprw rc=%d", rc);
			return (rc);
		}
		roff += tlen;
		size -= tlen;		
		tlen  = MIN(SLASH_BMAP_SIZE, size);
	}
	/* Now iterate across the array of completed offtree requests
	 *   paging in data where needed.
	 */
	if (op == MSL_READ || (ATTR_SET(r->oftrq_op, OFTREQ_OP_PRFFP) ||
			       ATTR_SET(r->oftrq_op, OFTREQ_OP_PRFLP)))
		if ((rc = msl_pages_blocking_load(r, i)))
			return (rc);

	if (op == MSL_READ)
		/* Copying into the application buffer, and managing 
		 *  the offtree.
		 */
		tlen = msl_pages_copyout(r, i, buf, (size_t)size, off);
	} else
		/* Copy pages into the system cache and queue them for
		 *  xfer to the IOS.
		 */
		tlen = msl_pages_copyin(r, i, buf, (size_t)size, off);

	psc_assert(tlen == size);

	for (j=0; j < i; j++)
		msl_oftrq_destroy(r[j]);
       
	return ((int)size);
}
