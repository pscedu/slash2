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

__static void
msl_oftrq_build(struct offtree_req *r, struct bmap_cache_memb *b, 
		off_t off, size_t len, int op)
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
	r->oftrq_width  = r->oftrq_depth = 0;
	r->oftrq_op     = op;
	r->oftrq_off    = off & SLASH_BMAP_BLKMASK;
	r->oftrq_nblks  = (off + len) / SLASH_BMAP_BLKSZ + 
		(((off + len) % SLASH_BMAP_BLKSZ) ? 1 : 0);
	
	DEBUG_OFFTREQ(PLL_TRACE, r, "newly built request");
}

__static void
msl_fcm_get(struct fhent *fh)
	struct fcache_memb_handle *f = sl_fidc_get(&fidcFreeList);

	fidcache_memb_init(f);
	/* Incref so that this inode is not immediately 
	 *  considered for reaping.
	 */
	fcmh_incref(fcmh);
	/* Cross-associate the fcmh and fhent structures.
	 */
	fcmh->fcmh_fh = fh->fh;	
	fh->fh_pri    = fcmh;

	return (fcmh);
}

#define BMAP_MAX_GET 63
/**
 * msl_bmap_fetch - perform a blocking 'get' operation to retrieve one or more bmaps from the MDS.
 * @f: pointer to the fid cache structure to which this bmap belongs.
 * @b: the block id to retrieve (block size == SLASH_BMAP_SIZE).
 * @n: the number of bmaps to retrieve (serves as a simple read-ahead mechanism)
 */
__static int 
msl_bmap_fetch(struct fidcache_memb_handle *f, sl_blkno_t b, size_t n) 
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
			psc_errorx("MDS returned more bmaps than"
				   " expected! mp->nblks(%zu) > n(%zu)", 
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

__static struct bmap_cache_memb *
msl_bmap_load(struct fcache_memb_handle *f, sl_blkno_t n, int prefetch)
{
	struct bmap_cache_memb t, *b;
	int rc=0;

	t.bcm_blkno=n;

	b = SPLAY_FIND(bmap_cache, &f->fcmh_bmap_cache, &t);
	if (!b) {
		/* Retrieve the bmap from the sl_mds.
		 */			
		rc = msl_bmap_fetch(f, t.bcm_blkno, prefetch);
		if (rc)
			return NULL;
	}
	return (SPLAY_FIND(bmap_cache, &f->fcmh_bmap_cache, &t));
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
 * msl_bmap_to_import - Given a bmap, perform a series of lookups to locate the ION import.  The ION was chosen by the mds and returned in the msl_bmap_fetch routine. msl_bmap_to_import queries the configuration to find the ION's private info - this is where the import pointer is kept.  If no import has yet been allocated a new is made.
 * @b: the bmap
 * 
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

int
msl_read_cb(struct pscrpc_request *rq, void *arg, int status)
{
	struct pscrpc_async_args *args = arg;
	struct dynarray     *a = arg->pointer_arg[MSL_READ_CB_POINTER_SLOT];
	struct offtree_iov  *v;
	struct offtree_memb *m;
	int i, n=dynarray_len(a);

	if (status) {
                DEBUG_REQ(PLL_ERROR, rq, "non-zero status status %d, "
                          "rq_status %d", status, rq->rq_status);
		psc_fatalx("Resolve issues surrounding this failure");
		// Freeing of dynarray, offtree state, etc
                return (status);
        }

	for (i=0; i < n; i++) {
		v = dynarray_getpos(i);
		m = (struct offtree_memb *)v->oftiov_memb;
		/* Take the lock and do some sanity checks.
		 */
		spinlock(&m->oft_lock);
		oftm_read_prepped_verify(m);
		psc_assert(!ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG));
		psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING));
		ATTR_UNSET(v->oftiov_flags, OFTIOV_FAULTING);
		/* Set datardy but leave OFT_READPNDG (in the oftm) until 
		 *   the memcpy -> application buffer has taken place.
		 */ 
		ATTR_SET(v->oftiov_flags, OFTIOV_DATARDY);
		DEBUG_OFFTIOV(PLL_TRACE, v, "OFTIOV_DATARDY");
		//ATTR_UNSET(m->oft_flags, OFT_READPNDG);
		//atomic_dec(&m->oft_op_ref);
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
 */
__static void
msl_pagereq_finalize(struct offtree_req *r, struct dynarray *a)
{
	struct pscrpc_import      *imp;
	struct pscrpc_request_set *rqset;
	struct pscrpc_request     *req;
        struct pscrpc_bulk_desc   *desc;
	struct bmap_cache_memb    *bcm;
	struct iovec              *iovs;
	int    i, n=dynarray_len(a);

	psc_assert(n);	
	psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG));
	ATTR_SET(v->oftiov_flags, OFTIOV_FAULTING));
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

	if ((rc = rsx_newreq(imp, SRCI_VERSION, SRMT_READ
			     sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0) {
                errno = -rc;
		psc_fatalx("rsx_newreq() bad time to fail :(");
        }
	/* Setup the callback, supplying the dynarray as a argument.
	 */
	req->rq_interpret_reply = msl_read_cb;	
        req->rq_async_args.pointer_arg[MSL_READ_CB_POINTER_SLOT] = a;
	/* Prep the iovs and bulk descriptor
	 */
	iovs = PSCALLOC(sizeof(*iovs) * n);
	for (i=0; i < n; i++)
		memcpy(iovs[0], dynarray_getpos(i), sizeof(struct iovec));

	rsx_bulkclient(req, &desc, BULK_PUT_SINK, SRM_BULK_PORTAL, iovs, n);
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

__static inline size_t
msl_coalesce_buf_tally(struct dynarray *a)
{
	struct offtree_iov *v;
	int    n = dynarray_len(a), i;
	size_t total=0;

	for (i=0; i < n; i++) {
		v = dynarray_getpos(a, i);
		total += v->oftiov_nblks;
	}
	return total;
}

/**
 * msl_pages_prefetch - given an filled offtree request, check its iov's for unfaulted pages and initiate the prefect for those pages.  State will be saved in the request's offtree_fill structure.
 * @r: the request.
 */
__static void
msl_pages_prefetch(struct offtree_req *r)
{	
	struct offtree_iov  *v;
	struct offtree_memb *m;
	struct dynarray     *a=r->oftrq_darray, *coalesce=NULL;
	off_t                o=r->oftrq_off;
	size_t               niovs=dynarray_len(a);
	ssize_t              nblks=0;
	u32                  i, j, n;
	int                  rc=0, nc_blks=0;

#define launch_cb {					\
		psc_assert(coalesce);			\
		msl_pagereq_finalize(r, coalesce);	\
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
		/* The dynarray may contain iov's that we don't need, 
		 *  skip them.
		 */
		if (o > OFT_IOV2E_OFF_(v))
			continue;
		/* Map the offtree leaf holding this iov.
		 */
		m = (struct offtree_memb *)v->oftiov_memb;
		DEBUG_OFFTIOV(PLL_INFO, v, "processing..");
		DEBUG_OFT(PLL_INFO, m, "..processing");
		/* Ensure the offtree leaf is sane.
		 */
		oftm_read_prepped_verify(m);
		
		nblks += v->oftiov_nblks;
		/* Skip iov's which have already been queued for 
		 *  retrieval but track them.  Take the lock here
		 *  to serialize access to the iov flags.
		 */
		spinlock(&m->oft_lock);
		/* oftiov_nblks cannot be bigger than slCacheNblks!
		 */
		psc_assert(v->oftiov_nblks <= slCacheNblks);
		psc_assert(nc_blks < slCacheNblks);
		/* Iov's cannot be split across requests but they may be 
		 *  coalesced into one request.  We're assured that the 
		 *  largest contiguous piece of memory is <= to LNET_MTU.
		 */
		if (!ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY)   &&
		    !ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG) &&
		    !ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING)) {
			/* Mark it.
			 */
			ATTR_SET(v->oftiov_flags, OFTIOV_FAULTPNDG);

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
				 *  is.
				 * Launch the current coalesce buffer (which
				 *  must exist). 
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
			if (coalesce)
				launch_cb;

			if (ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTPNDG) ||
			    ATTR_TEST(v->oftiov_flags, OFTIOV_FAULTING)) { 
				/* This would be a problem..
				 */
				psc_assert(!ATTR_TEST(v->oftiov_flags, 
						      OFTIOV_DATARDY));
				/* There must be at least 2 pending operations
				 *  (ours plus the one who set OFTIOV_FAULT*)
				 */
				psc_assert(atomic_read(m->oft_op_ref) > 1);
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
			} else
				psc_assert(ATTR_TEST(v->oftiov_flags, 
						     OFTIOV_DATARDY));
		}
		/* Finished testing / setting attrs, release the lock.
		 */
		freelock(&m->oft_lock);	
	}
	/* There may be a cb lingering.
	 */
	if (coalesce)
		lauch_cb;

	/* Verify that the number of blks matches that of the request.
	 */	
	//	if (nblks != req->oftrq_nblks)
	//	psc_fatalx("nbufs=%zu != oftrq_nblks=%zu",
	//		   nblks, req->oftrq_nblks);
}

/**
 * msl_pages_copyout - copy pages to the user application buffer.
 *
 */
__static size_t
msl_pages_copyout(struct offtree_req *r, int n, char *buf, 
		  size_t size, off_t off)
{
	struct dynarray     *a;
	struct offtree_iov  *v;
	struct offtree_memb *m;
	int   l, i, j;
	off_t o, t;

	/* Relative offset into the buffer.
	 */
	psc_assert(r[0].oftrq_off <= off);
	t = off - r[0].oftrq_off;
	
	for (i=0; i < n; i++) {
		a = r[i].oftrq_darray;
		l = dynarray_len(a);
		for (j=0; j < l; j++) {
			v = dynarray_getpos(j);
			/* These pages aren't involved, skip.
			 */
			if (off > OFT_IOV2E_OFF_(v))
				continue;
			/* Assert that the pages are kosher for copying.
			 */
			psc_assert(ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY));
			
			m = (struct offtree_memb *)v->oftiov_memb;
			oftm_read_prepped_verify(m);
			
			if (!i && !j)
				;
		}				
	}	
}

int
msl_read(struct fhent *fh, char *buf, size_t size, off_t off)
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
		b = msl_bmap_load(f, s, (i ? 0 : (e-s)));
		if (!b)
			return -EIO;
		/* Malloc offtree request and pass to the initializer.
		 */
		r = realloc(r, (sizeof(*r)) * i);
		msl_oftrq_build(r[i], b, roff, tlen, OFTREQ_OP_READ);
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
	for (j=0; j < i; j++)
		msl_pages_prefetch(r[j]);
	/* Iterate again, this time checking / waiting for all page-ins to 
	 *   complete.  Note that msl_read_cb sets OFTIOV_DATARDY.
	 */
	for (j=0; j < i; j++) {
		struct offtree_fill *f = r[j].oftrq_fill;
		psc_waitq_t *w;
		int          k;

		/* If a request set is present then block on its completion.
		 */
		if (f->oftfill_reqset) {
			if ((rc = pscrpc_set_wait(f->oftfill_reqset))) {
				psc_error("pscrpc_set_wait rc=%d", rc);;
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
			spinlock(&m->oft_lock);
			/* Has the other thread finished working on this?
			 */
			if (!ATTR_TEST(v->oftiov_flags, OFTIOV_DATARDY))
				psc_waitq_wait(w, &m->oft_lock);
			else
				freelock(&m->oft_lock);
		}
	}
	/*   Copying into the application buffer, and managing the offtree.
	 */
	tlen = msl_pages_copyout(r, i, buf, size, off);

	psc_assert(tlen == size);
	return ((int)size);
}
