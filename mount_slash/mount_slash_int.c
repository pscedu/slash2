/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/log.h"
#include "psc_util/random.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "bmpc.h"
#include "buffer.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"

/* async RPC pointers */
#define MSL_CB_POINTER_SLOT_BMPCES	0
#define MSL_CB_POINTER_SLOT_CSVC	1
#define MSL_CB_POINTER_SLOT_BIORQ	2
#define MSL_CB_POINTER_SLOT_BIORQS	3

/* Flushing fs threads wait here for I/O completion. */
struct psc_waitq msl_fhent_flush_waitq = PSC_WAITQ_INIT;

struct timespec msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

extern struct psc_waitq bmapflushwaitq;

__static int
msl_biorq_cmp(const void *x, const void *y)
{
	const struct bmpc_ioreq * a = x;
	const struct bmpc_ioreq * b = y;

	//DEBUG_BIORQ(PLL_TRACE, a, "compare..");
	//DEBUG_BIORQ(PLL_TRACE, b, "..compare");

	if (a->biorq_off == b->biorq_off)
		/* Larger requests with the same start offset should have
		 *   ordering priority.
		 */
		return (CMP(b->biorq_len, a->biorq_len));
	return (CMP(a->biorq_off, b->biorq_off));
}

/* msl_biorq_build -
 * Notes: roff is bmap aligned.
 */

#define MS_DEF_READAHEAD_PAGES 8

__static void
msl_biorq_build(struct bmpc_ioreq **newreq, struct bmapc_memb *b,
    struct msl_fhent *mfh, uint32_t roff, uint32_t len, int op)
{
	struct bmpc_ioreq *r;
	struct bmap_pagecache *bmpc;
	struct bmap_pagecache_entry *bmpce, bmpce_search, *bmpce_new;
	uint32_t aoff = (roff & ~BMPC_BUFMASK); /* aligned, relative offset */
	uint32_t alen = len + (roff & BMPC_BUFMASK);
	uint64_t foff = roff + bmap_foff(b); /* filewise offset */
	int i, npages = 0, rbw = 0;

	DEBUG_BMAP(PLL_INFO, b,
	    "adding req for (off=%u) (size=%u)", roff, len);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
	    "adding req for (off=%u) (size=%u)", roff, len);

	psc_assert(len);
	psc_assert((roff + len) <= SLASH_BMAP_SIZE);
	psc_assert(op == BIORQ_WRITE || op == BIORQ_READ);
	*newreq = r = PSCALLOC(sizeof(struct bmpc_ioreq));

	bmpc_ioreq_init(r, roff, len, op, b, mfh);

	/* O_APPEND must be sent be sent via directio
	 */
	//if (mfh->mfh_oflags & O_APPEND)
	//	r->biorq_flags |= BIORQ_APPEND | BIORQ_DIO;

	/* Take a ref on the bmap now so that it won't go away before
	 *   pndg IO's complete.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

	if (r->biorq_flags & BIORQ_DIO)
		/* The bmap is set to use directio, we may then skip
		 *   cache preparation.
		 */
		goto out;

	bmpc = bmap_2_bmpc(b);
	/* How many pages are needed to accommodate the request?
	 *   Determine and record whether RBW (read-before-write)
	 *   operations are needed on the first or last pages.
	 */
	if (roff & BMPC_BUFMASK && op == BIORQ_WRITE)
		rbw = BIORQ_RBWFP;

	npages = alen / BMPC_BUFSZ;

	if (alen % BMPC_BUFSZ) {
		npages++;
		if (op == BIORQ_WRITE) {
			if (npages == 1 && !rbw)
				rbw = BIORQ_RBWFP;
			else if (npages > 1)
				rbw |= BIORQ_RBWLP;
		}
	}

	psc_assert(npages <= BMPC_IOMAXBLKS);

	/* Lock the bmap's page cache and try to locate cached pages
	 *   which correspond to this request.
	 */
	i = 0;
	bmpce_new = NULL;
	BMPC_LOCK(bmpc);
	while (i < npages) {
		bmpce_search.bmpce_off = aoff + (i * BMPC_BUFSZ);
		bmpce = SPLAY_FIND(bmap_pagecachetree, &bmpc->bmpc_tree,
		    &bmpce_search);
		if (!bmpce) {
			if (bmpce_new == NULL) {
				BMPC_ULOCK(bmpc);
				bmpce_new = psc_pool_get(bmpcePoolMgr);
				BMPC_LOCK(bmpc);
				continue;
			}
			bmpce = bmpce_new;
			bmpce_new = NULL;
			bmpce_useprep(bmpce, r);
			bmpce->bmpce_off = aoff + (i * BMPC_BUFSZ);
			SPLAY_INSERT(bmap_pagecachetree,
			    &bmpc->bmpc_tree, bmpce);
		}

		BMPCE_LOCK(bmpce);
		/* Increment the ref cnt via the lru mgmt
		 *   function.
		 */
		bmpce_handle_lru_locked(bmpce, bmpc, op, 1);

		BMPCE_ULOCK(bmpce);
		psc_dynarray_add(&r->biorq_pages, bmpce);
		i++;
	}
	BMPC_ULOCK(bmpc);
	if (unlikely(bmpce_new))
		psc_pool_return(bmpcePoolMgr, bmpce_new);

	psc_assert(psc_dynarray_len(&r->biorq_pages) == npages);

	/* Pass1: Retrieve memory pages from the cache on behalf of our pages
	 *   stuck in GETBUF.
	 */
	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		/* Only this thread may assign a buffer to the bmpce.
		 */
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    (bmpce->bmpce_flags & BMPCE_GETBUF)) {
			void *tmp;
			uint64_t fsz  = fcmh_getsize(mfh->mfh_fcmh);
			uint32_t rfsz = fsz - bmap_foff(b);

			/* Increase the rdref cnt in preparation for any
			 *   RBW ops but only on new pages owned by this
			 *   page cache entry.  For now bypass
			 *   bmpce_handle_lru_locked() for this op.
			 */
			if (!i && (rbw & BIORQ_RBWFP) &&
			    (fsz > foff ||
			     /* If file ends in this page then fetch */
			     (rfsz > bmpce->bmpce_off &&
			      rfsz < bmpce->bmpce_off + BMPC_BLKSZ))) {
				    bmpce->bmpce_flags |= BMPCE_RBWPAGE;
				    psc_atomic16_inc(&bmpce->bmpce_rdref);
				    r->biorq_flags |= BIORQ_RBWFP;

			} else if ((i == (npages - 1) &&
				    (rbw & BIORQ_RBWLP)) &&
				   (fsz > (foff + len) ||
				    (rfsz > bmpce->bmpce_off &&
				     rfsz < bmpce->bmpce_off + BMPC_BLKSZ))) {
				bmpce->bmpce_flags |= BMPCE_RBWPAGE;
				psc_atomic16_inc(&bmpce->bmpce_rdref);
				r->biorq_flags |= BIORQ_RBWLP;
			}

			psc_assert(!bmpce->bmpce_base);
			BMPCE_ULOCK(bmpce);

			tmp = bmpc_alloc();

			BMPCE_LOCK(bmpce);
			psc_assert(bmpce->bmpce_flags & BMPCE_GETBUF);
			bmpce->bmpce_base = tmp;
			bmpce->bmpce_flags &= ~BMPCE_GETBUF;
			psc_waitq_wakeall(bmpce->bmpce_waitq);
		}
		BMPCE_ULOCK(bmpce);
	}

	/* Pass2: Sanity Check
	 */
	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(bmpce);
		psc_assert(bmpce->bmpce_off == aoff + (i * BMPC_BUFSZ));

		if (op == BIORQ_WRITE)
			psc_assert(psc_atomic16_read(&bmpce->bmpce_wrref) > 0);
		else
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) > 0);
		if (biorq_is_my_bmpce(r, bmpce)) {
			/* The page is my reponsibility, ensure a cache
			 *   block has been assigned.
			 */
			psc_assert(bmpce->bmpce_base);
			psc_assert(bmpce->bmpce_flags & BMPCE_INIT);
			bmpce->bmpce_flags &= ~BMPCE_INIT;

			if (op == BIORQ_READ)
				bmpce->bmpce_flags |= BMPCE_READPNDG;
		}
		BMPCE_ULOCK(bmpce);
	}
 out:
	DEBUG_BIORQ(PLL_NOTIFY, r, "new req");
	if (op == BIORQ_READ || (r->biorq_flags & BIORQ_DIO))
		pll_add(&bmap_2_bmpc(b)->bmpc_pndg_biorqs, r);
	else
		pll_add_sorted(&bmap_2_bmpc(b)->bmpc_new_biorqs, r, msl_biorq_cmp);
}

__static void
bmap_biorq_del(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);

	/* The request must be attached to the bmpc.
	 */
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_WRITE && !(r->biorq_flags & BIORQ_DIO)) {
		atomic_dec(&bmpc->bmpc_pndgwr);
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) >= 0);

		if (atomic_read(&bmpc->bmpc_pndgwr))
			psc_assert(b->bcm_flags & BMAP_DIRTY);

		else {
			b->bcm_flags &= ~BMAP_DIRTY;
			/* Don't assert pll_empty(&bmpc->bmpc_pndg)
			 *   since read requests may be present.
			 */
			DEBUG_BMAP(PLL_INFO, b, "unset DIRTY nitems_pndg(%d)",
			   pll_nitems(&bmpc->bmpc_pndg_biorqs));
		}
	}

	if (!(b->bcm_flags & (BMAP_CLI_FLUSHPROC|BMAP_TIMEOQ)) &&
	    (!bmpc_queued_ios(bmpc))) {
		psc_assert(!atomic_read(&bmpc->bmpc_pndgwr));
		b->bcm_flags |= BMAP_TIMEOQ;
		lc_addtail(&bmapTimeoutQ, b);
	}

	BMPC_ULOCK(bmpc);
	DEBUG_BMAP(PLL_INFO, b, "remove biorq=%p nitems_pndg(%d)",
		   r, pll_nitems(&bmpc->bmpc_pndg_biorqs));

	bmap_op_done_type(b, BMAP_OPCNT_BIORQ);
}

__static void
msl_biorq_unref(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(r->biorq_bmap);
	int i;

	psc_assert(r->biorq_flags & BIORQ_DESTROY);
	psc_assert(!(r->biorq_flags & BIORQ_INFL));

	BMPC_LOCK(bmpc);
	for (i = 0; i < psc_dynarray_len(&r->biorq_pages); i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);

		bmpce_handle_lru_locked(bmpce, bmpc,
		 (r->biorq_flags & BIORQ_WRITE) ? BIORQ_WRITE : BIORQ_READ, 0);

		BMPCE_ULOCK(bmpce);
	}
	BMPC_ULOCK(bmpc);
}

__static void
msl_biorq_destroy(struct bmpc_ioreq *r)
{
	struct msl_fhent *f = r->biorq_fhent;
#if FHENT_EARLY_RELEASE
	int fhent = 1;
#endif

	spinlock(&r->biorq_lock);

	/* Reads req's have their BIORQ_SCHED and BIORQ_INFL flags
	 *    cleared in msl_read_cb to unblock waiting
	 *    threads at the earliest possible moment.
	 */
	if (!(r->biorq_flags & BIORQ_DIO)) {
		if (r->biorq_flags & BIORQ_WRITE) {
			psc_assert(r->biorq_flags & BIORQ_INFL);
			psc_assert(r->biorq_flags & BIORQ_SCHED);
			r->biorq_flags &= ~(BIORQ_INFL|BIORQ_SCHED);
		} else {
			psc_assert(!(r->biorq_flags & BIORQ_INFL));
			psc_assert(!(r->biorq_flags & BIORQ_SCHED));
		}
	}

	r->biorq_flags |= BIORQ_DESTROY;

#if FHENT_EARLY_RELEASE
	if (r->biorq_flags & BIORQ_NOFHENT)
		fhent = 0;
#endif

	freelock(&r->biorq_lock);

	DEBUG_BIORQ(PLL_INFO, r, "destroying (nwaiters=%d)",
		    atomic_read(&r->biorq_waitq.wq_nwaiters));

	/* One last shot to wakeup any blocked threads.
	 */
	while (atomic_read(&r->biorq_waitq.wq_nwaiters)) {
		psc_waitq_wakeall(&r->biorq_waitq);
		sched_yield();
	}

	msl_biorq_unref(r);
	bmap_biorq_del(r);

#if FHENT_EARLY_RELEASE
	if (fhent) {
		spinlock(&f->mfh_lock);
		pll_remove(&f->mfh_biorqs, r);
		psc_waitq_wakeall(&msl_fhent_flush_waitq);
		freelock(&f->mfh_lock);
	}
#else
	spinlock(&f->mfh_lock);
	pll_remove(&f->mfh_biorqs, r);
	psc_waitq_wakeall(&msl_fhent_flush_waitq);
	freelock(&f->mfh_lock);
#endif

	psc_dynarray_free(&r->biorq_pages);

	if (r->biorq_rqset)
		pscrpc_set_destroy(r->biorq_rqset); /* XXX assert(#elem == 1) */

	psc_assert(!atomic_read(&r->biorq_waitq.wq_nwaiters));

	PSCFREE(r);
}

struct msl_fhent *
msl_fhent_new(struct fidc_membh *f)
{
	struct msl_fhent *mfh;

	fcmh_op_start_type(f, FCMH_OPCNT_OPEN);

	mfh = PSCALLOC(sizeof(*mfh));
	mfh->mfh_fcmh = f;
	INIT_SPINLOCK(&mfh->mfh_lock);
	pll_init(&mfh->mfh_biorqs, struct bmpc_ioreq, biorq_mfh_lentry,
	    &mfh->mfh_lock);
	return (mfh);
}

/**
 * bmapc_memb_init - initialize a bmap structure.
 * @b: the bmap struct
 */
void
msl_bmap_init(struct bmapc_memb *b)
{
	struct bmap_cli_info *bci;

	bci = bmap_2_bci(b);
	bmpc_init(&bci->bci_bmpc);
}

void
bmap_biorq_expire(struct bmapc_memb *b)
{
	struct bmpc_ioreq *biorq;

	/*
	 * Note that the following two lists and the bmapc_memb
	 * structure itself all share the same lock.
	 */
	BMPC_LOCK(bmap_2_bmpc(b));
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_new_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_pndg_biorqs) {
		spinlock(&biorq->biorq_lock);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		freelock(&biorq->biorq_lock);
	}
	BMPC_ULOCK(bmap_2_bmpc(b));

	BMAP_LOCK(b);
	/* Minimize biorq scanning via this hint.
	 */
	b->bcm_flags |= BMAP_CLI_BIORQEXPIRE;
	BMAP_ULOCK(b);

	psc_waitq_wakeall(&bmapflushwaitq);
}

void
bmap_biorq_waitempty(struct bmapc_memb *b)
{
	BMAP_LOCK(b);
	bcm_wait_locked(b, (!pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs) ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs)  ||
			    (b->bcm_flags & BMAP_CLI_FLUSHPROC)));

	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs));
	BMAP_ULOCK(b);
}

/**
 * msl_bmap_cache_rls - called from rcm.c (SRMT_BMAPDIO).
 * @b:  the bmap whose cached pages should be released.
 */
void
msl_bmap_cache_rls(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	//psc_assert(b->bcm_flags & BMAP_DIO);

	bmap_biorq_expire(b);
	bmap_biorq_waitempty(b);

	bmpc_lru_del(bmpc);

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}

void
msl_bmap_final_cleanup(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	bmap_biorq_waitempty(b);

	/* Mind lock ordering, remove from LRU first.
	 */
	bmpc_lru_del(bmpc);

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAP_CLOSING);
	psc_assert(!(b->bcm_flags & BMAP_DIRTY));
	/* Assert that this bmap can no longer be scheduled by the
	 *   write back cache thread.
	 */
	psc_assert(psclist_disjoint(&b->bcm_lentry));
	/* Assert that this thread cannot be seen by the page cache
	 *   reaper (it was lc_remove'd above by bmpc_lru_del()).
	 */
	psc_assert(psclist_disjoint(&bmpc->bmpc_lentry));
	BMAP_ULOCK(b);

	DEBUG_BMAP(PLL_INFO, b, "freeing");

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}

void
msl_bmap_reap_init(struct bmapc_memb *bmap, const struct srt_bmapdesc *sbd)
{
	struct bmap_cli_info *bci = bmap_2_bci(bmap);
	int locked;

	psc_assert(!pfl_memchk(sbd, 0, sizeof(*sbd)));

	locked = BMAP_RLOCK(bmap);

	bci->bci_sbd = *sbd;
	/* Record the start time,
	 *  XXX the directio status of the bmap needs to be returned by the
	 *     mds so we can set the proper expiration time.
	 */
	PFL_GETTIMESPEC(&bci->bci_xtime);

	timespecadd(&bci->bci_xtime, &msl_bmap_timeo_inc,
	    &bci->bci_etime);
	timespecadd(&bci->bci_xtime, &msl_bmap_max_lease,
	    &bci->bci_xtime);

	/* Take the reaper ref cnt early and place the bmap
	 *    onto the reap list
	 */
	bmap->bcm_flags |= BMAP_TIMEOQ;
	bmap_op_start_type(bmap, BMAP_OPCNT_REAPER);

	BMAP_URLOCK(bmap, locked);
	/* Add ourselves here, otherwise zero length files
	 *   will not be removed.
	 */
	lc_addtail(&bmapTimeoutQ, bmap);
}

/**
 * msl_bmap_retrieve - Perform a blocking 'LEASEBMAP' operation to
 *	retrieve one or more bmaps from the MDS.
 * @f: pointer to the fid cache structure to which this bmap belongs.
 * @b: the block id to retrieve (block size == SLASH_BMAP_SIZE).
 * @n: the number of bmaps to retrieve (serves as a simple read-ahead mechanism)
 */
int
msl_bmap_retrieve(struct bmapc_memb *bmap, enum rw rw)
{
	int rc, nretries = 0, getreptbl = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_leasebmap_req *mq;
	struct srm_leasebmap_rep *mp;
	struct fcmh_cli_info *fci;
	struct bmap_cli_info *bci;
	struct fidc_membh *f;

	psc_assert(bmap->bcm_flags & BMAP_INIT);
	psc_assert(bmap->bcm_fcmh);

	f = bmap->bcm_fcmh;
	fci = fcmh_2_fci(f);

 retry:
	FCMH_LOCK(f);
	if ((f->fcmh_flags & (FCMH_CLI_HAVEREPLTBL |
	    FCMH_CLI_FETCHREPLTBL)) == 0) {
		f->fcmh_flags |= FCMH_CLI_FETCHREPLTBL;
		getreptbl = 1;
	}
	FCMH_ULOCK(f);

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAP, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = f->fcmh_fg;
	mq->prefios = prefIOS; /* Tell MDS of our preferred ION */
	mq->bmapno = bmap->bcm_bmapno;
	mq->rw = rw;
	if (getreptbl)
		mq->flags |= SRM_LEASEBMAPF_GETREPLTBL;

	bci = bmap_2_bci(bmap);

	DEBUG_FCMH(PLL_INFO, f, "retrieving bmap (bmapno=%u) (rw=%d)",
	    bmap->bcm_bmapno, rw);

	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	if (rc)
		goto out;
	memcpy(&bmap->bcm_corestate, &mp->bcs, sizeof(mp->bcs));

	FCMH_LOCK(f);

	msl_bmap_reap_init(bmap, &mp->sbd);

	DEBUG_BMAP(PLL_INFO, bmap, "rw=%d nid=%#"PRIx64" bmapnid=%#"PRIx64,
	    rw, mp->sbd.sbd_ion_nid, bmap_2_ion(bmap));

	if (getreptbl) {
		/* XXX don't forget that on write we need to invalidate
		 *   the local replication table..
		 */
		fci->fci_nrepls = mp->nrepls;
		memcpy(&fci->fci_reptbl, &mp->reptbl,
		    sizeof(sl_replica_t) * SL_MAX_REPLICAS);
		f->fcmh_flags |= FCMH_CLI_HAVEREPLTBL;
		psc_waitq_wakeall(&f->fcmh_waitq);
	}

 out:
	FCMH_RLOCK(f);
	if (getreptbl)
		f->fcmh_flags &= ~FCMH_CLI_FETCHREPLTBL;
	FCMH_ULOCK(f);
	if (rq) {
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc == SLERR_BMAP_DIOWAIT) {
		/* Retry for bmap to be DIO ready.
		 */
		DEBUG_BMAP(PLL_WARN, bmap,
		    "SLERR_BMAP_DIOWAIT (rt=%d)", nretries);

		sleep(BMAP_CLI_DIOWAIT_SECS);
		if (nretries > BMAP_CLI_MAX_LEASE * 2)
			return (-ETIMEDOUT);
		goto retry;
	}

	return (rc);
}

/**
 * msl_bmap_modeset - Set READ or WRITE as access mode on an open file
 *	block map.
 * @b: bmap.
 * @rw: access mode to set the bmap to.
 *
 * XXX have this take a bmapc_memb.
 *
 * Notes:  XXX I think this logic can be simplified when setting mode
 *	from WRONLY to RDWR.  In WRONLY this client already knows the
 *	address of the only ION from which this bmap can be read.
 *	Therefore, it should be able to interface with that ION without
 *	intervention from the MDS.
 */
__static int
msl_bmap_modeset(struct bmapc_memb *b, enum rw rw)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	int rc, nretries = 0;

	psc_assert(rw == SL_WRITE || rw == SL_READ);
 retry:
	psc_assert(b->bcm_flags & BMAP_MDCHNG);

	if (b->bcm_flags & BMAP_WR)
		/* Write enabled bmaps are allowed to read with no
		 *   further action being taken.
		 */
		return (0);

	/* Add write mode to this bmap.
	 */
	psc_assert(rw == SL_WRITE && (b->bcm_flags & BMAP_RD));

	rc = slc_rmc_getimp(&csvc);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCHWRMODE, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(struct srt_bmapdesc));
	mq->prefios = prefIOS;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;

	if (rc == 0)
		memcpy(bmap_2_sbd(b), &mp->sbd,
		    sizeof(struct srt_bmapdesc));

 out:
	if (rq) {
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc == SLERR_BMAP_DIOWAIT) {
		DEBUG_BMAP(PLL_WARN, b, "SLERR_BMAP_DIOWAIT rt=%d", nretries);
		sleep(BMAP_CLI_DIOWAIT_SECS);
		if (nretries > (BMAP_CLI_MAX_LEASE * 2))
			return (-ETIMEDOUT);
		goto retry;
	}

	return (rc);
}

/**
 * msl_bmap_to_csvc - Given a bmap, perform a series of lookups to
 *	locate the ION csvc.  The ION was chosen by the mds and
 *	returned in the msl_bmap_retrieve routine. msl_bmap_to_csvc
 *	queries the configuration to find the ION's private info - this
 *	is where the import pointer is kept.  If no import has yet been
 *	allocated a new is made.
 * @b: the bmap
 * Notes: the bmap is locked to avoid race conditions with import checking.
 *	the bmap's refcnt must have been incremented so that it is not
 *	freed from under us.
 * XXX Dev Needed: If the bmap is a read-only then any replica may be
 *	accessed (so long as it is recent).  Therefore
 *	msl_bmap_to_csvc() should have logic to accommodate this.
 */
struct slashrpc_cservice *
msl_bmap_to_csvc(struct bmapc_memb *b, int exclusive)
{
	struct slashrpc_cservice *csvc;
	struct sl_resm *resm;
	int locked;

	/* Sanity check on the opcnt.
	 */
	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	locked = BMAP_RLOCK(b);
	resm = libsl_nid2resm(bmap_2_ion(b));
	psc_assert(resm->resm_nid == bmap_2_ion(b));
	BMAP_URLOCK(b, locked);

	if (exclusive) {
		csvc = slc_geticsvc(resm);
		if (csvc)
			psc_assert(csvc->csvc_import->imp_connection->
			    c_peer.nid == bmap_2_ion(b));
	} else {
#if 0
		/* grab a random resm from any replica */
		for (n = 0; n < resm->resm_res->res_nnids; n++)
			for (j = 0; j < resm->resm_res->res_nnids; j++) {
				csvc = slc_geticsvc(resm);
				if (csvc)
					return (csvc);
			}
#endif
		csvc = slc_geticsvc(resm);
	}
	return (csvc);
}

struct slashrpc_cservice *
msl_try_get_replica_resm(struct bmapc_memb *bcm, int iosidx)
{
	struct slashrpc_cservice *csvc;
	struct bmap_cli_info *bci;
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *resm;

	fci = fcmh_2_fci(bcm->bcm_fcmh);
	bci = bmap_2_bci(bcm);

	if (SL_REPL_GET_BMAP_IOS_STAT(bcm->bcm_repls,
	    iosidx * SL_BITS_PER_REPLICA) != BREPLST_VALID)
		return (NULL);

	res = libsl_id2res(fci->fci_reptbl[iosidx].bs_id);

#if 0
	FOREACH_RND(&it, psc_dynarray_len(&res->res_members)) {
		resm = psc_dynarray_getpos(&res->res_members, it.ri_rnd_idx);
		csvc = slc_geticsvc_nb(resm);
		if (csvc)
			return (csvc);
	}
#endif

	FOREACH_RND(&it, psc_dynarray_len(&res->res_members)) {
		resm = psc_dynarray_getpos(&res->res_members, it.ri_rnd_idx);
		csvc = slc_geticsvc(resm);
		if (csvc)
			return (csvc);
	}
	return (NULL);
}

struct slashrpc_cservice *
msl_bmap_choose_replica(struct bmapc_memb *b)
{
	struct slashrpc_cservice *csvc;
	struct fcmh_cli_info *fci;
	int n, rnd;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	fci = fcmh_get_pri(b->bcm_fcmh);

	/* first, try preferred IOS */
	rnd = psc_random32u(fci->fci_nrepls);
	for (n = 0; n < fci->fci_nrepls; n++, rnd++) {
		if (rnd >= fci->fci_nrepls)
			rnd = 0;

		if (fci->fci_reptbl[rnd].bs_id == prefIOS) {
			csvc = msl_try_get_replica_resm(b, rnd);
			if (csvc)
				return (csvc);
		}
	}

	/* rats, not available; try anyone available now */
	rnd = psc_random32u(fci->fci_nrepls);
	for (n = 0; n < fci->fci_nrepls; n++, rnd++) {
		if (rnd >= fci->fci_nrepls)
			rnd = 0;

		csvc = msl_try_get_replica_resm(b, rnd);
		if (csvc)
			return (csvc);
	}

	/*
	 * still nothing, go into multiwait, awaking periodically for
	 * cancelling or ceiling time out.
	 */
#if 0
	for (;;) {
		psc_multiwait_s(, 1);
	}
#endif
	return (NULL);
}

/**
 * msl_read_cb - RPC callback used only for read or RBW operations.
 *	The primary purpose is to set the bmpce's to DATARDY so that
 *	other threads waiting for DATARDY may be unblocked.
 *  Note: Unref of the biorq will happen after the pages have been
 *     copied out to the applicaton buffers.
 */
int
msl_read_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CB_POINTER_SLOT_CSVC];
	struct psc_dynarray *a = args->pointer_arg[MSL_CB_POINTER_SLOT_BMPCES];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CB_POINTER_SLOT_BIORQ];
	struct bmap_pagecache_entry *bmpce;
	struct bmapc_memb *b;
	int clearpages = 0, op = rq->rq_reqmsg->opc, i, rc;

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		goto out;

	b = r->biorq_bmap;
	psc_assert(b);

	psc_assert(op == SRMT_READ);
	psc_assert(a);

	DEBUG_BMAP(PLL_INFO, b, "callback");
	DEBUG_BIORQ(PLL_INFO, r, "callback bmap=%p", b);

	if (rq->rq_status) {
		DEBUG_REQ(PLL_ERROR, rq, "non-zero status %d",
		    rq->rq_status);
		rc = rq->rq_status;
		goto out;
	}

	spinlock(&r->biorq_lock);
	psc_assert(r->biorq_flags & BIORQ_SCHED);
	psc_assert(r->biorq_flags & BIORQ_INFL);

	/* Call the inflight CB only on the iov's in the dynarray -
	 *   not the iov's in the request since some of those may
	 *   have already been staged in.
	 */
	for (i = 0; i < psc_dynarray_len(a); i++) {
		bmpce = psc_dynarray_getpos(a, i);
		BMPCE_LOCK(bmpce);

		psc_assert(bmpce->bmpce_waitq);
		psc_assert(biorq_is_my_bmpce(r, bmpce));

		if (bmpce->bmpce_flags & BMPCE_RBWPAGE) {
			/* The RBW stuff needs to be managed outside of
			 *   the LRU; this is not the best place but should
			 *   suffice for now.
			 */
			psc_assert(psc_atomic16_read(&bmpce->bmpce_rdref) == 1);
			psc_atomic16_dec(&bmpce->bmpce_rdref);
			bmpce->bmpce_flags |= BMPCE_RBWRDY;
			DEBUG_BMPCE(PLL_INFO, bmpce, "infl dec for RBW, DATARDY not set");
		} else {
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			DEBUG_BMPCE(PLL_INFO, bmpce, "datardy via read_cb");
			/* Disown bmpce by null'ing the waitq pointer.
			 */
			bmpce->bmpce_waitq = NULL;
		}

		if (clearpages) {
			DEBUG_BMPCE(PLL_WARN, bmpce, "clearing page");
			memset(bmpce->bmpce_base, 0, BMPC_BUFSZ);
		}
		BMPCE_ULOCK(bmpce);
	}
	freelock(&r->biorq_lock);

	/* Free the dynarray which was allocated in msl_read_rpc_create().
	 */
	psc_dynarray_free(a);
	PSCFREE(a);

 out:
	sl_csvc_decref(csvc);
	return (rc);
}

int
msl_io_rpcset_cb(__unusedx struct pscrpc_request_set *set, void *arg, int rc)
{
	struct psc_dynarray *biorqs = arg;
	struct bmpc_ioreq *r;
	int i;

	if (rc) {
		DYNARRAY_FOREACH(r, i, biorqs)
			;
		return (rc);
	}

	DYNARRAY_FOREACH(r, i, biorqs)
		msl_biorq_destroy(r);
	psc_dynarray_free(biorqs);
	psc_free(biorqs, 0);
	return (rc);
}

int
msl_io_rpc_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct psc_dynarray *biorqs = args->pointer_arg[1];
	struct bmpc_ioreq *r;
	int rc, i;

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		return (rc);

	if (rq->rq_status) {
		DYNARRAY_FOREACH(r, i, biorqs)
			;
	}

	DEBUG_REQ(PLL_DEBUG, rq, "biorqs=%p len=%d",
	    biorqs, psc_dynarray_len(biorqs));

	DYNARRAY_FOREACH(r, i, biorqs)
		msl_biorq_destroy(r);
	psc_dynarray_free(biorqs);
	psc_free(biorqs, 0);

	return (0);
}

int
msl_dio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	int rc, op = rq->rq_reqmsg->opc;
	struct srm_io_req *mq;

	psc_assert(op == SRMT_READ || op == SRMT_WRITE);

	rc = authbuf_check(rq, PSCRPC_MSG_REPLY);
	if (rc)
		return (rc);

	if (rq->rq_status) {
	}

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	psc_assert(mq);

	DEBUG_REQ(PLL_DEBUG, rq, "completed dio req (op=%d) off=%u sz=%u",
	    op, mq->offset, mq->size);

	return (rc);
}

__static int
msl_pages_dio_getput(struct bmpc_ioreq *r, char *b)
{
	struct slashrpc_cservice  *csvc = NULL;
	struct pscrpc_request	  *rq = NULL;
	struct bmapc_memb	  *bcm;
	struct bmap_cli_info	  *bci;
	struct iovec		  *iovs;
	struct srm_io_req	  *mq;
	struct srm_io_rep	  *mp;

	size_t len, nbytes, size = r->biorq_len;
	int i, op, n = 0, rc = 1;

	psc_assert(r->biorq_flags & BIORQ_DIO);
	psc_assert(r->biorq_bmap);
	psc_assert(size);

	bcm = r->biorq_bmap;
	bci = bmap_2_bci(bcm);

	n = howmany(size, LNET_MTU);
	iovs = PSCALLOC(sizeof(*iovs) * n);

 retry:
	DEBUG_BIORQ(PLL_INFO, r, "dio req");

	op = r->biorq_flags & BIORQ_WRITE ? SRMT_WRITE : SRMT_READ;

	csvc = (op == SRMT_WRITE) ?
	    msl_bmap_to_csvc(bcm, 1) : msl_bmap_choose_replica(bcm);
	if (csvc == NULL)
		goto error;

	if (r->biorq_rqset == NULL)
		r->biorq_rqset = pscrpc_prep_set();

	/*
	 * This buffer hasn't been segmented into LNET sized
	 *  chunks.  Set up buffers into 1MB chunks or smaller.
	 */
	for (i = 0, nbytes = 0; i < n; i++, nbytes += len) {
		len = MIN(LNET_MTU, size - nbytes);

		rc = SL_RSX_NEWREQ(csvc, op, rq, mq, mp);
		if (rc)
			goto error;

		rq->rq_interpret_reply = msl_dio_cb;

		iovs[i].iov_base = b + nbytes;
		iovs[i].iov_len  = len;

		rc = rsx_bulkclient(rq, (op == SRMT_WRITE ?
		    BULK_GET_SOURCE : BULK_PUT_SINK), SRIC_BULK_PORTAL,
		    &iovs[i], 1);
		if (rc)
			goto error;

		mq->offset = r->biorq_off + nbytes;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		mq->flags |= SRM_IOF_DIO |
		    (r->biorq_flags & BIORQ_APPEND ? SRM_IOF_APPEND : 0);
		memcpy(&mq->sbd, &bci->bci_sbd, sizeof(mq->sbd));

		authbuf_sign(rq, PSCRPC_MSG_REQUEST);
		pscrpc_set_add_new_req(r->biorq_rqset, rq);
		if (pscrpc_push_req(rq)) {
			pscrpc_set_remove_req(r->biorq_rqset, rq);
			goto error;
		}
	}
	/* Should be no need for a callback since this call is fully
	 *   blocking.
	 */
	psc_assert(nbytes == size);
	pscrpc_set_wait(r->biorq_rqset);
	pscrpc_set_destroy(r->biorq_rqset);
	r->biorq_rqset = NULL;
	PSCFREE(iovs);

	msl_biorq_destroy(r);

	sl_csvc_decref(csvc);
	return (0);

 error:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (r->biorq_rqset) {
		spinlock(&r->biorq_rqset->set_lock);
		if (psc_listhd_empty(&r->biorq_rqset->set_requests)) {
			pscrpc_set_destroy(r->biorq_rqset);
			r->biorq_rqset = NULL;
		} else
			freelock(&r->biorq_rqset->set_lock);
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}
	if (msl_offline_retry(r))
		goto retry;
	PSCFREE(iovs);
	return (-1);
}

__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	struct bmapc_memb *b=r->biorq_bmap;
	struct bmap_pagecache *bmpc=bmap_2_bmpc(b);

	BMAP_LOCK(b);
	BMPC_LOCK(bmpc);
	/* This req must already be attached to the cache.
	 *   The BIORQ_FLUSHRDY bit prevents the request
	 *   from being processed prematurely.
	 */
	spinlock(&r->biorq_lock);
	r->biorq_flags |= BIORQ_FLUSHRDY;
	DEBUG_BIORQ(PLL_INFO, r, "BIORQ_FLUSHRDY");
	psc_assert(psclist_conjoint(&r->biorq_lentry,
	    psc_lentry_hd(&r->biorq_lentry)));
	atomic_inc(&bmpc->bmpc_pndgwr);
	freelock(&r->biorq_lock);

	if (b->bcm_flags & BMAP_DIRTY) {
		/* If the bmap is already dirty then at least
		 *   one other writer must be present.
		 */
		psc_assert(atomic_read(&bmpc->bmpc_pndgwr) > 1);
		psc_assert((pll_nitems(&bmpc->bmpc_pndg_biorqs) +
			    pll_nitems(&bmpc->bmpc_new_biorqs)) > 1);

	} else {
		if (b->bcm_flags & BMAP_TIMEOQ) {
			LIST_CACHE_LOCK(&bmapTimeoutQ);
			b->bcm_flags &= ~BMAP_TIMEOQ;
			psc_assert(!(b->bcm_flags & BMAP_DIRTY));

			if (psclist_conjoint(&b->bcm_lentry,
			    &bmapTimeoutQ.plc_listhd))
				lc_remove(&bmapTimeoutQ, b);
			LIST_CACHE_ULOCK(&bmapTimeoutQ);
		}

		b->bcm_flags |= BMAP_DIRTY;

		if (!(b->bcm_flags & BMAP_CLI_FLUSHPROC)) {
			/* Give control of the msdb_lentry to the bmap_flush
			 *   thread.
			 */
			b->bcm_flags |= BMAP_CLI_FLUSHPROC;
			psc_assert(psclist_disjoint(&b->bcm_lentry));
			DEBUG_BMAP(PLL_INFO, b, "add to bmapFlushQ");
			lc_addtail(&bmapFlushQ, b);
		}
	}

	DEBUG_BMAP(PLL_INFO, b, "biorq=%p list_empty(%d)",
		   r, pll_empty(&bmpc->bmpc_pndg_biorqs));
	BMPC_ULOCK(bmpc);
	BMAP_ULOCK(b);
}

__static int
msl_read_rpc_create(struct bmpc_ioreq *r, int startpage, int npages)
{
	struct slashrpc_cservice *csvc = NULL;
	struct bmap_pagecache_entry *bmpce;
	struct pscrpc_request *rq = NULL;
	struct psc_dynarray *a = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	int rc, i;

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

 retry:
	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	BMAP_LOCK(r->biorq_bmap);
	csvc = (r->biorq_bmap->bcm_flags & BMAP_WR) ?
	    msl_bmap_to_csvc(r->biorq_bmap, 1) :
	    msl_bmap_choose_replica(r->biorq_bmap);
	BMAP_ULOCK(r->biorq_bmap);

	if (csvc == NULL)
		goto error;

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		goto error;

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i + startpage);

		BMPCE_LOCK(bmpce);
		/* Sanity checks.
		 */
		psc_assert(biorq_is_my_bmpce(r, bmpce));
		/* BMPCE_DATARDY should not be set, otherwise we wouldn't
		 *   be here.
		 */
		psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
		bmpce_usecheck(bmpce, BIORQ_READ,
			       biorq_getaligned_off(r, (i + startpage)));

		DEBUG_BMPCE(PLL_INFO, bmpce, "adding to rpc");

		BMPCE_ULOCK(bmpce);

		iovs[i].iov_base = bmpce->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			mq->offset = bmpce->bmpce_off;
		psc_dynarray_add(a, bmpce);
	}

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    npages);
	PSCFREE(iovs);
	if (rc)
		goto error;

	mq->size = npages * BMPC_BUFSZ;
	mq->op = SRMIOP_RD;
	memcpy(&mq->sbd, bmap_2_sbd(r->biorq_bmap), sizeof(mq->sbd));

	r->biorq_flags |= BIORQ_INFL;

	DEBUG_BIORQ(PLL_NOTIFY, r, "launching read req");

	/* XXX Using a set for any type of read may be overkill.
	 */
	if (!r->biorq_rqset)
		r->biorq_rqset = pscrpc_prep_set();

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	pscrpc_set_add_new_req(r->biorq_rqset, rq);
	/* Setup the callback, supplying the dynarray as an argument.
	 */
	rq->rq_interpret_reply = msl_read_cb;
	rq->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_BMPCES] = a;
	rq->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_BIORQ] = r;
	rq->rq_async_args.pointer_arg[MSL_CB_POINTER_SLOT_CSVC] = csvc;
	rc = pscrpc_push_req(rq);
	if (rc)
		goto error;
	return (0);

 error:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		pscrpc_req_finished(rq);
		rq = NULL;
	}
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}
	if (msl_offline_retry(r))
		goto retry;
	PSCFREE(a);
	return (-1);
}

/**
 * msl_pages_prefetch - Launch read RPCs for pages that are owned by the
 *	given I/O request.  This function is called to perform a pure
 *	read request or a read-before-write for a write request.
 */
__static int
msl_pages_prefetch(struct bmpc_ioreq *r)
{
	int sched = 0, rc = 0, i, npages;
	struct bmap_pagecache_entry *bmpce;
	struct bmapc_memb *bcm;

	bcm    = r->biorq_bmap;
	npages = psc_dynarray_len(&r->biorq_pages);

	DEBUG_BIORQ(PLL_NOTIFY, r, "check prefetch");

	psc_assert(!r->biorq_rqset);

	/* Only read in the pages owned by this request.  To do this
	 *   the below loop marks only the iov slots which correspond
	 *   to page cache entries owned by this request as determined
	 *   by biorq_is_my_bmpce().
	 */
	if (r->biorq_flags & BIORQ_READ) {
		int j = -1;

		for (i = 0; i < npages && !rc; i++) {
			bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
			BMPCE_LOCK(bmpce);

			bmpce_usecheck(bmpce, BIORQ_READ,
				       biorq_getaligned_off(r, i));

			if (biorq_is_my_bmpce(r, bmpce))
				psc_assert(!(bmpce->bmpce_flags &
					     BMPCE_DATARDY));

			BMPCE_ULOCK(bmpce);

			/* Try to set the start bmpce if it's not yet
			 *   assigned.
			 */
			if (j < 0) {
				if (biorq_is_my_bmpce(r, bmpce))
					j = i;
			} else {
				if (!biorq_is_my_bmpce(r, bmpce)) {
					rc = msl_read_rpc_create(r, j, i-j);
					j = -1;
					sched = 1;

				} else if ((i-j) == BMPC_MAXBUFSRPC) {
					rc = msl_read_rpc_create(r, j, i-j);
					j = i;
					sched = 1;
				}
			}
		}

		if (j >= 0 && !rc) {
			/* Catch any unsent frags at the end of the array.
			 */
			rc = msl_read_rpc_create(r, j, i-j);
			sched = 1;
		}

	} else { /* BIORQ_WRITE */
		if ((r->biorq_flags & (BIORQ_RBWFP|BIORQ_RBWLP)) ==
		    (BIORQ_RBWFP|BIORQ_RBWLP) &&
		    psc_dynarray_len(&r->biorq_pages) == 2) {
			for (i = 0; i < 2; i++) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
			}
			rc = msl_read_rpc_create(r, 0, 2);
			sched = 1;

		} else {
			if (r->biorq_flags & BIORQ_RBWFP) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages, 0);
				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_create(r, 0, 1);
				sched = 1;
			}
			if (r->biorq_flags & BIORQ_RBWLP) {
				bmpce = psc_dynarray_getpos(&r->biorq_pages,
							    npages - 1);
				psc_assert(biorq_is_my_bmpce(r, bmpce));
				psc_assert(!(bmpce->bmpce_flags & BMPCE_DATARDY));
				psc_assert(bmpce->bmpce_flags & BMPCE_RBWPAGE);
				rc = msl_read_rpc_create(r, npages - 1, 1);
				sched = 1;
			}
		}
	}

	if (!rc && sched)
		r->biorq_flags |= BIORQ_SCHED;
	return (rc);
}

/**
 * msl_pages_blocking_load - Manage data prefetching activities.  This
 *	includes waiting on other threads to complete RPCs for data in
 *	which we're interested.
 */
__static int
msl_pages_blocking_load(struct bmpc_ioreq *r)
{
	int rc = 0, i, npages = psc_dynarray_len(&r->biorq_pages);
	struct bmap_pagecache_entry *bmpce;

	if (r->biorq_rqset) {
		rc = pscrpc_set_wait(r->biorq_rqset);

		/*
		 * The set cb is not being used; msl_read_cb() is
		 *   called on every RPC in the set.  This was causing
		 *   the biorq to have its flags mod'd in an incorrect
		 *   fashion.  For now, the following lines will be moved
		 *   here.
		 */
		spinlock(&r->biorq_lock);
		if (rc)
			r->biorq_flags &= ~BIORQ_INFL;
		else {
			r->biorq_flags &= ~(BIORQ_RBWLP | BIORQ_RBWFP |
			    BIORQ_INFL | BIORQ_SCHED);
			DEBUG_BIORQ(PLL_INFO, r, "read cb complete");
			psc_waitq_wakeall(&r->biorq_waitq);
		}
		freelock(&r->biorq_lock);
		/* Destroy and cleanup the set now.
		 */
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;
		if (rc)
			return (rc);
	}

	for (i = 0; i < npages; i++) {
		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		BMPCE_LOCK(bmpce);
		DEBUG_BMPCE(PLL_TRACE, bmpce, " ");

		if (!biorq_is_my_bmpce(r, bmpce))
			/* For pages not owned by this request,
			 *    wait for them to become DATARDY.
			 */
			while (!(bmpce->bmpce_flags & BMPCE_DATARDY)) {
				DEBUG_BMPCE(PLL_TRACE, bmpce, "waiting");
				psc_waitq_wait(bmpce->bmpce_waitq,
				    &bmpce->bmpce_lock);
				BMPCE_LOCK(bmpce);
			}

		if ((r->biorq_flags & BIORQ_READ) ||
		    !biorq_is_my_bmpce(r, bmpce))
			/* Read requests must have had their bmpce's
			 *   put into DATARDY by now (i.e. all RPCs
			 *   must have already been completed).
			 *   Same goes for pages owned by other requests.
			 */
			psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		BMPCE_ULOCK(bmpce);
	}
	return (rc);
}

/**
 * msl_pages_copyin - Copy user pages into buffer cache and schedule the
 *	slabs to be sent to the ION backend.
 * @r: array of request structs.
 * @buf: the source (application) buffer.
 */
__static void
msl_pages_copyin(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	uint32_t toff, tsize, nbytes;
	char *dest, *src;
	int i, npages;

	src    = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;
	npages = psc_dynarray_len(&r->biorq_pages);

	for (i = 0; i < npages; i++) {
		/* All pages are involved, therefore tsize should have value.
		 */
		psc_assert(tsize);

		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);
		/* Re-check RBW sanity.  The waitq pointer within the bmpce
		 *   must still be valid in order for this check to work.
		 */
		BMPCE_LOCK(bmpce);
		if (bmpce->bmpce_flags & BMPCE_RBWPAGE) {
			psc_assert(bmpce->bmpce_flags & BMPCE_RBWRDY);
			psc_assert(biorq_is_my_bmpce(r, bmpce));
		}

		/* Set the starting buffer pointer into
		 *  our cache vector.
		 */
		dest = (char *)bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			/* The first cache buffer pointer may need
			 *    a bump if the request offset is unaligned.
			 */
			bmpce_usecheck(bmpce, BIORQ_WRITE,
				       (toff & ~BMPC_BUFMASK));
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			dest += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else {
			bmpce_usecheck(bmpce, BIORQ_WRITE, toff);
			nbytes = MIN(BMPC_BUFSZ, tsize);
		}

		DEBUG_BMPCE(PLL_NOTIFY, bmpce, "tsize=%u nbytes=%u toff=%u",
			    tsize, nbytes, toff);
		BMPCE_ULOCK(bmpce);
		/* Do the deed.
		 */
		memcpy(dest, src, nbytes);
		/* If the bmpce belongs to this request and is not yet
		 *   DATARDY (ie wasn't an RBW block) then set DATARDY
		 *   and wakeup anyone who was blocked.  Note the waitq
		 *   pointer will hang around until the request has completed.
		 * Note:  wrrefs are held until the
		 */
		BMPCE_LOCK(bmpce);
		if (biorq_is_my_bmpce(r, bmpce) &&
		    !(bmpce->bmpce_flags & BMPCE_DATARDY)) {
			bmpce->bmpce_flags |= BMPCE_DATARDY;
			bmpce->bmpce_flags &= ~(BMPCE_RBWPAGE|BMPCE_RBWRDY);
			psc_waitq_wakeall(bmpce->bmpce_waitq);
			bmpce->bmpce_waitq = NULL;
		}
		BMPCE_ULOCK(bmpce);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	/* Queue these iov's for transmission to IOS.
	 */
	msl_pages_schedflush(r);
}

/**
 * msl_pages_copyout - Copy pages to the user application buffer.
 */
__static void
msl_pages_copyout(struct bmpc_ioreq *r, char *buf)
{
	struct bmap_pagecache_entry *bmpce;
	uint32_t toff, tsize;
	size_t nbytes;
	int i, npages;
	char *dest, *src;

	dest   = buf;
	tsize  = r->biorq_len;
	toff   = r->biorq_off;
	npages = psc_dynarray_len(&r->biorq_pages);

	psc_assert(npages);

	for (i = 0; i < npages; i++) {
		psc_assert(tsize);

		bmpce = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(bmpce);

		psc_assert(bmpce->bmpce_flags & BMPCE_DATARDY);

		bmpce_usecheck(bmpce, BIORQ_READ, biorq_getaligned_off(r, i));

		src = (char *)bmpce->bmpce_base;
		if (!i && (toff > bmpce->bmpce_off)) {
			psc_assert((toff - bmpce->bmpce_off) < BMPC_BUFSZ);
			src += toff - bmpce->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - bmpce->bmpce_off),
				     tsize);
		} else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		DEBUG_BMPCE(PLL_INFO, bmpce, "tsize=%u nbytes=%zu toff=%u",
			    tsize, nbytes, toff);

		memcpy(dest, src, nbytes);
		BMPCE_ULOCK(bmpce);

		toff  += nbytes;
		dest  += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	msl_biorq_destroy(r);
}

#define MSL_BIORQ_COMPLETE	((void *)0x1)

/**
 * msl_io - I/O gateway routine which bridges pscfs and the SLASH2 client
 *	cache and backend.  msl_io() handles the creation of biorq's
 *	and the loading of bmaps (which are attached to the file's
 *	fcache_memb_handle and is ultimately responsible for data being
 *	prefetched (as needed), copied into or from the cache, and (on
 *	write) being pushed to the correct I/O server.
 * @mfh: file handle structure passed to us by pscfs which contains the
 *	pointer to our fcmh.
 * @buf: the application source/dest buffer.
 * @size: size of buffer.
 * @off: file logical offset similar to pwrite().
 * @rw: the operation type (SL_READ or SL_WRITE).
 */
int
msl_io(struct msl_fhent *mfh, char *buf, const size_t size,
    const off_t off, enum rw rw)
{
#define MAX_BMAPS_REQ 4
	struct bmpc_ioreq *r[MAX_BMAPS_REQ];
	struct bmapc_memb *b, *bref = NULL;
	size_t s, e, tlen, tsize;
	int nr, i, rc;
	off_t roff;
	char *p;

	memset(r, 0, sizeof(r));

	psc_assert(mfh);
	psc_assert(mfh->mfh_fcmh);

	DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
	    "buf=%p size=%zu off=%"PRId64" rw=%d",
	    buf, size, off, rw);

	/*
	 *  Get the start and end block regions from the input
	 *  parameters.
	 */
	s = off / SLASH_BMAP_SIZE;
	e = ((off + size) - 1) / SLASH_BMAP_SIZE;
	nr = e - s + 1;
	if (nr > MAX_BMAPS_REQ) {
		rc = -EINVAL;
		goto out;
	}

 restart:
	tsize = size;
	FCMH_LOCK(mfh->mfh_fcmh);

	if (!size || (rw == SL_READ &&
	    off >= (off_t)fcmh_2_fsz(mfh->mfh_fcmh))) {
		FCMH_ULOCK(mfh->mfh_fcmh);
		rc = 0;
		goto out;
	}

	/*
	 * All I/O's block here for pending truncate requests.
	 * XXX there is a race here.  we should set CLI_TRUNC ourselves
	 * until we are done setting up the I/O to block intervening
	 * truncates.
	 */
	fcmh_wait_locked(mfh->mfh_fcmh,
	    mfh->mfh_fcmh->fcmh_flags & FCMH_CLI_TRUNC);
	FCMH_ULOCK(mfh->mfh_fcmh);

	/* Relativize the length and offset (roff is not aligned). */
	roff  = off - (s * SLASH_BMAP_SIZE);
	psc_assert(roff < SLASH_BMAP_SIZE);

	/* Length of the first bmap request. */
	tlen  = MIN(SLASH_BMAP_SIZE - (size_t)roff, size);

	/*
	 * Foreach block range, get its bmap and make a request into its
	 *  page cache.  This first loop retrieves all the pages.
	 */
	for (i = 0; i < nr; i++) {
		if (r[i])
			goto load_next;

		DEBUG_FCMH(PLL_INFO, mfh->mfh_fcmh,
		    "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%d",
		    tsize, tlen, off, roff, rw);

		psc_assert(tsize);

 retry_bmap:
		/*
		 * Load up the bmap; if it's not available then we're
		 * out of luck because we have no idea where the data
		 * is!
		 */
		rc = bmap_get(mfh->mfh_fcmh, s + i, rw, &b);
		if (rc) {
			DEBUG_FCMH(PLL_ERROR, mfh->mfh_fcmh,
			    "bno=%zd sz=%zu tlen=%zu "
			    "off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT" "
			    "rw=%d rc=%d",
			    s + i, tsize, tlen, off, roff, rw, rc);
			if (msl_fd_offline_retry(mfh))
				goto retry_bmap;
			goto out;
		}

		/*
		 * Re-relativize the offset if this request spans more
		 * than 1 bmap.
		 */
		msl_biorq_build(&r[i], b, mfh,
		    roff - (i * SLASH_BMAP_SIZE), tlen,
		    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE);
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);

		/*
		 * If we are not doing direct I/O, launch read for read
		 * requests and pre-read for unaligned write requests.
		 */
		if (!(r[i]->biorq_flags & BIORQ_DIO) && ((r[i]->biorq_flags & BIORQ_READ) ||
		     (r[i]->biorq_flags & BIORQ_RBWFP) || (r[i]->biorq_flags & BIORQ_RBWLP))) {
			rc = msl_pages_prefetch(r[i]);
			if (rc) {
				rc = msl_offline_retry_ignexpire(r[i]);
				msl_biorq_destroy(r[i]);
				if (rc)
					goto retry_bmap;
				rc = -EIO;
				goto out;
			}
		}

		BMAP_CLI_BUMP_TIMEO(b);

 load_next:
		roff += tlen;
		tsize -= tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);
	}

	/* Note that the offsets used here are file-wise offsets not
	 *   offsets into the buffer.
	 */
	for (i = 0, tlen = 0, p = buf; i < nr; i++, p += tlen) {
		if (r[i] == MSL_BIORQ_COMPLETE)
			continue;

		/* Associate the biorq's with the mfh. */
		pll_addtail(&mfh->mfh_biorqs, r[i]);

		tlen = r[i]->biorq_len;

		if (r[i]->biorq_flags & BIORQ_DIO) {
			rc = msl_pages_dio_getput(r[i], p);
			if (rc) {
				pll_remove(&mfh->mfh_biorqs, r[i]);
				rc = msl_offline_retry_ignexpire(r[i]);
				if (rc) {
					msl_biorq_destroy(r[i]);
					r[i] = NULL;
					goto restart;
				}
				rc = -EIO;
				goto out;
			}
		} else {
			/* Wait here for any pages to be faulted in from
			 *    the ION.
			 */
			if (rw == SL_READ ||
			    ((r[i]->biorq_flags & BIORQ_RBWFP) ||
			     (r[i]->biorq_flags & BIORQ_RBWLP))) {
				rc = msl_pages_blocking_load(r[i]);
				if (rc) {
					rc = msl_offline_retry(r[i]);
					if (rc) {
						/*
						 * The app wants to
						 * retry the failed I/O.
						 * What we must do in
						 * this logic is tricky
						 * since we don't want
						 * to re-lease the bmap.
						 * We hold a fake ref
						 * to the bmap so it
						 * doesn't get reclaimed
						 * until bmap_get() gets
						 * its own ref.
						 */
						if (bref)
							bmap_op_done_type(bref,
							    BMAP_OPCNT_BIORQ);
						bref = r[i]->biorq_bmap;
						bmap_op_start_type(bref,
						    BMAP_OPCNT_BIORQ);
					} else
						rc = msl_offline_retry_ignexpire(r[i]);
					if (rc) {
						msl_biorq_destroy(r[i]);
						r[i] = NULL;
						goto restart;
					}
					rc = -EIO;
					goto out;
				}
			}

			if (rw == SL_READ)
				msl_pages_copyout(r[i], p);
			else
				msl_pages_copyin(r[i], p);
		}
		r[i] = MSL_BIORQ_COMPLETE;
	}

	if (rw == SL_WRITE) {
		fcmh_setlocalsize(mfh->mfh_fcmh, off + size);
		rc = size;
	} else {
		ssize_t fsz = fcmh_getsize(mfh->mfh_fcmh);

		/*
		 * The client cache is operating on pages (i.e. 32k) so
		 *   any short read must be caught here.
		 */
		if (fsz < (ssize_t)(size + off))
			rc = size - (size + off - fsz);
		else
			rc = size;
	}
 out:
	if (bref)
		bmap_op_done_type(bref, BMAP_OPCNT_BIORQ);
	for (i = 0; i < nr; i++)
		if (r[i] && r[i] != MSL_BIORQ_COMPLETE)
			msl_biorq_destroy(r[i]);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags(&flags, &seq);
	PFL_PRFLAG(BMAP_CLI_FLUSHPROC, &flags, &seq);
	if (flags)
		printf(" unknown: %#x\n", flags);
	printf("\n");
}
#endif

struct bmap_ops bmap_ops = {
	msl_bmap_init,
	msl_bmap_retrieve,
	msl_bmap_modeset,
	msl_bmap_final_cleanup
};
