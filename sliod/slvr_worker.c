/* $Id$ */

#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "bmap_iod.h"
#include "fid.h"
#include "fidcache.h"
#include "sliod.h"
#include "slvr.h"

struct pscrpc_nbreqset *slvrNbReqSet;
struct biod_infslvr_tree binfSlvrs;

__static SPLAY_GENERATE(crcup_reftree, biod_crcup_ref, bcr_tentry, bcr_cmp);

__static int
slvr_worker_crcup_genrq(const struct dynarray *ref_array)
{
	struct biod_crcup_ref *bcrc_ref;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *req;
	struct pscrpc_bulk_desc *desc;
	struct iovec *iovs;
	size_t len;
	int rc;
	uint32_t i;

	rc = RSX_NEWREQ(rmi_csvc->csvc_import, SRMI_VERSION,
			SRMT_BMAPCRCWRT, req, mq, mp);
	if (rc) {
		return rc;
	}

	PSC_CRC64_INIT(&mq->crc);

	mq->ncrc_updates = dynarray_len(ref_array);
	req->rq_async_args.pointer_arg[0] = (void *)ref_array;

	psc_assert((mq->ncrc_updates <= MAX_BMAP_NCRC_UPDATES) &&
		   mq->ncrc_updates);

	len = (mq->ncrc_updates * sizeof(struct srm_bmap_crcup));
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcrc_ref = dynarray_getpos(ref_array, i);

		iod_inode_getsize(bcrc_ref->bcr_crcup.fid,
				       (off_t *)&bcrc_ref->bcr_crcup.fsize);

		mq->ncrcs_per_update[i] = bcrc_ref->bcr_crcup.nups;

		iovs[i].iov_base = &bcrc_ref->bcr_crcup;
		len += iovs[i].iov_len = ((mq->ncrcs_per_update[i] *
					   sizeof(struct srm_bmap_crcwire)) +
					  sizeof(struct srm_bmap_crcup));

		psc_crc64_add(&mq->crc, iovs[i].iov_base, iovs[i].iov_len);
	}
	psc_assert(len <= LNET_MTU);

	PSC_CRC64_FIN(&mq->crc);

	rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
			     iovs, mq->ncrc_updates);
	PSCFREE(iovs);

	nbreqset_add(slvrNbReqSet, req);
	nbrequest_reap(slvrNbReqSet);

	return (rc);
}

__static void
slvr_worker_push_crcups(void)
{
	int			 i;
	struct timespec		 now;
	struct biod_crcup_ref	*bcrc_ref;
	struct dynarray		*ref_array;

	if (!trylock(&binfSlvrs.binfst_lock))
		return;

	ref_array = PSCALLOC(sizeof(struct dynarray));

	/* First, try to gather full crcup's */
	SPLAY_FOREACH(bcrc_ref, crcup_reftree, &binfSlvrs.binfst_tree) {
		if (bcrc_ref->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
			dynarray_add(ref_array, bcrc_ref);
		if (dynarray_len(ref_array) >= MAX_BMAP_NCRC_UPDATES)
			break;
	}
	/*
	 * Second, try to gather old crcups. We need to purge the ones we already
	 * have from the tree first.
	 */
	for (i = 0; i < dynarray_len(ref_array); i++) {
		bcrc_ref = dynarray_getpos(ref_array, i);
		SPLAY_REMOVE(crcup_reftree, &binfSlvrs.binfst_tree, bcrc_ref);
	}
	if (dynarray_len(ref_array) >= MAX_BMAP_NCRC_UPDATES) {
		goto done;
	}
	clock_gettime(CLOCK_REALTIME, &now);
	SPLAY_FOREACH(bcrc_ref, crcup_reftree, &binfSlvrs.binfst_tree) {
		if (now.tv_sec <= (bcrc_ref->bcr_age.tv_sec + BIOD_CRCUP_MAX_AGE))
			dynarray_add(ref_array, bcrc_ref);
		if (dynarray_len(ref_array) >= MAX_BMAP_NCRC_UPDATES)
			break;
	}
	if (!dynarray_len(ref_array)) {
		PSCFREE(ref_array);
		freelock(&binfSlvrs.binfst_lock);
		return;
	}
	for (i = 0; i < dynarray_len(ref_array); i++) {
		bcrc_ref = dynarray_getpos(ref_array, i);
		SPLAY_REMOVE(crcup_reftree, &binfSlvrs.binfst_tree, bcrc_ref);
	}
done:
	freelock(&binfSlvrs.binfst_lock);
	slvr_worker_crcup_genrq(ref_array);
}


int
slvr_nbreqset_cb(__unusedx struct pscrpc_request *req,
		 struct pscrpc_async_args *args)
{
	struct dynarray *a = args->pointer_arg[0];
	struct biod_crcup_ref *b=NULL;
	struct slvr_ref *s;
	struct srm_generic_rep *mp;
	int i, err=0;
	uint32_t j;

	ENTRY;

	mp = psc_msg_buf(req->rq_repmsg, 0, sizeof(*mp));
	if (req->rq_status || mp->rc)
		err = 1;

	psc_assert(a);

	for (i=0; i < dynarray_len(a); i++) {
		b = dynarray_getpos(a, i);

		for (j=0; j < b->bcr_nups; j++) {
			s = b->bcr_slvrs[j];

			SLVR_LOCK(s);

			DEBUG_SLVR(err ? PLL_ERROR : PLL_INFO, s,
				   "crcup %s fid(%"PRId64":%"PRId64")"
				   " bmap(%u) slvr(%hu) crc=%"PRIx64,
				   err ? "error" : "ok",
				   fcmh_2_fid(slvr_2_fcmh(s)),
				   fcmh_2_gen(slvr_2_fcmh(s)),
				   slvr_2_bmap(s)->bcm_blkno,
				   s->slvr_num, s->slvr_crc);

			psc_assert(s->slvr_flags & SLVR_RPCPNDG);
			s->slvr_flags &= ~SLVR_RPCPNDG;

			/* If the crc is dirty and there are no
			 *   pending ops then the sliver was not
			 *   moved to the rpc queue because
			 *   SLVR_RPCPNDG had been set.  Therefore
			 *   we should try to schedule the sliver,
			 *   otherwise may sit in the LRU forever.
			 */
			if (!s->slvr_pndgwrts && s->slvr_flags & SLVR_CRCDIRTY) {
				slvr_schedule_crc(s);
			} 
			SLVR_ULOCK(s);
		}
		PSCFREE(b);
	}
	dynarray_free(a);
	PSCFREE(a);

	return (0);
}


__static void
slvr_worker_int(void)
{
	struct slvr_ref *s;
	struct biod_crcup_ref tbcrc_ref, *bcrc_ref=NULL;
	struct timespec now;
	int add=0;

 start:
	clock_gettime(CLOCK_REALTIME, &now);
	now.tv_sec += BIOD_CRCUP_MAX_AGE;
	if (!(s = lc_gettimed(&rpcqSlvrs, &now))) {
		/* Nothing available, try to push any existing
		 *  crc updates.
		 */
		slvr_worker_push_crcups();
		return;
	}

	SLVR_LOCK(s);

	DEBUG_SLVR(PLL_INFO, s, "slvr_worker");
	/* Sliver assertions:
	 *  !LRU & RPCPNDG - ensure that slvr is in the right state.
	 *  CRCDIRTY - must have work to do.
	 *  PINNED - slab must not move from beneath us because the
	 *           contents must be crc'd.
	 */
	psc_assert(!(s->slvr_flags & SLVR_LRU));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_RPCPNDG);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	/* Try our best to determine whether or we should hold off
	 *   the crc operation, strive to only crc slivers which have
	 *   no pending writes.  This section directly below may race
	 *   with slvr_wio_done().
	 */
	if (s->slvr_pndgwrts > 0) {
		s->slvr_flags &= ~SLVR_RPCPNDG;
		s->slvr_flags |= SLVR_LRU;
		lc_addqueue(&lruSlvrs, s);
		SLVR_ULOCK(s);
		goto start;
	}
	/* Ok, we've got a sliver to work on.
	 *   From this point until we set to inflight, the slvr_lentry
	 *   should be disjointed.
	 */
	s->slvr_flags |= SLVR_CRCING;

	SLVR_ULOCK(s);

	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s));
	/* biodi_wire() will only be present if this bmap is in read
	 *   mode.
	 */
	if (slvr_2_biodi_wire(s)) {
		psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_DATA);
		psc_assert(slvr_2_crcbits(s) & BMAP_SLVR_CRC);
	}

	/* At this point the slvr's slab can be freed but for that
	 *   to happen we need to go back to the LRU.  The RPCPNDG bit
	 *   set above will prevent this sliver from being rescheduled.
	 *   Remember, sliod must guarantee the sliver is not inflight
	 *   more than once.
	 * The crc stored in s->slvr_crc will not be modified until
	 *   we unset the RPCPNDG bit, allowing the slvr to be processed
	 *   once again.
	 */
	SLVR_LOCK(s);
	s->slvr_flags |= SLVR_LRU;

	DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU");

	if (!s->slvr_pndgwrts && !s->slvr_pndgreads)
		slvr_lru_unpin(s);
	SLVR_ULOCK(s);
	/* Put the slvr back to the LRU so it may have its slab reaped.
	 */
	lc_addqueue(&lruSlvrs, s);
	/* Search the inflight sliver tree for an existing biod_crcup_ref.
	 */
	spinlock(&binfSlvrs.binfst_lock);
	/* Lock the biodi to get (or possibly set) the bcr_id.
	 */
	spinlock(&slvr_2_biod(s)->biod_lock);
	/* Only search the tree if the biodi id has a non-zero value.
	 */
	if (slvr_2_biod(s)->biod_bcr_id) {
		tbcrc_ref.bcr_id = slvr_2_biod(s)->biod_bcr_id;
		bcrc_ref = SPLAY_FIND(crcup_reftree, &binfSlvrs.binfst_tree,
				      &tbcrc_ref);
	}

	if (!bcrc_ref) {
		/* Don't have a ref in the tree, let's add one.
		 */
		slvr_2_biod(s)->biod_bcr_id = binfSlvrs.binfst_counter++;
		add = 1;

	} else {
		psc_assert(bcrc_ref->bcr_crcup.blkno ==
			   slvr_2_bmap(s)->bcm_blkno);

		psc_assert(bcrc_ref->bcr_crcup.fid ==
			   fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh));

		psc_assert(bcrc_ref->bcr_nups < MAX_BMAP_NCRC_UPDATES);
		bcrc_ref->bcr_slvrs[bcrc_ref->bcr_nups++] = s;

		bcrc_ref->bcr_crcup.crcs[bcrc_ref->bcr_crcup.nups].crc = s->slvr_crc;
		bcrc_ref->bcr_crcup.crcs[bcrc_ref->bcr_crcup.nups].slot = s->slvr_num;
		bcrc_ref->bcr_crcup.nups++;

		psc_assert(bcrc_ref->bcr_nups == bcrc_ref->bcr_crcup.nups);

		if (bcrc_ref->bcr_nups == MAX_BMAP_INODE_PAIRS)
			/* The crcup is filled to the max, clear the bcr_id
			 *  from the biodi, later we'll remove it from the
			 *  tree.
			 */
			slvr_2_biod(s)->biod_bcr_id = 0;
	}
	/* Should be done with the biodi, unlock it.
	 */
	freelock(&slvr_2_biod(s)->biod_lock);

	if (add) {
		bcrc_ref = PSCALLOC(sizeof(struct biod_crcup_ref) +
				    (sizeof(struct srm_bmap_crcwire) *
				     MAX_BMAP_INODE_PAIRS));

		bcrc_ref->bcr_slvrs[0] = s;
		bcrc_ref->bcr_id = slvr_2_biod(s)->biod_bcr_id;
		clock_gettime(CLOCK_REALTIME, &bcrc_ref->bcr_age);

		bcrc_ref->bcr_crcup.fid = fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh);
		bcrc_ref->bcr_crcup.blkno = slvr_2_bmap(s)->bcm_blkno;

		bcrc_ref->bcr_crcup.crcs[0].crc = s->slvr_crc;
		bcrc_ref->bcr_crcup.crcs[0].slot = s->slvr_num;
		bcrc_ref->bcr_crcup.nups = 1;

		SPLAY_INSERT(crcup_reftree, &binfSlvrs.binfst_tree, bcrc_ref);
	}

	freelock(&binfSlvrs.binfst_lock);
	slvr_worker_push_crcups();
}

void *
slvr_worker(__unusedx void *arg)
{
	for (;;) {
		slvr_worker_int();
		sched_yield();
	}
}

void
slvr_worker_init(void)
{
	int i;

	binfSlvrs.binfst_counter = 1;
	LOCK_INIT(&binfSlvrs.binfst_lock);

	slvrNbReqSet = nbreqset_init(NULL, slvr_nbreqset_cb);
	if (!slvrNbReqSet)
		psc_fatalx("nbreqset_init() failed");

	for (i=0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, 0, slvr_worker, NULL, 0,
			    "slislvrthr%d", i);
}
