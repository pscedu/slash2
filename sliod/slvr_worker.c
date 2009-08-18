/* $Id$ */

#include <time.h>

#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_ds/listcache.h"
#include "psc_ds/dynarray.h"
#include "psc_util/assert.h"
#include "psc_util/lock.h"
#include "psc_util/atomic.h"

#include "slvr.h"
#include "iod_bmap.h"

struct pscrpc_nbreqset *slvrNbReqSet;
struct biod_infslvr_tree binfSlvrs;

extern struct psc_listcache lruSlvrs;
extern struct psc_listcache rpcqSlvrs;

__static SPLAY_GENERATE(crcup_reftree, biod_crcup_ref, bcr_tentry, bcr_cmp);

__static int
slvr_worker_crcup_genrq(const struct dynarray *a)
{
	struct biod_crcup_ref *bcrc_ref;
	struct srm_bmap_crcwrt_req *mq;
	struct iovec *iovs;
	size_t len;
	int i, rc=0;

	//XXX Need an RSX_NEWREQ here
	//#define RSX_NEWREQ(imp, version, op, rq, mq, mp) 


	mq->ncrc_updates = dynarray_len(a);

	psc_assert((bcrcup_req->ncrc_updates <= MAX_BMAP_NCRC_UPDATES) &&
		   bcrcup_req->ncrc_updates);

	len = (mq->ncrc_updates * sizeof(struct srm_bmap_crcup));
        iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);
	
	for (i=0; i < mq->ncrc_updates; i++) {
		bcrc_ref = dynarray_getpos(a, i);
		mq->ncrcs_per_update[i] = bcrc_ref->nups;

                iovs[i].iov_base = &bcrc_ref->bcr_crcup;
                len += iovs[i].iov_len = ((mq->ncrcs_per_update[i] *
					   sizeof(struct srm_bmap_crcwire)) +
					  sizeof(struct srm_bmap_crcup));
	}
	psc_assert(len <= LNET_MTU);

        rc = rsx_bulkserver(rq, &desc, BULK_GET_SOURC, SRIM_BULK_PORTAL,
                            iovs, mq->ncrc_updates);
        pscrpc_free_bulk(desc);
        if (rc)
                goto out;

	return (rc);
}

__static void 
slvr_worker_push_crcups(void)
{
        struct biod_crcup_ref *bcrc_ref=NULL;
	struct dynarray *a;

	if (!trylock(&binfSlvrs.binfst_lock))
		return;

	a = PSCALLOC(sizeof(struct dynarray));       

	/* Try to locate the full crcup's first.
	 */
	SPLAY_FOREACH(bcrc_ref, crcup_reftree, &binfSlvrs.binfst_tree) {
		if (bcrc_ref->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
			dynarray_add(a, bcrc_ref);

		if (dynarray_len(a) >= MAX_BMAP_NCRC_UPDATES)
			break;
	}
	/* Now try to find old ones.
	 */
	if (dynarray_len(a) < MAX_BMAP_NCRC_UPDATES) {
		struct timespec ts;

		clock_gettime(CLOCK_REALTIME, &ts);

		SPLAY_FOREACH(bcrc_ref, crcup_reftree, 
			      &binfSlvrs.binfst_tree) {

			if (ts.tv_sec <= (bcrc_ref->bcr_age.tv_sec + 
					  BIOD_CRCUP_MAX_AGE))
				dynarray_add(a, bcrc_ref);
			
			if (dynarray_len(a) >= MAX_BMAP_NCRC_UPDATES)
				break;
		}
	}

	psc_assert(dynarray_len(a) <= MAX_BMAP_NCRC_UPDATES);
	if (!dynarray_len(a)) {
		PSCFREE(a);
		return;
	}

	for (i=0; i < dynarray_len(a); i++) {
		bcrc_ref = dynarray_getpos(a, i);
		SPLAY_REMOVE(crcup_reftree, &binfSlvrs.binfst_tree, bcrc_ref);
	}
	/* Drop the lock
	 */
	freelock(&binfSlvrs.binfst_lock);
	/* Need to package an rpc here, if !dynarray_len(a) then free
	 *  the dynarray.
	 */

	slvr_worker_crcup_genrq(a);
}

void 
slvr_worker_int(void)
{
	struct slvr_ref *s;
	struct biod_crcup_ref tbcrc_ref, *bcrc_ref=NULL;
	struct dynarray *a=NULL;
	struct timespec ts;

	int add=0, i;

 start:	
	s = lc_getwait(&rpcqSlvrs);

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
	
	/* Try our best to determine whether or we should hold off 
	 *   the crc operation, strive to only crc slivers which have 
	 *   no pending writes.  This section directly below may race 
	 *   with slvr_wio_done().
	 */
	if (psc_atomic16_read(&s->slvr_pndgwrts) > 0) {
		if (!LIST_CACHE_TRYLOCK(&lruSlvrs)) {
			/* Don't deadlock, take the locks in the 
			 *   correct order.
			 */
			SLVR_ULOCK(s);
			/* SLVR_RPCPNDG bit will prevent the sliver from 
			 *   being re-added to the rpcq list.
			 */
			LIST_CACHE_LOCK(&lruSlvrs);
			SLVR_LOCK(s);
		}
		/* Guaranteed to have both locks.
		 */
		if (psc_atomic16_read(&s->slvr_pndgwrts) > 0) {
			s->slvr_flags &= ~SLVR_RPCPNDG;
			s->slvr_flags |= SLVR_LRU;
			lc_addqueue(&lruSlvrs, s);
			SLVR_ULOCK(s);
			LIST_CACHE_ULOCK(&lruSlvrs);
			goto start;

		} else
			/* It was > 0 but another write must have just 
			 *   finished.
			 */
			LIST_CACHE_ULOCK(&lruSlvrs);
	}
	/* Ok, we've got a sliver to work on.
	 *   From this point until we set to inflight, the slvr_lentry 
	 *   should be disjointed.
	 */
	s->slvr_flags |= (SLVR_SCHEDULED|SLVR_CRCING);
	
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
	DEBUG_SLVR(PLL_INFO, s, "prep for move to LRU");
	s->slvr_flags |= SLVR_LRU;
	s->slvr_flags &= ~SLVR_SCHEDULED;
	if (!(psc_atomic16_read(&s->slvr_pndgwrts) ||
	      psc_atomic16_read(&s->slvr_pndgreads)))
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
		struct srm_bmap_crcwire *crcw;

		psc_assert(bcrc_ref->bcr_crcup.blkno == 
			   slvr_2_bmap(s)->bcm_blkno);

		psc_assert(bcrc_ref->bcr_crcup.fid == 
			   fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh));

		crcw = &bcrc_ref->bcr_crcup.crcs[bcrc_ref->bcr_crcup.nups++];
		crcw->crc  = s->slvr_crc;
		crcw->slot = s->slvr_num;
		
		if (bcrc_ref->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
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
		struct srm_bmap_crcwire *crcw;

		bcrc_ref = PSCALLOC(sizeof(struct biod_crcup_ref) + 
				    (sizeof(struct srm_bmap_crcwire) * 
				     MAX_BMAP_INODE_PAIRS));
		bcrc_ref->bcr_slvr = s;		
		bcrc_ref->bcr_id = slvr_2_biod(s)->biod_bcr_id;
		clock_gettime(CLOCK_REALTIME, &bcrc_ref->bcr_age);

		bcrc_ref->bcr_crcup.fid = fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh);
		bcrc_ref->bcr_crcup.blkno = slvr_2_bmap(s)->bcm_blkno;
		
		crcw = &bcrc_ref->bcr_crcup.crcs[bcrc_ref->bcr_crcup.nups++];
                crcw->crc  = s->slvr_crc;
                crcw->slot = s->slvr_num;

		SPLAY_INSERT(crcup_reftree, &binfSlvrs.binfst_tree, bcrc_ref);
	}

	slvr_worker_push_crcups();
}

int 
slvr_nbreqset_cb(struct pscrpc_request *req, struct pscrpc_async_args *args)
{
	struct dynarray *a = args->pointer_arg[0];
	struct slvr_ref *s;
	int i;

	psc_assert(a);
	
	for (i=0; i < dynarray_len(a); i++) {
		s = dynarray_getpos(a, i);
		
		SLVR_LOCK(s);

		DEBUG_SLVR(PLL_INFO, s, "callback, rpc complete");
		psc_assert(s->slvr_flags & SLVR_RPCPNDG);		
		s->slvr_flags &= ~SLVR_RPCPNDG;

		SLVR_ULOCK(s);

		if (!psc_atomic16_read(&s->slvr_pndgwrts) && 
		    s->slvr_flags & SLVR_CRCDIRTY) 
			/* If the crc is dirty and there are no pending
			 *   ops then the sliver was not moved to the 
			 *   rpc queue because SLVR_RPCPNDG had been set.
			 *   Therefore we should try to schedule the
			 *   sliver.
			 */
			slvr_try_rpcqueue(s);
	}
	PSCFREE(a);
}

void slvr_worker_init(void)
{
	binfSlvrs.binfst_counter = 1;
	LOCK_INIT(&binfSlvrs.binfst_lock);

	slvrNbReqSet = nbreqset_init(NULL, slvr_nbreqset_cb);
	if (!slvrNbReqSet)
		psc_fatalx("nbreqset_init() failed");

	// XXX Need a connect call to the mds.
}
