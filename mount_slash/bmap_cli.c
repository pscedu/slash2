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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <stddef.h>

#include "psc_util/random.h"
#include "psc_rpc/rpc.h"

#include "slconfig.h"
#include "bmap_cli.h"
#include "bmpc.h"
#include "fidc_cli.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slerr.h"

/**
 * bmapc_memb_init - Initialize a bmap substructure.
 * @b: the bmap struct
 */
void
msl_bmap_init(struct bmapc_memb *b)
{
	struct bmap_cli_info *bci;

	bci = bmap_2_bci(b);
	bmpc_init(&bci->bci_bmpc);

	INIT_PSC_LISTENTRY(&bci->bci_lentry);
}

/**
 * msl_bmap_modeset - Set READ or WRITE as access mode on an open file
 *	block map.
 * @b: bmap.
 * @rw: access mode to set the bmap to.
 */
__static int
msl_bmap_modeset(struct bmapc_memb *b, enum rw rw, __unusedx int flags)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	struct fcmh_cli_info *fci;
	struct fidc_membh *f;
	struct sl_resource *r;
	int rc, nretries = 0;

	f = b->bcm_fcmh;
	fci = fcmh_2_fci(f);

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

	rc = slc_rmc_getimp1(&csvc, fci->fci_resm);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCHWRMODE, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(struct srt_bmapdesc));
	mq->prefios[0] = prefIOS;
	rc = SL_RSX_WAITREP(csvc, rq, mp);
	if (rc == 0)
		rc = mp->rc;
	
	if (rc == 0)
		memcpy(bmap_2_sbd(b), &mp->sbd,
		    sizeof(struct srt_bmapdesc));

	r = libsl_id2res(bmap_2_sbd(b)->sbd_ios_id);
	psc_assert(r);
	if (r->res_type == SLREST_ARCHIVAL_FS) {
		/* Prepare for archival write by ensuring that all subsequent
		 *    IO's are direct.
		 */
		BMAP_LOCK(b);
		b->bcm_flags |= BMAP_DIO;
		BMAP_ULOCK(b);

		msl_bmap_cache_rls(b);
	}
	
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
		if (nretries > BMAP_CLI_MAX_LEASE * 2)
			return (-ETIMEDOUT);
		goto retry;
	}

	return (rc);
}

__static int
msl_bmap_lease_tryext_cb(struct pscrpc_request *rq,
			 struct pscrpc_async_args *args)
{
	struct bmapc_memb *b = args->pointer_arg[MSL_CBARG_BMAP];
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct srm_leasebmapext_rep *mp =
		pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	int rc;

	psc_assert(&rq->rq_async_args == args);

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAP_CLI_LEASEEXTREQ);

	MSL_GET_RQ_STATUS(csvc, rq, mp, rc);
	if (rc)
		goto out;

	memcpy(&bmap_2_bci(b)->bci_sbd, &mp->sbd, sizeof(struct srt_bmapdesc));

	PFL_GETTIMESPEC(&bmap_2_bci(b)->bci_xtime);

	timespecadd(&bmap_2_bci(b)->bci_xtime, &msl_bmap_timeo_inc,
	    &bmap_2_bci(b)->bci_etime);
	timespecadd(&bmap_2_bci(b)->bci_xtime, &msl_bmap_max_lease,
	    &bmap_2_bci(b)->bci_xtime);
 out:
	BMAP_CLEARATTR(b, BMAP_CLI_LEASEEXTREQ);

	DEBUG_BMAP(rc ? PLL_ERROR : PLL_NOTIFY, b,
	   "lease extension (rc=%d)", rc);

	bmap_op_done_type(b, BMAP_OPCNT_LEASEEXT);
	return (rc);
}

int
msl_bmap_lease_tryext(struct bmapc_memb *b, int *secs_rem, int force)
{
	int secs = 0, rc = 0, waslocked, extended = 0;

	waslocked = BMAP_RLOCK(b);

	if (!force && 
	    (b->bcm_flags & (BMAP_CLI_LEASEEXTREQ) ||
	     (secs = bmap_2_bci(b)->bci_xtime.tv_sec - CURRENT_SECONDS) >
	     BMAP_CLI_EXTREQSECS))
		BMAP_URLOCK(b, waslocked);

	else {
		struct slashrpc_cservice *csvc = NULL;
		struct pscrpc_request *rq = NULL;
		struct srm_leasebmapext_req *mq;
		struct srm_leasebmapext_rep *mp;

		BMAP_SETATTR(b, BMAP_CLI_LEASEEXTREQ);
		bmap_op_start_type(b, BMAP_OPCNT_LEASEEXT);
		/* Unlock no matter what.
		 */
		BMAP_ULOCK(b);

		rc = slc_rmc_getimp1(&csvc,
		    fcmh_2_fci(b->bcm_fcmh)->fci_resm);
		if (rc)
			goto error;
		rc = SL_RSX_NEWREQ(csvc, SRMT_EXTENDBMAPLS, rq, mq, mp);
		if (rc)
			goto error;

		memcpy(&mq->sbd, &bmap_2_bci(b)->bci_sbd,
		    sizeof(struct srt_bmapdesc));
		authbuf_sign(rq, PSCRPC_MSG_REQUEST);

		rq->rq_async_args.pointer_arg[MSL_CBARG_BMAP] = b;
		rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;

		if (secs <= 10 || force) {
			/* The lease is old - issue a blocking request.
			 */
			rc = SL_RSX_WAITREP(csvc, rq, mp);
			if (!rc)
				rc = msl_bmap_lease_tryext_cb(rq, 
				      &rq->rq_async_args);

			if (!rc)
				extended = 1;
			pscrpc_req_finished(rq);
			sl_csvc_decref(csvc);
			
			DEBUG_BMAP(rc ? PLL_ERROR : PLL_WARN, b,
			   "blocking lease extension req (rc=%d) (secs=%d)", 
			   rc, secs);
		
		} else {
			/* Otherwise, let the async handler do the dirty work.
			 */
			rq->rq_interpret_reply = msl_bmap_lease_tryext_cb;
			rq->rq_comp = &rpcComp;

			rc = pscrpc_nbreqset_add(pndgBmaplsReqs, rq);
			if (rc) {
 error:
				if (rq)
					pscrpc_req_finished(rq);
				if (csvc)
					sl_csvc_decref(csvc);
			}
			DEBUG_BMAP(rc ? PLL_ERROR : PLL_NOTIFY, b,
			   "non-blocking lease extension req (rc=%d) "
			   " (secs=%d)", rc, secs);
		}

		if (waslocked == PSLRV_WASLOCKED)
			BMAP_LOCK(b);
	}
	
	if (secs_rem) {
		if (!secs || extended)
			secs = bmap_2_bci(b)->bci_xtime.tv_sec -
				CURRENT_SECONDS;
		*secs_rem = secs;
	}

	return (rc);
}

/**
 * msl_bmap_retrieve - Perform a blocking 'LEASEBMAP' operation to
 *	retrieve one or more bmaps from the MDS.
 * @b: the bmap ID to retrieve.
 * @rw: read or write access
 */
int
msl_bmap_retrieve(struct bmapc_memb *bmap, enum rw rw,
    __unusedx int flags)
{
	int rc, nretries = 0, getreptbl = 0;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_leasebmap_req *mq;
	struct srm_leasebmap_rep *mp;
	struct fcmh_cli_info *fci;
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

	rc = slc_rmc_getimp1(&csvc, fci->fci_resm);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAP, rq, mq, mp);
	if (rc)
		goto out;

	mq->fg = f->fcmh_fg;
	mq->prefios[0] = prefIOS; /* Tell MDS of our preferred ION */
	mq->bmapno = bmap->bcm_bmapno;
	mq->rw = rw;
	if (getreptbl)
		mq->flags |= SRM_LEASEBMAPF_GETREPLTBL;

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

	DEBUG_BMAP(PLL_INFO, bmap, "rw=%d nid=%#"PRIx64" "
	    "sbd_seq=%"PRId64" bmapnid=%"PRIx64, rw,
	    mp->sbd.sbd_ion_nid, mp->sbd.sbd_seq, bmap_2_ion(bmap));

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
 * msl_bmap_cache_rls - called from rcm.c (SRMT_BMAPDIO).
 * @b:  the bmap whose cached pages should be released.
 */
void
msl_bmap_cache_rls(struct bmapc_memb *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	bmap_biorq_expire(b);
	bmap_biorq_waitempty(b);

	bmpc_lru_del(bmpc);

	BMPC_LOCK(bmpc);
	bmpc_freeall_locked(bmpc);
	BMPC_ULOCK(bmpc);
}

void
msl_bmap_reap_init(struct bmapc_memb *b, const struct srt_bmapdesc *sbd)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);
	int locked;

	psc_assert(!pfl_memchk(sbd, 0, sizeof(*sbd)));

	locked = BMAP_RLOCK(b);

	bci->bci_sbd = *sbd;
	/* Record the start time,
	 *  XXX the directio status of the bmap needs to be returned by
	 *	the MDS so we can set the proper expiration time.
	 */
	PFL_GETTIMESPEC(&bci->bci_xtime);

	timespecadd(&bci->bci_xtime, &msl_bmap_timeo_inc,
	    &bci->bci_etime);
	timespecadd(&bci->bci_xtime, &msl_bmap_max_lease,
	    &bci->bci_xtime);
	/*
	 * Take the reaper ref cnt early and place the bmap onto the
	 * reap list
	 */
	b->bcm_flags |= BMAP_TIMEOQ;

	/* Is this a write for a archival fs?  If so, set the bmap for DIO.
	 */
	if (sbd->sbd_ios_id != IOS_ID_ANY) {
		struct sl_resource *r = libsl_id2res(sbd->sbd_ios_id);
				
		psc_assert(r);		
		psc_assert(b->bcm_flags & BMAP_WR);

		if (r->res_type == SLREST_ARCHIVAL_FS)
			b->bcm_flags |= BMAP_DIO;
	}	
	bmap_op_start_type(b, BMAP_OPCNT_REAPER);

	BMAP_URLOCK(b, locked);
	/* Add ourselves here, otherwise zero length files
	 *   will not be removed.
	 */
	lc_addtail(&bmapTimeoutQ, bci);
}

/**
 * msl_bmap_to_csvc - Given a bmap, perform a series of lookups to
 *	locate the ION csvc.  The ION was chosen by the MDS and
 *	returned in the msl_bmap_retrieve routine.
 * @b: the bmap
 * @exclusive: whether to return connections to the specific ION the MDS
 *	told us to use instead of any ION in any IOS whose state is
 *	marked VALID for this bmap.
 * XXX: If the bmap is a read-only then any replica may be accessed (so
 *	long as it is recent).
 */
struct slashrpc_cservice *
msl_bmap_to_csvc(struct bmapc_memb *b, int exclusive)
{
	struct slashrpc_cservice *csvc;
	struct fcmh_cli_info *fci;
	struct psc_multiwait *mw;
	struct rnd_iterator it;
	struct sl_resm *resm;
	int i, locked;
	void *p;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	if (exclusive) {
		locked = BMAP_RLOCK(b);
		resm = libsl_nid2resm(bmap_2_ion(b));
		psc_assert(resm->resm_nid == bmap_2_ion(b));
		BMAP_URLOCK(b, locked);

		csvc = slc_geticsvc(resm);
		if (csvc)
			psc_assert(csvc->csvc_import->imp_connection->
			    c_peer.nid == bmap_2_ion(b));
		return (csvc);
	}

	fci = fcmh_get_pri(b->bcm_fcmh);
	mw = msl_getmw();
	for (i = 0; i < 2; i++) {
		psc_multiwait_reset(mw);
		psc_multiwait_entercritsect(mw);

		/* first, try preferred IOS */
		FOREACH_RND(&it, fci->fci_nrepls) {
			if (fci->fci_reptbl[it.ri_rnd_idx].bs_id != prefIOS)
				continue;
			csvc = msl_try_get_replica_res(b, it.ri_rnd_idx);
			if (csvc) {
				psc_multiwait_leavecritsect(mw);
				return (csvc);
			}
		}

		/* rats, not available; try anyone available now */
		FOREACH_RND(&it, fci->fci_nrepls) {
			if (fci->fci_reptbl[it.ri_rnd_idx].bs_id == prefIOS)
				continue;
			csvc = msl_try_get_replica_res(b,
			    it.ri_rnd_idx);
			if (csvc) {
				psc_multiwait_leavecritsect(mw);
				return (csvc);
			}
		}

		if (i)
			break;

		/*
		 * No connection was immediately available; wait a small
		 * amount of time to wait for any to come online.
		 */
		psc_multiwait_secs(mw, &p, 5);
	}
	psc_multiwait_leavecritsect(mw);
	return (NULL);
}

void
bmap_biorq_waitempty(struct bmapc_memb *b)
{
	BMAP_LOCK(b);
	bcm_wait_locked(b, (!pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs) ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs)  ||
			    !pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_ra)     ||
			    (b->bcm_flags & BMAP_CLI_FLUSHPROC)));

	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_new_biorqs));
	psc_assert(pll_empty(&bmap_2_bmpc(b)->bmpc_pndg_ra));
	BMAP_ULOCK(b);
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
		BIORQ_LOCK(biorq);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		BIORQ_ULOCK(biorq);
	}
	PLL_FOREACH(biorq, &bmap_2_bmpc(b)->bmpc_pndg_biorqs) {
		BIORQ_LOCK(biorq);
		biorq->biorq_flags |= BIORQ_FORCE_EXPIRE;
		DEBUG_BIORQ(PLL_INFO, biorq, "FORCE_EXPIRE");
		BIORQ_ULOCK(biorq);
	}
	BMPC_ULOCK(bmap_2_bmpc(b));

	/* Minimize biorq scanning via this hint.
	 */
	BMAP_SETATTR(b, BMAP_CLI_BIORQEXPIRE);

	psc_waitq_wakeall(&bmapflushwaitq);
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

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	PFL_PRFLAG(BMAP_CLI_FLUSHPROC, &flags, &seq);
	PFL_PRFLAG(BMAP_CLI_BIORQEXPIRE, &flags, &seq);
	PFL_PRFLAG(BMAP_CLI_LEASEEXTREQ, &flags, &seq);
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
