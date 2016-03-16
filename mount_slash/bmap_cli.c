/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <stddef.h>

#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/fs.h"
#include "pfl/random.h"
#include "pfl/rpc.h"

#include "bmap_cli.h"
#include "fidc_cli.h"
#include "mount_slash.h"
#include "pgcache.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"

/* number of bmaps to allow before reaper kicks into gear */
#define BMAP_CACHE_MAX		1024

#define BMAP_DIOWAIT_USEC	100
#define BMAP_DIOWAIT_MAX_TRIES	20	/* BMAP_DIOWAIT_USEC * 2**N / 1e6 */

enum {
	MSL_BMODECHG_CBARG_BMAP,
	MSL_BMODECHG_CBARG_COMPL,
	MSL_BMODECHG_CBARG_CSVC,
};

void msl_bmap_reap_init(struct bmap *);

int slc_bmap_max_cache = BMAP_CACHE_MAX;

void
msl_bmap_reap(void)
{
	/* XXX force expire and issue a wakeup */

	/* wake up the reaper if we are out of resources */
	if (lc_nitems(&msl_bmaptimeoutq) > slc_bmap_max_cache)
		psc_waitq_wakeall(&msl_bmaptimeoutq.plc_wq_empty);
}

/*
 * Initialize CLI-specific data of a bmap structure.
 * @b: the bmap struct
 */
void
msl_bmap_init(struct bmap *b)
{
	struct bmap_cli_info *bci;

	DEBUG_BMAP(PLL_DIAG, b, "start initing");
	bci = bmap_2_bci(b);
	bmpc_init(&bci->bci_bmpc);
	pfl_rwlock_init(&bci->bci_rwlock);
	INIT_PSC_LISTENTRY(&bci->bci_lentry);
}

/*
 * Save a bmap lease received from the MDS.
 */
void
msl_bmap_stash_lease(struct bmap *b, const struct srt_bmapdesc *sbd,
    int rc, const char *action, int blocking)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);

	BMAP_LOCK_ENSURE(b);

	if (rc) {
		PFL_GETTIMESPEC(&bci->bci_etime);
		bci->bci_error = rc;
		b->bcm_flags |= BMAPF_LEASEFAILED;

		DEBUG_BMAP(PLL_ERROR, b, "stash lease failed action=%s "
		    "blocking=%d rc=%d", action, blocking, rc);
	} else {
		psc_assert(sbd->sbd_seq);
		psc_assert(sbd->sbd_fg.fg_fid);
		psc_assert(sbd->sbd_fg.fg_fid == fcmh_2_fid(b->bcm_fcmh));

		if (b->bcm_flags & BMAPF_WR)
			psc_assert(sbd->sbd_ios != IOS_ID_ANY);

		bci->bci_error = 0;
		b->bcm_flags &= ~BMAPF_LEASEFAILED;

		/*
		 * Record the start time.
		 *
		 * XXX the directio status of the bmap needs to be
		 *     returned by the MDS so we can set the proper
		 * expiration time.
		 */
		PFL_GETTIMESPEC(&bci->bci_etime);
		timespecadd(&bci->bci_etime, &msl_bmap_max_lease,
		    &bci->bci_etime);

		*bmap_2_sbd(b) = *sbd;

		DEBUG_BMAP(PLL_DIAG, b, "stash lease; action=%s "
		    "blocking=%d nseq=%"PRId64" ios=%#x "
		    "etime="PSCPRI_TIMESPEC,
		    action, blocking, sbd->sbd_seq, sbd->sbd_ios,
		    PFLPRI_PTIMESPEC_ARGS(&bci->bci_etime));
	}
}

int
msl_rmc_bmodechg_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_BMODECHG_CBARG_CSVC];
	struct psc_compl *compl = args->pointer_arg[MSL_BMODECHG_CBARG_COMPL];
	struct bmap *b = args->pointer_arg[MSL_BMODECHG_CBARG_BMAP];
	struct srm_bmap_chwrmode_rep *mp;
	struct sl_resource *r;
	int rc;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);

	BMAP_LOCK(b);
	msl_bmap_stash_lease(b, &mp->sbd, rc, "modechange", !!compl);
	if (!rc) {
		psc_assert((b->bcm_flags & BMAP_RW_MASK) == BMAPF_RD);
		b->bcm_flags = (b->bcm_flags & ~BMAPF_RD) | BMAPF_WR;
		r = libsl_id2res(bmap_2_sbd(b)->sbd_ios);
		if (r->res_type == SLREST_ARCHIVAL_FS) {
			/*
			 * Prepare for archival write by ensuring that
			 * all subsequent IO's are direct.
			 */
			b->bcm_flags |= BMAPF_DIO;

			BMAP_ULOCK(b);
			msl_bmap_cache_rls(b);
			BMAP_LOCK(b);
		}
	}

	if (compl) {
		BMAP_ULOCK(b);

		/* synchronous */
		psc_compl_ready(compl, 1);
	} else {
		/* asynchronous */
		b->bcm_flags &= ~BMAPF_MODECHNG;

		/*
		 * Will do bmap_wake_locked() for anyone waiting for us.
		 */
		bmap_op_done_type(b, BMAP_OPCNT_ASYNC);
	}

	sl_csvc_decref(csvc);
	return (rc);
}

/*
 * Set READ or WRITE as access mode on an open file bmap.
 * @b: bmap.
 * @rw: access mode to set the bmap to.
 */
__static int
msl_bmap_modeset(struct bmap *b, enum rw rw, int flags)
{
	useconds_t diowait_usec = BMAP_DIOWAIT_USEC;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_chwrmode_req *mq;
	struct srm_bmap_chwrmode_rep *mp;
	struct pscfs_req *pfr = NULL;
	struct fcmh_cli_info *fci;
	struct psc_thread *thr;
	struct psc_compl compl;
	struct pfl_fsthr *pft;
	struct fidc_membh *f;
	int rc, nretries = 0;

	thr = pscthr_get();
	if (thr->pscthr_type == PFL_THRT_FS) {
		pft = thr->pscthr_private;
		pfr = pft->pft_pfr;
	}

	f = b->bcm_fcmh;
	fci = fcmh_2_fci(f);

	psc_assert(rw == SL_WRITE || rw == SL_READ);

 retry:
	psc_assert(b->bcm_flags & BMAPF_MODECHNG);

	if (b->bcm_flags & BMAPF_WR) {
		/*
		 * Write enabled bmaps are allowed to read with no
		 * further action being taken.
		 */
		if (flags & BMAPGETF_NONBLOCK) {
			BMAP_LOCK(b);
			b->bcm_flags &= ~BMAPF_MODECHNG;
			BMAP_ULOCK(b);
		}
		return (0);
	}

	/* Add write mode to this bmap. */

	psc_assert(rw == SL_WRITE && (b->bcm_flags & BMAPF_RD));

	/* XXX respect NONBLOCK */
	rc = slc_rmc_getcsvc1(&csvc, fci->fci_resm);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCHWRMODE, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->sbd = *bmap_2_sbd(b);
	mq->prefios[0] = msl_pref_ios;

	if ((flags & BMAPGETF_NONBLOCK) == 0) {
		psc_compl_init(&compl);
		rq->rq_async_args.pointer_arg[MSL_BMODECHG_CBARG_COMPL] =
		    &compl;
	}
	rq->rq_async_args.pointer_arg[MSL_BMODECHG_CBARG_BMAP] = b;
	rq->rq_async_args.pointer_arg[MSL_BMODECHG_CBARG_CSVC] = csvc;
	rq->rq_interpret_reply = msl_rmc_bmodechg_cb;
	if (flags & BMAPGETF_NONBLOCK)
		bmap_op_start_type(b, BMAP_OPCNT_ASYNC);
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		if (flags & BMAPGETF_NONBLOCK)
			bmap_op_done_type(b, BMAP_OPCNT_ASYNC);
		else
			psc_compl_destroy(&compl);
		PFL_GOTOERR(out, rc);
	}
	if (flags & BMAPGETF_NONBLOCK)
		return (0);

	psc_compl_wait(&compl);
	psc_compl_destroy(&compl);

	BMAP_LOCK(b);
	/* XXX this is not race safe */
	rc = bmap_2_bci(b)->bci_error;
	BMAP_ULOCK(b);

	rq = NULL;
	csvc = NULL;

	if (rc == -SLERR_BMAP_DIOWAIT) {
		OPSTAT_INCR("bmap-modeset-diowait");

		/* Retry for bmap to be DIO ready. */
		DEBUG_BMAP(PLL_DIAG, b,
		    "SLERR_BMAP_DIOWAIT (try=%d)", nretries);

		nretries++;
		if (nretries > BMAP_DIOWAIT_MAX_TRIES)
			return (-ETIMEDOUT);
		usleep(diowait_usec);
		/* XXX detect overflow */
		diowait_usec += diowait_usec;
		goto retry;
	}

	if (rc)
		DEBUG_BMAP(PLL_WARN, b, "unable to modeset bmap rc=%d",
		    rc);

	return (rc);

 out:
	pscrpc_req_finished(rq);
	rq = NULL;
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc && pfr && slc_rmc_retry(pfr, &rc))
		goto retry;

	return (rc);
}

__static int
msl_rmc_bmlreassign_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmap *b = args->pointer_arg[MSL_CBARG_BMAP];
	struct srm_reassignbmap_rep *mp;
	int rc;

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAPF_REASSIGNREQ);

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);
	msl_bmap_stash_lease(b, &mp->sbd, rc, "reassign", 1);
	if (rc) {
		/*
		 * If the MDS replies with SLERR_ION_OFFLINE then don't
		 * bother with further retry attempts.
		 */
		if (rc == -SLERR_ION_OFFLINE)
			bmap_2_bci(b)->bci_nreassigns = 0;
	}

	b->bcm_flags &= ~BMAPF_REASSIGNREQ;

	bmap_op_done_type(b, BMAP_OPCNT_REASSIGN);

	sl_csvc_decref(csvc);

	return (rc);
}

__static int
msl_rmc_bmltryext_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmap *b = args->pointer_arg[MSL_CBARG_BMAP];
	struct srm_leasebmapext_rep *mp;
	int rc;

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAPF_LEASEEXTREQ);

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);
	msl_bmap_stash_lease(b, &mp->sbd, rc, "extend", 1);
	/*
	 * Unflushed data in this bmap is now invalid.
	 *
	 * XXX Move the bmap out of the fid cache so that others
	 * don't stumble across it while its active I/O's are
	 * failed.
	 */

	b->bcm_flags &= ~BMAPF_LEASEEXTREQ;

	bmap_op_done_type(b, BMAP_OPCNT_LEASEEXT);

	sl_csvc_decref(csvc);

	return (rc);
}

int
msl_bmap_lease_secs_remaining(struct bmap *b)
{
	struct timespec ts;
	int secs;

	BMAP_LOCK(b);
	PFL_GETTIMESPEC(&ts);
	secs = bmap_2_bci(b)->bci_etime.tv_sec - ts.tv_sec;
	BMAP_ULOCK(b);

	return (secs);
}

void
msl_bmap_lease_tryreassign(struct bmap *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct bmap_cli_info  *bci  = bmap_2_bci(b);
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_reassignbmap_req *mq;
	struct srm_reassignbmap_rep *mp;
	int rc;

	BMAP_LOCK(b);

	/*
	 * For lease reassignment to take place we must have the full
	 * complement of biorq's still in the cache.
	 *
	 * Additionally, no biorqs may be on the wire since those could
	 * be committed by the sliod.
	 */
	if ((b->bcm_flags & BMAPF_REASSIGNREQ) ||
	    RB_EMPTY(&bmpc->bmpc_new_biorqs) ||
	    !pll_empty(&bmpc->bmpc_pndg_biorqs) ||
	    bci->bci_nreassigns >= SL_MAX_IOSREASSIGN) {
		BMAP_ULOCK(b);
		OPSTAT_INCR("msl.bmap-reassign-bail");
		return;
	}

	bci->bci_prev_sliods[bci->bci_nreassigns] =
	    bci->bci_sbd.sbd_ios;
	bci->bci_nreassigns++;

	b->bcm_flags |= BMAPF_REASSIGNREQ;

	DEBUG_BMAP(PLL_WARN, b, "reassign from ios=%u "
	    "(nreassigns=%d)", bci->bci_sbd.sbd_ios,
	    bci->bci_nreassigns);

	bmap_op_start_type(b, BMAP_OPCNT_REASSIGN);

	BMAP_ULOCK(b);

	psc_assert(fcmh_2_fci(b->bcm_fcmh)->fci_resm == msl_rmc_resm);
	rc = slc_rmc_getcsvc1(&csvc, fcmh_2_fci(b->bcm_fcmh)->fci_resm);
	if (rc)
		goto out;

	rc = SL_RSX_NEWREQ(csvc, SRMT_REASSIGNBMAPLS, rq, mq, mp);
	if (rc)
		goto out;

	mq->sbd = bci->bci_sbd;
	memcpy(&mq->prev_sliods, &bci->bci_prev_sliods,
	    sizeof(sl_ios_id_t) * (bci->bci_nreassigns + 1));
	mq->nreassigns = bci->bci_nreassigns;
	mq->pios = msl_pref_ios;

	rq->rq_async_args.pointer_arg[MSL_CBARG_BMAP] = b;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_interpret_reply = msl_rmc_bmlreassign_cb;
	rc = SL_NBRQSET_ADD(csvc, rq);

 out:
	DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b,
	    "lease reassign req (rc=%d)", rc);

	if (rc) {
		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAPF_REASSIGNREQ;
		bmap_op_done_type(b, BMAP_OPCNT_REASSIGN);

		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);
	}
}

/*
 * Attempt to extend the lease time on a bmap.  If successful, this will
 * result in the creation and assignment of a new lease sequence number
 * from the MDS.
 *
 * @blockable:  means the caller will not block if a renew RPC is
 *	outstanding.  Currently, only fsthreads which try lease
 *	extension prior to initiating I/O are 'blockable'.  This is so
 *	the system doesn't take more work on bmaps whose leases are
 *	about to expire.
 * Notes: should the lease extension fail, all dirty write buffers must
 *	be expelled and the flush error code should be set to notify the
 *	holders of open file descriptors.
 */
int
msl_bmap_lease_tryext(struct bmap *b, int blockable)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_leasebmapext_req *mq;
	struct srm_leasebmapext_rep *mp;
	struct srt_bmapdesc *sbd;
	struct timespec ts;
	int secs, rc;

	BMAP_LOCK_ENSURE(b);
	if (b->bcm_flags & BMAPF_TOFREE) {
		psc_assert(!blockable);
		BMAP_ULOCK(b);
		return (0); // 1?
	}

	if (b->bcm_flags & BMAPF_LEASEFAILED) {
		rc = bmap_2_bci(b)->bci_error;
		BMAP_ULOCK(b);
		return (rc);
	}

	/* already waiting for LEASEEXT reply */
	if (b->bcm_flags & BMAPF_LEASEEXTREQ) {
		if (!blockable) {
			BMAP_ULOCK(b);
			return (0);
		}
		DEBUG_BMAP(PLL_DIAG, b, "blocking on lease renewal");
		bmap_op_start_type(b, BMAP_OPCNT_LEASEEXT);
		bmap_wait_locked(b, b->bcm_flags & BMAPF_LEASEEXTREQ);
		rc = bmap_2_bci(b)->bci_error;
		bmap_op_done_type(b, BMAP_OPCNT_LEASEEXT);
		return (rc);
	}

	/* if we aren't in the expiry window, bail */
	PFL_GETTIMESPEC(&ts);
	secs = (int)(bmap_2_bci(b)->bci_etime.tv_sec - ts.tv_sec);
	if (secs >= BMAP_CLI_EXTREQSECS &&
	    !(b->bcm_flags & BMAPF_LEASEEXPIRED)) {
		if (blockable)
			OPSTAT_INCR("msl.bmap-lease-ext-hit");
		BMAP_ULOCK(b);
		return (0);
	}

	if (b->bcm_flags & BMAPF_LEASEEXPIRED)
		b->bcm_flags &= ~BMAPF_LEASEEXPIRED;

	b->bcm_flags |= BMAPF_LEASEEXTREQ;
	bmap_op_start_type(b, BMAP_OPCNT_LEASEEXT);

	BMAP_ULOCK(b);

	sbd = bmap_2_sbd(b);
	psc_assert(sbd->sbd_fg.fg_fid == fcmh_2_fid(b->bcm_fcmh));

	rc = slc_rmc_getcsvc1(&csvc, fcmh_2_fci(b->bcm_fcmh)->fci_resm);
	if (rc)
		goto out;
	rc = SL_RSX_NEWREQ(csvc, SRMT_EXTENDBMAPLS, rq, mq, mp);
	if (rc)
		goto out;

	mq->sbd = *sbd;

	rq->rq_async_args.pointer_arg[MSL_CBARG_BMAP] = b;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_interpret_reply = msl_rmc_bmltryext_cb;
	rc = SL_NBRQSET_ADD(csvc, rq);

 out:
	BMAP_LOCK(b);
	DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b,
	    "lease extension req (rc=%d) (secs=%d)", rc, secs);
	if (rc) {
		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);

		bmap_2_bci(b)->bci_error = rc;
		b->bcm_flags &= ~BMAPF_LEASEEXTREQ;
		b->bcm_flags |= BMAPF_LEASEFAILED;

		bmap_wake_locked(b);
		bmap_op_done_type(b, BMAP_OPCNT_LEASEEXT);
	} else if (blockable) {
		/*
		 * We should never cache data without a lease.
		 */
		OPSTAT_INCR("msl.bmap-lease-ext-wait");
		bmap_wait_locked(b, b->bcm_flags & BMAPF_LEASEEXTREQ);
		rc = bmap_2_bci(b)->bci_error;
		BMAP_ULOCK(b);
	} else
		BMAP_ULOCK(b);

	return (rc);
}

/*
 * Compare entries in a file's replica table for ordering purposes:
 * - replicas on any member of our preferred IOS(es).
 * - replicas on non-archival resources.
 * - replicas on non-degraded resources.
 * - anything else.
 */
int
#ifdef HAVE_QSORT_R_THUNK
slc_reptbl_cmp(void *arg, const void *a, const void *b)
#else
slc_reptbl_cmp(const void *a, const void *b, void *arg)
#endif
{
	const int *ta = a, *tb = b;
	const struct fcmh_cli_info *fci = arg;
	struct sl_resource *xr, *yr;
	int rc, xv, yv;

	xr = libsl_id2res(fci->fci_inode.reptbl[*ta].bs_id);
	yr = libsl_id2res(fci->fci_inode.reptbl[*tb].bs_id);

	/* check general validity */
	xv = xr == NULL ? 1 : -1;
	yv = yr == NULL ? 1 : -1;
	rc = CMP(xv, yv);
	if (rc || (xr == NULL && yr == NULL))
		return (rc);

	/* check if preferred I/O system */
	xv = xr->res_flags & RESF_PREFIOS ? 1 : -1;
	yv = yr->res_flags & RESF_PREFIOS ? 1 : -1;
	rc = CMP(xv, yv);
	if (rc)
		return (rc);

	/* try non-archival and non-degraded IOS */
	xv = xr->res_type == SLREST_ARCHIVAL_FS ? 1 : -1;
	yv = yr->res_type == SLREST_ARCHIVAL_FS ? 1 : -1;
	rc = CMP(xv, yv);
	if (rc)
		return (rc);

	/* try degraded IOS */
	xv = res2rpci(xr)->rpci_flags & RPCIF_AVOID ? 1 : -1;
	yv = res2rpci(yr)->rpci_flags & RPCIF_AVOID ? 1 : -1;
	return (CMP(xv, yv));
}

__static int
msl_rmc_bmlget_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_BMLGET_CBARG_CSVC];
	struct psc_compl *compl = args->pointer_arg[MSL_BMLGET_CBARG_COMPL];
	struct bmap *b = args->pointer_arg[MSL_BMLGET_CBARG_BMAP];
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct srm_leasebmap_rep *mp;
	struct fidc_membh *f;
	int rc;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);
	if (!rc) {
		f = b->bcm_fcmh;
		FCMH_LOCK(f);
		msl_fcmh_stash_inode(f, &mp->ino);
		FCMH_ULOCK(f);
	}
	BMAP_LOCK(b);
	msl_bmap_stash_lease(b, &mp->sbd, rc, "get", !!compl);
	if (!rc) {
		memcpy(bci->bci_repls, mp->repls, sizeof(mp->repls));
		msl_bmap_reap_init(b);
	}

	if (compl) {
		BMAP_ULOCK(b);

		/* synchronous */
		psc_compl_ready(compl, 1);
	} else {
		BMAP_LOCK_ENSURE(b);

		/* asynchronous */
		b->bcm_flags &= ~BMAPF_LOADING;
		if (!rc)
			b->bcm_flags |= BMAPF_LOADED;

		/* will do bmap_wake_locked() for anyone waiting for us */
		bmap_op_done_type(b, BMAP_OPCNT_ASYNC);
	}

	sl_csvc_decref(csvc);
	return (rc);
}

/*
 * Perform a blocking 'LEASEBMAP' operation to retrieve one or more
 * bmaps from the MDS.
 *
 * @b: the bmap ID to retrieve.
 * @rw: read or write access
 * @flags: access flags (BMAPGETF_*).
 */
int
msl_bmap_retrieve(struct bmap *b, int flags)
{
	useconds_t diowait_usec = BMAP_DIOWAIT_USEC;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_leasebmap_req *mq;
	struct srm_leasebmap_rep *mp;
	struct pscfs_req *pfr = NULL;
	struct fcmh_cli_info *fci;
	struct psc_thread *thr;
	struct psc_compl compl;
	struct pfl_fsthr *pft;
	struct fidc_membh *f;
	int rc, nretries = 0;

	thr = pscthr_get();
	if (thr->pscthr_type == PFL_THRT_FS) {
		pft = thr->pscthr_private;
		pfr = pft->pft_pfr;
	}

	f = b->bcm_fcmh;
	fci = fcmh_2_fci(f);

 retry:
	// XXX respect NONBLOCK
	rc = slc_rmc_getcsvc1(&csvc, fci->fci_resm);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_GETBMAP, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg = f->fcmh_fg;
	mq->prefios[0] = msl_pref_ios;
	mq->bmapno = b->bcm_bmapno;
	mq->rw = b->bcm_flags & BMAPF_RD ? SL_READ : SL_WRITE;
	mq->flags |= SRM_LEASEBMAPF_GETINODE;
	if (flags & BMAPGETF_NODIO)
		mq->flags |= SRM_LEASEBMAPF_NODIO;

	DEBUG_FCMH(PLL_DIAG, f, "retrieving bmap (bmapno=%u)",
	    b->bcm_bmapno);
	DEBUG_BMAP(PLL_DIAG, b, "retrieving bmap");

	if ((flags & BMAPGETF_NONBLOCK) == 0) {
		psc_compl_init(&compl);
		rq->rq_async_args.pointer_arg[MSL_BMLGET_CBARG_COMPL] =
		    &compl;
	}

	rq->rq_async_args.pointer_arg[MSL_BMLGET_CBARG_BMAP] = b;
	rq->rq_async_args.pointer_arg[MSL_BMLGET_CBARG_CSVC] = csvc;
	rq->rq_interpret_reply = msl_rmc_bmlget_cb;
	if (flags & BMAPGETF_NONBLOCK)
		bmap_op_start_type(b, BMAP_OPCNT_ASYNC);
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		if (flags & BMAPGETF_NONBLOCK)
			bmap_op_done_type(b, BMAP_OPCNT_ASYNC);
		else
			psc_compl_destroy(&compl);
		PFL_GOTOERR(out, rc);
	}
	if (flags & BMAPGETF_NONBLOCK)
		return (0);

	psc_compl_wait(&compl);
	psc_compl_destroy(&compl);

	BMAP_LOCK(b);
	/* XXX this is not race safe */
	rc = bmap_2_bci(b)->bci_error;
	BMAP_ULOCK(b);

	rq = NULL;
	csvc = NULL;

	if (rc == -SLERR_BMAP_DIOWAIT) {
		OPSTAT_INCR("bmap-retrieve-diowait");

		/* Retry for bmap to be DIO ready. */
		DEBUG_BMAP(PLL_DIAG, b,
		    "SLERR_BMAP_DIOWAIT (try=%d)", nretries);

		nretries++;
		if (nretries > BMAP_DIOWAIT_MAX_TRIES)
			return (-ETIMEDOUT);
		usleep(diowait_usec);
		/* XXX detect overflow */
		diowait_usec += diowait_usec;
		goto retry;
	}
	if (rc == -SLERR_BMAP_IN_PTRUNC)
		rc = -EAGAIN;

	if (rc)
		DEBUG_BMAP(PLL_WARN, b, "unable to retrieve bmap rc=%d",
		    rc);

	return (rc);

 out:
	pscrpc_req_finished(rq);
	rq = NULL;
	if (csvc) {
		sl_csvc_decref(csvc);
		csvc = NULL;
	}

	if (rc && pfr && slc_rmc_retry(pfr, &rc))
		goto retry;

	return (rc);
}

/*
 * Called from rcm.c (SRMT_BMAPDIO).
 *
 * @b: the bmap whose cached pages should be released.
 */
void
msl_bmap_cache_rls(struct bmap *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct bmap_pagecache_entry *e;

	pfl_rwlock_rdlock(&bci->bci_rwlock);
	RB_FOREACH(e, bmap_pagecachetree, &bmpc->bmpc_tree) {
		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCEF_DISCARD;
		BMPCE_ULOCK(e);
	}
	pfl_rwlock_unlock(&bci->bci_rwlock);
}

void
msl_bmap_reap_init(struct bmap *b)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct srt_bmapdesc *sbd = bmap_2_sbd(b);

	BMAP_LOCK_ENSURE(b);

	/*
	 * Take the reaper ref cnt early and place the bmap onto the
	 * reap list.
	 */
	b->bcm_flags |= BMAPF_TIMEOQ;
	if (sbd->sbd_flags & SRM_LEASEBMAPF_DIO)
		b->bcm_flags |= BMAPF_DIO;

	/*
	 * Is this a write for an archival fs?  If so, set the bmap for
	 * DIO.
	 */
	if (sbd->sbd_ios != IOS_ID_ANY && !(b->bcm_flags & BMAPF_DIO)) {
		struct sl_resource *r = libsl_id2res(sbd->sbd_ios);

		psc_assert(r);
		psc_assert(b->bcm_flags & BMAPF_WR);

		if (r->res_type == SLREST_ARCHIVAL_FS)
			b->bcm_flags |= BMAPF_DIO;
	}

	bmap_op_start_type(b, BMAP_OPCNT_REAPER);

	/*
	 * Add ourselves here otherwise zero length files will not be
	 * removed.
	 */
	lc_addtail(&msl_bmaptimeoutq, bci);
}

int
msl_rmc_bmaprelease_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct srm_bmap_release_req *mq;
	uint32_t i;
	int rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_release_rep, rc);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	for (i = 0; i < mq->nbmaps; i++)
		psclog(rc ? PLL_ERROR : PLL_DIAG,
		    "fid="SLPRI_FID" bmap=%u key=%"PRId64" "
		    "seq=%"PRId64" rc=%d", mq->sbd[i].sbd_fg.fg_fid,
		    mq->sbd[i].sbd_bmapno, mq->sbd[i].sbd_key,
		    mq->sbd[i].sbd_seq, rc);

	sl_csvc_decref(csvc);
	return (rc);
}

void
msl_bmap_release(struct sl_resm *resm)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct resm_cli_info *rmci;
	int rc;

	rmci = resm2rmci(resm);

	csvc = (resm == msl_rmc_resm) ?
	    slc_getmcsvc(resm) : slc_geticsvc(resm);
	if (csvc == NULL) {
		rc = -abs(resm->resm_csvc->csvc_lasterrno); /* XXX race */
		if (rc == 0)
			rc = -ETIMEDOUT;
		goto out;
	}

	psc_assert(rmci->rmci_bmaprls.nbmaps);
	rc = SL_RSX_NEWREQ(csvc, SRMT_RELEASEBMAP, rq, mq, mp);
	if (rc)
		goto out;

	memcpy(mq, &rmci->rmci_bmaprls, sizeof(*mq));

	rq->rq_interpret_reply = msl_rmc_bmaprelease_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rc = SL_NBRQSET_ADD(csvc, rq);

 out:
	rmci->rmci_bmaprls.nbmaps = 0;
	if (rc) {
		/*
		 * At this point the bmaps have already been purged from
		 * our cache.  If the MDS RLS request fails then the MDS
		 * should time them out on his own.  In any case, the
		 * client must reacquire leases to perform further I/O
		 * on any bmap in this set.
		 */
		psclog_errorx("failed res=%s (rc=%d)", resm->resm_name,
		    rc);

		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);
	}
}

void
msbreleasethr_main(struct psc_thread *thr)
{
	struct psc_dynarray rels = DYNARRAY_INIT;
	struct psc_dynarray bcis = DYNARRAY_INIT;
	struct timespec nto, curtime;
	struct resm_cli_info *rmci;
	struct bmap_cli_info *bci;
	struct fcmh_cli_info *fci;
	struct bmapc_memb *b;
	struct sl_resm *resm;
	int exiting, i, nitems;

	/*
	 * XXX: just put the resm's in the dynarray.  When pushing out
	 * the bid's, assume an ion unless resm == msl_rmc_resm.
	 */
	psc_dynarray_ensurelen(&rels, MAX_BMAP_RELEASE);
	psc_dynarray_ensurelen(&bcis, MAX_BMAP_RELEASE);
	while (pscthr_run(thr)) {
		LIST_CACHE_LOCK(&msl_bmaptimeoutq);
		if (lc_peekheadwait(&msl_bmaptimeoutq) == NULL) {
			LIST_CACHE_ULOCK(&msl_bmaptimeoutq);
			break;
		}
		OPSTAT_INCR("msl.release-wakeup");
		PFL_GETTIMESPEC(&curtime);
		timespecadd(&curtime, &msl_bmap_max_lease, &nto);

		nitems = lc_nitems(&msl_bmaptimeoutq);
		exiting = pfl_listcache_isdead(&msl_bmaptimeoutq);
		LIST_CACHE_FOREACH(bci, &msl_bmaptimeoutq) {
			b = bci_2_bmap(bci);
			if (!BMAP_TRYLOCK(b))
				continue;

			psc_assert(b->bcm_flags & BMAPF_TIMEOQ);
			psc_assert(psc_atomic32_read(&b->bcm_opcnt) > 0);

			if (psc_atomic32_read(&b->bcm_opcnt) > 1) {
				DEBUG_BMAP(PLL_DIAG, b, "skip due to refcnt");
				BMAP_ULOCK(b);
				continue;
			}
			if (exiting)
				goto evict;
			if (timespeccmp(&curtime, &bci->bci_etime, >=))
				goto evict;

			/*
			 * Evict bmaps that are not even expired if
			 * # of bmaps on timeoutq exceeds 25% of max
			 * allowed.
			 */
			if (nitems > slc_bmap_max_cache / 4)
				goto evict;

			if (timespeccmp(&bci->bci_etime, &nto, <)) {
				nto.tv_sec = bci->bci_etime.tv_sec;
				nto.tv_nsec = bci->bci_etime.tv_nsec;
			}

			DEBUG_BMAP(PLL_DEBUG, b, "skip due to not expire");
			BMAP_ULOCK(b);
			continue;
 evict:

			/*
			 * A bmap should be taken off the flush queue
			 * after all its biorq are finished.
			 */
			psc_assert(!(b->bcm_flags & BMAPF_FLUSHQ));

			nitems--;
			psc_dynarray_add(&bcis, bci);
			if (psc_dynarray_len(&bcis) >= MAX_BMAP_RELEASE)
				break;
		}
		LIST_CACHE_ULOCK(&msl_bmaptimeoutq);

		DYNARRAY_FOREACH(bci, i, &bcis) {
			b = bci_2_bmap(bci);
			b->bcm_flags &= ~BMAPF_TIMEOQ;
			lc_remove(&msl_bmaptimeoutq, bci);

			if (b->bcm_flags & BMAPF_WR) {
				/* Setup a msg to an ION. */
				psc_assert(bmap_2_ios(b) != IOS_ID_ANY);

				resm = libsl_ios2resm(bmap_2_ios(b));
				rmci = resm2rmci(resm);

				DEBUG_BMAP(PLL_DIAG, b, "res(%s)",
				    resm->resm_res->res_name);
				OPSTAT_INCR("msl.bmap-release-write");
			} else {
				fci = fcmh_get_pri(b->bcm_fcmh);
				resm = fci->fci_resm;
				rmci = resm2rmci(resm);
				OPSTAT_INCR("msl.bmap-release-read");
			}

			memcpy(&rmci->rmci_bmaprls.sbd[rmci->rmci_bmaprls.nbmaps],
			    &bci->bci_sbd, sizeof(bci->bci_sbd));

			rmci->rmci_bmaprls.nbmaps++;
			psc_dynarray_add_ifdne(&rels, resm);

			DEBUG_BMAP(PLL_DEBUG, b, "release");
			bmap_op_done_type(b, BMAP_OPCNT_REAPER);
		}

		DYNARRAY_FOREACH(resm, i, &rels)
			msl_bmap_release(resm);

		psc_dynarray_reset(&rels);
		psc_dynarray_reset(&bcis);

		PFL_GETTIMESPEC(&curtime);
		if (timespeccmp(&curtime, &nto, <) && !exiting) {
			LIST_CACHE_LOCK(&msl_bmaptimeoutq);
			psc_waitq_waitabs(&msl_bmaptimeoutq.plc_wq_empty,
			    &msl_bmaptimeoutq.plc_lock, &nto);
		}
	}
	psc_dynarray_free(&rels);
	psc_dynarray_free(&bcis);
}

/*
 * Given a bmap, perform a series of lookups to locate the ION csvc.
 * The ION was chosen by the MDS and returned in the msl_bmap_retrieve
 * routine.
 *
 * @b: the bmap
 * @exclusive: whether to return connections to the specific ION the MDS
 *	told us to use instead of any ION in any IOS whose state is
 *	marked VALID for this bmap.
 *
 * XXX: If the bmap is a read-only then any replica may be accessed (so
 *	long as it is recent).
 */
int
msl_bmap_to_csvc(struct bmap *b, int exclusive, struct sl_resm **pm,
    struct slashrpc_cservice **csvcp)
{
	int has_residency, i, j, locked, rc;
	struct fcmh_cli_info *fci;
	struct pfl_multiwait *mw;
	struct sl_resm *m;
	void *p;

	if (pm)
		*pm = NULL;
	*csvcp = NULL;

	psc_assert(atomic_read(&b->bcm_opcnt) > 0);

	if (exclusive) {
		/*
		 * Write: lease is bound to a single IOS so it must be
		 * used.
		 */
		locked = BMAP_RLOCK(b);
		m = libsl_ios2resm(bmap_2_ios(b));
		psc_assert(m->resm_res->res_id == bmap_2_ios(b));
		BMAP_URLOCK(b, locked);
		*csvcp = slc_geticsvc(m);
		if (*csvcp) {
			if (pm)
				*pm = m;
			return (0);
		}
		rc = m->resm_csvc->csvc_lasterrno;
		if (rc)
			return (-abs(rc));
		return (-ETIMEDOUT);
	}

	fci = fcmh_get_pri(b->bcm_fcmh);
	mw = msl_getmw();

	/*
	 * Occasionally stir the order of replicas to distribute load.
	 */
	FCMH_LOCK(b->bcm_fcmh);
	if (fci->fci_inode.nrepls > 1 && ++fci->fcif_mapstircnt >= MAPSTIR_THRESH) {
		pfl_qsort_r(fci->fcif_idxmap, fci->fci_inode.nrepls,
		    sizeof(fci->fcif_idxmap[0]), slc_reptbl_cmp, fci);
		fci->fcif_mapstircnt = 0;
	}
	FCMH_ULOCK(b->bcm_fcmh);

	/*
	 * Now try two iterations:
	 *
	 *   (1) use any connections that are immediately available.
	 *
	 *   (2) if they aren't, the connection establishment is
	 *	 non-blocking, so wait a short amount of time
	 *	 (multiwait) until one wakes us up, after which we try
	 *	 again and use that connection.
	 */
	has_residency = 0;
	for (j = 0; j < 2; j++) {
		pfl_multiwait_reset(mw);
		pfl_multiwait_entercritsect(mw);

		for (i = 0; i < fci->fci_inode.nrepls; i++) {
			rc = msl_try_get_replica_res(b,
			    fci->fcif_idxmap[i], j ? has_residency : 1,
			    pm, csvcp);
			switch (rc) {
			case 0:
				pfl_multiwait_leavecritsect(mw);
				return (0);
			case -1: /* resident but offline */
				has_residency = 1;
				break;
			case -2: /* not resident */
				break;
			}
		}

//		hasdataflag = !!(bmap_2_sbd(b)->sbd_flags &
//		    SRM_LEASEBMAPF_DATA);
		if (psc_dynarray_len(&mw->mw_conds)) {
		} else {
			/*
			 * Residency scan revealed no VALID replicas.
			 * I.e. a hole in the file.  Try the next
			 * iteration which will return the first IOS
			 * online since any will suffice.
			 */
//			psc_assert(!hasvalid && !hasdataflag);
			continue;
		}

		/*
		 * No connection was immediately available; wait a small
		 * amount of time for any to finish connection
		 * (re)establishment.
		 */
		pfl_multiwait_secs(mw, &p, BMAP_CLI_MAX_LEASE);
		// XXX if ETIMEDOUT, return NULL, otherwise nonblock
		// recheck
	}
	pfl_multiwait_leavecritsect(mw);
	return (-ETIMEDOUT);
}

void
bmap_biorq_waitempty(struct bmap *b)
{
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(b);
	BMAP_LOCK(b);
	OPSTAT_INCR("msl.bmap-wait-empty");
	bmap_wait_locked(b, atomic_read(&b->bcm_opcnt) > 2);

	psc_assert(pll_empty(&bmpc->bmpc_pndg_biorqs));
	psc_assert(RB_EMPTY(&bmpc->bmpc_new_biorqs));
	BMAP_ULOCK(b);
}

/*
 * Implement bmo_final_cleanupf() operation.
 */
void
msl_bmap_final_cleanup(struct bmap *b)
{
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	psc_assert(!(b->bcm_flags & BMAPF_FLUSHQ));

	psc_assert(pll_empty(&bmpc->bmpc_pndg_biorqs));
	psc_assert(RB_EMPTY(&bmpc->bmpc_new_biorqs));

	/*
	 * Assert that this bmap can no longer be scheduled by the write
	 * back cache thread.
	 */
	psc_assert(psclist_disjoint(&b->bcm_lentry));

	DEBUG_BMAP(PLL_DIAG, b, "start freeing");

	bmpc_freeall(b);
	psc_assert(RB_EMPTY(&bmpc->bmpc_tree));

	DEBUG_BMAP(PLL_DIAG, b, "done freeing");

	psc_waitq_destroy(&bmpc->bmpc_waitq);
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	PFL_PRFLAG(BMAPF_LEASEEXTREQ, &flags, &seq);
	PFL_PRFLAG(BMAPF_REASSIGNREQ, &flags, &seq);
	PFL_PRFLAG(BMAPF_LEASEFAILED, &flags, &seq);
	PFL_PRFLAG(BMAPF_LEASEEXPIRED, &flags, &seq);
	PFL_PRFLAG(BMAPF_SCHED, &flags, &seq);
	PFL_PRFLAG(BMAPF_BENCH, &flags, &seq);
	PFL_PRFLAG(BMAPF_FLUSHQ, &flags, &seq);
	PFL_PRFLAG(BMAPF_TIMEOQ, &flags, &seq);
	if (flags)
		printf(" unknown: %#x\n", flags);
	printf("\n");
}
#endif

struct bmap_ops sl_bmap_ops = {
	msl_bmap_reap,			/* bmo_reapf() */
	msl_bmap_init,			/* bmo_init_privatef() */
	msl_bmap_retrieve,		/* bmo_retrievef() */
	msl_bmap_modeset,		/* bmo_mode_chngf() */
	msl_bmap_final_cleanup		/* bmo_final_cleanupf() */
};
