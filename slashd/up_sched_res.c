/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
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

/*
 * Update scheduler for site resources: this interface provides the
 * mechanism for managing updates such as truncation/deletion garbage
 * collection and replication activity to peer resources.
 */

#define PSC_SUBSYS SLMSS_UPSCH
#include "subsys_mds.h"

#include <sys/param.h>

#include <dirent.h>
#include <stdio.h>

#include <sqlite3.h>

#include "pfl/cdefs.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fs.h"
#include "pfl/list.h"
#include "pfl/mlist.h"
#include "pfl/multiwait.h"
#include "pfl/pthrutil.h"
#include "pfl/random.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/thread.h"
#include "pfl/treeutil.h"
#include "pfl/workthr.h"

#include "bmap_mds.h"
#include "batchrpc.h"
#include "mdsio.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"
#include "slutil.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

/* RPC callback numeric arg indexes */
#define IN_OFF		0
#define IN_AMT		1

/* RPC callback pointer arg indexes */
#define IP_CSVC		0
#define IP_DSTRESM	1
#define IP_SRCRESM	2
#define IP_BMAP		3

psc_spinlock_t           slm_upsch_lock;
struct psc_waitq	 slm_upsch_waitq;

/* (gdb) p &slm_upsch_queue.plc_explist.pexl_nseen.opst_lifetime */
struct psc_listcache     slm_upsch_queue;

struct psc_poolmaster	 slm_upgen_poolmaster;
struct psc_poolmgr	*slm_upgen_pool;

int	upsch_total;
int	slm_upsch_delay = 5;

void (*upd_proctab[])(struct slm_update_data *);

extern struct slrpc_batch_rep_handler slm_batch_rep_preclaim;
extern struct slrpc_batch_rep_handler slm_batch_rep_repl;

/*
 * Handle batch replication finish/error.  If success, we update the
 * bmap to the new residency states.  If error, we revert all changes
 * and set things back to a virgin state for future processing.
 */
void
slm_batch_repl_cb(void *req, void *rep, void *scratch, int rc)
{
	sl_bmapgen_t bgen;
	int tmprc, tract[NBREPLST], retifset[NBREPLST];
	struct slm_batchscratch_repl *bsr = scratch;
	struct sl_resm *dst_resm, *src_resm;
	struct srt_replwk_rep *pp = rep;
	struct srt_replwk_req *q = req;
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;

	if (!rc && pp && pp->rc)
		rc = pp->rc;

	if (rc)
		OPSTAT_INCR("repl-schedwk-err");
	else
		OPSTAT_INCR("repl-schedwk-ok");

	dst_resm = res_getmemb(bsr->bsr_res);
	src_resm = libsl_ios2resm(q->src_resid);

	tmprc = slm_fcmh_get(&q->fg, &f);
	if (tmprc)
		goto out;

	tmprc = bmap_get(f, q->bno, SL_WRITE, &b);
	if (tmprc)
		goto out;
	bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);

	// XXX grab bmap write lock before checking bgen!!!

	// XXX check fgen

	/*
	 * This check, if needed, does not have to be in the
	 * permanent storage. With batch RPC in place, if
	 * the MDS restarts, the batch RPC request will be
	 * accepted in the first place.
	 */
	BHGEN_GET(b, &bgen);
	if (!rc && q->bgen != bgen)
		rc = SLERR_GEN_OLD;

	brepls_init(tract, -1);
	brepls_init(retifset, 0);

	if (rc == 0) {
		/*
 		 * If a bmap is already marked as GARBAGE, the
 		 * replication will be done in vain.  See
 		 * mds_repl_delrq().
 		 */
		tract[BREPLST_REPL_SCHED] = BREPLST_VALID;
		/*
 		 * Wow, we just don't redo the work.
 		 *
 		 * If the state of the bmap changes in between, we
 		 * ignore the work as well.
 		 */
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		retifset[BREPLST_REPL_SCHED] = 1;
		retifset[BREPLST_REPL_QUEUED] = 1;

		OPSTAT_INCR("repl-success");
		OPSTAT2_ADD("repl-success-aggr", bsr->bsr_amt);
	} else {
		if (pp == NULL ||
		    rc == PFLERR_ALREADY ||
		    rc == SLERR_ION_OFFLINE ||
		    rc == ECONNRESET) {
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			OPSTAT_INCR("repl-fail-soft");
		} else {
			/* Fatal error: cancel replication. */
			tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE;
			OPSTAT_INCR("repl-fail-hard");
		}

		retifset[BREPLST_REPL_SCHED] = 1;

		/* 
		 * Common error codes:
		 *
		 * ECONNRESET = 104
		 * PFLERR_ALREADY = _PFLERR_START + 3 = 503
		 */
		DEBUG_BMAP(PLL_WARN, b, "replication "
		    "arrangement failure; src=%s dst=%s "
		    "rc=%d",
		    src_resm ? src_resm->resm_name : NULL,
		    dst_resm ? dst_resm->resm_name : NULL,
		    rc);

		OPSTAT_INCR("repl-failure");
		OPSTAT2_ADD("repl-failure-aggr", bsr->bsr_amt);
	}

	if (mds_repl_bmap_apply(b, tract, retifset, bsr->bsr_off))
		mds_bmap_write_logrepls(b);
	else
		OPSTAT_INCR("repl-in-vain");

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);

	resmpair_bw_adj(src_resm, dst_resm, -bsr->bsr_amt, rc);
	//upschq_resm(dst_resm, UPDT_PAGEIN);
}

/*
 * Try arranging a REPL_SCHEDWK for a bmap between a source and dst IOS
 * pair.  We estimate the data to reserve bandwidth then add the entry
 * to a batch RPC for the destination.  We mark the bmap residency table
 * and update the upsch database to avoid reprocessing until we receive
 * success or failure status from the destination IOS.
 */
int
slm_upsch_tryrepl(struct bmap *b, int off, struct sl_resm *src_resm,
    struct sl_resource *dst_res)
{
	int chg = 0, tract[NBREPLST], retifset[NBREPLST], rc;
	struct slrpc_cservice *csvc = NULL;
	struct slm_batchscratch_repl *bsr;
	struct srt_replwk_req q;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;
	sl_bmapno_t lastbno;
	int64_t amt;

	dst_resm = res_getmemb(dst_res);
	f = b->bcm_fcmh;

	amt = slm_bmap_calc_repltraffic(b);
	if (amt == 0) {
		/* No data in this bmap; simply mark as replicated. */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		mds_repl_bmap_apply(b, tract, NULL, off);
		mds_bmap_write_logrepls(b);
		return (1);
	}

	if (!resmpair_bw_adj(src_resm, dst_resm, amt, 0)) {
		OPSTAT_INCR("repl-throttle");
		return (0);
	}

	bsr = PSCALLOC(sizeof(*bsr));
	bsr->bsr_off = off;
	bsr->bsr_amt = amt;
	bsr->bsr_res = dst_res;

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, NULL);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = resm_getcsvcerr(dst_resm));

	q.fg = b->bcm_fcmh->fcmh_fg;
	q.bno = b->bcm_bmapno;
	BHGEN_GET(b, &q.bgen);
	q.src_resid = src_resm->resm_res_id;
	q.len = SLASH_BMAP_SIZE;
	lastbno = fcmh_nvalidbmaps(f);
	if (lastbno > 0)
		lastbno--;

	/* shorten length if this is the last bmap */
	if (b->bcm_bmapno == lastbno) {
		uint64_t sz;

		sz = fcmh_getsize(f);
		if (sz > b->bcm_bmapno * (uint64_t)SLASH_BMAP_SIZE)
			sz -= b->bcm_bmapno * SLASH_BMAP_SIZE;
		if (sz == 0)
			PFL_GOTOERR(out, rc = -ENODATA);

		q.len = MIN(sz, SLASH_BMAP_SIZE);
	}

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	chg = 1;

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		DEBUG_BMAP(PLL_FATAL, b,
		    "invalid bmap replica state [off %d]: %d", off, rc);

	/*
	 * If it was still QUEUED, which means we marked it SCHED, then
	 * proceed; otherwise, bail: perhaps the user dequeued the
	 * replication request or something.
	 */
	if (rc != BREPLST_REPL_QUEUED) {
		psclog_warnx("Unexpected bmap state [off %d]: %d", off, rc);
		PFL_GOTOERR(out, rc = -ENODEV);
	}

	rc = slrpc_batch_req_add(dst_res,
	    &slm_db_hipri_workq, csvc, SRMT_REPL_SCHEDWK,
	    SRMI_BULK_PORTAL, SRIM_BULK_PORTAL, &q, sizeof(q), bsr,
	    &slm_batch_rep_repl, slm_upsch_delay);
	if (rc)
		PFL_GOTOERR(out, rc);

	OPSTAT2_ADD("repl-sched", amt);

#if 0
	/*
	 * Write it out. This is needed because we can't expect the bmap
	 * to be cached until it is replicated.
	 */
	rc = mds_bmap_write_logrepls(b);
	if (rc)
		goto out;
#else
	rc = mds_bmap_write(b, NULL, NULL);
	if (rc)
		goto out;
#endif

	return (1);

 out:
	DEBUG_BMAP(PLL_DIAG, b, "dst_resm=%s src_resm=%s rc=%d",
	    dst_resm->resm_name, src_resm->resm_name, rc);

	if (chg) {
		/* undo brepls change */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;

		BMAP_LOCK(b);
		bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
		mds_repl_bmap_apply(b, tract, NULL, off);
		BMAP_ULOCK(b);
	}

	resmpair_bw_adj(src_resm, dst_resm, -bsr->bsr_amt, rc);

	PSCFREE(bsr);

	if (csvc)
		sl_csvc_decref(csvc);

 	/* Signal our caller to continue to try. */
	return (0);
}

void
slm_upsch_finish_ptrunc(struct slrpc_cservice *csvc,
    struct bmap *b, int sched, int rc, int off)
{
	struct fidc_membh *f;
	struct fcmh_mds_info *fmi;
	int ret, tract[NBREPLST], retifset[NBREPLST];

	psc_assert(b);

	f = b->bcm_fcmh;
	DEBUG_FCMH(PLL_MAX, f, "ptrunc finished");

	if (sched) {
		/*
		 * If successful, the IOS is responsible to send a
		 * SRMT_BMAPCRCWRT RPC to update CRCs in the block
		 * and the disk usage. However, we don't wait for
		 * it to happen.
		 */
		brepls_init(tract, -1);
		tract[BREPLST_TRUNCPNDG_SCHED] = rc ?
		    BREPLST_TRUNCPNDG : BREPLST_VALID;
		brepls_init_idx(retifset);

		BMAP_LOCK(b);
		bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
		ret = mds_repl_bmap_apply(b, tract, retifset, off);
		if (ret != BREPLST_TRUNCPNDG_SCHED)
			DEBUG_BMAPOD(PLL_FATAL, b, "bmap is corrupted");
		mds_bmap_write_logrepls(b);
		BMAP_ULOCK(b);
	}

	if (!rc) {
		FCMH_LOCK(f);
		fmi = fcmh_2_fmi(f);
		fmi->fmi_ptrunc_nios--;
		if (!fmi->fmi_ptrunc_nios)
			f->fcmh_flags &= ~FCMH_MDS_IN_PTRUNC;
		FCMH_ULOCK(f);
	}

	psclog(rc ? PLL_WARN : PLL_DIAG,
	    "partial truncation resolution: ios_repl_off=%d, rc=%d",
	    off, rc);

	if (csvc)
		sl_csvc_decref(csvc);
	bmap_op_done_type(b, BMAP_OPCNT_UPSCH);
}

int
slm_upsch_tryptrunc_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	int rc, off = av->space[IN_OFF];
	struct slrpc_cservice *csvc = av->pointer_arg[IP_CSVC];
	struct bmap *b = av->pointer_arg[IP_BMAP];

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srt_ptrunc_rep, rc);
	slm_upsch_finish_ptrunc(csvc, b, 1, rc, off);
	return (0);
}

/*
 * Try to issue a PTRUNC RPC to an ION.
 */
int
slm_upsch_tryptrunc(struct bmap *b, int off,
    struct sl_resource *dst_res)
{
	int tract[NBREPLST], retifset[NBREPLST], rc, sched = 0;
	struct pscrpc_request *rq = NULL;
	struct slrpc_cservice *csvc;
	struct srt_ptrunc_req *mq;
	struct srt_ptrunc_rep *mp;
	struct pscrpc_async_args av;
	struct slm_update_data *upd;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;

	upd = bmap_2_upd(b);
	f = upd_2_fcmh(upd);

	if (!slm_ptrunc_enabled) {
		DEBUG_FCMH(PLL_DIAG, f, "ptrunc averted");
		return (0);
	}
	dst_resm = res_getmemb(dst_res);
	bmap_op_start_type(b, BMAP_OPCNT_UPSCH);

	memset(&av, 0, sizeof(av));
	av.pointer_arg[IP_DSTRESM] = dst_resm;
	av.space[IN_OFF] = off;

	/*
	 * Make sure that a truncation is not already scheduled on
	 * this bmap.
	 */
	brepls_init(retifset, 0);
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;

	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		DEBUG_BMAPOD(PLL_FATAL, b,
		    "truncate already scheduled");

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, NULL);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = resm_getcsvcerr(dst_resm));
	av.pointer_arg[IP_CSVC] = csvc;

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fg = f->fcmh_fg;
	mq->bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &mq->bgen);
	mq->offset = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;

	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG_SCHED;
	retifset[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;
	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	if (rc != BREPLST_TRUNCPNDG)
		DEBUG_BMAPOD(PLL_FATAL, b, "bmap is corrupted");

	sched = 1;
	av.pointer_arg[IP_BMAP] = b;

	DEBUG_FCMH(PLL_MAX, f, "ptrunc req=%p, off=%d, id=%#x",
	    rq, off, dst_res->res_id);

	rq->rq_interpret_reply = slm_upsch_tryptrunc_cb;
	rq->rq_async_args = av;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc == 0)
		return (0);

 out:
	pscrpc_req_finished(rq);
	slm_upsch_finish_ptrunc(csvc, b, sched, rc, off);
	return (rc);
}

void
slm_batch_preclaim_cb(void *req, void *rep, void *scratch, int error)
{
	sl_replica_t repl;
	int rc, idx, tract[NBREPLST];
	struct slm_batchscratch_preclaim *bsp = scratch;
	struct srt_preclaim_req *q = req;
	struct srt_preclaim_rep *pp = rep;
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;

	if (!error && pp && pp->rc)
		error = -pp->rc;

	if (error == -PFLERR_NOTSUP) {
		struct resprof_mds_info *rpmi;

		rpmi = res2rpmi(bsp->bsp_res);
		RPMI_LOCK(rpmi);
		res2iosinfo(bsp->bsp_res)->si_flags |=
		    SIF_PRECLAIM_NOTSUP;
		RPMI_ULOCK(rpmi);
	}
	repl.bs_id = bsp->bsp_res->res_id;

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_SCHED] = error ?
	    BREPLST_GARBAGE : BREPLST_INVALID;

	rc = slm_fcmh_get(&q->fg, &f);
	if (rc)
		goto out;
	rc = bmap_get(f, q->bno, SL_WRITE, &b);
	if (rc)
		goto out;

	bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
	rc = mds_repl_iosv_lookup(current_vfsid, fcmh_2_inoh(f), &repl,
	    &idx, 1);
	if (rc >= 0) {
		mds_repl_bmap_walk(b, tract, NULL, 0, &idx, 1);
		mds_bmap_write_logrepls(b);
	}

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
}

int
slm_upsch_trypreclaim(struct sl_resource *r, struct bmap *b, int off)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
	struct slm_batchscratch_preclaim *bsp = NULL;
	struct slrpc_cservice *csvc;
	struct srt_preclaim_req q;
	struct sl_mds_iosinfo *si;
	struct fidc_membh *f;
	struct sl_resm *m;

	f = b->bcm_fcmh;
	if (!slm_preclaim_enabled) {
		OPSTAT_INCR("preclaim-averted");
		DEBUG_FCMH(PLL_DIAG, f, "preclaim averted");
		return (0);
	}

	OPSTAT_INCR("preclaim-attempt");
	si = res2iosinfo(r);
	if (si->si_flags & SIF_PRECLAIM_NOTSUP) {
		OPSTAT_INCR("preclaim-notsup");
		return (0);
	}

	m = res_getmemb(r);
	csvc = slm_geticsvc(m, NULL, CSVCF_NONBLOCK | CSVCF_NORECON,
	    NULL);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = resm_getcsvcerr(m));

	q.fg = b->bcm_fcmh->fcmh_fg;
	q.bno = b->bcm_bmapno;
	BHGEN_GET(b, &q.bgen);

	bsp = PSCALLOC(sizeof(*bsp));
	bsp->bsp_res = r;

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE] = BREPLST_GARBAGE_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	if (rc != BREPLST_GARBAGE) {
		DEBUG_BMAPOD(PLL_DEBUG, b, "consistency error; expected "
		    "state=GARBAGE at off %d", off);
		PFL_GOTOERR(out, rc = EINVAL);
	}

	rc = slrpc_batch_req_add(r,
	    &slm_db_lopri_workq, csvc, SRMT_PRECLAIM, SRMI_BULK_PORTAL,
	    SRIM_BULK_PORTAL, &q, sizeof(q), bsp,
	    &slm_batch_rep_preclaim, 30);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = mds_bmap_write_logrepls(b);
	psc_assert(rc == 0);

	return (1);

 out:
	PSCFREE(bsp);
	if (csvc)
		sl_csvc_decref(csvc);
	return (0);
}

/*
 * Process a HLDROP from LNet/PFLRPC, meaning that a connection to an
 * IOS has been lost.  Since replication is idempotent, we take the easy
 * route and just revert in progress work and will reissue later.
 */
void
upd_proc_hldrop(struct slm_update_data *tupd)
{
	int rc, tract[NBREPLST], retifset[NBREPLST], iosidx;
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	struct bmap_mds_info *bmi;
	struct bmap *b;
	sl_replica_t repl;

	upg = upd_getpriv(tupd);
	repl.bs_id = upg->upg_resm->resm_res_id;

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;

	rpmi = res2rpmi(upg->upg_resm->resm_res);
	RPMI_LOCK(rpmi);
	while (psc_dynarray_len(&rpmi->rpmi_upschq)) {
		upd = psc_dynarray_getpos(&rpmi->rpmi_upschq, 0);
		psc_dynarray_removepos(&rpmi->rpmi_upschq, 0);
		RPMI_ULOCK(rpmi);

		bmi = upd_getpriv(upd);
		b = bmi_2_bmap(bmi);
		rc = mds_repl_iosv_lookup(current_vfsid,
		    upd_2_inoh(upd), &repl, &iosidx, 1);
		if (rc) {
			psclog_error("iosv_lookup: rc=%d", rc);
			goto next;
		}
		BMAP_LOCK(b);
		bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
		if (mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx,
		    1)) {
			mds_bmap_write_logrepls(b);
		}

		BMAP_ULOCK(b);
 next:
		UPD_DECREF(upd);

		RPMI_LOCK(rpmi);
	}
	RPMI_ULOCK(rpmi);
}

int
slm_upsch_sched_repl(struct bmap_mds_info *bmi,  int dst_idx)
{
	int off, pass, valid_exists = 0;
	struct sl_resource *dst_res;
	struct sl_resm *m;
	struct fidc_membh *f;
	struct bmap *b;
	struct sl_mds_iosinfo *si;
	struct slrpc_cservice *csvc;
	struct sl_resource *src_res;
	struct rnd_iterator src_res_i;
	sl_ios_id_t iosid;

	b = bmi_2_bmap(bmi);
	f = b->bcm_fcmh;
	off = SL_BITS_PER_REPLICA * dst_idx;

	iosid = fcmh_2_repl(f, dst_idx);
	dst_res = libsl_id2res(iosid);

	/* look for a repl source */
	for (pass = 0; pass < 2; pass++) {
		FOREACH_RND(&src_res_i, fcmh_2_nrepls(f)) {
			if (src_res_i.ri_rnd_idx == dst_idx)
				continue;

			src_res = libsl_id2res(
			    fcmh_getrepl(f, src_res_i.ri_rnd_idx).bs_id);

			/*
			 * Skip ourself and old/inactive * replicas.
			 */
			if (src_res == NULL ||
			    SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls,
			    SL_BITS_PER_REPLICA *
			    src_res_i.ri_rnd_idx) != BREPLST_VALID)
				continue;

			valid_exists = 1;

			si = res2iosinfo(src_res);

			psclog_debug("attempt to "
			    "arrange repl with %s -> %s? "
			    "pass=%d siflg=%d",
			    src_res->res_name,
			    dst_res->res_name,
			    pass, !!(si->si_flags &
			    (SIF_DISABLE_LEASE |
			     SIF_DISABLE_ADVLEASE)));

			if (pass ^ (src_res->res_type == SLREST_ARCHIVAL_FS ||
			     !!(si->si_flags & (SIF_DISABLE_LEASE |
			      SIF_DISABLE_ADVLEASE))))
				continue;

			psclog_debug("trying to arrange " "repl with %s -> %s", 
			    src_res->res_name, dst_res->res_name);

			/*
			 * Search source nodes for an idle, online connection.
			 */
			m = res_getmemb(src_res);
			csvc = slm_geticsvc(m, NULL,
			    CSVCF_NONBLOCK | CSVCF_NORECON, NULL);
			if (csvc == NULL)
				continue;
			sl_csvc_decref(csvc);

			if (slm_upsch_tryrepl(b, off, m, dst_res))
				goto out;
		}
	}
	if (!valid_exists) {
		int tract[NBREPLST], retifset[NBREPLST];

		DEBUG_BMAPOD(PLL_DIAG, b, "no source "
		    "replicas exist; canceling "
		    "impossible replication request; "
		    "dst_ios=%s", dst_res->res_name);

		OPSTAT_INCR("upsch-impossible");

		brepls_init(tract, -1);
		tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE;

		brepls_init(retifset, 0);
		retifset[BREPLST_REPL_QUEUED] = 1;

		if (mds_repl_bmap_apply(b, tract, retifset, off)) {
			mds_bmap_write_logrepls(b);
			goto out;
		}
	}

	return 0;
 out:
	return 1;

}

/*
 * Process a bmap for upsch work. Called by upd_proc().
 */
void
upd_proc_bmap(struct slm_update_data *upd)
{
	int rc, off, val;
	struct rnd_iterator dst_res_i;
	struct sl_resource *dst_res;
	struct bmap_mds_info *bmi;
	struct fidc_membh *f;
	struct bmap *b;
	sl_ios_id_t iosid;

	bmi = upd_getpriv(upd);
	b = bmi_2_bmap(bmi);
	f = b->bcm_fcmh;

	DEBUG_FCMH(PLL_DEBUG, f, "upd=%p", upd);

	BMAP_LOCK(b);
	bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);

	DEBUG_BMAPOD(PLL_DEBUG, b, "processing");

	/*
	 * Scan residency states (through file's inode table) of bmap
	 * for an update.
	 */
	FOREACH_RND(&dst_res_i, fcmh_2_nrepls(f)) {
		iosid = fcmh_2_repl(f, dst_res_i.ri_rnd_idx);
		dst_res = libsl_id2res(iosid);
		if (dst_res == NULL) {
			/*
			 * IOS can be removed during the lifetime of a 
			 * deployment. So this is not an error.
			 */
			DEBUG_BMAP(PLL_WARN, b, "invalid iosid: %u(0x%x)",
			    iosid, iosid);
			continue;
		}
		off = SL_BITS_PER_REPLICA * dst_res_i.ri_rnd_idx;
		val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);
		switch (val) {
		case BREPLST_REPL_QUEUED:
			/*
			 * There is still a lease out; we'll wait for it
			 * to be relinquished.
			 */
			if (bmap_2_bmi(b)->bmi_wr_ion) {
				psclog_debug("skipping because write "
				    "lease still active");
				break;
			}
			psclog_debug("trying to arrange repl dst=%s",
			    dst_res->res_name);

			if (slm_upsch_sched_repl(bmi, dst_res_i.ri_rnd_idx))
				goto out;
			break;

		case BREPLST_TRUNCPNDG:
			rc = slm_upsch_tryptrunc(b, off, dst_res);
			break;

		case BREPLST_GARBAGE:
			rc = slm_upsch_trypreclaim(dst_res, b, off);
			if (rc > 0)
				goto out;
			break;
#if 0
		case BREPLST_VALID:
	 		/* 
			 * I guess it is possible that the state has 
			 * already been marked as valid (a user requeue 
			 * a request and the previous request has come 
			 * back successfully in between.
			 *
			 * In fact, there is a window between we update
			 * the bmap and we update the SQL table. So we
			 * might want users to requeue. Of course, if
			 * the SQL table is gone, we have to requeue.
			 */ 
			OPSTAT_INCR("upsch-already-valid");
			break;
#endif
		}
	}
 out:

	BMAP_ULOCK(b);
}

/*
 * Page in one unit of work.  This examines a single bmap for any work
 * that needs done, such as replication, garbage reclamation, etc.
 */
void
upd_proc_pagein_unit(__unusedx struct slm_update_data *upd)
{

}

int
upd_pagein_wk(void *p)
{
	int sched = 0;
	struct slm_wkdata_upschq *wk = p;

	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;

	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	int rc, retifset[NBREPLST];
	struct sl_fidgen fg;

	fg.fg_fid = wk->fg.fg_fid;
	fg.fg_gen = FGEN_ANY;
	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		goto out;
	rc = bmap_get(f, wk->bno, SL_WRITE, &b);
	if (rc)
		goto out;

	BMAP_ULOCK(b);
	if (fcmh_2_nrepls(f) > SL_DEF_REPLICAS)
		mds_inox_ensure_loaded(fcmh_2_inoh(f));

	/*
 	 * XXX why do we care about SCHED here?  upd_proc_bmap()
 	 * does not really care about them.  This seems fine
 	 * today because we only page in requests in 'Q' state.
 	 */
	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_QUEUED] = 1;
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_TRUNCPNDG] = 1;
	if (slm_preclaim_enabled) {
		retifset[BREPLST_GARBAGE] = 1;
		retifset[BREPLST_GARBAGE_SCHED] = 1;
	}

	BMAP_LOCK(b);
	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		upsch_enqueue(bmap_2_upd(b));
	else {
		OPSTAT_INCR("upsch-no-work");
		rc = 1;
	}
	BMAP_ULOCK(b);

 out:
	if (rc) {
		/*
		 * XXX Do we need to do any work if rc is an error code
		 * instead 1 here?
		 *
		 * We only try once because an IOS might down. So it is
		 * up to the user to requeue his request.
		 */
		struct slm_wkdata_upsch_purge *purge_wk;

		purge_wk = pfl_workq_getitem(slm_wk_upsch_purge,
		    struct slm_wkdata_upsch_purge);
		purge_wk->fid = fg.fg_fid;
		if (b)
			purge_wk->bno = b->bcm_bmapno;
		else
			purge_wk->bno = BMAPNO_ANY;
		pfl_workq_putitemq_head(&slm_db_lopri_workq, purge_wk);
	}
	rpmi = res2rpmi(wk->resm->resm_res);
	si = res2iosinfo(wk->resm->resm_res);
	RPMI_LOCK(rpmi);
	si->si_paging--;
	if (!si->si_paging) {
		si->si_flags &= ~SIF_UPSCH_PAGING; 
		sched = 1;
	}
	RPMI_ULOCK(rpmi);
	if (sched) {
		OPSTAT_INCR("upsch-pagein-batch");
		upschq_resm(wk->resm, UPDT_PAGEIN);
	}

	if (b)
		bmap_op_done(b);

	if (f) 
		fcmh_op_done(f);
	return (0);
}

int
upd_proc_pagein_cb(struct slm_sth *sth, void *p)
{
	struct slm_wkdata_upschq *wk;
	struct psc_dynarray *da = p;

	/*
 	 * Accumulate work items here and submit them in a batch later
 	 * so that we know when the paging is really done.
 	 */

	/* pfl_workrq_pool */
	wk = pfl_workq_getitem(upd_pagein_wk, struct slm_wkdata_upschq);
	wk->fg.fg_fid = sqlite3_column_int64(sth->sth_sth, 0);
	wk->bno = sqlite3_column_int(sth->sth_sth, 1);
	psc_dynarray_add(da, wk);
	return (0);
}

/*
 * Handle UPDT_PAGEIN.
 *
 * Page in some work for the update scheduler to do.  This consults the
 * upsch database, potentially restricting to a single resource for work
 * to schedule.
 */
void
upd_proc_pagein(struct slm_update_data *upd)
{
	int i, len, sched = 0;
	struct slm_wkdata_upschq *wk;
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi = NULL;
	struct sl_mds_iosinfo *si;
	struct sl_resource *r;
	struct psc_dynarray da = DYNARRAY_INIT;

	while (lc_nitems(&slm_db_hipri_workq))
		usleep(1000000/4);
	/*
	 * Page some work in.  We make a heuristic here to avoid a large
	 * number of operations inside the database callback.
	 *
	 * This algorithm suffers because each piece of work pulled in
	 * is not technically fair.  But each invocation of this routine
	 * selects a different user at random, so over time, no users
	 * will starve.
	 */
	upg = upd_getpriv(upd);
	r = upg->upg_resm->resm_res;
	rpmi = res2rpmi(r);
	si = res2iosinfo(r);

#define UPSCH_PAGEIN_BATCH	128

	psc_dynarray_ensurelen(&da, UPSCH_PAGEIN_BATCH);

	spinlock(&slm_upsch_lock);

	dbdo(upd_proc_pagein_cb, &da,
	    " SELECT    fid,"
	    "           bno"
	    "		nonce"
	    " FROM      upsch"
	    " WHERE     resid = IFNULL(?, resid)"
	    "   AND     status = 'Q'" 
	    " LIMIT     ?"   
	    " OFFSET    ?",  
	    upg->upg_resm ? SQLITE_INTEGER : SQLITE_NULL,
	    upg->upg_resm ? r->res_id : 0, 
	    SQLITE_INTEGER, UPSCH_PAGEIN_BATCH,
	    SQLITE_INTEGER, 
	    upg->upg_resm ? r->res_offset : 0);

	freelock(&slm_upsch_lock);

	RPMI_LOCK(rpmi);
	len = psc_dynarray_len(&da);
	if (!len) {
		if (r->res_offset)
			sched = 1;
		r->res_offset = 0;
		si->si_flags &= ~SIF_UPSCH_PAGING;
	} else {
		r->res_offset += len;
		OPSTAT_ADD("upsch-db-pagein", len);
		DYNARRAY_FOREACH(wk, i, &da) {
			si->si_paging++;
			wk->resm = upg->upg_resm;
			pfl_workq_putitem(wk);
		}
	}
	RPMI_ULOCK(rpmi);

	if (sched) {
		OPSTAT_INCR("upsch-pagein-wrap");
		upschq_resm(upg->upg_resm, UPDT_PAGEIN);
	}
	psc_dynarray_free(&da);
}

#if 0

#define UPSCH_MAX_ITEMS_RES 32

	dbdo(upd_proc_pagein_cb, NULL,
	    " SELECT	fid,"
	    "		bno,"
	    "		nonce"
	    " FROM	upsch,"
	    " WHERE	resid = IFNULL(?, resid)"
	    "   AND	status = 'Q'"
	    "   AND	gid = (SELECT gid FROM gsort ORDER BY RANDOM())"
	    " ORDER BY	sys_pri DESC,"
	    "		usr_pri DESC,"
	    "		RANDOM()"
	    " LIMIT	?",
	    upg->upg_resm ? SQLITE_INTEGER : SQLITE_NULL,
	    upg->upg_resm ? r->res_id : 0,
	    SQLITE_INTEGER, UPSCH_MAX_ITEMS_RES);

#endif

void
upd_proc(struct slm_update_data *upd)
{
	struct slm_update_generic *upg;

	DPRINTF_UPD(PLL_DIAG, upd, "start");

	UPD_LOCK(upd);
	upd->upd_flags &= ~UPDF_LIST;
	UPD_WAIT(upd);
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();
	UPD_ULOCK(upd);

	/*
 	 * Call the one of the following handlers:
 	 *
 	 * UPDT_BMAP: upd_proc_bmap()
 	 * UPDT_HLDROP: upd_proc_hldrop()
 	 * UPDT_PAGEIN: upd_proc_pagein()
 	 * UPDT_PAGEIN_UNIT: upd_proc_pagein_unit()
 	 */
	switch (upd->upd_type) {
	case UPDT_BMAP:
		OPSTAT_INCR("upsch-bmap");
		break;
	case UPDT_HLDROP:
		OPSTAT_INCR("upsch-hldrop");
		break;
	case UPDT_PAGEIN:
		OPSTAT_INCR("upsch-pagein");
		break;
	case UPDT_PAGEIN_UNIT:
		OPSTAT_INCR("upsch-pagein-unit");
		break;
	default:
		psc_fatalx("Unknown type %d", upd->upd_type);
	}

	upd_proctab[upd->upd_type](upd);

	UPD_LOCK(upd);
	upd->upd_flags &= ~UPDF_BUSY;
	upd->upd_owner = 0;
	UPD_WAKE(upd);
	UPD_ULOCK(upd);

	switch (upd->upd_type) {
	case UPDT_BMAP:
		UPD_DECREF(upd);
		break;
	case UPDT_HLDROP:
	case UPDT_PAGEIN:
	case UPDT_PAGEIN_UNIT:
		upg = upd_getpriv(upd);
		psc_pool_return(slm_upgen_pool, upg);
		break;
	}
}

int
slm_upsch_tally_cb(struct slm_sth *sth, void *p)
{
	int *val = p;

	*val = sqlite3_column_int(sth->sth_sth, 0);

	return (0);
}

/*
 * Called at startup to reset all in-progress changes back to starting
 * (e.g. SCHED -> QUEUED).
 */

int
slm_upsch_requeue_cb(struct slm_sth *sth, __unusedx void *p)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	struct sl_fidgen fg;
	sl_bmapno_t bno;

	OPSTAT_INCR("revert-cb");

	fg.fg_fid = sqlite3_column_int64(sth->sth_sth, 0);
	fg.fg_gen = FGEN_ANY;
	bno = sqlite3_column_int(sth->sth_sth, 1);

	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = bmap_getf(f, bno, SL_WRITE, BMAPGETF_CREATE, &b);
	if (rc)
		PFL_GOTOERR(out, rc);

	BMAP_ULOCK(b);
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	BMAP_LOCK(b);

	bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
	rc = mds_repl_bmap_walk_all(b, tract, retifset, 0);
	if (rc) {
		OPSTAT_INCR("bmap-requeue-replay");
		mds_bmap_write(b, NULL, NULL);
	}
	BMAP_ULOCK(b);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
slm_upsch_insert(struct bmap *b, sl_ios_id_t resid, int sys_prio,
    int usr_prio)
{
	struct sl_resource *r;
	int rc;

	r = libsl_id2res(resid);
	if (r == NULL)
		return (ESRCH);
	spinlock(&slm_upsch_lock);
	rc = dbdo(NULL, NULL,
	    " INSERT INTO upsch ("
	    "	resid,"						/* 1 */
	    "	fid,"						/* 2 */
	    "	bno,"						/* 3 */
	    "	uid,"						/* 4 */
	    "	gid,"						/* 5 */
	    "	status,"
	    "	sys_prio,"					/* 6 */
	    "	usr_prio,"					/* 7 */
	    "	nonce"						/* 8 */
	    ") VALUES ("
	    "	?,"						/* 1 */
	    "	?,"						/* 2 */
	    "	?,"						/* 3 */
	    "	?,"						/* 4 */
	    "	?,"						/* 5 */
	    "	'Q',"
	    "	?,"						/* 6 */
	    "	?,"						/* 7 */
	    "	?"						/* 8 */
	    ")",
	    SQLITE_INTEGER, resid,				/* 1 */
	    SQLITE_INTEGER64, bmap_2_fid(b),			/* 2 */
	    SQLITE_INTEGER, b->bcm_bmapno,			/* 3 */
	    SQLITE_INTEGER, b->bcm_fcmh->fcmh_sstb.sst_uid,	/* 4 */
	    SQLITE_INTEGER, b->bcm_fcmh->fcmh_sstb.sst_gid,	/* 5 */
	    SQLITE_INTEGER, sys_prio,				/* 6 */
	    SQLITE_INTEGER, usr_prio,				/* 7 */
	    SQLITE_INTEGER, sl_sys_upnonce);			/* 8 */
	freelock(&slm_upsch_lock);
	upschq_resm(res_getmemb(r), UPDT_PAGEIN);
	if (!rc)
		OPSTAT_INCR("upsch-insert-ok");
	else
		OPSTAT_INCR("upsch-insert-err");
	return (rc);
}

void
slmupschthr_main(struct psc_thread *thr)
{
	struct slm_update_data *upd;
#if 0
	struct sl_resource *r;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j;
#endif

	while (pscthr_run(thr)) {
#if 0
		if (lc_nitems(&slm_upsch_queue) < 128) {
			CONF_FOREACH_RESM(s, r, i, m, j) {
				if (!RES_ISFS(r))
					continue;
				/* schedule a call to upd_proc_pagein() */
				upschq_resm(m, UPDT_PAGEIN);
			}
		}
#endif
		upd = lc_getwait(&slm_upsch_queue);
		upd_proc(upd);
	}
}

void
slm_upsch_init(void)
{
	psc_poolmaster_init(&slm_upgen_poolmaster,
	    struct slm_update_generic, upg_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, "upgen");
	slm_upgen_pool = psc_poolmaster_getmgr(&slm_upgen_poolmaster);

	INIT_SPINLOCK(&slm_upsch_lock);
	psc_waitq_init(&slm_upsch_waitq, "upsch");
	lc_reginit(&slm_upsch_queue, struct slm_update_data,
	    upd_lentry, "upschq");
}

void
slmupschthr_spawn(void)
{
	struct psc_thread *thr;
	struct sl_resource *r;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j;

	for (i = 0; i < SLM_NUPSCHED_THREADS; i++) {
		thr = pscthr_init(SLMTHRT_UPSCHED, slmupschthr_main,
		    sizeof(struct slmupsch_thread), "slmupschthr%d", i);
		pscthr_setready(thr);
	}
	/* Jump start the process */
	CONF_FOREACH_RESM(s, r, i, m, j) {
		if (!RES_ISFS(r))
			continue;
			/* schedule a call to upd_proc_pagein() */
			upschq_resm(m, UPDT_PAGEIN);
	}
}

/*
 * Schedule a PAGEIN for a resm.
 */
void
upschq_resm(struct sl_resm *m, int type)
{
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	struct sl_mds_iosinfo *si;
	struct slrpc_cservice *csvc;

	if (type == UPDT_PAGEIN) {
		csvc = slm_geticsvc(m, NULL, 
		    CSVCF_NONBLOCK | CSVCF_NORECON, NULL);
		if (!csvc)
			return;
		sl_csvc_decref(csvc);
		rpmi = res2rpmi(m->resm_res);
		si = res2iosinfo(m->resm_res);
		RPMI_LOCK(rpmi);
		if (si->si_flags & SIF_UPSCH_PAGING) {
			RPMI_ULOCK(rpmi);
			return;
		}
		si->si_flags |= SIF_UPSCH_PAGING;
		RPMI_ULOCK(rpmi);
	}

	upg = psc_pool_get(slm_upgen_pool);
	memset(upg, 0, sizeof(*upg));
	INIT_PSC_LISTENTRY(&upg->upg_lentry);
	upg->upg_resm = m;
	upd = &upg->upg_upd;
	upd_init(upd, type);
	upsch_enqueue(upd);

	spinlock(&upd->upd_lock);
	UPD_UNBUSY(upd);
}

/*
 * Initialize a peer resource update.
 * @upd: peer update structure.
 * @type: type of update.
 */
void
upd_init(struct slm_update_data *upd, int type)
{
	psc_assert(type == UPDT_BMAP   || type == UPDT_HLDROP || 
		   type == UPDT_PAGEIN || type == UPDT_PAGEIN_UNIT);

	psc_assert(pfl_memchk(upd, 0, sizeof(*upd)) == 1);
	INIT_PSC_LISTENTRY(&upd->upd_lentry);
	upd->upd_type = type;
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();

	INIT_SPINLOCK(&upd->upd_lock);
	psc_waitq_init(&upd->upd_waitq, "upd");

	switch (type) {
	case UPDT_BMAP: {
		struct bmap_mds_info *bmi;
		struct bmap *b;

		bmi = upd_getpriv(upd);
		b = bmi_2_bmap(bmi);
		DPRINTF_UPD(PLL_DIAG, upd, "init fid="SLPRI_FID" bno=%u",
		    b->bcm_fcmh->fcmh_fg.fg_fid, b->bcm_bmapno);
		break;
	    }
	default:
		DPRINTF_UPD(PLL_DIAG, upd, "init");
	}
}

void
upd_destroy(struct slm_update_data *upd)
{
	DPRINTF_UPD(PLL_DIAG, upd, "destroy");
	psc_assert(psclist_disjoint(&upd->upd_lentry));
	psc_assert(!(upd->upd_flags & UPDF_BUSY));
	memset(upd, 0, sizeof(*upd));
}

int
slm_wk_upsch_purge(void *p)
{
	struct slm_wkdata_upsch_purge *wk = p;

	if (wk->bno == BMAPNO_ANY)
		dbdo(NULL, NULL,
		    " DELETE FROM	upsch"
		    " WHERE		fid = ?",
		    SQLITE_INTEGER64, wk->fid);
	else
		dbdo(NULL, NULL,
		    " DELETE FROM	upsch"
		    " WHERE		fid = ?"
		    "	AND		bno = ?",
		    SQLITE_INTEGER64, wk->fid,
		    SQLITE_INTEGER, wk->bno);
	return (0);
}

void
upsch_enqueue(struct slm_update_data *upd)
{
	spinlock(&upd->upd_lock);

	/* enqueue work for slmupschthr_main() */
	if (!(upd->upd_flags & UPDF_LIST)) {
		upd->upd_flags |= UPDF_LIST;
		if (upd->upd_type == UPDT_BMAP)
			OPSTAT_INCR("repl-add-bmap");
		else 
			OPSTAT_INCR("repl-add-other");
		lc_add(&slm_upsch_queue, upd);
		UPD_INCREF(upd);
	}
	freelock(&upd->upd_lock);
}

void *
upd_getpriv(struct slm_update_data *upd)
{
	char *p = (void *)upd;

	switch (upd->upd_type) {
	case UPDT_BMAP:
		return (p - offsetof(struct bmap_mds_info, bmi_upd));
	case UPDT_HLDROP:
	case UPDT_PAGEIN:
	case UPDT_PAGEIN_UNIT:
		return (p - offsetof(struct slm_update_generic, upg_upd));
	default:
		psc_fatal("type");
	}
}

#if PFL_DEBUG > 0
void
dump_upd(struct slm_update_data *upd)
{
	DPRINTF_UPD(PLL_MAX, upd, "");
}
#endif

/* see upd_type_enum, called by upd_proc() */
void (*upd_proctab[])(struct slm_update_data *) = {
	upd_proc_bmap,
	upd_proc_hldrop,
	upd_proc_pagein,
	upd_proc_pagein_unit
};

struct slrpc_batch_rep_handler slm_batch_rep_repl = {
	slm_batch_repl_cb,				/* bph_cbf */
	sizeof(struct srt_replwk_req),			/* bph_qlen */
	sizeof(struct srt_replwk_rep),			/* bph_plen */
};

struct slrpc_batch_rep_handler slm_batch_rep_preclaim = {
	slm_batch_preclaim_cb,				/* bph_cbf */
	sizeof(struct srt_preclaim_req),		/* bph_qlen */
	sizeof(struct srt_preclaim_rep),		/* bph_plen */
};
