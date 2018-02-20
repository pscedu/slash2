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

struct psc_waitq	 slm_pager_workq = PSC_WAITQ_INIT("pager");

/* (gdb) p &slm_upsch_queue.plc_explist.pexl_nseen.opst_lifetime */
struct psc_listcache     slm_upsch_queue;

int	slm_upsch_repl_expire = 3;
int	slm_upsch_preclaim_expire = 20;
int	slm_upsch_page_interval = 300;
int	slm_upsch_batch_size = 64;

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
	/*
	 * This check, if needed, does not have to be in the
	 * permanent storage. With batch RPC in place, if
	 * the MDS restarts, the batch RPC request will be
	 * accepted in the first place.
	 *
	 * If the file generation number changes in between, 
	 * its bmap should not be in the expected state, and 
	 * the replication effort should be rejected at the 
	 * bmap level.
	 */
	BHGEN_GET(b, &bgen);
	if (!rc && q->bgen != bgen)
		rc = SLERR_GEN_OLD;

	brepls_init(tract, -1);
	brepls_init(retifset, 0);

	if (rc == 0) {
		/*
 		 * Reject bmap in any other state other than the 
 		 * expected one. For example, the bmap could be 
 		 * marked as GARBAGE by mds_repl_delrq(). If so, 
 		 * replication will be done in vain.
 		 */
		tract[BREPLST_REPL_SCHED] = BREPLST_VALID;
		retifset[BREPLST_REPL_SCHED] = 1;

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
			/* Fatal error: cancel replication.  Saw this. */
			tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE_QUEUED;
			OPSTAT_INCR("repl-fail-hard");
		}

		retifset[BREPLST_REPL_SCHED] = 1;

		/* 
		 * Error codes we have seen:
		 *
		 * ETIMEDOUT = 110
		 * ECONNRESET = 104
		 * PFLERR_ALREADY = _PFLERR_START + 3 = 503
		 * PFLERR_TIMEDOUT = _PFLERR_START + 11 = 511
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
	else {
		if (!rc) {
			OPSTAT_INCR("repl-drop");
			OPSTAT2_ADD("repl-drop-aggr", bsr->bsr_amt);
		}
	}

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);

	resmpair_bw_adj(src_resm, dst_resm, -bsr->bsr_amt, rc);
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
	uint64_t sz;

	dst_resm = res_getmemb(dst_res);
	f = b->bcm_fcmh;

	OPSTAT_INCR("repl");
	amt = slm_bmap_calc_repltraffic(b);
	if (amt == 0) {
		/* No data in this bmap; simply mark as replicated. */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		mds_repl_bmap_apply(b, tract, NULL, off);
		mds_bmap_write_logrepls(b);
		OPSTAT_INCR("repl-no-data");
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
	    CSVCF_NORECON, NULL, 0);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = resm_getcsvcerr(dst_resm));

	q.fg = b->bcm_fcmh->fcmh_fg;
	q.bno = b->bcm_bmapno;
	BHGEN_GET(b, &q.bgen);
	q.src_resid = src_resm->resm_res_id;
	lastbno = fcmh_nvalidbmaps(f);
	if (lastbno > 0)
		lastbno--;

	/* shorten length if this is the last bmap */
	if (b->bcm_bmapno == lastbno) {
		sz = fcmh_getsize(f);
		if (sz > b->bcm_bmapno * (uint64_t)SLASH_BMAP_SIZE)
			sz -= b->bcm_bmapno * SLASH_BMAP_SIZE;
		if (sz == 0)
			PFL_GOTOERR(out, rc = -ENODATA);

		q.len = MIN(sz, SLASH_BMAP_SIZE);
	} else
		q.len = SLASH_BMAP_SIZE;

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	chg = 1;

	/*
 	 * 11/02/2017: Hit this during replication stress test (rc = 1
 	 * and off = 27).  However, we hold the bmap lock all the way.
 	 *
 	 * Hmm, bmap_wait_locked() might drop lock temporarily.
 	 *
 	 * This issue should be fixed once I get rid of SQLite.
 	 */
	if (rc == BREPLST_VALID || rc == BREPLST_REPL_SCHED)
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
	    &slm_batch_rep_repl, slm_upsch_repl_expire, 
	    slm_upsch_batch_size);
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

		mds_repl_bmap_apply(b, tract, NULL, off);
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
    struct bmap *b, int rc, int off)
{
	struct fidc_membh *f;
	struct fcmh_mds_info *fmi;
	int ret, tract[NBREPLST], retifset[NBREPLST];

	psc_assert(b);
	f = b->bcm_fcmh;

	/*
	 * Regardless of success or failure, the operation on the
	 * IOS is done for now.  We might try later.
	 */
	FCMH_LOCK(f);
	fmi = fcmh_2_fmi(f);
	fmi->fmi_ptrunc_nios--;
	if (!fmi->fmi_ptrunc_nios) {
		OPSTAT_INCR("msl.ptrunc-done");
		f->fcmh_flags &= ~FCMH_MDS_IN_PTRUNC;
	}
	FCMH_ULOCK(f);

	DEBUG_FCMH(PLL_MAX, f, "ptrunc finished, rc = %d", rc);

	/*
	 * If successful, the IOS is responsible to send a
	 * SRMT_BMAPCRCWRT RPC to update CRCs in the block
	 * and the disk usage. However, we don't wait for
	 * it to happen.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_TRUNC_SCHED] = rc ?
	    BREPLST_TRUNC_QUEUED : BREPLST_VALID;
	brepls_init_idx(retifset);

	BMAP_LOCK(b);
	ret = mds_repl_bmap_apply(b, tract, retifset, off);
	if (ret != BREPLST_TRUNC_SCHED) {
		OPSTAT_INCR("msl.ptrunc-bmap-err");
		DEBUG_BMAPOD(PLL_DEBUG, b, "bmap is corrupted");
	}
	mds_bmap_write_logrepls(b);
	BMAP_ULOCK(b);

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

	OPSTAT_INCR("msl.ptrunc-bmap-done");
	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srt_ptrunc_rep, rc);
	slm_upsch_finish_ptrunc(csvc, b, rc, off);
	return (0);
}

/*
 * Try to issue a PTRUNC RPC to an ION.
 */
int
slm_upsch_tryptrunc(struct bmap *b, int off,
    struct sl_resource *dst_res)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
	struct pscrpc_request *rq = NULL;
	struct slrpc_cservice *csvc;
	struct srt_ptrunc_req *mq;
	struct srt_ptrunc_rep *mp;
	struct pscrpc_async_args av;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;

	f = b->bcm_fcmh;
	dst_resm = res_getmemb(dst_res);
	bmap_op_start_type(b, BMAP_OPCNT_UPSCH);

	memset(&av, 0, sizeof(av));
	av.pointer_arg[IP_DSTRESM] = dst_resm;
	av.space[IN_OFF] = off;

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, NULL, 0);
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
	tract[BREPLST_TRUNC_QUEUED] = BREPLST_TRUNC_SCHED;

	brepls_init(retifset, 0);
	retifset[BREPLST_TRUNC_QUEUED] = BREPLST_TRUNC_QUEUED;

	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	if (rc != BREPLST_TRUNC_QUEUED) {
		OPSTAT_INCR("msl.ptrunc-bmap-bail");
		DEBUG_BMAPOD(PLL_DEBUG, b, "bmap inconsistency: expected "
		    "state=TRUNC at off %d", off);
		PFL_GOTOERR(out, rc = EINVAL);
	}

	av.pointer_arg[IP_BMAP] = b;

	DEBUG_FCMH(PLL_MAX, f, "ptrunc req=%p, off=%d, id=%#x",
	    rq, off, dst_res->res_id);

	rq->rq_interpret_reply = slm_upsch_tryptrunc_cb;
	rq->rq_async_args = av;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc == 0) {
		OPSTAT_INCR("msl.ptrunc-bmap-send");
		return (0);
	}

 out:
	pscrpc_req_finished(rq);

	/* There has to be a better way than this lock/unlock juggle */
	BMAP_ULOCK(b);
	slm_upsch_finish_ptrunc(csvc, b, rc, off);
	BMAP_LOCK(b);

	return (rc);
}

void
slm_batch_preclaim_cb(void *req, void *rep, void *scratch, int error)
{
	sl_replica_t repl;
	struct resprof_mds_info *rpmi;
	int rc, idx, tract[NBREPLST];
	struct slm_batchscratch_preclaim *bsp = scratch;
	struct srt_preclaim_req *q = req;
	struct srt_preclaim_rep *pp = rep;
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;

	if (!error && pp && pp->rc)
		error = -pp->rc;

	/*
	 * If our I/O server does not support punching a hole, fake success 
	 * and go ahead mark the bmap as invalid. Note that PFLERR_NOTSUP
	 * is not the same as EOPNOTSUPP.
	 */
	if (error == -PFLERR_NOTSUP) {
		error = 0;
		rpmi = res2rpmi(bsp->bsp_res);
		RPMI_LOCK(rpmi);
		res2iosinfo(bsp->bsp_res)->si_flags |=
		    SIF_PRECLAIM_NOTSUP;
		RPMI_ULOCK(rpmi);
	}
	rc = slm_fcmh_get(&q->fg, &f);
	if (rc)
		goto out;
	rc = bmap_get(f, q->bno, SL_WRITE, &b);
	if (rc)
		goto out;

	repl.bs_id = bsp->bsp_res->res_id;

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_SCHED] = error ?
	    BREPLST_GARBAGE_QUEUED : BREPLST_INVALID;

	/*
 	 * Map I/O ID to index into the table and then modify the bmap.
 	 */
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
	    NULL, 0);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = resm_getcsvcerr(m));

	q.fg = b->bcm_fcmh->fcmh_fg;
	q.bno = b->bcm_bmapno;
	BHGEN_GET(b, &q.bgen);

	bsp = PSCALLOC(sizeof(*bsp));
	bsp->bsp_res = r;

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_QUEUED] = BREPLST_GARBAGE_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);
	if (rc != BREPLST_GARBAGE_QUEUED) {
		OPSTAT_INCR("msl.preclaim-bmap-bail");
		DEBUG_BMAPOD(PLL_DEBUG, b, "bmap inconsistency: expected "
		    "state=GARBAGE at off %d", off);
		PFL_GOTOERR(out, rc = EINVAL);
	}

	rc = slrpc_batch_req_add(r,
	    &slm_db_hipri_workq, csvc, SRMT_PRECLAIM, SRMI_BULK_PORTAL,
	    SRIM_BULK_PORTAL, &q, sizeof(q), bsp,
	    &slm_batch_rep_preclaim, slm_upsch_preclaim_expire, 
	    slm_upsch_batch_size);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = mds_bmap_write_logrepls(b);
	if (rc) {
		OPSTAT_INCR("msl.bmap-write-err");
		psclog_warnx("bmap write: fid="SLPRI_FID", bno = %d, rc = %d", 
		    fcmh_2_fid(f), b->bcm_bmapno, rc);
	}
	return (1);

 out:
	PSCFREE(bsp);
	if (csvc)
		sl_csvc_decref(csvc);
	return (0);
}

int
slm_upsch_sched_repl(struct bmap_mds_info *bmi,  int dst_idx)
{
	int off, valid_exists = 0;
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
	FOREACH_RND(&src_res_i, fcmh_2_nrepls(f)) {
		if (src_res_i.ri_rnd_idx == dst_idx)
			continue;

		/* 
		 * (gdb) p ((struct fcmh_mds_info *)(f+1)) \
		 * 	->fmi_inodeh->inoh_extras.inox_repls
		 */ 
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

		si = res2iosinfo(dst_res);
		psclog_debug("attempt to "
		    "arrange repl with %s -> %s? siflg=%d",
		    src_res->res_name,
		    dst_res->res_name,
		    si->si_flags & (SIF_DISABLE_LEASE | SIF_DISABLE_ADVLEASE));

		if (si->si_flags & (SIF_DISABLE_LEASE | SIF_DISABLE_ADVLEASE)) {
			OPSTAT_INCR("upsch-skip-lease");
			continue;
		}

		psclog_debug("trying to arrange " "repl with %s -> %s", 
		    src_res->res_name, dst_res->res_name);

		/*
		 * Search source nodes for an idle, online connection.
		 */
		m = res_getmemb(src_res);
		csvc = slm_geticsvc(m, NULL,
		    CSVCF_NONBLOCK | CSVCF_NORECON, NULL, 0);
		if (csvc == NULL)
			continue;
		sl_csvc_decref(csvc);

		/* bail if success */
		if (slm_upsch_tryrepl(b, off, m, dst_res))
			goto out;
	}

	if (!valid_exists) {
		int tract[NBREPLST], retifset[NBREPLST];

		DEBUG_BMAPOD(PLL_DIAG, b, "no source "
		    "replicas exist; canceling "
		    "impossible replication request; "
		    "dst_ios=%s", dst_res->res_name);

		OPSTAT_INCR("upsch-impossible");

		brepls_init(tract, -1);
		tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE_QUEUED;

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
	int rc, off, val, invalid = 0, valid = 0;
	struct rnd_iterator dst_res_i;
	struct sl_resource *dst_res;
	struct bmap_mds_info *bmi;
	struct fidc_membh *f;
	struct bmap *b;
	sl_ios_id_t iosid;
	struct slash_inode_handle *ih;

	bmi = upd_getpriv(upd);
	b = bmi_2_bmap(bmi);
	f = b->bcm_fcmh;

	/*
 	 * We use FOREACH_RND() below, so make sure we load everything to
 	 * avoid a segment fault, which appears with heavy replication 
 	 * tests.
 	 */
	ih = fcmh_2_inoh(f);
	rc = mds_inox_ensure_loaded(ih);
	if (rc) {
		OPSTAT_INCR("repl-load-err");
		psclog_warnx("proc bmap: fid="SLPRI_FID", bno = %d, rc = %d", 
		    fcmh_2_fid(f), b->bcm_bmapno, rc);
		return;
	}

	DEBUG_FCMH(PLL_DEBUG, f, "upd=%p", upd);

	BMAP_LOCK(b);
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
			DEBUG_BMAP(PLL_DIAG, b, "invalid iosid: %u (0x%x)",
			    iosid, iosid);
			invalid++; 
			OPSTAT_INCR("upsch-invalid-ios");
			continue;
		}
		valid = 1;
		/*
 		 * This code tells me that the relative position of an I/O
 		 * server is the same in the per-file I/O list and per-bmap
 		 * bmap states. See also msl_try_get_replica_res().
 		 */
		off = SL_BITS_PER_REPLICA * dst_res_i.ri_rnd_idx;
		val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);
		switch (val) {
		case BREPLST_REPL_QUEUED:
			/*
			 * There is still a lease out; we'll wait for it
			 * to be relinquished.
			 */
			if (bmap_2_bmi(b)->bmi_wr_ion) {
				OPSTAT_INCR("upsch-skip-write");
				psclog_debug("skipping because write "
				    "lease still active");
				break;
			}
			psclog_debug("trying to arrange repl dst=%s",
			    dst_res->res_name);

			if (slm_upsch_sched_repl(bmi, dst_res_i.ri_rnd_idx))
				goto out;
			break;

		case BREPLST_TRUNC_QUEUED:
			rc = slm_upsch_tryptrunc(b, off, dst_res);
			break;

		case BREPLST_GARBAGE_QUEUED:
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

	if (invalid)
		DEBUG_FCMH(PLL_DIAG, f, "%d invalid IOS", invalid);
	if (!valid)
		DEBUG_FCMH(PLL_WARN, f, "no valid IOS present");
 out:

	BMAP_ULOCK(b);
}

int
upd_pagein_wk(void *p)
{
	uint32_t i;
	int valid = 0;
	struct slm_wkdata_upschq *wk = p;

	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	int rc, retifset[NBREPLST];
	struct sl_fidgen fg;
	sl_ios_id_t iosid;
	struct sl_resource *res;

	fg.fg_fid = wk->fid;
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

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_QUEUED] = 1;
	retifset[BREPLST_TRUNC_QUEUED] = 1;
	if (slm_preclaim_enabled) {
		retifset[BREPLST_GARBAGE_QUEUED] = 1;
		retifset[BREPLST_GARBAGE_SCHED] = 1;
	}

	BMAP_LOCK(b);
	for (i = 0; i < fcmh_2_nrepls(f); i++) {
		iosid = fcmh_2_repl(f, i);
		res = libsl_id2res(iosid);
		if (res != NULL) {
			valid = 1;
			break;
		}
	}
	if (!valid) {
		DEBUG_FCMH(PLL_WARN, f, "No valid IOS for bmap %d", 
		    b->bcm_bmapno);
		OPSTAT_INCR("upsch-no-invalid");
	}
	if (valid && mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT)) {
		OPSTAT_INCR("upsch-enqueue-work");
		upsch_enqueue(bmap_2_upd(b));
	} else {
		OPSTAT_INCR("upsch-enqueue-nowork");
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
		OPSTAT_INCR("upsch-purge-work");
		pfl_workq_putitemq(&slm_db_hipri_workq, purge_wk);
	}

	if (b)
		bmap_op_done(b);
	if (f) 
		fcmh_op_done(f);
	return (0);
}

int
upd_proc_pagein_cb(sqlite3_stmt *sth, void *p)
{
	struct slm_wkdata_upschq *wk;
	struct psc_dynarray *da = p;

	/*
 	 * Accumulate work items here and submit them in a batch later
 	 * so that we know when the paging is really done.
 	 */

	/* pfl_workrq_pool */
	wk = pfl_workq_getitem(upd_pagein_wk, struct slm_wkdata_upschq);
	wk->fid = sqlite3_column_int64(sth, 0);
	wk->bno = sqlite3_column_int(sth, 1);
	psc_dynarray_add(da, wk);
	return (0);
}

/*
 * Page in some work for the update scheduler to do.  This consults the
 * upsch database, potentially restricting to a single resource for work
 * to schedule.
 */
void
slm_page_work(struct sl_resource *r, struct psc_dynarray *da)
{
	int i, len;
	struct slm_wkdata_upschq *wk;

	/*
	 * Page some work in.  We make a heuristic here to avoid a large
	 * number of operations inside the database callback.
	 *
	 * This algorithm suffers because each piece of work pulled in
	 * is not technically fair.  But each invocation of this routine
	 * selects a different user at random, so over time, no users
	 * will starve.
	 */
    	r->res_offset = 0;
	while (1) {
		dbdo(upd_proc_pagein_cb, da,
		    " SELECT    fid,"
		    "           bno"
		    "		nonce"
		    " FROM      upsch"
		    " WHERE     resid = IFNULL(?, resid)"
		    " LIMIT     ?"   
		    " OFFSET    ?",  
		    SQLITE_INTEGER, r->res_id, 
		    SQLITE_INTEGER, UPSCH_PAGEIN_BATCH,
		    SQLITE_INTEGER, r->res_offset);
		DYNARRAY_FOREACH(wk, i, da) {
			OPSTAT_INCR("upsch-pagein-work");
			pfl_workq_putitem(wk);
		}
		len = psc_dynarray_len(da);
		if (len) {
		    	r->res_offset += len;
			psc_dynarray_reset(da);
			continue;
		}
		break;
	}
}

#if 0

	/* Keep this so I don't have to re-learn cryptic SQL syntax */
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
	    SQLITE_INTEGER, UPSCH_PAGEIN_BATCH);

#endif

void
upd_proc(struct slm_update_data *upd)
{
	upd_proc_bmap(upd);
	UPD_DECREF(upd);
}

int
slm_upsch_tally_cb(sqlite3_stmt *sth, void *p)
{
	int *val = p;

	*val = sqlite3_column_int(sth, 0);

	return (0);
}

/*
 * Called at startup to reset all in-progress changes back to starting
 * (e.g. SCHED -> QUEUED).
 */

int
slm_upsch_requeue_cb(sqlite3_stmt *sth, __unusedx void *p)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	struct sl_fidgen fg;
	sl_bmapno_t bno;

	OPSTAT_INCR("revert-cb");

	fg.fg_fid = sqlite3_column_int64(sth, 0);
	fg.fg_gen = FGEN_ANY;
	bno = sqlite3_column_int(sth, 1);

	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = bmap_getf(f, bno, SL_WRITE, BMAPGETF_CREATE, &b);
	if (rc)
		PFL_GOTOERR(out, rc);

	BMAP_ULOCK(b);
	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_QUEUED;

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	BMAP_LOCK(b);

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
	struct slm_wkdata_upschq *wk;
	int rc;

	r = libsl_id2res(resid);
	if (r == NULL)
		return (ESRCH);
	/*
	 * Constraints of the table: UNIQUE(resid, fid, bno).
	 */
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

	if (!rc) {
		/*
		 * XXX this is an optimization path, use non-blocking.
		 */
		wk = pfl_workq_getitem(upd_pagein_wk, struct slm_wkdata_upschq);
		wk->bno = b->bcm_bmapno;
		wk->fid = bmap_2_fid(b);
		pfl_workq_putitem(wk);
		OPSTAT_INCR("upsch-insert-ok");
	} else
		OPSTAT_INCR("upsch-insert-err");
	return (rc);
}

void
slmupschthr_main(struct psc_thread *thr)
{
	struct slm_update_data *upd;
	while (pscthr_run(thr)) {
		upd = lc_getwait(&slm_upsch_queue);
        	spinlock(&upd->upd_lock);
		upd->upd_flags &= ~UPDF_LIST;
        	freelock(&upd->upd_lock);
		upd_proc(upd);
	}
}

void
slmpagerthr_main(struct psc_thread *thr)
{
	int i, j;
	struct sl_resm *m;
	struct sl_site *s;
	struct timeval stall;
	struct sl_resource *r;
	struct slrpc_cservice *csvc;
	struct psc_dynarray da = DYNARRAY_INIT;

	stall.tv_usec = 0;
	psc_dynarray_ensurelen(&da, UPSCH_PAGEIN_BATCH);
	while (pscthr_run(thr)) {
		CONF_FOREACH_RESM(s, r, i, m, j) {
			if (!RES_ISFS(r))
				continue;
			csvc = slm_geticsvc(m, NULL, 
			    CSVCF_NONBLOCK | CSVCF_NORECON, NULL, 0);
			if (!csvc) {
				OPSTAT_INCR("upsch-page-skip");
				continue;
			}
			sl_csvc_decref(csvc);
			/*
 			 * Page work can happen in the following cases: 
 			 *
 			 * (1) definitely at start up (done)
 			 * (2) when an IOS comes online (to do)
 			 * (3) every 5 minutes (tunable)
 			 *
 			 * The page interval is chosen so that most likely
 			 * the work has already been done by our shortcut.
 			 */
			OPSTAT_INCR("upsch-page-work");
			slm_page_work(r, &da);
			psc_dynarray_reset(&da);
		}
		spinlock(&slm_upsch_lock);
		stall.tv_sec = slm_upsch_page_interval;
		psc_waitq_waitrel_tv(&slm_pager_workq, &slm_upsch_lock, &stall);
	}
	psc_dynarray_free(&da);
}

void
slm_upsch_init(void)
{
	INIT_SPINLOCK(&slm_upsch_lock);
	psc_waitq_init(&slm_upsch_waitq, "upsch");
	lc_reginit(&slm_upsch_queue, struct slm_update_data,
	    upd_lentry, "upschq");
}

void
slmupschthr_spawn(void)
{
	int i;
	struct psc_thread *thr;

	for (i = 0; i < SLM_NUPSCHED_THREADS; i++) {
		thr = pscthr_init(SLMTHRT_UPSCHED, slmupschthr_main, 
		    0, "slmupschthr%d", i);
		pscthr_setready(thr);
	}
	thr = pscthr_init(SLMTHRT_PAGER, slmpagerthr_main, 0, "slmpagerthr");
	pscthr_setready(thr);
}

/*
 * Initialize a peer resource update.
 * @upd: peer update structure.
 * @type: type of update.
 */
void
upd_init(struct slm_update_data *upd)
{
	upd->upd_flags = 0;
	INIT_PSC_LISTENTRY(&upd->upd_lentry);
	INIT_SPINLOCK(&upd->upd_lock);
}

void
upd_destroy(struct slm_update_data *upd)
{
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
        /* enqueue work for slmupschthr_main() */
        spinlock(&upd->upd_lock);
        if (!(upd->upd_flags & UPDF_LIST)) {
                upd->upd_flags |= UPDF_LIST;
                lc_add(&slm_upsch_queue, upd);
                UPD_INCREF(upd);
        }
        freelock(&upd->upd_lock);
}

void *
upd_getpriv(struct slm_update_data *upd)
{
	void *p = (void *)upd;
	return (p - offsetof(struct bmap_mds_info, bmi_upd));
}

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
