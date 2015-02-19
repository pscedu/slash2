/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
#define IN_RC		0
#define IN_OFF		1
#define IN_AMT		2

/* RPC callback pointer arg indexes */
#define IP_CSVC		0
#define IP_DSTRESM	1
#define IP_SRCRESM	2
#define IP_BMAP		3

struct psc_mlist	 slm_upschq;
struct psc_multiwait	 slm_upsch_mw;
struct psc_poolmaster	 slm_upgen_poolmaster;
struct psc_poolmgr	*slm_upgen_pool;

void (*upd_proctab[])(struct slm_update_data *);

void
upd_rpmi_add(struct resprof_mds_info *rpmi, struct slm_update_data *upd)
{
	int locked;

	locked = RPMI_RLOCK(rpmi);
	if (!psc_dynarray_add_ifdne(&rpmi->rpmi_upschq, upd))
		UPD_INCREF(upd);
	RPMI_URLOCK(rpmi, locked);
}

void
upd_rpmi_remove(struct resprof_mds_info *rpmi,
    struct slm_update_data *upd)
{
	int locked, idx;

	locked = RPMI_RLOCK(rpmi);
	idx = psc_dynarray_finditem(&rpmi->rpmi_upschq, upd);
	if (idx != -1)
		psc_dynarray_removepos(&rpmi->rpmi_upschq, idx);
	RPMI_URLOCK(rpmi, locked);
	if (idx != -1)
		UPD_DECREF(upd);
}

/**
 * Handle batch replication finish/error.  If success, we update the
 * bmap to the new residency states.  If error, we revert all changes
 * and set things back to a virgin state for future processing.
 */
void
slm_batch_repl_cb(struct batchrq *br, int ecode)
{
	sl_bmapgen_t bgen;
	int rc, idx, tract[NBREPLST], retifset[NBREPLST];
	struct srt_replwk_repent *bp = br->br_reply;
	struct sl_resm *dst_resm, *src_resm;
	struct slm_batchscratch_repl *bsr;
	struct srt_replwk_reqent *bq;
	struct fidc_membh *f;
	struct bmap *b;

	if (bp && bp->rc == 0)
		OPSTAT_INCR("repl_schedwk");
	else
		OPSTAT_INCR("repl_schedwk_fail");

	dst_resm = res_getmemb(br->br_res);

	brepls_init(tract, -1);
	brepls_init(retifset, 0);

	for (bq = br->br_buf, idx = 0;
	    (char *)bq < (char *)br->br_buf + br->br_len;
	    bq++, bp ? bp++ : 0, idx++) {
		b = NULL;
		f = NULL;
		bsr = psc_dynarray_getpos(&br->br_scratch, idx);
		src_resm = libsl_ios2resm(bq->src_resid);

		rc = slm_fcmh_get(&bq->fg, &f);
		if (rc)
			goto skip;
		rc = bmap_get(f, bq->bno, SL_WRITE, &b);
		if (rc)
			goto skip;

		// XXX check fgen

		BHGEN_GET(b, &bgen);
		if (bp && bp->rc == 0 && bq->bgen != bgen)
			bp->rc = SLERR_GEN_OLD;

		if (ecode == 0 && (char *)bp <
		    (char *)br->br_reply + br->br_replen &&
		    bp && bp->rc == 0) {
			tract[BREPLST_REPL_SCHED] = BREPLST_VALID;
			tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
			retifset[BREPLST_REPL_SCHED] = 1;
			retifset[BREPLST_REPL_QUEUED] = 1;

			OPSTAT2_ADD("replcompl", bsr->bsr_amt);
		} else {
			tract[BREPLST_REPL_QUEUED] =
			    BREPLST_REPL_QUEUED;
			tract[BREPLST_REPL_SCHED] =
			    bp == NULL || bp->rc == SLERR_ION_OFFLINE ?
			    BREPLST_REPL_QUEUED : BREPLST_GARBAGE;
			retifset[BREPLST_REPL_SCHED] = 1;
			retifset[BREPLST_REPL_QUEUED] = 0;

			DEBUG_BMAP(PLL_WARN, b, "replication "
			    "arrangement failure; src=%s dst=%s rc=%d",
			    src_resm ? src_resm->resm_name : NULL,
			    dst_resm ? dst_resm->resm_name : NULL,
			    ecode ? ecode : bp ? bp->rc : -999);
		}

		if (mds_repl_bmap_apply(b, tract, retifset,
		    bsr->bsr_off))
			mds_bmap_write_logrepls(b);
		slm_repl_bmap_rel(b);
		b = NULL;

 skip:
		if (b)
			bmap_op_done(b);
		if (f)
			fcmh_op_done(f);

		mds_repl_nodes_adjbusy(src_resm, dst_resm,
		    -bsr->bsr_amt, NULL);
		upschq_resm(dst_resm, UPDT_PAGEIN);
//		upschq_resm(src_resm, UPDT_PAGEIN);

		PSCFREE(bsr);
	}
}

/**
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
	struct slashrpc_cservice *csvc = NULL;
	struct slm_batchscratch_repl *bsr;
	struct srt_replwk_reqent pe;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;
	int64_t amt, moreamt;
	sl_bmapno_t lastbno;

	dst_resm = res_getmemb(dst_res);
	f = b->bcm_fcmh;

	amt = slm_bmap_calc_repltraffic(b);
	if (amt == 0) {
		/* No data in this bmap; simply mark as replicated. */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_QUEUED] = BREPLST_VALID;
		mds_repl_bmap_apply(b, tract, NULL, off);
		mds_bmap_write_logrepls(b);
		upschq_resm(dst_resm, UPDT_PAGEIN);
//		upschq_resm(src_resm, UPDT_PAGEIN);
		return (1);
	}

	spinlock(&repl_busytable_lock);
	amt = mds_repl_nodes_adjbusy(src_resm, dst_resm, amt, &moreamt);
	if (amt == 0) {
		/*
		 * No bandwidth available.
		 * Add "src to become unbusy" condition to multiwait.
		 */
		psc_multiwait_setcondwakeable(&slm_upsch_mw,
		    &src_resm->resm_csvc->csvc_mwc, 1);
		psc_multiwait_setcondwakeable(&slm_upsch_mw,
		    &dst_resm->resm_csvc->csvc_mwc, 1);

		/* push batch out immediately */
	}
	freelock(&repl_busytable_lock);
	if (amt == 0)
		return (0);

	bsr = PSCALLOC(sizeof(*bsr));
	bsr->bsr_off = off;
	bsr->bsr_amt = amt;

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, &slm_upsch_mw);
	if (csvc == NULL)
		PFL_GOTOERR(fail, rc = resm_getcsvcerr(dst_resm));

	pe.fg = b->bcm_fcmh->fcmh_fg;
	pe.bno = b->bcm_bmapno;
	BHGEN_GET(b, &pe.bgen);
	pe.src_resid = src_resm->resm_res_id;
	pe.len = SLASH_BMAP_SIZE;
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
			PFL_GOTOERR(fail, rc = -ENODATA);

		pe.len = MIN(sz, SLASH_BMAP_SIZE);
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
	if (rc != BREPLST_REPL_QUEUED)
		PFL_GOTOERR(fail, rc = -ENODEV);

	rc = batchrq_add(dst_res, csvc, SRMT_REPL_SCHEDWK,
	    SRMI_BULK_PORTAL, SRIM_BULK_PORTAL, &pe, sizeof(pe), bsr,
	    slm_batch_repl_cb, 5);
	if (rc)
		PFL_GOTOERR(fail, rc);

	OPSTAT2_ADD("replsched", amt);

	dbdo(NULL, NULL,
	    " UPDATE	upsch"
	    " SET	status = 'S'"
	    " WHERE	resid = ?"
	    "   AND	fid = ?"
	    "   AND	bno = ?",
	    SQLITE_INTEGER, dst_res->res_id,
	    SQLITE_INTEGER64, bmap_2_fid(b),
	    SQLITE_INTEGER, b->bcm_bmapno);

	/*
	 * We use this release API because mds_repl_bmap_apply() grabbed
	 * the necessary write locks that we must relinquish because we
	 * do not write changes to disk.
	 */
	bmap_op_start_type(b, BMAP_OPCNT_UPSCH);
	slm_repl_bmap_rel_type(b, BMAP_OPCNT_UPSCH);

	/*
	 * We succesfully scheduled some work; if there is more
	 * bandwidth available, schedule more.
	 */
	if (moreamt)
		upschq_resm(dst_resm, UPDT_PAGEIN);

	return (1);

 fail:
	DEBUG_BMAP(PLL_DIAG, b, "dst_resm=%s src_resm=%s rc=%d",
	    dst_resm->resm_name, src_resm->resm_name, rc);

	if (chg) {
		/* undo brepls change */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
		mds_repl_bmap_apply(b, tract, NULL, off);
		bmap_op_start_type(b, BMAP_OPCNT_UPSCH);
		slm_repl_bmap_rel_type(b, BMAP_OPCNT_UPSCH);
	}

	if (bsr->bsr_amt)
		mds_repl_nodes_adjbusy(src_resm, dst_resm,
		    -bsr->bsr_amt, NULL);

	UPSCH_WAKE();

	PSCFREE(bsr);

	if (csvc)
		sl_csvc_decref(csvc);
	return (0);
}

void
slm_upsch_finish_ptrunc(struct slashrpc_cservice *csvc,
    struct sl_resm *dst_resm, struct bmap *b, int rc, int off)
{
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	int tract[NBREPLST];

	if (rc && b) {
		/* undo brepls changes */
		brepls_init(tract, -1);
		tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;
		mds_repl_bmap_apply(b, tract, NULL, off);

		rpmi = res2rpmi(dst_resm->resm_res);
		upd = bmap_2_upd(b);
		upd_rpmi_remove(rpmi, upd);
	}

	psclog(rc ? PLL_WARN: PLL_DIAG,
	    "partial truncation resolution failed rc=%d", rc);

	if (csvc)
		sl_csvc_decref(csvc);
	if (b)
		bmap_op_done_type(b, BMAP_OPCNT_UPSCH);
	UPSCH_WAKE();
}

int
slm_upsch_wk_finish_ptrunc(void *p)
{
	struct slm_wkdata_upsch_cb *wk = p;
	struct fidc_membh *f;

	f = wk->b->bcm_fcmh;
	/* skip; there is more important work to do */
	if (!FCMH_TRYBUSY(f))
		return (1);
	fcmh_op_start_type(f, FCMH_OPCNT_UPSCH);
	slm_upsch_finish_ptrunc(wk->csvc, wk->dst_resm, wk->b, wk->rc,
	    wk->off);
	if (FCMH_HAS_BUSY(f))
		FCMH_UNBUSY(f);
	fcmh_op_done_type(f, FCMH_OPCNT_UPSCH);
	return (0);
}

int
slm_upsch_tryptrunc_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slashrpc_cservice *csvc = av->pointer_arg[IP_CSVC];
	struct slm_wkdata_upsch_cb *wk;
	int rc = 0;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_ptrunc_rep, rc);
	if (rc == 0)
		rc = av->space[IN_RC];
	if (rc == 0)
		slrpc_rep_in(csvc, rq);

	if (rc)
		DEBUG_REQ(PLL_ERROR, rq, "rc=%d", rc);

	wk = pfl_workq_getitem(slm_upsch_wk_finish_ptrunc,
	    struct slm_wkdata_upsch_cb);
	wk->rc = rc;
	wk->csvc = csvc;
	wk->b = av->pointer_arg[IP_BMAP];
	wk->off = av->space[IN_OFF];
	wk->dst_resm = av->pointer_arg[IP_DSTRESM];
	pfl_workq_putitemq(&slm_db_lopri_workq, wk);
	return (0);
}

/**
 * Try to issue a PTRUNC resolution to an ION.
 * Returns:
 *   -1	: The activity can never happen; give up.
 *    0	: Unsuccessful.
 *    1	: Success.
 */
int
slm_upsch_tryptrunc(struct bmap *b, int off,
    struct sl_resource *dst_res)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc;
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct pscrpc_async_args av;
	struct slm_update_data *upd;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;

	upd = bmap_2_upd(b);
	f = upd_2_fcmh(upd);
	dst_resm = res_getmemb(dst_res);

	memset(&av, 0, sizeof(av));
	av.pointer_arg[IP_DSTRESM] = dst_resm;
	av.space[IN_OFF] = off;

	/*
	 * If another ION is already handling this ptrunc CRC
	 * recomputation, go do something else.
	 */
	brepls_init(retifset, 0);
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;
	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		return (-1);

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, &slm_upsch_mw);
	if (csvc == NULL)
		PFL_GOTOERR(fail, rc = resm_getcsvcerr(dst_resm));
	av.pointer_arg[IP_CSVC] = csvc;

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAP_PTRUNC, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(fail, rc);
	mq->fg = f->fcmh_fg;
	mq->bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &mq->bgen);
	mq->offset = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;

	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	if (rc == BREPLST_TRUNCPNDG) {
		av.pointer_arg[IP_BMAP] = b;
		bmap_op_start_type(b, BMAP_OPCNT_UPSCH);

		upd_rpmi_add(res2rpmi(dst_resm->resm_res), upd);

		rq->rq_interpret_reply = slm_upsch_tryptrunc_cb;
		rq->rq_async_args = av;
		rc = SL_NBRQSET_ADD(csvc, rq);
		if (rc == 0) {
			bmap_op_start_type(b, BMAP_OPCNT_UPSCH);
			slm_repl_bmap_rel_type(b, BMAP_OPCNT_UPSCH);
			return (1);
		}
	} else
		rc = -ENODEV;

 fail:
	if (rq)
		pscrpc_req_finished(rq);
	slm_upsch_finish_ptrunc(av.pointer_arg[IP_CSVC],
	    dst_resm, av.pointer_arg[IP_BMAP], rc, off);
	return (0);
}

void
slm_batch_preclaim_cb(struct batchrq *br, int rc)
{
	sl_replica_t repl;
	int idx, tract[NBREPLST];
	struct resprof_mds_info *rpmi;
	struct srt_preclaim_reqent *pe;
	struct fidc_membh *f;
	struct bmap *b;

	if (rc == -PFLERR_NOTSUP) {
		rpmi = res2rpmi(br->br_res);
		RPMI_LOCK(rpmi);
		res2iosinfo(br->br_res)->si_flags |=
		    SIF_PRECLAIM_NOTSUP;
		RPMI_ULOCK(rpmi);
	}
	repl.bs_id = br->br_res->res_id;

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE_SCHED] = rc ?
	    BREPLST_GARBAGE : BREPLST_INVALID;

	for (pe = br->br_buf; (char *)pe <
	    (char *)br->br_buf + br->br_len; pe++) {
		rc = slm_fcmh_get(&pe->fg, &f);
		if (rc)
			continue;
		rc = bmap_get(f, pe->bno, SL_WRITE, &b);
		if (rc)
			goto fail;
		rc = mds_repl_iosv_lookup(current_vfsid, fcmh_2_inoh(f),
		    &repl, &idx, 1);
		if (rc >= 0) {
			mds_repl_bmap_walk(b, tract, NULL, 0, &idx, 1);
			mds_bmap_write_logrepls(b);
		}
 fail:
		if (b)
			bmap_op_done(b);
		fcmh_op_done(f);
	}
}

int
slm_upsch_trypreclaim(struct sl_resource *r, struct bmap *b, int off)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
	struct slashrpc_cservice *csvc;
	struct srt_preclaim_reqent pe;
	struct sl_mds_iosinfo *si;
	struct sl_resm *m;

	si = res2iosinfo(r);
	if (si->si_flags & SIF_PRECLAIM_NOTSUP)
		return (0);

	m = res_getmemb(r);
	csvc = slm_geticsvc(m, NULL, CSVCF_NONBLOCK | CSVCF_NORECON,
	    &slm_upsch_mw);
	if (csvc == NULL)
		PFL_GOTOERR(fail, rc = resm_getcsvcerr(m));

	pe.fg = b->bcm_fcmh->fcmh_fg;
	pe.bno = b->bcm_bmapno;
	BHGEN_GET(b, &pe.bgen);

	rc = batchrq_add(r, csvc, SRMT_PRECLAIM, SRIM_BULK_PORTAL,
	    SRIM_BULK_PORTAL, &pe, sizeof(pe), NULL,
	    slm_batch_preclaim_cb, 30);
	if (rc)
		PFL_GOTOERR(fail, rc);

	brepls_init(tract, -1);
	tract[BREPLST_GARBAGE] = BREPLST_GARBAGE_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	if (rc != BREPLST_GARBAGE)
		psclog_errorx("consistency error");

	rc = mds_bmap_write_logrepls(b);

 fail:
	if (rc && rc != -PFLERR_NOTCONN)
		psclog_errorx("error rc=%d", rc);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc ? 0 : 1);
}

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
		if (mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx,
		    1))
			mds_bmap_write_logrepls(b);
		else {
			BMAPOD_MODIFY_DONE(b, 0);
			BMAP_UNBUSY(b);
			FCMH_UNBUSY(b->bcm_fcmh);
		}
 next:
		UPD_DECREF(upd);

		RPMI_LOCK(rpmi);
	}
	RPMI_ULOCK(rpmi);
	slm_iosv_clearbusy(&repl, 1);
	mds_repl_node_clearallbusy(upg->upg_resm);
}

void
upd_proc_bmap(struct slm_update_data *upd)
{
	int rc, off, val, pass, iosidx;
	struct rnd_iterator dst_res_i, src_res_i;
	struct sl_resource *dst_res, *src_res;
	struct slashrpc_cservice *csvc;
	struct sl_mds_iosinfo *si;
	struct bmap_mds_info *bmi;
	struct fidc_membh *f;
	struct sl_resm *m;
	struct bmap *b;
	sl_ios_id_t iosid;

	bmi = upd_getpriv(upd);
	b = bmi_2_bmap(bmi);
	f = b->bcm_fcmh;

	/* skip, there is more important work to do */
	if (b->bcm_flags & BMAP_MDS_REPLMODWR)
		return;

//	if (IN_PTRUNC)

	UPD_UNBUSY(upd);

	FCMH_WAIT_BUSY(f);
	FCMH_ULOCK(f);

	BMAP_WAIT_BUSY(b);
	BMAP_ULOCK(b);

	UPD_WAIT(upd);
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();

	DEBUG_BMAPOD(PLL_DEBUG, b, "processing");

	/*
	 * Scan residency states (through file's inode table) of bmap
	 * for an update.
	 */
	iosidx = -1;
	FOREACH_RND(&dst_res_i, fcmh_2_nrepls(f)) {
		iosid = fcmh_2_repl(f, dst_res_i.ri_rnd_idx);
		dst_res = libsl_id2res(iosid);
		if (dst_res == NULL) {
			DEBUG_BMAP(PLL_ERROR, b, "invalid iosid: %u",
			    iosid);
			continue;
		}
		off = SL_BITS_PER_REPLICA * dst_res_i.ri_rnd_idx;
		val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);
		switch (val) {
		case BREPLST_REPL_QUEUED:
			/*
			 * There is still a lease out; we'll wait for it
			 * to be relinquished.
			 *
			 * XXX: mask mwc off and make sure lease
			 * relinquishment masks us back on and wakes up.
			 */
			if (bmap_2_bmi(b)->bmi_wr_ion) {
				psclog_debug("skipping because write "
				    "lease still active");
				break;
			}
			psclog_debug("trying to arrange repl dst=%s",
			    dst_res->res_name);

			/* look for a repl source */
			for (pass = 0; pass < 2; pass++) {
				FOREACH_RND(&src_res_i, fcmh_2_nrepls(f)) {
					src_res = libsl_id2res(
					    fcmh_getrepl(f,
					    src_res_i.ri_rnd_idx).bs_id);

					/*
					 * Skip ourself and old/inactive
					 * replicas.
					 */
					if (src_res == NULL ||
					    src_res_i.ri_rnd_idx == iosidx ||
					    SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls,
					    SL_BITS_PER_REPLICA *
					    src_res_i.ri_rnd_idx) != BREPLST_VALID)
						continue;

					si = res2iosinfo(src_res);

					psclog_debug("attempt to "
					    "arrange repl with %s -> %s? "
					    "pass=%d siflg=%d",
					    src_res->res_name,
					    dst_res->res_name,
					    pass, !!(si->si_flags &
					    (SIF_DISABLE_LEASE |
					     SIF_DISABLE_ADVLEASE)));

					if (pass ^
					    (src_res->res_type ==
					     SLREST_ARCHIVAL_FS ||
					     !!(si->si_flags &
					     (SIF_DISABLE_LEASE |
					      SIF_DISABLE_ADVLEASE))))
						continue;

					psclog_debug("trying to arrange "
					    "repl with %s -> %s",
					    src_res->res_name,
					    dst_res->res_name);

					/*
					 * Search source nodes for an
					 * idle, online connection.
					 */
					m = res_getmemb(src_res);
					csvc = slm_geticsvc(m, NULL,
					    CSVCF_NONBLOCK |
					    CSVCF_NORECON,
					    &slm_upsch_mw);
					if (csvc == NULL)
						continue;
					sl_csvc_decref(csvc);

					if (slm_upsch_tryrepl(b, off, m,
					    dst_res))
						return;
				}
			}
			break;

		case BREPLST_TRUNCPNDG:
			rc = slm_upsch_tryptrunc(b, off, dst_res);
			if (rc > 0)
				return;
			break;

		case BREPLST_GARBAGE:
			rc = slm_upsch_trypreclaim(dst_res, b, off);
			if (rc > 0)
				return;
			break;
		}
	}
	if (BMAPOD_HASWRLOCK(bmap_2_bmi(b)))
		BMAPOD_MODIFY_DONE(b, 0);
	BMAP_UNBUSY(b);
	FCMH_UNBUSY(f);
}

void
upd_proc_pagein_unit(struct slm_update_data *upd)
{
	struct bmap_mds_info *bmi = NULL;
	struct slm_update_generic *upg;
	struct fidc_membh *f = NULL;
	struct bmap *b = NULL;
	int rc, retifset[NBREPLST];

	upg = upd_getpriv(upd);
	rc = slm_fcmh_get(&upg->upg_fg, &f);
	if (rc)
		goto out;
	rc = bmap_get(f, upg->upg_bno, SL_WRITE, &b);
	if (rc)
		goto out;
	bmi = bmap_2_bmi(b);

	if (fcmh_2_nrepls(f) > SL_DEF_REPLICAS)
		mds_inox_ensure_loaded(fcmh_2_inoh(f));

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_QUEUED] = 1;
//	retifset[BREPLST_GARBAGE_QUEUED] = 1;
	retifset[BREPLST_TRUNCPNDG] = 1;

	BMAP_WAIT_BUSY(b);
	BMAPOD_WRLOCK(bmi);

	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		upsch_enqueue(bmap_2_upd(b));
	else
		rc = 1;

 out:
	if (rc) {
		struct slm_wkdata_upsch_purge *wk;

		wk = pfl_workq_getitem(slm_wk_upsch_purge,
		    struct slm_wkdata_upsch_purge);
		wk->fid = upg->upg_fg.fg_fid;
		if (b)
			wk->bno = b->bcm_bmapno;
		else
			wk->bno = BMAPNO_ANY;
		pfl_workq_putitemq(&slm_db_lopri_workq, wk);
	}
	if (b) {
		BMAPOD_ULOCK(bmi);
		BMAP_UNBUSY(b);
		bmap_op_done(b);
	}
	if (f)
		fcmh_op_done(f);
}

int
upd_pagein_wk(void *p)
{
	struct slm_wkdata_upschq *wk = p;
	struct slm_update_generic *upg;

	upg = psc_pool_get(slm_upgen_pool);
	memset(upg, 0, sizeof(*upg));
	INIT_PSC_LISTENTRY(&upg->upg_lentry);
	upd_init(&upg->upg_upd, UPDT_PAGEIN_UNIT);
	upg->upg_fg.fg_fid = wk->fg.fg_fid;
	upg->upg_fg.fg_gen = FGEN_ANY;
	upg->upg_bno = wk->bno;
	upsch_enqueue(&upg->upg_upd);
	UPD_UNBUSY(&upg->upg_upd);
	return (0);
}

int
upd_proc_pagein_cb(struct slm_sth *sth, __unusedx void *p)
{
	struct slm_wkdata_upschq *wk;

	wk = pfl_workq_getitem(upd_pagein_wk, struct slm_wkdata_upschq);
	wk->fg.fg_fid = sqlite3_column_int64(sth->sth_sth, 0);
	wk->bno = sqlite3_column_int(sth->sth_sth, 1);
	pfl_workq_putitem(wk);
	return (0);
}

void
upd_proc_pagein(struct slm_update_data *upd)
{
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *r;

	upg = upd_getpriv(upd);
	if (upg->upg_resm) {
		r = upg->upg_resm->resm_res;
		rpmi = res2rpmi(r);
		si = res2iosinfo(r);

		RPMI_LOCK(rpmi);
		si->si_flags &= ~SIF_UPSCH_PAGING;
		RPMI_ULOCK(rpmi);
	}

	/*
	 * Page some work in.  We make a heuristic here to avoid a large
	 * number of operations inside the database callback.
	 *
	 * This algorithm suffers because each piece of work pulled in
	 * is not technically fair.  But each invocation of this routine
	 * selects a different user at random, so over time, no users
	 * will starve.
	 */
	dbdo(upd_proc_pagein_cb, NULL,
	    " SELECT	fid,"
	    "		bno,"
	    "		nonce"
	    " FROM	upsch u,"
	    "		gsort gs,"
	    "		usort us"
	    " WHERE	resid = IFNULL(?, resid)"
	    "   AND	status = 'Q'"
	    "	AND	gs.gid = u.gid"
	    "	AND	us.uid = u.uid"
	    " ORDER BY	sys_prio DESC,"
	    "		gs.rnd,"
	    "		us.rnd,"
	    "		usr_prio DESC,"
	    "		RANDOM()"
	    " LIMIT	32",
	    upg->upg_resm ? SQLITE_INTEGER : SQLITE_NULL,
	    upg->upg_resm ? r->res_id : 0);
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
	int locked;

	locked = UPSCH_HASLOCK();
	if (locked)
		UPSCH_ULOCK();

	UPD_LOCK(upd);
	UPD_WAIT(upd);
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();
	upd_proctab[upd->upd_type](upd);
	upd->upd_flags &= ~UPDF_BUSY;
	UPD_WAKE(upd);
	UPD_ULOCK(upd);

	if (locked)
		UPSCH_LOCK();

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
slm_upsch_revert_cb(struct slm_sth *sth, __unusedx void *p)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	struct fidc_membh *f = NULL;
	struct sl_fidgen fg;
	struct bmap *b = NULL;
	sl_bmapno_t bno;

	fg.fg_fid = sqlite3_column_int64(sth->sth_sth, 0);
	fg.fg_gen = FGEN_ANY;
	bno = sqlite3_column_int(sth->sth_sth, 1);

	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = bmap_getf(f, bno, SL_WRITE, BMAPGETF_LOAD, &b);
	if (rc)
		PFL_GOTOERR(out, rc);

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	rc = mds_repl_bmap_walk_all(b, tract, retifset, 0);
	if (rc) {
		mds_bmap_write_logrepls(b);
	} else {
		BMAPOD_MODIFY_DONE(b, 0);
		BMAP_UNBUSY(b);
		FCMH_UNBUSY(f);
	}

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

void
slm_upsch_insert(struct bmap *b, sl_ios_id_t resid, int sys_prio,
    int usr_prio)
{
	struct sl_resource *r;

	dbdo(NULL, NULL,
	    " INSERT INTO upsch ("
	    "	resid, fid, bno, uid, gid, status, sys_prio, usr_prio, nonce "
	    ") VALUES ("
	    "	?,     ?,   ?,   ?,   ?,   'Q',    ?,        ?,        ?"
	    ")",
	    SQLITE_INTEGER, resid,
	    SQLITE_INTEGER64, bmap_2_fid(b),
	    SQLITE_INTEGER, b->bcm_bmapno,
	    SQLITE_INTEGER, b->bcm_fcmh->fcmh_sstb.sst_uid,
	    SQLITE_INTEGER, b->bcm_fcmh->fcmh_sstb.sst_gid,
	    SQLITE_INTEGER, sys_prio,
	    SQLITE_INTEGER, usr_prio,
	    SQLITE_INTEGER, sl_sys_upnonce);
	r = libsl_id2res(resid);
	upschq_resm(res_getmemb(r), UPDT_PAGEIN);
}

void
slmupschthr_main(struct psc_thread *thr)
{
	struct slm_update_data *upd;
	struct sl_resource *r;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j, rc;

	while (pscthr_run(thr)) {
		CONF_FOREACH_RESM(s, r, i, m, j)
			if (RES_ISFS(r))
				psc_multiwait_setcondwakeable(&slm_upsch_mw,
				    &m->resm_csvc->csvc_mwc, 0);

		UPSCH_LOCK();
		psc_multiwait_entercritsect(&slm_upsch_mw);
		upd = psc_mlist_tryget(&slm_upschq);
		if (upd)
			upd_proc(upd);
		UPSCH_ULOCK();
		if (upd)
			psc_multiwait_leavecritsect(&slm_upsch_mw);
		else {
			rc = psc_multiwait_secs(&slm_upsch_mw, &upd, 30);
			if (rc == -ETIMEDOUT)
				upschq_resm(NULL, UPDT_PAGEIN);
		}
	}
}

void
slm_upsch_init(void)
{
	psc_poolmaster_init(&slm_upgen_poolmaster,
	    struct slm_update_generic, upg_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "upgen");
	slm_upgen_pool = psc_poolmaster_getmgr(&slm_upgen_poolmaster);

	psc_mlist_reginit(&slm_upschq, NULL, struct slm_update_data,
	    upd_lentry, "upschq");

	mds_repl_buildbusytable();
}

void
slmupschthr_spawn(void)
{
	struct psc_thread *thr;
	struct sl_resource *r;
	struct sl_site *s;
	struct sl_resm *m;
	int i, j;

	psc_multiwait_init(&slm_upsch_mw, "upsch");
	if (psc_multiwait_addcond(&slm_upsch_mw,
	    &slm_upschq.pml_mwcond_empty) == -1)
		psc_fatal("psc_multiwait_addcond");

	CONF_FOREACH_RESM(s, r, i, m, j)
		if (RES_ISFS(r))
			psc_multiwait_addcond(&slm_upsch_mw,
			    &m->resm_csvc->csvc_mwc);

	thr = pscthr_init(SLMTHRT_UPSCHED, slmupschthr_main, NULL,
	    sizeof(struct slmupsch_thread), "slmupschthr");
	pscthr_setready(thr);

	/* page in initial replrq workload */
	CONF_FOREACH_RES(s, r, i)
		if (RES_ISFS(r))
			upschq_resm(res_getmemb(r), UPDT_PAGEIN);
}

/**
 * upschq_resm - Schedule a PAGEIN for a resm.
 */
void
upschq_resm(struct sl_resm *m, int type)
{
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	struct sl_mds_iosinfo *si;

	if (type == UPDT_PAGEIN && m) {
		int proc = 1;

		rpmi = res2rpmi(m->resm_res);
		si = res2iosinfo(m->resm_res);
		RPMI_LOCK(rpmi);
		if (si->si_flags & SIF_UPSCH_PAGING)
			proc = 0;
		else
			si->si_flags |= SIF_UPSCH_PAGING;
		RPMI_ULOCK(rpmi);

		if (!proc)
			return;
	}
	upg = psc_pool_get(slm_upgen_pool);
	memset(upg, 0, sizeof(*upg));
	INIT_PSC_LISTENTRY(&upg->upg_lentry);
	upg->upg_resm = m;
	upd = &upg->upg_upd;
	upd_init(upd, type);
	upsch_enqueue(upd);
	UPD_UNBUSY(upd);
}

/**
 * upd_initf - Initialize a peer resource update.
 * @upd: peer update structure.
 * @type: type of update.
 * @flags: operation flags.
 */
void
upd_initf(struct slm_update_data *upd, int type, __unusedx int flags)
{
	psc_assert(pfl_memchk(upd, 0, sizeof(*upd)) == 1);
	INIT_PSC_LISTENTRY(&upd->upd_lentry);
	upd->upd_type = type;
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();
	psc_mutex_init(&upd->upd_mutex);
	psc_multiwaitcond_init(&upd->upd_mwc, upd, 0, "upd-%p", upd);

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
	psc_mutex_destroy(&upd->upd_mutex);
	psc_multiwaitcond_destroy(&upd->upd_mwc);
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
	int locked;

	locked = UPD_RLOCK(upd);
	if (!psc_mlist_conjoint(&slm_upschq, upd)) {
		if (upd->upd_type == UPDT_BMAP &&
		    (upd_2_fcmh(upd)->fcmh_flags & FCMH_MDS_IN_PTRUNC) == 0)
			psc_mlist_addtail(&slm_upschq, upd);
		else
			psc_mlist_addhead(&slm_upschq, upd);
		UPD_INCREF(upd);
	}
	UPD_URLOCK(upd, locked);
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
		return (p - offsetof(struct slm_update_generic,
		    upg_upd));
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

void (*upd_proctab[])(struct slm_update_data *) = {
	upd_proc_bmap,
	upd_proc_hldrop,
	upd_proc_pagein,
	upd_proc_pagein_unit
};
