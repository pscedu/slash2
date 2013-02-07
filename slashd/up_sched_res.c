/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/fs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/treeutil.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/mlist.h"
#include "psc_util/multiwait.h"
#include "psc_util/pthrutil.h"
#include "psc_util/random.h"
#include "psc_util/thread.h"

#include "bmap_mds.h"
#include "mdsio.h"
#include "odtable_mds.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slerr.h"
#include "slutil.h"
#include "up_sched_res.h"
#include "worker.h"

extern int current_vfsid;

/* RPC callback numeric arg indexes */
#define IN_RC		0
#define IN_UNDO_WR	1
#define IN_OFF		2
#define IN_AMT		3

/* RPC callback pointer arg indexes */
#define IP_CSVC		0
#define IP_DSTRESM	1
#define IP_SRCRESM	2
#define IP_BMAP		3

struct psc_mlist	 slm_upschq;
struct psc_multiwait	 slm_upsch_mw;
struct psc_poolmaster	 slm_upgen_poolmaster;
struct psc_poolmgr	*slm_upgen_pool;

extern int (*upd_proctab[])(struct slm_update_data *);

void
upd_tryremove(struct slm_update_data *upd)
{
	struct bmapc_memb *b = upd_2_bmap(upd);
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	int lk, retifset[NBREPLST];

	brepls_init(retifset, 1);
	retifset[BREPLST_VALID] = 0;
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_GARBAGE] = 0;

	lk = BMAPOD_REQRDLOCK(bmi);
	if (!mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT)) {
		if (upd->upd_recpt) {
			UPD_LOCK(upd);
			mds_odtable_freeitem(slm_repl_odt,
			    upd->upd_recpt);
			upd->upd_recpt = NULL;
			UPD_ULOCK(upd);
			DEBUG_UPD(PLL_DIAG, upd,
			    "removed odtable entry");
		} else {
			psclog_errorx("no upd_recpt, "
			    "likely a very serious problem");
		}
	}
	BMAPOD_UREQLOCK(bmi, lk);
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

int
slmupschedthr_tryrepldst_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slashrpc_cservice *csvc = av->pointer_arg[IP_CSVC];
	struct sl_resm *dst_resm = av->pointer_arg[IP_DSTRESM];
	struct sl_resm *src_resm = av->pointer_arg[IP_SRCRESM];
	struct bmapc_memb *b = av->pointer_arg[IP_BMAP];
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	int64_t amt = av->space[IN_AMT];
	int rc = 0, tract[NBREPLST];

	if (rq)
		SL_GET_RQ_STATUS_TYPE(csvc, rq,
		    struct srm_repl_schedwk_rep, rc);
	if (rc == 0)
		rc = av->space[IN_RC];

	if (rc) {
		if (b) {
			/* undo brepls change */
			brepls_init(tract, -1);
			tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
			mds_repl_bmap_apply(b, tract, NULL,
			    av->space[IN_OFF]);
			if (av->space[IN_UNDO_WR])
				mds_bmap_write_logrepls(b);
		}

		if (rq)
			DEBUG_REQ(PLL_ERROR, rq,
			    "dst_resm=%s src_resm=%s rc=%d",
			    dst_resm->resm_name, src_resm->resm_name,
			    rc);
		if (b)
			DEBUG_BMAP(PLL_ERROR, b,
			    "dst_resm=%s src_resm=%s rq=%p rc=%d",
			    dst_resm->resm_name, src_resm->resm_name,
			    rq, rc);
	} else {
		/* Run standard successful RPC processing. */
		slrpc_rep_in(csvc, rq);
		amt = 0;
	}

	/*
	 * Remove update from queue.
	 *   (1) if successful, we are done.
	 *   (2) if network failure, this will will be paged back in and
	 *	 tried again when the connection is reestablished.
	 *   (3) if other failure, don't want to keep it in the system.
	 */
	if (b) {
		rpmi = res2rpmi(dst_resm->resm_res);
		upd = bmap_2_upd(b);
		upd_rpmi_remove(rpmi, upd);
	}

	if (amt)
		mds_repl_nodes_adjbusy(src_resm, dst_resm, -amt);
	if (csvc)
		sl_csvc_decref(csvc);
	if (b)
		slm_repl_bmap_rel_type(b, BMAP_OPCNT_UPSCH);
	UPSCH_WAKE();
	return (0);
}

int
slmupschedthr_tryrepldst(struct slm_update_data *upd,
    struct bmapc_memb *b, int off, struct sl_resm *src_resm,
    struct sl_resource *dst_res, int j)
{
	int tract[NBREPLST], retifset[NBREPLST], rc = 0;
	struct pscrpc_request *rq = NULL;
	struct srm_repl_schedwk_req *mq;
	struct srm_repl_schedwk_rep *mp;
	struct slashrpc_cservice *csvc;
	struct resprof_mds_info *rpmi;
	struct pscrpc_async_args av;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;
	sl_bmapno_t lastbno;
	int64_t amt = 0;

	f = upd_2_fcmh(upd);
	dst_resm = psc_dynarray_getpos(&dst_res->res_members, j);

	memset(&av, 0, sizeof(av));
	av.pointer_arg[IP_DSTRESM] = dst_resm;
	av.pointer_arg[IP_SRCRESM] = src_resm;
	av.space[IN_OFF] = off;

	/* HLDROP can reset this */
	rpmi = res2rpmi(dst_resm->resm_res);
	RPMI_LOCK(rpmi);
	if (!psc_dynarray_add_ifdne(&rpmi->rpmi_upschq, upd))
		UPD_INCREF(upd);
	RPMI_ULOCK(rpmi);

	csvc = slm_geticsvc(dst_resm, NULL, CSVCF_NONBLOCK |
	    CSVCF_NORECON, &slm_upsch_mw);
	if (csvc == NULL)
		PFL_GOTOERR(fail, rc = resm_getcsvcerr(dst_resm));
	av.pointer_arg[IP_CSVC] = csvc;

	/*
	 * If upsch is still processing, we retain the lock.
	 * Otherwise, it would have been released while it waits for the
	 * callback to be issued.
	 */
	amt = slm_bmap_calc_repltraffic(b);

	spinlock(&repl_busytable_lock);
	amt = mds_repl_nodes_adjbusy(src_resm, dst_resm, amt);
	if (amt == 0) {
		/* add "src to become unbusy" condition to multiwait */
		psc_multiwait_setcondwakeable(&slm_upsch_mw,
		    &src_resm->resm_csvc->csvc_mwc, 1);
		psc_multiwait_setcondwakeable(&slm_upsch_mw,
		    &dst_resm->resm_csvc->csvc_mwc, 1);
	}
	freelock(&repl_busytable_lock);
	if (amt == 0)
		PFL_GOTOERR(fail, rc = -EBUSY);

	av.space[IN_AMT] = amt;

	/* Issue replication work request */
	rc = SL_RSX_NEWREQ(csvc, SRMT_REPL_SCHEDWK, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(fail, rc);
	OPSTAT_INCR(SLM_OPST_REPL_SCHEDWK);

	mq->src_resid = src_resm->resm_res_id;
	mq->len = SLASH_BMAP_SIZE;
	lastbno = fcmh_nvalidbmaps(f);
	if (lastbno > 0)
		lastbno--;
	if (b->bcm_bmapno == lastbno) {
		uint64_t sz;

		sz = fcmh_getsize(f);
		if (sz > b->bcm_bmapno * (uint64_t)SLASH_BMAP_SIZE)
			sz -= b->bcm_bmapno * SLASH_BMAP_SIZE;
		if (sz == 0)
			PFL_GOTOERR(fail, rc = ENODATA);

		mq->len = MIN(sz, SLASH_BMAP_SIZE);
	}
	mq->fg = f->fcmh_fg;
	mq->bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &mq->bgen);

	/* Mark as SCHED here in case the RPC finishes quickly. */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_REPL_SCHED;
	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	if (rc == BREPLST_VALID ||
	    rc == BREPLST_REPL_SCHED)
		DEBUG_BMAP(PLL_FATAL, b,
		    "invalid bmap replica state [off %d]: %d", off, rc);

	av.pointer_arg[IP_BMAP] = b;
	bmap_op_start_type(b, BMAP_OPCNT_UPSCH);

	/*
	 * If it was still QUEUED, which means we marked it SCHED, then
	 * proceed; otherwise, bail: perhaps the user dequeued the
	 * replication request or something.
	 */
	if (rc == BREPLST_REPL_QUEUED) {
		rc = mds_bmap_write_logrepls(b);
		av.space[IN_UNDO_WR] = 1;
		if (rc)
			PFL_GOTOERR(fail, rc);

		/*
		 * It is OK if repl sched is resent across reboots
		 * idempotently.
		 */
		rq->rq_interpret_reply = slmupschedthr_tryrepldst_cb;
		rq->rq_async_args = av;
		rc = SL_NBRQSET_ADD(csvc, rq);
		if (rc == 0)
			return (1);
	} else
		rc = ENODEV;

 fail:
	av.space[IN_RC] = rc;
	slmupschedthr_tryrepldst_cb(rq, &av);
	return (0);
}

void
slmupschedthr_finish_ptrunc(struct slashrpc_cservice *csvc,
    struct bmap *b, int rc, int off, int undowr)
{
	int tract[NBREPLST];

	if (rc) {
		if (b) {
			/* undo brepls changes */
			brepls_init(tract, -1);
			tract[BREPLST_TRUNCPNDG_SCHED] =
			    BREPLST_TRUNCPNDG;
			mds_repl_bmap_apply(b, tract, NULL, off);
			if (undowr)
				mds_bmap_write_logrepls(b);
		}

		psclog_warnx("partial truncation resolution failed "
		    "rc=%d", rc);
	}

	if (csvc)
		sl_csvc_decref(csvc);
	if (b)
		slm_repl_bmap_rel_type(b, BMAP_OPCNT_UPSCH);
	UPSCH_WAKE();
}

int
slmupschedthr_wk_finish_ptrunc(void *p)
{
	struct slm_wkdata_upsch_cb *wk = p;

	slmupschedthr_finish_ptrunc(wk->csvc,
	    wk->b, wk->rc, wk->off, wk->undowr);
	return (0);
}

int
slmupschedthr_tryptrunc_cb(struct pscrpc_request *rq,
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

	wk = pfl_workq_getitem(slmupschedthr_wk_finish_ptrunc,
	    struct slm_wkdata_upsch_cb);
	wk->csvc = csvc;
	wk->b = av->pointer_arg[IP_BMAP];
	wk->rc = rc;
	wk->off = av->space[IN_OFF];
	wk->undowr = av->space[IN_UNDO_WR];
	pfl_workq_putitem(wk);
	return (0);
}

/**
 * slmupschedthr_tryptrunc - Try to issue a PTRUNC resolution to an ION.
 * Returns:
 *   -1	: The activity can never happen; give up.
 *    0	: Unsuccessful.
 *    1	: Success.
 */
int
slmupschedthr_tryptrunc(struct slm_update_data *upd,
    struct bmapc_memb *b, int off, struct sl_resource *dst_res,
    int idx)
{
	int tract[NBREPLST], retifset[NBREPLST], rc;
	struct pscrpc_request *rq = NULL;
	struct slashrpc_cservice *csvc;
	struct srm_bmap_ptrunc_req *mq;
	struct srm_bmap_ptrunc_rep *mp;
	struct pscrpc_async_args av;
	struct sl_resm *dst_resm;
	struct fidc_membh *f;

	f = upd_2_fcmh(upd);
	dst_resm = psc_dynarray_getpos(&dst_res->res_members, idx);

	memset(&av, 0, sizeof(av));
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

	/* Mark as SCHED here in case the RPC finishes quickly. */
	brepls_init(tract, -1);
	tract[BREPLST_TRUNCPNDG] = BREPLST_TRUNCPNDG;

	brepls_init_idx(retifset);
	rc = mds_repl_bmap_apply(b, tract, retifset, off);

	av.pointer_arg[IP_BMAP] = b;
	bmap_op_start_type(b, BMAP_OPCNT_UPSCH);

	if (rc == BREPLST_TRUNCPNDG) {
		rc = mds_bmap_write_logrepls(b);
		av.space[IN_UNDO_WR] = 1;
		if (rc)
			PFL_GOTOERR(fail, rc);

		rq->rq_interpret_reply = slmupschedthr_tryptrunc_cb;
		rq->rq_async_args = av;
		rc = SL_NBRQSET_ADD(csvc, rq);
		if (rc == 0)
			return (1);
	} else
		rc = ENODEV;

 fail:
	if (rq)
		pscrpc_req_finished(rq);
	slmupschedthr_finish_ptrunc(av.pointer_arg[IP_CSVC],
	    av.pointer_arg[IP_BMAP], rc, off,
	    av.space[IN_UNDO_WR]);
	return (0);
}

int
upd_proc_garbage(__unusedx struct slm_update_data *tupd)
{
	psc_fatal("no");
}

int
upd_proc_hldrop(struct slm_update_data *tupd)
{
	int i, rc, tract[NBREPLST], retifset[NBREPLST], iosidx;
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct slm_update_data *upd;
	struct bmap_mds_info *bmi;
	struct bmapc_memb *b;
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
	DYNARRAY_FOREACH(upd, i, &rpmi->rpmi_upschq) {
		bmi = upd_getpriv(upd);
		b = bmi_2_bmap(bmi);
		rc = mds_repl_iosv_lookup(current_vfsid,
		    upd_2_inoh(upd), &repl, &iosidx, 1);
		// XXX check rc??
		if (mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx,
		    1))
			mds_bmap_write_repls_rel(b);
		UPD_DECREF(upd);
	}
	psc_dynarray_reset(&rpmi->rpmi_upschq);
	RPMI_ULOCK(rpmi);
	slm_iosv_clearbusy(&repl, 1);
	mds_repl_node_clearallbusy(upg->upg_resm);
	return (1);
}

int
upd_proc_bmap(struct slm_update_data *upd)
{
	int rc = 1, off, val, tryarchival, iosidx;
	struct rnd_iterator dst_res_i, dst_resm_i;
	struct rnd_iterator src_res_i, src_resm_i;
	struct sl_resource *dst_res, *src_res;
	struct slashrpc_cservice *csvc;
	struct resprof_mds_info *rpmi;
	struct bmap_mds_info *bmi;
	struct sl_resm *src_resm;
	struct bmapc_memb *b;
	struct fidc_membh *f;
	sl_ios_id_t iosid;

	bmi = upd_getpriv(upd);
	b = bmi_2_bmap(bmi);
	f = b->bcm_fcmh;

	UPD_UNBUSY(upd);

	FCMH_WAIT_BUSY(f);
	FCMH_ULOCK(f);

	BMAP_WAIT_BUSY(b);
	BMAP_ULOCK(b);

	UPD_WAIT(upd);
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();

	BMAPOD_MODIFY_START(b);

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
			psclog_errorx("invalid iosid %u", iosid);
			continue;
		}
		rpmi = res2rpmi(dst_res);
		off = SL_BITS_PER_REPLICA * dst_res_i.ri_rnd_idx;
		val = SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls, off);
		switch (val) {
		case BREPLST_REPL_QUEUED:
			/*
			 * There is still a lease out; we'll wait for it
			 * to be relinquished.
			 *
			 * XXX: mask mwc off and make sure lease
			 * relinquishment masks us back on and wakes up.
			 */
			if (bmap_2_bmi(b)->bmdsi_wr_ion)
				break;

			/* look for a repl source */
			for (tryarchival = 0; tryarchival < 2;
			    tryarchival++) {
				FOREACH_RND(&src_res_i, fcmh_2_nrepls(f)) {
					src_res = libsl_id2res(fcmh_getrepl(f,
					    src_res_i.ri_rnd_idx).bs_id);

					/* skip ourself and old/inactive replicas */
					if (src_res_i.ri_rnd_idx == iosidx ||
					    SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls,
					    SL_BITS_PER_REPLICA *
					    src_res_i.ri_rnd_idx) != BREPLST_VALID)
						continue;

					if (tryarchival ^
					    (src_res->res_type == SLREST_ARCHIVAL_FS))
						continue;

					/*
					 * Search source nodes for an
					 * idle, online connection.
					 */
					FOREACH_RND(&src_resm_i,
					    psc_dynarray_len(&src_res->res_members)) {
						src_resm = psc_dynarray_getpos(
						    &src_res->res_members,
						    src_resm_i.ri_rnd_idx);
						csvc = slm_geticsvc(src_resm, NULL,
						    CSVCF_NONBLOCK | CSVCF_NORECON,
						    &slm_upsch_mw);
						if (csvc == NULL)
							continue;
						sl_csvc_decref(csvc);

						/* scan destination resms */
						FOREACH_RND(&dst_resm_i,
						    psc_dynarray_len(&dst_res->res_members))
							if (slmupschedthr_tryrepldst(upd,
							    b, off, src_resm, dst_res,
							    dst_resm_i.ri_rnd_idx))
								return (0);
					}
				}
			}
			break;
		case BREPLST_TRUNCPNDG:
			FOREACH_RND(&dst_resm_i,
			    psc_dynarray_len(&dst_res->res_members)) {
				int rv;

				rv = slmupschedthr_tryptrunc(upd, b,
				    off, dst_res,
				    dst_resm_i.ri_rnd_idx);
				if (rv < 0)
					break;
				if (rv)
					return (0);
			}
			break;
#if 0
		case BREPLST_GARBAGE:
			break;

	int ngarb;
	struct bmapc_memb *bn;
	sl_bmapno_t bno;

			/* XXX just look at fcmh size to determine this */
			ngarb = 0;

			/*
			 * We found garbage.  We must scan from EOF
			 * toward the beginning of the file since we
			 * can't reclaim garbage in the middle of a
			 * file.
			 */
			bno = fcmh_nallbmaps(f);
			if (bno)
				bno--;
			if (b->bcm_bmapno != bno) {
				if (f->fcmh_flags & FCMH_IN_PTRUNC)
					PFL_GOTOERR(out, rc = 1);
				BMAPOD_MODIFY_DONE(b, 0);
				bmap_op_done(b);
				if (mds_bmap_load(f, bno, &b))
					break;
				BMAP_LOCK(b);
			}
			val = BREPLST_INVALID;

			/*
			 * XXX get rid of the bmap scanning; replace
			 * with fcmh size
			 */
			for (bn = b, b = NULL;; ) {
				val = SL_REPL_GET_BMAP_IOS_STAT(
				    bn->bcm_repls, off);
				if (val == BREPLST_GARBAGE)
					ngarb++;
				if ((val != BREPLST_GARBAGE &&
				    val != BREPLST_INVALID) ||
				    bno == 0)
					break;

				if (b)
					bmap_op_done(b);
				b = bn;
				bn = NULL;

				if (f->fcmh_flags & FCMH_IN_PTRUNC)  {
					bmap_op_done(b);
					PFL_GOTOERR(out, rc = 1);
				}

				bno--;
				if (mds_bmap_load(f, bno, &bn))
					continue;
			}

			if (bn == NULL) {
				bn = b;
				b = NULL;
			}

			if (val == BREPLST_GARBAGE_SCHED ||
			    ngarb == 0) {
				if (b)
					bmap_op_done(b);
				b = bn;
				break;
			}
			if (b == NULL && val != BREPLST_GARBAGE) {
				b = bn;
				break;
			}

			if (val == BREPLST_GARBAGE ||
			    val == BREPLST_INVALID) {
				psc_assert(bno == 0);
				if (b)
					bmap_op_done(b);
				b = bn;
			} else
				bmap_op_done(bn);

			FOREACH_RND(&dst_resm_i,
			    psc_dynarray_len(&dst_res->res_members))
				/*
				 * We succeed as long as one
				 * member can do the work
				 * because all members share the
				 * same backend.
				 *
				 * XXX cluster_noshare.
				 */
				if (slmupschedthr_trygarbage(upd, b,
				    off, dst_res,
				    dst_resm_i.ri_rnd_idx))
					return (0);
			break;
#endif
		}
		upd_rpmi_remove(rpmi, upd);
	}
// out:
	if (BMAPOD_HASWRLOCK(bmap_2_bmi(b)))
		BMAPOD_MODIFY_DONE(b, 0);
	BMAP_UNBUSY(b);
	FCMH_UNBUSY(f);
	return (rc);
}

#define DBF_RESID	1
#define DBF_FID		2
#define DBF_BNO		5

#define PARSENUM(buf, max)						\
	_PFL_RVSTART {							\
		unsigned long long _l;					\
		char *_endp;						\
									\
		_l = strtoull((buf), &_endp, 10);			\
		if (_l > (max) || *_endp || _endp == (buf)) {		\
			psclog_error("error parsing db buf %s", (buf));	\
			return (0);					\
		}							\
		_l;							\
	} _PFL_RVEND

int
upd_proc_pagein_unit(struct slm_update_data *upd)
{
	sl_replica_t iosv[SL_MAX_REPLICAS];
	struct slm_update_generic *upg;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	uint32_t j, n;
	int rc;

	upg = upd_getpriv(upd);
	rc = slm_fcmh_get(&upg->upg_fg, &f);
	if (rc) {
		struct slm_wkdata_upsch_purge *wk;

		if (rc == ENOENT) {
			wk = pfl_workq_getitem(slm_wk_upsch_purge,
			    struct slm_wkdata_upsch_purge);
			wk->fid = upg->upg_fg.fg_fid;
			pfl_workq_putitem(wk);
		}
		goto out;
	}
	rc = mds_bmap_load(f, upg->upg_bno, &b);
	if (rc)
		goto out;

	if (fcmh_2_nrepls(f) > SL_DEF_REPLICAS)
		mds_inox_ensure_loaded(fcmh_2_inoh(f));

	/* Requeue pending updates on all registered sites. */
	for (j = n = 0; j < fcmh_2_nrepls(f); j++) {
		switch (SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls,
		    SL_BITS_PER_REPLICA * j)) {
		case BREPLST_REPL_QUEUED:
		case BREPLST_TRUNCPNDG:
			iosv[n++].bs_id = fcmh_getrepl(f, j).bs_id;
			break;
		}
	}
	if (n)
		upsch_enqueue(bmap_2_upd(b), iosv, n);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (1);
}

int
upd_proc_pagein_cb(struct slm_sth *sth, __unusedx void *p)
{
	struct slm_update_generic *upg;

	upg = psc_pool_get(slm_upgen_pool);
	memset(upg, 0, sizeof(*upg));
	INIT_PSC_LISTENTRY(&upg->upg_lentry);
	upd_init(&upg->upg_upd, UPDT_PAGEIN_UNIT);
//	iosid = PARSENUM(av[DBF_RESID], UINT32_MAX);
	upg->upg_fg.fg_fid = sqlite3_column_int64(sth->sth_sth, DBF_FID);
	upg->upg_fg.fg_gen = FGEN_ANY;
	upg->upg_bno = sqlite3_column_int(sth->sth_sth, DBF_BNO);
	upsch_enqueue(&upg->upg_upd, NULL, 0);
	UPD_UNBUSY(&upg->upg_upd);
	return (0);
}

int
upd_proc_pagein(struct slm_update_data *upd)
{
	struct slm_update_generic *upg;
	struct resprof_mds_info *rpmi;
	struct sl_resource *r;
	struct sl_mds_iosinfo *si;
	int n;

	upg = upd_getpriv(upd);
	r = upg->upg_resm->resm_res;
	rpmi = res2rpmi(r);
	si = res2iosinfo(r);

	RPMI_LOCK(rpmi);
	n = UPSCH_MAX_ITEMS_RES	- psc_dynarray_len(&rpmi->rpmi_upschq);
	si->si_flags &= ~SIF_UPSCH_PAGING;
	RPMI_ULOCK(rpmi);

	if (n > 0)
		dbdo(upd_proc_pagein_cb, NULL,
		    " SELECT	*"
		    " FROM	upsch"
		    " WHERE	resid = ?"
		    " ORDER BY	pri DESC,"
		    "		RANDOM()"
		    " LIMIT	?",
		    SQLITE_INTEGER, r->res_id,
		    SQLITE_INTEGER, n);

	return (1);
}

int
upd_proc(struct slm_update_data *upd)
{
	int rc, locked;

	locked = UPSCH_HASLOCK();
	if (locked)
		UPSCH_ULOCK();
	UPD_LOCK(upd);
	UPD_WAIT(upd);
	upd->upd_flags |= UPDF_BUSY;
	upd->upd_owner = pthread_self();
	rc = upd_proctab[upd->upd_type](upd);
	upd->upd_flags &= ~UPDF_BUSY;
	UPD_WAKE(upd);
	if (rc) {
		UPD_ULOCK(upd);
		UPD_DECREF(upd);
	} else {
		/*
		 * Item indicated it should stay in the system; add it
		 * back to our thing here.
		 */
		psc_mlist_addtail(&slm_upschq, upd);
		UPD_ULOCK(upd);
	}
	if (locked)
		UPSCH_LOCK();
	if (!rc)
		return (rc);

	switch (upd->upd_type) {
	case UPDT_HLDROP:
	case UPDT_PAGEIN:
	case UPDT_PAGEIN_UNIT: {
		struct slm_update_generic *upg;

		upg = upd_getpriv(upd);
		psc_pool_return(slm_upgen_pool, upg);
		break;
	    }
	}
	return (rc);
}

void
slmupschedthr_main(__unusedx struct psc_thread *thr)
{
	struct slm_update_data *upd;
	struct sl_resource *r;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j, rc;

	while (pscthr_run()) {
		rc = 0;

		CONF_FOREACH_RESM(s, r, i, m, j)
			if (RES_ISFS(r))
				psc_multiwait_setcondwakeable(&slm_upsch_mw,
				    &m->resm_csvc->csvc_mwc, 0);

		UPSCH_LOCK();
		psc_multiwait_entercritsect(&slm_upsch_mw);
		upd = psc_mlist_tryget(&slm_upschq);
		if (upd)
			rc = upd_proc(upd);
		UPSCH_ULOCK();
	if (rc)
			psc_multiwait_leavecritsect(&slm_upsch_mw);
		else
			psc_multiwait_secs(&slm_upsch_mw, &upd, 5);
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
slmupschedthr_spawn(void)
{
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

	pscthr_init(SLMTHRT_UPSCHED, 0, slmupschedthr_main, NULL,
	    0, "slmupschedthr");

	/* page in initial replrq workload */
	CONF_FOREACH_RES(s, r, i)
		if (RES_ISFS(r))
			upschq_resm(psc_dynarray_getpos(&r->res_members,
			    0), UPDT_PAGEIN);
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

	if (type == UPDT_PAGEIN) {
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
	upsch_enqueue(upd, NULL, 0);
	UPD_UNBUSY(upd);
}

/**
 * upd_initf - Initialize a peer resource update.
 * @upd: peer update structure.
 * @type: type of update.
 * @flags: operation flags.
 */
void
upd_initf(struct slm_update_data *upd, int type, int flags)
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
		struct bmapc_memb *b;

		bmi = upd_getpriv(upd);
		b = bmi_2_bmap(bmi);
		DEBUG_UPD(PLL_DIAG, upd, "init fid="SLPRI_FID" bno=%u",
		    b->bcm_fcmh->fcmh_fg.fg_fid, b->bcm_bmapno);
		if ((flags & UPD_INITF_NOKEY) == 0)
			slm_repl_upd_odt_read(b);
		break;
	    }
	default:
		DEBUG_UPD(PLL_DEBUG, upd, "init");
	}
}

void
upd_destroy(struct slm_update_data *upd)
{
	DEBUG_UPD(PLL_DEBUG, upd, "destroy");
	psc_assert(psclist_disjoint(&upd->upd_lentry));
	psc_assert(!(upd->upd_flags & UPDF_BUSY));
	psc_mutex_destroy(&upd->upd_mutex);
	psc_multiwaitcond_destroy(&upd->upd_mwc);
	PSCFREE(upd->upd_recpt);
	memset(upd, 0, sizeof(*upd));
}

int
upsch_purge_cb(struct slm_sth *sth, __unusedx void *p)
{
	struct odtable_receipt *odtr;

	odtr = PSCALLOC(sizeof(*odtr));
	odtr->odtr_elem = sqlite3_column_int64(sth->sth_sth, 0);
	odtr->odtr_key = sqlite3_column_int64(sth->sth_sth, 1);
	mds_odtable_freeitem(slm_repl_odt, odtr);
	return (0);
}

void
upsch_purge(slfid_t fid)
{
	dbdo(upsch_purge_cb, NULL,
	    " SELECT"
	    "	recpt_elem,"
	    "	recpt_key"
	    " FROM"
	    "	upsch"
	    " WHERE"
	    "	fid = ?",
	    SQLITE_INTEGER64, fid);
	dbdo(NULL, NULL,
	    " DELETE FROM "
	    "	upsch"
	    " WHERE"
	    "	fid = ?",
	    SQLITE_INTEGER64, fid);
}

int
slm_wk_upsch_purge(void *p)
{
	struct slm_wkdata_upsch_purge *wk = p;

	upsch_purge(wk->fid);
	return (0);
}

void
upsch_enqueue(struct slm_update_data *upd, const sl_replica_t *iosv,
    int nios)
{
	struct resprof_mds_info *rpmi;
	struct sl_resource *r;
	int locked, n;

	locked = UPD_RLOCK(upd);
	for (n = 0; n < nios; n++) {
		r = libsl_id2res(iosv[n].bs_id);
		rpmi = res2rpmi(r);
		RPMI_LOCK(rpmi);
		if (!psc_dynarray_add_ifdne(&rpmi->rpmi_upschq, upd))
			UPD_INCREF(upd);
		RPMI_ULOCK(rpmi);
	}
	if (!psc_mlist_conjoint(&slm_upschq, upd)) {
		if (upd->upd_type == UPDT_BMAP &&
		    (upd_2_fcmh(upd)->fcmh_flags & FCMH_IN_PTRUNC) == 0)
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
	DEBUG_UPD(PLL_MAX, upd, "");
}
#endif

int (*upd_proctab[])(struct slm_update_data *) = {
	upd_proc_bmap,
	upd_proc_garbage,
	upd_proc_hldrop,
	upd_proc_pagein,
	upd_proc_pagein_unit
};
