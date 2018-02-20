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
 * Routines for handling replication requests from the MDS.  Requests
 * are always directed at the destination IOS, so this entails tracking
 * the transfers via work request units and scheduling them.  The
 * MDS(es) are in charge of oversubscription but there are windows when
 * we can get bursty.
 */

#include <sys/statvfs.h>

#include <stdio.h>

#include "pfl/atomic.h"
#include "pfl/listcache.h"
#include "pfl/pool.h"
#include "pfl/rpc.h"
#include "pfl/vbitmap.h"

#include "batchrpc.h"
#include "bmap.h"
#include "bmap_iod.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"
#include "sliod.h"
#include "sltypes.h"

struct psc_poolmaster	 sli_replwkrq_poolmaster;
struct psc_poolmgr	*sli_replwkrq_pool;

/* a replication request exists on one of these */
struct psc_listcache	 sli_replwkq_pending;

/* and all registered replication requests are listed here */
struct psc_lockedlist	 sli_replwkq_active = 
    PLL_INIT(&sli_replwkq_active, struct sli_repl_workrq, srw_active_lentry);

struct sli_repl_workrq *
sli_repl_findwq(const struct sl_fidgen *fgp, sl_bmapno_t bmapno)
{
	struct sli_repl_workrq *w = NULL;

	PLL_LOCK(&sli_replwkq_active);
	PLL_FOREACH(w, &sli_replwkq_active)
		if (SAMEFG(&w->srw_fg, fgp) && w->srw_bmapno == bmapno)
			break;
	PLL_ULOCK(&sli_replwkq_active);
	return (w);
}

#define SIGN(x)		((x) < 0 ? -1 : 1)

void
sli_bwqueued_adj(int32_t *p, int amt_bytes)
{
	struct slrpc_cservice *csvc;
	int amt;

	amt = SIGN(amt_bytes) * howmany(abs(amt_bytes), BW_UNITSZ);

	spinlock(&sli_bwqueued_lock);
	*p += amt;
	sli_bwqueued.sbq_aggr += amt;
	freelock(&sli_bwqueued_lock);

	spinlock(&sli_ssfb_lock);
	sli_ssfb_send.tv_sec = 0;	/* reset timer */
	freelock(&sli_ssfb_lock);

	/* XXX use non-blocking version */
	if (!sli_rmi_getcsvc(&csvc)) {
		CSVC_LOCK(csvc);
		clock_gettime(CLOCK_MONOTONIC, &csvc->csvc_mtime);
		csvc->csvc_mtime.tv_sec -= CSVC_PING_INTV;
		// XXX could do a wakeup here to send update immediately
		CSVC_ULOCK(csvc);
		sl_csvc_decref(csvc);
	}
}

/*
 * Add a piece of work to the replication scheduling engine. It is called
 * from slrpc_batch_handle_request() via a worker thread.
 */
int
sli_repl_addwk(struct slrpc_batch_rep *bp, void *req, void *rep)
{
	struct sli_repl_workrq *w = NULL;
	struct srt_replwk_req *q = req;
	struct srt_replwk_rep *p = rep;
	struct sl_resource *res = NULL;
	struct fidc_membh *f = NULL;
	struct bmap_iod_info *bii;
	struct bmap *b = NULL;
	size_t len;
	int rc, i;

	rc = 0;
	if (q->fg.fg_fid == FID_ANY)
		PFL_GOTOERR(out, rc = EINVAL);
	if (q->len < 1 || q->len > SLASH_BMAP_SIZE)
		PFL_GOTOERR(out, rc = EINVAL);

	if (q->src_resid != IOS_ID_ANY) {
		res = libsl_id2res(q->src_resid);
		if (res == NULL)
			PFL_GOTOERR(out, rc = SLERR_ION_UNKNOWN);
	}

	/*
	 * Check if this work is already queued e.g. from before the MDS
	 * crashes, comes back online, and assigns gratuitous requeue
	 * work.
	 */
	if (sli_repl_findwq(&q->fg, q->bno)) {
		OPSTAT_INCR("repl-already-queued");
		PFL_GOTOERR(out, rc = PFLERR_ALREADY);
	}

	rc = sli_fcmh_get(&q->fg, &f);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = bmap_get(f, q->bno, SL_READ, &b);
	if (rc)
		PFL_GOTOERR(out, rc);

	/*
	 * If the MDS asks us to replicate a sliver, I do not
	 * have the space allocated, at least according to MDS.
	 */
	if (!sli_has_enough_space(f, q->bno, q->bno * SLASH_BMAP_SIZE,
	    q->len)) {
		OPSTAT_INCR("repl-out-of-space");
		PFL_GOTOERR(out, rc = ENOSPC);
	}

	w = psc_pool_get(sli_replwkrq_pool);
	memset(w, 0, sizeof(*w));
	INIT_PSC_LISTENTRY(&w->srw_active_lentry);
	INIT_PSC_LISTENTRY(&w->srw_pending_lentry);
	INIT_SPINLOCK(&w->srw_lock);
	w->srw_src_res = res;
	w->srw_fg = q->fg;
	w->srw_bmapno = q->bno;
	w->srw_bgen = q->bgen;
	w->srw_len = q->len;
	w->srw_bp = bp;
	w->srw_rep = rep;
	w->srw_bcm = b;

	slrpc_batch_rep_incref(bp);

	bmap_op_start_type(w->srw_bcm, BMAP_OPCNT_REPLWK);

	/* mark slivers for replication */
	sli_bwqueued_adj(&sli_bwqueued.sbq_ingress, q->len);

	for (i = len = 0;
	    i < SLASH_SLVRS_PER_BMAP && len < w->srw_len;
	    i++, len += SLASH_SLVR_SIZE) {
		bii = bmap_2_bii(w->srw_bcm);
		bii->bii_crcstates[i] |= BMAP_SLVR_WANTREPL;
		w->srw_nslvr_tot++;
	}

	PFLOG_REPLWK(PLL_DEBUG, w, "created; #slivers=%d",
	    w->srw_nslvr_tot);

	/* for sli_repl_findwq() to detect duplicate request */
	pll_add(&sli_replwkq_active, w);

	/* for slireplpndthr_main() */
	if (sli_replwk_queue(w))
		OPSTAT_INCR("repl-queue-pending");
	else
		OPSTAT_INCR("repl-queue-noop");

 out:
	p->rc = rc;
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (0);
}

void
sli_replwkrq_decref(struct sli_repl_workrq *w, int rc)
{
	spinlock(&w->srw_lock);

	rc = pflrpc_portable_errno(rc);

	/*
	 * This keeps the very first error and causes our thread to drop
	 * its reference to us.
	 */
	if (rc && w->srw_status == 0)
		w->srw_status = rc;

	if (!psc_atomic32_dec_and_test0(&w->srw_refcnt)) {
		PFLOG_REPLWK(PLL_DEBUG, w, "decref");
		freelock(&w->srw_lock);
		return;
	}
	PFLOG_REPLWK(PLL_DEBUG, w, "destroying");

	pll_remove(&sli_replwkq_active, w);

	sli_bwqueued_adj(&sli_bwqueued.sbq_ingress, -w->srw_len);

	slrpc_batch_rep_decref(w->srw_bp, w->srw_status);

	if (w->srw_bcm)
		bmap_op_done_type(w->srw_bcm, BMAP_OPCNT_REPLWK);

	psc_pool_return(sli_replwkrq_pool, w);
}

int
sli_replwk_queue(struct sli_repl_workrq *w)
{
	int queued = 0;
	LIST_CACHE_LOCK(&sli_replwkq_pending);
	spinlock(&w->srw_lock);
	/* XXX use a flag to cut overhead */
	if (!lc_conjoint(&sli_replwkq_pending, w)) {
		psc_atomic32_inc(&w->srw_refcnt);
		PFLOG_REPLWK(PLL_DEBUG, w, "incref");
		lc_add(&sli_replwkq_pending, w);
		queued = 1;
	}
	freelock(&w->srw_lock);
	LIST_CACHE_ULOCK(&sli_replwkq_pending);
	return (queued);
}

/*
 * Try to replicate some data from another IOS.
 */
void
sli_repl_try_work(struct sli_repl_workrq *w,
    struct sli_repl_workrq **last)
{
	int rc, slvridx, slvrno;
	struct slrpc_cservice *csvc;
	struct bmap_iod_info *bii;
	struct sl_resm *src_resm;

	spinlock(&w->srw_lock);
	if (w->srw_status) {
		freelock(&w->srw_lock);
		goto out;
	}

	BMAP_LOCK(w->srw_bcm);
	slvrno = 0;
	bii = bmap_2_bii(w->srw_bcm);
	while (slvrno < SLASH_SLVRS_PER_BMAP) {
		if (bii->bii_crcstates[slvrno] & BMAP_SLVR_WANTREPL)
			break;
		slvrno++;
	}

	if (slvrno == SLASH_SLVRS_PER_BMAP) {
		BMAP_ULOCK(w->srw_bcm);
		/* No work to do; we are done with this bmap. */
		freelock(&w->srw_lock);
		goto out;
	}

	/* Find a free slot we can use to transmit the sliver. */
	for (slvridx = 0; slvridx < nitems(w->srw_slvr); slvridx++)
		if (w->srw_slvr[slvridx] == NULL)
			break;

	if (slvridx == nitems(w->srw_slvr)) {
		/* All slots are in use on this work item. */
		BMAP_ULOCK(w->srw_bcm);
		freelock(&w->srw_lock);
		LIST_CACHE_LOCK(&sli_replwkq_pending);
		sli_replwk_queue(w);
		if (w == *last)
			/*
			 * There is no other work to do.  Wait for a
			 * slot to open or for other work to arrive.
			 */
			psc_waitq_waitrel_us(
			    &sli_replwkq_pending.plc_wq_empty,
			    &sli_replwkq_pending.plc_lock, 10);
		else {
			if (*last == NULL)
				*last = w;
			LIST_CACHE_ULOCK(&sli_replwkq_pending);
		}
		goto out;
	}
	*last = NULL;

	bii->bii_crcstates[slvrno] &= ~BMAP_SLVR_WANTREPL;
	BMAP_ULOCK(w->srw_bcm);

	/* Mark slot as occupied. */
	w->srw_slvr[slvridx] = SLI_REPL_SLVR_SCHED;
	freelock(&w->srw_lock);

	/* Acquire connection to replication source & issue READ. */
	src_resm = psc_dynarray_getpos(&w->srw_src_res->res_members, 0);
	csvc = sli_geticsvc(src_resm, 0);

	/*
 	 * We just take the work off the sli_replwkq_pending. Now we
 	 * are putting it back again?
 	 */
	sli_replwk_queue(w);

	if (csvc == NULL)
		rc = SLERR_ION_OFFLINE;
	else {
		rc = sli_rii_issue_repl_read(csvc, slvrno, slvridx, w);
		sl_csvc_decref(csvc);
	}
	if (rc) {
		OPSTAT_INCR("repl-ignore-error");
		spinlock(&w->srw_lock);
		w->srw_slvr[slvridx] = NULL;
		BMAP_LOCK(w->srw_bcm);
		bii = bmap_2_bii(w->srw_bcm);
		bii->bii_crcstates[slvrno] |= BMAP_SLVR_WANTREPL;
		BMAP_ULOCK(w->srw_bcm);
		freelock(&w->srw_lock);
	}

 out:
	sli_replwkrq_decref(w, 0);
}

void
slireplpndthr_main(struct psc_thread *thr)
{
	struct sli_repl_workrq *w, *last;

	last = NULL;
	while (pscthr_run(thr)) {
		w = lc_getwait(&sli_replwkq_pending);
		if (w)
			sli_repl_try_work(w, &last);
	}
}

void
sli_repl_init(void)
{
	int i;

	psc_poolmaster_init(&sli_replwkrq_poolmaster,
	    struct sli_repl_workrq, srw_pending_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, "replwkrq");
	sli_replwkrq_pool = psc_poolmaster_getmgr(&sli_replwkrq_poolmaster);

	lc_reginit(&sli_replwkq_pending, struct sli_repl_workrq,
	    srw_pending_lentry, "replwkpnd");

	for (i = 0; i < 1; i++) {
		pscthr_init(SLITHRT_REPLPND, slireplpndthr_main, 0, 
		    "slireplpndthr%d", i);
	}
}
