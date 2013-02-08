/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>

#include "psc_ds/listcache.h"
#include "psc_ds/vbitmap.h"
#include "psc_rpc/rpc.h"
#include "psc_util/atomic.h"
#include "psc_util/pool.h"

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
    PLL_INIT(&sli_replwkq_active, struct sli_repl_workrq,
	    srw_active_lentry);

struct sli_repl_workrq *
sli_repl_findwq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno)
{
	struct sli_repl_workrq *w = NULL;

	PLL_LOCK(&sli_replwkq_active);
	PLL_FOREACH(w, &sli_replwkq_active)
		if (SAMEFG(&w->srw_fg, fgp) && w->srw_bmapno == bmapno)
			break;
	PLL_ULOCK(&sli_replwkq_active);
	return (w);
}

int
sli_repl_addwk(int op, struct sl_resource *res,
    const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    sl_bmapgen_t bgen, int len)
{
	struct sli_repl_workrq *w;
	int rc, i;

	/*
	 * Check if this work is already queued, e.g. from before the
	 * MDS crashes, comes back online, and assigns gratitious
	 * requeue work.
	 */
	w = sli_repl_findwq(fgp, bmapno);
	if (w)
		return (-SLERR_ALREADY);

	w = psc_pool_get(sli_replwkrq_pool);
	memset(w, 0, sizeof(*w));
	psc_atomic32_set(&w->srw_refcnt, 1);
	INIT_PSC_LISTENTRY(&w->srw_active_lentry);
	INIT_PSC_LISTENTRY(&w->srw_pending_lentry);
	INIT_SPINLOCK(&w->srw_lock);
	w->srw_src_res = res;
	w->srw_fg = *fgp;
	w->srw_bmapno = bmapno;
	w->srw_bgen = bgen;
	w->srw_len = len;
	w->srw_op = op;

	/* get an fcmh for the file */
	rc = sli_fcmh_get(&w->srw_fg, &w->srw_fcmh);
	if (rc)
		goto out;

	/* get the replication chunk's bmap */
	rc = bmap_get(w->srw_fcmh, w->srw_bmapno, SL_READ, &w->srw_bcm);
	if (rc)
		psclog_errorx("bmap_get %u: %s",
		    w->srw_bmapno, slstrerror(rc));
	else {
		bmap_op_start_type(w->srw_bcm, BMAP_OPCNT_REPLWK);
		bmap_op_done(w->srw_bcm);

		/* mark slivers for replication */
		if (op == SLI_REPLWKOP_REPL) {
			BMAP_LOCK(w->srw_bcm);
			for (i = len = 0;
			    i < SLASH_SLVRS_PER_BMAP &&
			    len < (int)w->srw_len;
			    i++, len += SLASH_SLVR_SIZE) {
				w->srw_bcm->bcm_crcstates[i] |=
				    BMAP_SLVR_WANTREPL;
				w->srw_nslvr_tot++;
			}
			BMAP_ULOCK(w->srw_bcm);
		}
	}
	psclog_info("fid="SLPRI_FG" bmap=%d #slivers=%d",
	    SLPRI_FG_ARGS(fgp), bmapno, w->srw_nslvr_tot);

 out:
	if (rc) {
		if (w->srw_fcmh)
			fcmh_op_done(w->srw_fcmh);

		psc_pool_return(sli_replwkrq_pool, w);
	} else {
		/* add to current processing list */
		pll_add(&sli_replwkq_active, w);
		lc_add(&sli_replwkq_pending, w);
	}
	return (rc);
}

void
sli_replwkrq_decref(struct sli_repl_workrq *w, int rc)
{
	(void)reqlock(&w->srw_lock);

	/*
	 * This keeps the very first error and cause our thread to drop
	 * its reference to us.
	 */
	if (rc && w->srw_status == 0)
		w->srw_status = rc;

	if (!psc_atomic32_dec_and_test0(&w->srw_refcnt)) {
		DEBUG_SRW(w, PLL_MAX, "decref");
		freelock(&w->srw_lock);
		return;
	}
	DEBUG_SRW(w, PLL_MAX, "destroying");

	pll_remove(&sli_replwkq_active, w);
	/* inform MDS we've finished */
	sli_rmi_issue_repl_schedwk(w);

	if (w->srw_bcm)
		bmap_op_done_type(w->srw_bcm, BMAP_OPCNT_REPLWK);
	if (w->srw_fcmh)
		fcmh_op_done(w->srw_fcmh);

	psc_pool_return(sli_replwkrq_pool, w);
}

void
slireplpndthr_main(__unusedx struct psc_thread *thr)
{
	struct sli_repl_workrq *w, *wrap;
	struct slashrpc_cservice *csvc;
	struct sl_resm *src_resm;
	int rc, slvridx, slvrno;

	wrap = NULL;
	while (pscthr_run()) {
		slvrno = 0;
		w = lc_getwait(&sli_replwkq_pending);
		spinlock(&w->srw_lock);
		if (w->srw_status)
			goto release;

		if (w->srw_op == SLI_REPLWKOP_PTRUNC) {
			/*
			 * XXX this needs to truncate data in the slvr
			 * cache.
			 *
			 * XXX if there is srw_offset, we must send a
			 * CRC update for the sliver.
			 */
			goto release;
		}

		/* find a sliver to transmit */
		BMAP_LOCK(w->srw_bcm);
		while (slvrno < SLASH_SLVRS_PER_BMAP) {
			if (w->srw_bcm->bcm_crcstates[slvrno] &
			    BMAP_SLVR_WANTREPL)
				break;
			slvrno++;
		}

		if (slvrno == SLASH_SLVRS_PER_BMAP) {
			BMAP_ULOCK(w->srw_bcm);
			/* no work to do; we are done with this bmap */
			goto release;
		}

		/* find a pointer slot we can use to transmit the sliver */
		for (slvridx = 0; slvridx < REPL_MAX_INFLIGHT_SLVRS;
		    slvridx++)
			if (w->srw_slvr_refs[slvridx] == NULL)
				break;

		if (slvridx == REPL_MAX_INFLIGHT_SLVRS) {
			BMAP_ULOCK(w->srw_bcm);
			freelock(&w->srw_lock);
			LIST_CACHE_LOCK(&sli_replwkq_pending);
			lc_add(&sli_replwkq_pending, w);
			if (w == wrap)
				psc_waitq_waitrel_us(
				    &sli_replwkq_pending.plc_wq_empty,
				    &sli_replwkq_pending.plc_lock, 10);
			else {
				if (wrap == NULL)
					wrap = w;
				LIST_CACHE_ULOCK(&sli_replwkq_pending);
			}
			continue;
		}
		wrap = NULL;

		w->srw_bcm->bcm_crcstates[slvrno] &= ~BMAP_SLVR_WANTREPL;
		BMAP_ULOCK(w->srw_bcm);

		/* mark slot as occupied */
		w->srw_slvr_refs[slvridx] = SLI_REPL_SLVR_SCHED;
		freelock(&w->srw_lock);

		/* acquire connection to replication source & issue READ */
		src_resm = psc_dynarray_getpos(
		    &w->srw_src_res->res_members, 0);
		csvc = sli_geticsvc(src_resm);
		lc_add(&sli_replwkq_pending, w);
		if (csvc == NULL)
			rc = SLERR_ION_OFFLINE;
		else {
			rc = sli_rii_issue_repl_read(csvc, slvrno,
			    slvridx, w);
			sl_csvc_decref(csvc);
		}
		if (rc) {
			spinlock(&w->srw_lock);
			w->srw_slvr_refs[slvridx] = NULL;
			BMAP_LOCK(w->srw_bcm);
			w->srw_bcm->bcm_crcstates[slvrno] |=
			    BMAP_SLVR_WANTREPL;
			BMAP_ULOCK(w->srw_bcm);
			freelock(&w->srw_lock);
		}
		continue;

 release:
		sli_replwkrq_decref(w, 0);
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&sli_replwkrq_poolmaster,
	    struct sli_repl_workrq, srw_pending_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, NULL, NULL, "replwkrq");
	sli_replwkrq_pool = psc_poolmaster_getmgr(&sli_replwkrq_poolmaster);

	lc_reginit(&sli_replwkq_pending, struct sli_repl_workrq,
	    srw_pending_lentry, "replwkpnd");

	pscthr_init(SLITHRT_REPLPND, 0, slireplpndthr_main, NULL, 0,
	    "slireplpndthr");
}
