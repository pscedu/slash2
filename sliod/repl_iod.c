/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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

struct pscrpc_nbreqset	 sli_replwk_nbset =
    PSCRPC_NBREQSET_INIT(sli_replwk_nbset, NULL, NULL);

struct psc_poolmaster	 sli_replwkrq_poolmaster;
struct psc_poolmgr	*sli_replwkrq_pool;

/* a replication request exists on one of these */
struct psc_listcache	 sli_replwkq_pending;

/* and all registered replication requests are listed here */
struct psc_lockedlist	 sli_replwkq_active =
    PLL_INIT(&sli_replwkq_active, struct sli_repl_workrq,
	    srw_active_lentry);

int
sli_repl_addwk(int op, uint64_t nid, const struct slash_fidgen *fgp,
    sl_bmapno_t bmapno, sl_bmapgen_t bgen, int len)
{
	struct sli_repl_workrq *w;
	int rc, i;

	/*
	 * Check if this work is already queued, e.g. from before the
	 * MDS crashes, comes back online, and assigns gratitious
	 * requeue work.
	 */
	PLL_LOCK(&sli_replwkq_active);
	PLL_FOREACH(w, &sli_replwkq_active)
		if (SAMEFG(&w->srw_fg, fgp) &&
		    w->srw_bmapno == bmapno)
			break;
	PLL_ULOCK(&sli_replwkq_active);
	if (w)
		return (EALREADY);

	w = psc_pool_get(sli_replwkrq_pool);
	memset(w, 0, sizeof(*w));
	INIT_PSC_LISTENTRY(&w->srw_state_lentry);
	INIT_PSC_LISTENTRY(&w->srw_active_lentry);
	INIT_SPINLOCK(&w->srw_lock);
	psc_atomic32_set(&w->srw_refcnt, 1);
	w->srw_nid = nid;
	w->srw_fg = *fgp;
	w->srw_bmapno = bmapno;
	w->srw_bgen = bgen;
	w->srw_len = len;
	w->srw_op = op;

	/* lookup replication source peer */
	if (nid)
		w->srw_resm = libsl_nid2resm(w->srw_nid);

	/* get an fcmh for the file */
	rc = sli_fcmh_get(&w->srw_fg, &w->srw_fcmh);
	if (rc)
		goto out;

	/* get the replication chunk's bmap */
	rc = bmap_get(w->srw_fcmh, w->srw_bmapno,
	    SL_READ, &w->srw_bcm);
	if (rc)
		psclog_errorx("bmap_get %u: %s",
		    w->srw_bmapno, slstrerror(rc));
	else {
		bmap_op_start_type(w->srw_bcm, BMAP_OPCNT_REPLWK);
		bmap_op_done_type(w->srw_bcm, BMAP_OPCNT_LOOKUP);

		/* mark slivers for replication */
		BMAP_LOCK(w->srw_bcm);
		if (op == SLI_REPLWKOP_REPL)
			for (i = len = 0;
			    i < SLASH_SLVRS_PER_BMAP &&
			    len < (int)w->srw_len;
			    i++, len += SLASH_SLVR_SIZE)
				if (w->srw_bcm->bcm_crcstates[i] &
				    BMAP_SLVR_DATA) {
					w->srw_bcm->bcm_crcstates[i] |=
					    BMAP_SLVR_WANTREPL;
					w->srw_nslvr_tot++;
				}
		BMAP_ULOCK(w->srw_bcm);
	}

 out:
	if (rc) {
		if (w->srw_fcmh)
			fcmh_op_done_type(w->srw_fcmh,
			    FCMH_OPCNT_LOOKUP_FIDC);

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
	reqlock(&w->srw_lock);

	if (rc && w->srw_status == 0)
		w->srw_status = rc;

	if (!psc_atomic32_dec_and_test0(&w->srw_refcnt)) {
		freelock(&w->srw_lock);
		return;
	}

	pll_remove(&sli_replwkq_active, w);
	/* inform MDS we've finished */
	sli_rmi_issue_repl_schedwk(w);

	if (w->srw_bcm)
		bmap_op_done_type(w->srw_bcm, BMAP_OPCNT_REPLWK);
	if (w->srw_fcmh)
		fcmh_op_done_type(w->srw_fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	psc_pool_return(sli_replwkrq_pool, w);
}

void
slireplpndthr_main(__unusedx struct psc_thread *thr)
{
	struct slashrpc_cservice *csvc;
	struct sli_repl_workrq *w;
	int rc, slvridx, slvrno;

	while (pscthr_run()) {
		rc = 0;
		slvridx = REPL_MAX_INFLIGHT_SLVRS;
		w = lc_getwait(&sli_replwkq_pending);

		spinlock(&w->srw_lock);
		if (w->srw_status)
			goto end;

		if (w->srw_op == SLI_REPLWKOP_PTRUNC) {
			/*
			 * XXX this needs to truncate data in the slvr
			 * cache.
			 *
			 * XXX if there is srw_offset, we must send a
			 * CRC update for the sliver.
			 */
			goto end;
		}

		/* find a sliver to transmit */
		BMAP_LOCK(w->srw_bcm);
		for (slvrno = 0; slvrno < SLASH_SLVRS_PER_BMAP; slvrno++)
			if (w->srw_bcm->bcm_crcstates[slvrno] & BMAP_SLVR_WANTREPL)
				break;

		if (slvrno == SLASH_SLVRS_PER_BMAP) {
			BMAP_ULOCK(w->srw_bcm);
			goto end;
		}

		/* find a pointer slot we can use to transmit the sliver */
		for (slvridx = 0; slvridx < REPL_MAX_INFLIGHT_SLVRS; slvridx++)
			if (w->srw_slvr_refs[slvridx] == NULL)
				break;

		if (slvridx == REPL_MAX_INFLIGHT_SLVRS) {
			BMAP_ULOCK(w->srw_bcm);
			goto end;
		}

		w->srw_bcm->bcm_crcstates[slvrno] &= ~BMAP_SLVR_WANTREPL;
		BMAP_ULOCK(w->srw_bcm);

		/* mark slot as occupied */
		w->srw_slvr_refs[slvridx] = SLI_REPL_SLVR_SCHED;
		freelock(&w->srw_lock);

		/* acquire connection to replication source & issue READ */
		csvc = sli_geticsvc(w->srw_resm);
		if (csvc == NULL) {
			rc = SLERR_ION_OFFLINE;
			goto end;
		}
		rc = sli_rii_issue_repl_read(csvc, slvrno, slvridx, w);
		sl_csvc_decref(csvc);
		if (rc)
			goto end;

		spinlock(&w->srw_lock);
		/*
		 * Place back on queue to process again on next
		 * iteration.  If it's full, we'll wait for a slot to
		 * open up then.
		 */
		if (psclist_disjoint(&w->srw_state_lentry)) {
			lc_addhead(&sli_replwkq_pending, w);
			psc_atomic32_inc(&w->srw_refcnt);
		}
 end:
		if (rc && slvridx != REPL_MAX_INFLIGHT_SLVRS)
			w->srw_slvr_refs[slvridx] = NULL;
		sli_replwkrq_decref(w, rc);
		sched_yield();
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&sli_replwkrq_poolmaster,
	    struct sli_repl_workrq, srw_state_lentry, PPMF_AUTO, 256,
	    256, 0, NULL, NULL, NULL, "replwkrq");
	sli_replwkrq_pool = psc_poolmaster_getmgr(&sli_replwkrq_poolmaster);

	lc_reginit(&sli_replwkq_pending, struct sli_repl_workrq,
	    srw_state_lentry, "replwkpnd");

	pscthr_init(SLITHRT_REPLPND, 0, slireplpndthr_main, NULL, 0,
	    "slireplpndthr");

	pscrpc_nbreapthr_spawn(&sli_replwk_nbset, SLITHRT_REPLREAP,
	    "slireplreapthr");
}
