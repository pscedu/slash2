/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_ds/pool.h"
#include "psc_rpc/rpc.h"

#include "bmap.h"
#include "fidcache.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slerr.h"
#include "sliod.h"
#include "sltypes.h"

struct pscrpc_nbreqset	 sli_replwk_nbset =
    PSCRPC_NBREQSET_INIT(sli_replwk_nbset, NULL, NULL);

struct psc_poolmaster	 sli_replwkrq_poolmaster;
struct psc_poolmgr	*sli_replwkrq_pool;

/* a replication request exists on one of these */
struct psc_listcache	 sli_replwkq_pending;
struct psc_listcache	 sli_replwkq_inflight;
struct psc_listcache	 sli_replwkq_finished;

/* and all registered replication requests are listed here */
struct psc_lockedlist	 sli_replwkq_active =
    PLL_INITIALIZER(&sli_replwkq_active, struct sli_repl_workrq,
	    srw_active_lentry);

void
sli_repl_addwk(uint64_t nid, struct slash_fidgen *fgp,
    sl_bmapno_t bmapno, int len)
{
	char buf[PSC_NIDSTR_SIZE];
	struct sli_repl_workrq *w;
	struct sl_resm *resm;

	w = psc_pool_get(sli_replwkrq_pool);
	memset(w, 0, sizeof(*w));
	w->srw_nid = nid;
	w->srw_fg = *fgp;
	w->srw_bmapno = bmapno;
	w->srw_len = len;

	/* lookup replication source peer */
	resm = libsl_nid2resm(w->srw_nid);
	if (resm == NULL) {
		psc_errorx("%s: unknown resource member",
		    psc_nid2str(w->srw_nid, buf));
		w->srw_status = SLERR_ION_UNKNOWN;
		goto out;
	}

	/* get an fcmh for the file */
	w->srw_fcmh = iod_inode_lookup(&w->srw_fg);
	w->srw_status = iod_inode_open(w->srw_fcmh, SL_WRITE);
	if (w->srw_status) {
		DEBUG_FCMH(PLL_ERROR, w->srw_fcmh, "iod_inode_open");
		goto out;
	}

	/* get the replication chunk's bmap */
	w->srw_status = iod_bmap_load(w->srw_fcmh,
	    w->srw_bmapno, SL_WRITE, &w->srw_bcm);
	if (w->srw_status)
		psc_errorx("iod_bmap_load %u: %s",
		    w->srw_bmapno, slstrerror(w->srw_status));

 out:
	/* add to current processing list */
	pll_add(&sli_replwkq_active, w);

	if (w->srw_status)
		lc_add(&sli_replwkq_pending, w);
	else
		lc_add(&sli_replwkq_finished, w);
}

__dead void *
slireplfinthr_main(__unusedx void *arg)
{
	struct sli_repl_workrq *w;

	for (;;) {
		w = lc_getwait(&sli_replwkq_finished);
		pll_remove(&sli_replwkq_active, w);
		/* inform MDS we've finished */
		sli_rmi_issue_repl_schedwk(w);

		if (w->srw_bcm)
			bmap_op_done(w->srw_bcm);
		if (w->srw_fcmh)
			fidc_membh_dropref(w->srw_fcmh);
		psc_pool_return(sli_replwkrq_pool, w);
		sched_yield();
	}
}

__dead void *
slireplpndthr_main(__unusedx void *arg)
{
	struct slashrpc_cservice *csvc;
	struct sli_repl_workrq *w;

	for (;;) {
		w = lc_getwait(&sli_replwkq_pending);
		csvc = sli_geticsvc(w->srw_resm);
		if (csvc == NULL)
			w->srw_status = SLERR_ION_OFFLINE;
		else
			w->srw_status = sli_rii_issue_repl_read(csvc->csvc_import, w);
		if (csvc)
			sl_csvc_decref(csvc);
		lc_add(w->srw_status ? &sli_replwkq_finished :
		    &sli_replwkq_inflight, w);
		sched_yield();
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&sli_replwkrq_poolmaster, struct sli_repl_workrq,
	    srw_state_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL, "replwkrq");
	sli_replwkrq_pool = psc_poolmaster_getmgr(&sli_replwkrq_poolmaster);

	lc_reginit(&sli_replwkq_pending, struct sli_repl_workrq,
	    srw_state_lentry, "replwkpnd");
	lc_reginit(&sli_replwkq_inflight, struct sli_repl_workrq,
	    srw_state_lentry, "replwkinf");
	lc_reginit(&sli_replwkq_finished, struct sli_repl_workrq,
	    srw_state_lentry, "replwkfin");

	pscthr_init(SLITHRT_REPLFIN, 0, slireplfinthr_main,
	    NULL, 0, "slireplfinthr");
	pscthr_init(SLITHRT_REPLPND, 0, slireplpndthr_main,
	    NULL, 0, "slireplpndthr");

	pscrpc_nbreapthr_spawn(&sli_replwk_nbset, SLITHRT_REPLREAP, "slireplreapthr");
}
