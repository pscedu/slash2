/* $Id$ */

#include <stdio.h>

#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_rpc/rpc.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slerr.h"
#include "sltypes.h"

struct pscrpc_nbreqset	 sli_replwk_nbset =
    PSCRPC_NBREQSET_INIT(sli_replwk_nbset, NULL, NULL);

struct psc_poolmaster	 sli_replwkbuf_poolmaster;
struct psc_poolmgr	*sli_replwkbuf_pool;

struct psc_poolmaster	 sli_replwkrq_poolmaster;
struct psc_poolmgr	*sli_replwkrq_pool;

struct psc_listcache	 sli_replwkq_pending;
struct psc_listcache	 sli_replwkq_inflight;
struct psc_listcache	 sli_replwkq_finished;

void
sli_repl_addwk(uint64_t nid, struct slash_fidgen *fgp, sl_bmapno_t bmapno)
{
	struct sli_repl_workrq *w;

	w = psc_pool_get(sli_replwkrq_pool);
	w->srw_nid = nid;
	w->srw_fg = *fgp;
	w->srw_bmapno = bmapno;
	lc_add(&sli_replwkq_pending, w);
}

void
sli_repl_finishwk(struct sli_repl_workrq *w, int status)
{
	w->srw_status = status;
	lc_remove(&sli_replwkq_inflight, w);
	lc_add(&sli_replwkq_finished, w);
}

__dead void *
slireplfinthr_main(__unusedx void *arg)
{
	struct sli_repl_workrq *w;

	for (;;) {
		w = lc_getwait(&sli_replwkq_finished);
		sli_rmi_issue_repl_schedwk(w);
		psc_pool_return(sli_replwkbuf_pool, w->srw_srb);
		psc_pool_return(sli_replwkrq_pool, w);
		sched_yield();
	}
}

__dead void *
slireplinfthr_main(__unusedx void *arg)
{
	struct psc_listcache *lc;

	lc = &sli_replwkrq_pool->ppm_lc;
	for (;;) {
		nbrequest_reap(&sli_replwk_nbset);
		/* wait for a few seconds unless the thing fills up */
		LIST_CACHE_LOCK(lc);
		if (lc_sz(lc))
			/* XXX add minimum wait time to avoid CPU hogging */
			psc_waitq_waitrel_s(&lc->lc_wq_empty,
			    &lc->lc_lock, 3);
		else
			LIST_CACHE_LOCK(lc);
		sched_yield();
	}
}

__dead void *
slireplpndthr_main(__unusedx void *arg)
{
	struct slashrpc_cservice *csvc;
	struct sli_repl_workrq *w;
	char buf[PSC_NIDSTR_SIZE];
	struct sl_resm *resm;

	for (;;) {
		w = lc_getwait(&sli_replwkq_pending);
		resm = libsl_nid2resm(w->srw_nid);
		if (resm == NULL) {
			psc_errorx("%s: unknown resource member",
			    psc_nid2str(w->srw_nid, buf));
			w->srw_status = SLERR_ION_UNKNOWN;
			sli_rmi_issue_repl_schedwk(w);
			psc_pool_return(sli_replwkrq_pool, w);
			goto next;
		}
		csvc = sli_geticonn(resm);
		if (csvc == NULL) {
			w->srw_status = SLERR_ION_OFFLINE;
			sli_rmi_issue_repl_schedwk(w);
			psc_pool_return(sli_replwkrq_pool, w);
			goto next;
		}
		w->srw_status = sli_rii_issue_read(csvc->csvc_import, w);
		lc_add(&sli_replwkq_inflight, w);
 next:
		sched_yield();
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&sli_replwkrq_poolmaster, struct sli_repl_workrq,
	    srw_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL, "replwkrq");
	sli_replwkrq_pool = psc_poolmaster_getmgr(&sli_replwkrq_poolmaster);

	_psc_poolmaster_init(&sli_replwkbuf_poolmaster,
	    sizeof(struct sli_repl_buf) + SLASH_BMAP_SIZE,
	    offsetof(struct sli_repl_buf, srb_lentry),
	    0, 0, 32, 0, NULL, NULL, NULL, NULL, "replwkbuf");
	sli_replwkbuf_pool = psc_poolmaster_getmgr(&sli_replwkbuf_poolmaster);

	lc_reginit(&sli_replwkq_pending, struct sli_repl_workrq,
	    srw_lentry, "replwkpnd");
	lc_reginit(&sli_replwkq_inflight, struct sli_repl_workrq,
	    srw_lentry, "replwkinf");
	lc_reginit(&sli_replwkq_finished, struct sli_repl_workrq,
	    srw_lentry, "replwkfin");
}
