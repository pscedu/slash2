/* $Id$ */

#include <stdio.h>

#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"

#include "repl_iod.h"
#include "rpc_iod.h"
#include "sltypes.h"

struct psc_poolmaster	 repl_workrq_poolmaster;
struct psc_poolmgr	*repl_workrq_pool;
struct psc_listcache	 repl_workq_pending;
struct psc_listcache	 repl_workq_inflight;
struct psc_listcache	 repl_workq_finished;

void
sli_repl_addwk(uint64_t nid, uint64_t fid, sl_bmapno_t bmapno)
{
	struct sli_repl_workrq *w;

	w = psc_pool_get(repl_workrq_pool);
	w->srw_nid = nid;
	w->srw_fid = fid;
	w->srw_bmapno = bmapno;
	lc_add(&repl_workq_pending, w);
}

void
sli_repl_finishwk(struct sli_repl_workrq *w, int status)
{
	w->srw_status = status;
	lc_remove(&repl_workq_inflight, w);
	lc_add(&repl_workq_finished, w);
}

__dead void *
slireplfinthr_main(void *arg)
{
	struct psc_thread *thr = arg;
	struct sli_repl_workrq *w;
	int rc;

	for (;;) {
		w = lc_getwait(&repl_workq_finished);
//		rc = sli_rmi_issue_schedwk(imp, w);
		psc_pool_return(repl_workrq_pool, w);
		sched_yield();
	}
}

__dead void *
slireplinfthr_main(void *arg)
{
	struct psc_thread *thr = arg;

	for (;;) {
		sched_yield();
//		check_set();
	}
}

__dead void *
slireplpndthr_main(void *arg)
{
	struct psc_thread *thr = arg;
	struct sli_repl_workrq *w;
	int rc;

	for (;;) {
		w = lc_getwait(&repl_workq_pending);
//		rc = sli_rii_issue_read(imp, w->rwkrq_fid, w->rwkrq_bmapno);
		lc_add(&repl_workq_inflight, w);
		sched_yield();
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&repl_workrq_poolmaster, struct sli_repl_workrq,
	    srw_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL, "replwkrq");
	repl_workrq_pool = psc_poolmaster_getmgr(&repl_workrq_poolmaster);

	lc_reginit(&repl_workq_pending, struct sli_repl_workrq,
	    srw_lentry, "replwkpnd");
	lc_reginit(&repl_workq_inflight, struct sli_repl_workrq,
	    srw_lentry, "replwkinf");
	lc_reginit(&repl_workq_finished, struct sli_repl_workrq,
	    srw_lentry, "replwkfin");
}
