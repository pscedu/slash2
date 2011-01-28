/* $Id$ */
/* %PSC_COPYRIGHT% */

#include "pfl/cdefs.h"
#include "psc_ds/listcache.h"
#include "psc_util/pool.h"
#include "psc_util/thread.h"

#include "slashd.h"

struct psc_poolmaster	 slm_workrq_poolmaster;
struct psc_poolmgr	*slm_workrq_pool;
struct psc_listcache	 slm_workq;

#define SLM_NWORKER_THREADS	4

void
slm_worker_main(__unusedx struct psc_thread *thr)
{
	struct slm_workrq *wkrq;

	for (;;) {
		wkrq = lc_getwait(&slm_workq);
		if (wkrq->wkrq_cbf(wkrq)) {
			LIST_CACHE_LOCK(&slm_workq);
			lc_addhead(&slm_workq, wkrq);
			if (lc_sz(&slm_workq) == 1)
				psc_waitq_waitrel_us(
				    &slm_workq.plc_wq_empty,
				    &slm_workq.plc_lock, 1);
			else
				LIST_CACHE_ULOCK(&slm_workq);
		} else
			psc_pool_return(slm_workrq_pool, wkrq);
	}
}

void
slm_workers_spawn(void)
{
	int i;

	psc_poolmaster_init(&slm_workrq_poolmaster, struct slm_workrq,
	    wkrq_lentry, PPMF_AUTO, 8, 8, 0, NULL, NULL, NULL,
	    "workrq");
	slm_workrq_pool = psc_poolmaster_getmgr(&slm_workrq_poolmaster);
	lc_reginit(&slm_workq, struct slm_workrq, wkrq_lentry, "workq");
	for (i = 0; i < SLM_NWORKER_THREADS; i++)
		pscthr_init(SLMTHRT_WORKER, 0, slm_worker_main,
		    NULL, 0, "slmwkr%d", i);
}
