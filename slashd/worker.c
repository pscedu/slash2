/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "psc_ds/listcache.h"
#include "psc_util/pool.h"
#include "psc_util/thread.h"

#include "worker.h"

struct psc_poolmaster	 pfl_workrq_poolmaster;
struct psc_poolmgr	*pfl_workrq_pool;
struct psc_listcache	 pfl_workq;

void *
_pfl_workq_getitem(int (*cb)(void *), size_t len)
{
	struct pfl_workrq *wk;

	psc_assert(len <= pfl_workrq_pool->ppm_entsize - sizeof(*wk));
	wk = psc_pool_get(pfl_workrq_pool);
	wk->wkrq_cbf = cb;
	return (PSC_AGP(wk, sizeof(*wk)));
}

void
pfl_workq_putitem(void *p)
{
	struct pfl_workrq *wk;

	psc_assert(p);
	wk = PSC_AGP(p, -sizeof(*wk));
	psclog_debug("placing work %p on queue", wk);
	lc_addtail(&pfl_workq, wk);
}

void
pfl_wkthr_main(__unusedx struct psc_thread *thr)
{
	struct pfl_workrq *wkrq;
	void *p;

	for (;;) {
		wkrq = lc_getwait(&pfl_workq);
		p = PSC_AGP(wkrq, sizeof(*wkrq));
		if (wkrq->wkrq_cbf(p)) {
			LIST_CACHE_LOCK(&pfl_workq);
			lc_addhead(&pfl_workq, wkrq);
			if (lc_nitems(&pfl_workq) == 1)
				psc_waitq_waitrel_us(
				    &pfl_workq.plc_wq_empty,
				    &pfl_workq.plc_lock, 1);
			else
				LIST_CACHE_ULOCK(&pfl_workq);
		} else
			psc_pool_return(pfl_workrq_pool, wkrq);
	}
}

void
pfl_workq_init(size_t bufsiz)
{
	_psc_poolmaster_init(&pfl_workrq_poolmaster,
	    sizeof(struct pfl_workrq) + bufsiz,
	    offsetof(struct pfl_workrq, wkrq_lentry), PPMF_AUTO, 128,
	    128, 0, NULL, NULL, NULL, NULL, "workrq");
	pfl_workrq_pool = psc_poolmaster_getmgr(&pfl_workrq_poolmaster);
	lc_reginit(&pfl_workq, struct pfl_workrq, wkrq_lentry, "workq");
}

void
pfl_wkthr_spawn(int thrtype, int nthr, const char *thrname)
{
	int i;

	for (i = 0; i < nthr; i++)
		pscthr_init(thrtype, 0, pfl_wkthr_main, NULL, 0,
		    thrname, i);
}
