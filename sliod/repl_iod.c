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

struct psc_poolmaster	 repl_workrq_poolmaster;
struct psc_poolmgr	*repl_workrq_pool;
struct psc_poolmaster	 sli_repl_bufpoolmaster;
struct psc_poolmgr	*sli_repl_bufpool;
struct psc_listcache	 repl_workq_pending;
struct psc_listcache	 repl_workq_inflight;
struct psc_listcache	 repl_workq_finished;

void
sli_repl_addwk(uint64_t nid, struct slash_fidgen *fgp, sl_bmapno_t bmapno)
{
	struct sli_repl_workrq *w;

	w = psc_pool_get(repl_workrq_pool);
	w->srw_nid = nid;
	w->srw_fg = *fgp;
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
slireplfinthr_main(__unusedx void *arg)
{
	struct sli_repl_workrq *w;

	for (;;) {
		w = lc_getwait(&repl_workq_finished);
		sli_rmi_issue_repl_schedwk(w);
		psc_pool_return(sli_repl_bufpool, w->srw_srb);
		psc_pool_return(repl_workrq_pool, w);
		sched_yield();
	}
}

__dead void *
slireplinfthr_main(__unusedx void *arg)
{
	for (;;) {
		sched_yield();
//		check_set();
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
		w = lc_getwait(&repl_workq_pending);
		resm = libsl_nid2resm(w->srw_nid);
		if (resm == NULL) {
			psc_errorx("%s: unknown resource member",
			    psc_nid2str(w->srw_nid, buf));
			w->srw_status = SLERR_ION_UNKNOWN;
			sli_rmi_issue_repl_schedwk(w);
			psc_pool_return(repl_workrq_pool, w);
			goto next;
		}
		csvc = sli_geticonn(resm);
		if (csvc == NULL) {
			w->srw_status = SLERR_ION_OFFLINE;
			sli_rmi_issue_repl_schedwk(w);
			psc_pool_return(repl_workrq_pool, w);
			goto next;
		}
		w->srw_status = sli_rii_issue_read(csvc->csvc_import, w);
		lc_add(&repl_workq_inflight, w);
 next:
		sched_yield();
	}
}

void
sli_repl_init(void)
{
	psc_poolmaster_init(&repl_workrq_poolmaster, struct sli_repl_workrq,
	    srw_lentry, PPMF_AUTO, 256, 256, 0, NULL, NULL, NULL, "replwkrq");
	repl_workrq_pool = psc_poolmaster_getmgr(&repl_workrq_poolmaster);

	_psc_poolmaster_init(&sli_repl_bufpoolmaster,
	    sizeof(struct sli_repl_buf) + SLASH_BMAP_SIZE,
	    offsetof(struct sli_repl_buf, srb_lentry),
	    0, 0, 32, 0, NULL, NULL, NULL, NULL, "replbuf");
	sli_repl_bufpool = psc_poolmaster_getmgr(&sli_repl_bufpoolmaster);

	lc_reginit(&repl_workq_pending, struct sli_repl_workrq,
	    srw_lentry, "replwkpnd");
	lc_reginit(&repl_workq_inflight, struct sli_repl_workrq,
	    srw_lentry, "replwkinf");
	lc_reginit(&repl_workq_finished, struct sli_repl_workrq,
	    srw_lentry, "replwkfin");
}
