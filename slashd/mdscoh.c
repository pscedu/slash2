/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <time.h>

#include "pfl/cdefs.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/atomic.h"
#include "psc_util/log.h"

#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "mdscoh.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

struct psc_listcache	pndgBmapCbs;
struct psc_listcache	inflBmapCbs;
struct pscrpc_nbreqset	bmapCbSet =
    PSCRPC_NBREQSET_INIT(bmapCbSet, NULL, mdscoh_cb);

#define SLM_CBARG_SLOT_BML	0
#define SLM_CBARG_SLOT_CSVC	1

void
mdscoh_reap(void)
{
	pscrpc_nbreqset_reap(&bmapCbSet);
}

int
mdscoh_cb(struct pscrpc_request *req, __unusedx struct pscrpc_async_args *a)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	struct bmap_mds_lease *bml;
	int flags;

	mq = pscrpc_msg_buf(req->rq_reqmsg, 0, sizeof(*mq));
	mp = pscrpc_msg_buf(req->rq_repmsg, 0, sizeof(*mp));
	bml = req->rq_async_args.pointer_arg[SLM_CBARG_SLOT_BML];
	csvc = req->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC];

	DEBUG_BMAP(mp->rc ? PLL_ERROR : PLL_NOTIFY, bml_2_bmap(bml),
	   "cli=%s bml=%p seq=%"PRId64" rc=%d",
	   libcfs_id2str(req->rq_import->imp_connection->c_peer),
	   bml, bml->bml_seq, mp->rc);

	lc_remove(&inflBmapCbs, bml);

	BML_LOCK(bml);
	psc_assert(bml->bml_flags & BML_COH);
	bml->bml_flags &= ~BML_COH;
	flags = bml->bml_flags;
	BML_ULOCK(bml);

	if (flags & BML_COHDIO) {
		BMAP_LOCK(bml_2_bmap(bml));
		psc_assert(bml_2_bmap(bml)->bcm_flags & BMAP_DIORQ);
		bml_2_bmap(bml)->bcm_flags &= ~BMAP_DIORQ;
		bml_2_bmap(bml)->bcm_flags |= BMAP_DIO;
		BMAP_ULOCK(bml_2_bmap(bml));

		DEBUG_BMAP(PLL_WARN, bml_2_bmap(bml), "converted to dio");
	}

	/* Lock again and try to free
	 */
	BML_LOCK(bml);
	if (!(bml->bml_flags & BML_FREEING) && (bml->bml_flags & BML_COHRLS)) {
		bml->bml_flags |= BML_FREEING;
		BML_ULOCK(bml);
		mds_bmap_bml_release(bml);
	} else
		BML_ULOCK(bml);

	/* bmap_op_done_type() will wake any waiters.
	 */
	bmap_op_done_type(bml_2_bmap(bml), BMAP_OPCNT_COHCB);

	sl_csvc_decref(csvc);

	return (0);
}

/**
 * mdscoh_req - Request a lease holder to do direct I/O as the result of
 *	a conflicting access request.
 */
int
mdscoh_req(struct bmap_mds_lease *bml, int block)
{
	struct pscrpc_export *exp = bml->bml_exp;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	int rc = 0;

	DEBUG_BMAP(PLL_NOTIFY, bml_2_bmap(bml), "bml=%p", bml);

	BML_LOCK(bml);
	psc_assert(!(bml->bml_flags & BML_COH));

	if (!(bml->bml_flags & BML_EXP)) {
		BML_ULOCK(bml);
		return (block ? -ENOTCONN : 0);
	}
	bml->bml_flags |= BML_COH;
	/* XXX How do we deal with a closing export?
	 */
	csvc = slm_getclcsvc(exp);
	if (csvc == NULL)
		bml->bml_flags &= ~BML_COH;
	BML_ULOCK(bml);
	if (csvc == NULL)
		return (-1);

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPDIO, rq, mq, mp);
	if (rc)
		goto out;

	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_BML] = bml;
	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;

	mq->fid = fcmh_2_fid(bml_2_bmap(bml)->bcm_fcmh);
	mq->blkno = bml_2_bmap(bml)->bcm_bmapno;
	mq->dio = 1;
	mq->seq = bml->bml_seq;

	if (block == MDSCOH_BLOCK) {
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
	} else {
		bmap_op_start_type(bml_2_bmap(bml), BMAP_OPCNT_COHCB);
		authbuf_sign(rq, PSCRPC_MSG_REQUEST);
		psc_assert(pscrpc_nbreqset_add(&bmapCbSet, rq) == 0);
		lc_addtail(&inflBmapCbs, bml);
		rq = NULL;
		csvc = NULL;
	}

 out:
	if (rq)
		pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);
	return (rc);
}

void
slmcohthr_begin(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		mdscoh_reap();
		sleep(1);
	}
}

void
slmcohthr_spawn(void)
{
	pscthr_init(SLMTHRT_COH, 0, slmcohthr_begin,
	    NULL, 0, "slmcohthr");
}
