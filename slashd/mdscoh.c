/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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

#include "bmap.h"
#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "mdscoh.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

struct pscrpc_nbreqset	bmapCbSet =
    PSCRPC_NBREQSET_INIT(bmapCbSet, NULL, mdscoh_cb);

#define SLM_CBARG_SLOT_CSVC	0

struct pscrpc_completion mdsCohCompl;

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
	struct fidc_membh *f;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_lease *bml;
	int rc = 0, new_bmap = 0;

	mq = pscrpc_msg_buf(req->rq_reqmsg, 0, sizeof(*mq));
	mp = pscrpc_msg_buf(req->rq_repmsg, 0, sizeof(*mp));
	csvc = req->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC];
	
	if (req->rq_err)
		rc = req->rq_err;

	else if (req->rq_status)
		rc = req->rq_status;

	if (rc) {
		psclog_warnx("cli=%s seq=%"PRId64" rq_status=%d mp->rc=%d",
		   libcfs_id2str(req->rq_import->imp_connection->c_peer),
		   mq->seq, req->rq_status, mp ? mp->rc : -1);	
		goto out;
	}

	if (mp && mp->rc)
		rc = mp->rc;

	//XXX if the client has given up the lease then we shouldn't consider
	// that an error and should proceed.

	/* Leases can come and go regardless of pending coh cb's.
	 */
	f = fidc_lookup_fid(mq->fid);
	if (!f)
		PFL_GOTOERR(out, rc = -ENOENT);
	
	b = bmap_lookup_cache_locked(f, mq->blkno, &new_bmap);

	FCMH_LOCK(f);
	fcmh_decref(f, FCMH_OPCNT_LOOKUP_FIDC);
	FCMH_ULOCK(f);

	if (!b)
		PFL_GOTOERR(out, rc = -ENOENT);

	bml = mds_bmap_getbml_locked(b, mq->seq, 
	     req->rq_peer.nid, req->rq_peer.pid);

	if (!bml)
		PFL_GOTOERR(out, rc = -ENOENT);

	BML_LOCK(bml);
	bml->bml_flags |= BML_CDIO;
	if (bml->bml_flags & BML_COHDIO) {
		/* Test for BMAP_DIORQ.  If it was removed, the
		 * the writer holding the lease, on behalf of 
		 * which this rq was issued, has relinquished
		 * its lease.  Therefore, DIO conversion may be 
		 * bypassed here.
		 */
		if (b->bcm_flags & BMAP_DIORQ) {
			b->bcm_flags &= ~BMAP_DIORQ;
			b->bcm_flags |= BMAP_DIO;
		}

		bml->bml_flags &= ~BML_COHDIO;
		
		DEBUG_BMAP(PLL_WARN, bml_2_bmap(bml), "converted to dio=%d",
			   !!(b->bcm_flags & BMAP_DIO));
	}
	BML_ULOCK(bml);
	mds_bmap_bml_release(bml);

 out:
	if (b) {
		DEBUG_BMAP(rc ? PLL_WARN : PLL_NOTIFY, b,
		   "cli=%s seq=%"PRId64" rq_status=%d mp->rc=%d",
		   libcfs_id2str(req->rq_import->imp_connection->c_peer),
		   mq->seq, req->rq_status, mp ? mp->rc : -1);	
		/* I release bmap lock.
		 */
		bmap_op_done(b);
	}
	sl_csvc_decref(csvc);

	return (rc);
}

/**
 * mdscoh_req - Request a lease holder to do direct I/O as the result of
 *	a conflicting access request.
 */
int
mdscoh_req(struct bmap_mds_lease *bml)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	int rc = 0;

	DEBUG_BMAP(PLL_NOTIFY, bml_2_bmap(bml), "bml=%p", bml);

	BML_LOCK(bml);
	if (bml->bml_flags & BML_RECOVER) {
		psc_assert(!bml->bml_exp);
		BML_ULOCK(bml);
		return (-ENOTCONN);
	
	}
	psc_assert(bml->bml_exp);		

	csvc = slm_getclcsvc(bml->bml_exp);
	if (csvc == NULL) {
		BML_ULOCK(bml);
		return (-ENOTCONN);
	}
	BML_ULOCK(bml);

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPDIO, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return (rc);
	}

	rq->rq_comp = &mdsCohCompl;
	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;

	mq->fid = fcmh_2_fid(bml_2_bmap(bml)->bcm_fcmh);
	mq->blkno = bml_2_bmap(bml)->bcm_bmapno;
	mq->seq = bml->bml_seq;
	mq->dio = 1;

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	psc_assert(pscrpc_nbreqset_add(&bmapCbSet, rq) == 0);

	return (0);
}

void
slmcohthr_begin(__unusedx struct psc_thread *thr)
{
	pscrpc_completion_init(&mdsCohCompl);

	while (pscthr_run()) {
		mdscoh_reap();
		pscrpc_completion_waitrel_s(&mdsCohCompl, 1);
	}
}

void
slmcohthr_spawn(void)
{
	pscthr_init(SLMTHRT_COH, 0, slmcohthr_begin,
	    NULL, 0, "slmcohthr");
}
