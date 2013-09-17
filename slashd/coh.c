/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * coh - Routines for orchestrating coherency/properness across bmap
 *	lease assignments so e.g. multiple IOS do not get assigned.
 */

#include <time.h>

#include "pfl/cdefs.h"
#include "pfl/listcache.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "psc_util/atomic.h"
#include "psc_util/completion.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "mdscoh.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

struct pscrpc_nbreqset	slm_bmap_cbset=
    PSCRPC_NBREQSET_INIT(slm_bmap_cbset, NULL, mdscoh_cb);

#define SLM_CBARG_SLOT_CSVC	0

struct psc_compl slm_coh_compl = PSC_COMPL_INIT;

int
mdscoh_cb(struct pscrpc_request *req,
    __unusedx struct pscrpc_async_args *a)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	struct fidc_membh *f;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_lease *bml;
	int rc = 0, new_bmap = 0;

	OPSTAT_INCR(SLM_OPST_COHERENT_CB);
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

	/*
	 * XXX if the client has given up the lease then we shouldn't
	 * consider that an error and should proceed.
	 */

	/* Leases can come and go regardless of pending coh cb's. */
	f = fidc_lookup_fid(mq->fid);
	if (!f)
		PFL_GOTOERR(out, rc = -ENOENT);

	b = bmap_lookup_cache(f, mq->blkno, &new_bmap);

	FCMH_LOCK(f);
	fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);

	if (!b)
		PFL_GOTOERR(out, rc = -ENOENT);

	bml = mds_bmap_getbml_locked(b, mq->seq, req->rq_peer.nid,
	    req->rq_peer.pid);

	if (!bml)
		PFL_GOTOERR(out, rc = -ENOENT);

	BML_LOCK(bml);
	bml->bml_flags |= BML_DIO;
	bml->bml_flags &= ~BML_DIOCB;
	BML_ULOCK(bml);
	mds_bmap_bml_release(bml);

 out:
	if (b) {
		DEBUG_BMAP(rc ? PLL_WARN : PLL_INFO, b,
		    "cli=%s seq=%"PRId64" rq_status=%d mp->rc=%d",
		    libcfs_id2str(req->rq_import->imp_connection->c_peer),
		    mq->seq, req->rq_status, mp ? mp->rc : -1);

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
	int rc;

	DEBUG_BMAP(PLL_INFO, bml_2_bmap(bml), "bml=%p", bml);

	BML_LOCK_ENSURE(bml);
	if (bml->bml_flags & BML_RECOVER) {
		psc_assert(!bml->bml_exp);
		BML_ULOCK(bml);
		return (-PFLERR_NOTCONN);
	}
	psc_assert(bml->bml_exp);

	csvc = slm_getclcsvc(bml->bml_exp);
	if (csvc == NULL) {
		BML_ULOCK(bml);
		return (-PFLERR_NOTCONN);
	}
	BML_ULOCK(bml);

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPDIO, rq, mq, mp);
	if (rc) {
		sl_csvc_decref(csvc);
		return (rc);
	}

	rq->rq_compl = &slm_coh_compl;
	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;

	mq->fid = fcmh_2_fid(bml_2_bmap(bml)->bcm_fcmh);
	mq->blkno = bml_2_bmap(bml)->bcm_bmapno;
	mq->seq = bml->bml_seq;
	mq->dio = 1;

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);
	psc_assert(pscrpc_nbreqset_add(&slm_bmap_cbset, rq) == 0);

	OPSTAT_INCR(SLM_OPST_COHERENT_REQ);
	return (0);
}

void
slmcohthr_begin(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		psc_compl_waitrel_s(&slm_coh_compl, 1);
		pscrpc_nbreqset_reap(&slm_bmap_cbset);
	}
}

void
slmcohthr_spawn(void)
{
	pscthr_init(SLMTHRT_COH, 0, slmcohthr_begin, NULL, 0,
	    "slmcohthr");
}
