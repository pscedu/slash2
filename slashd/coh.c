/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/atomic.h"
#include "pfl/cdefs.h"
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/workthr.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "cache_params.h"
#include "fidc_mds.h"
#include "mdscoh.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

#define SLM_CBARG_SLOT_CSVC	0

struct slm_wkdata_coh_releasebml {
	slfid_t			fid;
	sl_bmapno_t		bno;
	uint64_t		seq;
	lnet_process_id_t	peer;
};

int
slmcoh_releasebml(void *p)
{
	int rc = 0, new_bmap = 0;
	char buf[PSCRPC_NIDSTR_SIZE];
	struct slm_wkdata_coh_releasebml *wk = p;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_lease *bml;
	struct fidc_membh *f;

	/* Leases can come and go regardless of pending coh cb's. */
	rc = fidc_lookup_fid(wk->fid, &f);
	if (rc)
		PFL_GOTOERR(out, rc);

	b = bmap_lookup_cache(f, wk->bno, &new_bmap);
	if (!b)
		PFL_GOTOERR(out, rc = -ENOENT);

	bml = mds_bmap_getbml(b, wk->seq, wk->peer.nid, wk->peer.pid);
	if (!bml)
		PFL_GOTOERR(out, rc = -ENOENT);

	BML_LOCK(bml);
	bml->bml_flags |= BML_DIO;
	bml->bml_flags &= ~BML_DIOCB;
	BML_ULOCK(bml);
	mds_bmap_bml_release(bml);

 out:
	if (b) {
		DEBUG_BMAP(rc ? PLL_WARN : PLL_DIAG, b,
		    "cli=%s seq=%"PRId64" rc=%d",
		    pscrpc_id2str(wk->peer, buf), wk->seq, rc);
		bmap_op_done(b);
	} else
		psclog_warnx("cli=%s seq=%"PRId64" rc=%d",
		    pscrpc_id2str(wk->peer, buf), wk->seq, rc);
	if (f)
		fcmh_op_done(f);
	return (0);
}

int
mdscoh_cb(struct pscrpc_request *rq,
    __unusedx struct pscrpc_async_args *a)
{
	struct slashrpc_cservice *csvc =
	    rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC];
	struct slm_wkdata_coh_releasebml *wk;
	struct srm_bmap_dio_req *mq;
	char buf[PSCRPC_NIDSTR_SIZE];
	int rc;

	OPSTAT_INCR(SLM_OPST_COHERENT_CB);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_dio_rep, rc);
	if (rc) {
		psclog_warnx("cli=%s seq=%"PRId64" rc=%d",
		    pscrpc_id2str(rq->rq_import->imp_connection->c_peer,
		    buf), mq->seq, rc);
		goto out;
	}

	/*
	 * XXX if the client has given up the lease then we shouldn't
	 * consider that an error and should proceed.
	 */

	wk = pfl_workq_getitem(slmcoh_releasebml,
	    struct slm_wkdata_coh_releasebml);
	wk->fid = mq->fid;
	wk->bno = mq->blkno;
	wk->seq = mq->seq;
	wk->peer = rq->rq_peer;
	pfl_workq_putitem(wk);

 out:
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

	DEBUG_BMAP(PLL_DIAG, bml_2_bmap(bml), "bml=%p", bml);

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

	mq->fid = fcmh_2_fid(bml_2_bmap(bml)->bcm_fcmh);
	mq->blkno = bml_2_bmap(bml)->bcm_bmapno;
	mq->seq = bml->bml_seq;
	mq->dio = 1;

	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;
	rq->rq_interpret_reply = mdscoh_cb;
	psc_assert(SL_NBRQSET_ADD(csvc, rq) == 0);

	OPSTAT_INCR(SLM_OPST_COHERENT_REQ);
	return (0);
}
