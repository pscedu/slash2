/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for orchestrating coherency/properness across bmap lease
 * assignments so e.g. multiple IOS do not get assigned.
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
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"

#define SLM_CBARG_SLOT_CSVC	0
#define SLM_CBARG_SLOT_BML	1

void
slm_coh_bml_release(struct bmap_mds_lease *bml)
{
	struct bmap_mds_info *bmi;
	struct bmap *b;

	BML_LOCK(bml);
	bml->bml_flags &= ~BML_DIOCB;
	BML_ULOCK(bml);

	bmi = bml->bml_bmi;
	b = bmi_2_bmap(b);
	BMAP_LOCK(b);
	bmi->bmi_diocb--;
	BMAP_ULOCK(b);

	mds_bmap_bml_release(bml);
}

int
slm_rcm_bmapdio_cb(struct pscrpc_request *rq,
    __unusedx struct pscrpc_async_args *a)
{
	struct slashrpc_cservice *csvc =
	    rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC];
	struct bmap_mds_lease *bml =
	    rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_BML];
	struct srm_bmap_dio_req *mq;
	char buf[PSCRPC_NIDSTR_SIZE];
	int rc;

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_bmap_dio_rep, rc);
	if (rc)
		goto out;

	/*
	 * XXX if the client has given up the lease then we shouldn't
	 * consider that an error and should proceed.
	 */

	BML_LOCK(bml);
	bml->bml_flags |= BML_DIO;
	BML_ULOCK(bml);

 out:
	DEBUG_BMAP(rc ? PLL_WARN : PLL_DIAG, bml_2_bmap(bml),
	    "cli=%s seq=%"PRId64" rc=%d",
	    pscrpc_id2str(rq->rq_import->imp_connection->c_peer,
	    buf), mq->seq, rc);

	slm_coh_bml_release(bml);

	sl_csvc_decref(csvc);

	return (0);
}

/*
 * Request a lease holder to do direct I/O as the result of a
 * conflicting access request.
 *
 * Note: @bml is unlocked upon return.
 */
void
mdscoh_req(struct bmap_mds_lease *bml)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_bmap_dio_req *mq;
	struct srm_bmap_dio_rep *mp;
	int rc;

	DEBUG_BMAP(PLL_DIAG, bml_2_bmap(bml), "bml=%p", bml);

	BML_LOCK_ENSURE(bml);
	BMAP_LOCK_ENSURE(b);

	/* Take a reference for the asynchronous RPC. */
	bmi->bmi_diocb++;
	bml->bml_refcnt++;
	bml->bml_flags |= BML_DIOCB;

	if (bml->bml_flags & BML_RECOVER) {
		psc_assert(!bml->bml_exp);
		PFL_GOTOERR(out, rc = -PFLERR_NOTCONN);
	}
	psc_assert(bml->bml_exp);

	csvc = slm_getclcsvc(bml->bml_exp);
	if (csvc == NULL)
		PFL_GOTOERR(out, rc = -PFLERR_NOTCONN);
	BML_ULOCK(bml);

	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPDIO, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->fid = fcmh_2_fid(bml_2_bmap(bml)->bcm_fcmh);
	mq->bno = bml_2_bmap(bml)->bcm_bmapno;
	mq->seq = bml->bml_seq;
	mq->dio = 1;

	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_BML] = bml;
	rq->rq_interpret_reply = slm_rcm_bmapdio_cb;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc == 0)
		return;

 out:
	pscrpc_req_finished(rq);
	if (csvc)
		sl_csvc_decref(csvc);

	slm_coh_bml_release(bml);
}
