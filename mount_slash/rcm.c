/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Routines for handling RPC requests for CLIENT from MDS.
 */

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"

#include "ctl_cli.h"
#include "ctlsvr_cli.h"
#include "slashrpc.h"

/**
 * msrcm_handle_getreplst - handle a GETREPLST request for client from MDS,
 *	which would have been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctl_replst_cont *mrc;
	struct msctl_replstq *mrsq;
	struct sl_resource *res;
	int n;

	RSX_ALLOCREP(rq, mq, mp);

	mrc = psc_pool_get(msctl_replstmc_pool);

	/* find corresponding queue */
	PLL_LOCK(&msctl_replsts);
	PLL_FOREACH(mrsq, &msctl_replsts)
		if (mrsq->mrsq_id == mq->id) {
			if (mq->rc) {
				/* XXX completion all mrcs */
				psc_completion_done(&mrsq->mrsq_compl, 0);
				break;
			}
			/* fill in data */
			memset(mrc, 0, sizeof(*mrc));
			psc_completion_init(&mrc->mrc_compl);
			pll_init(&mrc->mrc_bdata,
			    struct msctl_replst_slave_cont,
			    mrsc_lentry, NULL);
			mrc->mrc_mrs.mrs_id = mq->id;
			mrc->mrc_mrs.mrs_fg = mq->fg;
			mrc->mrc_mrs.mrs_nbmaps = mq->nbmaps;
			mrc->mrc_mrs.mrs_newreplpol = mq->newreplpol;
			mrc->mrc_mrs.mrs_nios = mq->nrepls;
			for (n = 0; n < (int)mq->nrepls; n++) {
				res = libsl_id2res(mq->repls[n].bs_id);
				if (res)
					strlcpy(mrc->mrc_mrs.mrs_iosv[n],
					    res->res_name, RES_NAME_MAX);
				else
					strlcpy(mrc->mrc_mrs.mrs_iosv[n],
					    "<unknown IOS>", RES_NAME_MAX);
			}
			pll_add(&mrsq->mrsq_mrcs, mrc);
			mrc = NULL;
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	if (mrc)
		psc_pool_return(msctl_replstmc_pool, mrc);
	return (0);
}

/**
 * msrcm_handle_getreplst_slave - handle a GETREPLST request for client from MDS,
 *	which would have been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst_slave(struct pscrpc_request *rq)
{
	struct msctl_replst_slave_cont *mrsc;
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct msctl_replst_cont *mrc;
	struct pscrpc_bulk_desc *desc;
	struct msctl_replstq *mrsq;
	struct iovec iov;

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->len < 0 || mq->len > SRM_REPLST_PAGESIZ) {
		mp->rc = EINVAL;
		return (mp->rc);
	}

	mrsc = psc_pool_get(msctl_replstsc_pool);

	/* find corresponding queue */
	PLL_LOCK(&msctl_replsts);
	PLL_FOREACH(mrsq, &msctl_replsts)
		if (mrsq->mrsq_id == mq->id) {
			PLL_LOCK(&mrsq->mrsq_mrcs);
			PLL_FOREACH(mrc, &mrsq->mrsq_mrcs)
				if (SAMEFG(&mrc->mrc_mrs.mrs_fg, &mq->fg))
					break;
			PLL_ULOCK(&mrsq->mrsq_mrcs);

			if (mrc == NULL)
				break;

			if (mq->rc)
				psc_completion_done(&mrc->mrc_compl, 0);
			else if (mq->len < 1)
				mp->rc = EINVAL;
			else {
				iov.iov_base = mrsc->mrsc_mrsl.mrsl_data;
				iov.iov_len = mq->len;

				mrsc->mrsc_len = mq->len;
				mrsc->mrsc_mrsl.mrsl_id = mq->id;
				mrsc->mrsc_mrsl.mrsl_boff = mq->boff;
				mrsc->mrsc_mrsl.mrsl_nbmaps = mq->nbmaps;
				mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
				    SRCM_BULK_PORTAL, &iov, 1);
				pll_add(&mrc->mrc_bdata, mrsc);
				mrsc = NULL;
			}
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	if (mrsc)
		psc_pool_return(msctl_replstsc_pool, mrsc);
	return (mp->rc);
}

/**
 * msrcm_handle_releasebmap - handle a RELEASEBMAP request for client from MDS.
 * @rq: request.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

/**
 * msrcm_handle_connect - handle a CONNECT request for client from MDS.
 * @rq: request.
 */
int
msrcm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCM_MAGIC || mq->version != SRCM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/**
 * slc_rcm_handler - handle a request for CLIENT from MDS.
 * @rq: request.
 */
int
slc_rcm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = msrcm_handle_connect(rq);
		break;
	case SRMT_REPL_GETST:
		rc = msrcm_handle_getreplst(rq);
		break;
	case SRMT_REPL_GETST_SLAVE:
		rc = msrcm_handle_getreplst_slave(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = msrcm_handle_releasebmap(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
