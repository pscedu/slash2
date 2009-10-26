/* $Id$ */

/*
 * Routines for handling RPC requests for CLIENT from MDS.
 */

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"

#include "ctl_cli.h"
#include "slashrpc.h"

/*
 * msrcm_handle_getreplst - handle a GETREPLST request for client from MDS,
 *	which would have been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct msctl_replstq *mrsq;
	struct sl_site *site;
	int n;

	RSX_ALLOCREP(rq, mq, mp);

	/* find corresponding queue */
	PLL_LOCK(&msctl_replsts);
	PLL_FOREACH(mrsq, &msctl_replsts)
		if (mrsq->mrsq_id == mq->id) {
			if (mq->rc) {
				lc_kill(&mrsq->mrsq_lc);
				break;
			}

//			strlcpy(mrc->mrc_mrs.mrs_fn, mq->fn,
//			    sizeof(mrc->mrc_mrs.mrs_fn));
			mrsq->mrsq_id = mq->id;
			mrsq->mrsq_nios = mq->nrepls;
			for (n = 0; n < (int)mq->nrepls; n++) {
				site = libsl_id2site(mq->repls[n].bs_id);
				strlcpy(mrsq->mrsq_iosv[n],
				    site->site_name, SITE_NAME_MAX);
			}
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	return (0);
}

/*
 * msrcm_handle_getreplst_slave - handle a GETREPLST request for client from MDS,
 *	which would have been initiated by a client request originally.
 * @rq: request.
 */
int
msrcm_handle_getreplst_slave(struct pscrpc_request *rq)
{
	struct srm_replst_slave_req *mq;
	struct srm_replst_slave_rep *mp;
	struct msctl_replst_cont *mrc;
	struct msctl_replstq *mrsq;

	RSX_ALLOCREP(rq, mq, mp);

	/* find corresponding queue */
	PLL_LOCK(&msctl_replsts);
	PLL_FOREACH(mrsq, &msctl_replsts)
		if (mrsq->mrsq_id == mq->id) {
			if (mq->rc) {
				lc_kill(&mrsq->mrsq_lc);
				break;
			}

			mrc = PSCALLOC(sizeof(*mrc) + mq->len);
			mrc->mrc_mrs.mrs_id = mq->id;
			//bulkserver();
			lc_add(&mrsq->mrsq_lc, mrc);
			break;
		}
	PLL_ULOCK(&msctl_replsts);
	return (0);
}

/*
 * msrcm_handle_releasebmap - handle a RELEASEBMAP request for client from MDS.
 */
int
msrcm_handle_releasebmap(struct pscrpc_request *rq)
{
	struct srm_releasebmap_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

/*
 * msrcm_handle_connect - handle a CONNECT request for client from MDS.
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

/*
 * msrcm_handler - handle a request for client from MDS.
 * @rq: request.
 */
int
msrcm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = msrcm_handle_connect(rq);
		break;
	case SRMT_GETREPLST:
		rc = msrcm_handle_getreplst(rq);
		break;
	case SRMT_GETREPLST_SLAVE:
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
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
