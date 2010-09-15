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
 * Routines for handling RPC requests for MDS from MDS.
 */

#include <stdio.h>

#include "pfl/str.h"
#include "psc_rpc/export.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "fidc_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "sljournal.h"

/* list of peer MDSes and its lock */
extern struct psc_dynarray	 mds_namespace_peerlist;
extern psc_spinlock_t		 mds_namespace_peerlist_lock;

extern struct sl_mds_peerinfo	*localinfo;

int
slm_rmm_cmp_peerinfo(const void *a, const void *b)
{
	const struct sl_mds_peerinfo *x = a, *y = b;

	return (CMP(x->sp_siteid, y->sp_siteid));
}

int
slm_rmm_apply_update(struct slmds_jent_namespace *jnamespace)
{
	int rc;

	rc = mds_redo_namespace(jnamespace);
	if (rc)
		psc_atomic32_inc(&localinfo->sp_stats.ns_stats[NS_DIR_RECV][
		    jnamespace->sjnm_op][NS_SUM_FAIL]);
	else
		psc_atomic32_inc(&localinfo->sp_stats.ns_stats[NS_DIR_RECV][
		    jnamespace->sjnm_op][NS_SUM_SUCC]);
	return (rc);
}

/**
 * slm_rmm_handle_connect - Handle a CONNECT request from another MDS.
 */
int
slm_rmm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMM_MAGIC || mq->version != SRMM_VERSION)
		mp->rc = EINVAL;
	return (0);
}

/**
 * slm_rmm_handle_send_namespace - Handle a SEND_NAMESPACE request from
 *	another MDS.
 */
int
slm_rmm_handle_namespace_update(struct pscrpc_request *rq)
{
	struct slmds_jent_namespace *jnamespace;
	struct srm_send_namespace_req *mq;
	struct sl_mds_peerinfo *p, pi;
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct iovec iov;
	int i, rc, count;
	uint64_t crc;
	uint64_t seqno;

	SL_RSX_ALLOCREP(rq, mq, mp);

	count = mq->count;
	seqno = mq->seqno;
	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	rc = mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMM_BULK_PORTAL, &iov, 1);
	if (rc)
		goto out;

	if (desc)
		pscrpc_free_bulk(desc);

	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc) {
		rc = mp->rc = EINVAL;
		goto out;
	}

	/* Search for the peer information by the given site ID. */
	pi.sp_siteid = mq->siteid;
	spinlock(&mds_namespace_peerlist_lock);
	i = psc_dynarray_bsearch(&mds_namespace_peerlist, &pi,
	    slm_rmm_cmp_peerinfo);
	if (i >= psc_dynarray_len(&mds_namespace_peerlist))
		p = NULL;
	else
		p = psc_dynarray_getpos(&mds_namespace_peerlist, i);
	freelock(&mds_namespace_peerlist_lock);
	if (!p || p->sp_siteid != mq->siteid) {
		psc_info("fail to find site ID %d", mq->siteid);
		rc = mp->rc = EINVAL;
		goto out;
	}

	/*
	 * Make sure that the seqno number matches what we expect
	 * (strictly in-order delivery).  If not, reject right away.
	 */
	if (p->sp_recv_seqno > seqno) {
		/*
		 * This is okay; our peer may have just lost patience
		 * with us and decide to resend.
		 */
		psc_notify("seq number %"PRIx64" is less than %"PRIx64,
		    seqno, p->sp_recv_seqno);
		mp->rc = EINVAL;
		goto out;
	}
	if (p->sp_recv_seqno < seqno) {
		psc_notify("seq number %"PRIx64" is greater than %"PRIx64,
		    seqno, p->sp_recv_seqno);
		mp->rc = EINVAL;
		goto out;
	}

	/* iterate through the namespace update buffer and apply updates */
	jnamespace = (struct slmds_jent_namespace *) iov.iov_base;
	for (i = 0; i < count; i++) {
		mp->rc = slm_rmm_apply_update(jnamespace);
		if (mp->rc)
			break;
		jnamespace = (struct slmds_jent_namespace *)
		    ((char *)jnamespace + jnamespace->sjnm_reclen);
	}
	/* Should I ask for a resend if I have trouble applying updates? */
	p->sp_recv_seqno = seqno + count;

 out:
	PSCFREE(iov.iov_base);
	return (rc);
}

/**
 * slm_rmm_handler - Handle a request for MDS from another MDS.
 */
int
slm_rmm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slc_getmcsvc(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slm_rmm_handle_connect(rq);
		break;
	case SRMT_NAMESPACE_UPDATE:
		rc = slm_rmm_handle_namespace_update(rq);
		break;
	default:
		psc_errorx("unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
