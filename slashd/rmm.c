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
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "rpc_mds.h"
#include "fidc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "sljournal.h"

int
slm_rmm_apply_update(__unusedx struct slmds_jent_namespace *jnamespace)
{
	int rc;
	struct srt_stat stat;

	switch (jnamespace->sjnm_op) {
	    case SJ_NAMESPACE_OP_CREATE:
		rc = mdsio_replay_create(
			jnamespace->sjnm_parent_s2id, jnamespace->sjnm_target_s2id, 
			jnamespace->sjnm_type, jnamespace->sjnm_uid,
			jnamespace->sjnm_gid, jnamespace->sjnm_mode, 
			jnamespace->sjnm_name);
		break;
	    case SJ_NAMESPACE_OP_REMOVE:
		rc = mdsio_replay_remove(
			jnamespace->sjnm_parent_s2id, jnamespace->sjnm_target_s2id, 
			jnamespace->sjnm_type, jnamespace->sjnm_name);
		break;
	    case SJ_NAMESPACE_OP_ATTRIB:
		stat.sst_uid = jnamespace->sjnm_uid;
		stat.sst_gid = jnamespace->sjnm_gid;
		stat.sst_mode = jnamespace->sjnm_mode;
		rc = mdsio_replay_attrib(jnamespace->sjnm_target_s2id, &stat, jnamespace->sjnm_mask);
		break;
	    default:
		psc_errorx("Unexpected opcode %d", jnamespace->sjnm_op);
		rc = -EINVAL;
	}
	return rc;
}

/*
 * slm_rmm_handle_connect - handle a CONNECT request from another MDS.
 */
int
slm_rmm_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMM_MAGIC || mq->version != SRMM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/*
 * slm_rmm_handle_send_namespace - handle a CONNECT request from another MDS.
 */
int
slm_rmm_handle_namespace_update(__unusedx struct pscrpc_request *rq)
{
	int i, rc;
	int count;
	uint64_t seqno;
	struct iovec iov;
	struct srm_generic_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct srm_send_namespace_req *mq;
	struct slmds_jent_namespace *jnamespace;
	psc_crc64_t crc;

	SL_RSX_ALLOCREP(rq, mq, mp);
	
	count = mq->count;
	seqno = mq->seqno;
	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK, SRMM_BULK_PORTAL, &iov, 1);
	if (mp->rc)
		goto out;
	if (desc)
		pscrpc_free_bulk(desc);
	/*
	 * Add code: If the sequence number is too old, reject right away.
	 */
	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc) {
		mp->rc = EINVAL;
		goto out;
	}

	/* iterate through the buffer and apply updates */
	rc = 0;
	jnamespace = (struct slmds_jent_namespace *) iov.iov_base;
	for (i = 0; i < count; i++) {
		rc = slm_rmm_apply_update(jnamespace);
		if (rc)
			break;
		jnamespace = (struct slmds_jent_namespace *)
			((char *)jnamespace + jnamespace->sjnm_reclen); 
	}
	mp->rc = rc;

out:
	PSCFREE(iov.iov_base);
	return (mp->rc);
}

/*
 * slm_rmm_handler - handle a request from another MDS.
 */
int
slm_rmm_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slm_rmm_handle_connect(rq);
		break;
		break;
	case SRMT_NAMESPACE_UPDATE:
		rc = slm_rmm_handle_namespace_update(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
#ifdef AUTHBUF
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
#endif
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
