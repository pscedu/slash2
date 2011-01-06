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
 * Routines for handling RPC requests for ION from MDS.
 */

#include <errno.h>
#include <stdio.h>

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "authbuf.h"
#include "bmap.h"
#include "bmap_iod.h"
#include "repl_iod.h"
#include "rpc_iod.h"
#include "slashrpc.h"
#include "slconn.h"
#include "slerr.h"
#include "sliod.h"

struct reclaim_log_entry {
	uint64_t		 xid;
	slfid_t			 fid;
	slfgen_t		 gen;
};

/**
 * sli_rim_handle_reclaim - handle RECLAIM RPC from the MDS as a result
 *	of unlink or truncate to zero. The MDS won't send us a new RPC
 *	until we reply, so we should be thread-safe.
 */
int
sli_rim_handle_reclaim(struct pscrpc_request *rq)
{
	char fidfn[PATH_MAX];
	struct slash_fidgen oldfg;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct pscrpc_bulk_desc *desc;
	uint64_t crc, xid;
	int16_t count;
	struct iovec iov;
	struct reclaim_log_entry *entry;
	int i, rc;

	SL_RSX_ALLOCREP(rq, mq, mp);

	count = mq->count;
	xid = mq->xid;
	iov.iov_len = mq->size;
	iov.iov_base = PSCALLOC(mq->size);

	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMM_BULK_PORTAL, &iov, 1);

	if (mp->rc)
		goto out;
	if (desc)
		pscrpc_free_bulk(desc);

	psc_crc64_calc(&crc, iov.iov_base, iov.iov_len);
	if (crc != mq->crc) {
		mp->rc = EINVAL;
		goto out;
	}   

	entry = (struct reclaim_log_entry *)iov.iov_base;
	for (i = 0; i < count; i++) {
		oldfg.fg_fid = entry->fid;
		oldfg.fg_gen = entry->gen;
		fg_makepath(&oldfg, fidfn);

                /* 
		 * We do upfront garbage collection, so ENOENT should be fine.  
 		 * Also simply creating a file  without any I/O won't create 
 		 * a backing file on the I/O server.
 		 *
 		 * Anyway, we don't report an error back to MDS because it can
 		 * do nothing.
 		 */
		rc = unlink(fidfn);

		psclog_debug("fid="SLPRI_FG", xid=%"PRId64 "rc=%d",
		    SLPRI_FG_ARGS(&oldfg), entry->xid, rc);
		entry++;
	}
out:

	PSCFREE(iov.iov_base);
	return (mp->rc);
}

int
sli_rim_handle_repl_schedwk(struct pscrpc_request *rq)
{
	struct srm_repl_schedwk_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->fg.fg_fid == FID_ANY)
		mp->rc = EINVAL;
	else if (mq->len < 1 || mq->len > SLASH_BMAP_SIZE)
		mp->rc = EINVAL;
	else
		mp->rc = sli_repl_addwk(mq->nid, &mq->fg,
		    mq->bmapno, mq->bgen, mq->len);

	bim_updateseq(mp->data);

	return (0);
}

int
sli_rim_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRIM_MAGIC || mq->version != SRIM_VERSION)
		mp->rc = -EINVAL;

	bim_updateseq(mp->data);

	return (0);
}

int
sli_rim_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    sli_getmcsvcx(_resm, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_REPL_SCHEDWK:
		rc = sli_rim_handle_repl_schedwk(rq);
		break;
	case SRMT_RECLAIM:
		rc = sli_rim_handle_reclaim(rq);
		break;
	case SRMT_CONNECT:
		rc = sli_rim_handle_connect(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
