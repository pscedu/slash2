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

static uint64_t		next_reclaim_seqno;
static psc_spinlock_t	next_reclaim_seqno_lock = SPINLOCK_INIT;

/**
 * sli_rim_handle_reclaim - handle RECLAIM RPC from the MDS as a result
 *	of unlink or truncate to zero.
 */
int
sli_rim_handle_reclaim(struct pscrpc_request *rq)
{
	char fidfn[PATH_MAX];
	struct slash_fidgen oldfg;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	spinlock(&next_reclaim_seqno_lock);
	if (mq->seqno == next_reclaim_seqno) {
		oldfg.fg_fid = mq->fg.fg_fid;
		oldfg.fg_gen = mq->fg.fg_gen;
		fg_makepath(&oldfg, fidfn);

		/*
		 * We do upfront garbage collection,
		 * so ENOENT should be fine.
		 */
		if (unlink(fidfn) == -1 &&
		    errno != ENOENT)
			mp->rc = errno;

		if (mp->rc == 0)
			next_reclaim_seqno++;
	} else
		mp->rc = EINVAL;

	psclog_debug("fid="SLPRI_FG", seqno=%"PRId64", "
	    "next_reclaim_seqno=%"PRId64", rc=%d",
	    SLPRI_FG_ARGS(&mq->fg), mq->seqno, next_reclaim_seqno,
	    mp->rc);

	mp->seqno = next_reclaim_seqno;
	freelock(&next_reclaim_seqno_lock);
	return (0);
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
