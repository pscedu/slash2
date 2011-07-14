/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011, Pittsburgh Supercomputing Center (PSC).
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

#include <errno.h>

#include "psc_rpc/rpc.h"

#include "rpc_cli.h"
#include "slashrpc.h"

/*
 * Routines for handling RPC requests for CLI from ION.
 */

/**
 * slc_rci_handle_read - Handle a READ completion for CLI from ION.
 * @rq: request.
 */
int
slc_rci_handle_read(struct pscrpc_request *rq)
{
	struct srm_io_req *mq;
	struct srm_io_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	return (0);
}

/**
 * slc_rci_handle_connect - Handle a CONNECT request for CLI from ION.
 * @rq: request.
 */
int
slc_rci_handle_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_connect_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCI_MAGIC || mq->version != SRCI_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

/**
 * slc_rci_handler - Handle a request for CLI from ION.
 * @rq: request.
 */
int
slc_rci_handler(struct pscrpc_request *rq)
{
	int rc;

	rq->rq_status = SL_EXP_REGISTER_RESM(rq->rq_export,
	    slc_geticsvcxf(_resm, 0, rq->rq_export));
	if (rq->rq_status)
		return (pscrpc_error(rq));

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slc_rci_handle_connect(rq);
		break;
	case SRMT_READ:
		rc = slc_rci_handle_read(rq);
		break;
	default:
		psclog_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
