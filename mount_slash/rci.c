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

#include "pfl/fs.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"

#include "bmpc.h"
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
	struct pscfs_req *pfr = NULL;
	struct slc_async_req *car;
	struct psc_listcache *lc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	struct iovec iov;
	size_t len = 0;
	ssize_t rc;

	SL_RSX_ALLOCREP(rq, mq, mp);
	m = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (m == NULL) {
		mp->rc = SLERR_ION_UNKNOWN;
		goto error;
	}

	lc = &resm2rmci(m)->rmci_async_reqs;

	LIST_CACHE_LOCK(lc);
	LIST_CACHE_FOREACH(car, lc)
		if (mq->id == car->car_id)
			break;
	LIST_CACHE_ULOCK(lc);

	if (car == NULL) {
		mp->rc = EINVAL;
		goto error;
	}

	pfr = car->car_pfr;

	if (mq->rc)
		;
	else if (car->car_cbf == msl_readahead_cb) {

		struct bmap_pagecache_entry *bmpce, **bv;
		struct iovec iovs[MAX_BMAPS_REQ];
		int i;

		bv = car->car_argv.pointer_arg[MSL_CBARG_BMPCE];
		for (i = 0; *bv; i++, bv++) {
			bmpce = *bv;
			iovs[i].iov_base = bmpce->bmpce_base;
			iovs[i].iov_len = BMPC_BUFSZ;
		}

		mq->rc = rsx_bulkclient(rq, BULK_PUT_SINK,
		    SRIC_BULK_PORTAL, iovs, i);

	} else if (car->car_cbf == msl_read_cb) {

		struct bmap_pagecache_entry *bmpce;
		struct iovec iovs[MAX_BMAPS_REQ];
		struct psc_dynarray *a;
		struct bmpc_ioreq *r;
		int i;

		a = car->car_argv.pointer_arg[MSL_CBARG_BMPCE];
		r = car->car_argv.pointer_arg[MSL_CBARG_BIORQ];
		DYNARRAY_FOREACH(bmpce, i, a) {
			iovs[i].iov_base = bmpce->bmpce_base;
			iovs[i].iov_len = BMPC_BUFSZ;
		}

		mq->rc = rsx_bulkclient(rq, BULK_PUT_SINK,
		    SRIC_BULK_PORTAL, iovs, psc_dynarray_len(a));

//		rc = msl_pages_copyout(r, p);

	} else if (car->car_cbf == msl_dio_cb) {

		iov.iov_base = car->car_argv.pointer_arg[MSL_CBARG_BUF];
		iov.iov_len = mq->size;

		mq->rc = rsx_bulkclient(rq, BULK_PUT_SINK,
		    SRCI_BULK_PORTAL, &iov, 1);
	} else
		psc_fatalx("unknown callback");
	rc = car->car_cbf(rq, mq->rc, &car->car_argv);
	msl_aiorqcol_finish(car, rc, len);
	psc_pool_return(slc_async_req_pool, car);

 error:
	if (mp->rc)
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);
	return (mp->rc);
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
