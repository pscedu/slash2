/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2012, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/ctlsvr.h"

#include "bmpc.h"
#include "rpc_cli.h"
#include "slashrpc.h"

/*
 * Routines for handling RPC requests for CLI from ION.
 */

#define RCI_AIO_READ_WAIT	1000000
#define CAR_LOOKUP_MAX		1000

/**
 * slc_rci_handle_io - Handle a READ or WRITE completion for CLI from
 *	ION.
 * @rq: request.
 */
int
slc_rci_handle_io(struct pscrpc_request *rq)
{
	struct slc_async_req *car;
	struct psc_listcache *lc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	struct iovec iov;
	int tries = 0, found = 0;

	SL_RSX_ALLOCREP(rq, mq, mp);

	m = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (m == NULL) {
		mp->rc = SLERR_ION_UNKNOWN;
		goto out;
	}

	lc = &resm2rmci(m)->rmci_async_reqs;

	while (!found && (tries++ < CAR_LOOKUP_MAX)) {
		LIST_CACHE_LOCK(lc);
		LIST_CACHE_FOREACH(car, lc)
			if (car->car_id == mq->id) {
				lc_remove(lc, car);
				found = 1;
				break;
			}

		if (!found) {
			OPSTAT_INCR(OPSTAT_READ_AIO_NOT_FOUND);
			struct timespec ts = { 0, RCI_AIO_READ_WAIT };

			/*
			 * The AIO RPC from the sliod beat our fs
			 * thread.  Give our thread a chance to put the
			 * 'car' onto the list.  No RPC is involved,
			 * this wait should only require a few ms at the
			 * most.
			 */
			psc_waitq_waitrel(&lc->plc_wq_empty,
			    &lc->plc_lock, &ts);
		} else {
			LIST_CACHE_ULOCK(lc);
		}
	}

	if (found) {
		psc_assert(car->car_id == mq->id);

	} else {
		mp->rc = -ENOENT;
		goto out;
	}

	psclog_info("car=%p car_id=%"PRIx64" q=%p", car, car->car_id,
	    car->car_fsrqinfo);

	if (car->car_cbf == msl_read_cb) {
		struct bmap_pagecache_entry *e;
		struct iovec iovs[MAX_BMAPS_REQ];
		struct psc_dynarray *a;
		int i;

		OPSTAT_INCR(OPSTAT_READ_CB);
		a = car->car_argv.pointer_arg[MSL_CBARG_BMPCE];

		msl_fsrqinfo_readywait(car->car_fsrqinfo);

		DYNARRAY_FOREACH (e, i, a) {
			if (!mq->rc) {
				iovs[i].iov_base = e->bmpce_base;
				iovs[i].iov_len = BMPC_BUFSZ;

			} else {
				e->bmpce_flags |= BMPCE_EIO;
			}
		}

		if (!mq->rc)
			mq->rc = rsx_bulkserver(rq, BULK_GET_SINK,
			    SRCI_BULK_PORTAL, iovs, psc_dynarray_len(a));

	} else if (car->car_cbf == msl_readahead_cb) {
		struct bmap_pagecache_entry *e, **bmpces =
		    car->car_argv.pointer_arg[MSL_CBARG_BMPCE];
		struct iovec *iovs = NULL;
		int i;

		OPSTAT_INCR(OPSTAT_READ_AHEAD_CB);
		for (i = 0;; i++) {
			e = bmpces[i];
			if (!e)
				break;

			iovs = PSC_REALLOC(iovs,
			    sizeof(struct iovec) * (i + 1));

			iovs[i].iov_base = e->bmpce_base;
			iovs[i].iov_len = BMPC_BUFSZ;
			if (mq->rc)
				e->bmpce_flags |= BMPCE_EIO;
		}
		if (mq->rc == 0)
			mq->rc = rsx_bulkserver(rq, BULK_GET_SINK,
			    SRCI_BULK_PORTAL, iovs, i);

		PSCFREE(iovs);

	} else if (car->car_cbf == msl_dio_cb) {
		OPSTAT_INCR(OPSTAT_DIO_CB);
		msl_fsrqinfo_readywait(car->car_fsrqinfo);

		if (mq->rc)
			goto out;

		if (mq->op == SRMIOP_RD) {
			struct bmpc_ioreq *r =
			    car->car_argv.pointer_arg[MSL_CBARG_BIORQ];

			iov.iov_base = r->biorq_buf;
			iov.iov_len = car->car_len;

			mq->rc = rsx_bulkserver(rq, BULK_GET_SINK,
			    SRCI_BULK_PORTAL, &iov, 1);
		} else {
			MFH_LOCK(car->car_fsrqinfo->mfsrq_fh);
			msl_fsrqinfo_state(car->car_fsrqinfo,
			    MFSRQ_AIOWAIT, -1, 0);
			msl_fsrqinfo_aioreadyset(car->car_fsrqinfo);
			MFH_ULOCK(car->car_fsrqinfo->mfsrq_fh);

			car->car_cbf = NULL;
		}

	} else {
		psc_fatalx("unknown callback");
	}

	/*
	 * The callback needs to be run even if the RPC failed so
	 * cleanup can happen.
	 */
	if (car->car_cbf)
		car->car_cbf(rq, mq->rc, &car->car_argv);

	psclog_info("return car=%p car_id=%"PRIx64" q=%p", car,
	    car->car_id, car->car_fsrqinfo);

	psc_pool_return(slc_async_req_pool, car);

 out:
	if (mp->rc)
		pscrpc_msg_add_flags(rq->rq_repmsg, MSG_ABORT_BULK);

	return (mp->rc);
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
		rc = slrpc_handle_connect(rq, SRCI_MAGIC, SRCI_VERSION,
		    SLCONNT_IOD);
		break;
	case SRMT_READ:
	case SRMT_WRITE:
		rc = slc_rci_handle_io(rq);
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
