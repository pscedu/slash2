/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2014, Pittsburgh Supercomputing Center (PSC).
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
 * Routines for handling RPC requests for CLI from ION.
 */

#include <errno.h>

#include "pfl/ctlsvr.h"
#include "pfl/fs.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"

#include "pgcache.h"
#include "rpc_cli.h"
#include "slashrpc.h"

#define RCI_AIO_READ_WAIT_NS	1000000
#define CAR_LOOKUP_MAX		10000

int
slc_rci_handle_ctl(struct pscrpc_request *rq)
{
	struct srt_ctlsetopt *c;
	struct srm_ctl_req *mq;
	struct srm_ctl_rep *mp;

	SL_RSX_ALLOCREP(rq, mq, mp);
	switch (mq->opc) {
	case SRM_CTLOP_SETOPT:
		c = (void *)mq->buf;
		switch (c->opt) {
		case SRMCTL_OPT_HEALTH: {
			struct resprof_cli_info *rpci;
			struct sl_resm *m;

			m = libsl_try_nid2resm(rq->rq_peer.nid);
			rpci = res2rpci(m->resm_res);
			rpci->rpci_flags &= ~RPCIF_AVOID;
			switch (c->opv) {
			case 2: /* degraded: avoid */
				rpci->rpci_flags |= RPCIF_AVOID;
				break;
			}
			break;
		    }
		}
		break;
	default:
		psclog_errorx("unrecognized control action; opc=%d",
		    mq->opc);
		mp->rc = -PFLERR_NOSYS;
		break;
	}
	return (0);
}

/**
 * slc_rci_handle_io - Handle a READ or WRITE completion for CLI from
 *	ION.
 * @rq: request.
 */
int
slc_rci_handle_io(struct pscrpc_request *rq)
{
	int tries = 0, nwait = 0, found = 0;
	struct bmpc_ioreq *r = NULL;
	struct iovec *iovs = NULL;
	struct slc_async_req *car;
	struct psc_listcache *lc;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	struct iovec iov;

	SL_RSX_ALLOCREP(rq, mq, mp);

	m = libsl_try_nid2resm(rq->rq_export->exp_connection->c_peer.nid);
	if (m == NULL)
		PFL_GOTOERR(out, mp->rc = -SLERR_ION_UNKNOWN);

	lc = &resm2rmci(m)->rmci_async_reqs;

	while (!found && tries++ < CAR_LOOKUP_MAX) {
		LIST_CACHE_LOCK(lc);
		LIST_CACHE_FOREACH(car, lc)
			if (car->car_id == mq->id) {
				lc_remove(lc, car);
				found = 1;
				break;
			}

		if (!found) {
			struct timespec ts = { 0, RCI_AIO_READ_WAIT_NS };

			nwait++;
			OPSTAT_INCR(SLC_OPST_READ_AIO_WAIT);

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
	if (nwait > OPSTAT_CURR(SLC_OPST_READ_AIO_WAIT_MAX))
		OPSTAT_ASSIGN(SLC_OPST_READ_AIO_WAIT_MAX, nwait);

	if (!found) {
		psclog_warnx("could not find async req id=%#"PRIx64,
		    mp->id);
		OPSTAT_INCR(SLC_OPST_READ_AIO_NOT_FOUND);
		PFL_GOTOERR(out, mp->rc = -ENOENT);
	}

	r = car->car_argv.pointer_arg[MSL_CBARG_BIORQ];
	if (car->car_cbf == msl_read_cb) {
		struct bmap_pagecache_entry *e;
		struct psc_dynarray *a;
		int i;

		OPSTAT_INCR(SLC_OPST_READ_CB);
		a = car->car_argv.pointer_arg[MSL_CBARG_BMPCE];

		/*
		 * MAX_BMAPS_REQ * SLASH_BMAP_SIZE / BMPC_BUFSZ is just
		 * too large.
		 */
		iovs = PSCALLOC(sizeof(struct iovec) * psc_dynarray_len(a));
		DYNARRAY_FOREACH(e, i, a) {
			if (!mq->rc) {
				iovs[i].iov_base = e->bmpce_base;
				iovs[i].iov_len = BMPC_BUFSZ;
			}
		}

		if (!mq->rc)
			mq->rc = slrpc_bulkserver(rq, BULK_GET_SINK,
			    SRCI_BULK_PORTAL, iovs,
			    psc_dynarray_len(a));

		PSCFREE(iovs);

	} else if (car->car_cbf == msl_dio_cb) {

		if (mq->op == SRMIOP_RD)
			OPSTAT_INCR(SLC_OPST_DIO_CB_READ);
		else
			OPSTAT_INCR(SLC_OPST_DIO_CB_WRITE);

		/* FixMe: Should wake up waiters regardless of results */
		if (mq->rc)
			PFL_GOTOERR(out, mq->rc);

		if (mq->op == SRMIOP_RD) { 
			iov.iov_base = r->biorq_buf;
			iov.iov_len = r->biorq_len;

			mq->rc = slrpc_bulkserver(rq, BULK_GET_SINK,
			    SRCI_BULK_PORTAL, &iov, 1);
		} 
	} else {
		psc_fatalx("unknown callback");
	}

	/*
	 * The callback needs to be run even if the RPC failed so
	 * cleanup can happen.
	 */
	car->car_cbf(rq, mq->rc, &car->car_argv);

	psclog_diag("return car=%p car_id=%"PRIx64" q=%p, r=%p", car,
	    car->car_id, car->car_fsrqinfo, r);

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
	case SRMT_CTL:
		rc = slc_rci_handle_ctl(rq);
		break;
	default:
		psclog_errorx("unexpected opcode %d",
		    rq->rq_reqmsg->opc);
		rq->rq_status = -PFLERR_NOSYS;
		return (pscrpc_error(rq));
	}
	authbuf_sign(rq, PSCRPC_MSG_REPLY);
	pscrpc_target_send_reply_msg(rq, rc, 0);
	return (rc);
}
