/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2016, Google, Inc.
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * The batch RPC API provides a structure and set of routines for
 * packing multiple distinct but related messages into a single RPC for
 * throughput efficiency.
 */

#ifndef _BATCHRPC_H_
#define _BATCHRPC_H_

#include <sys/time.h>
#include <sys/types.h>

#include <stdint.h>

#include "pfl/dynarray.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"

#include "slconfig.h"
#include "slconn.h"

/*
 * A single failure will doom the entire batch. So a larger number
 * may not be always good.
 */
#define	SLRPC_BATCH_MIN_COUNT		4
#define	SLRPC_BATCH_MAX_COUNT		4096

struct psc_listcache;

struct slrpc_batch_rep;

struct slrpc_batch_req_handler {
	int	(*bqh_cbf)(struct slrpc_batch_rep *, void *, void *);
	int	  bqh_qlen;
	int	  bqh_plen;
	int	  bqh_snd_ptl:16;	/* bulk RPC portal */
	int	  bqh_rcv_ptl:16;	/* bulk RPC portal */
};

struct slrpc_batch_rep_handler {
	void	(*bph_cbf)(void *, void *, void *, int);
	int	  bph_qlen;
	int	  bph_plen;
};

struct slrpc_batch_req {
	psc_spinlock_t			  bq_lock;
	int				  bq_refcnt;
	uint64_t			  bq_bid;		/* batch RPC ID */
	struct psc_listentry		  bq_lentry;		/* list membership */
	struct sl_resource		 *bq_res;
	struct timeval			  bq_expire;		/* when to transmit */
	struct psc_listcache		 *bq_workq;		/* work queue to process events */

	struct pscrpc_request		 *bq_rq;
	struct slrpc_cservice		 *bq_csvc;
	int				  bq_snd_ptl:16;	/* bulk RPC portal */
	int				  bq_rcv_ptl:16;	/* bulk RPC portal */
	int				  bq_flags;		/* see BATCHF_* below */
	int				  bq_rc;		/* return/processing return code */
	int				  bq_finish;		/* debug */
	int32_t			  	  bq_opc;		/* underlying RPC operation code */

	int				  bq_cnt;
	int				  bq_size;		/* capacity */

	void				 *bq_reqbuf;		/* outgoing request bulk RPC */
	void				 *bq_repbuf;		/* incoming reply bulk RPC */
	int				  bq_reqlen;
	int				  bq_replen;

	struct psc_dynarray		  bq_scratch;		/* per-item private data */

	struct slrpc_batch_rep_handler	 *bq_handler;		/* callback run over each item on reply */
};

struct slrpc_batch_rep {
	psc_spinlock_t			  bp_lock;
	uint64_t			  bp_bid;		/* batch RPC ID */
	struct psc_listentry		  bp_lentry;		/* global list membership */
	struct slrpc_batch_req_handler	 *bp_handler;

	struct pscrpc_request		 *bp_rq;
	struct slrpc_cservice		 *bp_csvc;
	int				  bp_refcnt;
	int				  bp_flags;
	int				  bp_rc;
	uint32_t			  bp_opc;		/* underlying RPC operation code */

	int				  bp_cnt;

	void				 *bp_reqbuf;		/* incoming request bulk RPC */
	void				 *bp_repbuf;		/* outgoing reply bulk RPC */
	int				  bp_reqlen;
	int				  bp_replen;
};

#define BATCHF_INFL			(1 << 0)	/* request RPC inflight */
#define BATCHF_DELAY			(1 << 1)	/* wait to batch more  */
#define BATCHF_REPLY			(1 << 2)	/* awaiting RPC reply */
#define BATCHF_FREEING			(1 << 3)	/* trying to destroy */

#define PFLOG_BATCH_REQ(level, bq, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batchrpcrq@%p, bid=%"PRIu64", rq=%p, flags=%#x opc=%d, "	\
	    "qlen=%d, plen=%d, rc=%d, "fmt,				\
	    (bq), (bq)->bq_bid, (bq)->bq_rq, (bq)->bq_flags,		\
	    (bq)->bq_opc, (bq)->bq_reqlen, (bq)->bq_replen, 		\
	    (bq)->bq_rc, ##__VA_ARGS__)

#define PFLOG_BATCH_REP(level, bp, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batchrpcrp@%p bid=%"PRIu64" refs=%d flags=%#x opc=%d "	\
	    "reqbuf=%p qlen=%d repbuf=%p plen=%d rc=%d "fmt,		\
	    (bp), (bp)->bp_bid, (bp)->bp_refcnt, (bp)->bp_flags,	\
	    (bp)->bp_opc, (bp)->bp_reqbuf, (bp)->bp_reqlen,		\
	    (bp)->bp_repbuf, (bp)->bp_replen, (bp)->bp_rc, ##__VA_ARGS__)

int	slrpc_batch_req_add(struct sl_resource *,
	    struct psc_listcache *, struct slrpc_cservice *,
	    int32_t, int, int, void *, int, void *,
	    struct slrpc_batch_rep_handler *, int, int);

void	slrpc_batches_init(int, int, const char *);
void	slrpc_batches_destroy(void);
void	slrpc_batches_drop(struct sl_resource *res);

void	slrpc_batch_rep_decref(struct slrpc_batch_rep *, int);

void	slrpc_batch_rep_incref(struct slrpc_batch_rep *);
void	_slrpc_batch_rep_decref(const struct pfl_callerinfo *,
	    struct slrpc_batch_rep *, int);

int	slrpc_batch_handle_reply(struct pscrpc_request *);
int	slrpc_batch_handle_request(struct slrpc_cservice *,
	    struct pscrpc_request *, struct slrpc_batch_req_handler *);

#endif /* _BATCHRPC_H_ */
