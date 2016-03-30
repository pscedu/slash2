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
	uint64_t			  bq_bid;		/* batch RPC ID */
	struct psc_listcache		 *bq_res_batches;	/* resource's list of batches */
	struct psc_listentry		  bq_lentry_global;	/* global list membership */
	struct psc_listentry		  bq_lentry_res;	/* membership on bq_res_batches */
	struct timeval			  bq_expire;		/* when to transmit */
	struct psc_listcache		 *bq_workq;		/* work queue to process events */

	struct pscrpc_request		 *bq_rq;
	struct slashrpc_cservice	 *bq_csvc;
	int				  bq_snd_ptl:16;	/* bulk RPC portal */
	int				  bq_rcv_ptl:16;	/* bulk RPC portal */
	int				  bq_flags;		/* see BATCHF_* below */
	int				  bq_refcnt;
	int				  bq_error;		/* return/processing error code */
	uint32_t			  bq_opc;		/* underlying RPC operation code */

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
	struct slashrpc_cservice	 *bp_csvc;
	int				  bp_refcnt;
	int				  bp_flags;
	int				  bp_error;
	uint32_t			  bp_opc;		/* underlying RPC operation code */

	void				 *bp_reqbuf;		/* incoming request bulk RPC */
	void				 *bp_repbuf;		/* outgoing reply bulk RPC */
	int				  bp_reqlen;
	int				  bp_replen;
};

#define BATCHF_RQINFL			(1 << 0)	/* request RPC inflight */
#define BATCHF_WAITREPLY		(1 << 1)	/* awaiting RPC reply */
#define BATCHF_SCHED_FINISH		(1 << 2)	/* scheduled for cleanup */
#define BATCHF_FREEING			(1 << 3)	/* trying to destroy */

#define BATCHF_REPLIED			(1 << 4)	/* reply sent */

#define SLRPC_BATCH_REQ_LOCK(bq)	spinlock(&(bq)->bq_lock)
#define SLRPC_BATCH_REQ_ULOCK(bq)	freelock(&(bq)->bq_lock)
#define SLRPC_BATCH_REQ_RLOCK(bq)	reqlock(&(bq)->bq_lock)
#define SLRPC_BATCH_REQ_URLOCK(bq, lk)	ureqlock(&(bq)->bq_lock, (lk))

#define SLRPC_BATCH_REP_LOCK(bp)	spinlock(&(bp)->bp_lock)
#define SLRPC_BATCH_REP_ULOCK(bp)	freelock(&(bp)->bp_lock)
#define SLRPC_BATCH_REP_RLOCK(bp)	reqlock(&(bp)->bp_lock)
#define SLRPC_BATCH_REP_URLOCK(bp, lk)	ureqlock(&(bp)->bp_lock, (lk))

#define PFLOG_BATCH_REQ(level, bq, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batchrpcrq@%p bid=%"PRIu64" refs=%d flags=%#x opc=%d "	\
	    "reqbuf=%p qlen=%d repbuf=%p plen=%d rc=%d "fmt,		\
	    (bq), (bq)->bq_bid, (bq)->bq_refcnt, (bq)->bq_flags,	\
	    (bq)->bq_opc, (bq)->bq_reqbuf, (bq)->bq_reqlen,		\
	    (bq)->bq_repbuf, (bq)->bq_replen, (bq)->bq_error, ##__VA_ARGS__)

#define PFLOG_BATCH_REP(level, bp, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batchrpcrp@%p bid=%"PRIu64" refs=%d flags=%#x opc=%d "	\
	    "reqbuf=%p qlen=%d repbuf=%p plen=%d rc=%d "fmt,		\
	    (bp), (bp)->bp_bid, (bp)->bp_refcnt, (bp)->bp_flags,	\
	    (bp)->bp_opc, (bp)->bp_reqbuf, (bp)->bp_reqlen,		\
	    (bp)->bp_repbuf, (bp)->bp_replen, (bp)->bp_error, ##__VA_ARGS__)

int	slrpc_batch_req_add(struct psc_listcache *,
	    struct psc_listcache *, struct slashrpc_cservice *,
	    uint32_t, int, int, void *, size_t, void *,
	    struct slrpc_batch_rep_handler *, int);

void	slrpc_batches_init(int, const char *);
void	slrpc_batches_destroy(void);
void	slrpc_batches_drop(struct psc_listcache *);

#define slrpc_batch_rep_decref(rep, error)				\
	_slrpc_batch_rep_decref(PFL_CALLERINFO(), (rep), (error))

void	slrpc_batch_rep_incref(struct slrpc_batch_rep *);
void	_slrpc_batch_rep_decref(const struct pfl_callerinfo *,
	    struct slrpc_batch_rep *, int);

int	slrpc_batch_handle_reply(struct pscrpc_request *);
int	slrpc_batch_handle_request(struct slashrpc_cservice *,
	    struct pscrpc_request *, struct slrpc_batch_req_handler *);

#endif /* _BATCHRPC_H_ */
