/* $Id$ */
/* %GPL_LICENSE% */

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
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"

#include "slconfig.h"
#include "slconn.h"

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

	struct pscrpc_request		 *bq_rq;
	struct slashrpc_cservice	 *bq_csvc;
	int				  bq_snd_ptl:16;	/* bulk RPC portal */
	int				  bq_rcv_ptl:16;	/* bulk RPC portal */
	int				  bq_flags;		/* see BATCHF_* below */
	int				  bq_refcnt;
	int				  bq_error;		/* return/processing error code */

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
	    "batchrpcrq@%p bid=%"PRIu64" refs=%d reqbuf=%p "		\
	    "flags=%#x qlen=%zd rc=%d "fmt,				\
	    (bq), (bq)->bq_bid, (bq)->bq_refcnt, (bq)->bq_reqbuf,	\
	    (bq)->bq_flags, (bq)->bq_reqlen, (bq)->bq_error, ##__VA_ARGS__)

#define PFLOG_BATCH_REP(level, bp, fmt, ...)				\
	psclogs((level), PSS_RPC,					\
	    "batchrpcrp@%p bid=%"PRIu64" refs=%d reqbuf=%p "		\
	    "flags=%#x qlen=%zd rc=%d "fmt,				\
	    (bp), (bp)->bp_bid, (bp)->bp_refcnt, (bp)->bp_reqbuf,	\
	    (bp)->bp_flags, (bp)->bp_reqlen, (bp)->bp_error, ##__VA_ARGS__)

int	slrpc_batch_req_add(struct psc_listcache *,
	    struct slashrpc_cservice *, uint32_t, int, int, void *,
	    size_t, void *, struct slrpc_batch_rep_handler *, int);

void	slrpc_batch_rep_incref(struct slrpc_batch_rep *);
void	slrpc_batch_rep_decref(struct slrpc_batch_rep *, int);

int	slrpc_batch_handle_reply(struct pscrpc_request *);
int	slrpc_batch_handle_request(struct slashrpc_cservice *,
	    struct pscrpc_request *, struct slrpc_batch_req_handler *);

#endif /* _BATCHRPC_H_ */
