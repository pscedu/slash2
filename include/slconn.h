/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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
 * This interface provides connections to hosts (servers and clients) in
 * a SLASH2 deployment.
 */

#ifndef _SLCONN_H_
#define _SLCONN_H_

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>

#include "pfl/atomic.h"
#include "pfl/err.h"
#include "pfl/export.h"
#include "pfl/fault.h"
#include "pfl/lock.h"
#include "pfl/multiwait.h"
#include "pfl/opstats.h"
#include "pfl/str.h"

#include "slerr.h"

struct pscrpc_import;
struct pscrpc_export;

struct sl_resm;

extern struct pfl_rwlock	 sl_conn_lock;
extern int			 sl_conn_debug;

struct sl_exp_cli {
	struct slrpc_cservice	*expc_csvc;
	uint32_t		 expc_stkvers;	/* see slctlrep_getconn() */
	uint64_t		 expc_uptime;
};

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS
};

struct slconn_params {
	struct slrpc_cservice	**scp_csvcp;
	int			  scp_flags;
	struct psc_dynarray	 *scp_peernids;
	enum slconn_type	  scp_peertype;

	uint32_t		  scp_rqptl;
	uint32_t		  scp_rpptl;
	uint64_t		  scp_magic;
	uint32_t		  scp_version;

	int			(*scp_useablef)(void *);
	void			 *scp_useablearg;
};

struct slconn_thread {
	int			(*sct_pingupc)(void *);
	void			 *sct_pingupcarg;
	struct psc_dynarray	  sct_monres;
	struct pfl_multiwait	  sct_mw;
	struct pfl_multiwaitcond  sct_mwc;
};

/*
 * Structure for client to connect to a remote server.
 */
struct slrpc_cservice {
	struct slconn_params	 csvc_params;

	/*
	 * An import is for sending requests and receiving replies, and its
	 * peer, an export, is for receiving requests and sending replies.
	 */
	struct pscrpc_import	*csvc_import;
	int			 csvc_lasterrno;	/* zeroed after a success */
	/*
 	 * The reference count associated with a resource is not dropped
 	 * (except when the client module is unloaded by wokfs).
 	 */
	int			 csvc_refcnt;

	void			*csvc_hldropf;
	void			*csvc_hldroparg;

	struct timespec		 csvc_mtime;		/* last activity */
	struct psclist_head	 csvc_lentry;

	psc_spinlock_t		 csvc_lock;
	struct psc_waitq	 csvc_waitq;

#define csvc_flags	csvc_params.scp_flags
#define csvc_magic	csvc_params.scp_magic
#define csvc_version	csvc_params.scp_version
#define csvc_peertype	csvc_params.scp_peertype
#define csvc_peernids	csvc_params.scp_peernids
#define csvc_rqptl	csvc_params.scp_rqptl
#define csvc_rpptl	csvc_params.scp_rpptl
};
#define slrpc_cservice slrpc_cservice

/* csvc_flags */
#define CSVCF_CONNECTING	(1 << 0)	/* conn attempt in progress */
#define CSVCF_CONNECTED		(1 << 1)	/* conn online */
#define CSVCF_WATCH		(1 << 2)	/* on watch list, for debugging */
#define CSVCF_PING		(1 << 3)	/* send keepalives */
#define CSVCF_PINGING		(1 << 4)	/* wait ping reply */
#define CSVCF_DISCONNECTING	(1 << 5)	/* want to disconnect but in use; ASAP */
#define CSVCF_TOFREE		(1 << 6)	/* don't use */

#define CSVCF_ONLIST		(1 << 7)	/* debugging: on sl_clients list */

/* sl_csvc_get() flags, shared in numerical space */

#define CSVCF_NONBLOCK		(1 << 8)	/* don't timeout waiting for establishment */
#define CSVCF_NORECON		(1 << 9)	/* don't attempt reconnection if down */

#define CSVCF_FLAGSHIFT		(1 << 10)

#define CSVC_CONN_INTV		10		/* seconds */
#define CSVC_PING_INTV		30		/* seconds */

/* keep in sync with sl_conn_prdat() */
#define DEBUG_CSVC(lvl, csvc, fmt, ...)					\
	psclog((lvl), "csvc@%p fl=%#x:%s%s%s%s ref:%d " fmt,		\
	    (csvc), (csvc)->csvc_flags,					\
	    (csvc)->csvc_flags & CSVCF_CONNECTING	? "C" : "",	\
	    (csvc)->csvc_flags & CSVCF_CONNECTED	? "O" : "",	\
	    (csvc)->csvc_flags & CSVCF_WATCH		? "W" : "",	\
	    (csvc)->csvc_flags & CSVCF_PING		? "P" : "",	\
	    (csvc)->csvc_refcnt, ##__VA_ARGS__)

struct sl_expcli_ops {
	struct slrpc_cservice * (*secop_allocpri)(struct pscrpc_export *);
};

#define CSVC_CALLERINFO			PFL_CALLERINFO()

#define sl_csvc_get(csvcp, flg, exp, nids,				\
	    pq, pp, mag, vers, ctype, mw, timeout)			\
	_sl_csvc_get(CSVC_CALLERINFO, (csvcp), (flg), (exp), (nids),	\
	    (pq), (pp), (mag), (vers), (ctype), (mw), (timeout))

#define sl_csvc_decref(csvc)		_sl_csvc_decref(CSVC_CALLERINFO, (csvc), 0)
#define sl_csvc_decref_locked(csvc)	_sl_csvc_decref(CSVC_CALLERINFO, (csvc), 1)

#define sl_csvc_disconnect(csvc)	_sl_csvc_disconnect(CSVC_CALLERINFO, (csvc), 0)
#define sl_csvc_disconnect_locked(csvc)	_sl_csvc_disconnect(CSVC_CALLERINFO, (csvc), 1)

#define slrpc_getname(rq)						\
	slrpc_getname_for_opcode(pflrpc_req_get_opcode(rq))

#define SL_EXP_REGISTER_RESM(exp, getcsvc)				\
	({								\
		struct slrpc_cservice *_csvc = NULL;			\
		struct sl_resm *_resm;					\
		int _rc = 0;						\
									\
		_resm = libsl_try_nid2resm(				\
		    (exp)->exp_connection->c_peer.nid);			\
		if (_resm) {						\
			EXPORT_LOCK(exp);				\
			if ((exp)->exp_hldropf == NULL) {		\
				_csvc = getcsvc;			\
				psc_assert(_csvc);			\
				exp->exp_hldropf = sl_exp_hldrop_resm;	\
			}						\
			EXPORT_ULOCK(exp);				\
		} else							\
			_rc = SLERR_RES_UNKNOWN;			\
		(_rc);							\
	})

#define CSVC_LOCK(csvc)			spinlock(&(csvc)->csvc_lock)
#define CSVC_ULOCK(csvc)		freelock(&(csvc)->csvc_lock)

#define sl_csvc_waitrel_s(csvc, s)	_sl_csvc_waitrelv((csvc), (s), 0L)

#define SRPCWAITF_DEFER_BULK_AUTHBUF_CHECK (1 << 0)

#ifndef SL_FAULT_PREFIX
#  define SL_FAULT_PREFIX ""
#endif

#define SL_RSX_NEWREQ(csvc, op, rq, mq, mp)				\
	({								\
		static struct pfl_opstat *_opst;			\
		int _rc = 0;						\
									\
		pfl_fault_here_rc(&_rc, -EHOSTDOWN,			\
		    "%srpc.issue.%s", SL_FAULT_PREFIX,			\
		    slrpc_getname_for_opcode(op));			\
		_rc = _rc ? _rc : (slrpc_ops.slrpc_newreq ?		\
		    slrpc_ops.slrpc_newreq : slrpc_newgenreq)((csvc),	\
		    (op), &(rq), sizeof(*(mq)), sizeof(*(mp)), &(mq));	\
		if (_rc == 0) {						\
			if (_opst == NULL)				\
				_opst = pfl_opstat_initf(OPSTF_BASE10,	\
				    "rpc.issue.%s.sent",		\
				    slrpc_getname_for_opcode(op));	\
			pfl_opstat_incr(_opst);				\
		}							\
		_rc;							\
	})

#define _SLRPC_REP_IN(csvc, rq, flags, error, mp)			\
	({								\
		(error) = slrpc_rep_in((csvc), (rq), (flags), (error));	\
									\
		if ((error) || (mp)->rc) {				\
			static struct pfl_opstat *_opst_err;		\
									\
			if (_opst_err == NULL)				\
				_opst_err = pfl_opstat_initf(		\
				    OPSTF_BASE10, "rpc.issue.%s.err",	\
				    slrpc_getname(rq));			\
			pfl_opstat_incr(_opst_err);			\
		} else {						\
			static struct pfl_opstat *_opst_ok;		\
									\
			if (_opst_ok == NULL)				\
				_opst_ok = pfl_opstat_initf(		\
				    OPSTF_BASE10, "rpc.issue.%s.ok",	\
				    slrpc_getname(rq));			\
			pfl_opstat_incr(_opst_ok);			\
		}							\
									\
		(error);						\
	})

#define SL_RSX_WAITREPF(csvc, rq, mp, flags)				\
	({								\
		int _error;						\
									\
		_error = slrpc_waitrep((csvc), (rq), sizeof(*(mp)),	\
		    &(mp));						\
		_SLRPC_REP_IN((csvc), (rq), (flags), _error, (mp));	\
	})

#define SL_RSX_WAITREP(csvc, rq, mp)					\
	SL_RSX_WAITREPF((csvc), (rq), (mp), 0)

#define SL_RSX_ALLOCREP(rq, mq, mp)					\
	do {								\
		static struct pfl_opstat *_opst;			\
		int _rc;						\
									\
		_rc = (slrpc_ops.slrpc_allocrep ?			\
		    slrpc_ops.slrpc_allocrep : slrpc_allocgenrep)((rq),	\
		    &(mq), sizeof(*(mq)), &(mp), sizeof(*(mp)),		\
		    offsetof(typeof(*(mp)), rc));			\
		if (_rc)						\
			return (_rc);					\
		if ((mp)->rc)						\
			return ((mp)->rc);				\
									\
		if (_opst == NULL)					\
			_opst = pfl_opstat_initf(OPSTF_BASE10,		\
			    "rpc.handle.%s", strstr(__func__,		\
			    "_handle_") + 8);				\
		pfl_opstat_incr(_opst);					\
	} while (0)

#define SL_NBRQSETX_ADD(set, csvc, rq)					\
	({								\
		int _rc;						\
									\
		if (slrpc_ops.slrpc_req_out)				\
			slrpc_ops.slrpc_req_out((csvc), (rq));		\
		authbuf_sign((rq), PSCRPC_MSG_REQUEST);			\
		_rc = pscrpc_nbreqset_add((set), (rq));			\
		if (_rc && slrpc_ops.slrpc_req_out_failed)		\
			slrpc_ops.slrpc_req_out_failed((csvc), (rq));	\
		_rc;							\
	})

#define SL_NBRQSET_ADD(csvc, rq)					\
	SL_NBRQSETX_ADD(sl_nbrqset, (csvc), (rq))

#define SL_GET_RQ_STATUSF(csvc, rq, mp, flags, error)			\
	do {								\
		(mp) = NULL;						\
		(error) = 0;						\
		if ((rq)->rq_repmsg)					\
			(error) = (rq)->rq_repmsg->status;		\
		if ((error) == 0)					\
			(error) = (rq)->rq_status;			\
		if ((error) == 0 && (rq)->rq_err)			\
			(error) = SLERR_RPCIO;				\
		if ((error) == 0) {					\
			(mp) = pscrpc_msg_buf((rq)->rq_repmsg, 0,	\
			    sizeof(*(mp)));				\
			(error) = (mp) ? (mp)->rc : -PFLERR_BADMSG;	\
		}							\
		_SLRPC_REP_IN((csvc), (rq), (flags), (error), (mp));	\
	} while (0)

#define SL_GET_RQ_STATUS(csvc, rq, mp, error)				\
	SL_GET_RQ_STATUSF((csvc), (rq), (mp), 0, (error))

#define SL_GET_RQ_STATUS_TYPE(csvc, rq, type, rc)			\
	do {								\
		type *_mp;						\
									\
		SL_GET_RQ_STATUS((csvc), (rq), _mp, (rc));		\
	} while (0)

struct slrpc_ops {
	int	 (*slrpc_newreq)(struct slrpc_cservice *, int, struct pscrpc_request **, int, int, void *);
	void	 (*slrpc_req_in)(struct pscrpc_request *);
	void	 (*slrpc_req_out)(struct slrpc_cservice *, struct pscrpc_request *);
	void	 (*slrpc_req_out_failed)(struct slrpc_cservice *, struct pscrpc_request *);
	int	 (*slrpc_allocrep)(struct pscrpc_request *, void *, int, void *, int, int);
	void	 (*slrpc_rep_in)(struct slrpc_cservice *, struct pscrpc_request *, int);
	void	 (*slrpc_rep_out)(struct pscrpc_request *);
};

struct slrpc_cservice *
	_sl_csvc_get(const struct pfl_callerinfo *,
	    struct slrpc_cservice **, int, struct pscrpc_export *,
	    struct psc_dynarray *, uint32_t, uint32_t, uint64_t,
	    uint32_t, enum slconn_type, struct pfl_multiwait *, int);
void	_sl_csvc_decref(const struct pfl_callerinfo *, struct slrpc_cservice *, int);
void	_sl_csvc_disconnect(const struct pfl_callerinfo *, struct slrpc_cservice *, int);
void	 sl_csvc_incref(struct slrpc_cservice *);
int	 sl_csvc_useable(struct slrpc_cservice *);
void	_sl_csvc_waitrelv(struct slrpc_cservice *, long, long);

void	sl_csvc_online(struct slrpc_cservice *);

void	 sl_exp_hldrop_resm(struct pscrpc_export *);
void	*sl_exp_getpri_cli(struct pscrpc_export *, int);

void	 sl_resm_hldrop(struct sl_resm *);

struct psc_thread
	*slconnthr_spawn(int, const char *, int (*)(void *), void *);
void	 slconnthr_watch(struct psc_thread *, struct slrpc_cservice *,
	    int, int (*)(void *), void *);

void	 slrpc_initcli(void);
void	 slrpc_destroy(void);

int	 slrpc_handle_connect(struct pscrpc_request *, uint64_t,
	    uint32_t, enum slconn_type);

int	 slrpc_newgenreq(struct slrpc_cservice *, int,
	    struct pscrpc_request **, int, int, void *);

int	 slrpc_waitrep(struct slrpc_cservice *,
	    struct pscrpc_request *, int, void *);

int	 slrpc_allocrepn(struct pscrpc_request *, void *, int, void *,
	    int, const int *, int);
int	 slrpc_allocgenrep(struct pscrpc_request *, void *, int, void *,
	    int, int);

void	 slrpc_rep_out(struct pscrpc_request *);
int	 slrpc_rep_in(struct slrpc_cservice *,
	    struct pscrpc_request *, int, int);

const char *
	 slrpc_getname_for_opcode(int);

int	 slrpc_bulkclient(struct pscrpc_request *, int, int, struct iovec *, int);
int	 slrpc_bulkserver(struct pscrpc_request *, int, int, struct iovec *, int);

void	 slrpc_bulk_sign(struct pscrpc_request *, void *, struct iovec *, int);
int	 slrpc_bulk_check(struct pscrpc_request *, const void *, struct iovec *, int);
int	 slrpc_bulk_checkmsg(struct pscrpc_request *, struct pscrpc_msg *, struct iovec *, int);

extern struct psc_dynarray	 sl_lnet_prids;
extern struct psc_lockedlist	 sl_clients;
extern struct sl_expcli_ops	 sl_expcli_ops;
extern struct pscrpc_request_set*sl_nbrqset;
extern struct slrpc_ops		 slrpc_ops;

#endif /* _SLCONN_H_ */
