/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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
 * This interface provides connections to hosts (servers and clients) in
 * a SLASH network.
 */

#ifndef _SLCONN_H_
#define _SLCONN_H_

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>

#include "pfl/err.h"
#include "pfl/export.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/multiwait.h"

#include "slerr.h"

struct pscrpc_import;
struct pscrpc_export;

struct sl_resm;

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS
};

struct slconn_params {
	struct slashrpc_cservice**scp_csvcp;
	psc_atomic32_t		  scp_flags;
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
	struct psc_multiwait	  sct_mw;
	struct psc_multiwaitcond  sct_mwc;
};

/**
 * @slashrpc_cservice - Structure for client to connect to a remote
 *	server.
 */
struct slashrpc_cservice {
	struct slconn_params	 csvc_params;
	struct pscrpc_import	*csvc_import;
	int			 csvc_lasterrno;
	psc_atomic32_t		 csvc_refcnt;
	struct timespec		 csvc_mtime;		/* last activity */
	struct psclist_head	 csvc_lentry;
	struct pfl_mutex	 csvc_mutex;
	struct psc_multiwaitcond csvc_mwc;
#define	csvc_flags	csvc_params.scp_flags
#define	csvc_magic	csvc_params.scp_magic
#define	csvc_version	csvc_params.scp_version
#define csvc_peertype	csvc_params.scp_peertype
#define csvc_peernids	csvc_params.scp_peernids
#define csvc_rqptl	csvc_params.scp_rqptl
#define csvc_rpptl	csvc_params.scp_rpptl
};

/* csvc_flags */
#define CSVCF_CONNECTING	(1 << 0)		/* conn attempt in progress */
#define CSVCF_CONNECTED		(1 << 1)		/* conn online */
#define CSVCF_ABANDON		(1 << 2)		/* conn should be dropped */
#define CSVCF_WANTFREE		(1 << 3)		/* csvc mem resources need freed */
#define CSVCF_PING		(1 << 4)		/* send keepalives */

/* sl_csvc_get() flags, shared in numerical space */
#define CSVCF_NONBLOCK		(1 << 5)		/* don't timeout waiting for new establishment */
#define CSVCF_NORECON		(1 << 6)		/* do not attempt reconnection if down */

#define CSVC_RECONNECT_INTV	10			/* seconds */
#define CSVC_PING_INTV		10			/* seconds */

#define DEBUG_CSVC(lvl, csvc, fmt, ...)					\
	do {								\
		int _flags;						\
									\
		_flags = psc_atomic32_read(&(csvc)->csvc_flags);	\
		psclog((lvl), "csvc@%p fl=%#x:%s%s%s%s%s ref:%d " fmt,	\
		    (csvc), _flags,					\
		    _flags & CSVCF_CONNECTING	 ? "C" : "",		\
		    _flags & CSVCF_CONNECTED	 ? "O" : "",		\
		    _flags & CSVCF_ABANDON	 ? "A" : "",		\
		    _flags & CSVCF_WANTFREE	 ? "F" : "",		\
		    _flags & CSVCF_PING		 ? "P" : "",		\
		    psc_atomic32_read(&(csvc)->csvc_refcnt),		\
		    ##__VA_ARGS__);					\
	} while (0)

struct sl_expcli_ops {
	void	(*secop_allocpri)(struct pscrpc_export *);
	void	(*secop_destroy)(void *);
};

#define CSVC_CALLERINFO			PFL_CALLERINFO()

#define sl_csvc_get(csvcp, flg, exp, nids,				\
	    pq, pp, mag, vers, ctype, mw)				\
	_sl_csvc_get(CSVC_CALLERINFO, (csvcp), (flg), (exp), (nids),	\
	    (pq), (pp), (mag), (vers), (ctype), (mw))

#define sl_csvc_decref(csvc)		_sl_csvc_decref(CSVC_CALLERINFO, (csvc))
#define sl_csvc_disconnect(csvc)	_sl_csvc_disconnect(CSVC_CALLERINFO, (csvc))

#define SL_EXP_REGISTER_RESM(exp, getcsvc)				\
	_PFL_RVSTART {							\
		struct slashrpc_cservice *_csvc = NULL;			\
		struct sl_resm *_resm;					\
		int _rc = 0;						\
									\
		_resm = libsl_try_nid2resm(				\
		    (exp)->exp_connection->c_peer.nid);			\
		if (_resm) {						\
			_csvc = _resm->resm_csvc;			\
			if (_csvc) {					\
				CSVC_LOCK(_csvc);			\
				if (sl_csvc_useable(_csvc))		\
					CSVC_ULOCK(_csvc);		\
				else					\
					_csvc = NULL;			\
			}						\
			if (_csvc == NULL) {				\
				_csvc = getcsvc;			\
				sl_csvc_decref(_csvc);			\
			}						\
									\
			if ((exp)->exp_hldropf == NULL) {		\
				EXPORT_LOCK(exp);			\
				exp->exp_hldropf = sl_exp_hldrop_resm;	\
				EXPORT_ULOCK(exp);			\
			}						\
		} else							\
			_rc = SLERR_RES_UNKNOWN;			\
		(_rc);							\
	} _PFL_RVEND

#define CSVC_LOCK(csvc)			_psc_mutex_lock(CSVC_CALLERINFO, &(csvc)->csvc_mutex)
#define CSVC_ULOCK(csvc)		_psc_mutex_unlock(CSVC_CALLERINFO, &(csvc)->csvc_mutex)
#define CSVC_RLOCK(csvc)		_psc_mutex_reqlock(CSVC_CALLERINFO, &(csvc)->csvc_mutex)
#define CSVC_URLOCK(csvc, lk)		_psc_mutex_ureqlock(CSVC_CALLERINFO, &(csvc)->csvc_mutex, (lk))
#define CSVC_LOCK_ENSURE(csvc)		psc_mutex_ensure_locked(&(csvc)->csvc_mutex)

#define CSVC_WAKE(csvc)			psc_multiwaitcond_wakeup(&(csvc)->csvc_mwc)

#define sl_csvc_waitrel_s(csvc, s)	_sl_csvc_waitrelv((csvc), (s), 0L)

#define sl_csvc_waitevent_rel_s(csvc, cond, s)				\
	do {								\
		struct timeval _start_tm, _now_tm, _diff_tm;		\
									\
		PFL_GETTIMEVAL(&_start_tm);				\
		for (;;) {						\
			sl_csvc_reqlock(csvc);				\
			if (cond)					\
				break;					\
			PFL_GETTIMEVAL(&_now_tm);			\
			timersub(&_now_tm, &_start_tm, &_diff_tm);	\
			if (_diff_tm.tv_sec >= (s))			\
				break;					\
			sl_csvc_waitrel_s((csvc), (s));			\
		}							\
	} while (0)

#define SL_RSX_NEWREQ(csvc, op, rq, mq, mp)				\
	slrpc_newreq((csvc), (op), &(rq), sizeof(*(mq)), sizeof(*(mp)),	\
	    &(mq))

#define SL_RSX_WAITREP(csvc, rq, mp)					\
	_PFL_RVSTART {							\
		int _rc;						\
									\
		_rc = slrpc_waitrep((csvc), (rq), sizeof(*(mp)), &(mp));\
		/*							\
		 * XXX this horrible hack.  if one side thinks there's	\
		 * no connection, how can the other side not?		\
		 */							\
		if (_rc == 0 && (mp)->rc == PFLERR_NOTCONN)		\
			sl_csvc_disconnect(csvc);			\
		_rc;							\
	} _PFL_RVEND

#define SL_RSX_ALLOCREP(rq, mq, mp)					\
	do {								\
		int _rc;						\
									\
		_rc = slrpc_allocrep((rq), &(mq), sizeof(*(mq)),	\
		    &(mp), sizeof(*(mp)), offsetof(typeof(*(mp)), rc));	\
		if (_rc)						\
			return (_rc);					\
		if ((mp)->rc)						\
			return ((mp)->rc);				\
	} while (0)

#define SL_NBRQSET_ADD(csvc, rq)					\
	_PFL_RVSTART {							\
		slrpc_req_out((csvc), (rq));				\
		authbuf_sign((rq), PSCRPC_MSG_REQUEST);			\
		pscrpc_nbreqset_add(sl_nbrqset, (rq));			\
	} _PFL_RVEND

#define SL_GET_RQ_STATUS(csvc, rq, mp, error)				\
	do {								\
		(error) = (rq)->rq_repmsg->status;			\
		if ((error) == 0)					\
			(error) = (rq)->rq_status;			\
		if ((error) == 0)					\
			(error) = authbuf_check((rq), PSCRPC_MSG_REPLY);\
		if ((error) == 0)					\
			(error) = (mp) ? (mp)->rc : -ENOMSG;		\
		if ((error) == -PFLERR_NOTCONN && (csvc))		\
			sl_csvc_disconnect(csvc);			\
	} while (0)

#define SL_GET_RQ_STATUS_TYPE(csvc, rq, type, rc)			\
	do {								\
		type *_mp;						\
									\
		_mp = pscrpc_msg_buf((rq)->rq_repmsg, 0, sizeof(*_mp));	\
		SL_GET_RQ_STATUS((csvc), (rq), _mp, (rc));		\
	} while (0)

struct slashrpc_cservice *
	 _sl_csvc_get(const struct pfl_callerinfo *,
	     struct slashrpc_cservice **, int, struct pscrpc_export *,
	     struct psc_dynarray *, uint32_t, uint32_t, uint64_t,
	     uint32_t, enum slconn_type, struct psc_multiwait *);
void	_sl_csvc_decref(const struct pfl_callerinfo *, struct slashrpc_cservice *);
void	_sl_csvc_disconnect(const struct pfl_callerinfo *, struct slashrpc_cservice *);
void	 sl_csvc_incref(struct slashrpc_cservice *);
void	 sl_csvc_markfree(struct slashrpc_cservice *);
int	 sl_csvc_useable(struct slashrpc_cservice *);
void	_sl_csvc_waitrelv(struct slashrpc_cservice *, long, long);

void	 sl_exp_hldrop_resm(struct pscrpc_export *);
void	*sl_exp_getpri_cli(struct pscrpc_export *);

void	 sl_resm_hldrop(struct sl_resm *);

struct psc_thread
	*slconnthr_spawn(int, const char *, int (*)(void *), void *);
void	 slconnthr_watch(struct psc_thread *, struct slashrpc_cservice *,
	    int, int (*)(void *), void *);

int	 slrpc_handle_connect(struct pscrpc_request *, uint64_t, uint32_t, enum slconn_type);

int	 slrpc_newgenreq(struct slashrpc_cservice *, int,
		struct pscrpc_request **, int, int, void *);
int	 slrpc_newreq(struct slashrpc_cservice *, int,
		struct pscrpc_request **, int, int, void *);

int	 slrpc_waitrep(struct slashrpc_cservice *,
		struct pscrpc_request *, int, void *);

int	 slrpc_allocrepn(struct pscrpc_request *, void *, int, void *,
		int, const int *, int);
int	 slrpc_allocgenrep(struct pscrpc_request *, void *, int, void *,
		int, int);
int	 slrpc_allocrep(struct pscrpc_request *, void *, int, void *,
		int, int);

void	 slrpc_req_out(struct slashrpc_cservice *, struct pscrpc_request *);
void	 slrpc_rep_in(struct slashrpc_cservice *, struct pscrpc_request *);
void	 slrpc_req_in(struct pscrpc_request *);

extern struct psc_dynarray	 sl_lnet_prids;
extern struct psc_lockedlist	 sl_clients;
extern struct sl_expcli_ops	 sl_expcli_ops;
extern struct pscrpc_nbreqset	*sl_nbrqset;

#endif /* _SLCONN_H_ */
