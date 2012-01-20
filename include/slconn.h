/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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
 * This interface provides connections to hosts (servers and clients) in
 * a SLASH network.
 */

#ifndef _SLCONN_H_
#define _SLCONN_H_

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>

#include "psc_rpc/export.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"

#include "slerr.h"

struct pscrpc_import;
struct pscrpc_export;

struct sl_resm;

enum slconn_type {
	SLCONNT_CLI,
	SLCONNT_IOD,
	SLCONNT_MDS,
	SLNCONNT
};

union lockmutex {
	psc_spinlock_t		*lm_lock;
	struct pfl_mutex	*lm_mutex;
	void			*lm_ptr;
};

struct slconn_thread {
	struct sl_resm		*sct_resm;
	uint32_t		 sct_rqptl;
	uint32_t		 sct_rpptl;
	uint64_t		 sct_magic;
	uint32_t		 sct_version;
	union lockmutex		 sct_lockinfo;
	void			*sct_waitinfo;
	enum slconn_type	 sct_peertype;
	int			 sct_flags;
	int			(*sct_cb)(void *);
	void			*sct_cbarg;
};

struct slashrpc_cservice {
	enum slconn_type	 csvc_ctype;
	uint64_t		 csvc_magic;
	struct pscrpc_import	*csvc_import;
	union lockmutex		 csvc_lockinfo;
	void			*csvc_waitinfo;
	psc_atomic32_t		 csvc_flags;
	uint32_t		 csvc_version;
	int			 csvc_lasterrno;
	psc_atomic32_t		 csvc_refcnt;
	time_t			 csvc_mtime;		/* last connection try */
	struct psclist_head	 csvc_lentry;
#define csvc_lock	csvc_lockinfo.lm_lock
#define csvc_mutex	csvc_lockinfo.lm_mutex
};

/* csvc_flags */
#define CSVCF_CONNECTING	(1 << 0)		/* connection attempt in progress */
#define CSVCF_CONNECTED		(1 << 1)
#define CSVCF_USE_MULTIWAIT	(1 << 2)
#define CSVCF_ABANDON		(1 << 3)		/* connection should be dropped */
#define CSVCF_WANTFREE		(1 << 4)		/* csvc mem resources need freed */

/* sl_csvc_get() flags */
#define CSVCF_NONBLOCK		(1 << 5)		/* don't timeout waiting for new establishment */
#define CSVCF_NORECON		(1 << 6)		/* do not attempt reconnection if down */

#define CSVC_RECONNECT_INTV	10			/* seconds */
#define CSVC_PING_INTV          10			/* seconds */

#define DEBUG_CSVC(lvl, csvc, fmt, ...)					\
	do {								\
		int _flags;						\
									\
		_flags = psc_atomic32_read(&(csvc)->csvc_flags);	\
		psclog((lvl), "csvc@%p fl=%#x:%s%s%s%s%s ref:%d " fmt,	\
		    (csvc), _flags,					\
		    _flags & CSVCF_CONNECTING	 ? "C" : "",		\
		    _flags & CSVCF_CONNECTED	 ? "O" : "",		\
		    _flags & CSVCF_USE_MULTIWAIT ? "M" : "",		\
		    _flags & CSVCF_ABANDON	 ? "A" : "",		\
		    _flags & CSVCF_WANTFREE	 ? "F" : "",		\
		    psc_atomic32_read(&(csvc)->csvc_refcnt),		\
		    ##__VA_ARGS__);					\
	} while (0)

struct sl_expcli_ops {
	void	(*secop_allocpri)(struct pscrpc_export *);
	void	(*secop_destroy)(void *);
};

#define CSVC_CALLERINFO			PFL_CALLERINFO()

#define sl_csvc_get(csvcp, flg, exp, nid,				\
	    pq, pp, mag, vers, lockp, waitinfo, ctype, arg)		\
	_sl_csvc_get(CSVC_CALLERINFO, (csvcp), (flg), (exp), (nid),	\
	    (pq), (pp), (mag), (vers), (lockp), (waitinfo), (ctype), (arg))

#define sl_csvc_decref(csvc)		_sl_csvc_decref(CSVC_CALLERINFO, (csvc))
#define sl_csvc_disconnect(csvc)        _sl_csvc_disconnect(CSVC_CALLERINFO, (csvc))

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
				sl_csvc_lock(_csvc);			\
				if (sl_csvc_useable(_csvc))		\
					sl_csvc_unlock(_csvc);		\
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

#define sl_csvc_waitrel_s(csvc, s)	_sl_csvc_waitrelv((csvc), (s), 0L)

#define sl_csvc_lock(csvc)						\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_mutex_lock_pci(CSVC_CALLERINFO,		\
			    (csvc)->csvc_mutex);			\
		else							\
			spinlock_pci(CSVC_CALLERINFO,			\
			    (csvc)->csvc_lock);				\
	} while (0)

#define sl_csvc_unlock(csvc)						\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_mutex_unlock_pci(CSVC_CALLERINFO,		\
			    (csvc)->csvc_mutex);			\
		else							\
			freelock_pci(CSVC_CALLERINFO,			\
			    (csvc)->csvc_lock);				\
	} while (0)

#define sl_csvc_reqlock(csvc)						\
	(sl_csvc_usemultiwait(csvc) ?					\
	    psc_mutex_reqlock_pci(CSVC_CALLERINFO, (csvc)->csvc_mutex) :\
	    reqlock_pci(CSVC_CALLERINFO, (csvc)->csvc_lock))

#define sl_csvc_ureqlock(csvc, waslocked)				\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_mutex_ureqlock_pci(CSVC_CALLERINFO,		\
			    (csvc)->csvc_mutex, (waslocked));		\
		else							\
			ureqlock_pci(CSVC_CALLERINFO,			\
			    (csvc)->csvc_lock, (waslocked));		\
	} while (0)

#define sl_csvc_lock_ensure(csvc)					\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_mutex_ensure_locked((csvc)->csvc_mutex);	\
		else							\
			LOCK_ENSURE((csvc)->csvc_lock);			\
	} while (0)

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
		if (_rc == 0 && (mp)->rc == SLERR_NOTCONN)		\
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

struct slashrpc_cservice *
	 _sl_csvc_get(const struct pfl_callerinfo *,
	     struct slashrpc_cservice **, int, struct pscrpc_export *,
	     const struct psc_dynarray *, uint32_t, uint32_t, uint64_t,
	     uint32_t, void *, void *, enum slconn_type, void *);
void	_sl_csvc_decref(const struct pfl_callerinfo *, struct slashrpc_cservice *);
void	_sl_csvc_disconnect(const struct pfl_callerinfo *, struct slashrpc_cservice *);
void	 sl_csvc_incref(struct slashrpc_cservice *);
void	 sl_csvc_markfree(struct slashrpc_cservice *);
int	 sl_csvc_useable(struct slashrpc_cservice *);
int	 sl_csvc_usemultiwait(struct slashrpc_cservice *);
void	_sl_csvc_waitrelv(struct slashrpc_cservice *, long, long);
void	 sl_csvc_wake(struct slashrpc_cservice *);

void	 sl_exp_hldrop_resm(struct pscrpc_export *);
void	*sl_exp_getpri_cli(struct pscrpc_export *);

void	 sl_resm_hldrop(struct sl_resm *);

void	 slconnthr_spawn(struct sl_resm *, uint32_t, uint32_t, uint64_t,
		uint32_t, void *, int, void *, enum slconn_type, int,
		const char *, int (*)(void *), void *);

int	 slrpc_handle_connect(struct pscrpc_request *, uint64_t, uint32_t, enum slconn_type);

int	 slrpc_newgenreq(struct slashrpc_cservice *, int,
		struct pscrpc_request **, int, int, void *);
int	 slrpc_newreq(struct slashrpc_cservice *, int,
		struct pscrpc_request **, int, int, void *);

int	 slrpc_waitgenrep(struct pscrpc_request *, int, void *);
int	 slrpc_waitrep(struct slashrpc_cservice *,
		struct pscrpc_request *, int, void *);

int	 slrpc_allocrepn(struct pscrpc_request *, void *, int, void *,
		int, const int *, int);
int	 slrpc_allocgenrep(struct pscrpc_request *, void *, int, void *,
		int, int);
int	 slrpc_allocrep(struct pscrpc_request *, void *, int, void *,
		int, int);

extern struct psc_dynarray	 lnet_prids;
extern struct psc_lockedlist	 client_csvcs;
extern struct sl_expcli_ops	 sl_expcli_ops;
extern struct pscrpc_nbreqset	*sl_nbrqset;
extern uint32_t			 sys_upnonce;

#endif /* _SLCONN_H_ */
