/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_rpc/export.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"

#include <stdint.h>

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
	pthread_mutex_t		*lm_mutex;
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
	enum slconn_type	 sct_conntype;
	int			 sct_flags;
};

struct slashrpc_cservice {
	enum slconn_type	 csvc_ctype;
	struct pscrpc_import	*csvc_import;
	union lockmutex		 csvc_lockinfo;
	void			*csvc_waitinfo;
	psc_atomic32_t		 csvc_flags;
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

struct sl_expcli_ops {
	void	(*secop_allocpri)(struct pscrpc_export *);
	void	(*secop_destroy)(void *);
};

#define _SL_EXP_REGISTER_RESM(exp, getcsvc)				\
	{								\
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
	}

#define SL_EXP_REGISTER_RESM(exp, getcsvc)				\
	(_SL_EXP_REGISTER_RESM((exp), getcsvc))

#define sl_csvc_waitrel_s(csvc, s)	_sl_csvc_waitrelv((csvc), (s), 0L)

#define sl_csvc_lock(csvc)						\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_pthread_mutex_lock((csvc)->csvc_mutex);	\
		else							\
			spinlock((csvc)->csvc_lock);			\
	} while (0)

#define sl_csvc_unlock(csvc)						\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_pthread_mutex_unlock((csvc)->csvc_mutex);	\
		else							\
			freelock((csvc)->csvc_lock);			\
	} while (0)

#define sl_csvc_reqlock(csvc)						\
	(sl_csvc_usemultiwait(csvc) ?					\
	    psc_pthread_mutex_reqlock((csvc)->csvc_mutex) :		\
	    reqlock((csvc)->csvc_lock))

#define sl_csvc_ureqlock(csvc, waslocked)				\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_pthread_mutex_ureqlock((csvc)->csvc_mutex,	\
			    (waslocked));				\
		else							\
			ureqlock((csvc)->csvc_lock, (waslocked));	\
	} while (0)

#define sl_csvc_lock_ensure(csvc)					\
	do {								\
		if (sl_csvc_usemultiwait(csvc))				\
			psc_pthread_mutex_ensure_locked(		\
			    (csvc)->csvc_mutex);			\
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

struct slashrpc_cservice *
	 sl_csvc_get(struct slashrpc_cservice **, int, struct pscrpc_export *,
	    lnet_nid_t, uint32_t, uint32_t, uint64_t, uint32_t,
	    void *, void *, enum slconn_type, void *);
void	 sl_csvc_decref(struct slashrpc_cservice *);
void	 sl_csvc_disconnect(struct slashrpc_cservice *);
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
		const char *);

extern struct psc_dynarray	lnet_prids;
extern struct psc_lockedlist	client_csvcs;
extern struct sl_expcli_ops	sl_expcli_ops;

#endif /* _SLCONN_H_ */
