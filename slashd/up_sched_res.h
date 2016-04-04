/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2010-2016, Pittsburgh Supercomputing Center
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
 * Update scheduler: this component manages updates to I/O systems such
 * as file chunks to replicate and garbage reclamation.  One thread is
 * spawned per site to watch over activity destined for any I/O system
 * contained therein.
 */

#ifndef _UP_SCHED_RES_H_
#define _UP_SCHED_RES_H_

#include "subsys_mds.h"

#include <sqlite3.h>

struct slm_update_data {
	int				 upd_type:4;
	int				 upd_flags;
	pthread_t			 upd_owner;
	struct pfl_mutex		 upd_mutex;
	struct pfl_multiwaitcond	 upd_mwc;
	struct psc_listentry		 upd_lentry;
};

/* upd_flags */
#define UPDF_BUSY			(1 << 0)	/* item is being modified */

/* upd_type, which is used, among other things, to index *upd_proctab[] */
enum {
	UPDT_BMAP,			/* upd_proc_bmap() */
	UPDT_HLDROP,			/* upd_proc_hldrop() */
	UPDT_PAGEIN,			/* upd_proc_pagein() */
	UPDT_PAGEIN_UNIT		/* upd_proc_pagein_unit() */
};

#define upd_2_bmi(upd)			((struct bmap_mds_info *)upd_getpriv(upd))
#define upd_2_bmap(upd)			bmi_2_bmap(upd_2_bmi(upd))
#define upd_2_fcmh(upd)			upd_2_bmap(upd)->bcm_fcmh
#define upd_2_inoh(upd)			fcmh_2_inoh(upd_2_fcmh(upd))

struct slm_update_generic {
	struct sl_resm			*upg_resm;
	struct sl_fidgen		 upg_fg;
	sl_bmapno_t			 upg_bno;
	struct slm_update_data		 upg_upd;
	struct psc_listentry		 upg_lentry;
};

#define UPD_CALLERINFO()		PFL_CALLERINFOSS(SLMSS_UPSCH)

#define UPD_LOCK(upd)			_psc_mutex_lock(UPD_CALLERINFO(), &(upd)->upd_mutex)
#define UPD_ULOCK(upd)			_psc_mutex_unlock(UPD_CALLERINFO(), &(upd)->upd_mutex)
#define UPD_RLOCK(upd)			_psc_mutex_reqlock(UPD_CALLERINFO(), &(upd)->upd_mutex)
#define UPD_URLOCK(upd, lk)		_psc_mutex_ureqlock(UPD_CALLERINFO(), &(upd)->upd_mutex, (lk))
#define UPD_HASLOCK(upd)		_psc_mutex_haslock(UPD_CALLERINFO(), &(upd)->upd_mutex)
#define UPD_WAKE(upd)			pfl_multiwaitcond_wakeup(&(upd)->upd_mwc)

#define UPD_WAIT(upd)							\
	do {								\
		UPD_RLOCK(upd);						\
		while ((upd)->upd_flags & UPDF_BUSY) {			\
			pfl_multiwaitcond_wait(&(upd)->upd_mwc,		\
			    &(upd)->upd_mutex);				\
			UPD_LOCK(upd);					\
		}							\
	} while (0)

#define UPD_UNBUSY(upd)							\
	do {								\
		UPD_RLOCK(upd);						\
		(upd)->upd_flags &= ~UPDF_BUSY;				\
		(upd)->upd_owner = 0;					\
		UPD_WAKE(upd);						\
		UPD_ULOCK(upd);						\
	} while (0)

#define UPD_INCREF(upd)							\
	do {								\
		void *_p;						\
									\
		_p = upd_getpriv(upd);					\
		switch ((upd)->upd_type) {				\
		case UPDT_PAGEIN:					\
		case UPDT_HLDROP:					\
			break;						\
		case UPDT_BMAP: {					\
			struct bmap_mds_info *_bmi = _p;		\
			struct bmapc_memb *_b = bmi_2_bmap(_bmi);	\
									\
			bmap_op_start_type(_b, BMAP_OPCNT_UPSCH);	\
			break;						\
		    }							\
		default:						\
			psc_assert("invalid type");			\
		}							\
	} while (0)

#define UPD_DECREF(upd)							\
	do {								\
		void *_p;						\
									\
		_p = upd_getpriv(upd);					\
		switch ((upd)->upd_type) {				\
		case UPDT_PAGEIN:					\
		case UPDT_HLDROP:					\
			break;						\
		case UPDT_BMAP: {					\
			struct bmap_mds_info *_bmi = _p;		\
			struct bmapc_memb *_b = bmi_2_bmap(_bmi);	\
									\
			bmap_op_done_type(_b, BMAP_OPCNT_UPSCH);	\
			break;						\
		    }							\
		default:						\
			psc_assert("invalid type");			\
		}							\
	} while (0)

#define DPRINTF_UPD(level, upd, msg, ...)				\
	psclogs((level), SLMSS_UPSCH,					\
	    "upd@%p %s=%p type=%d flags=%u:%s " msg,			\
	    (upd),							\
	    (upd)->upd_type == UPDT_BMAP ? "fcmh": "ptr",		\
	    (upd)->upd_type == UPDT_BMAP ?				\
	      bmi_2_bmap(upd_getpriv(upd))->bcm_fcmh : NULL,		\
	    (upd)->upd_type, (upd)->upd_flags,				\
	    (upd)->upd_flags & UPDF_BUSY	? "b" : "",		\
	    ## __VA_ARGS__)

#define upd_init(upd, type)	upd_initf((upd), (type), 0)

void	 upsch_enqueue(struct slm_update_data *);
void	 upsch_purge(slfid_t);
void	 upschq_resm(struct sl_resm *, int);

int	 slm_wk_upsch_purge(void *);

void	 slm_upsch_init(void);
void	 slmupschthr_spawn(void);

int	 slm_upsch_insert(struct bmap *, sl_ios_id_t, int, int);
int	 slm_upsch_revert_cb(struct slm_sth *, void *);

void	 upd_initf(struct slm_update_data *, int, int);
void	 upd_destroy(struct slm_update_data *);
void	*upd_getpriv(struct slm_update_data *);
void	 upd_rpmi_remove(struct resprof_mds_info *, struct slm_update_data *);

#define UPSCH_LOCK()		MLIST_LOCK(&slm_upschq)
#define UPSCH_ULOCK()		MLIST_ULOCK(&slm_upschq)
#define UPSCH_HASLOCK()		MLIST_HASLOCK(&slm_upschq)
#define UPSCH_RLOCK()		MLIST_REQLOCK(&slm_upschq)
#define UPSCH_URLOCK(lk)	MLIST_URLOCK(&slm_upschq, (lk))
#define UPSCH_LOCK_ENSURE()	MLIST_LOCK_ENSURE(&slm_upschq)
#define UPSCH_WAKE()		pfl_multiwaitcond_wakeup(&slm_upschq.pml_mwcond_empty)

extern struct pfl_mlist		 slm_upschq;

struct slm_sth {
	struct pfl_hashentry	 sth_hentry;
	const char		*sth_fmt;
	sqlite3_stmt		*sth_sth;
	struct pfl_mutex	 sth_mutex;
};

#ifndef SQLITE_INTEGER64
#define SQLITE_INTEGER64 (SQLITE_INTEGER + 1000)
#endif

#endif /* _UP_SCHED_RES_H_ */
