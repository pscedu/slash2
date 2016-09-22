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

#define	SLM_UPSCH_PAUSE			30

extern int slm_upsch_delay;

extern psc_spinlock_t           slm_upsch_lock;
extern struct psc_waitq		slm_upsch_waitq;
extern struct psc_listcache     slm_upsch_queue;

struct slm_update_data {
	int				 upd_type:4;
	int				 upd_flags;
	pthread_t			 upd_owner;
	psc_spinlock_t			 upd_lock;
	struct psc_waitq		 upd_waitq;
	struct psc_listentry		 upd_lentry;
};

/* upd_flags */
#define UPDF_BUSY			(1 << 0)	/* item is being modified */
#define UPDF_LIST			(1 << 1)	/* item is on list */

/* upd_type, which is used, among other things, to index *upd_proctab[] */
enum upd_type_enum {
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

#define UPD_LOCK(upd)			spinlock(&(upd)->upd_lock)
#define UPD_ULOCK(upd)			freelock(&(upd)->upd_lock)

#define UPD_WAIT(upd)							\
	do {								\
		LOCK_ENSURE(&(upd)->upd_lock);				\
		while ((upd)->upd_flags & UPDF_BUSY) {			\
			psc_waitq_wait(&(upd)->upd_waitq, 		\
				&(upd)->upd_lock);			\
			spinlock(&(upd)->upd_lock);			\
		}							\
	} while (0)

#define UPD_WAKE(upd)	psc_waitq_wakeall(&(upd)->upd_waitq); 
		
#define UPD_UNBUSY(upd)							\
	do {								\
		LOCK_ENSURE(&(upd)->upd_lock);				\
		(upd)->upd_flags &= ~UPDF_BUSY;				\
		(upd)->upd_owner = 0;					\
		psc_waitq_wakeall(&(upd)->upd_waitq);			\
		freelock(&(upd)->upd_lock);				\
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

void	 upsch_enqueue(struct slm_update_data *);
void	 upsch_purge(slfid_t);
void	 upschq_resm(struct sl_resm *, int);

int	 slm_wk_upsch_purge(void *);

void	 slm_upsch_init(void);
void	 slmupschthr_spawn(void);

int	 slm_upsch_insert(struct bmap *, sl_ios_id_t, int, int);

int	 slm_upsch_tally_cb(struct slm_sth *, void *);
int	 slm_upsch_revert_cb(struct slm_sth *, void *);

void	 upd_init(struct slm_update_data *, int);
void	 upd_destroy(struct slm_update_data *);
void	*upd_getpriv(struct slm_update_data *);

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
