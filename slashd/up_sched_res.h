/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2013, Pittsburgh Supercomputing Center (PSC).
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
 * Update scheduler: this component manages updates to I/O systems such
 * as file chunks to replicate and garbage reclamation.  One thread is
 * spawned per site to watch over activity destined for any I/O system
 * contained therein.
 */

#ifndef _UP_SCHED_RES_H_
#define _UP_SCHED_RES_H_

#include "subsys_mds.h"

#include <sqlite3.h>

struct odtable_receipt;

#define UPSCH_MAX_ITEMS			1024
#define UPSCH_MAX_ITEMS_RES		256

struct slm_update_data {
	int				 upd_type:4;
	int				 upd_flags;
	pthread_t			 upd_owner;
	struct pfl_mutex		 upd_mutex;
	struct psc_multiwaitcond	 upd_mwc;
	struct psc_listentry		 upd_lentry;
	struct odtable_receipt		*upd_recpt;
};

/* upd_flags */
#define UPDF_BUSY			(1 << 0)	/* item is being modified */

/* upd_type */
enum {
	UPDT_BMAP,
	UPDT_GARBAGE,
	UPDT_HLDROP,
	UPDT_PAGEIN,
	UPDT_PAGEIN_UNIT
};

#define upd_2_bmi(upd)			((struct bmap_mds_info *)upd_getpriv(upd))
#define upd_2_bmap(upd)			bmi_2_bmap(upd_2_bmi(upd))
#define upd_2_fcmh(upd)			upd_2_bmap(upd)->bcm_fcmh
#define upd_2_inoh(upd)			fcmh_2_inoh(upd_2_fcmh(upd))

struct slm_update_generic {
	struct sl_resm			*upg_resm;
	struct slash_fidgen		 upg_fg;
	sl_bmapno_t			 upg_bno;
	struct slm_update_data		 upg_upd;
	struct psc_listentry		 upg_lentry;
};

#define UPD_LOCK(upd)			psc_mutex_lock(&(upd)->upd_mutex)
#define UPD_ULOCK(upd)			psc_mutex_unlock(&(upd)->upd_mutex)
#define UPD_RLOCK(upd)			psc_mutex_reqlock(&(upd)->upd_mutex)
#define UPD_URLOCK(upd, lk)		psc_mutex_ureqlock(&(upd)->upd_mutex, (lk))
#define UPD_HASLOCK(upd)		psc_mutex_haslock(&(upd)->upd_mutex)
#define UPD_WAKE(upd)			psc_multiwaitcond_wakeup(&(upd)->upd_mwc)

#define UPD_WAIT(upd)							\
	do {								\
		UPD_RLOCK(upd);						\
		while ((upd)->upd_flags & UPDF_BUSY) {			\
			psc_multiwaitcond_wait(&(upd)->upd_mwc,		\
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

#define DEBUG_UPD(level, upd, msg, ...)					\
	psclogs((level), SLMSS_UPSCH,					\
	    "upd@%p type=%d flags=%u:%s " msg,				\
	    (upd), (upd)->upd_type, (upd)->upd_flags,			\
	    (upd)->upd_flags & UPDF_BUSY	? "b" : "",		\
	    ## __VA_ARGS__)

#define UPD_INITF_NOKEY		(1 << 0)	/* don't consult upsch db for odt key  */

#define upd_init(upd, type)	upd_initf((upd), (type), 0)

void	 upsch_enqueue(struct slm_update_data *);
void	 upsch_purge(slfid_t);
void	 upschq_resm(struct sl_resm *, int);

int	 slm_wk_upsch_purge(void *);

void	 upd_initf(struct slm_update_data *, int, int);
void	 upd_destroy(struct slm_update_data *);
void	*upd_getpriv(struct slm_update_data *);
void	 upd_tryremove(struct slm_update_data *);
void	 upd_rpmi_remove(struct resprof_mds_info *, struct slm_update_data *);

int	 slm_ptrunc_odt_startup_cb(void *, struct odtable_receipt *, void *);
int	 slm_repl_odt_startup_cb(void *, struct odtable_receipt *, void *);

#define UPSCH_LOCK()		MLIST_LOCK(&slm_upschq)
#define UPSCH_ULOCK()		MLIST_ULOCK(&slm_upschq)
#define UPSCH_HASLOCK()		MLIST_HASLOCK(&slm_upschq)
#define UPSCH_RLOCK()		MLIST_REQLOCK(&slm_upschq)
#define UPSCH_URLOCK(lk)	MLIST_URLOCK(&slm_upschq, (lk))
#define UPSCH_ENSURE_LOCKED()	MLIST_ENSURE_LOCKED(&slm_upschq)
#define UPSCH_WAKE()		psc_multiwaitcond_wakeup(&slm_upschq.pml_mwcond_empty)

extern struct psc_mlist		 slm_upschq;
extern sqlite3			*slm_dbh;
extern struct pfl_mutex		 slm_dbh_mut;
extern struct psc_hashtbl	 slm_sth_hashtbl;

struct slm_sth {
	struct psc_hashent	 sth_hentry;
	const char		*sth_fmt;
	sqlite3_stmt		*sth_sth;
	struct pfl_mutex	 sth_mutex;
};

#ifndef SQLITE_INTEGER64
#define SQLITE_INTEGER64 (SQLITE_INTEGER + 1000)
#endif

#endif /* _UP_SCHED_RES_H_ */
