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

#define UPSCH_PAGEIN_BATCH	128

extern int slm_upsch_batch_size;
extern int slm_upsch_repl_expire;
extern int slm_upsch_preclaim_expire;
extern int slm_upsch_page_interval;

extern psc_spinlock_t           slm_upsch_lock;
extern struct psc_waitq		slm_upsch_waitq;
extern struct psc_listcache     slm_upsch_queue;

struct slm_update_data {
	int				 upd_flags;
	psc_spinlock_t			 upd_lock;
	struct psc_listentry		 upd_lentry;
};

/* upd_flags */
#define UPDF_BUSY			(1 << 0)	/* item is being modified */
#define UPDF_LIST			(1 << 1)	/* item is on list */

struct slm_update_generic {
	struct sl_resm			*upg_resm;
	struct sl_fidgen		 upg_fg;
	sl_bmapno_t			 upg_bno;
	struct slm_update_data		 upg_upd;
	struct psc_listentry		 upg_lentry;
};

#define UPD_LOCK(upd)			spinlock(&(upd)->upd_lock)
#define UPD_ULOCK(upd)			freelock(&(upd)->upd_lock)

#define UPD_INCREF(upd)							\
	do {								\
		void * _p;						\
		struct bmapc_memb *_b;					\
		struct bmap_mds_info *_bmi;				\
		_p = (void *)upd;					\
		_p = _p - offsetof(struct bmap_mds_info, bmi_upd);	\
		_bmi = (struct bmap_mds_info *)_p;			\
		_b = bmi_2_bmap(_bmi);					\
		bmap_op_start_type(_b, BMAP_OPCNT_UPSCH);		\
	} while (0)

#define UPD_DECREF(upd)							\
	do {								\
		void *_p;						\
		struct bmapc_memb *_b;					\
		struct bmap_mds_info *_bmi;				\
		_p = (void *)upd;					\
		_p = _p - offsetof(struct bmap_mds_info, bmi_upd);	\
		_bmi = (struct bmap_mds_info *)_p;			\
		_b = bmi_2_bmap(_bmi);					\
		bmap_op_done_type(_b, BMAP_OPCNT_UPSCH);		\
	} while (0)


void	 upsch_enqueue(struct slm_update_data *);
void	 upsch_purge(slfid_t);
void	 upschq_resm(struct sl_resm *, int);

int	 slm_wk_upsch_purge(void *);

void	 slm_upsch_init(void);
void	 slmupschthr_spawn(void);

int	 slm_upsch_insert(struct bmap *, sl_ios_id_t, int, int);

int	 slm_upsch_tally_cb(sqlite3_stmt *, void *);
int	 slm_upsch_requeue_cb(sqlite3_stmt *, void *);

void	 upd_init(struct slm_update_data *);
void	 upd_destroy(struct slm_update_data *);
void	*upd_getpriv(struct slm_update_data *);

struct slm_sth {
	struct pfl_hashentry	 sth_hentry;
	const char		*sth_fmt;
	sqlite3_stmt		*sth_sth;
};

#ifndef SQLITE_INTEGER64
#define SQLITE_INTEGER64 (SQLITE_INTEGER + 1000)
#endif

#endif /* _UP_SCHED_RES_H_ */
