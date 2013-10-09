/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _REPL_IOD_H_
#define _REPL_IOD_H_

#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lockedlist.h"
#include "pfl/rpc.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "sltypes.h"

struct bmapc_memb;
struct fidc_membh;
struct sl_resm;
struct slvr;

#define REPL_MAX_INFLIGHT_SLVRS	4			/* maximum # inflight slivers between IONs */

#define SLI_REPL_SLVR_SCHED	((void *)0x1)

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	int			 srw_op;
	sl_bmapno_t		 srw_bmapno;
	sl_bmapgen_t		 srw_bgen;		/* bmap generation */
	struct sl_resource	*srw_src_res;		/* repl source */
	uint32_t		 srw_len;		/* bmap size */

	uint32_t		 srw_status;		/* return code to pass back to MDS */
	psc_spinlock_t		 srw_lock;
	psc_atomic32_t		 srw_refcnt;		/* number of inflight slivers */
	int			 srw_nslvr_tot;
	int			 srw_nslvr_cur;

	struct bmapc_memb	*srw_bcm;
	struct fidc_membh	*srw_fcmh;
	struct psclist_head	 srw_active_lentry;	/* entry in the active list */
	struct psclist_head	 srw_pending_lentry;	/* entry in the pending list */

	struct slvr		*srw_slvr[REPL_MAX_INFLIGHT_SLVRS];
};

enum {
	SLI_REPLWKOP_PTRUNC,
	SLI_REPLWKOP_REPL
};

#define DEBUG_SRW(srw, level, msg)					\
	psclog((level), "srw@%p refcnt=%d " msg,			\
	    (srw), psc_atomic32_read(&(srw)->srw_refcnt))

struct sli_repl_workrq *
	sli_repl_findwq(const struct slash_fidgen *, sl_bmapno_t);

int	sli_repl_addwk(int, struct sl_resource *,
	    const struct slash_fidgen *, sl_bmapno_t, sl_bmapgen_t, int);
void	sli_repl_init(void);

void	sli_replwkrq_decref(struct sli_repl_workrq *, int);

void replwk_queue(struct sli_repl_workrq *);

extern struct psc_lockedlist	 sli_replwkq_active;
extern struct psc_listcache	 sli_replwkq_pending;

#endif /* _REPL_IOD_H_ */
