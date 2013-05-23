/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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
struct slvr_ref;

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

	struct slvr_ref		*srw_slvr_refs[REPL_MAX_INFLIGHT_SLVRS];
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
