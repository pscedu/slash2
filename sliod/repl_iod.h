/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/lockedlist.h"
#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "fid.h"
#include "sltypes.h"

struct psc_vbitmap;

struct bmapc_memb;
struct fidc_membh;
struct sl_resm;
struct slvr_ref;

#define REPL_MAX_INFLIGHT_SLVRS	64			/* maximum # inflight slivers between IONs */

#define SLI_REPL_SLVR_SCHED	((void *)0x1)

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	sl_bmapno_t		 srw_bmapno;
	sl_bmapgen_t		 srw_bgen;		/* bmap generation */
	uint64_t		 srw_nid;		/* repl source network address */
	uint32_t		 srw_len;		/* bmap size */

	uint32_t		 srw_status;		/* return code to pass back to MDS */
	psc_spinlock_t		 srw_lock;
	psc_atomic32_t		 srw_refcnt;
	int			 srw_nslvr_tot;
	int			 srw_nslvr_cur;

	struct sl_resm		*srw_resm;		/* source peer info */
	struct bmapc_memb	*srw_bcm;
	struct fidc_membh	*srw_fcmh;
	struct psclist_head	 srw_state_lentry;	/* entry for which state list */
	struct psclist_head	 srw_active_lentry;	/* entry in global active list */

	struct slvr_ref		*srw_slvr_refs[REPL_MAX_INFLIGHT_SLVRS];
};

int	sli_repl_addwk(uint64_t, const struct slash_fidgen *,
		sl_bmapno_t, sl_bmapgen_t, int);
void	sli_repl_init(void);

void	sli_replwkrq_decref(struct sli_repl_workrq *, int);

extern struct pscrpc_nbreqset	 sli_replwk_nbset;
extern struct psc_lockedlist	 sli_replwkq_active;
extern struct psc_listcache	 sli_replwkq_pending;

#endif /* _REPL_IOD_H_ */
