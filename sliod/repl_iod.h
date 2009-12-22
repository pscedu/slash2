/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_ds/list.h"

#include "cache_params.h"
#include "fid.h"
#include "sltypes.h"

#define REPL_MAX_INFLIGHT_SLVRS	64			/* maximum # inflight slivers between IONs */

struct sli_repl_workrq {
	struct slash_fidgen	 srw_fg;
	sl_bmapno_t		 srw_bmapno;
	uint64_t		 srw_nid;
	uint32_t		 srw_len;		/* bmap size */
	uint32_t		 srw_status;		/* return code to pass back to MDS */

	struct sl_resm		*srw_resm;
	struct bmapc_memb	*srw_bcm;
	struct fidc_membh	*srw_fcmh;
	struct psclist_head	 srw_state_lentry;	/* entry for which state list */
	struct psclist_head	 srw_active_lentry;	/* entry in global active list */

	struct slvr_ref		*srw_slvr_refs[REPL_MAX_INFLIGHT_SLVRS];
	psc_spinlock_t		 srw_lock;
	struct psc_vbitmap	*srw_inflight;
	uint8_t			 srw_sliver_rem[SLASH_SLVRS_PER_BMAP / NBBY];
};

void sli_repl_addwk(uint64_t, struct slash_fidgen *, sl_bmapno_t, int);
void sli_repl_init(void);

extern struct pscrpc_nbreqset	 sli_replwk_nbset;
extern struct psc_listcache	 sli_replwkq_pending;
extern struct psc_listcache	 sli_replwkq_finished;
extern struct psc_listcache	 sli_replwkq_inflight;
extern struct psc_lockedlist	 sli_replwkq_active;
