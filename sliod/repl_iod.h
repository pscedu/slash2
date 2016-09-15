/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
 * Definitions for file data replication support.  Replication is
 * arranged by the MDS and IOS are instructed to pull data from other to
 * satisfy requests.
 */

#ifndef _REPL_IOD_H_
#define _REPL_IOD_H_

#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/lockedlist.h"
#include "pfl/rpc.h"

#include "fid.h"
#include "sltypes.h"

struct bmapc_memb;
struct fidc_membh;
struct sl_resm;
struct slrpc_batch_rep;
struct slvr;

#define SLI_REPL_SLVR_SCHED	((void *)0x1)

struct sli_repl_workrq {				/* sli_replwkrq_pool */
	struct sl_fidgen	 srw_fg;
	sl_bmapno_t		 srw_bmapno;
	sl_bmapgen_t		 srw_bgen;		/* bmap generation */
	uint32_t		 srw_len;		/* bmap size */
	struct sl_resource	*srw_src_res;		/* repl source */

	psc_spinlock_t		 srw_lock;
	int32_t			 srw_status;		/* return code to pass back to MDS */
	psc_atomic32_t		 srw_refcnt;		/* number of inflight slivers */
	int			 srw_nslvr_tot;
	int			 srw_nslvr_cur;

	struct slrpc_batch_rep	*srw_bp;
	struct srt_replwk_rep   *srw_rep;		/* batch reply buffer entry for
							 * reporting return code for this work */

	struct bmapc_memb	*srw_bcm;
	struct psclist_head	 srw_active_lentry;	/* entry in the active list */
	struct psclist_head	 srw_pending_lentry;	/* entry in the pending list */

	struct slvr		*srw_slvr[SLASH_SLVRS_PER_BMAP];
};

#define PFLOG_REPLWK(level, srw, fmt, ...)				\
	psclog((level), "srw@%p refcnt=%d " fmt,			\
	    (srw), psc_atomic32_read(&(srw)->srw_refcnt), ##__VA_ARGS__)

struct sli_repl_workrq *
	sli_repl_findwq(const struct sl_fidgen *, sl_bmapno_t);

int	sli_repl_addwk(struct slrpc_batch_rep *, void *, void *);
void	sli_repl_init(void);

void	sli_replwkrq_decref(struct sli_repl_workrq *, int);

void	sli_bwqueued_adj(int32_t *, int);

int	sli_replwk_queue(struct sli_repl_workrq *);

extern struct psc_lockedlist	 sli_replwkq_active;
extern struct psc_listcache	 sli_replwkq_pending;

#endif /* _REPL_IOD_H_ */
