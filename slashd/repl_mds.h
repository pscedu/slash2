/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_MDS_REPL_H_
#define _SL_MDS_REPL_H_

#include "pfl/tree.h"

#include "fidc_mds.h"

#define SLM_REPLRQ_NBMAPS_MAX	64

struct resm_mds_info;

struct slm_replst_workreq {
	struct slrpc_cservice	*rsw_csvc;
	struct sl_fidgen	 rsw_fg;
	int			 rsw_cid;	/* client-issued ID */
	struct psclist_head	 rsw_lentry;
};

typedef void (*brepl_walkcb_t)(struct bmap *, int, int, void *);

int	 mds_repl_inv_except(struct bmap *, int);
int	 mds_repl_addrq(const struct sl_fidgen *, sl_bmapno_t,
	    sl_bmapno_t *, sl_replica_t *, int, int, int);

int	_mds_repl_bmap_apply(struct bmap *, const int *, const int *, int, int, int *, brepl_walkcb_t, void *);
int	_mds_repl_bmap_walk(struct bmap *, const int *, const int *, int, const int *, int, brepl_walkcb_t, void *);
int	 mds_repl_delrq(const struct sl_fidgen *, sl_bmapno_t, sl_bmapno_t *, sl_replica_t *, int);
int	_mds_repl_ios_lookup(int, struct slash_inode_handle *, sl_ios_id_t, int);
int	_mds_repl_iosv_lookup(int, struct slash_inode_handle *, const sl_replica_t [], int [], int, int);

int	 resmpair_bw_adj(struct sl_resm *, struct sl_resm *, int64_t, int);

void	 slm_repl_upd_write(struct bmap *, int);

void	 mds_brepls_check(uint8_t *, int);

/* replication state walking flags */
#define REPL_WALKF_SCIRCUIT	(1 << 0)	/* short circuit on return value set */
#define REPL_WALKF_MODOTH	(1 << 1)	/* modify everyone except specified IOS */

/* walk the bmap replica bitmap, iv and ni specify the IOS index array and its size */

#define mds_repl_bmap_walk(b, t, r, fl, iv, ni)			\
	_mds_repl_bmap_walk((b), (t), (r), (fl), (iv), (ni), NULL, NULL)

#define mds_repl_bmap_walk_all(b, t, r, fl)			\
	_mds_repl_bmap_walk((b), (t), (r), (fl), NULL, 0, NULL, NULL)

#define mds_repl_bmap_walkcb(b, t, r, fl, cbf, arg)		\
	_mds_repl_bmap_walk((b), (t), (r), (fl), NULL, 0, (cbf), (arg))

#define mds_repl_bmap_apply(b, tract, retifset, off)		\
	_mds_repl_bmap_apply((b), (tract), (retifset), 0, (off), NULL, NULL, NULL)

#define IOSV_LOOKUPF_ADD	1
#define IOSV_LOOKUPF_DEL	2
#define IOSV_LOOKUPF_LOOKUP	3

#define mds_repl_ios_lookup_add(vfsid, ih, iosid)		\
	_mds_repl_ios_lookup((vfsid), (ih), (iosid), IOSV_LOOKUPF_ADD)

#define mds_repl_ios_lookup(vfsid, ih, iosid)			\
	_mds_repl_ios_lookup((vfsid), (ih), (iosid), IOSV_LOOKUPF_LOOKUP)

#define mds_repl_iosv_lookup(vfsid, ih, ios, idx, nios)		\
	_mds_repl_iosv_lookup((vfsid), (ih), (ios), (idx), (nios), IOSV_LOOKUPF_LOOKUP)

#define mds_repl_iosv_lookup_add(vfsid, ih, ios, idx, nios)	\
	_mds_repl_iosv_lookup((vfsid), (ih), (ios), (idx), (nios), IOSV_LOOKUPF_ADD)

extern struct psc_listcache	 slm_replst_workq;
extern int			 slm_upsch_bandwidth;
extern struct psc_poolmgr	*slm_repl_status_pool;

#endif /* _SL_MDS_REPL_H_ */
