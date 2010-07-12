/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_MDS_REPL_H_
#define _SL_MDS_REPL_H_

#include "psc_ds/tree.h"

#include "fidc_mds.h"

struct resm_mds_info;
struct up_sched_work_item;

struct slm_replst_workreq {
	struct slashrpc_cservice	*rsw_csvc;
	struct slash_fidgen		 rsw_fg;
	int				 rsw_cid;	/* client-issued ID */
	struct psclist_head		 rsw_lentry;
};

int	 mds_repl_addrq(const struct slash_fidgen *, sl_bmapno_t, const sl_replica_t *, int);
int	_mds_repl_bmap_apply(struct bmapc_memb *, const int [], const int [], int, int, int *);
void	 mds_repl_bmap_rel(struct bmapc_memb *);
int	 mds_repl_bmap_walk(struct bmapc_memb *, const int [], const int [], int, const int *, int);
int	 mds_repl_delrq(const struct slash_fidgen *, sl_bmapno_t, const sl_replica_t *, int);
void	 mds_repl_init(void);
int	 mds_repl_inv_except(struct bmapc_memb *, sl_ios_id_t);
int	_mds_repl_ios_lookup(struct slash_inode_handle *, sl_ios_id_t, int, int);
int	 mds_repl_loadino(const struct slash_fidgen *, struct fidc_membh **);
void	 mds_repl_node_clearallbusy(struct resm_mds_info *);
int	_mds_repl_nodes_setbusy(struct resm_mds_info *, struct resm_mds_info *, int, int);
void	 mds_repl_reset_scheduled(sl_ios_id_t);

/* replication state walking flags */
#define REPL_WALKF_SCIRCUIT	(1 << 0)	/* short circuit on return value set */
#define REPL_WALKF_MODOTH	(1 << 1)	/* modify everyone except specified IOS */

#define mds_repl_bmap_walk_all(b, t, r, fl)	mds_repl_bmap_walk((b), (t), (r), (fl), NULL, 0)

#define mds_repl_bmap_apply(bcm, tract, retifset, off)			\
	_mds_repl_bmap_apply((bcm), (tract), (retifset), 0, (off), NULL)

#define mds_repl_nodes_setbusy(a, b, v)		_mds_repl_nodes_setbusy((a), (b), 1, (v))

#define mds_repl_ios_lookup_add(ih, ios, jrnl)	_mds_repl_ios_lookup((ih), (ios), 1, (jrnl))
#define mds_repl_ios_lookup(ih, ios)		_mds_repl_ios_lookup((ih), (ios), 0, 0)

extern struct psc_listcache	 slm_replst_workq;

extern struct psc_vbitmap	*repl_busytable;
extern psc_spinlock_t		 repl_busytable_lock;

extern sl_ino_t			 mds_repldir_inum;

#endif /* _SL_MDS_REPL_H_ */
