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

#ifndef _SL_MDS_REPL_H_
#define _SL_MDS_REPL_H_

#include "psc_ds/tree.h"

#include "fidc_mds.h"

struct mds_resm_info;

struct sl_replrq {
	struct slash_inode_handle	*rrq_inoh;
	pthread_mutex_t			 rrq_mutex;
	struct psc_multiwaitcond	 rrq_mwcond;
	int				 rrq_flags;
	psc_atomic32_t			 rrq_refcnt;
	int				 rrq_gen;
	union {
		struct psclist_head	 rrqu_lentry;
		SPLAY_ENTRY(sl_replrq)	 rrqu_tentry;
	} rrq_u;
#define rrq_tentry rrq_u.rrqu_tentry
#define rrq_lentry rrq_u.rrqu_lentry
};

/* replication request flags */
#define REPLRQF_BUSY	(1 << 0)
#define REPLRQF_DIE	(1 << 1)

int replrq_cmp(const void *, const void *);

SPLAY_HEAD(replrqtree, sl_replrq);
SPLAY_PROTOTYPE(replrqtree, sl_replrq, rrq_tentry, replrq_cmp);

struct sl_replrq *
	 mds_repl_findrq(const struct slash_fidgen *, int *);
int	 mds_repl_accessrq(struct sl_replrq *);
int	 mds_repl_addrq(const struct slash_fidgen *, sl_blkno_t, const sl_replica_t *, int);
int	 mds_repl_bmap_apply(struct bmapc_memb *, const int [], const int [], int, int, int *);
void	 mds_repl_bmap_rel(struct bmapc_memb *);
int	 mds_repl_bmap_walk(struct bmapc_memb *, const int [], const int [], int, const int *, int);
int	 mds_repl_delrq(const struct slash_fidgen *, sl_blkno_t, const sl_replica_t *, int);
void	 mds_repl_init(void);
int	 mds_repl_inv_except_locked(struct bmapc_memb *, sl_ios_id_t);
int	_mds_repl_ios_lookup(struct slash_inode_handle *, sl_ios_id_t, int);
int	 mds_repl_loadino(const struct slash_fidgen *, struct fidc_membh **);
int	_mds_repl_nodes_setbusy(struct mds_resm_info *, struct mds_resm_info *, int, int);
void	 mds_repl_reset_scheduled(sl_ios_id_t);
void	 mds_repl_tryrmqfile(struct sl_replrq *);
void	 mds_repl_unrefrq(struct sl_replrq *);

#define mds_repl_nodes_getbusy(a, b)		_mds_repl_nodes_setbusy((a), (b), 0, 0)
#define mds_repl_nodes_setbusy(a, b, v)		_mds_repl_nodes_setbusy((a), (b), 1, (v))

#define mds_repl_ios_lookup_add(ih, ios)	_mds_repl_ios_lookup((ih), (ios), 1)
#define mds_repl_ios_lookup(ih, ios)		_mds_repl_ios_lookup((ih), (ios), 0)

#define REPLRQ_INO(rrq)		(&(rrq)->rrq_inoh->inoh_ino)
#define REPLRQ_INOX(rrq)	(rrq)->rrq_inoh->inoh_extras
#define REPLRQ_NREPLS(rrq)	REPLRQ_INO(rrq)->ino_nrepls
#define REPLRQ_FG(rrq)		(&REPLRQ_INO(rrq)->ino_fg)
#define REPLRQ_FID(rrq)		REPLRQ_FG(rrq)->fg_fid
#define REPLRQ_FCMH(rrq)	(rrq)->rrq_inoh->inoh_fcmh
#define REPLRQ_NBMAPS(rrq)	fcmh_2_nbmaps(REPLRQ_FCMH(rrq))

#define REPLRQ_GETREPL(rrq, n)	((n) < INO_DEF_NREPLS ?			\
				    REPLRQ_INO(rrq)->ino_repls[n] :	\
				    REPLRQ_INOX(rrq)->inox_repls[(n) - 1])

extern struct psc_poolmgr	*replrq_pool;

extern struct replrqtree	 replrq_tree;
extern psc_spinlock_t		 replrq_tree_lock;

extern struct psc_vbitmap	*repl_busytable;
extern psc_spinlock_t		 repl_busytable_lock;

#endif /* _SL_MDS_REPL_H_ */
