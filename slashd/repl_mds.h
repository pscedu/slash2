/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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
	int				 rsw_cid;		/* client-issued ID */
	struct psclist_head		 rsw_lentry;
};

#if 0

struct slm_resmpair_bandwidth {
	struct psc_lockedlist		 srb_links;
	psc_spinlock_t			 srb_lock;
};

#endif

/* XXX hack: hardcode 1Gbps */
#define SLM_RESMLINK_DEF_BANDWIDTH	 (1024 * 1024 * INT64_C(1024) / 8)

struct slm_resmlink {
//	struct psc_listentry		 srl_lentry;
	int64_t				 srl_avail;		/* units of RESMLINK_UNITSZ bytes/sec */
	int64_t				 srl_used;
};

typedef void (*brepl_walkcb_t)(struct bmapc_memb *, int, int, void *);

int	 mds_repl_addrq(const struct slash_fidgen *, sl_bmapno_t, sl_replica_t *, int);
int	_mds_repl_bmap_apply(struct bmapc_memb *, const int *, const int *, int, int, int *, brepl_walkcb_t, void *);
int	_mds_repl_bmap_walk(struct bmapc_memb *, const int *, const int *, int, const int *, int, brepl_walkcb_t, void *);
int	 mds_repl_delrq(const struct slash_fidgen *, sl_bmapno_t, sl_replica_t *, int);
void	 mds_repl_init(void);
int	 mds_repl_inv_except(struct bmapc_memb *, int);
int	_mds_repl_ios_lookup(struct slash_inode_handle *, sl_ios_id_t, int, int);
int	 mds_repl_loadino(const struct slash_fidgen *, struct fidc_membh **);
void	 mds_repl_node_clearallbusy(struct resm_mds_info *);
int64_t mds_repl_nodes_adjbusy(struct resm_mds_info *, struct resm_mds_info *, int64_t);
void	 mds_repl_reset_scheduled(sl_ios_id_t);

void	 mds_brepls_check(uint8_t *, int);

/* replication state walking flags */
#define REPL_WALKF_SCIRCUIT	(1 << 0)	/* short circuit on return value set */
#define REPL_WALKF_MODOTH	(1 << 1)	/* modify everyone except specified IOS */

/*
 * The replication busy table contains slots gauging bandwidth
 * availability and activity to allow quick lookups of communication
 * status between arbitrary IONs.  Each resm has a unique busyid:
 *
 *			IONs				   busytable
 *	     A    B    C    D    E    F    G		#IONs | off (sz=6)
 *	  +----+----+----+----+----+----+----+		------+-----------
 *	A |    |  0 |  1 |  3 |  6 | 10 | 15 |		    0 |  0
 *	  +----+----+----+----+----+----+----+		    1 |  6
 *	B |    |    |  2 |  4 |  7 | 11 | 16 |		    2 | 11
 *	  +----+----+----+----+----+----+----+		    3 | 15
 *  I	C |    |    |    |  5 |  8 | 12 | 17 |		    4 | 18
 *  O	  +----+----+----+----+----+----+----+		    5 | 20
 *  N	D |    |    |    |    |  9 | 13 | 18 |		------+-----------
 *  s	  +----+----+----+----+----+----+----+		    n | n*(n+1)/2
 *	E |    |    |    |    |    | 14 | 19 |
 *	  +----+----+----+----+----+----+----+
 *	F |    |    |    |    |    |    | 20 |
 *	  +----+----+----+----+----+----+----+
 *	G |    |    |    |    |    |    |    |
 *	  +----+----+----+----+----+----+----+
 *
 * For checking if communication exists between resources with busyid
 * `min' and `max', we test the bit:
 *
 *	(max - 1) * (max) / 2 + min
 */
#define MDS_REPL_BUSYNODES(min, max)	(((max) - 1) * (max) / 2 + (min))

#define mds_repl_bmap_walk_all(b, t, r, fl)				\
	_mds_repl_bmap_walk((b), (t), (r), (fl), NULL, 0, NULL, NULL)

/* walk the bmap replica bitmap, iv and ni specify the IOS index array and its size */
#define mds_repl_bmap_walk(b, t, r, fl, iv, ni)				\
	_mds_repl_bmap_walk((b), (t), (r), (fl), (iv), (ni), NULL, NULL)

#define mds_repl_bmap_walkcb(b, t, r, fl, cbf, arg)			\
	_mds_repl_bmap_walk((b), (t), (r), (fl), NULL, 0, (cbf), (arg))

#define mds_repl_bmap_apply(bcm, tract, retifset, off)			\
	_mds_repl_bmap_apply((bcm), (tract), (retifset), 0, (off), NULL, NULL, NULL)

#define mds_repl_nodes_clearbusy(a, b)		 mds_repl_nodes_adjbusy((a), (b), INT64_MIN)

#define mds_repl_ios_lookup_add(ih, iosid, log)	_mds_repl_ios_lookup((ih), (iosid), 1, (log))
#define mds_repl_ios_lookup(ih, iosid)		_mds_repl_ios_lookup((ih), (iosid), 0, 0)

extern struct psc_listcache	 slm_replst_workq;

extern struct slm_resmlink	*repl_busytable;
extern psc_spinlock_t		 repl_busytable_lock;

#endif /* _SL_MDS_REPL_H_ */
