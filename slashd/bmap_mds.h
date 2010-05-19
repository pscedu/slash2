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

#ifndef _SLASHD_MDS_BMAP_H_
#define _SLASHD_MDS_BMAP_H_

#include <sys/time.h>

#include "psc_ds/lockedlist.h"
#include "psc_util/odtable.h"

#include "bmap.h"
#include "jflush.h"
#include "mdslog.h"
#include "slashd.h"

/*
 * bmap_mds_info - the bcm_pri data structure for the slash2 mds.
 *   bmap_mds_info holds all bmap specific context for the mds which
 *   includes the journal handle, ref counts for client readers and writers
 *   a point to our ION, a tree of our client's exports, a pointer to the
 *   on-disk structure, a receipt for the odtable, and a reqset for issuing
 *   callbacks (XXX is that really needed?).
 * Notes: both read and write clients are stored to bmdsi_exports, the ref
 *   counts are used to determine the number of both and hence the caching
 *   mode used at the clients.   bmdsi_wr_ion is a shortcut pointer used
 *   only when the bmap has client writers - all writers (and readers) are
 *   directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
	struct bmapc_memb		*bmdsi_bmap;    /* back pointer            */
	struct jflush_item		 bmdsi_jfi;	/* journal handle          */
	struct resm_mds_info		*bmdsi_wr_ion;	/* pointer to write ION    */
	struct odtable_receipt		*bmdsi_assign;	/* odtable receipt         */
	struct psc_lockedlist		 bmdsi_leases;  /* tracked bmap leases     */
	uint64_t			 bmdsi_seq;     /* Largest write bml seq # */
	uint32_t			 bmdsi_xid;	/* last op recv'd from ION */
	uint32_t			 bmdsi_writers;
	int				 bmdsi_flags;
};

/* bmap MDS modes */
#define BMAP_MDS_CRC_UP		(_BMAP_FLSHFT << 0)	/* CRC update in progress */
#define BMAP_MDS_CRCWRT		(_BMAP_FLSHFT << 1)
#define BMAP_MDS_NOION		(_BMAP_FLSHFT << 2)

/* bmap_mds_info modes */
#define BMIM_LOGCHG		(1 << 0)	/* in-mem change made, needs logged */
#define BMIM_DIO		(1 << 1)	/* directio enabled                 */

struct bmap_timeo_entry {
	uint64_t		 bte_maxseq;
	struct psclist_head	 bte_bmaps;
};

struct bmap_timeo_table {
	psc_spinlock_t		 btt_lock;
	uint64_t		 btt_maxseq;
	uint64_t		 btt_minseq;
	struct bmap_timeo_entry	*btt_entries;
	struct jflush_item	 btt_jfi;
	int			 btt_nentries;
};

#define BTE_ADD			0
#define BTE_DEL			1

#define BMAP_TIMEO_TBL_SZ	20
#define BMAP_TIMEO_MAX		120 //Seconds
#define BMAP_SEQLOG_FACTOR	100

struct bmap_mds_lease {
	uint64_t		  bml_seq;
	uint64_t		  bml_key;
	uint32_t		  bml_flags;
	psc_spinlock_t		  bml_lock;
	struct bmap_mds_info	 *bml_bmdsi;
	struct pscrpc_export	 *bml_exp;
	struct psclist_head	  bml_bmdsi_lentry;
	struct psclist_head	  bml_timeo_lentry;
	struct psclist_head	  bml_exp_lentry;
	struct psclist_head	  bml_coh_lentry;
};

#define bml_2_bmap(b)		(b)->bml_bmdsi->bmdsi_bmap

#define BML_LOCK(b)		spinlock(&(b)->bml_lock)
#define BML_ULOCK(b)		freelock(&(b)->bml_lock)

enum {
	BML_READ   = (1 << 0),
	BML_WRITE  = (1 << 1),
	BML_CDIO   = (1 << 2),
	BML_COHRLS = (1 << 3),
	BML_EXP    = (1 << 4),
	BML_TIMEOQ = (1 << 5),
	BML_COH    = (1 << 6)
};

/*
 * bmi_assign - the structure used for tracking the mds's bmap/ion
 *   assignments.  These structures are stored in a odtable.
 */
struct bmi_assign {
	lnet_nid_t		bmi_ion_nid;
	sl_ios_id_t		bmi_ios;
	slfid_t			bmi_fid;
	uint64_t		bmi_seq;
	sl_bmapno_t		bmi_bmapno;
	time_t			bmi_start;
};

#define bmap_2_bmdsi(b)		((struct bmap_mds_info *)(b)->bcm_pri)
#define bmap_2_bmdsjfi(b)	(&bmap_2_bmdsi(b)->bmdsi_jfi)
#define bmap_2_bmdsassign(b)	bmap_2_bmdsi(b)->bmdsi_assign
#define bmap_2_bgen(b)		(b)->bcm_od->bh_gen

#define mds_bmap_load(f, n, bp)	bmap_get((f), (n), 0, (bp))

int	 mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);
int	 mds_bmap_exists(struct fidc_membh *, sl_bmapno_t);
int	 mds_bmap_load_cli(struct fidc_membh *, sl_bmapno_t, int, enum rw,
	    sl_ios_id_t, struct srt_bmapdesc *, struct pscrpc_export *,
	    struct bmapc_memb **);
int	 mds_bmap_load_ion(const struct slash_fidgen *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_loadvalid(struct fidc_membh *, sl_bmapno_t,
	    struct bmapc_memb **);
int	 mds_bmap_bml_release(struct bmapc_memb *, uint64_t, uint64_t);
void	 mds_bmap_ref_drop(struct bmapc_memb *, int);
void	 mds_bmap_sync_if_changed(struct bmapc_memb *);
void	 mds_bmi_odtable_startup_cb(void *, struct odtable_receipt *);

void	 mds_bmap_getcurseq(uint64_t *, uint64_t *);
uint64_t mds_bmap_timeotbl_getnextseq(void);
uint64_t mds_bmap_timeotbl_mdsi(struct bmap_mds_lease *, int);

extern struct psc_poolmaster	 bmapMdsLeasePoolMaster;
extern struct psc_poolmgr	*bmapMdsLeasePool;

#endif /* _SLASHD_MDS_BMAP_H_ */
