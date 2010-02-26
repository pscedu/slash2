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

#include "psc_ds/tree.h"

#include "jflush.h"
#include "bmap.h"
#include "mdslog.h"

struct mexpbcm;
struct mexpfcm;

SPLAY_HEAD(bmap_exports, mexpbcm);

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
	struct jflush_item		 bmdsi_jfi;	/* journal handle          */
	struct resm_mds_info		*bmdsi_wr_ion;	/* pointer to write ION    */
	struct bmap_exports		 bmdsi_exports;	/* tree of client exports  */
	struct odtable_receipt		*bmdsi_assign;	/* odtable receipt         */
	struct pscrpc_request_set	*bmdsi_reqset;	/* cache callback RPC's    */
	uint32_t			 bmdsi_xid;	/* last op recv'd from ION */
	int				 bmdsi_flags;
};

/* bmap MDS modes */
#define BMAP_MDS_CRC_UP		(_BMAP_FLSHFT << 0)	/* CRC update in progress */
#define BMAP_MDS_CRCWRT		(_BMAP_FLSHFT << 1)
#define BMAP_MDS_NOION		(_BMAP_FLSHFT << 2)

/* bmap_mds_info modes */
#define BMIM_LOGCHG		(1 << 0)		/* in-mem change made, needs logged */

/*
 * bmi_assign - the structure used for tracking the mds's bmap/ion
 *   assignments.  These structures are stored in a odtable.
 */
struct bmi_assign {
	lnet_nid_t		bmi_ion_nid;
	sl_ios_id_t		bmi_ios;
//	struct slash_fidgen	bmi_fid;
	slfid_t			bmi_fid;
	sl_blkno_t		bmi_bmapno;
	time_t			bmi_start;
};

#define bmap_2_bmdsi(b)		((struct bmap_mds_info *)(b)->bcm_pri)
#define bmap_2_bmdsjfi(b)	(&bmap_2_bmdsi(b)->bmdsi_jfi)
#define bmap_2_bmdsassign(b)	bmap_2_bmdsi(b)->bmdsi_assign
#define bmap_2_bgen(b)		(b)->bcm_od->bh_gen

int	mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);
int	mds_bmap_exists(struct fidc_membh *, sl_blkno_t);
int	mds_bmap_load(struct fidc_membh *, sl_blkno_t, struct bmapc_memb **);
int	mds_bmap_load_cli(struct mexpfcm *, const struct srm_bmap_req *, struct bmapc_memb **);
int	mds_bmap_load_ion(const struct slash_fidgen *, sl_blkno_t, struct bmapc_memb **);
int	mds_bmap_loadvalid(struct fidc_membh *, sl_blkno_t, struct bmapc_memb **);
void	mds_bmap_ref_drop(struct bmapc_memb *, int);
void	mds_bmap_sync_if_changed(struct bmapc_memb *);

static __inline void
bmap_dio_sanity_locked(struct bmapc_memb *bmap, int dio_check)
{
	BMAP_LOCK_ENSURE(bmap);

	psc_assert(atomic_read(&bmap->bcm_wr_ref) >= 0);
	psc_assert(atomic_read(&bmap->bcm_rd_ref) >= 0);

	if (dio_check &&
	    ((atomic_read(&bmap->bcm_wr_ref) > 1) ||
	     (atomic_read(&bmap->bcm_wr_ref) &&
	      atomic_read(&bmap->bcm_rd_ref))))
		psc_assert(bmap->bcm_mode & BMAP_DIO);
}

#endif /* _SLASHD_MDS_BMAP_H_ */
