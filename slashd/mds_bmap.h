/* $Id$ */

#ifndef _SLASHD_MDS_BMAP_H_
#define _SLASHD_MDS_BMAP_H_

#include "psc_ds/tree.h"

#include "jflush.h"

struct mexpbcm;
struct mexpfcm;

/* bcm_mode flags */
#define	BMAP_MDS_WR	(1 << 0)
#define	BMAP_MDS_RD	(1 << 1)
#define	BMAP_MDS_DIO	(1 << 2)	/* directio */
#define	BMAP_MDS_FAILED	(1 << 3)	/* crc failure */
#define	BMAP_MDS_EMPTY	(1 << 4)	/* new bmap, not yet committed to disk */
#define	BMAP_MDS_CRC_UP	(1 << 5)	/* crc update in progress */
#define	BMAP_MDS_INIT	(1 << 6)

SPLAY_HEAD(bmap_exports, mexpbcm);

/*
 * bmap_mds_info - the bcm_pri data structure for the slash2 mds.
 *   Bmap_mds_info holds all bmap specific context for the mds which
 *   includes the journal handle, ref counts for client readers and writers
 *   a point to our ION, a tree of our client's exports, a pointer to the
 *   on-disk structure, a receipt for the odtable, and a reqset for issuing
 *   callbacks (XXX is that really needed?).
 * Notes: both read and write clients are stored to bmdsi_exports, the ref
 *   counts are used to determine the number of both and hence the caching
 *   mode used at the clients.   Bmdsi_wr_ion is a shortcut pointer used
 *   only when the bmap has client writers - all writers (and readers) are
 *   directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
	uint32_t			 bmdsi_xid;	/* last op recv'd from ION */
	struct jflush_item		 bmdsi_jfi;	/* journal handle          */
	atomic_t			 bmdsi_rd_ref;	/* reader clients          */
	atomic_t			 bmdsi_wr_ref;	/* writer clients          */
	struct mexp_ion			*bmdsi_wr_ion;	/* pointer to write ION    */
	struct bmap_exports		 bmdsi_exports;	/* tree of client exports  */
	struct slash_bmap_od		*bmdsi_od;	/* od-disk pointer         */
	struct odtable_receipt		*bmdsi_assign;	/* odtable receipt         */
	struct pscrpc_request_set	*bmdsi_reqset;	/* cache callback rpc's    */
};

static inline void
bmap_mds_info_init(struct bmap_mds_info *bmdsi)
{
	jfi_init(&bmdsi->bmdsi_jfi);
	bmdsi->bmdsi_xid = 0;
	atomic_set(&bmdsi->bmdsi_rd_ref, 0);
	atomic_set(&bmdsi->bmdsi_wr_ref, 0);
}

#define bmap_2_bmdsi(b)		((struct bmap_mds_info *)(b)->bcm_pri)
#define bmap_2_bmdsiod(b)	bmap_2_bmdsi(b)->bmdsi_od
#define bmap_2_bmdsjfi(b)	(&bmap_2_bmdsi(b)->bmdsi_jfi)
#define bmap_2_bmdsassign(b)	bmap_2_bmdsi(b)->bmdsi_assign

int  mds_bmap_crc_write(struct srm_bmap_crcup *, lnet_nid_t);
int  mds_bmap_load(struct mexpfcm *, struct srm_bmap_req *, struct bmapc_memb **);

#endif /* _SLASHD_MDS_BMAP_H_ */
