/* $Id$ */

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"

#include "inode.h"
#include "bmap.h"
#include "offtree.h"

/* 
 * msbmap_crcrepl_states - must be the same as bh_crcstates and bh_repls
 *  in slash_bmap_inode_od.
 */ 
struct msbmap_crcrepl_states
{
	u8 msbcr_crcstates[SL_CRCS_PER_BMAP]; /* crc descriptor bits  */
        u8 msbcr_repls[SL_REPLICA_NBYTES];  /* replica bit map        */
};

/*
 * msbmap_data - assigned to bmap->bcm_pri for mount slash client.
 */
struct msbmap_data {
	struct offtree_root	    *msbd_oftr;
	lnet_nid_t		     msbd_ion;
	struct msbmap_crcrepl_states msbd_msbcr;
	struct srt_bmapdesc_buf	     msbd_bdb;	/* open bmap descriptor */
	struct psclist_head          msbd_oftrqs;
};

#define bmap_2_msbd(b)				\
	((struct msbmap_data *)((b)->bcm_pri))

#define bmap_2_msoftr(b)					\
	(((struct msbmap_data *)((b)->bcm_pri))->msbd_oftr)

#define bmap_2_msion(b)						\
	((struct msbmap_data *)((b)->bcm_pri))->msbd_ion

static inline void
bmap_oftrq_add(struct bmapc_memb *b, struct offtree_req *r)
{
	BMAP_LOCK(b);	
        psclist_xadd(&r->oftrq_lentry, &bmap_2_msbd(b)->msbd_oftrqs);
        BMAP_ULOCK(b);
}
 
static inline void
bmap_oftrq_del(struct bmapc_memb *b, struct offtree_req *r)
{
	BMAP_LOCK(b);
	psclist_del(&r->oftrq_lentry);

	DEBUG_BMAP(PLL_INFO, b, "list_empty(%d)", 
		   psclist_empty(&bmap_2_msbd(b)->msbd_oftrqs));

        psc_waitq_wakeall(&b->bcm_waitq);
        BMAP_ULOCK(b);
}

/*
 * bmap_info_cli - hangs from the void * pointer in the sl_resm_t struct.
 *  It's tasked with holding the import to the correct ION.
 */
struct bmap_info_cli {
	struct pscrpc_import *bmic_import;
};

/* bcm_mode flags */
#define	BMAP_CLI_MCIP	(1 << (0 + BMAP_RSVRD_MODES)) /* mode change in prog */
#define	BMAP_CLI_MCC	(1 << (1 + BMAP_RSVRD_MODES)) /* mode change complete */

#endif /* _SLASH_CLI_BMAP_H_ */
