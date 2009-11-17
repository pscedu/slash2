/* $Id$ */

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "bmpc.h"

/*
 * msbmap_crcrepl_states - must be the same as bh_crcstates and bh_repls
 *  in slash_bmap_inode_od.
 */
struct msbmap_crcrepl_states {
	uint8_t	msbcr_crcstates[SL_CRCS_PER_BMAP]; /* crc descriptor bits  */
	uint8_t	msbcr_repls[SL_REPLICA_NBYTES];  /* replica bit map        */
};

/*
 * bmap_cli_data - assigned to bmap->bcm_pri for mount slash client.
 */
struct bmap_cli_info {
	struct bmap_pagecache		 msbd_bmpc;
	struct bmapc_memb		*msbd_bmap;
	lnet_nid_t			 msbd_ion;
	struct msbmap_crcrepl_states	 msbd_msbcr;
	struct srt_bmapdesc_buf		 msbd_bdb;	/* open bmap descriptor */
	struct psclist_head		 msbd_lentry;
};

#define bmap_2_msbd(b)			((struct bmap_cli_info *)(b)->bcm_pri)
#define bmap_2_msbmpc(b)		&(bmap_2_msbd(b)->msbd_bmpc)
#define bmap_2_msion(b)			bmap_2_msbd(b)->msbd_ion

/* bmap client modes */
#define BMAP_CLI_MCIP			(_BMAP_FLSHFT << 0)	/* mode change in progress */
#define	BMAP_CLI_MCC			(_BMAP_FLSHFT << 1)	/* mode change compete */

#endif /* _SLASH_CLI_BMAP_H_ */
