/* $Id$ */

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"

#include "offtree.h"

/*
 * msbmap_data - assigned to bmap->bcm_pri for mount slash client.
 */
struct msbmap_data {
	struct offtree_root	*msbd_oftr;
	struct slash_bmap_od	 msbd_bmapi;
	lnet_nid_t		 msbd_ion;
	struct srt_bmapdesc_buf	 msbd_bdb;	/* open bmap descriptor */
};

#define bmap_2_msoftr(b)					\
	(((struct msbmap_data *)((b)->bcm_pri))->msbd_oftr)

#define bmap_2_msion(b)						\
	((struct msbmap_data *)((b)->bcm_pri))->msbd_ion

/*
 * bmap_info_cli - hangs from the void * pointer in the sl_resm_t struct.
 *  It's tasked with holding the import to the correct ION.
 */
struct bmap_info_cli {
	struct pscrpc_import *bmic_import;
};

/* bcm_mode flags */
#define	BMAP_CLI_RD	(1 << 0)	/* bmap has read creds */
#define	BMAP_CLI_WR	(1 << 1)	/* write creds */
#define	BMAP_CLI_DIO	(1 << 2)	/* bmap is in dio mode */
#define	BMAP_CLI_MCIP	(1 << 3)	/* "mode change in progress" */
#define	BMAP_CLI_MCC	(1 << 4)	/* "mode change complete" */

#endif /* _SLASH_CLI_BMAP_H_ */
