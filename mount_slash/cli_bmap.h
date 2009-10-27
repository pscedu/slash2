/* $Id$ */

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
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
 * bmap_cli_data - assigned to bmap->bcm_pri for mount slash client.
 */
struct bmap_cli_data {
	struct offtree_root          *msbd_oftr;
	struct bmapc_memb           *msbd_bmap;
	lnet_nid_t		     msbd_ion;
	struct msbmap_crcrepl_states msbd_msbcr;
	struct srt_bmapdesc_buf	     msbd_bdb;	/* open bmap descriptor */
	struct psclist_head          msbd_oftrqs;
	struct psclist_head          msbd_lentry;
};

#define bmap_2_msbd(b)		((struct bmap_cli_data *)(b)->bcm_pri)
#define bmap_2_msoftr(b)        bmap_2_msbd(b)->msbd_oftr
#define bmap_2_msion(b)		bmap_2_msbd(b)->msbd_ion


/*
 * bmap_info_cli - private client data for struct sl_resm.
 *  It's tasked with holding the import to the correct ION.
 */
struct bmap_info_cli {
	struct pscrpc_import *bmic_import;
	struct timespec       bmic_connect_time;
	struct psc_waitq      bmic_waitq;
	psc_spinlock_t        bmic_lock;
	int                   bmic_flags;
};

struct resprof_cli_info {
	int rci_cnt;
	psc_spinlock_t rci_lock;
};

enum {
	BMIC_CONNECTING   = (1<<0),
	BMIC_CONNECTED    = (1<<1),
	BMIC_CONNECT_FAIL = (1<<2)
};

/* bcm_mode flags */
//#define	BMAP_CLI_MCIP	(1 << (0 + BMAP_RSVRD_MODES)) /* mode change in prog */
//#define	BMAP_CLI_MCC	(1 << (1 + BMAP_RSVRD_MODES)) /* mode change complete */

#define BMAP_CLI_MCIP (1 << 16)
#define	BMAP_CLI_MCC  (1 << 17)

void bmap_flush_init(void);

#endif /* _SLASH_CLI_BMAP_H_ */
