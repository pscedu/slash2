/* $Id$ */

#ifndef _SL_CTLSVR_CLI_H_
#define _SL_CTLSVR_CLI_H_

#include "psc_ds/list.h"
#include "psc_ds/lockedlist.h"
#include "psc_ds/pool.h"
#include "psc_util/completion.h"

#include "mount_slash/ctl_cli.h"

struct msctl_replstq {
	struct psclist_head		 mrsq_lentry;
	struct psc_completion		 mrsq_compl;	/* notification when all masters arrive */
	struct psc_lockedlist		 mrsq_mrcs;	/* msctl_replst_cont */
	int32_t				 mrsq_id;	/* user-provided identifer */
};

/* in-memory container for a replst_slave msg */
struct msctl_replst_slave_cont {
	struct psclist_head		 mrsc_lentry;
	uint32_t			 mrsc_len;	/* max: SRM_REPLST_PAGESIZ */

	/* must be at end, memory is allocated past */
	struct msctlmsg_replst_slave	 mrsc_mrsl;
};

/* in-memory container for a replst msg */
struct msctl_replst_cont {
	struct psclist_head		 mrc_lentry;
	struct psc_completion		 mrc_compl;	/* notification when all slaves arrive */
	struct psc_lockedlist		 mrc_bdata;	/* msctl_replst_slave_cont */
	struct msctlmsg_replst		 mrc_mrs;
};

extern struct psc_lockedlist		 msctl_replsts;
extern struct psc_poolmgr		*msctl_replstmc_pool;
extern struct psc_poolmgr		*msctl_replstsc_pool;

#endif /* _SLCLI_CTLSVR_H_ */
