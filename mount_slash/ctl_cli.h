/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include "inode.h"
#include "slconfig.h"

/* for retrieving info about replication status */
struct msctlmsg_replst {
	char			mrs_fn[PATH_MAX];
	char			mrs_ios[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrs_nios;
	uint32_t		mrs_nbmaps;
	uint32_t		mrs_id;
};

struct msctlmsg_replst_slave {
	uint32_t		mrs_id;
	uint32_t		mrs_boff;
	char			mrs_data[0];
};

struct msctl_replstq {
	struct psclist_head	mrsq_lentry;
	char			mrsq_iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	struct psc_listcache	mrsq_lc;
	uint32_t		mrsq_nios;
	int32_t			mrsq_id;
};

/* in-memory container for a replst msg */
struct msctl_replst_cont {
	struct psclist_head	mrc_lentry;
	struct msctlmsg_replst	mrc_mrs;
};

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	char			mrq_fn[PATH_MAX];
	char			mrq_ios[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrq_nios;
	sl_blkno_t		mrq_bmapno;
};

#define REPLRQ_FID_ALL		FID_ANY
#define REPLRQ_BMAPNO_ALL	(-1)

/* custom mount_slash message types */
#define SCMT_ADDREPLRQ		(NPCMT + 0)
#define SCMT_DELREPLRQ		(NPCMT + 1)
#define SCMT_GETREPLST		(NPCMT + 2)
#define SCMT_GETREPLST_SLAVE	(NPCMT + 3)

extern struct psc_lockedlist	msctl_replsts;
