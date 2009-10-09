/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#include "inode.h"

/* for retrieving info about replication status */
struct msctlmsg_replst {
	char			mrs_fn[PATH_MAX];
	uint32_t		mrs_bact;
	uint32_t		mrs_bold;
};

struct msctl_replstq {
	struct psclist_head	mrsq_lentry;
	struct psc_listcache	mrsq_lc;
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
	sl_blkno_t		mrq_bmapno;
};

#define REPLRQ_FID_ALL		FID_ANY
#define REPLRQ_BMAPNO_ALL	(-1)

/* custom mount_slash message types */
#define SCMT_ADDREPLRQ		(NPCMT + 0)
#define SCMT_DELREPLRQ		(NPCMT + 1)
#define SCMT_GETREPLST		(NPCMT + 2)

extern struct psc_lockedlist	msctl_replsts;
