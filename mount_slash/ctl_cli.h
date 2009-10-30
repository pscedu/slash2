/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

#ifndef _SL_CTL_CLI_H_
#define _SL_CTL_CLI_H_

#include "inode.h"
#include "slconfig.h"

/* for retrieving info about replication status */
struct msctlmsg_replst {
	char			mrs_fn[PATH_MAX];
	uint64_t		mrs_fid;	/* used intermittenly */
	char			mrs_iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrs_nios;
	uint32_t		mrs_nbmaps;	/* accounting for # of slaves */
	uint32_t		mrs_id;		/* user-provided identifer */
};

struct msctlmsg_replst_slave {
	uint32_t		mrsl_id;	/* user-provided identifer */
	uint32_t		mrsl_boff;	/* bmap starting offset */
	uint32_t		mrsl_len;	/* max: SRM_REPLST_PAGESIZ */
	char			mrsl_data[0];	/* bmap replica bits */
};

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	char			mrq_fn[PATH_MAX];
	char			mrq_iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrq_nios;
	sl_bmapno_t		mrq_bmapno;
};

#define REPLRQ_FID_ALL		FID_ANY
#define REPLRQ_BMAPNO_ALL	(-1)

/* custom mount_slash message types */
#define SCMT_ADDREPLRQ		(NPCMT + 0)
#define SCMT_DELREPLRQ		(NPCMT + 1)
#define SCMT_GETREPLST		(NPCMT + 2)
#define SCMT_GETREPLST_SLAVE	(NPCMT + 3)

#endif /* _SL_CTL_CLI_H_ */
