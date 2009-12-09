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
	struct slash_fidgen	mrs_fg;		/* used intermittenly */
	char			mrs_iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrs_nios;
	uint32_t		mrs_nbmaps;	/* accounting for # of slaves */
	uint32_t		mrs_id;		/* user-provided identifer */
	uint32_t		mrs_newreplpol;	/* default replication policy */
};

struct msctlmsg_replst_slave {
	uint32_t		mrsl_id;	/* user-provided identifer */
	uint32_t		mrsl_boff;	/* bmap starting offset */
	uint32_t		mrsl_nbmaps;	/* # of bmaps in this chunk */
	char			mrsl_data[0];	/* bh_repls data */
};

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	char			mrq_fn[PATH_MAX];
	char			mrq_iosv[SL_MAX_REPLICAS][SITE_NAME_MAX];
	uint32_t		mrq_nios;
	sl_bmapno_t		mrq_bmapno;
};

struct msctlmsg_fncmd_newreplpol {
	char			mfnrp_fn[PATH_MAX];
	int32_t			mfnrp_pol;
};

struct msctlmsg_fncmd_bmapreplpol {
	char			mfbrp_fn[PATH_MAX];
	sl_bmapno_t		mfbrp_bmapno;
	int32_t			mfbrp_pol;
};

#define REPLRQ_BMAPNO_ALL	(-1)

/* custom mount_slash message types */
#define MSCMT_ADDREPLRQ		(NPCMT + 0)
#define MSCMT_DELREPLRQ		(NPCMT + 1)
#define MSCMT_GETREPLST		(NPCMT + 2)
#define MSCMT_GETREPLST_SLAVE	(NPCMT + 3)
#define MSCMT_SET_BMAPREPLPOL	(NPCMT + 4)
#define MSCMT_SET_NEWREPLPOL	(NPCMT + 5)

/* mount_slash control commands */
#define MSCC_EXIT	0
#define MSCC_RECONFIG	1

#endif /* _SL_CTL_CLI_H_ */
