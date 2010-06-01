/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Interface for controlling live operation of a mount_slash instance.
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

/* mount_slash message types */
#define MSCMT_ADDREPLRQ		(NPCMT + 0)
#define MSCMT_DELREPLRQ		(NPCMT + 1)
#define MSCMT_GETREPLST		(NPCMT + 2)
#define MSCMT_GETREPLST_SLAVE	(NPCMT + 3)
#define MSCMT_SET_BMAPREPLPOL	(NPCMT + 4)
#define MSCMT_SET_NEWREPLPOL	(NPCMT + 5)
#define MSCMT_GETCONNS		(NPCMT + 6)

/* mount_slash control commands */
#define MSCC_EXIT	0
#define MSCC_FIDCACHE	1
#define MSCC_RECONFIG	2

#endif /* _SL_CTL_CLI_H_ */
