/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "fid.h"
#include "slconfig.h"

/* for retrieving info about replication status */
struct msctlmsg_replst {
	slfid_t			mrs_fid;
	struct slash_fidgen	mrs_fg;		/* used intermittenly */
	char			mrs_iosv[SITE_NAME_MAX][SL_MAX_REPLICAS];
	uint32_t		mrs_nios;
	uint32_t		mrs_nbmaps;	/* accounting for # of slaves */
	uint32_t		mrs_newreplpol;	/* default replication policy */
};

struct msctlmsg_replst_slave {
	slfid_t			mrsl_fid;
	uint32_t		mrsl_boff;	/* bmap starting offset */
	uint32_t		mrsl_nbmaps;	/* # of bmaps in this chunk */
	char			mrsl_data[0];	/* bcs_repls data */
};

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	slfid_t			mrq_fid;
	char			mrq_iosv[SITE_NAME_MAX][SL_MAX_REPLICAS];
	uint32_t		mrq_nios;
	sl_bmapno_t		mrq_bmapno;
};

struct msctlmsg_newreplpol {
	slfid_t			mfnrp_fid;
	int32_t			mfnrp_pol;
};

struct msctlmsg_bmapreplpol {
	slfid_t			mfbrp_fid;
	sl_bmapno_t		mfbrp_bmapno;
	sl_bmapno_t		mfbrp_nbmaps;
	int32_t			mfbrp_pol;
};

struct msctlmsg_fncmd {
	slfid_t			mfc_fid;
};

#define REPLRQ_BMAPNO_ALL	(-1)

/* mount_slash message types */
#define MSCMT_ADDREPLRQ		NPCMT
#define MSCMT_DELREPLRQ		(NPCMT +  1)
#define MSCMT_GETCONNS		(NPCMT +  2)
#define MSCMT_GETFCMH		(NPCMT +  3)
#define MSCMT_GETREPLST		(NPCMT +  4)
#define MSCMT_GETREPLST_SLAVE	(NPCMT +  5)
#define MSCMT_GET_BMAPREPLPOL	(NPCMT +  6)
#define MSCMT_GET_NEWREPLPOL	(NPCMT +  7)
#define MSCMT_IMPORT		(NPCMT +  8)
#define MSCMT_LCACHE_ADD	(NPCMT +  9)
#define MSCMT_LCACHE_REMOVE	(NPCMT + 10)
#define MSCMT_LCACHE_STATUS	(NPCMT + 11)
#define MSCMT_SET_BMAPREPLPOL	(NPCMT + 12)
#define MSCMT_SET_NEWREPLPOL	(NPCMT + 13)

#define SLASH_FSID		0x51a54

#endif /* _SL_CTL_CLI_H_ */
