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
 * Interface for controlling live operation of slashd.
 */

#include "slashrpc.h"

struct slmctlmsg_replpair {
	char			scrp_addrbuf[2][RESM_ADDRBUF_SZ];
	uint32_t		scrp_avail;
	uint32_t		scrp_used;
};

struct slmctlmsg_statfs {
	char			scsf_resname[RES_NAME_MAX];
	struct srt_statfs	scsf_ssfb;
};

/* bmap lease */
struct slmctlmsg_bml {
	struct slash_fidgen	scbl_fid;
	sl_bmapno_t		scbl_bno;
	sl_bmapgen_t		scbl_bgen;
	uint64_t		scbl_seq;
	uint64_t		scbl_cli_nid;
	uint32_t		scbl_cli_pid;
	sl_ios_id_t		scbl_iosid;
	uint32_t		scbl_flags;
	uint32_t		scbl_ndups;
	uint64_t		scbl_start;
	uint64_t		scbl_expire;
};

/* slrmcthr stats */
#define pcst_nopen		pcst_u32_1
#define pcst_nstat		pcst_u32_2
#define pcst_nclose		pcst_u32_3

/* slashd message types */
#define SLMCMT_GETBML		(NPCMT + 0)
#define SLMCMT_GETCONNS		(NPCMT + 1)
#define SLMCMT_GETFCMHS		(NPCMT + 2)
#define SLMCMT_GETREPLPAIRS	(NPCMT + 3)
#define SLMCMT_GETSTATFS	(NPCMT + 4)
#define SLMCMT_STOP		(NPCMT + 5)
