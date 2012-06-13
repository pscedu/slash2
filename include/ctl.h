/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SL_CTL_H_
#define _SL_CTL_H_

#include "fid.h"
#include "slconfig.h"

struct slctlmsg_conn {
	char			scc_addrbuf[RESM_ADDRBUF_SZ];
	int32_t			scc_type;	/* SLREST_* or SLCTL_REST_* */
	int32_t			scc_refcnt;
	int32_t			scc_flags;	/* CSVCF_* */
	int32_t			scc_txcr;
	uint32_t		scc_stkvers;
};

#define SLCTL_REST_CLI		0

struct slctlmsg_fcmh {
	struct slash_fidgen	scf_fg;		/* identity of the file */
	uint64_t		scf_size;
	int64_t			scf_blksize;
	int32_t			scf_ptruncgen;
	int32_t			scf_utimgen;
	int32_t			scf_st_mode;
	int32_t			scf_uid;
	int32_t			scf_gid;
	int32_t			scf_flags;	/* FCMH_* flags */
	int32_t			scf_refcnt;
};

#endif /* _SL_CTL_H_ */
