/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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
	int32_t			scc_stkvers;
	int64_t			scc_uptime;
};

#define CSVCF_CTL_OLDER		CSVCF_FLAGSHIFT
#define CSVCF_CTL_NEWER		(CSVCF_FLAGSHIFT << 1)

#define SLCTL_REST_CLI		0

struct slctlmsg_fcmh {
	struct sl_fidgen	scf_fg;		/* identity of the file */
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

/* fcmh classes, tucked in scf_fg.fg_gen */
#define SLCTL_FCL_ALL		0	/* everything */
#define SLCTL_FCL_BUSY		1	/* only FCMH_BUSY */

struct slctlmsg_bmap {
	struct sl_fidgen	scb_fg;
	uint32_t		scb_bno;
	sl_bmapgen_t		scb_bgen;

	uint32_t		scb_flags;
	 int32_t		scb_opcnt;

	/* lease (client) */
	uint64_t		scb_seq;
	uint64_t		scb_key;
	char			scb_resname[RES_NAME_MAX];
	uint64_t		scb_addr;
};

#endif /* _SL_CTL_H_ */
