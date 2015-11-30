/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

/*
 * Interface for controlling live operation of a sliod instance.
 */

#include "pfl/rpc.h"

#include "fid.h"
#include "slconfig.h"
#include "sltypes.h"

struct slictlmsg_replwkst {
	struct sl_fidgen	srws_fg;
	char			srws_peer_addr[RESM_ADDRBUF_SZ];
	sl_bmapno_t		srws_bmapno;
	uint32_t		srws_data_tot;
	uint32_t		srws_data_cur;
	 int32_t		srws_refcnt;
	/* XXX #inflight slivers? */
};

struct slictlmsg_fileop {
	char			sfop_fn[PATH_MAX];
	char			sfop_fn2[PATH_MAX];
	slfid_t			sfop_pfid;
	int32_t			sfop_flags;
};

struct slictlmsg_slvr {
	slfid_t			ss_fid;
	sl_bmapno_t		ss_bno;
	uint16_t		ss_slvrno;
	uint16_t		ss_flags;
	uint32_t		ss_refcnt;
	 int32_t		ss_err;
	struct pfl_timespec	ss_ts;
};

#define SLI_CTL_FOPF_RECURSIVE	(1 << 0)
#define SLI_CTL_FOPF_SYMBOLIC	(1 << 1)
#define SLI_CTL_FOPF_VERBOSE	(1 << 2)
#define SLI_CTL_FOPF_XREPL	(1 << 3)

/* sliod message types */
#define SLICMT_GET_REPLWKST	(NPCMT + 0)
#define SLICMT_GETCONN		(NPCMT + 1)
#define SLICMT_GETFCMH		(NPCMT + 2)
#define SLICMT_EXPORT		(NPCMT + 3)
#define SLICMT_IMPORT		(NPCMT + 4)
#define SLICMT_STOP		(NPCMT + 5)
#define SLICMT_GETBMAP		(NPCMT + 6)
#define SLICMT_GETSLVR		(NPCMT + 7)
