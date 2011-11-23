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

struct slmctlmsg_replpair {
	char			scrp_addrbuf[2][PSC_ALIGN(RESM_ADDRBUF_SZ, 8)];
	uint32_t		scrp_avail;
	uint32_t		scrp_used;
};

/* slrmcthr stats */
#define pcst_nopen		pcst_u32_1
#define pcst_nstat		pcst_u32_2
#define pcst_nclose		pcst_u32_3

/* slashd message types */
#define SLMCMT_GETCONNS		(NPCMT + 0)
#define SLMCMT_GETFCMHS		(NPCMT + 1)
#define SLMCMT_GETREPLPAIRS	(NPCMT + 2)
#define SLMCMT_STOP		(NPCMT + 3)
