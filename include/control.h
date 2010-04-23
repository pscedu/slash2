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

#ifndef _SL_CONTROL_H_
#define _SL_CONTROL_H_

#include "slconfig.h"

struct slctlmsg_conn {
	char		scc_resmaddr[PSC_ALIGN(RESM_ADDRBUF_SZ, 4)];
	int32_t		scc_type;	/* client is 0 */
	int32_t		scc_refcnt;
	int32_t		scc_cflags;	/* CSVCF_* */
	int32_t		scc_flags;
};

/* scc_flags */
#define SCCF_ONLINE	(1 << 0)

#endif /* _SL_CONTROL_H_ */
