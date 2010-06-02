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
	char			scc_addrbuf[PSC_ALIGN(RESM_ADDRBUF_SZ, 4)];
	int32_t			scc_type;	/* client is 0 */
	int32_t			scc_refcnt;
	int32_t			scc_cflags;	/* CSVCF_* */
	int32_t			scc_flags;
};

/* scc_flags */
#define SCCF_ONLINE		(1 << 0)

struct slctlmsg_file {
	struct slash_fidgen	scf_fg;		/* identity of the file */
	int64_t			scf_age;
	int64_t			scf_gen;
	int32_t			scf_ptruncgen;
	int32_t			scf_st_mode;
	int32_t			scf_state;
	int32_t			scf_refcnt;
};

void sl_conn_prhdr(struct psc_ctlmsghdr *, const void *);
void sl_conn_prdat(const struct psc_ctlmsghdr *, const void *);

#endif /* _SL_CONTROL_H_ */
