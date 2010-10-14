/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2010, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"

#include "ctl.h"
#include "ctlcli.h"
#include "pathnames.h"

#include "slashd/ctl_mds.h"

void
slmrmcthr_st_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" #open %8d #close %8d #stat %8d",
	    pcst->pcst_nopen, pcst->pcst_nclose, pcst->pcst_nstat);
}

void
slmrmmthr_st_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" #open %8d", pcst->pcst_nopen);
}

void
packshow_conns(__unusedx const char *thr)
{
	psc_ctlmsg_push(SLMCMT_GETCONNS, sizeof(struct slctlmsg_conn));
}

void
packshow_files(__unusedx const char *thr)
{
	struct slctlmsg_file *scf;

	scf = psc_ctlmsg_push(SLMCMT_GETFILES, sizeof(struct slctlmsg_file));
	scf->scf_fg.fg_fid = FID_ANY;
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "connections",	packshow_conns },
	{ "fidcache",		packshow_files },
	{ "files",		packshow_files },
	{ "loglevels",		psc_ctl_packshow_loglevel },
	{ "odtables",		psc_ctl_packshow_odtables },
	{ "stats",		psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ sl_conn_prhdr,	sl_conn_prdat,		sizeof(struct slctlmsg_conn),		NULL },
	{ sl_file_prhdr,	sl_file_prdat,		sizeof(struct slctlmsg_file),		NULL }
};
int psc_ctlmsg_nprfmts = nitems(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
/* CTL		*/ { psc_ctlthr_prdat },
/* ACSVC	*/ { NULL },
/* RMC		*/ { slmrmcthr_st_prdat },
/* RMI		*/ { NULL },
/* RMM		*/ { slmrmmthr_st_prdat }
};
int psc_ctl_nthrstatfmts = nitems(psc_ctl_thrstatfmts);

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
	{ "exit",	SMCC_EXIT }
};
int psc_ctlcmd_nreqs = nitems(psc_ctlcmd_reqs);

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HI] [-c cmd] [-h table] [-i iostat] [-L listspec]\n"
	    "\t[-m meter] [-P pool] [-p param[=value]] [-S socket] [-s value]\n",
	    progname);
	exit(1);
}

struct psc_ctlopt opts[] = {
	{ 'c', PCOF_FUNC, psc_ctlparse_cmd },
	{ 'H', PCOF_FLAG, &psc_ctl_noheader },
	{ 'h', PCOF_FUNC, psc_ctlparse_hashtable },
	{ 'I', PCOF_FLAG, &psc_ctl_inhuman },
	{ 'i', PCOF_FUNC, psc_ctlparse_iostats },
	{ 'L', PCOF_FUNC, psc_ctlparse_lc },
	{ 'm', PCOF_FUNC, psc_ctlparse_meter },
	{ 'P', PCOF_FUNC, psc_ctlparse_pool },
	{ 'p', PCOF_FUNC, psc_ctlparse_param },
	{ 's', PCOF_FUNC, psc_ctlparse_show }
};

int
main(int argc, char *argv[])
{
	pfl_init();
	progname = argv[0];
	psc_ctlcli_main(SL_PATH_SLMCTLSOCK, argc, argv, opts,
	    nitems(opts));
	exit(0);
}
