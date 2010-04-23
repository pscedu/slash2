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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"

#include "control.h"
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

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "connections",	sl_packshow_conn },
	{ "loglevels",		psc_ctl_packshow_loglevel },
	{ "stats",		psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS,
	{ sl_conn_prhdr,	sl_conn_prdat,	sizeof(struct slctlmsg_conn),		NULL }
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

int
main(int argc, char *argv[])
{
	const char *sockfn;
	int c;

	pfl_init();
	progname = argv[0];
	sockfn = SL_PATH_SLMCTLSOCK;
	while ((c = getopt(argc, argv, "c:Hh:Ii:L:m:P:p:S:s:")) != -1)
		switch (c) {
		case 'c':
			psc_ctlparse_cmd(optarg);
			break;
		case 'H':
			psc_ctl_noheader = 1;
			break;
		case 'h':
			psc_ctlparse_hashtable(optarg);
			break;
		case 'I':
			psc_ctl_inhuman = 1;
			break;
		case 'i':
			psc_ctlparse_iostats(optarg);
			break;
		case 'L':
			psc_ctlparse_lc(optarg);
			break;
		case 'm':
			psc_ctlparse_meter(optarg);
			break;
		case 'P':
			psc_ctlparse_pool(optarg);
			break;
		case 'p':
			psc_ctlparse_param(optarg);
			break;
		case 'S':
			sockfn = optarg;
			break;
		case 's':
			psc_ctlparse_show(optarg);
			break;
		default:
			usage();
		}

	argc -= optind;
	if (argc)
		usage();

	psc_ctlcli_main(sockfn);
	exit(0);
}
