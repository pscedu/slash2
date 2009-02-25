/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"

#include "pathnames.h"

#include "slashd/control.h"

int
slrpcmdsthr_st_prhdr(void)
{
	return (printf(" %-*s %8s %8s %8s\n", PSC_THRNAME_MAX, "thread",
	    "#open", "#close", "#stat"));
}

void
slrpcmdsthr_st_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" %-*s %8d %8d %8d", PSC_THRNAME_MAX, pcst->pcst_thrname,
	    pcst->pcst_nopen, pcst->pcst_nclose, pcst->pcst_nstat);
}

int
slrpcbethr_st_prhdr(void)
{
	return (printf(" %-*s %8s\n", PSC_THRNAME_MAX, "thread", "#open"));
}

void
slrpcbethr_st_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" %-*s %8d\n", PSC_THRNAME_MAX, pcst->pcst_thrname,
	    pcst->pcst_nopen);
}

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "loglevels",	psc_ctl_packshow_loglevel },
	{ "stats",	psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = NENTRIES(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS
};
int psc_ctlmsg_nprfmts = NENTRIES(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
	{ psc_ctlthr_prhdr,	psc_ctlthr_prdat },
	{ slrpcmdsthr_st_prhdr,	slrpcmdsthr_st_prdat },
	{ slrpcbethr_st_prhdr,	slrpcbethr_st_prdat }
};
int psc_ctl_nthrstatfmts = NENTRIES(psc_ctl_thrstatfmts);

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
};
int psc_ctlcmd_nreqs = NENTRIES(psc_ctlcmd_reqs);

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
	sockfn = _PATH_SLCTLSOCK;
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
