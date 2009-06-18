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

#include "mount_slash/control.h"

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "loglevels",	psc_ctl_packshow_loglevel },
	{ "stats",	psc_ctl_packshow_stats }
};
int psc_ctlshow_ntabents = nitems(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS
};
int psc_ctlmsg_nprfmts = nitems(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
/* CTL		*/	{ psc_ctlthr_prhdr,	psc_ctlthr_prdat },
/* RPCMDS	*/	{ NULL,			NULL },
/* RPCBE	*/	{ NULL,			NULL },
/* LND		*/	{ NULL,			NULL },
/* TINTV	*/	{ NULL,			NULL },
/* TIOS		*/	{ NULL,			NULL }
};
int psc_ctl_nthrstatfmts = nitems(psc_ctl_thrstatfmts);

struct psc_ctlcmd_req psc_ctlcmd_reqs[] = {
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
	sockfn = _PATH_MSCTLSOCK;
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
