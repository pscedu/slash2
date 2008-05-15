/* $Id$ */

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "psc_ds/list.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/log.h"
#include "psc_util/subsys.h"
#include "../mount_slash/control.h"

struct psc_ctlshow_ent psc_ctlshow_tab[] = {
	{ "loglevels",		PCMT_GETLOGLEVEL },
	{ "stats",		PCMT_GETSTATS }
};
int psc_ctlshow_ntabents = NENTRIES(psc_ctlshow_tab);

struct psc_ctlmsg_prfmt psc_ctlmsg_prfmts[] = {
	PSC_CTLMSG_PRFMT_DEFS
};
int psc_ctlmsg_nprfmts = NENTRIES(psc_ctlmsg_prfmts);

struct psc_ctl_thrstatfmt psc_ctl_thrstatfmts[] = {
/* CTL		*/	{ psc_ctlthr_prhdr,	psc_ctlthr_prdat },
/* RPCMDS	*/	{ NULL,			NULL },
/* RPCBE	*/	{ NULL,			NULL },
/* LND		*/	{ NULL,			NULL },
/* TINTV	*/	{ NULL,			NULL },
/* TIOS		*/	{ NULL,			NULL }
};
int psc_ctl_nthrstatfmts = NENTRIES(psc_ctl_thrstatfmts);

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HI] [-h table] [-i iostat] [-L listspec]\n"
	    "\t[-p param[=value]] [-S socket] [-s value]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *sockfn;
	int c;

	progname = argv[0];
	sockfn = _PATH_MSCTLSOCK;
	while ((c = getopt(argc, argv, "Hh:Ii:L:p:S:s:")) != -1)
		switch (c) {
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
			psc_ctlparse_iostat(optarg);
			break;
		case 'L':
			psc_ctlparse_lc(optarg);
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
