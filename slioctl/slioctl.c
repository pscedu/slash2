/* $Id$ */

#include <sys/param.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "psc_ds/list.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"
#include "psc_util/subsys.h"
#include "../sliod/control.h"

int
slrpciothr_prhdr(void)
{
	return (printf(" %-*s %8s\n", PSCTHR_NAME_MAX, "thread", "#write"));
}

void
slrpciothr_prdat(const struct psc_ctlmsg_stats *pcst)
{
	printf(" %-*s %8u\n", PSCTHR_NAME_MAX, pcst->pcst_thrname,
	    pcst->pcst_nwrite);
}

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
	sockfn = _PATH_SLIOCTLSOCK;
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
			psc_ctlparse_iostats(optarg);
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
