/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl.h"
#include "psc_util/alloc.h"
#include "psc_util/thread.h"

#include "slash.h"
#include "slconfig.h"

#define SLASH_THRTBL_SIZE 19
//#define _PATH_SLASHCONF "/etc/slash.conf"
#define _PATH_SLASHCONF "config/example.conf"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	extern int tcpnal_instances;
	struct psc_thread *pt;

	pt = PSCALLOC(sizeof(*pt));
	pscthr_init(pt, SLTHRT_LND, startf, "sllndthr%d",
	    tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
	pt->pscthr_private = arg;
}

int
main(int argc, char *argv[])
{
	const char *cfn;
	int c;

	progname = argv[0];
	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if (getenv("TCPLND_SERVER") == NULL)
		psc_fatalx("please export TCPLND_SERVER");
	lnet_thrspawnf = spawn_lnet_thr;

	cfn = _PATH_SLASHCONF;
	while ((c = getopt(argc, argv, "f:")) != -1)
		switch (c) {
		case 'f':
			cfn = optarg;
			break;
		default:
			usage();
		}
	pfl_init(SLASH_THRTBL_SIZE);
	slashGetConfig(cfn);
	libsl_init(PSC_SERVER);
	slmds_init();
	exit(0);
}
