/* $Id: zestConfigTest.c 1283 2007-07-17 16:43:48Z yanovich $ */

#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "slconfig.h"
#include "pfl.h"
#include "psc_util/log.h"
#include "psc_util/cdefs.h"
#include "psc_rpc/rpc.h"

char *progname;
char *f = "../../src/config/example.conf";
int serverNode;

int getOptions(int argc, char *argv[]);

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-S] [-l level] [-i file]\n", progname);
	exit(1);
}

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	extern int tcpnal_instances;
	struct psc_thread *pt;

	pt = PSCALLOC(sizeof(*pt));
	pscthr_init(pt, 7, startf, NULL, "sllndthr%d", tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
	pt->pscthr_private = arg;
}

int
main(int argc, char *argv[])
{
	progname = argv[0];
	lnet_thrspawnf = spawn_lnet_thr;
	pfl_init(19);
	psc_setloglevel(PLL_NOTICE);
	getOptions(argc, argv);
	slashGetConfig(f);
	libsl_init(serverNode);
	exit(0);
}

int
getOptions(int argc, char *argv[])
{
#define ARGS "i:l:S"
	int c, err = 0;

	optarg     = NULL;
	serverNode = 0;

	while ( !err && ((c = getopt(argc, argv, ARGS)) != -1))
		switch (c) {

		case 'l':
			psc_setloglevel(atoi(optarg));
			break;

		case 'i':
			f = optarg;
			break;

		case 'S':
			serverNode = 1;
			break;

		default :
			usage();
		}

	return 0;
}
