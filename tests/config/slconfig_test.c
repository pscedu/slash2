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

int getOptions(int argc,  char *argv[]);

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-S] [-l level] [-i file]\n", progname);
	exit(1);
}

int main(int argc,  char *argv[])
{
	sl_resm_t *resm;

	progname = argv[0];
	pfl_init(19);
	psc_setloglevel(PLL_NOTICE);
	getOptions(argc, argv);
	slashGetConfig(f);
	libsl_init(serverNode);
	exit(0);
}

int getOptions(int argc,  char *argv[])
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
