/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "slconfig.h"

#define _PATH_SLASHCONF "../../src/config/example.conf"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [conf-file]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *fn;
	int c;

	optarg = _PATH_SLASHCONF;
	progname = argv[0];
	while ((c = getopt(argc, argv, "f:")) != -1)
		switch (c) {
		case 'f':
			fn = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

	run_yacc(fn);
	exit(0);
}
