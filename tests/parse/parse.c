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
	int c;

	progname = argv[0];
	while ((c = getopt(argc, argv, "")) != -1)
		switch (c) {
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 0 &&
	    argc != 1)
		usage();

	run_yacc(argc ? *argv : _PATH_SLASHCONF);
	exit(0);
}
