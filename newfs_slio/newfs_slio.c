/* $Id$ */

#include <stdio.h>
#include <stdlib.h>

#include "slconfig.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-f cfgfile]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *cfgfn;
	int c;

	progname = argv[0];
	cfgfn = _PATH_SLASHCONF;
	while ((c = getopt(argc, argv, "f:")) != -1)
		switch (c) {
		case 'f':
			cfgfn = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();
	slashGetConfig(cfgfn);
	exit(0);
}
