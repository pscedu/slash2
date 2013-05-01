/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	progname = argv[0];
	while ((c = getopt(argc, argv, "")) != -1) {
		switch (c) {
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	exit(0);
}
