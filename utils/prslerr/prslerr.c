/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"

#include "slerr.h"

extern char *slash_errstrs[];
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
	int j, n;

	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	for (j = 0, n = _SLERR_START + 1; slash_errstrs[j]; j++, n++)
		printf("%4d: %s\n", n, slstrerror(n));
	exit(0);
}
