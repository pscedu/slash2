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
	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	/* start custom errnos */
	printf("%4d [SLERR_REPL_ALREADY_ACT]: %s\n", SLERR_REPL_ALREADY_ACT, slstrerror(SLERR_REPL_ALREADY_ACT));
	printf("%4d [SLERR_REPL_ALREADY_INACT]: %s\n", SLERR_REPL_ALREADY_INACT, slstrerror(SLERR_REPL_ALREADY_INACT));
	printf("%4d [SLERR_REPLS_ALL_INACT]: %s\n", SLERR_REPLS_ALL_INACT, slstrerror(SLERR_REPLS_ALL_INACT));
	printf("%4d [SLERR_INVALID_BMAP]: %s\n", SLERR_INVALID_BMAP, slstrerror(SLERR_INVALID_BMAP));
	printf("%4d [SLERR_UNKNOWN_IOS]: %s\n", SLERR_UNKNOWN_IOS, slstrerror(SLERR_UNKNOWN_IOS));
	printf("%4d [SLERR_ION_UNKNOWN]: %s\n", SLERR_ION_UNKNOWN, slstrerror(SLERR_ION_UNKNOWN));
	printf("%4d [SLERR_ION_OFFLINE]: %s\n", SLERR_ION_OFFLINE, slstrerror(SLERR_ION_OFFLINE));
	printf("%4d [SLERR_ION_NOTREPL]: %s\n", SLERR_ION_NOTREPL, slstrerror(SLERR_ION_NOTREPL));
	printf("%4d [SLERR_XACT_FAIL]: %s\n", SLERR_XACT_FAIL, slstrerror(SLERR_XACT_FAIL));
	printf("%4d [SLERR_SHORTIO]: %s\n", SLERR_SHORTIO, slstrerror(SLERR_SHORTIO));
	/* end custom errnos */
	exit(0);
}
