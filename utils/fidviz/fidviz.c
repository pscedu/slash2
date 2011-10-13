/* $Id$ */

#include <sys/param.h>

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "fid.h"
#include "slconfig.h"

void sli_fg_makepath(const struct slash_fidgen *, char *);

const char *progname;
struct sl_gconf	 globalConfig;

void __dead
usage(void)
{
	fprintf(stderr, "usage: %s fid\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *endp, fn[PATH_MAX];
	struct slash_fidgen fg;

	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	memset(&fg, 0, sizeof(fg));
	fg.fg_fid = strtoll(argv[0], &endp, 16);
	if (endp == argv[0] || *endp)
		errx(1, "%s: invalid FID", argv[0]);

	sli_fg_makepath(&fg, fn);
	printf("%s\n", fn);
	exit(0);
}
