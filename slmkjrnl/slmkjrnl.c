/* $Id$ */

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "sljournal.h"
#include "pathnames.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: [-d] %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char	c;
	int	dumponly = 0;

	progname = argv[0];
	pfl_init();
	while ((c = getopt(argc, argv, "d")) != -1)
		switch (c) {
		case 'd':
			dumponly = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc)
		usage();

	if (mkdir(_PATH_SLASHD_DIR, 0700) == -1)
		if (errno != EEXIST)
			err(1, "mkdir: %s", _PATH_SLASHD_DIR);

	if (!dumponly) {
		pjournal_format(_PATH_SLJOURNAL, SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE, SLJ_MDS_RA, 0);
		printf("SLASH log file %s has been created with %d %d-byte entries.\n",
				_PATH_SLJOURNAL, SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE);
	}
	pjournal_dump(_PATH_SLJOURNAL);
	exit(0);
}
