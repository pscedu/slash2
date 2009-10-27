/* $Id$ */

#include <sys/param.h>
#include <sys/stat.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "psc_util/log.h"

#include "fid.h"
#include "pathnames.h"

void wipefs(const char *);

const char *progname;
int wipe;
int ion;

void
slimmns_create_int(const char *fn, uint32_t curdepth, uint32_t maxdepth)
{
	char d[PATH_MAX];
	int i, rc;

	for (i = 0; i < 16; i++) {
		rc = snprintf(d, sizeof(d), "%s/%x", fn, i);
		if ((rc == -1) || (rc >= (int)sizeof(d)))
			psc_fatal("snprintf");

		if ((mkdir(d, 0711) == -1) && (errno != EEXIST))
			psc_fatal("mkdir %s", d);

		if (curdepth < maxdepth)
			slimmns_create_int(d, curdepth + 1, maxdepth);
	}
}

/*
 * Routine for creating the directory structure
 *  on a mapserver filesystem.
 */
void
slimmns_create(const char *root, uint32_t depth)
{
	char fn[PATH_MAX];
	int rc;

	if (!depth)
		depth = FID_PATH_DEPTH;

	/* create immutable namespace root directory */
	rc = snprintf(fn, sizeof(fn), "%s/%s",
	    root, FID_PATH_NAME);
	psc_assert(rc != -1 && rc < PATH_MAX);

	rc = mkdir(fn, 0711);
	if (rc == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);

	/* create immutable namespace subdirectories */
	slimmns_create_int(fn, 1, depth);

	if (ion)
		return;

	/* create replication queue directory */
	rc = snprintf(fn, sizeof(fn), "%s/%s",
	    root, SL_PATH_REPLS);
	psc_assert(rc != -1 && rc < (int)sizeof(fn));

	rc = mkdir(fn, 0700);
	if (rc == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-iW] /slash_rootfs\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "iW")) != -1)
		switch (c) {
		case 'i':
			ion = 1;
			break;
		case 'W':
			wipe = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	if (wipe)
		wipefs(argv[0]);
	slimmns_create(argv[0], 0);
	exit(0);
}
