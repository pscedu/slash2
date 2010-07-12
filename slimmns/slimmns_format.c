/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "psc_util/log.h"

#include "fid.h"
#include "mkfn.h"
#include "pathnames.h"

void wipefs(const char *);

const char	*progname;
int		 wipe;
int		 ion;

void
slimmns_create_int(const char *fn, uint32_t curdepth, uint32_t maxdepth)
{
	char d[PATH_MAX];
	int i;

	for (i = 0; i < 16; i++) {
		xmkfn(d, "%s/%x", fn, i);
		if (mkdir(d, 0711) == -1 && errno != EEXIST)
			psc_fatal("mkdir %s", d);

		if (curdepth < maxdepth)
			slimmns_create_int(d, curdepth + 1, maxdepth);
	}
}

/*
 * slimmns_create - Create an immutable namespace directory structure.
 */
void
slimmns_create(const char *root, uint32_t depth)
{
	char fn[PATH_MAX];
	int fd, rc;
	uint64_t txg = 0;

	if (!depth)
		depth = FID_PATH_DEPTH;

	/* create immutable namespace root directory */
	xmkfn(fn, "%s/%s", root, FID_PATH_NAME);
	rc = mkdir(fn, 0711);
	if (rc == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);

	/* create immutable namespace subdirectories */
	slimmns_create_int(fn, 1, depth);

	if (ion)
		return;

	/* create replication queue directory */
	xmkfn(fn, "%s/%s", root, SL_PATH_REPLS);
	rc = mkdir(fn, 0700);
	if (rc == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);

	xmkfn(fn, "%s/%s", root, SL_PATH_TXG);
	fd = open(fn, O_CREAT|O_TRUNC|O_WRONLY, 0600);
	if (fd < 0) 
		psc_fatal("open %s", fn);
	if (pwrite(fd, &txg, sizeof(txg), 0) != sizeof(txg))
		psc_fatal("write %s", fn);
	close(fd);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-iW] fsroot\n", progname);
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
