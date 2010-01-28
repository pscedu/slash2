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

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "mkfn.h"
#include "pathnames.h"
#include "slerr.h"
#include "sljournal.h"

int format;
int query;
int verbose;
const char *datadir = _PATH_SLASHD_DIR;
const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: [-fqv] [-D dir] %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char c, fn[PATH_MAX];
	int rc;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "D:fqv")) != -1)
		switch (c) {
		case 'D':
			datadir = optarg;
			break;
		case 'f':
			format = 1;
			break;
		case 'q':
			query = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc)
		usage();

	if (!format && !query)
		usage();

	if (mkdir(datadir, 0700) == -1)
		if (errno != EEXIST)
			err(1, "mkdir: %s", datadir);

	xmkfn(fn, "%s/%s", datadir, _RELPATH_SLJOURNAL);

	if (format) {
		rc = pjournal_format(fn, SLJ_MDS_JNENTS,
		    SLJ_MDS_ENTSIZE, SLJ_MDS_RA, 0);
		if (rc)
			psc_fatalx("failing formatting journal %s: %s",
			    fn, slstrerror(rc));
		if (verbose)
			warnx("created log file %s with %d %d-byte entries",
			    fn, SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE);
	}
	if (query)
		pjournal_dump(fn, verbose);
	exit(0);
}
