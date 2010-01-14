/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#include "pathnames.h"
#include "slerr.h"
#include "sljournal.h"

int format;
int query;
int verbose;
const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: [-fqv] [-c file] %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	const char *fn = _PATH_SLJOURNAL;
	char c;
	int rc;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "c:fqv")) != -1)
		switch (c) {
		case 'c':
			fn = optarg;
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

	if (mkdir(_PATH_SLASHD_DIR, 0700) == -1)
		if (errno != EEXIST)
			err(1, "mkdir: %s", _PATH_SLASHD_DIR);

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
