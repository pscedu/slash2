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

#include "pfl/fs.h"
#include "pfl/pfl.h"
#include "pfl/str.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "mkfn.h"
#include "pathnames.h"
#include "slerr.h"
#include "sljournal.h"

#include "slashd/mdsio.h"
#include "slashd/subsys_mds.h"

int		 format;
int		 query;
int		 verbose;
const char	*datadir = SL_PATH_DATADIR;
const char	*progname;
struct pscfs	 pscfs;
struct mdsio_ops mdsio_ops;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-fqv] [-b block-device] [-D dir] [-n nentries]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	ssize_t nents = SLJ_MDS_JNENTS;
	char *endp, c, fn[PATH_MAX];
	long l;
	int rc;

	pfl_init();
	sl_subsys_register();
	psc_subsys_register(SLMSS_ZFS, "zfs");
	psc_subsys_register(SLMSS_JOURNAL, "jrnl");

	fn[0] = '\0';
	progname = argv[0];
	while ((c = getopt(argc, argv, "b:D:fn:qv")) != -1)
		switch (c) {
		case 'b':
			strlcpy(fn, optarg, sizeof(fn));
			break;
		case 'D':
			datadir = optarg;
			break;
		case 'f':
			format = 1;
			break;
		case 'n':
			endp = NULL;
			l = strtol(optarg, &endp, 10);
			if (l <= 0 || l > INT_MAX ||
			    endp == optarg || *endp != '\0')
				errx(1, "invalid -n nentries: %s", optarg);
			nents = (ssize_t)l;
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

	if (fn[0] == '\0') {
		if (mkdir(datadir, 0700) == -1)
			if (errno != EEXIST)
				err(1, "mkdir: %s", datadir);

		xmkfn(fn, "%s/%s", datadir, SL_FN_OPJOURNAL);
	}

	if (format) {
		rc = pjournal_format(fn, nents, SLJ_MDS_ENTSIZE, SLJ_MDS_RA);
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
