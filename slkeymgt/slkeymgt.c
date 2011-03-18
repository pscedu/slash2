/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/log.h"

#include "authbuf.h"
#include "pathnames.h"

int		 create;
const char	*sl_datadir = SL_PATH_DATA_DIR;
const char	*progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-c] [-D dir]\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "cD:")) != -1)
		switch (c) {
		case 'c':
			create = 1;
			break;
		case 'D':
			sl_datadir = optarg;
			break;
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();
	if (create)
		authbuf_createkeyfile();
	else
		authbuf_checkkeyfile();
	exit(0);
}
