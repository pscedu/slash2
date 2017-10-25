/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/pfl.h"
#include "pfl/log.h"
#include "pfl/thread.h"

#include "authbuf.h"
#include "pathnames.h"
#include "slerr.h"

int		 create;
const char	*sl_datadir = SL_PATH_DATA_DIR;

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr, "usage: %s [-c] [-D dir]\n", __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	pfl_init();
	pscthr_init(0, NULL, 0, "slkeymgt");
	sl_errno_init();
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
