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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "pfl/log.h"

#include "bmap.h"

unsigned char buf[80];

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr, "usage: %s\n", __progname);
	exit(1);
}

#define CHECK(buf, n, state)						\
	do {								\
		int _j;							\
									\
		_j = SL_REPL_GET_BMAP_IOS_STAT((buf), (n));		\
		psc_assert(_j == (state));				\
	} while (0)

int
main(int argc, char *argv[])
{
	int i;

	pfl_init();
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	SL_REPL_SET_BMAP_IOS_STAT(buf, 21, BREPLST_VALID);
	CHECK(buf, 21, BREPLST_VALID);
	CHECK(buf, 21 - SL_BITS_PER_REPLICA, 0);
	CHECK(buf, 21 + SL_BITS_PER_REPLICA, 0);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39, BREPLST_REPL_SCHED);
	CHECK(buf, 39, BREPLST_REPL_SCHED);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39 + SL_BITS_PER_REPLICA, BREPLST_VALID);
	CHECK(buf, 39 + SL_BITS_PER_REPLICA, BREPLST_VALID);
	SL_REPL_SET_BMAP_IOS_STAT(buf, 39 - SL_BITS_PER_REPLICA, BREPLST_VALID);
	CHECK(buf, 39 - SL_BITS_PER_REPLICA, BREPLST_VALID);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39, BREPLST_INVALID);
	CHECK(buf, 39, BREPLST_INVALID);

	CHECK(buf, 39 + SL_BITS_PER_REPLICA, BREPLST_VALID);
	CHECK(buf, 39 - SL_BITS_PER_REPLICA, BREPLST_VALID);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 3, BREPLST_REPL_QUEUED);
	CHECK(buf, 3, BREPLST_REPL_QUEUED);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 3, BREPLST_REPL_QUEUED);
	CHECK(buf, 3, BREPLST_REPL_QUEUED);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 6, BREPLST_VALID);
	CHECK(buf, 3, BREPLST_REPL_QUEUED);
	CHECK(buf, 6, BREPLST_VALID);
	CHECK(buf, 9, 0);

	for (i = 0; i < 20; i++)
		SL_REPL_SET_BMAP_IOS_STAT(buf, i * SL_BITS_PER_REPLICA,
		    i % NBREPLST);
	for (i = 0; i < 20; i++)
		CHECK(buf, i * SL_BITS_PER_REPLICA, i % NBREPLST);

	memset(buf, 0, sizeof(buf));

	for (i = 0; i < 8; i++)
		SL_REPL_SET_BMAP_IOS_STAT(buf,
		    i * SL_BITS_PER_REPLICA, BREPLST_VALID);
	for (i = 0; i < 8; i++)
		CHECK(buf, i * SL_BITS_PER_REPLICA, BREPLST_VALID);
	exit(0);
}
