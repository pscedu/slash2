/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"
#include "psc_util/log.h"

#include "bmap.h"

unsigned char buf[80];
char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
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

	progname = argv[0];
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
