/* $Id$ */

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

	SL_REPL_SET_BMAP_IOS_STAT(buf, 21, SL_REPLST_ACTIVE);
	CHECK(buf, 21, SL_REPLST_ACTIVE);
	CHECK(buf, 21 - SL_BITS_PER_REPLICA, 0);
	CHECK(buf, 21 + SL_BITS_PER_REPLICA, 0);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39, SL_REPLST_SCHED);
	CHECK(buf, 39, SL_REPLST_SCHED);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39 + SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	CHECK(buf, 39 + SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	SL_REPL_SET_BMAP_IOS_STAT(buf, 39 - SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	CHECK(buf, 39 - SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 39, SL_REPLST_INACTIVE);
	CHECK(buf, 39, SL_REPLST_INACTIVE);

	CHECK(buf, 39 + SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	CHECK(buf, 39 - SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 3, SL_REPLST_OLD);
	CHECK(buf, 3, SL_REPLST_OLD);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 3, SL_REPLST_OLD);
	CHECK(buf, 3, SL_REPLST_OLD);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 6, SL_REPLST_ACTIVE);
	CHECK(buf, 3, SL_REPLST_OLD);
	CHECK(buf, 6, SL_REPLST_ACTIVE);
	CHECK(buf, 9, 0);

	for (i = 0; i < 20; i++)
		SL_REPL_SET_BMAP_IOS_STAT(buf, i * SL_BITS_PER_REPLICA,
		    i % SL_NREPLST);
	for (i = 0; i < 20; i++)
		CHECK(buf, i * SL_BITS_PER_REPLICA, i % SL_NREPLST);

	memset(buf, 0, sizeof(buf));

	for (i = 0; i < 8; i++)
		SL_REPL_SET_BMAP_IOS_STAT(buf,
		    i * SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	for (i = 0; i < 8; i++)
		CHECK(buf, i * SL_BITS_PER_REPLICA, SL_REPLST_ACTIVE);
	exit(0);
}
