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

int
main(int argc, char *argv[])
{
	progname = argv[0];
	pfl_init();
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	SL_REPL_SET_BMAP_IOS_STAT(buf, 21, SL_REPL_ACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 21) == SL_REPL_ACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 21 - SL_BITS_PER_REPLICA) == 0);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 21 + SL_BITS_PER_REPLICA) == 0);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 43, SL_REPL_SCHED);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43) == SL_REPL_SCHED);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 43 + SL_BITS_PER_REPLICA, SL_REPL_ACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43 + SL_BITS_PER_REPLICA) == SL_REPL_ACTIVE);
	SL_REPL_SET_BMAP_IOS_STAT(buf, 43 - SL_BITS_PER_REPLICA, SL_REPL_ACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43 - SL_BITS_PER_REPLICA) == SL_REPL_ACTIVE);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 43, SL_REPL_INACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43) == SL_REPL_INACTIVE);

	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43 + SL_BITS_PER_REPLICA) == SL_REPL_ACTIVE);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 43 - SL_BITS_PER_REPLICA) == SL_REPL_ACTIVE);

	SL_REPL_SET_BMAP_IOS_STAT(buf, 3, SL_REPL_OLD);
	psc_assert(SL_REPL_GET_BMAP_IOS_STAT(buf, 3) == SL_REPL_OLD);

	printf("hi\n");

	exit(0);
}
