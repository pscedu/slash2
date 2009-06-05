/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "psc_util/crc.h"

#include "inode.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	psc_crc_t crc;
	char b[1048576];
	sl_blkh_t bmapod;

	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	memset(b, 0, sizeof(b));
	PSC_CRC_CALC(crc, b, sizeof(b));
	printf("NULL 1MB buf CRC is %#"PRIx64"\n", crc);

	memset(&bmapod, 0, sizeof(sl_blkh_t));
	PSC_CRC_CALC(crc, &bmapod, sizeof(sl_blkh_t));
	printf("NULL sl_blkh_t CRC is %#"PRIx64"\n", crc);
	exit(0);
}
