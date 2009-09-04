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
	struct slash_bmap_od bmapod;
	psc_crc_t crc;
	char b[1048576];

	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	if (argc)
		usage();

	memset(b, 0, sizeof(b));
	psc_crc_calc(&crc, b, sizeof(b));
	printf("NULL 1MB buf CRC is %#"PRIx64"\n", crc);

	memset(&bmapod, 0, sizeof(bmapod));
	psc_crc_calc(&crc, &bmapod, sizeof(bmapod));
	printf("NULL sl_blkh_t CRC is %#"PRIx64"\n", crc);
	exit(0);
}
