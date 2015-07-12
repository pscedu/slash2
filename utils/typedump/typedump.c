/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2015, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>

#include <err.h>
#include <stdio.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/ctl.h"

/* start includes */
/* end includes */

struct bmap_ondisk bmapod;
char buf[1024 * 1024];
const char *progname;

void
pr(const char *name, uint64_t value, int hex)
{
	static int i;
	int n;

	if (i++ % 2) {
		n = printf("%s ", name);
		while (n++ <= 50)
			putchar('-');
		if (n < 53)
			printf("> ");
		if (hex)
			printf("%"PRIx64"\n", value);
		else
			printf("%"PRIu64"\n", value);
	} else {
		if (hex)
			printf("%-52s %"PRIx64"\n", name, value);
		else
			printf("%-52s %"PRIu64"\n", name, value);
	}
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	uint64_t crc;
	int c;

	progname = argv[0];
	while ((c = getopt(argc, argv, "")) != -1)
		switch (c) {
		default:
			usage();
		}
	argc -= optind;
	if (argc)
		usage();

#define PRTYPE(type)	pr(#type, sizeof(type), 0)
#define PRVAL(val)	pr(#val, (unsigned long)(val), 0)
#define PRVALX(val)	pr(#val, (unsigned long)(val), 1)

	/* start structs */
	/* end structs */

	/* start constants */
	/* end constants */

	/* start enums */
	/* end enums */

	PRVAL(SL_REPLICA_NBYTES);

	PRVAL(LNET_MTU);
	PRVAL(BMAP_OD_SZ);
	PRVAL(BMAP_OD_CRCSZ);

	PRVALX(FID_ANY);

	psc_crc64_calc(&crc, buf, sizeof(buf));
	printf("NULL 1MB buf CRC is %"PSCPRIxCRC64"\n", crc);

	psc_crc64_calc(&crc, &bmapod, sizeof(bmapod));
	printf("NULL bmapod CRC is %#"PRIx64"\n", crc);

	PRVAL(offsetof(struct bmap_mds_info, bmi_upd));

	PRTYPE(struct pscfs_dirent);

	exit(0);
}
