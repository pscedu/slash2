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

#include <sys/param.h>
#include <sys/uio.h>

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"

#include "slashd/inode.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s file ...\n", progname);
	exit(1);
}

void
dumpfid(const char *fn)
{
	struct slash_inode_extras_od inox;
	struct slash_inode_od ino;
	struct iovec iovs[2];
	uint64_t crc, od_crc;
	int fd, j, nr;
	ssize_t rc;

	fd = open(fn, O_RDONLY);
	if (fd == -1) {
		warn("%s", fn);
		return;
	}
	iovs[0].iov_base = &ino;
	iovs[0].iov_len = sizeof(ino);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = readv(fd, iovs, nitems(iovs));
	if (rc == -1) {
		warn("%s", fn);
		goto out;
	}
	if (rc != sizeof(ino) + sizeof(od_crc)) {
		warnx("%s: short I/O, want %zd got %zd",
		    fn, sizeof(ino) + sizeof(od_crc), rc);
		goto out;
	}
	psc_crc64_calc(&crc, &ino, sizeof(ino));
	printf("%s\t", fn);
	dump_ino(&ino);
	printf("   CRC %s\n", crc == od_crc ? "OK" : "BAD");

	lseek(fd, SL_EXTRAS_START_OFF, SEEK_SET);
	iovs[0].iov_base = &inox;
	iovs[0].iov_len = sizeof(inox);
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = readv(fd, iovs, nitems(iovs));
	if (rc == -1) {
		warn("%s", fn);
		goto out;
	}
	if (rc != sizeof(inox) + sizeof(od_crc)) {
		warnx("%s: short I/O, want %zd got %zd",
		    fn, sizeof(inox) + sizeof(od_crc), rc);
		goto out;
	}
	psc_crc64_calc(&crc, &inox, sizeof(inox));
	printf("\tcrc: %s xrepls:", crc == od_crc ? "OK" : "BAD");
	nr = ino.ino_nrepls;
	if (nr < SL_DEF_REPLICAS)
		nr = 0;
	else if (nr > SL_MAX_REPLICAS) {
		psc_errorx("ino_nrepls out of range (%d)", nr);
		nr = SL_MAX_REPLICAS;
	}
	for (j = 0; j + SL_DEF_REPLICAS < nr; j++)
		printf("%s%u", j ? "," : "", inox.inox_repls[j].bs_id);
	printf(" nbp:%d\n", ino.ino_replpol);
 out:
	close(fd);
}

int
main(int argc, char *argv[])
{
	pfl_init();
	progname = argv[0];
	if (getopt(argc, argv, "") != -1)
		usage();
	argc -= optind;
	argv += optind;
	if (!argc)
		usage();
	for (; *argv; argv++)
		dumpfid(*argv);
	exit(0);
}
