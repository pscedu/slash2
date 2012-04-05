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
	uint32_t nr, j;
	ssize_t rc;
	int fd;

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
	printf("%s:\n", fn);
	printf("  crc %s\n", crc == od_crc ? "OK" : "BAD");
	printf("  version %u\n", ino.ino_version);
	printf("  flags %#x\n", ino.ino_flags);
	printf("  bsz %u\n", ino.ino_bsz);
	printf("  nrepls %u\n", ino.ino_nrepls);
	printf("  replpol %u\n", ino.ino_replpol);

	nr = ino.ino_nrepls;
	if (nr > SL_DEF_REPLICAS) {
		if (nr > SL_MAX_REPLICAS)
			nr = SL_MAX_REPLICAS;
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
		printf("  inox crc: %s %lx %lx\n  xrepls:",
		    crc == od_crc ? "OK" : "BAD", crc, od_crc);
	}
	printf("  repls ");
	for (j = 0; j < nr; j++)
		printf("%s%u", j ? "," : "",
		    j < SL_DEF_REPLICAS ?
		    ino.ino_repls[j].bs_id :
		    inox.inox_repls[j - SL_DEF_REPLICAS].bs_id);
	printf("\n  nblks ");
	for (j = 0; j < nr; j++)
		printf("%s%"PRIu64, j ? "," : "",
		    j < SL_DEF_REPLICAS ?
		    ino.ino_repl_nblks[j] :
		    inox.inox_repl_nblks[j - SL_DEF_REPLICAS]);
	printf("\n");

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
