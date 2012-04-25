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

int	show;

const char *progname;

#define K_BSZ		(1 << 0)
#define	K_CRC		(1 << 1)
#define	K_FLAGS		(1 << 2)
#define	K_NBLKS		(1 << 3)
#define	K_NREPLS	(1 << 4)
#define	K_REPLPOL	(1 << 5)
#define	K_REPLS		(1 << 6)
#define	K_VERSION	(1 << 7)
#define	K_XCRC		(1 << 8)
#define	K_ALL		(~0)

const char *show_keywords[] = {
	"bszn",
	"crc",
	"flags",
	"nblks",
	"nrepls",
	"replpol",
	"repls",
	"version",
	"xcrc",
};

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-o keys] file ...\n", progname);
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
	if (show & K_CRC)
		printf("  crc %s\n", crc == od_crc ? "OK" : "BAD");
	if (show & K_VERSION)
		printf("  version %u\n", ino.ino_version);
	if (show & K_FLAGS)
		printf("  flags %#x\n", ino.ino_flags);
	if (show & K_BSZ)
		printf("  bsz %u\n", ino.ino_bsz);
	if (show & K_NREPLS)
		printf("  nrepls %u\n", ino.ino_nrepls);
	if (show & K_REPLPOL)
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
		if (show & K_XCRC)
			printf("  inox crc: %s %lx %lx\n",
			    crc == od_crc ? "OK" : "BAD", crc, od_crc);
	}
	if (show & K_REPLS) {
		printf("  repls ");
		for (j = 0; j < nr; j++)
			printf("%s%u", j ? "," : "",
			    j < SL_DEF_REPLICAS ?
			    ino.ino_repls[j].bs_id :
			    inox.inox_repls[j - SL_DEF_REPLICAS].bs_id);
		printf("\n");
	}
	if (show & K_NBLKS) {
		printf("  nblks ");
		for (j = 0; j < nr; j++)
			printf("%s%"PRIu64, j ? "," : "",
			    j < SL_DEF_REPLICAS ?
			    ino.ino_repl_nblks[j] :
			    inox.inox_repl_nblks[j - SL_DEF_REPLICAS]);
		printf("\n");
	}

 out:
	close(fd);
}

void
lookupshow(const char *flg)
{
	const char **p;
	int i;

	for (i = 0, p = show_keywords;
	    i < nitems(show_keywords); i++, p++)
		if (strcmp(flg, *p) == 0) {
			show |= 1 << i;
			return;
		}
	errx(1, "unknown show field: %s", flg);
}

int
main(int argc, char *argv[])
{
	int c;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "o:")) != -1) {
		switch (c) {
		case 'o':
			lookupshow(optarg);
			break;
		default:
			usage();
		}
	}
	argc -= optind;
	argv += optind;
	if (!argc)
		usage();
	if (!show)
		show = K_ALL;
	for (; *argv; argv++)
		dumpfid(*argv);
	exit(0);
}
