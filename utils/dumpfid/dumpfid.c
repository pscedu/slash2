/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/uio.h>
#include <sys/xattr.h>

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

#define	K_BMAPS		(1 <<  0)
#define	K_BSZ		(1 <<  1)
#define	K_CRC		(1 <<  2)
#define	K_FLAGS		(1 <<  3)
#define	K_FSIZE		(1 <<  4)
#define	K_NBLKS		(1 <<  5)
#define	K_NREPLS	(1 <<  6)
#define	K_REPLPOL	(1 <<  7)
#define	K_REPLS		(1 <<  8)
#define	K_VERSION	(1 <<  9)
#define	K_XCRC		(1 << 10)
#define	K_ALL		((~0) & ~K_BMAPS)

const char *show_keywords[] = {
	"bmaps",
	"bsz",
	"crc",
	"flags",
	"fsize",
	"nblks",
	"nrepls",
	"replpol",
	"repls",
	"version",
	"xcrc",
};

const char *repl_states = "-sq+tpgx";

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-u] [-o keys] file ...\n", progname);
	exit(1);
}

void
dumpfid(const char *fn)
{
	struct slash_inode_extras_od inox;
	struct slash_inode_od ino;
	struct iovec iovs[2];
	struct stat stb;
	uint64_t crc, od_crc;
	int64_t s2usz;
	uint32_t nr, j;
	sl_bmapno_t bno;
	ssize_t rc;
	int fd;

	fd = open(fn, O_RDONLY);
	if (fd == -1) {
		warn("%s", fn);
		return;
	}

	if (fstat(fd, &stb) == -1) {
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

#ifdef SL2_SIZE_EXTATTR
	{
		char fsize[64];

#define SLXAT_SIZE	".sl2-fsize"
		rc = fgetxattr(fd, SLXAT_SIZE, fsize, sizeof(fsize));
		s2usz = strtoull(fsize, NULL, 10);
	}
#else
	s2usz = stb.st_rdev;
#endif

	psc_crc64_calc(&crc, &ino, sizeof(ino));
	printf("%s:\n", fn);
	if (show & K_CRC)
		printf("  crc: %s %"PSCPRIxCRC64" %"PSCPRIxCRC64"\n",
		    crc == od_crc ? "OK" : "BAD", crc, od_crc);
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
	if (show & K_FSIZE && rc > 0)
		printf("  fsize %"PRId64"\n", s2usz);

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
			printf("  inox crc: %s %"PSCPRIxCRC64" %"PSCPRIxCRC64"\n",
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
	if (show & K_BMAPS &&
	    stb.st_size > SL_BMAP_START_OFF) {
		printf("  bmaps %"PSCPRIdOFFT"\n",
		    (stb.st_size - SL_BMAP_START_OFF) / BMAP_OD_SZ);
		if (lseek(fd, SL_BMAP_START_OFF, SEEK_SET) == -1)
			warn("seek");
		for (bno = 0; ; bno++) {
			struct {
				struct bmap_ondisk bod;
				uint64_t crc;
			} bd;
			int off;

			rc = read(fd, &bd, sizeof(bd));
			if (rc == 0)
				break;
			if (rc != sizeof(bd)) {
				warn("read");
				break;
			}
			printf("   %5u: gen %5u pol %u res ",
			    bno, bd.bod.bod_gen,
			    bd.bod.bod_replpol);

			for (j = 0, off = 0; j < nr;
			    j++, off += SL_BITS_PER_REPLICA)
				printf("%c", repl_states[
				    SL_REPL_GET_BMAP_IOS_STAT(
				    bd.bod.bod_repls, off)]);

//			nslvr = SLASH_SLVRS_PER_BMAP;
//			for (j = 0; j < nr; j++)
//				printf("%s%s", j ? "," : "", );

			printf("\n");
		}
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
