/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2011-2015, Pittsburgh Supercomputing Center (PSC).
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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

#include "fid.h"
#include "slashrpc.h"

enum {
	POP_PRINT,
	POP_SET_BATCHNO,
	POP_DELETE
};

int		 delete;

struct reclaim_prog_entry {
	uint64_t	res_xid;
	uint64_t	res_batchno;
	sl_ios_id_t	res_id;
	int32_t		_pad;
};

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-D] [-b batchno] [-i id] [-p prog-file] [-r reclaim-file]\n",
	    __progname);
	exit(1);
}

void
dump_reclaim_log(void *buf, int size)
{
	int i, count, order = 0;
	struct srt_reclaim_entry *entryp;
	uint64_t xid = 0;

	count = size / sizeof(struct srt_reclaim_entry);
	entryp = buf;
	if (entryp->xid != RECLAIM_MAGIC_VER ||
	    entryp->fg.fg_fid != RECLAIM_MAGIC_FID ||
	    entryp->fg.fg_gen != RECLAIM_MAGIC_GEN) {
		fprintf(stderr, "Reclaim log corrupted, invalid header.\n");
		exit(1);
	}
	count--;
	entryp = PSC_AGP(entryp, sizeof(struct srt_reclaim_entry));
	printf("   The entry size is %d bytes, total # of entries is %d\n\n",
	    (int)sizeof(struct srt_reclaim_entry), count);

	for (i = 0; i < count; i++) {
		if (entryp->xid < xid) {
			order++;
			printf("%4d:   xid = %"PRId64", fg = "SLPRI_FG" * \n",
			    i, entryp->xid, SLPRI_FG_ARGS(&entryp->fg));
		} else
			printf("%4d:   xid = %"PRId64", fg = "SLPRI_FG"\n",
			    i, entryp->xid, SLPRI_FG_ARGS(&entryp->fg));
		entryp = PSC_AGP(entryp, sizeof(struct srt_reclaim_entry));
	}
	printf("\n   Total number of out-of-order entries: %d\n", order);
}

void
dump_rpe(size_t n, const struct reclaim_prog_entry *rpe)
{
	printf("%5zd: %6d %12"PRId64" %12"PRId64"\n",
	    n, rpe->res_id, rpe->res_xid, rpe->res_batchno);
}

int
proc_reclaim_prog_log(void *buf, off_t *size, unsigned int id, int op,
    uint64_t batchno)
{
	int found = 0, modified = 0;
	struct reclaim_prog_entry *rpe;
	size_t count, i;

	count = *size / sizeof(*rpe);

	printf("%5s  %6s %12s %12s\n", "n", "id", "xid", "batchno");
	for (i = 0, rpe = buf; i < count; i++, rpe++) {
		if (id == rpe->res_id) {
			found = 1;
			switch (op) {
			case POP_SET_BATCHNO:
				if (rpe->res_batchno == batchno) {
					printf("skipping entry:\n");
					dump_rpe(i, rpe);
				} else {
					printf("modifying entry:\n");
					dump_rpe(i, rpe);
					printf("now:\n");
					rpe->res_batchno = batchno;
					dump_rpe(i, rpe);
					modified = 1;
				}
				break;
			case POP_DELETE:
				printf("deleting entry:\n");
				dump_rpe(i, rpe);
				memmove(rpe, rpe + 1,
				    (count - i - 1) * sizeof(*rpe));
				*size -= sizeof(*rpe);
				modified = 1;
				rpe--;
				break;
			}
		}
		if (op == POP_PRINT)
			dump_rpe(i, rpe);
	}
	if (!found && id)
		printf("\nThe ID %d is not found in the table.\n", id);
	return (modified);
}

int
main(int argc, char *argv[])
{
	int modified, reclaim_log = 0, reclaim_prog_log = 0, fd, op = 0;
	char c, *buf = NULL, *log = NULL;
	uint64_t batchno = 0;
	struct stat sbuf;
	int32_t id = 0;
	ssize_t nr;

	while ((c = getopt(argc, argv, "b:Di:p:r:")) != -1)
		switch (c) {
		case 'b':
			op = POP_SET_BATCHNO;
			batchno = strtol(optarg, NULL, 0);
			break;
		case 'D':
			op = POP_DELETE;
			break;
		case 'i':
			id = strtol(optarg, NULL, 0);
			break;
		case 'p':
			log = optarg;
			reclaim_prog_log = 1;
			break;
		case 'r':
			log = optarg;
			reclaim_log = 1;
			break;
		default:
			usage();
		}

	argc -= optind;
	if (argc)
		usage();
	if (reclaim_log && reclaim_prog_log)
		usage();
	if (!reclaim_log && !reclaim_prog_log)
		usage();

	fd = open(log, op == POP_PRINT ? O_RDONLY : O_RDWR);
	if (fd == -1)
		err(1, "failed to open %s", log);
	if (fstat(fd, &sbuf) == -1)
		err(1, "failed to stat %s", log);

	buf = malloc(sbuf.st_size);
	if (buf == NULL)
		err(1, NULL);

	nr = read(fd, buf, sbuf.st_size);
	if (nr != sbuf.st_size)
		err(1, "log file read");

	if (reclaim_log)
		dump_reclaim_log(buf, sbuf.st_size);
	if (reclaim_prog_log) {
		modified = proc_reclaim_prog_log(buf, &sbuf.st_size, id,
		    op, batchno);
		if (modified) {
			if (ftruncate(fd, 0) == -1)
				err(1, "truncate");
			lseek(fd, 0, SEEK_SET);
			nr = write(fd, buf, sbuf.st_size);
			if (nr != sbuf.st_size)
				err(1, "log file write");
		}
	}

	if (close(fd) == -1)
		err(1, "log file close");
	exit(0);
}
