/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <err.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/pfl.h"

#include "fid.h"
#include "slashrpc.h"

const char	*progname;
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
	fprintf(stderr,
	    "usage: %s [-v] [-b batchno] [-i id] [-p prog-file] [-r reclaim-file]\n",
	    progname);
	exit(1);
}

void
dump_reclaim_log(void *buf, int size)
{
	int i, count, order = 0, entrysize = RECLAIM_ENTRY_SIZE;
	struct srt_reclaim_entry *entryp;
	uint64_t xid = 0;

	count = size / sizeof(struct srt_reclaim_entry);
	entryp = buf;
	if (entryp->fg.fg_fid == RECLAIM_MAGIC_FID &&
	    entryp->fg.fg_gen == RECLAIM_MAGIC_GEN) {
		count--;
		entrysize = sizeof(struct srt_reclaim_entry);
		entryp = PSC_AGP(entryp, entrysize);
	}
	printf("   The entry size is %d bytes, total # of entries is %d\n\n",
	    entrysize, count);

	for (i = 0; i < count; i++) {
		if (entryp->xid < xid) {
			order++;
			printf("%4d:   xid = %"PRId64", fg = "SLPRI_FG" * \n",
			    i, entryp->xid, SLPRI_FG_ARGS(&entryp->fg));
		} else
			printf("%4d:   xid = %"PRId64", fg = "SLPRI_FG"\n",
			    i, entryp->xid, SLPRI_FG_ARGS(&entryp->fg));
		entryp = PSC_AGP(entryp, entrysize);
	}
	printf("\n   Total number of out-of-order entries: %d\n", order);
}

int
dump_reclaim_prog_log(void *buf, off_t *size, unsigned int id, int op,
    uint64_t batchno)
{
	struct reclaim_prog_entry *rpe;
	int i, found, modified;
	size_t count;

	count = size / sizeof(struct reclaim_prog_entry);

	found = 0;
	modified = 0;
	if (op == POP_PRINT)
		printf("%5s  %6s %12s %12s\n", "n", "id", "xid",
		    "batchno");
	for (i = 0, rpe = buf; i < count; i++, rpe++) {
		if (id == rpe->res_id) {
			found = 1;
			switch (op) {
			case POP_SET_BATCHNO:
				if (rpe->res_batchno != batchno) {
					rpe->res_batchno = batchno;
					modified = 1;
				}
				break;
			case POP_DELETE:
				memmove(rpe, rpe + entrysize,
				    count - sizeof(*rpe) * (i + 1));
				break;
			case POP_PRINT:
				printf("%5d: %6d %12"PRId64" %12"PRId64"d\n",
				    i, rpe->res_id, rpe->res_xid,
				    rpe->res_batchno, rpe->res_id);
				break;
			}
		}
	}
	if (!found && id)
		printf("\nThe ID %d is not found in the table.\n\n", id);

	printf("\nNew contents of the reclaim progress log are as follows:\n\n");
	for (i = 0, rpe = buf; i < count; i++, rpe++)
		printf("%4d:  xid = %10"PRId64", batchno = %10"PRId64", id = %d\n",
		    i, prog_entryp->res_xid, prog_entryp->res_batchno,
		    prog_entryp->res_id);
	return (1);
}

enum {
	POP_SET_BATCHNO = 1,
	POP_DELETE
};

int
main(int argc, char *argv[])
{
	int modified, reclaim_log = 0, reclaim_prog_log = 0, fd, op = 0;
	char c, *buf = NULL, *log = NULL;
	uint64_t batchno = 0;
	struct stat sbuf;
	int32_t id = 0;
	ssize_t nr;

	progname = argv[0];
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

	fd = open(log, batchno || id ? O_RDWR : O_RDONLY);
	if (fd < 0)
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
		modified = dump_reclaim_prog_log(buf, &sbuf.st_size, id,
		    op, batchno);
		if (modified) {
			if (ftruncate(fd, 0) == -1)
				err(1, "truncate");
			nr = write(fd, buf, sbuf.st_size);
			if (nr != sbuf.st_size)
				err(1, "log file write");
		}
	}

	if (close(fd) == -1)
		err(1, "log file close");
	exit(0);
}
