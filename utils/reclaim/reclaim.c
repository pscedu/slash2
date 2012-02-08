/* $Id: reclaim.c 18216 2011-10-21 18:31:04Z yanovich $ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011, Pittsburgh Supercomputing Center (PSC).
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
#include "slashrpc.h"

#include "fid.h"

const char *progname;

struct reclaim_prog_entry {
	uint64_t	res_xid;
	uint64_t	res_batchno;
	sl_ios_id_t	res_id;
	int32_t		_pad;
};

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-r reclaim log or -p reclaim prog log] [-b batchno] [-i id] [-v]\n",
	    progname);
	exit(1);
}

void
dump_reclaim_log(char *buf, int size)
{
	uint64_t xid = 0;
	struct srt_reclaim_entry *entryp;
	int i, count, order = 0, entrysize = RECLAIM_ENTRY_SIZE;

	count = size / sizeof(struct srt_reclaim_entry);
	entryp = (struct srt_reclaim_entry *)buf;
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
dump_reclaim_prog_log(char *buf, int size, uint64_t batchno, unsigned int id)
{
	int i, found, count, modified, entrysize;
	struct reclaim_prog_entry *prog_entryp;

	entrysize = sizeof(struct reclaim_prog_entry);
	count = size / sizeof(struct reclaim_prog_entry);
	prog_entryp = (struct reclaim_prog_entry *)buf;

	found = 0;
	modified = 0;
	printf("Current contents of the reclaim progress log are as follows:\n\n");
	for (i = 0; i < count; i++) {
		printf("%4d:  xid = %10ld, batchno = %10ld, id = %d\n",
			i, prog_entryp->res_xid, prog_entryp->res_batchno, 
			prog_entryp->res_id);
		if (id == prog_entryp->res_id) {
			found = 1;
			if (prog_entryp->res_batchno != batchno) {
				prog_entryp->res_batchno = batchno;
				modified = 1;
			}
		}
		prog_entryp = PSC_AGP(prog_entryp, entrysize);
	}
	if (!found && id)
		printf("\nThe id %d is not found in the table.\n\n", id);
	if (!modified)
		return (0);
	printf("\nNew contents of the reclaim progress log are as follows:\n\n");
	prog_entryp = (struct reclaim_prog_entry *)buf;
	for (i = 0; i < count; i++) {
		printf("%4d:  xid = %10ld, batchno = %10ld, id = %d\n",
			i, prog_entryp->res_xid, prog_entryp->res_batchno, 
			prog_entryp->res_id);
		prog_entryp = PSC_AGP(prog_entryp, entrysize);
	}
	return (1);
}

int
main(int argc, char *argv[])
{
	int32_t id = 0;
	uint64_t batchno = 0;
	int modified = 0;
	int reclaim_log = 0;
	int reclaim_prog_log = 0;
	struct stat sbuf;
	int rc, fd, verbose = 0;
	char c, *buf = NULL,*log = NULL;

	progname = argv[0];
	while ((c = getopt(argc, argv, "r:p:b:i:v")) != -1)
		switch (c) {
		case 'r':
			log = optarg;
			reclaim_log = 1;
			break;
		case 'p':
			log = optarg;
			reclaim_prog_log = 1;
			break;
		case 'b':
			batchno = strtol(optarg, NULL, 0);
			break;
		case 'i':
			id = strtol(optarg, NULL, 0);
			break;
		case 'v':
			verbose = 1;
			break;
		default:
			usage();
		}

	argc -= optind;
	argv += optind;
	if (argc)
		usage();
	if (reclaim_log && reclaim_prog_log)
		usage();
	if (!reclaim_log && !reclaim_prog_log)
		usage();

	if (stat(log, &sbuf) < 0)
		err(1, "failed to stat %s", log);
	fd = open(log, O_RDWR);
	if (fd < 0)
		err(1, "failed to open %s", log);

	buf = malloc(sbuf.st_size);

	rc = read(fd, buf, sbuf.st_size);
	if (rc != sbuf.st_size)
		err(1, "log file read");

	if (reclaim_log)
		dump_reclaim_log(buf, sbuf.st_size);
	if (reclaim_prog_log) {
		modified = dump_reclaim_prog_log(buf, sbuf.st_size, batchno, id);
		if (modified) {
			lseek(fd, 0, SEEK_SET);
			rc = write(fd, buf, sbuf.st_size);
			if (rc != sbuf.st_size)
				err(1, "log file write");
		}
	}

	if (close(fd) == -1)
		err(1, "log file close");
	exit(0);
}
