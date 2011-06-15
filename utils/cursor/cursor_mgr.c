/* $Id$ */
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

#include "psc_util/journal.h"

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-dv] [-x txg] -f cursor_file\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *cursor_file = NULL, c;
	int dump = 0, verbose = 0, fd, rc;
	uint64_t newtxg;
	struct psc_journal_cursor cursor;

	progname = argv[0];

	while ((c = getopt(argc, argv, "dvf:x:")) != -1)
		switch (c) {
		case 'd':
			dump = 1;
			break;
		case 'f':
			cursor_file = optarg;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'x':
			newtxg = strtol(optarg, NULL, 10);
			break;
		default:
			usage();
		}

	argc -= optind;
	argv += optind;
	if (argc || !cursor_file)
		usage();

	fd = open(cursor_file, O_RDWR);
	if (fd < 0)
		err(1, "failed to open %s", cursor_file);

	rc = pread(fd, &cursor, sizeof(struct psc_journal_cursor), 0);
	if (rc != sizeof(struct psc_journal_cursor))
		err(1, "cursor file pread() failed");

	if (dump || verbose) {
		printf("Cursor Contents:\n"
			"\tpjc_magic = %"PRIx64"\n"
			"\tpjc_version = %"PRIx64"\n"
			"\tpjc_timestamp = %"PRId64"\n"
			"\tpjc_uuid = %s\n"
			"\tpjc_commit_txg =  %"PRId64"\n"
			"\tpjc_distill_xid =  %"PRId64"\n"
			"\tpjc_fid = %"PRIx64"\n"
			"\tpjc_seqno_lwm = %"PRIx64"\n"
			"\tpjc_seqno_hwm = %"PRIx64"\n"
			"\tpjc_tail = %"PRIx64"\n"
			"\tpjc_update_seqno = %"PRIx64"\n"
			"\tpjc_reclaim_seqno = %"PRIx64"\n"
			"\tpjc_replay_xid = %"PRIx64"\n",
			cursor.pjc_magic, cursor.pjc_version, cursor.pjc_timestamp,
			cursor.pjc_uuid, cursor.pjc_commit_txg, cursor.pjc_distill_xid,
			cursor.pjc_fid, cursor.pjc_seqno_lwm, cursor.pjc_seqno_hwm,
			cursor.pjc_tail, cursor.pjc_update_seqno,
			cursor.pjc_reclaim_seqno, cursor.pjc_replay_xid);

		if (dump)
			exit(0);
	}

	cursor.pjc_commit_txg = newtxg;
	rc = pwrite(fd, &cursor, sizeof(struct psc_journal_cursor), 0);
	if (rc != sizeof(struct psc_journal_cursor))
		err(1, "cursor file pwrite() failed");

	exit(0);
}
