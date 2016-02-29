/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2011-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/journal.h"

#include "fid.h"

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr, "usage: %s [-dv] [-f fid] [-x txg] -c cursor_file\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *p, tmbuf[PFL_CTIME_BUFSIZ], *cursor_file = NULL, c;
	uint64_t newtxg = 0, newfid = 0, fid, cycle, newcycle;
	int dump = 0, verbose = 0, fd, rc;
	struct psc_journal_cursor cursor;

	while ((c = getopt(argc, argv, "c:df:vx:")) != -1)
		switch (c) {
		case 'c':
			cursor_file = optarg;
			break;
		case 'd':
			dump = 1;
			break;
		case 'f':
			newfid = strtol(optarg, NULL, 0);
			break;
		case 'v':
			verbose = 1;
			break;
		case 'x':
			newtxg = strtol(optarg, NULL, 0);
			break;
		default:
			usage();
		}

	argc -= optind;
	if (argc || !cursor_file)
		usage();

	fd = open(cursor_file, O_RDWR);
	if (fd < 0)
		err(1, "failed to open %s", cursor_file);

	rc = pread(fd, &cursor, sizeof(struct psc_journal_cursor), 0);
	if (rc != sizeof(struct psc_journal_cursor))
		err(1, "cursor file read");

	ctime_r((time_t *)&cursor.pjc_timestamp, tmbuf);
	p = strchr(tmbuf, '\n');
	if (p)
		*p = '\0';

	cycle = FID_GET_CYCLE(cursor.pjc_fid);
	if (dump || verbose) {
		printf("Cursor contents of %s:\n"
		    "\tmagic		%"PRIx64"\n"
		    "\tversion		%"PRIx64"\n"
		    "\ttimestamp	%"PRId64" (%s)\n"
		    "\tuuid		%s\n"
		    "\tcommit_txg	%"PRId64"\n"
		    "\tdistill_xid	%"PRId64"\n"
		    "\tfid		%#"PRIx64" "
		      "(flag=%"PRIx64", siteid=%"PRIx64", "
		      "cycle=%"PRIx64", inum=%"PRIx64")\n"
		    "\tseqno_lwm	%"PRIx64"\n"
		    "\tseqno_hwm	%"PRIx64"\n"
		    "\ttail		%"PRIx64"\n"
		    "\tupdate_seqno	%"PRIx64"\n"
		    "\treclaim_seqno	%"PRIx64"\n"
		    "\treplay_xid	%"PRIx64"\n\n",
		    cursor_file,
		    cursor.pjc_magic,
		    cursor.pjc_version,
		    cursor.pjc_timestamp, tmbuf,
		    cursor.pjc_uuid,
		    cursor.pjc_commit_txg,
		    cursor.pjc_distill_xid,
		    cursor.pjc_fid,
		    FID_GET_FLAGS(cursor.pjc_fid),
		    FID_GET_SITEID(cursor.pjc_fid),
		    FID_GET_CYCLE(cursor.pjc_fid),
		    FID_GET_INUM(cursor.pjc_fid),
		    cursor.pjc_seqno_lwm, cursor.pjc_seqno_hwm,
		    cursor.pjc_tail, cursor.pjc_update_seqno,
		    cursor.pjc_reclaim_seqno, cursor.pjc_replay_xid);

		fid = cursor.pjc_fid;
		fid = fid | FID_MAX_INUM;

		printf("The max FID for this cycle is %#"PRIx64"\n", fid);

		if (cycle < ((UINT64_C(1) << SLASH_FID_CYCLE_BITS) - 1)) {
			fid++;
			printf("The first FID for the next cycle is %#"PRIx64"\n", fid);
		}

		if (dump)
			exit(0);
	}

	if (!newtxg && !newfid)
		errx(1, "neither fid nor txg was specified");

	if (newtxg)
		cursor.pjc_commit_txg = newtxg;

	if (newfid) {
		newcycle = FID_GET_CYCLE(newfid);
		if (newcycle <= cycle)
			errx(1, "cycle must be increased when setting a new FID");
		cursor.pjc_fid = newfid;
	}

	rc = pwrite(fd, &cursor, sizeof(struct psc_journal_cursor), 0);
	if (rc != sizeof(struct psc_journal_cursor))
		err(1, "cursor file write");
	if (close(fd) == -1)
		err(1, "cursor file close");
	exit(0);
}
