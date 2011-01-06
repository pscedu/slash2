/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/stat.h>

#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// #include <uuid/uuid.h>

#include "pfl/pfl.h"
#include "pfl/cdefs.h"
#include "psc_util/hostname.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"

#include "creds.h"
#include "fid.h"
#include "mkfn.h"
#include "pathnames.h"

void wipefs(const char *);

const char	*progname;
int		 wipe;
int		 ion;
struct passwd	*pw;

void
slnewfs_mkdir(const char *fn)
{
	if (mkdir(fn, 0700) == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);
	if (pw)
		chown(fn, pw->pw_uid, pw->pw_gid);
}

void
slimmns_create_int(const char *pdirnam, uint32_t curdepth,
    uint32_t maxdepth)
{
	char subdirnam[PATH_MAX];
	int i;

	for (i = 0; i < 16; i++) {
		xmkfn(subdirnam, "%s/%x", pdirnam, i);
		slnewfs_mkdir(subdirnam);
		if (curdepth < maxdepth)
			slimmns_create_int(subdirnam, curdepth + 1,
			    maxdepth);
	}
}

/**
 * slimmns_create - Create an immutable namespace directory structure.
 */
void
slimmns_create(const char *root, uint32_t depth)
{
	struct psc_journal_cursor cursor;
	char fn[PATH_MAX];
	int fd;

	if (!depth)
		depth = FID_PATH_DEPTH;

	/* create immutable namespace root directory */
	xmkfn(fn, "%s/%s", root, FID_PATH_NAME);
	slnewfs_mkdir(fn);

	/* create immutable namespace subdirectories */
	slimmns_create_int(fn, 1, depth);

	if (ion)
		return;

	/* create replication queue directory */
	xmkfn(fn, "%s/%s", root, SL_PATH_UPSCH);
	slnewfs_mkdir(fn);

	xmkfn(fn, "%s/%s", root, SL_PATH_CURSOR);
	fd = open(fn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd == -1)
		psc_fatal("open %s", fn);
	if (pw)
		fchown(fd, pw->pw_uid, pw->pw_gid);

	memset(&cursor, 0, sizeof(struct psc_journal_cursor));
	cursor.pjc_magic = PJRNL_CURSOR_MAGIC;
	cursor.pjc_version = PJRNL_CURSOR_VERSION;
	cursor.pjc_timestamp = time(NULL);
	cursor.pjc_fid = SLFID_MIN;
	// uuid_generate(cursor.pjc_uuid);
	if (pwrite(fd, &cursor, sizeof(cursor), 0) != sizeof(cursor))
		psc_fatal("write %s", fn);
	close(fd);

	xmkfn(fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
	    SL_FN_UPDATELOG, 0,
	    psc_get_hostname(), cursor.pjc_timestamp);
	fd = open(fn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd == -1)
		psc_fatal("open %s", fn);
	close(fd);

	xmkfn(fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
	    SL_FN_RECLAIMLOG, 0,
	    psc_get_hostname(), cursor.pjc_timestamp);
	fd = open(fn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd == -1)
		psc_fatal("open %s", fn);
	close(fd);
}

__dead void
usage(void)
{
	fprintf(stderr, "usage: %s [-iW] fsroot\n", progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	int c;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "iW")) != -1)
		switch (c) {
		case 'i':
			ion = 1;
			break;
		case 'W':
			wipe = 1;
			break;
		default:
			usage();
		}
	argc -= optind;
	argv += optind;
	if (argc != 1)
		usage();

	sl_getuserpwent(&pw);
	if (pw == NULL)
		psc_error("getpwnam %s", SLASH_UID);

	if (wipe)
		wipefs(argv[0]);
	slimmns_create(argv[0], 0);
	exit(0);
}
