/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/journal.h"
#include "pfl/log.h"
#include "pfl/odtable.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/str.h"
#include "pfl/sys.h"

#include "creds.h"
#include "fid.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slconfig.h"
#include "slerr.h"

void wipefs(const char *);

int		 wipe;
int		 ion;
struct passwd	*pw;
uint64_t	 fsuuid;
sl_ios_id_t	 resid = IOS_ID_ANY;

struct psc_journal_cursor cursor;

void
slnewfs_mkdir(const char *fn)
{
	if (mkdir(fn, 0700) == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);
	if (pw && chown(fn, pw->pw_uid, pw->pw_gid) == -1)
		psclog_warn("chown %u %s", pw->pw_uid, fn);
}

void
slnewfs_create_int(const char *pdirnam, uint32_t curdepth,
    uint32_t maxdepth)
{
	char subdirnam[PATH_MAX];
	int i;

	for (i = 0; i < 16; i++) {
		xmkfn(subdirnam, "%s/%x", pdirnam, i);
		slnewfs_mkdir(subdirnam);
		if (curdepth < maxdepth)
			slnewfs_create_int(subdirnam, curdepth + 1,
			    maxdepth);
	}
}

void
slnewfs_touchfile(const char *fmt, ...)
{
	char fn[PATH_MAX];
	va_list ap;
	int fd;

	va_start(ap, fmt);
	xmkfnv(fn, fmt, ap);
	va_end(ap);

	fd = open(fn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd == -1)
		psc_fatal("%s", fn);
	close(fd);
}

/*
 * Create a SLASH2 metadata or user data directory structure.
 */
void
slnewfs_create(const char *fsroot, uint32_t depth)
{
	char *p, metadir[PATH_MAX], fn[PATH_MAX], strtm[32];
	time_t tm;
	FILE *fp;
	int fd;

	if (!depth)
		depth = FID_PATH_DEPTH;

	/* create root metadata directory */
	xmkfn(metadir, "%s/%s", fsroot, SL_RPATH_META_DIR);
	slnewfs_mkdir(metadir);

	/* create immutable namespace top directory */
	if (ion) {
		xmkfn(fn, "%s/%"PRIx64, metadir, fsuuid);
		slnewfs_mkdir(fn);

		strlcpy(metadir, fn, sizeof(metadir));
	}
	xmkfn(fn, "%s/%s", metadir, SL_RPATH_FIDNS_DIR);
	slnewfs_mkdir(fn);

	/* create immutable namespace subdirectories */
	slnewfs_create_int(fn, 1, depth);

	/* create the RESID file */
	xmkfn(fn, "%s/%s", metadir, SL_FN_RESID);
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	fprintf(fp, "%#x\n", resid);
	fclose(fp);

	/* creation time */
	xmkfn(fn, "%s/%s", metadir, "timestamp");
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	if (fchmod(fileno(fp), 0600) == -1)
		psclog_warn("chown %u %s", pw->pw_uid, fn);
	tm = cursor.pjc_timestamp;
	ctime_r(&tm, strtm);
	p = strchr(strtm, '\n');
	if (p)
		*p = '\0';
	fprintf(fp, "This pool was created %s on %s\n", strtm,
	    psc_hostname);
	fclose(fp);

	if (ion)
		return;

	/* create the FSUUID file */
	xmkfn(fn, "%s/%s", metadir, SL_FN_FSUUID);
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	if (!fsuuid)
		fsuuid = psc_random64();
	fprintf(fp, "%#18"PRIx64"\n", fsuuid);
	if (!ion)
		printf("The UUID of the file system is %#18"PRIx64"\n",
		    fsuuid);
	fclose(fp);

	/* create temporary processing directory */
	xmkfn(fn, "%s/%s", metadir, SL_RPATH_TMP_DIR);
	slnewfs_mkdir(fn);

	/* create the journal cursor file */
	xmkfn(fn, "%s/%s", metadir, SL_FN_CURSOR);
	fd = open(fn, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd == -1)
		psc_fatal("open %s", fn);
	if (pw && fchown(fd, pw->pw_uid, pw->pw_gid) == -1)
		psclog_warn("chown %u %s", pw->pw_uid, fn);

	memset(&cursor, 0, sizeof(struct psc_journal_cursor));
	cursor.pjc_magic = PJRNL_CURSOR_MAGIC;
	cursor.pjc_version = PJRNL_CURSOR_VERSION;
	cursor.pjc_timestamp = time(NULL);
	cursor.pjc_fid = SLFID_MIN;
	FID_SET_SITEID(cursor.pjc_fid, sl_resid_to_siteid(resid));
	if (pwrite(fd, &cursor, sizeof(cursor), 0) != sizeof(cursor))
		psc_fatal("write %s", fn);
	close(fd);

	/* more journals */
	slnewfs_touchfile("%s/%s.%d", metadir, SL_FN_UPDATELOG, 0);
	slnewfs_touchfile("%s/%s", metadir, SL_FN_UPDATEPROG);
	slnewfs_touchfile("%s/%s.%d", metadir, SL_FN_RECLAIMLOG, 0);
	slnewfs_touchfile("%s/%s", metadir, SL_FN_RECLAIMPROG);

	xmkfn(fn, "%s/%s", metadir, SL_FN_BMAP_ODTAB);
	pfl_odt_create(fn, ODT_ITEM_COUNT, ODT_ITEM_SIZE, wipe,
	    ODT_ITEM_START, 0, ODTBL_OPT_CRC);
}

__dead void
usage(void)
{
	extern const char *__progname;

	fprintf(stderr,
	    "usage: %s [-iW] [-R siteid:resid] [-u fsuuid] fsroot\n",
	    __progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *endp, *s_resid;
	sl_siteid_t site_id;
	long l;
	int c;

	pfl_init();
	sl_errno_init();
	pscthr_init(0, NULL, 0, "slmkfs");
	while ((c = getopt(argc, argv, "iI:R:u:W")) != -1)
		switch (c) {
		case 'I':
		case 'R':
			s_resid = strchr(optarg, ':');
			if (s_resid == NULL)
				errx(1, "-I %s: ID must be specified in "
				    "the format `SITE_ID:RESOURCE_ID'",
				    optarg);
			*s_resid++ = '\0';

			if (strncmp(optarg, "0x", 2))
				errx(1, "-I %s: SITE_ID must be in "
				    "hexadecimal format", optarg);
			endp = NULL;
			l = strtoul(optarg, &endp, 16);
			if (endp == optarg || *endp || l > UINT16_MAX)
				errx(1, "%s: invalid SITE_ID", optarg);
			site_id = l;

			if (strncmp(s_resid, "0x", 2))
				errx(1, "-I %s: RESOURCE_ID must be in "
				    "hexadecimal format", s_resid);
			endp = NULL;
			l = strtoul(s_resid, &endp, 16);
			if (endp == s_resid || *endp || l > UINT16_MAX)
				errx(1, "-I %s: invalid RESOURCE_ID",
				    s_resid);
			resid = sl_global_id_build(site_id, l);
			break;
		case 'i':
			ion = 1;
			break;
		case 'u':
			if (strncmp(optarg, "0x", 2))
				errx(1, "%s: FSUUID must be in "
				    "hexadecimal format", optarg);
			endp = NULL;
			fsuuid = strtoull(optarg, &endp, 16);
			if (endp == optarg || *endp)
				errx(1, "%s: invalid FSUUID", optarg);
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

	if (ion && !fsuuid)
		errx(1, "fsuuid (-u) must be specified for I/O servers");
	if (resid == IOS_ID_ANY)
		errx(1, "resource ID (-R) must be specified in the "
		    "format `SITE_ID:RESOURCE_ID' e.g. 0x1:0x1001");

	sl_getuserpwent(&pw);

	if (wipe)
		wipefs(argv[0]);
	slnewfs_create(argv[0], 0);
	exit(0);
}
