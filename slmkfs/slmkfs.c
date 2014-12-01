/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/hostname.h"
#include "pfl/journal.h"
#include "pfl/log.h"
#include "pfl/odtable.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/str.h"

#include "creds.h"
#include "fid.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slconfig.h"

void wipefs(const char *);

const char	*progname;
int		 wipe;
int		 ion;
struct passwd	*pw;
uint64_t         siteid;
uint64_t         fsuuid;
sl_ios_id_t	 resid = IOS_ID_ANY;
const char      *datadir = SL_PATH_DATA_DIR;

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

/**
 * slnewfs_create - Create a SLASH2 metadata or user data directory
 *	structure.
 */
void
slnewfs_create(const char *fsroot, uint32_t depth)
{
	char metadir[PATH_MAX], fn[PATH_MAX];
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
		xmkfn(fn, "%s/%s", metadir, SL_RPATH_FIDNS_DIR);
		slnewfs_mkdir(fn);
	} else {
		xmkfn(fn, "%s/%s", metadir, SL_RPATH_FIDNS_DIR);
		slnewfs_mkdir(fn);
	}

	/* create immutable namespace subdirectories */
	slnewfs_create_int(fn, 1, depth);

	/* create the RESID file */
	xmkfn(fn, "%s/%s", metadir, SL_FN_RESID);
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	fprintf(fp, "%#x\n", resid);
	fclose(fp);

	if (ion)
		return;

	/* create temporary processing directory */
	xmkfn(fn, "%s/%s", metadir, SL_RPATH_TMP_DIR);
	slnewfs_mkdir(fn);

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

	/* create the SITEID file */
	xmkfn(fn, "%s/%s", metadir, SL_FN_SITEID);
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	fprintf(fp, "%"PRIx64"\n", siteid);
	if (!ion)
		printf("The SITEID of the file system is %#"PRIx64"\n",
		    siteid);
	fclose(fp);

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
	FID_SET_SITEID(cursor.pjc_fid, siteid);
	if (pwrite(fd, &cursor, sizeof(cursor), 0) != sizeof(cursor))
		psc_fatal("write %s", fn);
	close(fd);

	/* more journals */
	slnewfs_touchfile("%s/%s.%d", metadir, SL_FN_UPDATELOG, 0);
	slnewfs_touchfile("%s/%s", metadir, SL_FN_UPDATEPROG);
	slnewfs_touchfile("%s/%s.%d", metadir, SL_FN_RECLAIMLOG, 0);
	slnewfs_touchfile("%s/%s", metadir, SL_FN_RECLAIMPROG);

	/* creation time */
	xmkfn(fn, "%s/%s", metadir, "timestamp");
	fp = fopen(fn, "w");
	if (fp == NULL)
		psc_fatal("open %s", fn);
	if (fchmod(fileno(fp), 0600) == -1)
		psclog_warn("chown %u %s", pw->pw_uid, fn);
	tm = cursor.pjc_timestamp;
	fprintf(fp, "This pool was created %s on %s\n", ctime(&tm),
	    psc_get_hostname());
	fclose(fp);

	xmkfn(fn, "%s/%s", metadir, SL_FN_BMAP_ODTAB);
	pfl_odt_create(fn, 128 * 1024, 128 - 16, wipe, 0x1000, 0,
	    ODTBL_OPT_CRC);

	xmkfn(fn, "%s/%s", metadir, SL_FN_PTRUNC_ODTAB);
	pfl_odt_create(fn, 128 * 1024, 128 - 16, wipe, 0x1000, 0,
	    ODTBL_OPT_CRC);
}

void
slcfg_init_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_init_resm(__unusedx struct sl_resm *resm)
{
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz;
int	 cfg_resm_pri_sz;

int
psc_usklndthr_get_type(__unusedx const char *namefmt)
{
	return (0);
}

void
psc_usklndthr_get_namev(char buf[PSC_THRNAME_MAX],
    __unusedx const char *namefmt, __unusedx va_list ap)
{
	strlcpy(buf, "cfg", PSC_THRNAME_MAX);
}

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-iW] [-c conf] [-D datadir] [-I siteid]\n"
	    "\t[-R resid] [-u fsuuid] fsroot\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	char *cfgfn = NULL, *endp;
	int c, specsid = 0;
	long l;

	pfl_init();
	progname = argv[0];
	while ((c = getopt(argc, argv, "c:D:I:iR:u:W")) != -1)
		switch (c) {
		case 'c':
			cfgfn = optarg;
			break;
		case 'D':
			datadir = optarg;
			break;
		case 'I':
			endp = NULL;
			siteid = strtoull(optarg, &endp, 0);
			if (endp == optarg || *endp)
				errx(1, "%s: invalid SITEID", optarg);
			if (siteid >= (1 << SLASH_FID_SITE_BITS))
				errx(1, "%"PRIu64": SITEID too big",
				    siteid);
			specsid = 1;
			break;
		case 'i':
			ion = 1;
			break;
		case 'R':
			if (strncmp(optarg, "0x", 2))
				errx(1, "%s: RESID must be in "
				    "hexadecimal format", optarg);
			endp = NULL;
			l = strtoul(optarg, &endp, 16);
			if (endp == optarg || *endp ||
			    l == IOS_ID_ANY || l > UINT32_MAX)
				errx(1, "%s: invalid RESID", optarg);
			resid = l;
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

	if (cfgfn)
		slcfg_parse(cfgfn);

	/* on ION, we must specify a uuid */
	if (ion && !fsuuid)
		errx(1, "fsuuid must be specified for I/O servers");
	if (!ion && !specsid)
		errx(1, "site ID must be specified for MDS servers");
	if (resid == IOS_ID_ANY)
		errx(1, "site ID must be specified for MDS servers");

	sl_getuserpwent(&pw);
	if (pw == NULL)
		psclog_warn("getpwnam %s", SLASH_UID);

	if (wipe)
		wipefs(argv[0]);
	slnewfs_create(argv[0], 0);
	exit(0);
}
