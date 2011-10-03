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

#undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fts.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "psc_util/log.h"

#include "pathnames.h"
#include "mkfn.h"

extern int ion;

void
wipefs(const char *dir)
{
	char *pathv[] = { (char *)dir, NULL };
	char fn[PATH_MAX];
	FTSENT *f;
	FTS *fp;

	/* first remove the user namespace */
	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", dir);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			/* skip SLASH internal metadata */
			if (strcmp(f->fts_name, SL_RPATH_META_DIR) == 0)
				fts_set(fp, f, FTS_SKIP);
			else if ((f->fts_info == FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
		} else if (unlink(f->fts_path) == -1)
			psclog_error("unlink %s", f->fts_path);
	}
	fts_close(fp);

	/* remove the SLASH fid namespace */
	xmkfn(fn, "%s/%s", dir, SL_RPATH_META_DIR, SL_RPATH_FIDNS_DIR);
	pathv[0] = fn;

	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", fn);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			if ((f->fts_info == FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
			/* do not descend into hardlinked directories */
			else if (f->fts_level > 6)
				fts_set(fp, f, FTS_SKIP);
		} else if (unlink(f->fts_path) == -1)
			psclog_error("unlink %s", f->fts_path);
	}
	fts_close(fp);

	if (ion)
		return;

	/* remove the slash replication queue */
	xmkfn(fn, "%s/%s", dir, SL_RPATH_META_DIR, SL_RPATH_UPSCH_DIR);
	pathv[0] = fn;

	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", fn);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			if ((f->fts_info == FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
			/* do not descend into hardlinked directories */
			else if (f->fts_level > 1)
				fts_set(fp, f, FTS_SKIP);
		} else if (unlink(f->fts_path) == -1)
			psclog_error("unlink %s", f->fts_path);
	}
	fts_close(fp);
}
