/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
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

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl/walk.h"
#include "pfl/log.h"

#include "pathnames.h"
#include "mkfn.h"

extern int ion;

int
wipefs_user(FTSENT *f, __unusedx void *arg)
{
	const char *p, *fn;
	int rc = 0;

	fn = f->fts_path;
	if (f->fts_level < 1)
		return (0);
	if (f->fts_info == FTS_D || f->fts_info == FTS_DP) {
		/* skip SLASH2 internal metadata */
		p = strrchr(fn, '/');
		if (p)
			p++;
		else
			p = fn;
		if (strcmp(p, SL_RPATH_META_DIR) == 0)
			pfl_fts_set(f, FTS_SKIP);
		else if (f->fts_info == FTS_DP && rmdir(fn) == -1)
			psc_fatal("rmdir %s", fn);
	} else if (unlink(fn) == -1)
		psclog_error("unlink %s", fn);
	return (rc);
}

int
wipefs_fidns(FTSENT *f, __unusedx void *arg)
{
	int rc = 0, skiplevel = ion ? 7 : 6;
	const char *fn;

	fn = f->fts_path;
	if (f->fts_level < 1)
		return (0);
	if (f->fts_info == FTS_D || f->fts_info == FTS_DP) {
		if (f->fts_info == FTS_DP && rmdir(fn) == -1)
			psc_fatal("rmdir %s", fn);

		/*
		 * Do not descend into hardlinked directories.
		 *
		 * MDS:
		 *	0 -> dir
		 *	1 -> dir/.slmd
		 *	2 -> dir/.slmd/fidns
		 *	3 -> dir/.slmd/fidns/0
		 *	4 -> dir/.slmd/fidns/0/1
		 *	5 -> dir/.slmd/fidns/0/1/2
		 *	6 -> dir/.slmd/fidns/0/1/2/3
		 *	7 -> dir/.slmd/fidns/0/1/2/3/file
		 *
		 * IO:
		 *	0 -> dir
		 *	1 -> dir/.slmd
		 *	2 -> dir/.slmd/fsuuid
		 *	3 -> dir/.slmd/fsuuid/fidns
		 *	4 -> dir/.slmd/fsuuid/fidns/0
		 *	5 -> dir/.slmd/fsuuid/fidns/0/1
		 *	6 -> dir/.slmd/fsuuid/fidns/0/1/2
		 *	7 -> dir/.slmd/fsuuid/fidns/0/1/2/3
		 *	8 -> dir/.slmd/fsuuid/fidns/0/1/2/3/file
		 */
		else if (f->fts_level > skiplevel)
			pfl_fts_set(f, FTS_SKIP);
	} else if (unlink(fn) == -1)
		psclog_error("unlink %s", fn);
	return (rc);
}

void
wipefs(const char *dir)
{
	char fn[PATH_MAX];
	struct stat stb;

	/*
	 * Remove the user namespace.  We do this separately because we
	 * skip fidns because of loops.
	 */
	pfl_filewalk(dir, PFL_FILEWALKF_RECURSIVE, NULL, wipefs_user,
	    NULL);

	/* remove the SLASH2 FID namespace */
	xmkfn(fn, "%s/%s", dir, SL_RPATH_META_DIR, SL_RPATH_FIDNS_DIR);
	if (stat(fn, &stb) == 0 || errno != ENOENT)
		pfl_filewalk(fn, PFL_FILEWALKF_RECURSIVE, NULL,
		    wipefs_fidns, NULL);
}
