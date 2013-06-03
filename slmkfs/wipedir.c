/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl/walk.h"
#include "psc_util/log.h"

#include "pathnames.h"
#include "mkfn.h"

extern int ion;

int
wipefs_user(const char *fn, const struct pfl_stat *pst, int info,
    int level, __unusedx void *arg)
{
	const char *p;
	int rc = 0;

	if (level < 1)
		return (0);
	if (S_ISDIR(pst->st_mode)) {
		/* skip SLASH internal metadata */
		p = strrchr(fn, '/');
		if (p)
			p++;
		else
			p = fn;
		if (strcmp(p, SL_RPATH_META_DIR) == 0)
			rc = PFL_FILEWALK_RC_SKIP;
		else if (info == PFWT_DP && rmdir(fn) == -1)
			psc_fatal("rmdir %s", fn);
	} else if (unlink(fn) == -1)
		psclog_error("unlink %s", fn);
	return (rc);
}

int
wipefs_fidns(const char *fn, const struct pfl_stat *pst, int info,
    int level, __unusedx void *arg)
{
	int rc = 0, skiplevel = ion ? 7 : 6;

	if (level < 1)
		return (0);
	if (S_ISDIR(pst->st_mode)) {
		if (info == PFWT_DP && rmdir(fn) == -1)
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
		else if (level > skiplevel)
			rc = PFL_FILEWALK_RC_SKIP;
	} else if (unlink(fn) == -1)
		psclog_error("unlink %s", fn);
	return (rc);
}

void
wipefs(const char *dir)
{
	char fn[PATH_MAX];

	/* remove the user namespace */
	pfl_filewalk(dir, PFL_FILEWALKF_RECURSIVE, wipefs_user, NULL);

	/* remove the SLASH fid namespace */
	xmkfn(fn, "%s/%s", dir, SL_RPATH_META_DIR, SL_RPATH_FIDNS_DIR);
	pfl_filewalk(fn, PFL_FILEWALKF_RECURSIVE, wipefs_fidns, NULL);
}
