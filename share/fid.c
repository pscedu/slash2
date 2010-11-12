/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2010, Pittsburgh Supercomputing Center (PSC).
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

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "psc_util/log.h"
#include "psc_util/mkdirs.h"

#include "fid.h"
#include "fidcache.h"
#include "mkfn.h"
#include "pathnames.h"
#include "slconfig.h"

/**
 * fid_makepath - Build the pathname in the FID object root that corresponds
 *	to a FID, allowing easily lookup of file metadata via FIDs.
 */
void
_fg_makepath(const struct slash_fidgen *fg, char *fid_path, int usegen)
{
	char a[FID_PATH_DEPTH];
	int i;

	a[0] = (fg->fg_fid & UINT64_C(0x0000000000f00000)) >> (BPHXC * 5);
	a[1] = (fg->fg_fid & UINT64_C(0x00000000000f0000)) >> (BPHXC * 4);
	a[2] = (fg->fg_fid & UINT64_C(0x000000000000f000)) >> (BPHXC * 3);

	for (i=0; i < FID_PATH_DEPTH; i++)
		a[i] += a[i] < 10 ? 0x30 : 0x57;

	if (usegen)
		xmkfn(fid_path, "%s/%s/%c/%c/%c/%016"PRIx64"_%"PRIx64,
		    globalConfig.gconf_fsroot, FID_PATH_NAME,
		    a[0], a[1], a[2], fg->fg_fid, fg->fg_gen);
	else
		xmkfn(fid_path, "%s/%s/%c/%c/%c/%016"PRIx64,
		    globalConfig.gconf_fsroot, FID_PATH_NAME,
		    a[0], a[1], a[2], fg->fg_fid);

	psclog_dbg("fid="SLPRI_FID" fidpath=%s", fg->fg_fid, fid_path);
}

/**
 * fid_link - Create an entry in the FID object root corresponding to a
 *	pathname in the file system.
 * @fid: file ID.
 * @fn: filename for which to create FID object entry.
 */
int
fid_link(slfid_t fid, const char *fn)
{
	char *p, fidpath[PATH_MAX];
	struct slash_fidgen fg;

	fg.fg_fid = fid;
	fid_makepath(&fg, fidpath);
	if ((p = strrchr(fidpath, '/')) != NULL) {
		*p = '\0';
		if (mkdirs(fidpath, 0711) == -1) /* XXX must be done as root */
			return (-1);
		*p = '/';
	}
	if (link(fn, fidpath) == -1) {
		if (errno == EEXIST)
			psc_error("tried to recreate already existing fidpath");
		else
			psc_fatal("link %s", fidpath);
	}
	return (0);
}
