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
#include <sys/xattr.h>

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
 * fid_makepath - build the pathname in the FID object root that corresponds
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

	psc_dbg("fid=%"PRIx64" fidpath=%s", fg->fg_fid, fid_path);
}

/**
 * fid_link - create an entry in the FID object root corresponding to a
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

#if SLASH_XATTR
int
fid_getxattr(const char *fidfn, const char *name, void *buf, ssize_t len)
{
	psc_crc64_t crc;
	ssize_t szrc;

	szrc = lgetxattr(fidfn, name, buf, len);

	if (szrc < 0) {
		psc_warn("lu fid(%s:%s) failed", fidfn, name);
		return (-1);

	} else if (szrc != len) {
		psc_warn("lu fid(%s:%s) failed bad sz (%zd)",
			 fidfn, name, szrc);
		errno = EIO;
		return (-1);
	}
	return (0);
}
#endif
