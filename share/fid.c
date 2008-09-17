/* $Id$ */

#include <sys/param.h>
#include <sys/xattr.h>

#include <stdio.h>

#include "psc_util/log.h"
#include "psc_util/mkdirs.h"

#include "fid.h"
#include "slconfig.h"
#include "pathnames.h"
#include "fidcache.h"

/**
 * fid_makepath - build the pathname in the FID object root that corresponds
 *	to a FID, allowing easily lookup of file metadata via FIDs.
 */
void
fid_makepath(slfid_t fid, char *fid_path)
{
	int rc;

	rc = snprintf(fid_path, PATH_MAX,
	    "%s/%s/%04x/%04x/%04x/%04x",
	    nodeInfo.node_res->res_fsroot, _PATH_OBJROOT,
	    (u32)((fid & 0x000000000000ffffULL)),
	    (u32)((fid & 0x00000000ffff0000ULL) >> 16),
	    (u32)((fid & 0x0000ffff00000000ULL) >> 32),
	    (u32)((fid & 0xffff000000000000ULL) >> 48));
	if (rc == -1)
		psc_fatal("snprintf");
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

	fid_makepath(fid, fidpath);
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

int
fid_open(slfid_t fid, int flags)
{
	char fidfn[PATH_MAX];

	psc_assert(!(flags & O_CREAT));
	fid_makepath(fid, fidfn);

	return (open(fidfn, flags));
}

int
fid_getxattr(const char *fidfn, const char *name, void *buf, ssize_t len)
{
	psc_crc_t crc;
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
