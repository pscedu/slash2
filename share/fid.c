/* $Id$ */

#include <sys/param.h>
#include <sys/xattr.h>

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "psc_util/log.h"
#include "psc_util/mkdirs.h"

#include "fid.h"
#include "slconfig.h"
#include "pathnames.h"
#include "fidcache.h"

#define FP_DEPTH 3
#define BPHXC    4

#define SL_PATH_MAX 128

/**
 * fid_makepath - build the pathname in the FID object root that corresponds
 *	to a FID, allowing easily lookup of file metadata via FIDs.
 */
void
fid_makepath(slfid_t fid, char *fid_path)
{
	int rc, i;
	char a[FP_DEPTH];

	a[0] = (uint8_t)((fid & 0x0000000000f00000ULL) >> (BPHXC*5));
	a[1] = (uint8_t)((fid & 0x00000000000f0000ULL) >> (BPHXC*4));
	a[2] = (uint8_t)((fid & 0x000000000000f000ULL) >> (BPHXC*3));

	for (i=0; i < FP_DEPTH; i++)
		a[i] = (a[i] < 10) ? (a[i] += 0x30) : (a[i] += 0x57);

	rc = snprintf(fid_path, SL_PATH_MAX, "%s/%s/%c/%c/%c/%016"PRIx64,
	      nodeInfo.node_res->res_fsroot, FID_PATH_NAME,
	      a[0], a[1], a[2], fid);

	psc_trace("fid=%"PRIx64" fidpath=;%s;", fid, fid_path);
	if (rc == -1)
		psc_fatal("snprintf");
}


/**
 * fid_fileops - create or open a fid on the IO server.
 * @fid: the numeric id.
 * @flags: open options.
 */
int
fid_fileops(slfid_t fid, int flags)
{
	char fidfn[SL_PATH_MAX];

	fid_makepath(fid, fidfn);

	return (open(fidfn, flags));
}

/**
 * fid_fileops_fg - create or open a fid on the IO server using the generation
 *    number as a file suffix.
 * @fg: file ID and generation.
 * @flags: open options.
 */
int
fid_fileops_fg(struct slash_fidgen *fg, int flags)
{
	char fidfn[SL_PATH_MAX];

	fid_makepath(fg->fg_fid, fidfn);
	snprintf((fidfn + strlen(fidfn)), SL_PATH_MAX,
		 "_%"PRIx64, fg->fg_gen);

	return (open(fidfn, flags));
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
	char *p, fidpath[SL_PATH_MAX];

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



#if SLASH_XATTR
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
#endif
