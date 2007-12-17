/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_types.h"
#include "psc_util/log.h"
#include "psc_util/assert.h"

#include "config.h"
#include "fid.h"

#define FID_PRINT_CHAR 16

/**
 * fid_makepath() creates the path from the fid's
 *   inum, allowing the fs to easily access files via their
 *   inode number.
 */
void
fid_makepath(const slash_fid_t *fid, char *fid_path)
{
	char  *c;
	u64    mask = 0x000000000000000f;
	int    i, rc;

	rc = snprintf(fid_path, FID_PATH_LEN,
		      "%s/%016"ZLPX64"/",
		      nodeProfile->znprof_objroot,
		      nodeInfo->znode_set_uuid);

	psc_assert(rc >= 0 && rc < FID_PATH_LEN);

	for (i = 0, c = fid_path + rc; i < FID_PATH_DEPTH; i++, c++) {

		u8 j = ( (fid->fid_inum & (mask << (i*4)) ) >> (i*4));

		if (j < 10)
			j += 0x30;
		else
			j += 0x57;
		*c     = (char)j;
		*(++c) = '/';
        }
	rc = snprintf(c, FID_PRINT_CHAR + 1, "%016"ZLPX64,
		      fid->fid_inum);

	psc_assert_msg(rc == FID_PRINT_CHAR, "rc == %d", rc);

	psc_info("Fidpath ;%s; %016"ZLPX64,
	      fid_path, fid->fid_inum);
}

/**
 * fid_link() places the fid (via inum) into the
 *   namespace server's filesystem.
 */
int
fid_link(const slash_fid_t *fid, const char *fnam)
{
	char fid_path[FID_PATH_LEN+1];
	int  rc = 0;

	fid_makepath(fid, fid_path);

	rc = link(fnam, fid_path);
	if (!rc)
		psc_info("linked userf ;%s; to fid_path ;%s; for fid %"ZLPX64,
		      fnam, fid_path, fid->fid_inum);
	else {
		rc = -errno;
		if (errno == EEXIST) {
			struct stat stb;

			psc_info("already linked fid_path %s for fid %"ZLPX64,
			      fid_path, fid->fid_inum);

#ifdef ZEST
			if (stat(fid_path, &stb) == -1) {
				psc_error("Failed stat() of ;%s;", fid_path);
				return -errno;
			}

			if (stb.st_ino != fid->fid_inum) {
				psc_error("BAD! Immutable namespace "
					  "corruption! %"ZLPX64" != %"ZLPX64,
					  fid->fid_inum, stb.st_ino);
                                return -EINVAL;
			} else
#endif
				return 0;

		} else {
			psc_error("failed to link fid_path %s for fid %"ZLPX64,
				  fid_path, fid->fid_inum);
		}
	}
	return rc;
}
