/* $Id$ */

#include <sys/param.h>
#include <sys/stat.h>

#include <unistd.h>
#include <stdio.h>
#include <errno.h>

#include "psc_util/assert.h"
#include "psc_types.h"
#include "slash_fid.h"

void
_meta_dir_create(u32 curdepth, u32 maxdepth)
{
	int  i;
	char d[2];

	for (i = 0; i < 16; i++) {
		snprintf(d, 2, "%x", i);

		if (mkdir(d, 0700) == -1)
			psc_fatal("mkdir %s", d);

		if (curdepth < (maxdepth - 1)) {
			psc_assert_perror( !chdir(d) );
			_meta_dir_create((curdepth + 1), maxdepth);
		}
	}

	if (curdepth > 1)
		psc_assert_perror( !chdir("..") );
}

/*
 * Routine for creating the directory structure
 *  on a mapserver filesystem.
 */
int
meta_dir_create(const char *meta_root,
		u64 fs_set_uuid, u32 meta_dir_depth)
{
	char wd[PATH_MAX], meta_fsid_str[16+1];
	int  rc;

	if (getcwd(wd, sizeof(wd)) == NULL)
		psc_fatal("getcwd");

	psc_assert_perror(chdir(meta_root) == 0);

	if (!meta_dir_depth)
		meta_dir_depth = FID_PATH_DEPTH;

	rc = snprintf(meta_fsid_str, FSID_LEN+1,
		      FSID_FMT, (u64)fs_set_uuid);
	psc_assert(rc == FSID_LEN);

	rc = mkdir(meta_fsid_str, 0700);
	if (rc < 0 && errno != EEXIST)
		psc_assert_perror(0);

	psc_assert_perror(chdir(meta_fsid_str) == 0);
	_meta_dir_create(1, psc_meta_dir_depth);
	psc_assert_perror(chdir(wd) == 0);

	return (0);
}
