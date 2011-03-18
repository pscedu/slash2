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

#include <sys/param.h>
#include <sys/stat.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_util/log.h"

#include "fid.h"

void
_meta_dir_create(uint32_t curdepth, uint32_t maxdepth)
{
	int  i;
	char d[2];

	for (i = 0; i < 16; i++) {
		snprintf(d, 2, "%x", i);

		if (mkdir(d, 0700) == -1)
			psc_fatal("mkdir %s", d);

		if (curdepth < maxdepth - 1) {
			if (chdir(d) == -1)
				psc_fatal("chdir %s", d);
			_meta_dir_create(curdepth + 1, maxdepth);
		}
	}

	if (curdepth > 1)
		if (chdir("..") == -1)
			psc_fatal("chdir ..");
}

/*
 * Routine for creating the directory structure
 *  on a mapserver filesystem.
 */
int
meta_dir_create(const char *meta_root, uint64_t fs_set_uuid,
    uint32_t meta_dir_depth)
{
	char wd[PATH_MAX], meta_fsid_str[FSID_LEN+1];
	int  rc;

	if (getcwd(wd, sizeof(wd)) == NULL)
		psc_fatal("getcwd");

	if (chdir(meta_root) == -1)
		psc_fatal("chdir: %s", meta_root);

	if (!meta_dir_depth)
		meta_dir_depth = FID_PATH_DEPTH;

	rc = snprintf(meta_fsid_str, FSID_LEN+1, SLPRI_FSID, fs_set_uuid);
	psc_assert(rc == FSID_LEN);

	rc = mkdir(meta_fsid_str, 0700);
	if (rc < 0 && errno != EEXIST)
		psc_fatal("mkdir %s", meta_fsid_str);

	if (chdir(meta_fsid_str) == -1)
		psc_fatal("chdir: %s", meta_fsid_str);
	_meta_dir_create(1, meta_dir_depth);
	if (chdir(wd) == -1)
		psc_fatal("chdir: %s", wd);

	return (0);
}
