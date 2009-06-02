/* $Id: metadir.c 5315 2009-02-24 08:46:59Z yanovich $ */

#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_types.h"
#include "psc_util/assert.h"
#include "fid.h"

static void
slimmns_create_int(const char *fn, u32 curdepth, u32 maxdepth)
{
	char d[PATH_MAX];
	int i, rc;

	for (i = 0; i < 16; i++) {
		rc = snprintf(d, sizeof(d), "%s/%x", fn, i);
		if ((rc == -1) || (rc >= (int)sizeof(d)))
			psc_fatal("snprintf");

		if ((mkdir(d, 0711) == -1) && (errno != EEXIST))
			psc_fatal("mkdir %s", d);

		if (curdepth < maxdepth)
			slimmns_create_int(d, curdepth + 1, maxdepth);
	}
}

/*
 * Routine for creating the directory structure
 *  on a mapserver filesystem.
 */
int
slimmns_create(const char *root, u32 depth)
{
	char fn[PATH_MAX];
	int rc;

	if (!depth)
		depth = FID_PATH_DEPTH;

	rc = snprintf(fn, sizeof(fn), "%s/%s",
	    root, FID_PATH_NAME);
	psc_assert(rc != -1 && rc < PATH_MAX);

	rc = mkdir(fn, 0711);
	if (rc == -1 && errno != EEXIST)
		psc_fatal("mkdir %s", fn);
	slimmns_create_int(fn, 1, depth);
	return (0);
}

int 
main(int argc, char *argv[])
{
	if (argc != 2) {
		fprintf(stderr, "Usage: %s /slashfs_root_dir\n", argv[0]);
		return (-1);
	}
	
	return (slimmns_create(argv[1], 0));
}
