/* $Id$ */

#undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */

#include <sys/types.h>
#include <sys/stat.h>

#include <fts.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "psc_util/log.h"

void
wipedir(const char *dir)
{
	char * const pathv[] = { (char *)dir, NULL };
	FTSENT *f;
	FTS *fp;

	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", dir);
	while ((f = fts_read(fp)) != NULL) {
		if (S_ISDIR(f->fts_statp->st_mode)) {
			if ((f->fts_info & FTS_DP) &&
			    strcmp(f->fts_path, dir) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
		} else if (unlink(f->fts_path) == -1)
			psc_fatal("unlink %s", f->fts_path);
	}
	fts_close(fp);
}
