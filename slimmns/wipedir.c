/* $Id$ */

#undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */

#include <sys/types.h>
#include <sys/stat.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fts.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "psc_util/log.h"

#include "pathnames.h"
#include "mkfn.h"

extern int ion;

void
wipefs(const char *dir)
{
	char *pathv[] = { (char *)dir, NULL };
	char fn[PATH_MAX];
	FTSENT *f;
	FTS *fp;

	/* first remove the user namespace */
	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", dir);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			/* skip slash namespace directories */
			if (strncmp(f->fts_name, SL_PATH_PREFIX,
			    strlen(SL_PATH_PREFIX)) == 0)
				fts_set(fp, f, FTS_SKIP);
			else if ((f->fts_info & FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
		} else if (unlink(f->fts_path) == -1)
			psc_error("unlink %s", f->fts_path);
	}
	fts_close(fp);

	/* remove the slash fid namespace */
	xmkfn(fn, "%s/%s", dir, SL_PATH_FIDNS);
	pathv[0] = fn;

	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", fn);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			if ((f->fts_info & FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
			/* do not descend into hardlinked directories */
			else if (f->fts_level > 3)
				fts_set(fp, f, FTS_SKIP);
		} else if (unlink(f->fts_path) == -1)
			psc_error("unlink %s", f->fts_path);
	}
	fts_close(fp);

	if (ion)
		return;

	/* remove the slash replication queue */
	xmkfn(fn, "%s/%s", dir, SL_PATH_REPLS);
	pathv[0] = fn;

	/* XXX security implications of FTS_NOCHDIR? */
	fp = fts_open(pathv, FTS_COMFOLLOW | FTS_NOCHDIR | FTS_PHYSICAL,
	    NULL);
	if (fp == NULL)
		psc_fatal("fts_open %s", fn);
	while ((f = fts_read(fp)) != NULL) {
		if (f->fts_level < 1)
			continue;
		if (S_ISDIR(f->fts_statp->st_mode)) {
			if ((f->fts_info & FTS_DP) &&
			    rmdir(f->fts_path) == -1)
				psc_fatal("rmdir %s", f->fts_path);
			/* do not descend into hardlinked directories */
			else if (f->fts_level > 1)
				fts_set(fp, f, FTS_SKIP);
		} else if (unlink(f->fts_path) == -1)
			psc_error("unlink %s", f->fts_path);
	}
	fts_close(fp);
}
