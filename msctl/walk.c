/* $Id$ */

#undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */

#include <sys/param.h>
#include <sys/stat.h>

#include <err.h>
#include <fts.h>
#include <stdio.h>
#include <stdlib.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"

#include "mount_slash/ctl_cli.h"
#include "msctl.h"

void
walk(const char *fn, void (*cbf)(const char *, void *), void *arg)
{
	char * const pathv[] = { (char *)fn, NULL };
	char buf[PATH_MAX];
	struct stat stb;
	FTSENT *f;
	FTS *fp;

	if (recursive) {
		/* XXX security implications of FTS_NOCHDIR? */
		fp = fts_open(pathv, FTS_COMFOLLOW |
		    FTS_NOCHDIR | FTS_PHYSICAL, NULL);
		if (fp == NULL)
			psc_fatal("fts_open %s", fn);
		while ((f = fts_read(fp)) != NULL) {
			if (f->fts_info & FTS_NS) {
				warn("%s", f->fts_path);
				continue;
			}
			if (S_ISREG(f->fts_statp->st_mode)) {
				if (realpath(f->fts_path, buf) == NULL)
					warn("%s", f->fts_path);
				else
					cbf(buf, arg);
			}
		}
		fts_close(fp);
	} else {
		if (stat(fn, &stb) == -1)
			warn("%s", fn);
		else if (!S_ISREG(stb.st_mode))
			warnx("%s: not a regular file", fn);
		else if (realpath(fn, buf) == NULL)
			warn("%s", fn);
		else
			cbf(buf, arg);
	}
}
