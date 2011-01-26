/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

#undef _FILE_OFFSET_BITS	/* FTS is not 64-bit ready */

#include <sys/param.h>
#include <sys/stat.h>

#include <err.h>
#include <fts.h>
#include <stdio.h>
#include <stdlib.h>

#include "pfl/str.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/ctlcli.h"
#include "psc_util/log.h"

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
			if (f->fts_info == FTS_NS) {
				warnx("%s: %s", f->fts_path,
				    strerror(f->fts_errno));
				continue;
			}
			if (S_ISREG(f->fts_statp->st_mode) ||
			    S_ISDIR(f->fts_statp->st_mode)) {
				if (realpath(f->fts_path, buf) == NULL)
					warn("%s", f->fts_path);
				else {
					if (verbose)
						warnx("processing %s", buf);
					cbf(buf, arg);
				}
			}
		}
		fts_close(fp);
	} else {
		if (stat(fn, &stb) == -1)
			err(1, "%s", fn);
		else if (!S_ISREG(stb.st_mode) && !S_ISDIR(stb.st_mode))
			errx(1, "%s: not a file or directory", fn);
		else if (realpath(fn, buf) == NULL)
			err(1, "%s", fn);
		else
			cbf(buf, arg);
	}
}
