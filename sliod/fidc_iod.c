/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/resource.h>

#include <fcntl.h>
#include <stddef.h>

#include "psc_util/log.h"
#include "psc_util/rlimit.h"

#include "sltypes.h"
#include "slutil.h"
#include "fidc_iod.h"
#include "fidcache.h"

int
sli_fcmh_getattr(struct fidc_membh *fcmh) {
	struct stat stb;

	if (fstat(fcmh_2_fd(fcmh), &stb))
		return (-errno);

	sl_externalize_stat(&stb, &fcmh->fcmh_sstb);

	FCMH_LOCK(fcmh);
	fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
	FCMH_ULOCK(fcmh);

	return (0);
}


int
sli_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_iod_info *fii;
	char fidfn[PATH_MAX];
	int incr, rc = 0;

	incr = psc_rlim_adj(RLIMIT_NOFILE, 1);

	/* try to get an file descriptor for this backing obj */
	fii = fcmh_2_fii(fcmh);
	memset(fii, 0, sizeof(struct fcmh_iod_info));

	fg_makepath(&fcmh->fcmh_fg, fidfn);
	fcmh_2_fd(fcmh) = open(fidfn, O_CREAT | O_RDWR, 0600);
	if (fcmh_2_fd(fcmh) == -1)
		rc = errno;
       	
	/* oops, an error; if we increased the rlim, decrease it */
	if (rc && incr)		
		psc_rlim_adj(RLIMIT_NOFILE, -1);
	else
		rc = sli_fcmh_getattr(fcmh);

	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *f)
{
	close(fcmh_2_fd(f));
	psc_rlim_adj(RLIMIT_NOFILE, -1);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		sli_fcmh_ctor,
/* dtor */		sli_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL
};
