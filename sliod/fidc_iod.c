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

#include "fidc_iod.h"
#include "fidcache.h"

int fcoo_priv_size = sizeof(struct fcoo_iod_info);

int
sli_fcmh_grow(void)
{
	rlim_t soft, hard;
	int rc;

	/* increase #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft + 1, hard + 1) == -1)
		psc_warn("setrlimit NOFILE %"PRId64, soft + 1);
	freelock(&psc_rlimit_lock);
	return (rc);
}

void
sli_fcmh_shrink(void)
{
	rlim_t soft, hard;
	int rc;

	/* decrease #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft - 1, hard - 1) == -1)
		psc_warn("setrlimit NOFILE %"PRId64, soft - 1);
	freelock(&psc_rlimit_lock);
}

/*
 * fcmh_load_fii - Associate an fcmh with a file handle to a data
 *	store for the file on the local file system.
 * @f: FID cache member handle of file to open.
 * @rw: read or write operation.
 */
int
fcmh_load_fii(struct fidc_membh *fcmh, enum rw rw)
{
	int flags, rc, locked;
	char fidfn[PATH_MAX];

	locked = FCMH_RLOCK(fcmh);
	rc = fcmh_load_fcoo(fcmh, rw);
	if (rc <= 0) {
		FCMH_URLOCK(fcmh, locked);
		return (rc);
	}
	FCMH_ULOCK(fcmh);

	flags = O_RDWR;
	if (rw == SL_WRITE)
		flags |= O_CREAT;

	fg_makepath(&fcmh->fcmh_fg, fidfn);
	fcmh_2_fd(fcmh) = open(fidfn, flags, 0600);
	if (fcmh_2_fd(fcmh) == -1) {
		rc = errno;
		fidc_fcoo_startfailed(fcmh);
	} else
		fidc_fcoo_startdone(fcmh);
	if (locked)
		FCMH_LOCK(fcmh);
	return (rc);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	NULL,
/* grow */	sli_fcmh_grow,
/* shrink */	sli_fcmh_shrink
};
