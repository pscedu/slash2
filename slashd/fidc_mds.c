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

#include <stdio.h>

#include "pfl/cdefs.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"

#if 0
__static int
fidc_xattr_load(slfid_t fid, sl_inodeh_t *inoh)
{
	char fidfn[FID_MAX_PATH];
	ssize_t sz=sizeof(struct slash_inode_od);
	psc_crc64_t crc;
	int rc;

	fid_makepath(fid, fidfn);

	rc = fid_getxattr(fidfn, SFX_INODE,  &inoh->inoh_ino, sz);
	if (rc)
		return (rc);

	PSC_CRC_CALC(crc, &inoh->inoh_ino, sz);
	if (crc != inoh->inoh_ino.ino_crc) {
		psc_warnx("Crc failure on inode");
		errno = EIO;
		return -1;
	}
	/* XXX move me
	if (inoh->inoh_ino.ino_nrepls) {
		sz = sizeof(sl_replica_t) * inoh->inoh_ino.ino_nrepls;
		inoh->inoh_replicas = PSCALLOC(sz);
		rc = fid_getxattr(fidfn, SFX_INODE,  inoh->inoh_replicas, sz);

		PSC_CRC_CALC(&crc, inoh->inoh_replicas, sz);
		if (crc != inoh->inoh_ino.ino_rs_crc) {
			psc_warnx("Crc failure on replicas");
			errno = EIO;
			return -1;
		}
	}
	*/
	return (0);
}
#endif

struct fidc_mds_info *
fidc_fid2fmdsi(slfid_t f, struct fidc_membh **fcmh)
{
	struct fidc_mds_info *fmdsi=NULL;
	int locked;

	*fcmh = fidc_lookup_simple(f);

	if (!*fcmh)
		return NULL;

	locked = reqlock(&(*fcmh)->fcmh_lock);
	if (!(*fcmh)->fcmh_fcoo)
		goto out;

	if (fidc_fcoo_wait_locked((*fcmh), FCOO_NOSTART) < 0)
		goto out;

	psc_assert((*fcmh)->fcmh_fcoo->fcoo_pri);
	fmdsi = (*fcmh)->fcmh_fcoo->fcoo_pri;
 out:
	ureqlock(&(*fcmh)->fcmh_lock, locked);
	return (fmdsi);
}

struct fidc_mds_info *
fidc_fcmh2fmdsi(struct fidc_membh *fcmh)
{
	struct fidc_mds_info *fmdsi=NULL;
	int locked;

	fmdsi = NULL;
	locked = reqlock(&fcmh->fcmh_lock);
	if (!fcmh->fcmh_fcoo)
		goto out;

	if (fidc_fcoo_wait_locked(fcmh, FCOO_NOSTART) < 0)
		goto out;

	psc_assert(fcmh->fcmh_fcoo->fcoo_pri);
	fmdsi = fcmh->fcmh_fcoo->fcoo_pri;
 out:
	ureqlock(&fcmh->fcmh_lock, locked);
	return (fmdsi);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	NULL,
/* grow */	NULL,
/* shrink */	NULL
};
