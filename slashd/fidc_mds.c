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
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "slashd.h"

int fcoo_priv_size = sizeof(struct fcoo_mds_info);

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

struct fcoo_mds_info *
fidc_fcmh2fmi(struct fidc_membh *fcmh)
{
	struct fcoo_mds_info *fmi = NULL;
	int locked;

	locked = reqlock(&fcmh->fcmh_lock);
	if (!fcmh->fcmh_fcoo)
		goto out;

	if (fidc_fcoo_wait_locked(fcmh, FCOO_NOSTART) < 0)
		goto out;

	fmi = fcoo_get_pri(fcmh->fcmh_fcoo);
 out:
	ureqlock(&fcmh->fcmh_lock, locked);
	return (fmi);
}

struct fcoo_mds_info *
fidc_fid2fmi(slfid_t f, struct fidc_membh **fcmh)
{
	*fcmh = fidc_lookup_simple(f);

	if (!*fcmh)
		return (NULL);
	return (fidc_fcmh2fmi(*fcmh));
}

int
slm_fidc_getattr(struct fidc_membh *fcmh,
    const struct slash_creds *cr)
{
	struct fcoo_mds_info *fmi;

	fmi = fcmh_2_fmi(fcmh);
	return (mdsio_lookup_slfid(fcmh->fcmh_fg.fg_fid, cr,
	    &fcmh->fcmh_sstb, &fcmh->fcmh_fg.fg_gen,
	    &fmi->fmi_mdsio_fid));
}

int
slm_fcmh_get(const struct slash_fidgen *fg, struct slash_creds *crp,
    struct fidc_membh **fcmhp)
{
	struct fcoo_mds_info *fmi;
	int rc;

	rc = fidc_lookup(fg, FIDC_LOOKUP_CREATE | FIDC_LOOKUP_LOAD |
	    FIDC_LOOKUP_FCOOSTART, NULL, FCMH_SETATTRF_NONE, crp, fcmhp);
	if (rc == 0) {
		struct fidc_membh *fcmh = *fcmhp;

		FCMH_LOCK(fcmh);
		if (fcmh->fcmh_state & FCMH_FCOO_STARTING) {

 init_fmi:
			fmi = fcoo_get_pri(fcmh->fcmh_fcoo);
			SPLAY_INIT(&fmi->fmi_exports);
			atomic_set(&fmi->fmi_refcnt, 0);

			slash_inode_handle_init(&fmi->fmi_inodeh, fcmh,
			    mds_inode_sync);

			fidc_fcoo_startdone(fcmh);
		} else {
			rc = fidc_fcoo_wait_locked(fcmh, FCOO_START);
			if (rc == 1)
				goto init_fmi;
		}
		if (rc == 0)
			rc = mds_fcmh_tryref_fmi(fcmh);
		FCMH_ULOCK(fcmh);
	}
	return (rc);
}

void
slm_fcmh_release(struct fidc_membh *fcmh)
{
	if (fcmh) {
		mds_inode_release(fcmh);
		fcmh_dropref(fcmh);
	}
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	slm_fidc_getattr,
/* grow */	NULL,
/* shrink */	NULL
};
