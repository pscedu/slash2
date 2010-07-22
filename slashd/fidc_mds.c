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

#include <fcntl.h>
#include <stdio.h>

#include "pfl/cdefs.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/rlimit.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "slashd.h"

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

int
slm_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_mds_info *fmi;
	int rc;

	fmi = fcmh_2_fmi(fcmh);
	memset(fmi, 0, sizeof(*fmi));

	rc = mdsio_lookup_slfid(fcmh->fcmh_fg.fg_fid, &rootcreds,
	    &fcmh->fcmh_sstb, &fcmh_2_mdsio_fid(fcmh));
	if (rc) {
		fcmh->fcmh_state |= FCMH_CTOR_FAILED;
		fmi->fmi_ctor_rc = rc;
		DEBUG_FCMH(PLL_WARN, fcmh, "mdsio_lookup_slfid failed (rc=%d)",
			   rc);
		return (rc);
	}
	fcmh->fcmh_fg.fg_gen = fcmh->fcmh_sstb.sst_gen;

	if (fcmh_isdir(fcmh))
		rc = mdsio_opendir(fcmh_2_mdsio_fid(fcmh),
		    &rootcreds, NULL, &fmi->fmi_mdsio_data);
	else if (fcmh_isreg(fcmh)) {
		slash_inode_handle_init(&fmi->fmi_inodeh, fcmh);
		rc = mdsio_opencreate(fcmh_2_mdsio_fid(fcmh),
		    &rootcreds, O_RDWR, 0, NULL, NULL, NULL, NULL,
		    &fcmh_2_mdsio_data(fcmh), NULL, NULL);
		if (rc == 0) {
			rc = mds_inode_read(&fmi->fmi_inodeh);
			if (rc)
				psc_fatalx("could not load inode; rc=%d", rc);
		}
	}
	return (rc);
}

void
slm_fcmh_dtor(struct fidc_membh *fcmh)
{
	struct fcmh_mds_info *fmi;
	int rc;

	fmi = fcmh_2_fmi(fcmh);
	if (!fmi->fmi_ctor_rc) {
		rc = mdsio_release(&rootcreds, fmi->fmi_mdsio_data);
		psc_assert(rc == 0);
	}

	if (fmi->fmi_inodeh.inoh_extras)
		PSCFREE(fmi->fmi_inodeh.inoh_extras);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slm_fcmh_ctor,
/* dtor */		slm_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL
};
