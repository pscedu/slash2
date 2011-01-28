/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/stat.h>

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

void
mds_fcmh_increase_fsz(struct fidc_membh *fcmh, off_t siz)
{
	sl_bmapno_t nb;
	int locked;

	locked = FCMH_RLOCK(fcmh);
	if (siz > fcmh_2_fsz(fcmh)) {
		nb = siz / SLASH_BMAP_SIZE -
		    fcmh_2_fsz(fcmh) / SLASH_BMAP_SIZE;
		if (nb > fcmh->fcmh_sstb.sst_nxbmaps)
			nb = fcmh->fcmh_sstb.sst_nxbmaps;
		fcmh->fcmh_sstb.sst_nxbmaps -= nb;
		fcmh_2_fsz(fcmh) = siz;
	}
	FCMH_URLOCK(fcmh, locked);
}

int
slm_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_mds_info *fmi;
	int rc;

	fmi = fcmh_2_fmi(fcmh);
	memset(fmi, 0, sizeof(*fmi));
	psc_dynarray_init(&fmi->fmi_ptrunc_clients);

	rc = mdsio_lookup_slfid(fcmh_2_fid(fcmh), &rootcreds,
	    &fcmh->fcmh_sstb, &fcmh_2_mdsio_fid(fcmh));
	if (rc) {
		fcmh->fcmh_flags |= FCMH_CTOR_FAILED;
		fmi->fmi_ctor_rc = rc;
		DEBUG_FCMH(PLL_WARN, fcmh,
		    "mdsio_lookup_slfid failed (rc=%d)", rc);
		return (rc);
	}

	if (fcmh_isdir(fcmh))
		rc = mdsio_opendir(fcmh_2_mdsio_fid(fcmh),
		    &rootcreds, NULL, &fmi->fmi_mdsio_data);
	else if (fcmh_isreg(fcmh)) {
		slash_inode_handle_init(&fmi->fmi_inodeh, fcmh);
		rc = mdsio_opencreate(fcmh_2_mdsio_fid(fcmh),
		    &rootcreds, O_RDWR, 0, NULL, NULL, NULL,
		    &fcmh_2_mdsio_data(fcmh), NULL, NULL);
		if (rc == 0) {
			rc = mds_inode_read(&fmi->fmi_inodeh);
			if (rc)
				psc_fatalx("could not load inode; rc=%d", rc);
		} else {
			fcmh->fcmh_flags |= FCMH_CTOR_FAILED;
			fmi->fmi_ctor_rc = rc;
			DEBUG_FCMH(PLL_WARN, fcmh,
			   "mdsio_opencreate failed (rc=%d)", rc);
		}
	} else
		DEBUG_FCMH(PLL_INFO, fcmh, "special file, no zfs obj");

	return (rc);
}

void
slm_fcmh_dtor(struct fidc_membh *fcmh)
{
	struct fcmh_mds_info *fmi;
	int rc;

	fmi = fcmh_2_fmi(fcmh);
	psc_assert(psc_dynarray_len(&fmi->fmi_ptrunc_clients) == 0);
	psc_dynarray_free(&fmi->fmi_ptrunc_clients);

	if (S_ISREG(fcmh->fcmh_sstb.sst_mode) ||
	    S_ISDIR(fcmh->fcmh_sstb.sst_mode)) {
		/* XXX Need to worry about other modes here */
		if (!fmi->fmi_ctor_rc) {
			rc = mdsio_release(&rootcreds,
			    fmi->fmi_mdsio_data);
			psc_assert(rc == 0);
		}
	}
	if (fmi->fmi_inodeh.inoh_extras)
		PSCFREE(fmi->fmi_inodeh.inoh_extras);
}

void
dump_ino(const struct slash_inode_od *ino)
{
	char buf[BUFSIZ];

	_debug_ino(buf, sizeof(buf), ino);
	printf("%s\n", buf);
}

static __inline void
dump_inoh(const struct slash_inode_handle *ih)
{
	char buf[BUFSIZ];

	_debug_ino(buf, sizeof(buf), &ih->inoh_ino);
	printf("fl:"INOH_FLAGS_FMT" %s\n", DEBUG_INOH_FLAGS(ih), buf);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slm_fcmh_ctor,
/* dtor */		slm_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL,
/* modify */		NULL
};
