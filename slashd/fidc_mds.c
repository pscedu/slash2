/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <stdio.h>

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "psc_util/atomic.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "slashd.h"

#include "zfs-fuse/zfs_slashlib.h"

int
slfid_to_vfsid(slfid_t fid, int *vfsid)
{
	int i, siteid;

	/*
	 * Our client uses this special fid to contact us during mount,
	 * at which time it does not know the site ID yet.
	 */
	if (fid == SLFID_ROOT) {
		*vfsid = current_vfsid;
		return (0);
	}

	/* only have default file system in the root */
	if (mount_index == 1) {
		*vfsid = current_vfsid;
		return (0);
	}

	siteid = FID_GET_SITEID(fid);
	for (i = 0; i < mount_index; i++) {
		if (zfsMount[i].siteid == (uint64_t)siteid) {
			*vfsid = i;
			return (0);
		}
	}
	return (-EINVAL);
}

int
_mds_fcmh_setattr(int vfsid, struct fidc_membh *f, int to_set,
    const struct srt_stat *sstb, int log)
{
	struct srt_stat sstb_out;
	int rc;

	FCMH_LOCK_ENSURE(f);
	FCMH_BUSY_ENSURE(f);
	FCMH_ULOCK(f);

	if (log)
		mds_reserve_slot(1);
	rc = mdsio_setattr(vfsid, fcmh_2_mdsio_fid(f), sstb, to_set,
	    &rootcreds, &sstb_out, fcmh_2_mdsio_data(f),
	    log ? mdslog_namespace : NULL);
	if (log)
		mds_unreserve_slot(1);

	if (!rc) {
		FCMH_LOCK(f);
		f->fcmh_sstb = sstb_out;
		FCMH_ULOCK(f);
	}

	return (rc);
}

int
slm_fcmh_ctor(struct fidc_membh *f, int flags)
{
	struct fcmh_mds_info *fmi;
	int rc, vfsid;

	DEBUG_FCMH(PLL_INFO, f, "ctor");

	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	if (rc) {
		DEBUG_FCMH(PLL_WARN, f, "invalid file system ID "
		    "(rc=%d)", rc);
		return (rc);
	}
	fmi = fcmh_2_fmi(f);
	memset(fmi, 0, sizeof(*fmi));
	psc_dynarray_init(&fmi->fmi_ptrunc_clients);

	rc = mdsio_lookup_slfid(vfsid, fcmh_2_fid(f), &rootcreds,
	    &f->fcmh_sstb, &fcmh_2_mdsio_fid(f));
	if (rc) {
		fmi->fmi_ctor_rc = rc;
		if ((flags & FIDC_LOOKUP_NOLOG) == 0)
			DEBUG_FCMH(PLL_WARN, f,
			    "mdsio_lookup_slfid failed (rc=%d)", rc);
		return (rc);
	}

	if (fcmh_isdir(f)) {
		rc = mdsio_opendir(vfsid, fcmh_2_mdsio_fid(f),
		    &rootcreds, NULL, &fmi->fmi_mdsio_data);
	} else if (fcmh_isreg(f)) {
		slash_inode_handle_init(&fmi->fmi_inodeh, f);
		/*
		 * We shouldn't need O_LARGEFILE because SLASH2
		 * metafiles are small.
		 *
		 * I created a file with size of 8070450532247928832
		 * using dd by seeking to a large offset and writing one
		 * byte.  Somehow, the ZFS size becomes 5119601018368.
		 * Without O_LARGEFILE, I got EOVERFLOW (75) here.  The
		 * SLASH2 size is correct though.
		 */
		rc = mdsio_opencreate(vfsid, fcmh_2_mdsio_fid(f),
		    &rootcreds, O_RDWR, 0, NULL, NULL, NULL,
		    &fcmh_2_mdsio_data(f), NULL, NULL, 0);
		if (rc == 0) {
			rc = mds_inode_read(&fmi->fmi_inodeh);
			if (rc)
				DEBUG_FCMH(PLL_WARN, f,
				    "could not load inode; rc=%d", rc);
		} else {
			fmi->fmi_ctor_rc = rc;
			DEBUG_FCMH(PLL_WARN, f,
			    "mdsio_opencreate failed (mf=%"PRIx64" rc=%d)",
			    fcmh_2_mdsio_fid(f), rc);
		}
	} else
		DEBUG_FCMH(PLL_INFO, f, "special file, no zfs obj");

	return (rc);
}

void
slm_fcmh_dtor(struct fidc_membh *f)
{
	struct fcmh_mds_info *fmi;
	int rc, vfsid;

	fmi = fcmh_2_fmi(f);
	psc_assert(psc_dynarray_len(&fmi->fmi_ptrunc_clients) == 0);
	psc_dynarray_free(&fmi->fmi_ptrunc_clients);

	if (S_ISREG(f->fcmh_sstb.sst_mode) ||
	    S_ISDIR(f->fcmh_sstb.sst_mode)) {
		/* XXX Need to worry about other modes here */
		if (!fmi->fmi_ctor_rc) {
			slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
			rc = mdsio_release(vfsid, &rootcreds,
			    fmi->fmi_mdsio_data);
			psc_assert(rc == 0);
		}
	}
	if (fmi->fmi_inodeh.inoh_extras)
		PSCFREE(fmi->fmi_inodeh.inoh_extras);
}

/**
 * _slm_fcmh_endow - "Endow" or apply inheritance to a new directory
 *	entry from its parent directory replica layout.
 * Note: the bulk of this is empty until we have a place to store such
 * info in the SLASH2 metafile.
 */
int
_slm_fcmh_endow(int vfsid, struct fidc_membh *p, struct fidc_membh *c,
    int log)
{
//	sl_replica_t repls[SL_MAX_REPLICAS];
	uint32_t pol;
//	int nrepls;
	int rc = 0;

	FCMH_LOCK(p);
	pol = p->fcmh_sstb.sstd_freplpol;
//	nrepls =
//	memcpy();
	FCMH_ULOCK(p);

	FCMH_WAIT_BUSY(c);
	if (fcmh_isdir(c)) {
		struct srt_stat sstb;

		sstb.sstd_freplpol = pol;
//		c->nrepls =
//		c->memcpy();
		mds_fcmh_setattr(vfsid, c, SL_SETATTRF_FREPLPOL, &sstb);
	} else {
		fcmh_2_ino(c)->ino_replpol = pol;
//		fcmh_2_ino(c)->ino_nrepls = 1;
//		memcpy(fcmh_2_ino(c)->ino_repls, repls, sizeof());
		FCMH_ULOCK(c);
		if (log)
			rc = mds_inode_write(vfsid, fcmh_2_inoh(c),
			    mdslog_ino_repls, c);
//		if (log)
//			rc = mds_inox_write(fcmh_2_inoh(c), mdslog_ino_repls, c);
	}
	FCMH_UNBUSY(c);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMH_IN_PTRUNC, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}
#endif

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slm_fcmh_ctor,
/* dtor */		slm_fcmh_dtor,
/* getattr */		NULL,
/* postsetattr */	NULL,
/* modify */		NULL
};
