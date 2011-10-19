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

int
_mds_fcmh_setattr(struct fidc_membh *f, int to_set,
    const struct srt_stat *sstb, int log)
{
	int rc = 0;

	FCMH_LOCK_ENSURE(f);
	psc_assert((f->fcmh_flags & FCMH_IN_SETATTR) == 0);

	f->fcmh_flags |= FCMH_IN_SETATTR;

	DEBUG_FCMH(PLL_INFO, f, "attributes updated, writing");

	FCMH_ULOCK(f);
	if (log)
		mds_reserve_slot(1);
	rc = mdsio_setattr(fcmh_2_mdsio_fid(f), sstb, to_set,
	    &rootcreds, &f->fcmh_sstb, fcmh_2_mdsio_data(f),
	    log ? mdslog_namespace : NULL);
	if (log)
		mds_unreserve_slot(1);
	FCMH_LOCK(f);
	psc_assert(f->fcmh_flags & FCMH_IN_SETATTR);

	f->fcmh_flags &= ~FCMH_IN_SETATTR;
	fcmh_wake_locked(f);
	return (rc);
}

int
slm_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_mds_info *fmi;
	int rc;

	DEBUG_FCMH(PLL_INFO, fcmh, "ctor");

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
		rc = mdsio_opendir(fcmh_2_mdsio_fid(fcmh), &rootcreds,
		    NULL, &fmi->fmi_mdsio_data);
	else if (fcmh_isreg(fcmh)) {
		slash_inode_handle_init(&fmi->fmi_inodeh, fcmh);
		rc = mdsio_opencreate(fcmh_2_mdsio_fid(fcmh),
		    &rootcreds, O_RDWR, 0, NULL, NULL, NULL,
		    &fcmh_2_mdsio_data(fcmh), NULL, NULL, 0);
		if (rc == 0) {
			rc = mds_inode_read(&fmi->fmi_inodeh);
			if (rc)
				DEBUG_FCMH(PLL_WARN, fcmh,
				    "could not load inode; rc=%d", rc);
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

int
_slm_fcmh_endow(struct fidc_membh *p, struct fidc_membh *c, int log)
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

	FCMH_LOCK(c);
	if (fcmh_isdir(c)) {
		struct srt_stat sstb;

		sstb.sstd_freplpol = pol;
//		c->nrepls =
//		c->memcpy();
		mds_fcmh_setattr(c, SL_SETATTRF_FREPLPOL, &sstb);
	} else {
//		fcmh_wait_locked(c, c->fcmh_flags & FCMH_IN_SETATTR);
		fcmh_2_ino(c)->ino_replpol = pol;
//		fcmh_2_ino(c)->ino_nrepls = 1;
//		memcpy(fcmh_2_ino(c)->ino_repls, repls, sizeof());
		if (log)
			rc = mds_inode_write(fcmh_2_inoh(c), mdslog_ino_repls, c);
//		if (log)
//			rc = mds_inox_write(fcmh_2_inoh(c), mdslog_ino_repls, c);
	}
	FCMH_ULOCK(c);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMH_IN_PTRUNC, &flags, &seq);
	PFL_PRFLAG(FCMH_IN_SETATTR, &flags, &seq);
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
