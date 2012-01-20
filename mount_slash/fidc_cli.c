/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2012, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>
#include <string.h>

#include "pfl/cdefs.h"
#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "pfl/time.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"

#include "cache_params.h"
#include "dircache.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"

/*
 * XXX check client attributes when generation number changes
 *
 */

struct dircache_mgr dircacheMgr;

/**
 * fcmh_setlocalsize - Apply a local WRITE update to a fid cache member
 *	handle.
 */
void
fcmh_setlocalsize(struct fidc_membh *fcmh, uint64_t size)
{
	int locked;

	locked = reqlock(&fcmh->fcmh_lock);
	if (size > fcmh_2_fsz(fcmh)) {
		psclog_info("fid: "SLPRI_FID", "
		    "size from %"PRId64" to %"PRId64,
		    fcmh_2_fid(fcmh), fcmh_2_fsz(fcmh), size);
		fcmh_2_fsz(fcmh) = size;
	}
	ureqlock(&fcmh->fcmh_lock, locked);
}

void
slc_fcmh_refresh_age(struct fidc_membh *fcmh)
{
	struct timeval tmp = { FCMH_ATTR_TIMEO, 0 };
	struct fcmh_cli_info *fci;

	fci = fcmh_2_fci(fcmh);
	PFL_GETTIMEVAL(&fci->fci_age);
	timeradd(&fci->fci_age, &tmp, &fci->fci_age);
}

void
slc_fcmh_initdci(struct fidc_membh *fcmh)
{
	struct fcmh_cli_info *fci;
	int locked;

	fci = fcmh_get_pri(fcmh);

	locked = FCMH_RLOCK(fcmh);
	psc_assert(fcmh_isdir(fcmh));

	if (!(fcmh->fcmh_flags & FCMH_CLI_INITDCI)) {
		INIT_SPINLOCK(&fci->fci_dci.di_lock);
		pll_init(&fci->fci_dci.di_list, struct dircache_ents,
		    de_lentry, &fci->fci_dci.di_lock);
		fci->fci_dci.di_dcm = &dircacheMgr;
		fci->fci_dci.di_fcmh = fcmh;
		fcmh->fcmh_flags |= FCMH_CLI_INITDCI;
	}

	FCMH_URLOCK(fcmh, locked);
}

int
slc_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct sl_site *s;
	sl_siteid_t siteid;
	int i;

	fci = fcmh_get_pri(fcmh);
	slc_fcmh_refresh_age(fcmh);
	siteid = FID_GET_SITEID(fcmh_2_fid(fcmh));
	if (fcmh_2_fid(fcmh) >= SLFID_MIN &&
	    siteid != slc_rmc_resm->resm_siteid) {
		s = libsl_siteid2site(siteid);
		if (s == NULL) {
			psclog_errorx("fid "SLPRI_FID" has "
			    "invalid site ID %d",
			    fcmh_2_fid(fcmh), siteid);
			return (ESTALE);
		}
		SITE_FOREACH_RES(s, res, i)
			if (res->res_type == SLREST_MDS) {
				fci->fci_resm = psc_dynarray_getpos(
				    &res->res_members, 0);
				return (0);
			}
		psclog_errorx("fid "SLPRI_FID" has invalid site ID %d",
		    fcmh_2_fid(fcmh), siteid);
		return (ESTALE);
	}
	fci->fci_resm = slc_rmc_resm;
	INIT_PSC_LISTENTRY(&fci->fci_lentry);
	return (0);
}

void
slc_fcmh_dtor(struct fidc_membh *fcmh)
{
	if (fcmh_isdir(fcmh) && DIRCACHE_INITIALIZED(fcmh)) {
		struct fcmh_cli_info *fci;

		fci = fcmh_get_pri(fcmh);
		psc_assert(pll_empty(&fci->fci_dci.di_list));
	}
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMH_CLI_HAVEREPLTBL, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_FETCHREPLTBL, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_INITDCI, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_TRUNC, &flags, &seq);
	if (flags)
		printf(" unknown: %x", flags);
	printf("\n");
}
#endif

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slc_fcmh_ctor,
/* dtor */		slc_fcmh_dtor,
/* getattr */		msl_stat,
/* postsetattr */	slc_fcmh_refresh_age,
/* modify */		NULL
};
