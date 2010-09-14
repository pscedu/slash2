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
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "dircache.h"

struct dircache_mgr dircacheMgr;

/**
 * fcmh_setlocalsize - Apply a local WRITE update to a fid cache member
 *	handle.
 */
void
fcmh_setlocalsize(struct fidc_membh *h, uint64_t size)
{
	int locked;

	locked = reqlock(&h->fcmh_lock);
	if (size > fcmh_2_fsz(h))
		fcmh_2_fsz(h) = size;
	ureqlock(&h->fcmh_lock, locked);
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
	psc_assert(!(fcmh->fcmh_flags & FCMH_CLI_INITDCI));

	INIT_PSCLIST_HEAD(&fci->fci_dci.di_list);
	INIT_SPINLOCK(&fci->fci_dci.di_lock);
	fci->fci_dci.di_dcm = &dircacheMgr;
	fci->fci_dci.di_fcmh = fcmh;
	fcmh->fcmh_flags |= FCMH_CLI_INITDCI;

	FCMH_URLOCK(fcmh, locked);
}

int
slc_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_cli_info *fci;

	fci = fcmh_get_pri(fcmh);
	memset(fci, 0, sizeof(*fci));
	slc_fcmh_refresh_age(fcmh);

	if (fcmh_isdir(fcmh))
		slc_fcmh_initdci(fcmh);
	return (0);
}

void
slc_fcmh_dtor(struct fidc_membh *fcmh)
{
	int locked = FCMH_RLOCK(fcmh);

	if (fcmh_isdir(fcmh) && DIRCACHE_INITIALIZED(fcmh)) {
		struct fcmh_cli_info *fci;

		fci = fcmh_get_pri(fcmh);

		psc_assert(psc_listhd_empty(&fci->fci_dci.di_list));
	}
	FCMH_URLOCK(fcmh, locked);
}

int
slc_fcmh_getattr(struct fidc_membh *fcmh)
{
	return (slash2fuse_stat(fcmh, &rootcreds));
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slc_fcmh_ctor,
/* dtor */		slc_fcmh_dtor,
/* getattr */		slc_fcmh_getattr,
/* postsetattr */	slc_fcmh_refresh_age
};
