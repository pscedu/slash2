/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_ds/dynarray.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/multilock.h"

#include "mdsexpc.h"
#include "slashd.h"
#include "slconfig.h"

void
slcfg_init_res(struct sl_resource *res)
{
	struct mds_resprof_info *mrpi;

	mrpi = res->res_pri = PSCALLOC(sizeof(*mrpi));
	LOCK_INIT(&mrpi->mrpi_lock);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct mds_resm_info *mrmi;

	mrmi = resm->resm_pri = PSCALLOC(sizeof(*mrmi));
	LOCK_INIT(&mrmi->mrmi_lock);
	psc_multiwaitcond_init(&mrmi->mrmi_mwcond,
	    NULL, 0, "mrmi-%s", resm->resm_addrbuf);
	psc_dynarray_init(&mrmi->mrmi_bmaps);
	psc_dynarray_init(&mrmi->mrmi_bmaps_deref);
	atomic_set(&mrmi->mrmi_refcnt, 0);
	mrmi->mrmi_resm = resm;
}

void
slcfg_init_site(struct sl_site *site)
{
	struct mds_site_info *msi;

	msi = site->site_pri = PSCALLOC(sizeof(*msi));
	psc_dynarray_init(&msi->msi_replq);
	LOCK_INIT(&msi->msi_lock);
	psc_multiwait_init(&msi->msi_mw, "msi-%s",
	    site->site_name + strspn(site->site_name, "@"));
	psc_multiwaitcond_init(&msi->msi_mwcond, NULL, 0, "msi-%s",
	    site->site_name + strspn(site->site_name, "@"));
}
