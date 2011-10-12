/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "psc_ds/listcache.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "bmap_cli.h"
#include "mount_slash.h"
#include "slconfig.h"

void
slcfg_init_res(struct sl_resource *res)
{
	struct resprof_cli_info *rpci;

	rpci = res2rpci(res);
	psc_dynarray_init(&rpci->rpci_pinned_bmaps);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct resm_cli_info *rmci;

	rmci = resm2rmci(resm);
	psc_dynarray_init(&resm->resm_nids);
	psc_mutex_init(&rmci->rmci_mutex);
	psc_multiwaitcond_init(&rmci->rmci_mwc, resm, PMWCF_WAKEALL,
	    "csvc-%s", resm->resm_res->res_name);
	lc_reginit(&rmci->rmci_async_reqs, struct slc_async_req,
	    car_lentry, "aiorq-%s:%d", resm->resm_res->res_name, 0);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz = sizeof(struct resprof_cli_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_cli_info);
