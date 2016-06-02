/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2015, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"

#include "bmap_cli.h"
#include "mount_slash.h"
#include "slconfig.h"

void
slc_init_rpci(struct resprof_cli_info *rpci)
{
	INIT_SPINLOCK(&rpci->rpci_lock);
	psc_waitq_init(&rpci->rpci_waitq, "rpci");
}

void
slc_destroy_rpci(struct resprof_cli_info *rpci)
{
	psc_waitq_destroy(&rpci->rpci_waitq);
}

void
slcfg_init_res(struct sl_resource *res)
{
	struct resprof_cli_info *rpci;

	rpci = res2rpci(res);
	slc_init_rpci(rpci);
}

void
slcfg_destroy_res(struct sl_resource *res)
{
	struct resprof_cli_info *rpci;

	rpci = res2rpci(res);
	slc_destroy_rpci(rpci);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct sl_resource *r = resm->resm_res;
	struct resm_cli_info *rmci;

	rmci = resm2rmci(resm);
	if (resm->resm_type == SLREST_ARCHIVAL_FS)
		lc_reginit(&rmci->rmci_async_reqs, struct slc_async_req,
		    car_lentry, "slash2/aiorq-%s:%d", r->res_name,
		    psc_dynarray_len(&r->res_members));
}

void
slcfg_destroy_resm(struct sl_resm *resm)
{
	struct resm_cli_info *rmci;

	rmci = resm2rmci(resm);
	if (resm->resm_type == SLREST_ARCHIVAL_FS)
		pfl_listcache_destroy_registered(
		    &rmci->rmci_async_reqs);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

void
slcfg_destroy_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz = sizeof(struct resprof_cli_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_cli_info);
