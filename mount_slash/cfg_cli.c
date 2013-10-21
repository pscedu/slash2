/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "pfl/listcache.h"
#include "pfl/alloc.h"
#include "pfl/lock.h"

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
	struct sl_resource *r = resm->resm_res;
	struct resm_cli_info *rmci;

	rmci = resm2rmci(resm);
	if (resm->resm_type == SLREST_ARCHIVAL_FS) {
		lc_reginit(&rmci->rmci_async_reqs, struct slc_async_req,
		    car_lentry, "aiorq-%s:%d", r->res_name,
		    psc_dynarray_len(&r->res_members));
	}
	psc_atomic32_set(&rmci->rmci_infl_rpcs, 0);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz;
int	 cfg_res_pri_sz = sizeof(struct resprof_cli_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_cli_info);
