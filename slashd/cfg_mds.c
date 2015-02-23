/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/alloc.h"
#include "pfl/dynarray.h"
#include "pfl/lock.h"
#include "pfl/multiwait.h"
#include "pfl/rpc.h"

#include "journal_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"

void
slcfg_init_res(struct sl_resource *r)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_peerinfo *sp;
	struct sl_mds_iosinfo *si;

	rpmi = res2rpmi(r);
	psc_mutex_init(&rpmi->rpmi_mutex);
	psc_waitq_init(&rpmi->rpmi_waitq);

	if (r->res_type == SLREST_MDS) {
		rpmi->rpmi_info = sp = PSCALLOC(sizeof(*sp));
		sp->sp_flags = SPF_NEED_JRNL_INIT;
		psc_meter_init(&sp->sp_batchmeter, 0, "nsupd-%s",
		    r->res_name);
	} else {
		rpmi->rpmi_info = si = PSCALLOC(sizeof(*si));
		si->si_flags = SIF_NEED_JRNL_INIT;
		if (RES_ISFS(r)) 
			psc_meter_init(&si->si_batchmeter, 0,
			    "reclaim-%s", r->res_name);
		if (r->res_flags & RESF_DISABLE_BIA)
			si->si_flags |= SIF_DISABLE_LEASE;
	}
	if (RES_ISFS(r) || r->res_type == SLREST_MDS)
		lc_reginit(&rpmi->rpmi_batchrqs, struct batchrq,
		    br_lentry, "bchrq-%s", r->res_name);
}

void
slcfg_init_resm(struct sl_resm *m)
{
	struct resm_mds_info *rmmi;

	rmmi = resm2rmmi(m);
	psc_atomic32_set(&rmmi->rmmi_refcnt, 0);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz = sizeof(struct site_mds_info);
int	 cfg_res_pri_sz = sizeof(struct resprof_mds_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_mds_info);
