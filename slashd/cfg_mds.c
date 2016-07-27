/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/dynarray.h"
#include "pfl/lock.h"
#include "pfl/multiwait.h"
#include "pfl/rpc.h"

#include "batchrpc.h"
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
	psc_waitq_init(&rpmi->rpmi_waitq, "rpmi");

	if (r->res_type == SLREST_MDS) {
		rpmi->rpmi_info = sp = PSCALLOC(sizeof(*sp));
		sp->sp_flags = SPF_NEED_JRNL_INIT;
		pfl_meter_init(&sp->sp_batchmeter, 0, "nsupd-%s",
		    r->res_name);
	} else {
		rpmi->rpmi_info = si = PSCALLOC(sizeof(*si));
		si->si_flags = SIF_NEED_JRNL_INIT;
		if (RES_ISFS(r))
			pfl_meter_init(&si->si_batchmeter, 0,
			    "reclaim-%s", r->res_name);
		if (r->res_flags & RESF_DISABLE_BIA)
			si->si_flags |= SIF_DISABLE_LEASE;
	}
}

void
slcfg_destroy_res(__unusedx struct sl_resource *r)
{
}

void
slcfg_init_resm(struct sl_resm *m)
{
	struct resm_mds_info *rmmi;

	rmmi = resm2rmmi(m);
	psc_atomic32_set(&rmmi->rmmi_refcnt, 0);
}

void
slcfg_destroy_resm(__unusedx struct sl_resm *m)
{
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}

void
slcfg_destroy_site(__unusedx struct sl_site *site)
{
}

int	 cfg_site_pri_sz = sizeof(struct site_mds_info);
int	 cfg_res_pri_sz = sizeof(struct resprof_mds_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_mds_info);
