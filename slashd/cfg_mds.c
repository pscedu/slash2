/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/multiwait.h"

#include "mdslog.h"
#include "slashd.h"
#include "slconfig.h"

void
slcfg_init_res(struct sl_resource *res)
{
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;

	rpmi = res2rpmi(res);
	INIT_SPINLOCK(&rpmi->rpmi_lock);

	if (res->res_type == SLREST_MDS) {
		peerinfo = PSCALLOC(sizeof(*peerinfo));
		rpmi->rpmi_info = peerinfo;
		peerinfo->sp_flags = SPF_NEED_JRNL_INIT;
		psc_meter_init(&peerinfo->sp_batchmeter, 0, "nsupd-%s",
		    res->res_name);
		peerinfo->sp_batchmeter.pm_maxp =
		    &current_update_batchno;
	} else {
		iosinfo = PSCALLOC(sizeof(*iosinfo));
		rpmi->rpmi_info = iosinfo;
		iosinfo->si_flags = SIF_NEED_JRNL_INIT;
		psc_meter_init(&iosinfo->si_batchmeter, 0, "reclaim-%s",
		    res->res_name);
		iosinfo->si_batchmeter.pm_maxp =
		    &current_reclaim_batchno;
	}
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct resm_mds_info *rmmi;

	rmmi = resm2rmmi(resm);
	psc_mutex_init(&rmmi->rmmi_mutex);
	psc_multiwaitcond_init(&rmmi->rmmi_mwcond,
	    NULL, 0, "rmmi-%s", resm->resm_res->res_name);
	atomic_set(&rmmi->rmmi_refcnt, 0);
	rmmi->rmmi_resm = resm;
}

void
slcfg_init_site(struct sl_site *site)
{
	struct site_mds_info *smi;

	smi = site2smi(site);
	psc_dynarray_init(&smi->smi_upq);
	INIT_SPINLOCK(&smi->smi_lock);
	psc_multiwait_init(&smi->smi_mw, "smi-%s",
	    site->site_name + strspn(site->site_name, "@"));
	psc_multiwaitcond_init(&smi->smi_mwcond, NULL, 0, "smi-%s",
	    site->site_name + strspn(site->site_name, "@"));
}

int	 cfg_site_pri_sz = sizeof(struct site_mds_info);
int	 cfg_res_pri_sz = sizeof(struct resprof_mds_info);
int	 cfg_resm_pri_sz = sizeof(struct resm_mds_info);
