/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

__static void
slcfg_resm_roundrobin(struct sl_resource *res, struct psc_dynarray *a)
{
	int i, idx;
	struct resprof_mds_info *rpmi = res2rpmi(res);
	struct sl_resm *resm;

	spinlock(&rpmi->rpmi_lock);
	idx = slm_get_rpmi_idx(res);
	freelock(&rpmi->rpmi_lock);

	for (i = 0; i < psc_dynarray_len(&res->res_members); i++, idx++) {
		if (idx >= psc_dynarray_len(&res->res_members))
		    idx = 0;

		resm = psc_dynarray_getpos(&res->res_members, idx);
		psc_dynarray_add_ifdne(a, resm);
	}
}

int
slcfg_get_ioslist(sl_ios_id_t piosid, struct psc_dynarray *a,
    int use_archival)
{
	struct sl_resource *pios, *res;
	int i;

	pios = libsl_id2res(piosid);
	if (!pios || (!RES_ISFS(pios) && !RES_ISCLUSTER(pios)))
		return (0);

	/* Add the preferred IOS member(s) first.  Note that PIOS may
	 *   be a CNOS, parallel IOS, or stand-alone.
	 */
	slcfg_resm_roundrobin(pios, a);

	DYNARRAY_FOREACH(res, i, &pios->res_site->site_resources) {
		if (!RES_ISFS(res) || (res == pios) ||
		    ((res->res_type == SLREST_ARCHIVAL_FS) &&
		     !use_archival))
			continue;

		slcfg_resm_roundrobin(res, a);
	}

	return (psc_dynarray_len(a));
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
