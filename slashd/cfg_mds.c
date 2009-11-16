/* $Id$ */

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
	struct resprof_mds_info *rmi;

	rmi = res->res_pri = PSCALLOC(sizeof(*rmi));
	LOCK_INIT(&rmi->rmi_lock);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct mds_resm_info *mri;

	mri = resm->resm_pri = PSCALLOC(sizeof(*mri));
	LOCK_INIT(&mri->mri_lock);
	psc_multilock_cond_init(&mri->mri_mlcond,
	    NULL, 0, "mri-%s", resm->resm_addrbuf);
	mri->mri_resm = resm;
}

void
slcfg_init_site(struct sl_site *site)
{
	struct mds_site_info *msi;

	msi = site->site_pri = PSCALLOC(sizeof(*msi));
	psc_dynarray_init(&msi->msi_replq);
	LOCK_INIT(&msi->msi_lock);
	psc_multilock_init(&msi->msi_ml, "msi-%s",
	    site->site_name + strspn(site->site_name, "@"));
	psc_multilock_cond_init(&msi->msi_mlcond, NULL, 0, "msi-%s",
	    site->site_name + strspn(site->site_name, "@"));
}
