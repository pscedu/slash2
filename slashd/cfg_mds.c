/* $Id$ */

#include "psc_util/alloc.h"

#include "mdsexpc.h"
#include "slashd.h"
#include "slconfig.h"

struct sl_resource *
slcfg_new_res(void)
{
	struct resprof_mds_info *rmi;
	struct sl_resource *res;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);

	rmi = res->res_pri = PSCALLOC(sizeof(*rmi));
	LOCK_INIT(&rmi->rmi_lock);

	return (res);
}

struct sl_resm *
slcfg_new_resm(void)
{
	struct sl_resm *resm;

	resm = PSCALLOC(sizeof(*resm));
	return (resm);
}

struct sl_site *
slcfg_new_site(void)
{
	struct mds_site_info *msi;
	struct sl_site *site;

	site = PSCALLOC(sizeof(*site));
	INIT_SITE(site);

	msi = site->site_pri = PSCALLOC(sizeof(*msi));
	psc_dynarray_init(&msi->msi_replq);
	LOCK_INIT(&msi->msi_lock);
	psc_waitq_init(&msi->msi_waitq);

	return (site);
}
