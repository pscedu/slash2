/* $Id$ */

#include "psc_util/alloc.h"

#include "slconfig.h"
#include "mdsexpc.h"

struct resource_profile *
slcfg_new_res(void)
{
	struct resource_profile *res;
	struct resprof_mds_info *rmi;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);
	rmi = PSCALLOC(sizeof(*rmi));
	LOCK_INIT(&rmi->rmi_lock);
	res->res_pri = rmi;
	return (res);
}

struct resource_member *
slcfg_new_resm(void)
{
	struct resource_member *resm;

	resm = PSCALLOC(sizeof(*resm));
	return (resm);
}

struct site_profile *
slcfg_new_site(void)
{
	struct site_profile *site;

	site = PSCALLOC(sizeof(*site));
	INIT_SITE(site);
	return (site);
}
