/* $Id$ */

#include "psc_util/alloc.h"

#include "slconfig.h"

struct resource_profile *
slcfg_new_res(void)
{
	struct resource_profile *res;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);
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
