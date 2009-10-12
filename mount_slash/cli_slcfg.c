/* $Id$ */

#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "cli_bmap.h"
#include "slconfig.h"

struct resource_profile *
slcfg_new_res(void)
{
	struct resource_profile *res;
	struct resprof_cli_info *rci;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);

	rci = res->res_pri = PSCALLOC(sizeof(*rci));
	LOCK_INIT(&rci->rci_lock);

	return (res);
}

struct resource_member *
slcfg_new_resm(void)
{
	struct resource_member *resm;
	struct bmap_info_cli *c;

	resm = PSCALLOC(sizeof(*resm));
	c = resm->resm_pri = PSCALLOC(sizeof(*c));
	LOCK_INIT(&c->bmic_lock);
	psc_waitq_init(&c->bmic_waitq);

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
