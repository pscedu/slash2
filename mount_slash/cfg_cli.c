/* $Id$ */

#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "cli_bmap.h"
#include "mount_slash.h"
#include "slconfig.h"

struct sl_resource *
slcfg_new_res(void)
{
	struct resprof_cli_info *rci;
	struct sl_resource *res;

	res = PSCALLOC(sizeof(*res));

	rci = res->res_pri = PSCALLOC(sizeof(*rci));
	LOCK_INIT(&rci->rci_lock);

	return (res);
}

struct sl_resm *
slcfg_new_resm(void)
{
	struct sl_resm *resm;
	struct cli_imp_ion *c;

	resm = PSCALLOC(sizeof(*resm));
	c = resm->resm_pri = PSCALLOC(sizeof(*c));
	LOCK_INIT(&c->ci_lock);
	psc_waitq_init(&c->ci_waitq);

	return (resm);
}

struct sl_site *
slcfg_new_site(void)
{
	struct sl_site *site;

	site = PSCALLOC(sizeof(*site));
	INIT_SITE(site);
	return (site);
}
