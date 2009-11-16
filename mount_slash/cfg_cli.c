/* $Id$ */

#include "pfl/cdefs.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "bmap_cli.h"
#include "mount_slash.h"
#include "slconfig.h"

void
slcfg_init_res(struct sl_resource *res)
{
	struct resprof_cli_info *rci;

	rci = res->res_pri = PSCALLOC(sizeof(*rci));
	LOCK_INIT(&rci->rci_lock);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct cli_imp_ion *c;

	c = resm->resm_pri = PSCALLOC(sizeof(*c));
	LOCK_INIT(&c->ci_lock);
	psc_waitq_init(&c->ci_waitq);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}
