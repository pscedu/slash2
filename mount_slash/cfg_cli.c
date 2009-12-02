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
	struct cli_resprof_info *crpi;

	crpi = res->res_pri = PSCALLOC(sizeof(*crpi));
	LOCK_INIT(&crpi->crpi_lock);
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct cli_resm_info *crmi;

	crmi = resm->resm_pri = PSCALLOC(sizeof(*crmi));
	LOCK_INIT(&crmi->crmi_lock);
	psc_waitq_init(&crmi->crmi_waitq);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}
