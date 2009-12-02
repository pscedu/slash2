/* $Id$ */

#include "pfl/cdefs.h"
#include "psc_util/alloc.h"

#include "slconfig.h"
#include "sliod.h"

void
slcfg_init_res(__unusedx struct sl_resource *res)
{
}

void
slcfg_init_resm(struct sl_resm *resm)
{
	struct iod_resm_info *irmi;

	irmi = resm->resm_pri = PSCALLOC(sizeof(*irmi));
	LOCK_INIT(&irmi->irmi_lock);
	psc_waitq_init(&irmi->irmi_waitq);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}
