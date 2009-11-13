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
	struct iod_resm_info *iri;

	iri = resm->resm_pri = PSCALLOC(sizeof(*iri));
	LOCK_INIT(&iri->iri_lock);
	psc_waitq_init(&iri->iri_waitq);
}

void
slcfg_init_site(__unusedx struct sl_site *site)
{
}
