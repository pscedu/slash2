/* $Id$ */

#include "psc_util/alloc.h"

#include "slconfig.h"
#include "sliod.h"

struct sl_resource *
slcfg_new_res(void)
{
	struct sl_resource *res;

	res = PSCALLOC(sizeof(*res));
	return (res);
}

struct sl_resm *
slcfg_new_resm(void)
{
	struct iod_resm_info *iri;
	struct sl_resm *resm;

	resm = PSCALLOC(sizeof(*resm));
	iri = resm->resm_pri = PSCALLOC(sizeof(*iri));
	LOCK_INIT(&iri->iri_lock);
	psc_waitq_init(&iri->iri_waitq);

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
