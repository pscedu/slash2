/* $Id$ */

#include "psc_util/alloc.h"

#include "slconfig.h"

struct sl_resource *
slcfg_new_res(void)
{
	struct sl_resource *res;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);
	return (res);
}

struct sl_resm *
slcfg_new_resm(void)
{
	struct sl_resm *resm;

	resm = PSCALLOC(sizeof(*resm));
	return (resm);
}
