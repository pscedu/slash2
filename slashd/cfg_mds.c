/* $Id$ */

#include "psc_util/alloc.h"

#include "fidc_mds.h"
#include "mdsexpc.h"
#include "slconfig.h"

struct sl_resource *
slcfg_new_res(void)
{
	struct resprof_mds_info *rmi;
	struct sl_resource *res;

	res = PSCALLOC(sizeof(*res));
	INIT_RES(res);

	rmi = res->res_pri = PSCALLOC(sizeof(*rmi));
	LOCK_INIT(&rmi->rmi_lock);

	return (res);
}

struct sl_resm *
slcfg_new_resm(void)
{
	struct sl_resm *resm;

	resm = PSCALLOC(sizeof(*resm));
	return (resm);
}
