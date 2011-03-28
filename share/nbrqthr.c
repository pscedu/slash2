/* $Id$ */
/* %PSC_COPYRIGHT% */

#include "pfl/cdefs.h"
#include "psc_rpc/rpc.h"
#include "psc_util/thread.h"

struct pscrpc_nbreqset	*sl_nbrqset;

void
sl_nbrqthr_main(__unusedx void *arg)
{
	while (pscthr_run()) {
		pscrpc_nbreqset_reap(sl_nbrqset);
		sleep(1);
	}
}

void
sl_nbrqthr_spawn(int thrtype, const char *thrname)
{
	sl_nbrqset = pscrpc_nbreqset_init(NULL, NULL);
	pscthr_init(thrtype, 0, NULL, NULL, 0, "%s", thrname);
}
