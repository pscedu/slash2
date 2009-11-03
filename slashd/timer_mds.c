/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "slashd.h"

void
sltimerthr_spawn(void)
{
	psc_timerthr_spawn(SLMTHRT_TINTV, "sltintvthr");
	pscthr_init(SLMTHRT_TIOS, 0, psc_timer_iosthr_main,
	    NULL, 0, "sltiosthr");
}
