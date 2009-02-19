/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "slashdthr.h"

void
sltimerthr_spawn(void)
{
	psc_timerthr_spawn(SLTHRT_TINTV, "sltintvthr");
	pscthr_init(SLTHRT_TIOS, 0, psc_timer_iosthr_main,
	    NULL, 0, "sltiosthr");
}
