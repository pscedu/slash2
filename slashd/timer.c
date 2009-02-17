/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "slashdthr.h"

struct psc_thread	sltiosthr;

void
sltimerthr_spawn(void)
{
	psc_timerthr_spawn(SLTHRT_TINTV, "sltintvthr");
	pscthr_init(&sltiosthr, SLTHRT_TIOS,
	    psc_timer_iosthr_main, NULL, 0, "sltiosthr");
}
