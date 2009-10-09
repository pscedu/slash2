/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "sliod.h"

void
sliotimerthr_spawn(void)
{
	psc_timerthr_spawn(SLIOTHRT_TINTV, "sliotintvthr");
	pscthr_init(SLIOTHRT_TIOS, 0, psc_timer_iosthr_main,
	    NULL, 0, "sliotioiosthr");
}
