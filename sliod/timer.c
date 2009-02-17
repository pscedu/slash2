/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "sliod.h"

struct psc_thread	sliotiosthr;

void
sliotimerthr_spawn(void)
{
	psc_timerthr_spawn(SLIOTHRT_TINTV, "sliotintvthr");
	pscthr_init(&sliotiosthr, SLIOTHRT_TIOS,
	    psc_timer_iosthr_main, NULL, 0, "sltioiosthr");
}
