/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "sliod.h"

void
slitimerthr_spawn(void)
{
	psc_timerthr_spawn(SLITHRT_TINTV, "slitintvthr");
	pscthr_init(SLITHRT_TIOS, 0, psc_timer_iosthr_main,
	    NULL, 0, "slitioiosthr");
}
