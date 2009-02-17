/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "mount_slash.h"

struct psc_thread	mstiosthr;

void
mstimerthr_spawn(void)
{
	psc_timerthr_spawn(MSTHRT_TINTV, "mstintvthr");
	pscthr_init(&mstiosthr, MSTHRT_TIOS,
	    psc_timer_iosthr_main, NULL, 0, "mstiosthr");
}
