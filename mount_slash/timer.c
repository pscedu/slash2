/* $Id$ */

#include "psc_util/thread.h"
#include "psc_util/timerthr.h"

#include "mount_slash.h"

void
mstimerthr_spawn(void)
{
	psc_timerthr_spawn(MSTHRT_TINTV, "mstintvthr");
	pscthr_init(MSTHRT_TIOS, 0, psc_timer_iosthr_main,
	    NULL, 0, "mstiosthr");
}
