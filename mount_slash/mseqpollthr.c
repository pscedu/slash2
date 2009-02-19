/* $Id$ */

#include <sched.h>

#include "psc_util/cdefs.h"
#include "psc_rpc/rpc.h"
#include "psc_util/thread.h"

#include "mount_slash.h"

void *
mseqpollthr_main(__unusedx void *arg)
{
	for (;;) {
		pscrpc_check_events(100);
		sched_yield();
	}
}

void
mseqpollthr_spawn(void)
{
	pscthr_init(MSTHRT_EQPOLL, 0, mseqpollthr_main,
	    NULL, 0, "mseqpollthr");
}
