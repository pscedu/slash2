/* $Id$ */

#include <sched.h>

#include "psc_util/cdefs.h"
#include "psc_rpc/rpc.h"
#include "psc_util/thread.h"

#include "mount_slash.h"

struct psc_thread mseqpoll;

void *
mzeqpollthr_main(__unusedx void *arg)
{
	for (;;) {
		pscrpc_check_events(100);
		sched_yield();
	}
}

void
mzeqpollthr_spawn(void)
{
	pscthr_init(&mseqpoll, MSTHRT_EQPOLL, mzeqpollthr_main,
	    NULL, "mseqpollthr");
}
