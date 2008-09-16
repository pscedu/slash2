/* $Id$ */

#include <stdio.h>

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
	    NULL, "mseqpoll");
}
