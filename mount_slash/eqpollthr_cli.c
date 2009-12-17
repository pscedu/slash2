/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sched.h>

#include "pfl/cdefs.h"
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
