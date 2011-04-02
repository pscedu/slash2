/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "psc_rpc/rpc.h"
#include "psc_util/thread.h"

struct pscrpc_nbreqset	*sl_nbrqset;

void
sl_nbrqthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		pscrpc_nbreqset_reap(sl_nbrqset);
		sleep(1);
	}
}

void
sl_nbrqthr_spawn(int thrtype, const char *thrname)
{
	sl_nbrqset = pscrpc_nbreqset_init(NULL, NULL);
	pscthr_init(thrtype, 0, sl_nbrqthr_main, NULL, 0, "%s", thrname);
}
