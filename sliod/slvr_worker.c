/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#define PSC_SUBSYS SLISS_SLVR
#include "subsys_iod.h"

#include <time.h>

#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fault.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/tree.h"

#include "bmap_iod.h"
#include "fid.h"
#include "fidc_iod.h"
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"

#define	SLI_SYNC_AHEAD_BATCH	10

void
sli_sync_ahead(struct psc_dynarray *a)
{
	int i, skip;
	struct fidc_membh *f;
	struct fcmh_iod_info *fii;

 restart:

	skip = 0;
	LIST_CACHE_LOCK(&sli_fcmh_dirty);
	LIST_CACHE_FOREACH(fii, &sli_fcmh_dirty) {
		f = fii_2_fcmh(fii);

		if (!FCMH_TRYLOCK(f)) {
			skip++;
			continue;
		}
		if (f->fcmh_flags & FCMH_IOD_SYNCFILE) {
			FCMH_ULOCK(f);
			continue;
		}
		fii->fii_nwrite = 0;
		f->fcmh_flags |= FCMH_IOD_SYNCFILE;
		fcmh_op_start_type(f, FCMH_OPCNT_SYNC_AHEAD);
		FCMH_ULOCK(f);
		psc_dynarray_add(a, f);
	}
	LIST_CACHE_ULOCK(&sli_fcmh_dirty);

	DYNARRAY_FOREACH(f, i, a) {

		fsync(fcmh_2_fd(f));
		OPSTAT_INCR("sync-ahead");

		DEBUG_FCMH(PLL_DIAG, f, "sync ahead");

		FCMH_LOCK(f);
		fii = fcmh_2_fii(f);
		if (fii->fii_nwrite < sli_sync_max_writes / 2) {
			OPSTAT_INCR("sync-ahead-remove");
			lc_remove(&sli_fcmh_dirty, fii);
			f->fcmh_flags &= ~FCMH_IOD_DIRTYFILE;
		}
		f->fcmh_flags &= ~FCMH_IOD_SYNCFILE;
		fcmh_op_done_type(f, FCMH_OPCNT_SYNC_AHEAD);
	}
	psc_dynarray_reset(a);
	if (skip) {
		sleep(5);
		goto restart;
	}
}

void
slisyncthr_main(struct psc_thread *thr)
{
	struct psc_dynarray a = DYNARRAY_INIT;

	psc_dynarray_ensurelen(&a, SLI_SYNC_AHEAD_BATCH);
	while (pscthr_run(thr)) {
		lc_peekheadwait(&sli_fcmh_dirty);
		sli_sync_ahead(&a);
	}
	psc_dynarray_free(&a);
}

void
sliupdthr_main(struct psc_thread *thr)
{
	struct fidc_membh *f;
	struct fcmh_iod_info *fii;

	while (pscthr_run(thr)) {
		LIST_CACHE_LOCK(&sli_fcmh_update);
		LIST_CACHE_FOREACH(fii, &sli_fcmh_update) {
			f = fii_2_fcmh(fii);
		}
	}
}
