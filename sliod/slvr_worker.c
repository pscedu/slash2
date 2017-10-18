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


int
sli_rmi_update_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	int i, rc;
	struct fidc_membh *f;
	struct fcmh_iod_info *fii;
	struct psc_dynarray *a = args->pointer_arg[0];
	struct slrpc_cservice *csvc = args->pointer_arg[1];

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_updatefile_rep, rc);
	if (rc) {
		DYNARRAY_FOREACH(fii, i, a) {
			f = fii_2_fcmh(fii);
			FCMH_LOCK(f);
			if (!(f->fcmh_flags & FCMH_IOD_UPDATEFILE)) {
				OPSTAT_INCR("requeue-update");
				lc_addhead(&sli_fcmh_update, fii);
				f->fcmh_flags |= FCMH_IOD_UPDATEFILE;
			}
			FCMH_ULOCK(f);
		}
		OPSTAT_INCR("update-failure");
	} else
		OPSTAT_INCR("update-success");
	sl_csvc_decref(csvc);
	psc_dynarray_free(a);
	PSCFREE(a);
}


/*
 * We used to do bulk RPC in non-blocking mode.  See how SRMT_BMAPCRCWRT
 * was implemented in the git history.
 */

void
sliupdthr_main(struct psc_thread *thr)
{
	int i, rc;
	struct stat stb;
	struct fidc_membh *f;
	struct fcmh_iod_info *fii, *tmp;
	struct slrpc_cservice *csvc;
	struct psc_dynarray *a;
	struct srm_updatefile_req *mq;
	struct srm_updatefile_rep *mp;
	struct timeval now;
	struct pscrpc_request *rq;

	while (pscthr_run(thr)) {

		i = 0;
		rq = NULL;
		csvc = NULL;

		a = PSCALLOC(sizeof(*a));
		psc_dynarray_init(a);

		rc = sli_rmi_getcsvc(&csvc);
		if (rc)
			goto out;

		rc = SL_RSX_NEWREQ(csvc, SRMT_UPDATEFILE, rq, mq, mp);
		if (rc)
			goto out;

		rq->rq_interpret_reply = sli_rmi_update_cb;
		rq->rq_async_args.pointer_arg[0] = a;
		rq->rq_async_args.pointer_arg[1] = csvc;

 again:

		lc_peekheadwait(&sli_fcmh_update);
		PFL_GETTIMEVAL(&now);
		LIST_CACHE_LOCK(&sli_fcmh_update);
		LIST_CACHE_FOREACH_SAFE(fii, tmp, &sli_fcmh_update) {
			f = fii_2_fcmh(fii);
			if (!FCMH_TRYLOCK(f))
				continue;
	
			/*
 			 * Wait for more writes to come in.  Also some
 			 * file systems might do not update st_nblocks
 			 * right away.
 			 */
			if (fii->fii_lastwrite + 5 < now.tv_sec) {
				FCMH_ULOCK(f);
				LIST_CACHE_ULOCK(&sli_fcmh_update);
				sleep(5);
				goto again;
			}

			if (fstat(fcmh_2_fd(f), &stb) == -1) {
				FCMH_ULOCK(f);	
				continue;
			}
			lc_remove(&sli_fcmh_update, fii);
			f->fcmh_flags &= ~FCMH_IOD_UPDATEFILE;
			/*
 			 * No change in storage usage.
 			 */
			if (fii->fii_nblks == stb.st_blocks) {
				FCMH_ULOCK(f);	
				continue;
			}
			psc_dynarray_add(a, fii);
			mq->updates[i].fg = f->fcmh_sstb.sst_fg;
			mq->updates[i].nblks = stb.st_blocks;

			FCMH_ULOCK(f);	

			i++;
			if (i >= MAX_FILE_UPDATES);
				break;
		}
		LIST_CACHE_ULOCK(&sli_fcmh_update);
		if (!i) {
			pscthr_yield();
			goto again; 
		}

		rc = SL_NBRQSET_ADD(csvc, rq);
		if (!rc)
			continue;
  out:

		DYNARRAY_FOREACH(fii, i, a) {
			f = fii_2_fcmh(fii);
			FCMH_LOCK(f);
			if (!(f->fcmh_flags & FCMH_IOD_UPDATEFILE)) {
				lc_addhead(&sli_fcmh_update, fii);
				f->fcmh_flags |= FCMH_IOD_UPDATEFILE;
			}
			FCMH_ULOCK(f);
		}

		psc_dynarray_free(a);
		PSCFREE(a);

		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);
	}
}
