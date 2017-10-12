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

struct psc_poolmaster		 bmap_crcupd_poolmaster;
struct psc_poolmgr		*bmap_crcupd_pool;
psc_atomic32_t			 sli_ninfl_bcrcupd;

struct psc_listcache		 bcr_ready;
struct timespec			 sli_bcr_pause = { 0, 200000L };

int
sli_rmi_bcrcupd_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[1];
	struct psc_dynarray *a = args->pointer_arg[0];
	struct srm_bmap_crcwrt_rep *mp;
	struct srm_bmap_crcwrt_req *mq;
	struct bmap_iod_info *bii;
	struct bcrcupd *bcr;
	int rc, i;

	SL_GET_RQ_STATUS(csvc, rq, mp, rc);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		bii = bcr->bcr_bii;

		/* 05/03/2017: Saw unknown, no buf */
		DEBUG_BCR(rc || !mp || mp->rc ?
		    PLL_ERROR : PLL_DIAG, bcr, "rq_status=%d rc=%d%s",
		    rq->rq_status, rc, mp ? "" : " (unknown, no buf)");

		BII_LOCK(bii);
		psc_assert(bii_2_bmap(bii)->bcm_flags &
		    BMAPF_CRUD_INFLIGHT);
		bii_2_bmap(bii)->bcm_flags &= ~BMAPF_CRUD_INFLIGHT;

		if (rc) {
			BII_ULOCK(bii);
			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");
			OPSTAT_INCR("crc-update-cb-failure");
			continue;
		}
		bcr_ready_remove(bcr);
	}

	/*
	 * If there were errors, log them but obviously the MDS will
	 * make the master choice about what our residency validity is.
	 */
	for (i = 0; i < (int)mq->ncrc_updates; i++)
		if (mp && mp->crcup_rc[i])
			psclog_errorx("MDS rejected our CRC update; "
			    "rc=%d", mp->crcup_rc[i]);

	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);

	psc_atomic32_dec(&sli_ninfl_bcrcupd);

	return (1);
}

/*
 * Send an RPC containing CRC updates for slivers to the MDS.  To avoid
 * potential woes caused by out-of-order deliveries, we allow at most
 * only one inflight CRC update RPC at any time.  Note that this does
 * not prevent us from having multiple threads to do the CRC
 * calculation.
 */
__static int
slvr_worker_crcup_genrq(const struct psc_dynarray *bcrs)
{
	struct pscrpc_request *rq = NULL;
	struct slrpc_cservice *csvc;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct iovec *iovs = NULL;
	struct bcrcupd *bcr;
	size_t len;
	uint32_t i;
	int rc;

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		return (rc);

	/* 04/01/2016: Hit crash due to NULL import */
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	rq->rq_interpret_reply = sli_rmi_bcrcupd_cb;
	rq->rq_async_args.pointer_arg[0] = (void *)bcrs;
	rq->rq_async_args.pointer_arg[1] = csvc;

	len = mq->ncrc_updates * sizeof(struct srt_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		rc = bcr_update_inodeinfo(bcr);
		if (rc) {
			/*
			 * XXX hack because the bcr code is awfully
			 * structured.
			 */
			bcr->bcr_crcup.fg.fg_fid = FID_ANY;
		}

		DEBUG_BCR(PLL_DIAG, bcr, "bcrs pos=%d fsz=%"PRId64, i,
		    bcr->bcr_crcup.fsize);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = sizeof(struct srt_bmap_crcup) +
		    (mq->ncrcs_per_update[i] *
		     sizeof(struct srt_bmap_crcwire));
	}
	psc_assert(len <= LNET_MTU);

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc)
		PFL_GOTOERR(out, rc);

	psc_atomic32_inc(&sli_ninfl_bcrcupd);

  out:
	PSCFREE(iovs);

	if (rc) {
		if (rq)
			pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);
	}
	return (rc);
}

#define	SLI_SYNC_AHEAD_BATCH	10

void
sli_sync_ahead(void)
{
	int i, skip;
	struct fidc_membh *f;
	struct fcmh_iod_info *fii;
	struct psc_dynarray a = DYNARRAY_INIT;

	psc_dynarray_ensurelen(&a, SLI_SYNC_AHEAD_BATCH);

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
		psc_dynarray_add(&a, f);
	}
	LIST_CACHE_ULOCK(&sli_fcmh_dirty);

	DYNARRAY_FOREACH(f, i, &a) {

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
	if (skip) {
		sleep(5);
		psc_dynarray_reset(&a);
		goto restart;
	}
	psc_dynarray_free(&a);
}

void
slisyncthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		lc_peekheadwait(&sli_fcmh_dirty);
		sli_sync_ahead();
	}
}

void
slvr_worker_init(void)
{
	int i;

	lc_reginit(&bcr_ready, struct bcrcupd, bcr_lentry, "bcr_ready");

	_psc_poolmaster_init(&bmap_crcupd_poolmaster,
	    sizeof(struct bcrcupd) +
	    sizeof(struct srt_bmap_crcwire) * MAX_BMAP_INODE_PAIRS,
	    offsetof(struct bcrcupd, bcr_lentry), PPMF_AUTO, 64,
	    64, 0, NULL, NULL, "bcrcupd");
	bmap_crcupd_pool = psc_poolmaster_getmgr(&bmap_crcupd_poolmaster);

	for (i = 0; i < NSLVRSYNC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_SYNC, slisyncthr_main, 0,
		    "slisyncthr%d", i);

}
