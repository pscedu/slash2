/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
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
#include "fidcache.h"
#include "rpc_iod.h"
#include "sliod.h"
#include "slvr.h"


int slvr_nbreqset_cb(struct pscrpc_request *, struct pscrpc_async_args *);

struct psc_poolmaster		 bmap_crcupd_poolmaster;
struct psc_poolmgr		*bmap_crcupd_pool;

struct psc_listcache		 bcr_ready;
struct pscrpc_nbreqset		*sl_nbrqset;
struct timespec			 sli_bcr_pause = { 0, 200000L };
struct psc_waitq		 sli_slvr_waitq = PSC_WAITQ_INIT;

/**
 * slvr_worker_crcup_genrq - Send an RPC containing CRC updates for
 *	slivers to the metadata server.  To avoid potential woes caused
 *	by out-of-order deliveries, we should allow at most one inflight
 *	CRC update RPC at any time.  Note that this does not prevent us
 *	from having multiple threads to do the CRC calculation.
 */
__static int
slvr_worker_crcup_genrq(const struct psc_dynarray *bcrs)
{
	struct slashrpc_cservice *csvc;
	struct srm_bmap_crcwrt_req *mq;
	struct srm_bmap_crcwrt_rep *mp;
	struct pscrpc_request *rq;
	struct bcrcupd *bcr;
	struct iovec *iovs = NULL;
	size_t len;
	uint32_t i;
	int rc;

	if (psc_fault_here_rc(SLI_FAULT_CRCUP_FAIL, &rc, EHOSTDOWN))
		return (rc);

	rc = sli_rmi_getcsvc(&csvc);
	if (rc)
		return (rc);
	rc = SL_RSX_NEWREQ(csvc, SRMT_BMAPCRCWRT, rq, mq, mp);
	if (rc)
		goto out;

	mq->ncrc_updates = psc_dynarray_len(bcrs);
	rq->rq_interpret_reply = slvr_nbreqset_cb;
	rq->rq_async_args.pointer_arg[0] = (void *)bcrs;
	rq->rq_async_args.pointer_arg[1] = csvc;

	len = mq->ncrc_updates * sizeof(struct srm_bmap_crcup);
	iovs = PSCALLOC(sizeof(*iovs) * mq->ncrc_updates);

	for (i = 0; i < mq->ncrc_updates; i++) {
		bcr = psc_dynarray_getpos(bcrs, i);

		rc = iod_inode_getinfo(&bcr->bcr_crcup.fg,
		    &bcr->bcr_crcup.fsize, &bcr->bcr_crcup.nblks,
		    &bcr->bcr_crcup.utimgen);

		if (rc)
			goto out;

		DEBUG_BCR(PLL_DIAG, bcr, "bcrs pos=%d fsz=%"PRId64, i,
		    bcr->bcr_crcup.fsize);

		mq->ncrcs_per_update[i] = bcr->bcr_crcup.nups;

		iovs[i].iov_base = &bcr->bcr_crcup;
		len += iovs[i].iov_len = sizeof(struct srm_bmap_crcup) +
		    (mq->ncrcs_per_update[i] *
		     sizeof(struct srt_bmap_crcwire));
	}
	psc_assert(len <= LNET_MTU);

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMI_BULK_PORTAL,
	    iovs, mq->ncrc_updates);

	if (rc)
		goto out;

	rc = SL_NBRQSET_ADD(csvc, rq);

  out:
	PSCFREE(iovs);

	if (rc)
		sl_csvc_decref(csvc);
	else
		OPSTAT_INCR(SLI_OPST_CRC_UPDATE);
	return (rc);
}

__static void
slvr_worker_push_crcups(void)
{
	static atomic_t busy = ATOMIC_INIT(0);
	struct bcrcupd *bcr;
	struct psc_dynarray *bcrs;
	struct timespec now, diff;
	int i, rc, didwork = 0;

	if (atomic_xchg(&busy, 1))
		return;
 again:

	OPSTAT_INCR(SLI_OPST_CRC_UPDATE_PUSH);

	pscrpc_nbreqset_reap(sl_nbrqset);

	/*
	 * Check if an earlier CRC update RPC, if any, has finished.  If
	 * one is still inflight, we won't be able to initiate a new one.
	 */
	bcrs = PSCALLOC(sizeof(struct psc_dynarray));
	/*
	 * Leave scheduled bcr's on the list so that in case of a
	 * failure, ordering will be maintained.
	 */
	LIST_CACHE_LOCK(&bcr_ready);
	PFL_GETTIMESPEC(&now);
	LIST_CACHE_FOREACH(bcr, &bcr_ready) {
		psc_assert(bcr->bcr_crcup.nups > 0);

		if (!BII_TRYLOCK(bcr->bcr_bii))
			continue;

		if (bcr_2_bmap(bcr)->bcm_flags & BMAP_IOD_INFLIGHT) {
			BII_ULOCK(bcr->bcr_bii);
			continue;
		}

		timespecsub(&now, &bcr->bcr_age, &diff);
		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS ||
		    diff.tv_sec >= BCR_MAX_AGE) {
			psc_dynarray_add(bcrs, bcr);
			bcr->bcr_bii->bii_bcr = NULL;
			bcr_2_bmap(bcr)->bcm_flags |= BMAP_IOD_INFLIGHT;
		}

		BII_ULOCK(bcr->bcr_bii);

		DEBUG_BCR(PLL_INFO, bcr, "scheduled nbcrs=%d total_bcrs=%d",
		    psc_dynarray_len(bcrs), lc_nitems(&bcr_ready));

		if (psc_dynarray_len(bcrs) == MAX_BMAP_NCRC_UPDATES)
			break;
	}
	LIST_CACHE_ULOCK(&bcr_ready);

	if (psc_dynarray_len(bcrs)) {
		didwork = 1;
		rc = slvr_worker_crcup_genrq(bcrs);
		/*
		 * If we fail to send an RPC, we must leave the
		 * reference in the tree for future attempt(s).
		 * Otherwise, the callback function (i.e.
		 * slvr_nbreqset_cb()) should remove them from the tree.
		 */
		if (rc) {
			for (i = 0; i < psc_dynarray_len(bcrs); i++) {
				bcr = psc_dynarray_getpos(bcrs, i);

				BII_LOCK(bcr->bcr_bii);
				bcr_2_bmap(bcr)->bcm_flags &=
				    ~BMAP_IOD_INFLIGHT;
				BII_ULOCK(bcr->bcr_bii);
			}
			psc_dynarray_free(bcrs);
			PSCFREE(bcrs);
		}
	}

	if (didwork) {
		didwork = 0;
		goto again;
	}

	PSCFREE(bcrs);
	atomic_set(&busy, 0);
}

int
slvr_nbreqset_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct srm_bmap_crcwrt_rep *mp;
	struct srm_bmap_crcwrt_req *mq;
	struct slashrpc_cservice *csvc;
	struct bmap_iod_info *bii;
	struct psc_dynarray *a;
	struct bcrcupd *bcr;
	int i;

	OPSTAT_INCR(SLI_OPST_CRC_UPDATE_CB);

	a = args->pointer_arg[0];
	csvc = args->pointer_arg[1];
	psc_assert(a);

	mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));

	for (i = 0; i < psc_dynarray_len(a); i++) {
		bcr = psc_dynarray_getpos(a, i);
		bii = bcr->bcr_bii;

		DEBUG_BCR(rq->rq_status || !mp || mp->rc ?
		    PLL_ERROR : PLL_INFO, bcr, "rq_status=%d rc=%d%s",
		    rq->rq_status, mp ? mp->rc : -4096,
		    mp ? "" : " (unknown, no buf)");

		BII_LOCK(bii);
		psc_assert(bii_2_bmap(bii)->bcm_flags & BMAP_IOD_INFLIGHT);
		bii_2_bmap(bii)->bcm_flags &= ~BMAP_IOD_INFLIGHT;

		if (rq->rq_status) {
			BII_ULOCK(bii);
			DEBUG_BCR(PLL_ERROR, bcr, "rescheduling");
			OPSTAT_INCR(SLI_OPST_CRC_UPDATE_CB_FAILURE);
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

	return (1);
}

/**
 * slislvrthr_proc - Attempt to setup a bmap CRC update RPC for dirty
 *	part(s) of a sliver.
 */
__static void
slislvrthr_proc(struct slvr *s)
{
	struct bmap_iod_info *bii;
	struct bcrcupd *bcr;
	struct bmap *b;
	uint64_t crc;

	SLVR_LOCK(s);
	DEBUG_SLVR(PLL_DEBUG, s, "got sliver");

	psc_assert(!(s->slvr_flags & SLVR_LRU));
	psc_assert(s->slvr_flags & SLVR_CRCDIRTY);
	psc_assert(s->slvr_flags & SLVR_PINNED);
	psc_assert(s->slvr_flags & SLVR_DATARDY);

	/*
	 * Try our best to determine whether we should hold off the
	 * CRC operation strivonly CRC slivers which have no pending
	 * writes.  This section directly below may race with
	 * slvr_wio_done().
	 */
	if (s->slvr_pndgwrts > 0) {
		s->slvr_flags |= SLVR_LRU;
		lc_addqueue(&sli_lruslvrs, s);

		DEBUG_SLVR(PLL_DIAG, s, "descheduled due to pndg writes");

		SLVR_ULOCK(s);
		return;
	}

	/*
	 * OK, we've got a sliver to work on.  From this point until we
	 * set to inflight, the slvr_lentry should be disjointed.
	 */

	psc_assert(psclist_disjoint(&s->slvr_lentry));
	psc_assert(slvr_do_crc(s, &crc));

	/* Be paranoid, ensure the sliver is not queued anywhere. */
	psc_assert(psclist_disjoint(&s->slvr_lentry));

	/* Put the slvr back to the LRU so it may have its slab reaped. */
	bii = slvr_2_bii(s);
	b = bii_2_bmap(bii);

	s->slvr_flags |= SLVR_LRU;
	lc_addqueue(&sli_lruslvrs, s);
	slvr_lru_tryunpin_locked(s);

	bmap_op_start_type(b, BMAP_OPCNT_BCRSCHED);
	SLVR_ULOCK(s);

	BII_LOCK(bii);
	bcr = bii->bii_bcr;

	if (bcr) {
		uint32_t i;
		int found;

		psc_assert(bcr->bcr_crcup.blkno == b->bcm_bmapno);
		psc_assert(bcr->bcr_crcup.fg.fg_fid ==
		    b->bcm_fcmh->fcmh_fg.fg_fid);
		psc_assert(bcr->bcr_crcup.nups < MAX_BMAP_INODE_PAIRS);

		/*
		 * If we already have a slot for our slvr_num then reuse
		 * it.
		 */
		for (i = 0, found = 0; i < bcr->bcr_crcup.nups; i++) {
			if (bcr->bcr_crcup.crcs[i].slot == s->slvr_num) {
				found = 1;
				break;
			}
		}

		bcr->bcr_crcup.crcs[i].crc = crc;
		if (!found) {
			bcr->bcr_crcup.nups++;
			bcr->bcr_crcup.crcs[i].slot = s->slvr_num;
		}

		DEBUG_BCR(PLL_DIAG, bcr, "add to existing bcr slot=%d "
		    "nups=%d", i, bcr->bcr_crcup.nups);

		if (bcr->bcr_crcup.nups == MAX_BMAP_INODE_PAIRS)
			bcr->bcr_bii->bii_bcr = NULL;

	} else {

		bii->bii_bcr = bcr = psc_pool_get(bmap_crcupd_pool);
		memset(bcr, 0, bmap_crcupd_pool->ppm_entsize);

		INIT_PSC_LISTENTRY(&bcr->bcr_lentry);
		COPYFG(&bcr->bcr_crcup.fg, &b->bcm_fcmh->fcmh_fg);

		bcr->bcr_bii = bii;
		bcr->bcr_crcup.blkno = b->bcm_bmapno;
		bcr->bcr_crcup.crcs[0].crc = crc;
		bcr->bcr_crcup.crcs[0].slot = s->slvr_num;
		bcr->bcr_crcup.nups = 1;

		bcr_ready_add(bcr);
		PFL_GETTIMESPEC(&bcr->bcr_age);
	}

	BII_ULOCK(bii);

	bmap_op_done_type(b, BMAP_OPCNT_BCRSCHED);
}

/**
 * slislvrthr_main - Guts of the sliver bmap CRC update RPC generator
 *	thread.  RPCs are constructed from slivers on the queue after
 *	the minimal expiration is reached and shipped off to the MDS.
 */
void
slislvrthr_main(struct psc_thread *thr)
{
	struct psc_dynarray ss = DYNARRAY_INIT_NOLOG;
	struct pfl_mutex mtx = PSC_MUTEX_INIT;
	struct timespec expire;
	struct slvr *s, *dummy;
	int i;

	while (pscthr_run(thr)) {

		psc_mutex_lock(&mtx);

		LIST_CACHE_LOCK(&sli_crcqslvrs);

		PFL_GETTIMESPEC(&expire);
		expire.tv_sec -= BCR_MIN_AGE;

		LIST_CACHE_FOREACH_SAFE(s, dummy, &sli_crcqslvrs) {
			if (!SLVR_TRYLOCK(s))
				continue;
			if (timespeccmp(&expire, &s->slvr_ts, >)) {
				lc_remove(&sli_crcqslvrs, s);
				psc_dynarray_add(&ss, s);
			}
			SLVR_ULOCK(s);
			if (psc_dynarray_len(&ss) >=
			    MAX_BMAP_NCRC_UPDATES)
				break;
		}
		LIST_CACHE_ULOCK(&sli_crcqslvrs);

		psc_mutex_unlock(&mtx);

		DYNARRAY_FOREACH(s, i, &ss)
			slislvrthr_proc(s);
		slvr_worker_push_crcups();

		PFL_GETTIMESPEC(&expire);
		expire.tv_sec += BCR_MIN_AGE;
		psc_waitq_waitabs(&sli_slvr_waitq, NULL, &expire);

		psc_dynarray_reset(&ss);
	}
	psc_dynarray_free(&ss);
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
	    64, 0, NULL, NULL, NULL, NULL, "bcrcupd");
	bmap_crcupd_pool = psc_poolmaster_getmgr(&bmap_crcupd_poolmaster);

	for (i = 0; i < NSLVRCRC_THRS; i++)
		pscthr_init(SLITHRT_SLVR_CRC, 0, slislvrthr_main, NULL,
		    0, "slislvrthr%d", i);
}
