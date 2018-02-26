/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
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

/*
 * Logic for bmap flushing: sending RPCs of dirty data modified by
 * clients to the IO systems they are bound to.  Write coalescing
 * (bwc) occurs to reduce RPC overhead.
 */

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <sys/time.h>
#include <sys/types.h>

#include <stdlib.h>

#include "pfl/cdefs.h"
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fault.h"
#include "pfl/fcntl.h"
#include "pfl/fsmod.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "pgcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"

struct timespec			 msl_bflush_timeout = { 2, 0L };
struct timespec			 msl_bflush_maxage = { 0, 10000000L };	/* 10 milliseconds */
struct psc_listcache		 msl_bmapflushq;
struct psc_listcache		 msl_bmaptimeoutq;

int				 msl_max_nretries = 256;

#define MIN_COALESCE_RPC_SZ	 LNET_MTU

struct psc_waitq		 slc_bflush_waitq = PSC_WAITQ_INIT("bflush");
psc_spinlock_t			 slc_bflush_lock = SPINLOCK_INIT;
int				 slc_bflush_tmout_flags;

psc_atomic32_t			 slc_write_coalesce_max;

__static int
bmap_flush_biorq_expired(const struct bmpc_ioreq *a, int force)
{
	struct timespec ts;

	if (force && (a->biorq_flags & BIORQ_EXPIRE))
		return (1);

	PFL_GETTIMESPEC(&ts);
	/* XXX timespeccmp(&a->biorq_expire, &ts, <) */
	if ((a->biorq_expire.tv_sec < ts.tv_sec ||
	    (a->biorq_expire.tv_sec == ts.tv_sec &&
	     a->biorq_expire.tv_nsec <= ts.tv_nsec)))
		return (1);

	return (0);
}

/*
 * Manually expire all bmaps attached to a file.  The RPC flusher will
 * soon notice and flush any pending data.
 */
void
bmap_free_all_locked(struct fidc_membh *f)
{
	int redo = 0;
	struct bmap_cli_info *bci;
	struct bmap *b;

 retry:

	FCMH_LOCK_ENSURE(f);

	/* 03/18/2016: Hit SIGSEGV when called from do_setattr() */
	pfl_rwlock_rdlock(&f->fcmh_rwlock);
	RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
		DEBUG_BMAP(PLL_DIAG, b, "mark bmap free");

		/*
		 * The MDS truncates the SLASH2 metadata file on a full
		 * truncate.  We need to throw away leases and request a
		 * new lease later, so that the MDS has a chance to
		 * update its metadata file on-disk.  Otherwise, we can
		 * use an existing lease to write the file and can not
		 * update the metadata file even if the bmap is still
		 * cached at the MDS because the generation # has been
		 * bumped for the full truncate.
		 *
		 * Finally, a read comes in, we request a read bmap.  At
		 * this point, all bmaps of the file have been freed at
		 * both MDS and client.  And the MDS cannot find a
		 * replica for a bmap in the metafile.
		 */
		if (!BMAP_TRYLOCK(b)) {
			redo = 1;
			break;
		}
		bci = bmap_2_bci(b);
		PFL_GETTIMESPEC(&bci->bci_etime);
		BMAP_ULOCK(b);
	}
	pfl_rwlock_unlock(&f->fcmh_rwlock);
	/*
	 * Need to race with the bmap timeout code path.
	 */
	if (redo) {
		redo = 0;
		FCMH_ULOCK(f);
		usleep(10);
		FCMH_LOCK(f);
		goto retry;
	}
	psc_waitq_wakeall(&msl_bmaptimeoutq.plc_wq_empty);
}


/*
 * Pin (mark read-only) all pages attached to a bmap write coalescer.
 */
void
bwc_pin_pages(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *pg;
	int i;

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		pg = bwc->bwc_bmpces[i];
		BMPCE_LOCK(pg);
		pg->bmpce_pins++;
		BMPCE_ULOCK(pg);
	}
}

/*
 * Unpin (release) all pages attached to a bmap write coalescer.
 */
void
bwc_unpin_pages(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *pg;
	int i;

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		pg = bwc->bwc_bmpces[i];
		BMPCE_LOCK(pg);
		pg->bmpce_pins--;
		BMPCE_WAKE(pg);
		BMPCE_ULOCK(pg);
	}
}

void
bmap_flushq_wake(int reason)
{
	int wake = 0;

	if (slc_bflush_tmout_flags & BMAPFLSH_RPCWAIT) {
		wake = 1;
		if (reason == BMAPFLSH_EXPIRE)
			psc_waitq_wakeall(&slc_bflush_waitq);
		else
			psc_waitq_wakeone(&slc_bflush_waitq);
	}

	if (reason == BMAPFLSH_EXPIRE)
		psc_waitq_wakeall(&msl_bmapflushq.plc_wq_empty);
	else
		psc_waitq_wakeone(&msl_bmapflushq.plc_wq_empty);

	psclog_diag("wakeup flusher: reason=%x wake=%d", reason, wake);
	(void)wake;
}

/*
 * Callback run after a WRITE is recieved by an IOS.
 */
__static int
msl_ric_bflush_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmpc_write_coalescer *bwc =
	    args->pointer_arg[MSL_CBARG_BIORQS];
	struct sl_resm *m = args->pointer_arg[MSL_CBARG_RESM];
	struct resprof_cli_info *rpci = res2rpci(m->resm_res);
	struct bmpc_ioreq *r;
	int i, rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	psclog_diag("callback to write RPC bwc=%p ios=%d infl=%d rc=%d",
	    bwc, m->resm_res_id, rpci->rpci_infl_rpcs, rc);
	(void)rpci;

	bwc_unpin_pages(bwc);

	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		if (rc) {
			bmap_flush_resched(r, rc);
		} else {
			msl_biorq_release(r);
		}
	}

	pfl_opstats_grad_incr(&slc_iorpc_iostats_wr, bwc->bwc_size);

	bwc_free(bwc);
	sl_csvc_decref(csvc);

	return (0);
}

__static int
bmap_flush_create_rpc(struct bmpc_write_coalescer *bwc,
    struct slrpc_cservice *csvc, struct bmap *b)
{
	struct pscrpc_request *rq = NULL;
	struct resprof_cli_info *rpci;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	char buf[PSCRPC_NIDSTR_SIZE];
	int rc;

	m = libsl_ios2resm(bmap_2_ios(b));
	rpci = res2rpci(m->resm_res);

	rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
	if (rc)
		goto out;

	rc = slrpc_bulkclient(rq, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
	    bwc->bwc_iovs, bwc->bwc_niovs);
	if (rc)
		goto out;

	mq->offset = bwc->bwc_soff;
	mq->size = bwc->bwc_size;
	mq->op = SRMIOP_WR;

	if (b->bcm_flags & BMAPF_BENCH)
		mq->flags |= SRM_IOF_BENCH;

	mq->sbd = *bmap_2_sbd(b);

	DEBUG_REQ(PLL_DIAG, rq, buf, "sending WRITE RPC to iosid=%#x "
	    "fid="SLPRI_FG" off=%u sz=%u ios=%u infl=%d",
	    m->resm_res_id, SLPRI_FG_ARGS(&mq->sbd.sbd_fg), mq->offset,
	    mq->size, bmap_2_ios(b), rpci->rpci_infl_rpcs);

	bwc_pin_pages(bwc);

	rq->rq_interpret_reply = msl_ric_bflush_cb;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQS] = bwc;
	rc = SL_NBRQSET_ADD(csvc, rq);
	if (!rc)
		return (0);

	bwc_unpin_pages(bwc);

 out:
	if (rq)
		pscrpc_req_finished(rq);
	return (rc);
}

/*
 * Called in error contexts where the biorq must be rescheduled by
 * putting it back to the new request queue.  Typically this is from a
 * write RPC cb.
 */
void
bmap_flush_resched(struct bmpc_ioreq *r, int rc)
{
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc;
	struct bmap_cli_info *bci = bmap_2_bci(r->biorq_bmap);

	DEBUG_BIORQ(PLL_DIAG, r, "resched rc=%d", rc);

	BMAP_LOCK(b);

	if (rc == -PFLERR_KEYEXPIRED) {				/* -501 */
		OPSTAT_INCR("msl.bmap-flush-expired");
		b->bcm_flags |= BMAPF_LEASEEXPIRE;
	}
	if (rc == -PFLERR_TIMEDOUT)				/* -511 */
		OPSTAT_INCR("msl.bmap-flush-timedout");

	BIORQ_LOCK(r);

	if (rc == -EAGAIN)
		goto requeue;

	if (rc == -ENOSPC || r->biorq_retries >= SL_MAX_BMAPFLSH_RETRIES ||
	    ((r->biorq_flags & BIORQ_EXPIRE) && 
	     (r->biorq_retries >= msl_max_retries * 32))) {

		BIORQ_ULOCK(r);
		if (rc && !bci->bci_flush_rc)
			bci->bci_flush_rc = rc;
		BMAP_ULOCK(r->biorq_bmap);

		OPSTAT_INCR("msl.bmap-flush-maxretry");
		msl_bmpces_fail(r, rc);
		msl_biorq_release(r);
		return;
	}


	if (r->biorq_last_sliod == bmap_2_ios(r->biorq_bmap) ||
	    r->biorq_last_sliod == IOS_ID_ANY)
		r->biorq_retries++;
	else
		r->biorq_retries = 1;

	/*
	 * Back off to allow the I/O server to recover or become less
	 * busy.  Also clear the force expire flag to avoid a spin
	 * within ourselves in the bmap flush loop.
	 *
	 * In theory, we could place them on a different queue based on
	 * its target sliod and woken them up with the connection is
	 * re-established with that sliod.  But that logic is too
	 * complicated to get right.
	 */
	if (rc != -PFLERR_KEYEXPIRED) {
		OPSTAT_INCR("msl.bmap-flush-backoff");
		PFL_GETTIMESPEC(&r->biorq_expire);
		r->biorq_expire.tv_sec += SL_MAX_BMAPFLSH_DELAY;
	}

 requeue:

	if (!(r->biorq_flags & BIORQ_ONTREE)) {
		bmpc = bmap_2_bmpc(b);
		PSC_RB_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_biorqs, r);
		r->biorq_flags |= BIORQ_ONTREE;
	}
	OPSTAT_INCR("msl.bmap-flush-resched");

	BIORQ_ULOCK(r);
	BMAP_ULOCK(b);
	/*
	 * If we were able to connect to an IOS, but the RPC fails
	 * somehow, try to use a different IOS if possible.
	 */
	msl_bmap_lease_reassign(r->biorq_bmap);
}

__static int
bmap_flush_send_rpcs(struct bmpc_write_coalescer *bwc)
{
	struct slrpc_cservice *csvc;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r;
	struct bmap_cli_info *bci;
	struct bmap *b;
	int i, rc;
	struct sl_resm *m;
	struct timespec ts0, ts1;

	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);

	rc = msl_bmap_to_csvc(r->biorq_bmap, 1, NULL, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	b = r->biorq_bmap;
	bci = bmap_2_bci(b);

	BMAP_LOCK(b);
	PFL_GETTIMESPEC(&ts0);
	ts0.tv_sec += 1;
	ts1.tv_sec = bci->bci_etime.tv_sec;
	BMAP_ULOCK(b);

	if (ts1.tv_sec <= ts0.tv_sec) {
		rc = -EAGAIN;
		OPSTAT_INCR("msl.bmap-lease-expired");
		PFL_GOTOERR(out, rc);
	}
	/*
 	 * Throttle RPC here. Otherwise, our RPC will be
 	 * stalled and when it eventually sent out, got
 	 * rejected by expired keys.
 	 */
	m = libsl_ios2resm(bmap_2_ios(b));
	rc = msl_resm_get_credit(m, ts1.tv_sec - ts0.tv_sec);
	if (rc) {
		rc = -EAGAIN;
		OPSTAT_INCR("msl.bmap-flush-throttled");
		PFL_GOTOERR(out, rc);
	}

	psc_assert(bwc->bwc_soff == r->biorq_off);

	BMAP_LOCK(b);
	bmpc = bmap_2_bmpc(b);
	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		/*
 		 * 04/26/2017: Crash here with bcm_bmapno = 3783215504, 
 		 * bcm_flags = 32751, rc=32751, from bmap_flush_resched().
 		 */
		psc_assert(b == r->biorq_bmap);
		/*
		 * No need to lock because we have already replied to
		 * the user space.  Furthermore, we flush each biorq in
		 * one RPC.  So the callback handler won't race with us.
		 */
		r->biorq_last_sliod = bmap_2_ios(b);
		r->biorq_flags &= ~BIORQ_ONTREE;
		PSC_RB_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_biorqs, r);
	}
	BMAP_ULOCK(b);

	psclog_diag("bwc cb arg (%p) size=%zu nbiorqs=%d",
	    bwc, bwc->bwc_size, psc_dynarray_len(&bwc->bwc_biorqs));

	rc = bmap_flush_create_rpc(bwc, csvc, b);
	if (!rc)
		return (0);
	msl_resm_put_credit(m);

 out:
	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs)
		bmap_flush_resched(r, rc);

	if (csvc)
		sl_csvc_decref(csvc);

	bwc_free(bwc);
	OPSTAT_INCR("msl.bmap-flush-rpc-fail");
	return (rc);
}

/*
 * This function determines the size of the region covered by an array
 * of requests.  Note that these requests can overlap in various ways.
 * But they have already been ordered based on their offsets.
 */
__static void
bmap_flush_coalesce_prep(struct bmpc_write_coalescer *bwc)
{
	struct bmpc_ioreq *r, *end = NULL;
	struct bmap_pagecache_entry *bmpce;
	uint32_t reqsz, tlen;
	off_t off, loff;
	int i, j;

	psc_assert(!bwc->bwc_nbmpces);

	DYNARRAY_FOREACH(r, i, &bwc->bwc_biorqs) {
		if (!end)
			end = r;
		else {
			/*
			 * biorq offsets may not decrease and holes are
			 * not allowed.
			 */
			psc_assert(r->biorq_off >= loff);
			psc_assert(r->biorq_off <= biorq_voff_get(end));
			if (biorq_voff_get(r) > biorq_voff_get(end))
				end = r;
		}

		loff = off = r->biorq_off;
		reqsz = r->biorq_len;

		DYNARRAY_FOREACH(bmpce, j, &r->biorq_pages) {
			DEBUG_BMPCE(PLL_DIAG, bmpce,
			    "adding if DNE nbmpces=%d (i=%d) "
			    "(off=%"PSCPRIdOFFT")", bwc->bwc_nbmpces, j,
			    off);

			bmpce_usecheck(bmpce, BIORQ_WRITE,
			    !j ? (r->biorq_off & ~BMPC_BUFMASK) : off);

			tlen = MIN(reqsz, !j ? BMPC_BUFSZ -
			    (off - bmpce->bmpce_off) : BMPC_BUFSZ);

			off += tlen;
			reqsz -= tlen;

			if (!bwc->bwc_nbmpces) {
				bwc->bwc_bmpces[bwc->bwc_nbmpces++] =
				    bmpce;
				DEBUG_BMPCE(PLL_DIAG, bmpce, "added");
				continue;
			}
			if (bwc->bwc_bmpces[bwc->bwc_nbmpces - 1]->
			    bmpce_off >= bmpce->bmpce_off)
				continue;

			psc_assert(bmpce->bmpce_off - BMPC_BUFSZ ==
			    bwc->bwc_bmpces[bwc->bwc_nbmpces - 1]->bmpce_off);
			bwc->bwc_bmpces[bwc->bwc_nbmpces++] = bmpce;
			DEBUG_BMPCE(PLL_DIAG, bmpce, "added");
		}
		psc_assert(!reqsz);
	}
	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);

	psc_assert(bwc->bwc_size ==
	    (end->biorq_off - r->biorq_off) + end->biorq_len);
}

/*
 * Scan the given list of bio requests and construct I/O vectors out of
 * them.  One iovec is limited to one page.
 */
__static void
bmap_flush_coalesce_map(struct bmpc_write_coalescer *bwc)
{
	struct bmap_pagecache_entry *bmpce;
	struct bmpc_ioreq *r;
	uint32_t tot_reqsz;
	int i;

	tot_reqsz = bwc->bwc_size;

	bmap_flush_coalesce_prep(bwc);

	psclog_diag("tot_reqsz=%u nitems=%d nbmpces=%d", tot_reqsz,
	    psc_dynarray_len(&bwc->bwc_biorqs), bwc->bwc_nbmpces);

	psc_assert(!bwc->bwc_niovs);

	r = psc_dynarray_getpos(&bwc->bwc_biorqs, 0);
	psc_assert(bwc->bwc_soff == r->biorq_off);

	for (i = 0; i < bwc->bwc_nbmpces; i++) {
		bmpce = bwc->bwc_bmpces[i];

		bwc->bwc_iovs[i].iov_base = bmpce->bmpce_entry->page_buf +
		    (i ? 0 : r->biorq_off - bmpce->bmpce_off);

		bwc->bwc_iovs[i].iov_len = MIN(tot_reqsz,
		    i ? BMPC_BUFSZ :
		    BMPC_BUFSZ - (r->biorq_off - bmpce->bmpce_off));

		tot_reqsz -= bwc->bwc_iovs[i].iov_len;
		bwc->bwc_niovs++;
		OPSTAT_INCR("msl.write-coalesce");
	}
	psc_atomic32_setmax(&slc_write_coalesce_max, bwc->bwc_niovs);

	psc_assert(bwc->bwc_niovs <= BMPC_COALESCE_MAX_IOV);
	psc_assert(!tot_reqsz);
}

/*
 * Check if we can flush the given bmp now.
 */
__static int
bmap_flushable(struct bmap *b)
{
	int flush;
	struct timespec ts;
	struct bmap_pagecache *bmpc;

	bmpc = bmap_2_bmpc(b);
	flush = !RB_EMPTY(&bmpc->bmpc_biorqs);

	if (flush) {
		PFL_GETTIMESPEC(&ts);
		ts.tv_sec += BMAP_CLI_EXTREQSECS;
		/*
		 * XXX: If the IOS is done, we will try to get a lease.
		 * However, MDS will reject us with -1010. So we can't
		 * flush the bmap. We should bail after some number of
		 * retries.
		 */
		if (timespeccmp(&bmap_2_bci(b)->bci_etime, &ts, <) ||
		    (b->bcm_flags & BMAPF_LEASEEXPIRE)) {
			OPSTAT_INCR("msl.flush-skip-expire");
			flush = 0;
		}
	}

	return (flush);
}

static void
bwc_desched(struct bmpc_write_coalescer *bwc)
{
	psc_dynarray_reset(&bwc->bwc_biorqs);
	bwc->bwc_soff = bwc->bwc_size = 0;
}

/*
 * Scan the given array of I/O requests for candidates to flush.  We
 * only flush when (1) a request has aged out or (2) we can construct a
 * large enough I/O.
 */
__static struct bmpc_write_coalescer *
bmap_flush_trycoalesce(const struct psc_dynarray *biorqs, int *indexp)
{
	int idx, large = 0, expired = 0;
	struct bmpc_write_coalescer *bwc;
	struct bmpc_ioreq *curr, *last = NULL;
	int32_t sz = 0;

	psc_assert(psc_dynarray_len(biorqs) > *indexp);

	bwc = bwc_alloc();

	for (idx = 0; idx + *indexp < psc_dynarray_len(biorqs);
	    idx++, last = curr) {
		curr = psc_dynarray_getpos(biorqs, idx + *indexp);

		if (curr->biorq_retries && !bmap_flush_biorq_expired(curr, 0)) {
			OPSTAT_INCR("msl.bmap-flush-wait-retry");
			break;
		}

		/*
		 * If any member is expired then we'll push everything
		 * out.
		 */
		if (!expired)
			expired = bmap_flush_biorq_expired(curr, 1);

		DEBUG_BIORQ(PLL_DIAG, curr, "biorq #%d (expired=%d)",
		    idx, expired);

		if (idx)
			/* Assert 'lowest to highest' ordering. */
			psc_assert(curr->biorq_off >= last->biorq_off);
		else {
			bwc->bwc_size = curr->biorq_len;
			bwc->bwc_soff = curr->biorq_off;
			psc_dynarray_add(&bwc->bwc_biorqs, curr);
			continue;
		}

		/*
		 * The next request, 'curr', can be added to the
		 * coalesce group because 'curr' overlaps or extends
		 * 'last'.
		 */
		if (curr->biorq_off <= biorq_voff_get(last)) {
			sz = biorq_voff_get(curr) - biorq_voff_get(last);
			if (sz > 0) {
				if (sz + bwc->bwc_size >
				    MIN_COALESCE_RPC_SZ) {
					/*
					 * Adding this biorq will push
					 * us over the limit.
					 */
					large = 1;
					break;
				} else {
					bwc->bwc_size += sz;
				}
				OPSTAT_INCR("msl.bmap-flush-coalesce-contig");
			}
			/*
			 * All subsequent requests that do not extend
			 * our range should be collapsed here.
			 */
			psc_dynarray_add(&bwc->bwc_biorqs, curr);

			/* keep the old last if we didn't extend */
			if (sz < 0)
				curr = last;

		} else if (expired) {
			/*
			 * Biorq is not contiguous with the previous.
			 * If the current set is expired send it out
			 * now.
			 */
			OPSTAT_INCR("msl.bmap-flush-coalesce-expire");
			break;

		} else {
			/*
			 * Otherwise, deschedule the current set and
			 * resume activity with 't' as the base.
			 */
			bwc_desched(bwc);
			bwc->bwc_size = curr->biorq_len;
			bwc->bwc_soff = curr->biorq_off;
			psc_dynarray_add(&bwc->bwc_biorqs, curr);
			OPSTAT_INCR("msl.bmap-flush-coalesce-restart");
		}
	}

	if (!(large || expired)) {
		/* Clean up any lingering biorq's. */
		bwc_desched(bwc);
		bwc_free(bwc);
		bwc = NULL;
	}

	*indexp += idx;

	return (bwc);
}


/*
 * Send out SRMT_WRITE RPCs to the I/O server.
 */
__static int
bmap_flush(struct psc_dynarray *reqs, struct psc_dynarray *bmaps)
{
	struct bmpc_write_coalescer *bwc;
	struct bmap_pagecache *bmpc;
	struct bmpc_ioreq *r;
	struct bmap *b, *tmpb;
	int i, j, rc, didwork = 0;
	struct sl_resm *m;

	LIST_CACHE_LOCK(&msl_bmapflushq);
	LIST_CACHE_FOREACH_SAFE(b, tmpb, &msl_bmapflushq) {

		DEBUG_BMAP(PLL_DIAG, b, "flushable?");

		if (!BMAP_TRYLOCK(b))
			continue;

		psc_assert(b->bcm_flags & BMAPF_FLUSHQ);

		if ((b->bcm_flags & BMAPF_SCHED) ||
		    (b->bcm_flags & BMAPF_REASSIGNREQ)) {
			BMAP_ULOCK(b);
			continue;
		}
		m = libsl_ios2resm(bmap_2_ios(b));
		rc = msl_resm_throttle_yield(m);
		if (!rc && bmap_flushable(b)) {
			b->bcm_flags |= BMAPF_SCHED;
			psc_dynarray_add(bmaps, b);
			bmap_op_start_type(b, BMAP_OPCNT_FLUSH);
		}
		BMAP_ULOCK(b);

		if (psc_dynarray_len(bmaps) >= msl_ios_max_inflight_rpcs)
			break;
	}
	LIST_CACHE_ULOCK(&msl_bmapflushq);

	for (i = 0; i < psc_dynarray_len(bmaps); i++) {
		b = psc_dynarray_getpos(bmaps, i);
		bmpc = bmap_2_bmpc(b);

		BMAP_LOCK(b);
		if (!bmap_flushable(b)) {
			OPSTAT_INCR("msl.bmap-flush-bail");
			goto next;
		}
		DEBUG_BMAP(PLL_DIAG, b, "try flush");
		RB_FOREACH(r, bmpc_biorq_tree, &bmpc->bmpc_biorqs) {
			DEBUG_BIORQ(PLL_DEBUG, r, "flushable");
			psc_dynarray_add(reqs, r);
		}
		BMAP_ULOCK(b);

		j = 0;
		rc = 0;
		while (!rc && j < psc_dynarray_len(reqs) &&
		    (bwc = bmap_flush_trycoalesce(reqs, &j))) {
			bmap_flush_coalesce_map(bwc);
			rc = bmap_flush_send_rpcs(bwc);
			if (!rc)
				didwork = 1;
		}
		psc_dynarray_reset(reqs);

		BMAP_LOCK(b);
 next:
		b->bcm_flags &= ~BMAPF_SCHED;
		bmap_op_done_type(b, BMAP_OPCNT_FLUSH);
	}
	psc_dynarray_reset(bmaps);

	return (didwork);
}

void
msflushthr_main(struct psc_thread *thr)
{
	struct timespec work, delta, tmp1, tmp2;
	struct msflush_thread *mflt;
	struct psc_dynarray reqs = DYNARRAY_INIT;
	struct psc_dynarray bmaps = DYNARRAY_INIT;

	mflt = msflushthr(thr);
	mflt->mflt_credits = 0;
	while (pscthr_run(thr)) {
		/*
		 * Reset deep counter incremented during successive RPC
		 * failure used to track when to give up (hard retry
		 * count).
		 */
		mflt->mflt_failcnt = 1;

		/* wait until some work appears */
		if (lc_peekheadwait(&msl_bmapflushq) == NULL)
			break;

		OPSTAT_INCR("msl.bmap-flush");

		PFL_GETTIMESPEC(&tmp1);
		while (bmap_flush(&reqs, &bmaps))
			;
		PFL_GETTIMESPEC(&tmp2);

		timespecsub(&tmp2, &tmp1, &work);

		spinlock(&slc_bflush_lock);
		slc_bflush_tmout_flags |= BMAPFLSH_RPCWAIT;
		psc_waitq_waitrel_ts(&slc_bflush_waitq,
		    &slc_bflush_lock, &msl_bflush_timeout);
		spinlock(&slc_bflush_lock);
		slc_bflush_tmout_flags &= ~BMAPFLSH_RPCWAIT;
		freelock(&slc_bflush_lock);

		PFL_GETTIMESPEC(&tmp1);
		timespecsub(&tmp1, &tmp2, &delta);

		psclogs_debug(SLSS_BMAP, "work time ("PSCPRI_TIMESPEC"),"
		    "wait time ("PSCPRI_TIMESPEC")",
		    PSCPRI_TIMESPEC_ARGS(&work),
		    PSCPRI_TIMESPEC_ARGS(&delta));
	}

	psc_dynarray_free(&reqs);
	psc_dynarray_free(&bmaps);
}

void
msbenchthr_main(__unusedx struct psc_thread *thr)
{
#if 0
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r;

	pscthr_setpause(thr, 1);
	while (pscthr_run(thr)) {
		e = psc_pool_get(bmpce_pool);
		r = bmpc_biorq_new(q, b, buf, rqnum, off, len, op); {
		msl_pages_schedflush(r);
	}
#endif
}

void
msbmapthr_spawn(void)
{
	struct msflush_thread *mflt;
	struct psc_thread *thr;
	int i;

	psc_waitq_init(&slc_bflush_waitq, "bmap-flush");

	lc_reginit(&msl_bmapflushq, struct bmap,
	    bcm_lentry, "bmapflushq");

	lc_reginit(&msl_bmaptimeoutq, struct bmap_cli_info,
	    bci_lentry, "bmaptimeout");

	for (i = 0; i < NUM_BMAP_FLUSH_THREADS; i++) {
		thr = pscthr_init(MSTHRT_FLUSH, msflushthr_main,
		    sizeof(struct msflush_thread), "msflushthr%d", i);
		mflt = msflushthr(thr);
		pfl_multiwait_init(&mflt->mflt_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}

	/*
 	 * We have one lease watcher and one lease release thread.
 	 * The code is thread-safe though. So we can add more if 
 	 * need be.
 	 */
	thr = pscthr_init(MSTHRT_BWATCH, msbwatchthr_main, 
	    sizeof(struct msbwatch_thread), "msbwatchthr");
	pfl_multiwait_init(&msbwatchthr(thr)->mbwt_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

	thr = pscthr_init(MSTHRT_BRELEASE, msbreleasethr_main,
	    sizeof(struct msbrelease_thread), "msbreleasethr");
	pfl_multiwait_init(&msbreleasethr(thr)->mbrt_mw, "%s",
	    thr->pscthr_name);
	pscthr_setready(thr);

#if 0
	pscthr_init(MSTHRT_BENCH, msbenchthr_main, NULL, 0,
	    "msbenchthr");
#endif

}
