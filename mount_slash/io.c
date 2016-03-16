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
 * Client I/O and related routines: caching, RPC scheduling, etc.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "pfl/completion.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fault.h"
#include "pfl/fs.h"
#include "pfl/fsmod.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/opstats.h"
#include "pfl/pfl.h"
#include "pfl/random.h"
#include "pfl/rpc.h"
#include "pfl/rpclog.h"
#include "pfl/rsx.h"
#include "pfl/treeutil.h"

#include "bmap.h"
#include "bmap_cli.h"
#include "pgcache.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"
#include "slerr.h"
#include "subsys_cli.h"

__static void	msl_biorq_complete_fsrq(struct bmpc_ioreq *);
__static size_t	msl_pages_copyin(struct bmpc_ioreq *);
__static void	msl_pages_schedflush(struct bmpc_ioreq *);

__static void	msl_update_attributes(struct msl_fsrqinfo *);

/* Flushing fs threads wait here for I/O completion. */
struct psc_waitq	 msl_fhent_aio_waitq = PSC_WAITQ_INIT;

struct timespec		 msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec		 msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

int			 msl_predio_window_size = 4 * 1024 * 1024;
int			 msl_predio_issue_minpages = LNET_MTU / BMPC_BUFSZ;
int			 msl_predio_issue_maxpages = SLASH_BMAP_SIZE / BMPC_BUFSZ * 8;

struct pfl_iostats_grad	 slc_iosyscall_iostats[8];
struct pfl_iostats_grad	 slc_iorpc_iostats[8];

struct psc_poolmaster	 slc_readaheadrq_poolmaster;
struct psc_poolmgr	*slc_readaheadrq_pool;
struct psc_listcache	 msl_readaheadq;

void
msl_update_iocounters(struct pfl_iostats_grad *ist, enum rw rw, int len)
{
	for (; ist->size && len >= ist->size; ist++)
		;
	pfl_opstat_incr(rw == SL_READ ? ist->rw.rd : ist->rw.wr);
}

#define msl_biorq_page_valid_accounting(r, idx)				\
	_msl_biorq_page_valid((r), (idx), 1)

#define msl_biorq_page_valid(r, idx)					\
	_msl_biorq_page_valid((r), (idx), 0)

__static int
_msl_biorq_page_valid(struct bmpc_ioreq *r, int idx, int accounting)
{
	struct bmap_pagecache_entry *e;
	uint32_t toff, tsize, nbytes;
	int i;

	toff = r->biorq_off;
	tsize = r->biorq_len;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {

		if (!i && toff > e->bmpce_off)
			nbytes = MIN(BMPC_BUFSZ - (toff - e->bmpce_off),
			    tsize);
		else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		if (i != idx) {
			toff += nbytes;
			tsize -= nbytes;
			continue;
		}

		if (e->bmpce_flags & BMPCEF_DATARDY) {
			if (accounting)
				OPSTAT2_ADD("msl.rd-cache-hit", nbytes);

			return (1);
		}

		if (toff >= e->bmpce_start &&
		    toff + nbytes <= e->bmpce_start + e->bmpce_len) {
			if (accounting) {
				OPSTAT2_ADD("msl.rd-cache-hit", nbytes);
				OPSTAT_INCR("msl.read-part-valid");
			}
			return (1);
		}
		if (accounting)
			psc_fatalx("biorq %p does not valid data", r);

		return (0);
	}
	psc_fatalx("biorq %p does not have page %d", r, idx);
}

/*
 * Enqueue some predictive I/O work.
 */
void
predio_enqueue(const struct sl_fidgen *fgp, sl_bmapno_t bno,
    enum rw rw, uint32_t off, int npages)
{
	struct readaheadrq *rarq;

	psc_assert(rw == SL_READ || rw == SL_WRITE);
	rarq = psc_pool_tryget(slc_readaheadrq_pool);
	if (rarq == NULL)
		return;
	INIT_PSC_LISTENTRY(&rarq->rarq_lentry);
	rarq->rarq_rw = rw;
	rarq->rarq_fg = *fgp;
	rarq->rarq_bno = bno;
	rarq->rarq_off = off;
	rarq->rarq_npages = npages;
	lc_add(&msl_readaheadq, rarq);
}

/*
 * Construct a request structure for an I/O issued on a bmap.
 * Notes: roff is bmap aligned.
 */
__static struct bmpc_ioreq *
msl_biorq_build(struct msl_fsrqinfo *q, struct bmap *b, char *buf,
    uint32_t roff, uint32_t len, int op)
{
	uint32_t aoff, alen, bmpce_off;
	struct msl_fhent *mfh = q->mfsrq_mfh;
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r;
	int i, npages;

	DEBUG_BMAP(PLL_DIAG, b,
	    "adding req for (off=%u) (size=%u)", roff, len);
	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "adding req for (off=%u) (size=%u)", roff, len);

	psc_assert(len);
	psc_assert(roff + len <= SLASH_BMAP_SIZE);

	r = bmpc_biorq_new(q, b, buf, roff, len, op);
	/*
	 * If the request is set to use Direct I/O, then we don't
	 * need to associate pages with it.
	 */
	if (r->biorq_flags & BIORQ_DIO)
		return (r);

	/*
	 * Align the offset and length to the start of a page.  Note
	 * that roff is already made relative to the start of the given
	 * bmap.
	 */
	aoff = roff & ~BMPC_BUFMASK;
	alen = len + (roff & BMPC_BUFMASK);

	/*
	 * Calculate the number of pages needed to accommodate this
	 * request.
	 */
	npages = alen / BMPC_BUFSZ;

	if (alen % BMPC_BUFSZ)
		npages++;

	/*
	 * Now populate pages which correspond to this request.
	 */
	for (i = 0; i < npages; i++) {
		bmpce_off = aoff + (i * BMPC_BUFSZ);

		bmpce_lookup(r, b, 0, bmpce_off,
		    &b->bcm_fcmh->fcmh_waitq, &e);
	}
	return (r);
}

__static void
msl_biorq_del(struct bmpc_ioreq *r)
{
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct bmap_pagecache_entry *e;
	int i;

	BMAP_LOCK(b);

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		bmpce_release(e);
	}
	psc_dynarray_free(&r->biorq_pages);

	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_ONTREE) {
		PSC_RB_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs, r);
		pll_remove(&bmpc->bmpc_new_biorqs_exp, r);
	}

	if (r->biorq_flags & BIORQ_FLUSHRDY) {
		psc_assert(bmpc->bmpc_pndg_writes > 0);
		psc_assert(b->bcm_flags & BMAPF_FLUSHQ);
		bmpc->bmpc_pndg_writes--;
		if (!bmpc->bmpc_pndg_writes) {
			b->bcm_flags &= ~BMAPF_FLUSHQ;
			// XXX locking violation
			lc_remove(&msl_bmapflushq, b);
			DEBUG_BMAP(PLL_DIAG, b,
			    "remove from msl_bmapflushq");
		}
	}

	DEBUG_BMAP(PLL_DIAG, b, "remove biorq=%p nitems_pndg=%d",
	    r, pll_nitems(&bmpc->bmpc_pndg_biorqs));

	psc_waitq_wakeall(&bmpc->bmpc_waitq);

	bmap_op_done_type(b, BMAP_OPCNT_BIORQ);
}

void
msl_bmpces_fail(struct bmpc_ioreq *r, int rc)
{
	struct bmap_pagecache_entry *e;
	int i;

	if (!(r->biorq_flags & BIORQ_WRITE))
		return;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		DEBUG_BMPCE(PLL_DIAG, e, "set BMPCEF_EIO");
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCEF_EIO;
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}
}

#define msl_biorq_destroy(r)	 _msl_biorq_destroy(PFL_CALLERINFO(), (r))

void
_msl_biorq_destroy(const struct pfl_callerinfo *pci,
    struct bmpc_ioreq *r)
{
	DEBUG_BIORQ(PLL_DIAG, r, "destroying");

	psc_assert(r->biorq_ref == 0);
	psc_assert(!(r->biorq_flags & BIORQ_DESTROY));
	r->biorq_flags |= BIORQ_DESTROY;

	/*
	 * DIO mode doesn't copy data as the address is used directly
	 * and as such doesn't need freed by us.
	 */
	if (r->biorq_flags & BIORQ_FREEBUF)
		PSCFREE(r->biorq_buf);

	msl_biorq_del(r);

	OPSTAT_INCR("msl.biorq-destroy");
	psc_pool_return(msl_biorq_pool, r);
}

#define biorq_incref(r)		_biorq_incref(PFL_CALLERINFO(), (r))

void
_biorq_incref(const struct pfl_callerinfo *pci, struct bmpc_ioreq *r)
{
	int locked;

	locked = BIORQ_RLOCK(r);
	r->biorq_ref++;
	DEBUG_BIORQ(PLL_DIAG, r, "incref");
	BIORQ_URLOCK(r, locked);
}

void
_msl_biorq_release(const struct pfl_callerinfo *pci,
    struct bmpc_ioreq *r)
{
	BIORQ_LOCK(r);
	if (r->biorq_ref == 1 &&
	    ((r->biorq_flags & BIORQ_READ) ||
	    !(r->biorq_flags & BIORQ_FLUSHRDY))) {
		BIORQ_ULOCK(r);
		/*
		 * A request can be split into several RPCs so we can't
		 * declare it as complete until after its reference
		 * count drops to zero.
		 */
		msl_biorq_complete_fsrq(r);
		BIORQ_LOCK(r);
	}

	psc_assert(r->biorq_ref > 0);
	r->biorq_ref--;
	DEBUG_BIORQ(PLL_DIAG, r, "decref");
	if (r->biorq_ref) {
		BIORQ_ULOCK(r);
		return;
	}
	/* no locking is needed afterwards */
	BIORQ_ULOCK(r);
	msl_biorq_destroy(r);
}

struct msl_fhent *
msl_fhent_new(struct pscfs_req *pfr, struct fidc_membh *f)
{
	struct pscfs_creds pcr;
	struct msl_fhent *mfh;

	mfh = psc_pool_get(msl_mfh_pool);
	memset(mfh, 0, sizeof(*mfh));
	mfh->mfh_refcnt = 1;
	mfh->mfh_fcmh = f;
	mfh->mfh_pid = pscfs_getclientctx(pfr)->pfcc_pid;
	mfh->mfh_sid = getsid(mfh->mfh_pid);
	mfh->mfh_accessing_euid = slc_getfscreds(pfr, &pcr)->pcr_uid;
	INIT_SPINLOCK(&mfh->mfh_lock);
	INIT_PSC_LISTENTRY(&mfh->mfh_lentry);

	if (!fcmh_isdir(f)) {
		struct pfl_callerinfo pci;

		pci.pci_subsys = SLCSS_INFO;
		if (psc_log_shouldlog(&pci, PLL_INFO))
			slc_getuprog(mfh->mfh_pid, mfh->mfh_uprog,
			    sizeof(mfh->mfh_uprog));
	}

	return (mfh);
}

/*
 * Obtain a csvc connection to an IOS that has residency for a given
 * bmap.
 *
 * @b: bmap.
 * @iosidx: numeric index into file inode replica table for IOS to try.
 * @require_valid: when READ is performed on a new bmap, which will has
 *	replicas marked VALID, set this flag to zero to acquire a
 *	connection to any IOS.  This is a hack as no RPC should take
 *	place at all...
 *	XXX This entire approach should be changed.
 * @csvcp: value-result service handle.
 */
int
msl_try_get_replica_res(struct bmap *b, int iosidx, int require_valid,
    struct sl_resm **pm, struct slashrpc_cservice **csvcp)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *m;

	if (require_valid && SL_REPL_GET_BMAP_IOS_STAT(bci->bci_repls,
	    iosidx * SL_BITS_PER_REPLICA) != BREPLST_VALID)
		return (-2);

	fci = fcmh_2_fci(b->bcm_fcmh);
	res = libsl_id2res(fci->fci_inode.reptbl[iosidx].bs_id);
	if (res == NULL) {
		DEBUG_FCMH(PLL_ERROR, b->bcm_fcmh,
		    "unknown IOS in reptbl: %#x",
		    fci->fci_inode.reptbl[iosidx].bs_id);
		return (-1);
	}

	/* XXX not a real shuffle */
	FOREACH_RND(&it, psc_dynarray_len(&res->res_members)) {
		m = psc_dynarray_getpos(&res->res_members,
		    it.ri_rnd_idx);
		*csvcp = slc_geticsvc_nb(m);
		if (*csvcp) {
			if (pm)
				*pm = m;
			return (0);
		}
	}
	return (-1);
}

#define msl_fsrq_aiowait_tryadd_locked(e, r)				\
	_msl_fsrq_aiowait_tryadd_locked(PFL_CALLERINFO(), (e), (r))

__static void
_msl_fsrq_aiowait_tryadd_locked(const struct pfl_callerinfo *pci,
    struct bmap_pagecache_entry *e, struct bmpc_ioreq *r)
{
	LOCK_ENSURE(&e->bmpce_lock);

	BIORQ_LOCK(r);
	if (!(r->biorq_flags & BIORQ_WAIT)) {
		biorq_incref(r);
		r->biorq_flags |= BIORQ_WAIT;
		DEBUG_BIORQ(PLL_DIAG, r, "blocked by %p", e);
		pll_add(&e->bmpce_pndgaios, r);
	}
	BIORQ_ULOCK(r);
}

__static int
msl_req_aio_add(struct pscrpc_request *rq,
    int (*cbf)(struct pscrpc_request *, int, struct pscrpc_async_args *),
    struct pscrpc_async_args *av)
{
	struct psc_dynarray *a = av->pointer_arg[MSL_CBARG_BMPCE];
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r = NULL;
	struct slc_async_req *car;
	struct srm_io_rep *mp;
	struct sl_resm *m;
	int i;

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	m = libsl_nid2resm(rq->rq_peer.nid);

	car = psc_pool_get(msl_async_req_pool);
	memset(car, 0, sizeof(*car));
	INIT_LISTENTRY(&car->car_lentry);
	car->car_id = mp->id;
	car->car_cbf = cbf;

	/*
	 * pscfs_req has the pointers to each biorq needed for
	 * completion.
	 */
	memcpy(&car->car_argv, av, sizeof(*av));

	if (cbf == msl_read_cleanup) {
		int naio = 0;

		OPSTAT_INCR("msl.aio-register-read");
		r = av->pointer_arg[MSL_CBARG_BIORQ];
		DYNARRAY_FOREACH(e, i, a) {
			BMPCE_LOCK(e);
			/* XXX potential conflict with new read RPC launch */
			if (e->bmpce_flags & BMPCEF_FAULTING) {
				naio++;
				e->bmpce_flags &= ~BMPCEF_FAULTING;
				e->bmpce_flags |= BMPCEF_AIOWAIT;
				DEBUG_BMPCE(PLL_DIAG, e, "naio=%d", naio);
			}
			BMPCE_ULOCK(e);
		}
		/* Should have found at least one aio'd page. */
		if (!naio)
			psc_fatalx("biorq %p has no AIO pages", r);

		car->car_fsrqinfo = r->biorq_fsrqi;

	} else if (cbf == msl_dio_cleanup) {

		OPSTAT_INCR("msl.aio-register-dio");

		r = av->pointer_arg[MSL_CBARG_BIORQ];
		if (r->biorq_flags & BIORQ_WRITE)
			av->pointer_arg[MSL_CBARG_BIORQ] = NULL;

		BIORQ_LOCK(r);
		r->biorq_flags |= BIORQ_AIOWAKE;
		BIORQ_ULOCK(r);

		car->car_fsrqinfo = r->biorq_fsrqi;
	}

	psclog_diag("add car=%p car_id=%"PRIx64" q=%p r=%p",
	    car, car->car_id, car->car_fsrqinfo, r);

	lc_add(&resm2rmci(m)->rmci_async_reqs, car);

	return (SLERR_AIOWAIT);
}

void
mfsrq_clrerr(struct msl_fsrqinfo *q)
{
	int lk;

	lk = MFH_RLOCK(q->mfsrq_mfh);
	if (q->mfsrq_err) {
		DPRINTF_MFSRQ(PLL_WARN, q, "clearing err=%d",
		    q->mfsrq_err);
		q->mfsrq_err = 0;
		OPSTAT_INCR("msl.offline-retry-clear-err");
	}
	MFH_URLOCK(q->mfsrq_mfh, lk);
}

void
mfsrq_seterr(struct msl_fsrqinfo *q, int rc)
{
	MFH_LOCK(q->mfsrq_mfh);
	if (q->mfsrq_err == 0 && rc) {
		q->mfsrq_err = rc;
		DPRINTF_MFSRQ(PLL_WARN, q, "setting err=%d", rc);
	}
	MFH_ULOCK(q->mfsrq_mfh);
}

void
biorq_bmpces_setflag(struct bmpc_ioreq *r, int flag)
{
	struct bmap_pagecache_entry *e;
	int i, newval;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		newval = e->bmpce_flags | flag;
		if (e->bmpce_flags & BMPCEF_READALC) {
			lc_remove(&msl_readahead_pages, e);
			lc_add(&msl_idle_pages, e);
			newval = (newval & ~BMPCEF_READALC) |
			    BMPCEF_IDLE;
		}
		e->bmpce_flags = newval;
		BMPCE_ULOCK(e);
	}
}

void
msl_complete_fsrq(struct msl_fsrqinfo *q, size_t len,
    struct bmpc_ioreq *r0)
{
	void *oiov = q->mfsrq_iovs;
	struct msl_fhent *mfh;
	struct pscfs_req *pfr;
	struct bmpc_ioreq *r;
	struct fidc_membh *f;
	int i;

	pfr = mfsrq_2_pfr(q);
	mfh = q->mfsrq_mfh;

	MFH_LOCK(mfh);
	if (!q->mfsrq_err) {
		q->mfsrq_len += len;
		psc_assert(q->mfsrq_len <= q->mfsrq_size);
		if (q->mfsrq_flags & MFSRQ_READ)
			mfh->mfh_nbytes_rd += len;
		else
			mfh->mfh_nbytes_wr += len;
	}

	psc_assert(q->mfsrq_ref > 0);
	q->mfsrq_ref--;
	DPRINTF_MFSRQ(PLL_DIAG, q, "decref");
	if (q->mfsrq_ref) {
		if (r0)
			biorq_incref(r0);
		MFH_ULOCK(mfh);
		return;
	}
	psc_assert((q->mfsrq_flags & MFSRQ_FSREPLIED) == 0);
	q->mfsrq_flags |= MFSRQ_FSREPLIED;
	mfh_decref(mfh);

	if (q->mfsrq_flags & MFSRQ_READ) {
		if (q->mfsrq_err) {
			pscfs_reply_read(pfr, NULL, 0,
			    abs(q->mfsrq_err));
		} else {
			if (q->mfsrq_len == 0) {
				pscfs_reply_read(pfr, NULL, 0, 0);
			} else if (q->mfsrq_iovs) {
				psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

				for (i = 0; i < MAX_BMAPS_REQ; i++) {
					r = q->mfsrq_biorq[i];
					if (!r)
						break;
					biorq_bmpces_setflag(r,
					    BMPCEF_ACCESSED);
				}

				pscfs_reply_read(pfr, q->mfsrq_iovs,
				    q->mfsrq_niov, 0);
			} else {
				struct iovec iov[MAX_BMAPS_REQ];
				int nio = 0;

				psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

				for (i = 0; i < MAX_BMAPS_REQ; i++) {
					r = q->mfsrq_biorq[i];
					if (!r)
						break;
					iov[nio].iov_base = r->biorq_buf;
					iov[nio].iov_len = r->biorq_len;
					nio++;

					biorq_bmpces_setflag(r,
					    BMPCEF_ACCESSED);
				}
				pscfs_reply_read(pfr, iov, nio, 0);
			}
		}
	} else {
		if (!q->mfsrq_err) {
			msl_update_attributes(q);
			psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

			for (i = 0; i < MAX_BMAPS_REQ; i++) {
				r = q->mfsrq_biorq[i];
				if (!r)
					break;
				biorq_bmpces_setflag(r,
				    BMPCEF_ACCESSED);
			}
		}
		pscfs_reply_write(pfr, q->mfsrq_len, abs(q->mfsrq_err));
	}

	f = mfh->mfh_fcmh;
	DEBUG_FCMH(q->mfsrq_err ? PLL_NOTICE : PLL_DIAG, f,
	    "reply: off=%"PRId64" size=%zu rw=%s "
	    "rc=%d", q->mfsrq_off, q->mfsrq_len,
	    q->mfsrq_flags & MFSRQ_READ ?
	    "read" : "write", q->mfsrq_err);

	PSCFREE(oiov);

	for (i = 0; i < MAX_BMAPS_REQ; i++) {
		r = q->mfsrq_biorq[i];
		if (!r)
			break;
		if (r == r0)
			continue;
		msl_biorq_release(r);
	}

	psc_pool_return(msl_iorq_pool, q);
}

void
msl_biorq_complete_fsrq(struct bmpc_ioreq *r)
{
	int i, needflush = 0;
	struct msl_fsrqinfo *q;
	size_t len = 0;

	/* don't do anything for readahead */
	BIORQ_LOCK(r);
	q = r->biorq_fsrqi;
	r->biorq_fsrqi = NULL;
	BIORQ_ULOCK(r);

	if (q == NULL)
		return;

	DEBUG_BIORQ(PLL_DIAG, r, "copying");

	/* ensure biorq is in fsrq */
	for (i = 0; i < MAX_BMAPS_REQ; i++)
		if (r == q->mfsrq_biorq[i])
			break;
	if (i == MAX_BMAPS_REQ)
		DEBUG_BIORQ(PLL_FATAL, r, "missing biorq in fsrq");

	MFH_LOCK(q->mfsrq_mfh);
	if (q->mfsrq_err) {
	} else if (r->biorq_flags & BIORQ_DIO) {
		/*
		 * Support mix of dio and cached reads.  This may occur
		 * if the read request spans bmaps.  The 'len' here was
		 * possibly adjusted against the tail of the file in
		 * msl_io().
		 */
		len = r->biorq_len;
	} else {
		if (q->mfsrq_flags & MFSRQ_READ) {
			/*
			 * Lock to update iovs attached to q.
			 * Fast because no actual copying.
			 */
			len = msl_pages_copyout(r, q);
		} else {
			len = msl_pages_copyin(r);
			needflush = 1;
		}
		q->mfsrq_flags |= MFSRQ_COPIED;
	}
	MFH_ULOCK(q->mfsrq_mfh);

	if (needflush)
		msl_pages_schedflush(r);

	msl_complete_fsrq(q, len, r);
}

/*
 * Try to complete biorqs waiting on this page cache entry.
 */
__static void
msl_bmpce_complete_biorq(struct bmap_pagecache_entry *e0, int rc)
{
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r;
	int i;

	/*
	 * The owning request of the cache entry should not be on its
	 * own pending list, so it should not go away in the process.
	 */
	while ((r = pll_get(&e0->bmpce_pndgaios))) {
		BIORQ_LOCK(r);
		r->biorq_flags &= ~BIORQ_WAIT;
		mfsrq_seterr(r->biorq_fsrqi, rc);
		BIORQ_ULOCK(r);
		DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
			BMPCE_LOCK(e);
			if (e->bmpce_flags & BMPCEF_FAULTING) {
				e->bmpce_flags &= ~BMPCEF_FAULTING;
				BMPCE_WAKE(e);
				BMPCE_ULOCK(e);
				continue;
			}
			if (e->bmpce_flags & BMPCEF_AIOWAIT) {
				psc_assert(e != e0);
				msl_fsrq_aiowait_tryadd_locked(e, r);
				DEBUG_BIORQ(PLL_DIAG, r,
				    "still blocked on (bmpce@%p)", e);
			}
			/*
			 * Need to check any error on the page here.
			 */
			BMPCE_ULOCK(e);
		}
		DEBUG_BIORQ(PLL_DIAG, r, "unblocked by (bmpce@%p)", e);
		msl_biorq_release(r);
	}
}

#define msl_bmpce_read_rpc_done(e, rc)					\
	_msl_bmpce_read_rpc_done(PFL_CALLERINFOSS(SLSS_BMAP), (e), (rc))

__static void
_msl_bmpce_read_rpc_done(const struct pfl_callerinfo *pci,
    struct bmap_pagecache_entry *e, int rc)
{
	BMPCE_LOCK(e);
	psc_assert(e->bmpce_waitq);

	/* AIOWAIT is removed no matter what. */
	e->bmpce_flags &= ~(BMPCEF_AIOWAIT | BMPCEF_FAULTING);

	if (rc) {
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCEF_EIO;
	} else {
		e->bmpce_flags &= ~BMPCEF_EIO;
		e->bmpce_flags |= BMPCEF_DATARDY;
	}

	DEBUG_BMPCE(PLL_DEBUG, e, "rpc_done");

	BMPCE_WAKE(e);
	BMPCE_ULOCK(e);
	msl_bmpce_complete_biorq(e, rc);
}

/*
 * RPC callback used only for read or RBW operations.  The primary
 * purpose is to set the bmpce's to DATARDY so that other threads
 * waiting for DATARDY may be unblocked.
 *
 * Note: Unref of the biorq will happen after the pages have been
 *	copied out to the applicaton buffers.
 */
int
msl_read_cleanup(struct pscrpc_request *rq, int rc,
    struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct psc_dynarray *a = args->pointer_arg[MSL_CBARG_BMPCE];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct bmap_pagecache_entry *e;
	struct srm_io_req *mq;
	struct bmap *b;
	int i;

	b = r->biorq_bmap;

	psc_assert(a);
	psc_assert(b);

	if (rq)
		DEBUG_REQ(rc ? PLL_ERROR : PLL_DIAG, rq,
		    "bmap=%p biorq=%p", b, r);

	(void)pfl_fault_here_rc("slash2/read_cb", &rc, EIO);

	DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b, "rc=%d "
	    "sbd_seq=%"PRId64, rc, bmap_2_sbd(b)->sbd_seq);
	DEBUG_BIORQ(rc ? PLL_ERROR : PLL_DIAG, r, "rc=%d", rc);

	DYNARRAY_FOREACH(e, i, a)
		msl_bmpce_read_rpc_done(e, rc);

	if (rc) {
		if (rc == -PFLERR_KEYEXPIRED) {
			BMAP_LOCK(b);
			b->bcm_flags |= BMAPF_LEASEEXPIRED;
			BMAP_ULOCK(b);
			OPSTAT_INCR("msl.bmap-read-expired");
		}
		if ((r->biorq_flags & BIORQ_READAHEAD) == 0)
			mfsrq_seterr(r->biorq_fsrqi, rc);
	} else {
		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		msl_update_iocounters(slc_iorpc_iostats, SL_READ,
		    mq->size);
		if (r->biorq_flags & BIORQ_READAHEAD)
			OPSTAT2_ADD("msl.readahead-issue", mq->size);
	}

	msl_biorq_release(r);

	/*
	 * Free the dynarray which was allocated in
	 * msl_read_rpc_launch().
	 */
	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);

	return (rc);
}

/*
 * Thin layer around msl_read_cleanup(), which does the real READ
 * completion processing, in case an AIOWAIT is discovered.  Upon
 * completion of the AIO, msl_read_cleanup() is called.
 */
int
msl_read_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	psc_assert(rq->rq_reqmsg->opc == SRMT_READ);

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_read_cleanup, args));

	return (msl_read_cleanup(rq, rc, args));
}

int
msl_dio_cleanup(struct pscrpc_request *rq, int rc,
    struct pscrpc_async_args *args)
{
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct msl_fsrqinfo *q;
	struct srm_io_req *mq;
	int op;

	/* rq is NULL it we are called from sl_resm_hldrop() */
	if (rq) {
		DEBUG_REQ(PLL_DIAG, rq, "cb");

		op = rq->rq_reqmsg->opc;
		psc_assert(op == SRMT_READ || op == SRMT_WRITE);

		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		psc_assert(mq);

		DEBUG_BIORQ(PLL_DIAG, r,
		    "dio complete (op=%d) off=%u sz=%u rc=%d",
		    op, mq->offset, mq->size, rc);
	}

	q = r->biorq_fsrqi;
	mfsrq_seterr(q, rc);
	msl_biorq_release(r);

	if (r->biorq_flags & BIORQ_AIOWAKE) {
		MFH_LOCK(q->mfsrq_mfh);
		psc_waitq_wakeall(&msl_fhent_aio_waitq);
		MFH_ULOCK(q->mfsrq_mfh);
	}

	DEBUG_BIORQ(PLL_DIAG, r, "aiowait wakeup");

	//msl_update_iocounters(slc_iorpc_iostats, rw, bwc->bwc_size);

	return (rc);
}

int
msl_dio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_dio_cleanup, args));

	return (msl_dio_cleanup(rq, rc, args));
}

__static int
msl_pages_dio_getput(struct bmpc_ioreq *r)
{
	int i, op, n, rc;
	size_t len, off, size;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request_set *nbs = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *bci;
	struct msl_fsrqinfo *q;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	struct sl_resm *m;
	struct bmap *b;
	uint64_t *v8;

	psc_assert(r->biorq_bmap);

	b = r->biorq_bmap;
	bci = bmap_2_bci(b);

	size = r->biorq_len;
	n = howmany(size, LNET_MTU);
	iovs = PSCALLOC(sizeof(*iovs) * n);

	v8 = (uint64_t *)r->biorq_buf;
	DEBUG_BIORQ(PLL_DEBUG, r, "dio req v8(%"PRIx64")", *v8);

	if (r->biorq_flags & BIORQ_WRITE) {
		op = SRMT_WRITE;
		OPSTAT_INCR("msl.dio-write");
	} else {
		op = SRMT_READ;
		OPSTAT_INCR("msl.dio-read");
	}

	/*
	 * XXX for read lease, we could inspect throttle limits of other
	 * residencies and use them if available.
	 */
	rc = msl_bmap_to_csvc(b, op == SRMT_WRITE, &m, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);
  retry:
	nbs = pscrpc_prep_set();

	/*
	 * The buffer associated with the request hasn't been segmented
	 * into LNET_MTU-sized chunks.  Do it now.
	 */
	for (i = 0, off = 0; i < n; i++, off += len) {
		len = MIN(LNET_MTU, size - off);

		if (op == SRMT_WRITE)
			rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq,
			    mp);
		else
			rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(out, rc);

		rq->rq_bulk_abortable = 1; // for aio?
		rq->rq_interpret_reply = msl_dio_cb;
		rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
		rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
		iovs[i].iov_base = r->biorq_buf + off;
		iovs[i].iov_len = len;

		rc = slrpc_bulkclient(rq, op == SRMT_WRITE ?
		    BULK_GET_SOURCE : BULK_PUT_SINK, SRIC_BULK_PORTAL,
		    &iovs[i], 1);
		if (rc)
			PFL_GOTOERR(out, rc);

		mq->offset = r->biorq_off + off;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		mq->flags |= SRM_IOF_DIO;

		memcpy(&mq->sbd, &bci->bci_sbd, sizeof(mq->sbd));

		biorq_incref(r);

		rc = SL_NBRQSETX_ADD(nbs, csvc, rq);
		if (rc) {
			msl_biorq_release(r);
			OPSTAT_INCR("msl.dio-add-req-fail");
			PFL_GOTOERR(out, rc);
		}
		rq = NULL;
	}

	/*
	 * Should be no need for a callback since this call is fully
	 * blocking.
	 */
	psc_assert(off == size);

	rc = pscrpc_set_wait(nbs);

	if (rc == -SLERR_AIOWAIT) {
		q = r->biorq_fsrqi;
		MFH_LOCK(q->mfsrq_mfh);
		BIORQ_LOCK(r);
		while (r->biorq_ref > 1) {
			BIORQ_ULOCK(r);
			DEBUG_BIORQ(PLL_DIAG, r, "aiowait sleep");
			psc_waitq_wait(&msl_fhent_aio_waitq,
			    &q->mfsrq_mfh->mfh_lock);
			BIORQ_LOCK(r);
			MFH_LOCK(q->mfsrq_mfh);
		}
		BIORQ_ULOCK(r);
		MFH_ULOCK(q->mfsrq_mfh);
		pscrpc_set_destroy(nbs);
		OPSTAT_INCR("msl.biorq-restart");

		/*
		 * Async I/O registered by sliod; we must wait for a
		 * notification from him when it is ready.
		 */
		goto retry;
	}

	if (rc == 0) {
		if (op == SRMT_WRITE)
			OPSTAT2_ADD("msl.dio-rpc-wr", size);
		else
			OPSTAT2_ADD("msl.dio-rpc-rd", size);

		q = r->biorq_fsrqi;
		MFH_LOCK(q->mfsrq_mfh);
		q->mfsrq_flags |= MFSRQ_COPIED;
		MFH_ULOCK(q->mfsrq_mfh);
	}

 out:
	if (rq)
		pscrpc_req_finished(rq);

	if (nbs)
		pscrpc_set_destroy(nbs);

	if (csvc)
		sl_csvc_decref(csvc);

	PSCFREE(iovs);

	DEBUG_BIORQ(PLL_DIAG, r, "rc=%d", rc);
	return (rc);
}

__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	int i;
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache_entry *e;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAPF_WR);

	/*
	 * The BIORQ_FLUSHRDY bit prevents the request from being
	 * processed prematurely.
	 */
	BIORQ_LOCK(r);
	biorq_incref(r);
	r->biorq_flags |= BIORQ_FLUSHRDY | BIORQ_ONTREE;
	bmpc->bmpc_pndg_writes++;
	PSC_RB_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs, r);
	pll_addtail(&bmpc->bmpc_new_biorqs_exp, r);
	DEBUG_BIORQ(PLL_DIAG, r, "sched flush");

	/* clear BMPCEF_FAULTING after incrementing bmpc_pndg_writes */
	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		psc_assert(e->bmpce_flags & BMPCEF_FAULTING);
		e->bmpce_flags &= ~BMPCEF_FAULTING;
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}

	BIORQ_ULOCK(r);

	if (!(b->bcm_flags & BMAPF_FLUSHQ)) {
		b->bcm_flags |= BMAPF_FLUSHQ;
		lc_addtail(&msl_bmapflushq, b);
		DEBUG_BMAP(PLL_DIAG, b, "add to msl_bmapflushq");
	} else
		bmap_flushq_wake(BMAPFLSH_TIMEOADD);

	DEBUG_BMAP(PLL_DIAG, b, "biorq=%p list_empty=%d",
	    r, pll_empty(&bmpc->bmpc_pndg_biorqs));
	BMAP_ULOCK(b);
}

/*
 * Launch an RPC for a given range of pages.  Note that a request can be
 * satisfied by multiple RPCs because parts of the range covered by the
 * request may have already been cached.
 */
__static int
msl_read_rpc_launch(struct bmpc_ioreq *r, struct psc_dynarray *bmpces,
    int startpage, int npages)
{
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_pagecache_entry *e;
	struct psc_dynarray *a = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	struct sl_resm *m;
	uint32_t off = 0;
	int rc = 0, i;

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);

		BMPCE_LOCK(e);
		/*
		 * A read must wait until all pending writes are flushed. So
		 * we should never see a pinned down page here.
		 */
		psc_assert(!(e->bmpce_flags & BMPCEF_PINNED));

		psc_assert(e->bmpce_flags & BMPCEF_FAULTING);
		psc_assert(!(e->bmpce_flags & BMPCEF_DATARDY));
		DEBUG_BMPCE(PLL_DIAG, e, "page = %d", i + startpage);
		BMPCE_ULOCK(e);

		iovs[i].iov_base = e->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			off = e->bmpce_off;
		psc_dynarray_add(a, e);
	}

	(void)pfl_fault_here_rc("slash2/readrpc_offline", &rc,
	    -ETIMEDOUT);
	if (rc)
		PFL_GOTOERR(out, rc);

	/*
	 * XXX we could inspect throttle limits of other residencies and
	 * use them if available.
	 */
	rc = msl_bmap_to_csvc(r->biorq_bmap,
	    r->biorq_bmap->bcm_flags & BMAPF_WR, &m, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (r->biorq_flags & BIORQ_READAHEAD)
		rc = msl_resm_throttle_yield(m);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	rq->rq_bulk_abortable = 1;
	rc = slrpc_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    npages);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->offset = off;
	mq->size = npages * BMPC_BUFSZ;
	psc_assert(mq->offset + mq->size <= SLASH_BMAP_SIZE);

	mq->op = SRMIOP_RD;
	memcpy(&mq->sbd, bmap_2_sbd(r->biorq_bmap), sizeof(mq->sbd));

	DEBUG_BIORQ(PLL_DIAG, r, "fid="SLPRI_FG" start=%d pages=%d "
	    "ios=%u",
	    SLPRI_FG_ARGS(&mq->sbd.sbd_fg), startpage, npages,
	    bmap_2_ios(r->biorq_bmap));

	/* Setup the callback, supplying the dynarray as an argument. */
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = a;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
	rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
	rq->rq_interpret_reply = msl_read_cb;

	biorq_incref(r);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		msl_biorq_release(r);

		OPSTAT_INCR("msl.read-add-req-fail");
		PFL_GOTOERR(out, rc);
	}

	PSCFREE(iovs);
	return (0);

 out:

	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed rc=%d", rc);
		pscrpc_req_finished(rq);
	}
	if (csvc)
		sl_csvc_decref(csvc);

	PSCFREE(iovs);

	DYNARRAY_FOREACH(e, i, a) {
		/* Didn't get far enough for the waitq to be removed. */
		psc_assert(e->bmpce_waitq);

		BMPCE_LOCK(e);
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCEF_EIO;
		e->bmpce_flags &= ~BMPCEF_FAULTING;
		DEBUG_BMPCE(PLL_DIAG, e, "set BMPCEF_EIO");
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}

	psc_dynarray_free(a);
	PSCFREE(a);

	DEBUG_BIORQ(PLL_DIAG, r, "RPC launch failed (rc=%d)", rc);
	return (rc);
}

__static int
msl_launch_read_rpcs(struct bmpc_ioreq *r)
{
	int rc = 0, i, j, needflush = 0;
	struct psc_dynarray pages = DYNARRAY_INIT;
	struct bmap_pagecache_entry *e;
	uint32_t off = 0;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {

  retry:
		BMPCE_LOCK(e);
		if (msl_biorq_page_valid(r, i)) {
			BMPCE_ULOCK(e);
			if (r->biorq_flags & BIORQ_READAHEAD)
				OPSTAT_INCR("msl.readahead-gratuitous");
			continue;
		}
		/*
		 * The faulting flag could be set by a concurrent writer
		 * that touches a different area in the page, so don't
		 * assume that data is ready when it is cleared.
		 */
		if (e->bmpce_flags & BMPCEF_FAULTING) {
			BMPCE_WAIT(e);
			goto retry;
		}

		e->bmpce_flags |= BMPCEF_FAULTING;
		psc_dynarray_add(&pages, e);
		DEBUG_BMPCE(PLL_DIAG, e, "npages=%d i=%d",
		    psc_dynarray_len(&pages), i);
		BMPCE_ULOCK(e);
		needflush = 1;
	}

	/*
	 * We must flush any pending writes first before reading from
	 * the storage.
	 */
	if (needflush) {
		BMAP_LOCK(r->biorq_bmap);
		bmpc_biorqs_flush(r->biorq_bmap);
		BMAP_ULOCK(r->biorq_bmap);
	}

	j = 0;
	DYNARRAY_FOREACH(e, i, &pages) {
		/*
		 * Note that i > j implies i > 0.  Due to cached pages,
		 * the pages in the array are not necessarily contiguous.
		 */
		if (i > j && e->bmpce_off != off) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j);
			if (rc)
				break;
			j = i;
		}
		if (i - j + 1 == BMPC_MAXBUFSRPC ||
		    i == psc_dynarray_len(&pages) - 1) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j + 1);
			if (rc) {
				i++;
				break;
			}
			j = i + 1;
		}
		off = e->bmpce_off + BMPC_BUFSZ;
	}

	/*
	 * Clean up remaining pages that were not launched.  Note that
	 * msl_read_rpc_launch() cleans up pages on its own in case of a
	 * failure.
	 */
	DYNARRAY_FOREACH_CONT(e, i, &pages) {
		BMPCE_LOCK(e);
		e->bmpce_rc = rc;
		e->bmpce_flags &= ~BMPCEF_FAULTING;
		e->bmpce_flags |= BMPCEF_EIO;
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}

	psc_dynarray_free(&pages);

	return (rc);
}

/*
 * Launch read RPCs for pages that are owned by the given I/O request.
 * This function is called to perform a pure read request or a
 * read-before-write for a write request.  It is also used to wait for
 * read-ahead pages to complete.
 */
int
msl_pages_fetch(struct bmpc_ioreq *r)
{
	int i, rc = 0, aiowait = 0, perfect_ra = 0;
	struct bmap_pagecache_entry *e;
	struct timespec ts0, ts1, tsd;

	if (r->biorq_flags & BIORQ_READ) {
		perfect_ra = 1;
		rc = msl_launch_read_rpcs(r);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	PFL_GETTIMESPEC(&ts0);

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {

		BMPCE_LOCK(e);
		while (e->bmpce_flags & BMPCEF_FAULTING) {
			DEBUG_BMPCE(PLL_DIAG, e, "waiting");
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
			perfect_ra = 0;
		}

		if (e->bmpce_flags & BMPCEF_EIO) {
			rc = e->bmpce_rc;
			BMPCE_ULOCK(e);
			break;
		}

		if (e->bmpce_flags & BMPCEF_READAHEAD) {
			if (!(r->biorq_flags & BIORQ_READAHEAD))
				/*
				 * BMPC_BUFSZ here is a lie but often
				 * true as the original size isn't
				 * available.
				 */
				OPSTAT2_ADD("msl.readahead-hit",
				    BMPC_BUFSZ);
		} else
			perfect_ra = 0;

		if (r->biorq_flags & BIORQ_WRITE) {
			/*
			 * Avoid a race with readahead thread.
			 */
			e->bmpce_flags |= BMPCEF_FAULTING;
			BMPCE_ULOCK(e);
			continue;
		}

		if (msl_biorq_page_valid(r, i)) {
			BMPCE_ULOCK(e);
			continue;
		}

		if (e->bmpce_flags & BMPCEF_AIOWAIT) {
			msl_fsrq_aiowait_tryadd_locked(e, r);
			aiowait = 1;
			BMPCE_ULOCK(e);
			OPSTAT_INCR("msl.aio-placed");
			break;
		}
		BMPCE_ULOCK(e);
	}

	PFL_GETTIMESPEC(&ts1);
	timespecsub(&ts1, &ts0, &tsd);
	OPSTAT_ADD("msl.biorq-fetch-wait-usecs",
	    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);

	if (rc == 0 && !aiowait && perfect_ra)
		OPSTAT2_ADD("msl.readahead-perfect", r->biorq_len);

 out:
	DEBUG_BIORQ(PLL_DIAG, r, "aio=%d rc=%d", aiowait, rc);
	return (rc);
}

/*
 * Copy user pages into buffer cache and schedule them to be sent to the
 * ION backend (i.e. application write(2) servicing).
 *
 * @r: array of request structs.
 */
__static size_t
msl_pages_copyin(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *e;
	uint32_t toff, tsize, nbytes;
	char *dest, *src;
	int i;

	src = r->biorq_buf;
	tsize = r->biorq_len;
	toff = r->biorq_off;

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		/*
		 * All pages are involved, therefore tsize should have
		 * value.
		 */
		psc_assert(tsize);

		BMPCE_LOCK(e);
		while (e->bmpce_flags & BMPCEF_PINNED) {
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
		}

		/*
		 * Re-check RBW sanity.  The waitq pointer within the
		 * bmpce must still be valid in order for this check to
		 * work.
		 *
		 * Set the starting buffer pointer into our cache
		 * vector.
		 */
		dest = e->bmpce_base;
		if (!i && toff > e->bmpce_off) {
			/*
			 * The first cache buffer pointer may need a
			 * bump if the request offset is unaligned.
			 */
			bmpce_usecheck(e, BIORQ_WRITE,
			    (toff & ~BMPC_BUFMASK));
			psc_assert(toff - e->bmpce_off < BMPC_BUFSZ);
			dest += toff - e->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - e->bmpce_off),
			    tsize);
		} else {
			bmpce_usecheck(e, BIORQ_WRITE, toff);
			nbytes = MIN(BMPC_BUFSZ, tsize);
		}

		/* Do the deed. */
		memcpy(dest, src, nbytes);

		if (toff == e->bmpce_off && nbytes == BMPC_BUFSZ)
			e->bmpce_flags |= BMPCEF_DATARDY;
		else if (e->bmpce_len == 0) {
			e->bmpce_start = toff;
			e->bmpce_len = nbytes;
		} else if (toff + nbytes >= e->bmpce_start ||
		    e->bmpce_start + e->bmpce_len >= toff) {

			uint32_t start, end;

			start = toff < e->bmpce_start ? toff : e->bmpce_start;
			end = (toff + nbytes) > (e->bmpce_start + e->bmpce_len) ?
			    toff + nbytes : e->bmpce_start + e->bmpce_len;
			e->bmpce_start = start;
			e->bmpce_len = end - start;
			psc_assert(e->bmpce_len <= BMPC_BUFSZ);
		}

		DEBUG_BMPCE(PLL_DIAG, e,
		    "tsize=%u nbytes=%u toff=%u start=%u len=%u",
		    tsize, nbytes, toff, e->bmpce_start, e->bmpce_len);

		BMPCE_ULOCK(e);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	return (r->biorq_len);
}

/*
 * Copy pages to the user application buffer (i.e. application read(2)
 * servicing).
 */
size_t
msl_pages_copyout(struct bmpc_ioreq *r, struct msl_fsrqinfo *q)
{
	size_t nbytes, tbytes = 0, rflen;
	struct bmap_pagecache_entry *e;
	int i, npages, tsize;
	char *dest, *src;
	off_t toff;

	dest = r->biorq_buf;
	toff = r->biorq_off;

	rflen = fcmh_getsize(r->biorq_bmap->bcm_fcmh) -
	    bmap_foff(r->biorq_bmap);

	if (biorq_voff_get(r) > rflen)
		/* The request goes beyond EOF. */
		tsize = rflen - r->biorq_off;
	else
		tsize = r->biorq_len;

	DEBUG_BIORQ(PLL_DIAG, r, "tsize=%d biorq_len=%u biorq_off=%u",
	    tsize, r->biorq_len, r->biorq_off);

	if (!tsize || tsize < 0)
		return (0);

	npages = psc_dynarray_len(&r->biorq_pages);

	q->mfsrq_iovs = PSC_REALLOC(q->mfsrq_iovs,
	    sizeof(struct iovec) * (q->mfsrq_niov + npages));

	/*
	 * Due to page prefetching, the pages contained in biorq_pages
	 * may exceed the requested len.
	 */
	for (i = 0; i < npages && tsize; i++) {
		e = psc_dynarray_getpos(&r->biorq_pages, i);

		BMPCE_LOCK(e);
		src = e->bmpce_base;
		if (!i && toff > e->bmpce_off) {
			psc_assert(toff - e->bmpce_off < BMPC_BUFSZ);
			src += toff - e->bmpce_off;
			nbytes = MIN(BMPC_BUFSZ - (toff - e->bmpce_off),
			    tsize);
		} else
			nbytes = MIN(BMPC_BUFSZ, tsize);

		DEBUG_BMPCE(PLL_DIAG, e, "tsize=%u nbytes=%zu toff=%"
		    PSCPRIdOFFT, tsize, nbytes, toff);

		msl_biorq_page_valid_accounting(r, i);

		bmpce_usecheck(e, BIORQ_READ, biorq_getaligned_off(r,
		    i));

		q->mfsrq_iovs[q->mfsrq_niov].iov_len = nbytes;
		q->mfsrq_iovs[q->mfsrq_niov].iov_base = src;
		q->mfsrq_niov++;

		BMPCE_ULOCK(e);

		toff   += nbytes;
		dest   += nbytes;
		tbytes += nbytes;
		tsize  -= nbytes;
	}
	if (npages)
		psc_assert(!tsize);

	return (tbytes);
}

/*
 * Calculate the next predictive I/O for an actual I/O request.
 *
 * The actual I/O must be within a window that was set from the previous
 * I/O.
 *
 * Predictive I/O may extend beyond the current bmap as I/O reaches
 * close to the bmap boundary, in which case predio activity is split
 * between 'this' bmap and the following (the caller checks if the next
 * bmap actually exists or not).
 *
 * @mfh: file handle.
 * @bno: bmap number the actual I/O applies to.
 * @rw: whether the predictive I/O is for read or write.
 * @off: offset into bmap of this I/O.
 * @npages: number of pages in the original I/O (will be skipped for the
 * predictive I/O).
 */
__static void
msl_issue_predio(struct msl_fhent *mfh, sl_bmapno_t bno, enum rw rw,
    uint32_t off, int npages)
{
	sl_bmapno_t orig_bno = bno;
	int bsize, tpages, rapages;
	off_t absoff, newissued;
	struct fidc_membh *f;

	MFH_LOCK(mfh);

	/*
	 * Allow either write I/O tracking or read I/O tracking but not
	 * both.
	 */
	if (rw == SL_WRITE) {
		if (mfh->mfh_flags & MFHF_TRACKING_RA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_RA;
			mfh->mfh_flags |= MFHF_TRACKING_WA;
			mfh->mfh_predio_lastoff = 0;
			mfh->mfh_predio_nseq = 0;
		} else if ((mfh->mfh_flags & MFHF_TRACKING_WA) == 0) {
			mfh->mfh_flags |= MFHF_TRACKING_WA;
		}
	} else {
		if (mfh->mfh_flags & MFHF_TRACKING_WA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_WA;
			mfh->mfh_flags |= MFHF_TRACKING_RA;
			mfh->mfh_predio_lastoff = 0;
			mfh->mfh_predio_nseq = 0;
		} else if ((mfh->mfh_flags & MFHF_TRACKING_RA) == 0) {
			mfh->mfh_flags |= MFHF_TRACKING_RA;
		}
	}

	absoff = bno * SLASH_BMAP_SIZE + off;

	/*
	 * If the first read starts from offset 0, the following will
	 * trigger a read-ahead.  This is because as part of the
	 * msl_fhent structure, the fields are zeroed during allocation.
	 *
	 * Ensure this I/O is within a window from our expectation.
	 * This allows predictive I/O amidst slightly out of order
	 * (typically because of application threading) or skipped I/Os.
	 */
	if (labs(absoff - mfh->mfh_predio_lastoff) <
	    msl_predio_window_size) {
		if (mfh->mfh_predio_nseq) {
			mfh->mfh_predio_nseq = MIN(
			    mfh->mfh_predio_nseq * 2,
			    msl_predio_issue_maxpages);
		} else
			mfh->mfh_predio_nseq = npages;
	} else
		mfh->mfh_predio_nseq = 0;

	mfh->mfh_predio_lastoff = absoff;

	if (mfh->mfh_predio_nseq)
		OPSTAT_INCR("msl.predio-window-hit");
	else {
		OPSTAT_INCR("msl.predio-window-miss");
		PFL_GOTOERR(out, 0);
	}

	rapages = mfh->mfh_predio_nseq;

	/* Note: this can extend past current EOF. */
	newissued = absoff + rapages * BMPC_BUFSZ;
	if (newissued < mfh->mfh_predio_issued) {
		/*
		 * Our tracking is incoherent; we'll sync up with the
		 * application now.
		 */
	} else {
		/* Don't issue too soon after a previous issue. */
		rapages = (newissued - mfh->mfh_predio_issued) /
		    BMPC_BUFSZ;
		if (rapages < msl_predio_issue_minpages)
			PFL_GOTOERR(out, 0);
		bno = mfh->mfh_predio_issued / SLASH_BMAP_SIZE;
		off = mfh->mfh_predio_issued % SLASH_BMAP_SIZE;
	}

	f = mfh->mfh_fcmh;
	/* Now issue an I/O for each bmap in the prediction. */
	for (; rapages && bno < fcmh_2_nbmaps(f);
	    rapages -= tpages, off += tpages * BMPC_BUFSZ) {
		if (off >= SLASH_BMAP_SIZE) {
			off = 0;
			bno++;
		}
		bsize = SLASH_BMAP_SIZE;
		/* Trim a readahead that extends past EOF. */
		if (rw == SL_READ && bno == fcmh_2_nbmaps(f) - 1) {
			bsize = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;
			if (bsize == 0 && fcmh_2_fsz(f))
				bsize = SLASH_BMAP_SIZE;
		}
		tpages = howmany(bsize - off, BMPC_BUFSZ);
		if (!tpages)
			break;
		if (tpages > rapages)
			tpages = rapages;

		/*
		 * Write-ahead is only used for acquiring bmap leases
		 * predictively, not for preparing pages, so skip
		 * unnecessary work.
		 */
		if (rw == SL_WRITE && bno == orig_bno)
			continue;

		predio_enqueue(&f->fcmh_fg, bno, rw, off, tpages);
	}

	mfh->mfh_predio_issued = bno * SLASH_BMAP_SIZE + off;

 out:
	MFH_ULOCK(mfh);
}

__static struct msl_fsrqinfo *
msl_fsrqinfo_init(struct pscfs_req *pfr, struct msl_fhent *mfh,
    char *buf, size_t size, off_t off, enum rw rw)
{
	struct msl_fsrqinfo *q;

	q = psc_pool_get(msl_iorq_pool);
	memset(q, 0, sizeof(*q));
	INIT_PSC_LISTENTRY(&q->mfsrq_lentry);
	q->mfsrq_pfr = pfr;
	q->mfsrq_mfh = mfh;
	q->mfsrq_buf = buf;
	q->mfsrq_size = size;
	q->mfsrq_off = off;
	q->mfsrq_ref = 1;
	q->mfsrq_flags = (rw == SL_READ) ? MFSRQ_READ : MFSRQ_NONE;

	mfh_incref(q->mfsrq_mfh);

	if (rw == SL_READ)
		OPSTAT_INCR("msl.fsrq-read");
	else
		OPSTAT_INCR("msl.fsrq-write");

	DPRINTF_MFSRQ(PLL_DIAG, q, "created");
	return (q);
}

void
msl_update_attributes(struct msl_fsrqinfo *q)
{
	struct fcmh_cli_info *fci;
	struct msl_fhent *mfh;
	struct fidc_membh *f;
	struct timespec ts;

	mfh = q->mfsrq_mfh;
	f = mfh->mfh_fcmh;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&ts);
	f->fcmh_sstb.sst_mtime = ts.tv_sec;
	f->fcmh_sstb.sst_mtime_ns = ts.tv_nsec;
	f->fcmh_flags |= FCMH_CLI_DIRTY_MTIME;
	if (q->mfsrq_off + q->mfsrq_len > fcmh_2_fsz(f)) {
		psclog_diag("fid: "SLPRI_FID", "
		    "size from %"PRId64" to %"PRId64,
		    fcmh_2_fid(f), fcmh_2_fsz(f),
		    q->mfsrq_off + q->mfsrq_len);
		fcmh_2_fsz(f) = q->mfsrq_off + q->mfsrq_len;
		f->fcmh_flags |= FCMH_CLI_DIRTY_DSIZE;
	}
	if (!(f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE)) {
		fci = fcmh_2_fci(f);
		fci->fci_etime.tv_sec = ts.tv_sec + FCMH_ATTR_TIMEO;
		fci->fci_etime.tv_nsec = ts.tv_nsec;
		f->fcmh_flags |= FCMH_CLI_DIRTY_QUEUE;
		lc_addtail(&msl_attrtimeoutq, fci);
		fcmh_op_start_type(f, FCMH_OPCNT_DIRTY_QUEUE);
	}
	FCMH_ULOCK(f);
}

/*
 * I/O gateway routine which bridges pscfs and the SLASH2 client cache
 * and backend.  msl_io() handles the creation of biorq's and the
 * loading of bmaps (which are attached to the file's fcmh and is
 * ultimately responsible for data being prefetched (as needed), copied
 * into or from the cache, and (on write) being pushed to the correct
 * I/O server.
 *
 * The function implements the backend of mslfsop_read() and
 * mslfsop_write().
 *
 * @pfr: file system request, used for tracking potentially asynchronous
 *	activity.
 * @mfh: file handle structure passed to us by pscfs which contains the
 *	pointer to our fcmh.
 * @buf: the application destination buffer (only used for WRITEs).
 * @size: size of buffer.
 * @off: file logical offset similar to pwrite().
 * @rw: the operation type (SL_READ or SL_WRITE).
 */
ssize_t
msl_io(struct pscfs_req *pfr, struct msl_fhent *mfh, char *buf,
    size_t size, const off_t off, enum rw rw)
{
	int nr, i, j, rc, retry = 0, npages;
	size_t start, end, tlen, tsize;
	struct bmap_pagecache_entry *e;
	struct msl_fsrqinfo *q = NULL;
	struct timespec ts0, ts1, tsd;
	struct bmpc_ioreq *r = NULL;
	struct fidc_membh *f;
	struct bmap *b;
	sl_bmapno_t bno;
	uint32_t aoff;
	uint64_t fsz;
	off_t roff;

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_DIAG, f, "request: off=%"PRId64", size=%zu, "
	    "buf=%p, rw=%s", off, size, buf, (rw == SL_READ) ?
	    "read" : "write");

	FCMH_LOCK(f);
	/*
	 * All I/O's block here for pending truncate requests.
	 *
	 * XXX there is a race here.  We should set CLI_TRUNC ourselves
	 * until we are done setting up the I/O to block intervening
	 * truncates.
	 */
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_CLI_TRUNC);
	fsz = fcmh_getsize(f);

	if (rw == SL_READ) {
		/* Catch read ops which extend beyond EOF. */
		if (size + (uint64_t)off > fsz)
			size = fsz - off;
	}

	FCMH_ULOCK(f);

	if (size == 0) {
		if (rw == SL_READ)
			pscfs_reply_read(pfr, NULL, 0, 0);
		else
			pscfs_reply_write(pfr, 0, 0);
		return (0);
	}

	/*
	 * Get the start and end block regions from the input
	 * parameters.  We support at most 1MiB I/O that span at most
	 * one bmap boundary.
	 */
	start = off / SLASH_BMAP_SIZE;
	end = (off + size - 1) / SLASH_BMAP_SIZE;
	nr = end - start + 1;
	if (nr > MAX_BMAPS_REQ)
		return (-EINVAL);

	// XXX locked?  we should be using a counter on the mfsrq, not
	// the entire mfh
	mfh->mfh_retries = 0;

	/*
	 * Initialize some state in the request to help with aio
	 * handling.
	 */
	q = msl_fsrqinfo_init(pfr, mfh, buf, size, off, rw);
	if (rw == SL_READ && (!size || off >= (off_t)fsz))
		PFL_GOTOERR(out2, rc = 0);

	msl_update_iocounters(slc_iosyscall_iostats, rw, size);

 restart:
	rc = 0;
	tsize = size;

	/* Relativize the length and offset (roff is not aligned). */
	roff = off - (start * SLASH_BMAP_SIZE);
	psc_assert(roff < SLASH_BMAP_SIZE);

	/* Length of the first bmap request. */
	tlen = MIN(SLASH_BMAP_SIZE - (size_t)roff, tsize);

	/*
	 * Step 1: request bmaps and build biorqs.
	 *
	 * For each block range, get its bmap and make a request into
	 * its page cache.  This first loop retrieves all the pages.
	 */
	for (i = 0; i < nr; i++) {
		DEBUG_FCMH(PLL_DIAG, f, "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%s", tsize, tlen, off, roff,
		    (rw == SL_READ) ? "read" : "write");

		psc_assert(tsize);

		PFL_GETTIMESPEC(&ts0);

		rc = bmap_get(f, start + i, rw, &b);
		if (rc)
			PFL_GOTOERR(out2, rc);

		rc = msl_bmap_lease_tryext(b, 1);
		if (rc) {
			bmap_op_done(b);
			PFL_GOTOERR(out2, rc);
		}
		BMAP_LOCK(b);

		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("msl.getbmap-wait-usecs",
		    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);

		/*
		 * Re-relativize the offset if this request spans more
		 * than 1 bmap.
		 */
		r = q->mfsrq_biorq[i];
		if (r) {
			r->biorq_retries++;
			DYNARRAY_FOREACH(e, j, &r->biorq_pages) {
				BMPCE_LOCK(e);
				if (e->bmpce_flags & BMPCEF_EIO) {
					e->bmpce_rc = 0;
					e->bmpce_flags &= ~BMPCEF_EIO;
					DEBUG_BMPCE(PLL_DIAG, e,
					    "clear BMPCEF_EIO");
				}
				BMPCE_ULOCK(e);
			}
			bmap_op_done_type(r->biorq_bmap,
			    BMAP_OPCNT_BIORQ);
			r->biorq_bmap = b;
		} else {
			BMAP_ULOCK(b);

			/*
			 * roff - (i * SLASH_BMAP_SIZE) should be zero
			 * if i == 1.
			 */
			r = msl_biorq_build(q, b, buf,
			    roff - (i * SLASH_BMAP_SIZE), tlen,
			    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE);

			q->mfsrq_biorq[i] = r;

			MFH_LOCK(mfh);
			q->mfsrq_ref++;
			MFH_ULOCK(mfh);

			DPRINTF_MFSRQ(PLL_DIAG, q, "incref");
		}

		bmap_op_start_type(b, BMAP_OPCNT_BIORQ);
		bmap_op_done(b);

		/*
		 * No need to update roff and tsize for the last
		 * iteration.  Plus, we need them for predictive I/O
		 * work in the next step.
		 */
		if (i == nr - 1)
			break;

		roff += tlen;
		tsize -= tlen;
		if (buf)
			buf += tlen;
		tlen = MIN(SLASH_BMAP_SIZE, tsize);
	}

	/* Step 2: trigger read-ahead or write-ahead if necessary. */
	if (retry || !msl_predio_issue_maxpages ||
	    b->bcm_flags & BMAPF_DIO)
		goto out1;

	/* Note that i can only be 0 or 1 after the above loop. */
	if (i == 1) {
		psc_assert(roff == SLASH_BMAP_SIZE);
		roff = 0;
	}

	/* Calculate predictive I/O offset. */
	bno = b->bcm_bmapno;
	aoff = roff + tlen;
	if (aoff & BMPC_BUFMASK) {
		aoff += BMPC_BUFSZ;
		aoff &= ~BMPC_BUFMASK;
	}
	if (aoff >= SLASH_BMAP_SIZE) {
		aoff -= SLASH_BMAP_SIZE;
		bno++;
	}
	npages = howmany(size, BMPC_BUFSZ);

	msl_issue_predio(mfh, bno, rw, aoff, npages);

 out1:
	/* Step 3: launch biorqs (if necessary). */
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];

		if (r->biorq_flags & BIORQ_DIO)
			rc = msl_pages_dio_getput(r);
		else
			rc = msl_pages_fetch(r);
		if (rc)
			break;
	}

 out2:
	/* Step 4: retry if at least one biorq failed. */
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f,
		    "q=%p bno=%zd sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%s rc=%d",
		    q, start + i, tsize, tlen, off,
		    roff, (rw == SL_READ) ? "read" : "write", rc);

		if (msl_fd_should_retry(mfh, pfr, rc)) {
			mfsrq_clrerr(q);
			retry = 1;
			OPSTAT_INCR("msl.io-retry");
			goto restart;
		}
		if (abs(rc) == SLERR_ION_OFFLINE)
			rc = -ETIMEDOUT;

		/*
		 * Make sure we don't copy pages from biorq in case of
		 * an error.
		 */
		mfsrq_seterr(q, rc);
	}

	/* Step 5: finish up biorqs. */
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];
		if (r)
			msl_biorq_release(r);
	}

	/* Step 6: drop our reference to the fsrq. */
	msl_complete_fsrq(q, 0, NULL);
	return (0);
}

void
msreadaheadthr_main(struct psc_thread *thr)
{
	struct bmap_pagecache_entry *pg;
	struct readaheadrq *rarq;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	struct bmap *b;
	int i, rc, npages;

	while (pscthr_run(thr)) {
		rarq = lc_getwait(&msl_readaheadq);
		if (rarq == NULL)
			break;
		b = NULL;
		f = NULL;

		npages = rarq->rarq_npages;
		if (rarq->rarq_off + npages * BMPC_BUFSZ >
		    SLASH_BMAP_SIZE)
			npages = (SLASH_BMAP_SIZE - rarq->rarq_off) /
			    BMPC_BUFSZ;
		psc_assert(npages);

		rc = sl_fcmh_peek_fg(&rarq->rarq_fg, &f);
		if (rc)
			goto end;
		rc = bmap_getf(f, rarq->rarq_bno, rarq->rarq_rw,
		    BMAPGETF_CREATE | BMAPGETF_NONBLOCK |
		    BMAPGETF_NODIO, &b);
		if (rc)
			goto end;
		if (b->bcm_flags & BMAPF_DIO)
			goto end;
		if ((b->bcm_flags & (BMAPF_LOADING | BMAPF_LOADED)) !=
		    BMAPF_LOADED) {
			bmap_op_done(b);
			fcmh_op_done(f);
			lc_add(&msl_readaheadq, rarq);
			continue;
		}
		BMAP_ULOCK(b);

		/*
		 * Writeahead only needs to do a bmap fetch.  We could
		 * make some bmpces hot but the majority of the latency
		 * comes from communicating with the MDS.
		 */
		if (rarq->rarq_rw == SL_WRITE)
			goto end;

		r = bmpc_biorq_new(NULL, b, NULL, rarq->rarq_off,
		    npages * BMPC_BUFSZ, BIORQ_READ | BIORQ_READAHEAD);
		bmap_op_start_type(b, BMAP_OPCNT_BIORQ);

		for (i = 0; i < npages; i++) {
			rc = bmpce_lookup(r, b, BMPCEF_READAHEAD,
			    rarq->rarq_off + i * BMPC_BUFSZ,
			    &f->fcmh_waitq, &pg);
			if (rc == EALREADY)
				continue;
			if (rc)
				break;
		}
		if (psc_dynarray_len(&r->biorq_pages))
			msl_launch_read_rpcs(r);
		msl_biorq_release(r);

 end:
		if (b)
			bmap_op_done(b);
		if (f)
			fcmh_op_done(f);
		psc_pool_return(slc_readaheadrq_pool, rarq);
	}
}

void
msreadaheadthr_spawn(void)
{
	struct msreadahead_thread *mrat;
	struct psc_thread *thr;
	int i;

	psc_poolmaster_init(&slc_readaheadrq_poolmaster,
	    struct readaheadrq, rarq_lentry, PPMF_AUTO, 4096, 4096,
	    4096, NULL, NULL, NULL, "readaheadrq");
	slc_readaheadrq_pool = psc_poolmaster_getmgr(
	    &slc_readaheadrq_poolmaster);

	lc_reginit(&msl_readaheadq, struct readaheadrq, rarq_lentry,
	    "readaheadq");

	for (i = 0; i < NUM_READAHEAD_THREADS; i++) {
		thr = pscthr_init(MSTHRT_READAHEAD, msreadaheadthr_main,
		    NULL, sizeof(*mrat), "msreadaheadthr%d", i);
		mrat = msreadaheadthr(thr);
		pfl_multiwait_init(&mrat->mrat_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}

void
msl_readahead_svc_destroy(void)
{
	pfl_poolmaster_destroy(&slc_readaheadrq_poolmaster);
}
