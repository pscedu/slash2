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
struct psc_waitq	 msl_fhent_aio_waitq = PSC_WAITQ_INIT("aio");

struct timespec		 msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec		 msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

int                      msl_predio_pipe_size = 256;
int                      msl_predio_max_pages = 64;

struct pfl_opstats_grad	 slc_iosyscall_iostats_rd;
struct pfl_opstats_grad	 slc_iosyscall_iostats_wr;
struct pfl_opstats_grad	 slc_iorpc_iostats_rd;
struct pfl_opstats_grad	 slc_iorpc_iostats_wr;

struct psc_poolmaster	 slc_readaheadrq_poolmaster;
struct psc_poolmgr	*slc_readaheadrq_pool;
struct psc_listcache	 msl_readaheadq;

int msl_read_cb(struct pscrpc_request *, struct pscrpc_async_args *);

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
		    &b->bcm_fcmh->fcmh_waitq);
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

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		bmpce_release_locked(e, bmpc);
	}
	psc_dynarray_free(&r->biorq_pages);

	BMAP_LOCK(b);

	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_ONTREE)
		PSC_RB_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_biorqs, r);

	if (r->biorq_flags & BIORQ_FLUSHRDY) {
		pll_remove(&bmpc->bmpc_biorqs_exp, r);
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

void
msl_biorq_destroy(struct bmpc_ioreq *r)
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
msl_biorq_release(struct bmpc_ioreq *r)
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
	mfh->mfh_accessing_uid = slc_getfscreds(pfr, &pcr, 0)->pcr_uid;
	mfh->mfh_accessing_gid = pcr.pcr_gid;
	mfh->mfh_accessing_euid = slc_getfscreds(pfr, &pcr, 1)->pcr_uid;
	mfh->mfh_accessing_egid = pcr.pcr_gid;
	INIT_SPINLOCK(&mfh->mfh_lock);
	INIT_PSC_LISTENTRY(&mfh->mfh_lentry);

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
    struct sl_resm **pm, struct slrpc_cservice **csvcp)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *m;

	if (require_valid && SL_REPL_GET_BMAP_IOS_STAT(bci->bci_repls,
	    iosidx * SL_BITS_PER_REPLICA) != BREPLST_VALID)
		return (-3);

	fci = fcmh_2_fci(b->bcm_fcmh);
	res = libsl_id2res(fci->fci_inode.reptbl[iosidx].bs_id);
	if (res == NULL) {
		/*
		 * This can happen because we don't remove IOS
		 * from a file's replication table even if all
		 * its bmaps are off the corresponding IOS.
		 */
		DEBUG_FCMH(PLL_WARN, b->bcm_fcmh,
		    "unknown or obsolete IOS in reptbl: %#x",
		    fci->fci_inode.reptbl[iosidx].bs_id);
		return (-2);
	}

	/* XXX not a real shuffle */
	FOREACH_RND(&it, psc_dynarray_len(&res->res_members)) {
		m = psc_dynarray_getpos(&res->res_members,
		    it.ri_rnd_idx);
		*csvcp = slc_geticsvc_nb(m, 0);
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
		r->biorq_ref++;
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
		e->bmpce_flags = newval;
		BMPCE_ULOCK(e);
	}
}

/*
 * Send a reply back to the userland file system interface for a READ
 * I/O operation.
 */
void
slc_fsreply_read(struct fidc_membh *f, struct pscfs_req *pfr,
    struct iovec *iov, int nio, int rc)
{
	size_t len;
	int i;

	for (len = 0, i = 0; i < nio; i++)
		len += iov[i].iov_len;

	/*
	 * Errors encountered during reads are considered rare so
	 * propagate them at a higher log level.
	 */
	DEBUG_FCMH(rc ? PLL_WARN : PLL_DIAG, f,
	    "reply read: pfr=%p size=%zu rc=%d", pfr, len, rc);

	pscfs_reply_read(pfr, iov, nio, rc);
}

/*
 * Send a reply back to the userland file system interface for a WRITE
 * I/O operation.
 */
void
slc_fsreply_write(struct fidc_membh *f, struct pscfs_req *pfr,
    size_t len, int rc)
{
	/*
	 * Errors encountered during writes are considered rare so
	 * propagate them at a higher log level.
	 */
	DEBUG_FCMH(rc ? PLL_WARN : PLL_DIAG, f,
	    "reply write: pfr=%p size=%zu rc=%d", pfr, len, rc);

	pscfs_reply_write(pfr, len, rc);
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
	f = mfh->mfh_fcmh;

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
		struct iovec *piov = NULL, iov[MAX_BMAPS_REQ];
		int nio = 0, rc = 0;

		if (q->mfsrq_err) {
			rc = abs(q->mfsrq_err);
		} else {
			if (q->mfsrq_len == 0) {
			} else if (q->mfsrq_iovs) {
				psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

				for (i = 0; i < MAX_BMAPS_REQ; i++) {
					r = q->mfsrq_biorq[i];
					if (!r)
						break;
				}

				piov = q->mfsrq_iovs;
				nio = q->mfsrq_niov;
			} else {
				psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

				for (i = 0; i < MAX_BMAPS_REQ; i++,
				    nio++) {
					r = q->mfsrq_biorq[i];
					if (!r)
						break;
					iov[nio].iov_base = r->biorq_buf;
					iov[nio].iov_len = r->biorq_len;
				}
				piov = iov;
			}
		}
		slc_fsreply_read(f, pfr, piov, nio, rc);
	} else {
		if (!q->mfsrq_err) {
			msl_update_attributes(q);
			psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

			for (i = 0; i < MAX_BMAPS_REQ; i++) {
				r = q->mfsrq_biorq[i];
				if (!r)
					break;
			}
		}
		slc_fsreply_write(f, pfr, q->mfsrq_len,
		    abs(q->mfsrq_err));
	}

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
			/*
 			 * XXX This if statement looks suspicous to me.
 			 * If we ever want to support AIO again, we need
 			 * to revisit this.
 			 */
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
	psc_assert(e->bmpce_flags & BMPCEF_FAULTING);
	e->bmpce_flags &= ~(BMPCEF_AIOWAIT | BMPCEF_FAULTING);

	if (rc) {
		e->bmpce_rc = rc;
		e->bmpce_len = 0;
		e->bmpce_flags |= BMPCEF_EIO;
		psc_assert(!(e->bmpce_flags & BMPCEF_DATARDY));
	} else {
		e->bmpce_flags &= ~BMPCEF_EIO;
		e->bmpce_flags |= BMPCEF_DATARDY;
	}

	DEBUG_BMPCE(PLL_DEBUG, e, "rpc_done");

	BMPCE_WAKE(e);
	BMPCE_ULOCK(e);
	msl_bmpce_complete_biorq(e, rc);
}

int
msl_read_attempt_retry(struct msl_fsrqinfo *fsrqi, int rc0,
    struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = NULL;
	struct psc_dynarray *a = args->pointer_arg[MSL_CBARG_BMPCE];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct iovec *iovs = args->pointer_arg[MSL_CBARG_IOVS];
	struct sl_resm *m = args->pointer_arg[MSL_CBARG_RESM];
	struct pscrpc_request *rq = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct bmap_pagecache_entry *e;
	int npages, rc = 0;
	uint32_t off;
	struct pscfs_req *pfr;

	pfr = mfsrq_2_pfr(fsrqi);

 restart:

	if (!slc_rpc_should_retry(pfr, &rc0))
		return (0);

	csvc = slc_geticsvc(m, 0);
	if (!csvc) {
		rc0 = m->resm_csvc->csvc_lasterrno;
		goto restart;
	}

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		 PFL_GOTOERR(out, rc);

	e = psc_dynarray_getpos(a, 0);
	npages = psc_dynarray_len(a);
	off = e->bmpce_off;

	rq->rq_bulk_abortable = 1;
	rc = slrpc_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
		npages);
	if (rc)
		PFL_GOTOERR(out, rc);

	mq->offset = off;
	mq->size = npages * BMPC_BUFSZ;
	psc_assert(mq->offset + mq->size <= SLASH_BMAP_SIZE);

	mq->op = SRMIOP_RD;
	mq->sbd = *bmap_2_sbd(r->biorq_bmap);
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = a;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
	rq->rq_async_args.pointer_arg[MSL_CBARG_RESM] = m;
	rq->rq_async_args.pointer_arg[MSL_CBARG_IOVS] = iovs;
	rq->rq_interpret_reply = msl_read_cb;

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc)
		 PFL_GOTOERR(out, rc);

	OPSTAT_INCR("msl.read-retried");
	return (1);

 out:
	pscrpc_req_finished(rq);
	sl_csvc_decref(csvc);
	return (0);
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
	struct slrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct psc_dynarray *a = args->pointer_arg[MSL_CBARG_BMPCE];
	struct bmpc_ioreq *r = args->pointer_arg[MSL_CBARG_BIORQ];
	struct iovec *iovs = args->pointer_arg[MSL_CBARG_IOVS];
	struct bmap_pagecache_entry *e;
	struct srm_io_req *mq;
	struct bmap *b;
	int i;
	char buf[PSCRPC_NIDSTR_SIZE];

	b = r->biorq_bmap;

	psc_assert(a);
	psc_assert(b);

	if (rq)
		DEBUG_REQ(rc ? PLL_ERROR : PLL_DIAG, rq, buf,
		    "bmap=%p biorq=%p", b, r);

	pfl_fault_here_rc(&rc, EIO, "slash2/read_cb");

	DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b, "rc=%d "
	    "sbd_seq=%"PRId64, rc, bmap_2_sbd(b)->sbd_seq);
	DEBUG_BIORQ(rc ? PLL_ERROR : PLL_DIAG, r, "rc=%d", rc);

if (!pfl_rpc_max_retry) {

	if (rc && r->biorq_fsrqi) {
		sl_csvc_decref(csvc);
		csvc = NULL;
		if (msl_read_attempt_retry(r->biorq_fsrqi, rc, args))
			return (0);
	}

}

	DYNARRAY_FOREACH(e, i, a)
		msl_bmpce_read_rpc_done(e, rc);

	if (rc) {
		if (rc == -PFLERR_KEYEXPIRED) {
			BMAP_LOCK(b);
			b->bcm_flags |= BMAPF_LEASEEXPIRE;
			BMAP_ULOCK(b);
			OPSTAT_INCR("msl.bmap-read-expired");
		}
		if ((r->biorq_flags & BIORQ_READAHEAD) == 0)
			mfsrq_seterr(r->biorq_fsrqi, rc);
	} else {
		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		pfl_opstats_grad_incr(&slc_iorpc_iostats_rd, mq->size);
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

	PSCFREE(iovs);

	if (csvc)
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
	struct slrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
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
	char buf[PSCRPC_NIDSTR_SIZE];
	int op;

	/* rq is NULL it we are called from sl_resm_hldrop() */
	if (rq) {
		DEBUG_REQ(PLL_DIAG, rq, buf, "cb");

		op = rq->rq_reqmsg->opc;
		psc_assert(op == SRMT_READ || op == SRMT_WRITE);

		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		psc_assert(mq);

		DEBUG_BIORQ(PLL_DIAG, r,
		    "dio complete (op=%d) off=%u sz=%u rc=%d",
		    op, mq->offset, mq->size, rc);

		pfl_opstats_grad_incr(mq->op == SRMIOP_WR ?
		    &slc_iorpc_iostats_wr : &slc_iorpc_iostats_rd,
		    mq->size);
	}


	q = r->biorq_fsrqi;
	if (r->biorq_flags & BIORQ_AIOWAKE) {
		MFH_LOCK(q->mfsrq_mfh);
		psc_waitq_wakeall(&msl_fhent_aio_waitq);
		MFH_ULOCK(q->mfsrq_mfh);
	}

	DEBUG_BIORQ(PLL_DIAG, r, "aiowait wakeup");

#if 0
	msl_update_iocounters(slc_iorpc_iostats, rw, bwc->bwc_size);
#endif

	return (rc);
}

int
msl_dio_cb(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
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
	struct slrpc_cservice *csvc;
	struct pscrpc_request_set *nbs;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *bci;
	struct msl_fsrqinfo *q;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct pscfs_req *pfr;
	struct iovec *iovs;
	struct sl_resm *m;
	struct bmap *b;
	uint64_t *v8;
	int refs;

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

  retry:
	refs = 0;
	nbs = NULL;
	csvc = NULL;
	/*
	 * XXX for read lease, we could inspect throttle limits of other
	 * residencies and use them if available.
	 */
	rc = msl_bmap_to_csvc(b, op == SRMT_WRITE, &m, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	nbs = pscrpc_prep_set();

	/*
	 * The buffer associated with the request hasn't been segmented
	 * into LNET_MTU-sized chunks.  Do it now.
	 */
	for (i = 0, off = 0; i < n; i++, off += len) {
		len = MIN(LNET_MTU, size - off);

		if (op == SRMT_WRITE)
			rc = SL_RSX_NEWREQ(csvc, SRMT_WRITE, rq, mq, mp);
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
		if (rc) {
			pscrpc_req_finished(rq);
			rq = NULL;
			PFL_GOTOERR(out, rc);
		}

		mq->offset = r->biorq_off + off;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		mq->flags |= SRM_IOF_DIO;

		memcpy(&mq->sbd, &bci->bci_sbd, sizeof(mq->sbd));

		refs++;
		biorq_incref(r);

		rc = SL_NBRQSETX_ADD(nbs, csvc, rq);
		if (rc) {
			refs--;
			msl_biorq_release(r);
			OPSTAT_INCR("msl.dio-add-req-fail");
			pscrpc_req_finished(rq);
			rq = NULL;
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
	q = r->biorq_fsrqi;
	psc_assert(q);
	pfr = mfsrq_2_pfr(q);

	pfl_fault_here_rc(&rc, EIO, "slash2/dio_wait");

	if (rc == -SLERR_AIOWAIT) {
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
		OPSTAT_INCR("msl.biorq-restart");

		/*
		 * Async I/O registered by sliod; we must wait for a
		 * notification from him when it is ready.
		 */
		for (i = 0; i < refs; i++)
			msl_biorq_release(r);
		pscrpc_set_destroy(nbs);
		sl_csvc_decref(csvc);
		goto retry;
	}

	if (rc == 0) {
		if (op == SRMT_WRITE)
			OPSTAT2_ADD("msl.dio-rpc-wr", size);
		else
			OPSTAT2_ADD("msl.dio-rpc-rd", size);

		MFH_LOCK(q->mfsrq_mfh);
		q->mfsrq_flags |= MFSRQ_COPIED;
		MFH_ULOCK(q->mfsrq_mfh);
	}
 out:
	/* Drop the reference we took for the RPCs. */
	for (i = 0; i < refs; i++)
		msl_biorq_release(r);

if (!pfl_rpc_max_retry) {

	if (rc && slc_rpc_should_retry(pfr, &rc)) {
		OPSTAT_INCR("msl.dio-retried");
		if (nbs)
			pscrpc_set_destroy(nbs);
		if (csvc)
			sl_csvc_decref(csvc);
		goto retry;
	}
}

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
	PSC_RB_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_biorqs, r);
	pll_addtail(&bmpc->bmpc_biorqs_exp, r);
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
	struct slrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_pagecache_entry *e;
	struct psc_dynarray *a = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	struct sl_resm *m;
	uint32_t off = 0;
	int rc = 0, i;
	char buf[PSCRPC_NIDSTR_SIZE];

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);

		BMPCE_LOCK(e);
		/*
		 * A read must wait until all pending writes are
		 * flushed.  So we should never see a pinned down
		 * page here.
		 */
		psc_assert(!e->bmpce_pins);

		psc_assert(e->bmpce_flags & BMPCEF_FAULTING);
		psc_assert(!(e->bmpce_flags & BMPCEF_DATARDY));
		DEBUG_BMPCE(PLL_DIAG, e, "page = %d", i + startpage);
		BMPCE_ULOCK(e);

		iovs[i].iov_base = e->bmpce_entry->page_buf;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			off = e->bmpce_off;
		psc_dynarray_add(a, e);
	}

	pfl_fault_here_rc(&rc, -ETIMEDOUT, "slash2/readrpc_offline");
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
	/*
	 * XXX what about the start of the first page and the end of the
	 * last page???
	 */
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
	rq->rq_async_args.pointer_arg[MSL_CBARG_IOVS] = iovs;
	rq->rq_interpret_reply = msl_read_cb;

	biorq_incref(r);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		msl_biorq_release(r);

		OPSTAT_INCR("msl.read-add-req-fail");
		PFL_GOTOERR(out, rc);
	}

	return (0);

 out:

	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, buf, "req failed rc=%d", rc);
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

		/*
		 * The faulting flag could be set by a concurrent writer
		 * that touches a different area in the page, so don't
		 * assume that data is ready when it is cleared.
		 */
		BMPCE_LOCK(e);
		while (e->bmpce_flags & BMPCEF_FAULTING) {
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
		}

		/*
		 * XXX If I don't make a valid page as FAULTING, a 
		 * write could sneak it...
		 */
		if (msl_biorq_page_valid(r, i)) {
			BMPCE_ULOCK(e);
			if (r->biorq_flags & BIORQ_READAHEAD)
				OPSTAT_INCR("msl.readahead-gratuitous");
			continue;
		}

		/*
		 * We are going to re-read the page, so clear its previous
		 * errors.  We could do the same thing for write, but we
		 * must test it with fault injection instead of just putting
		 * the code in and hope it work.
		 */
		if (e->bmpce_flags & BMPCEF_EIO) {
			OPSTAT_INCR("msl.read_clear_rc");
			e->bmpce_rc = 0;
			e->bmpce_len = 0;
			e->bmpce_flags &= ~BMPCEF_EIO;
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
		/* XXX make this interruptible */
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

	/* read-before-write will kill performance */
	if (r->biorq_flags & BIORQ_READ) {
		perfect_ra = 1;
		rc = msl_launch_read_rpcs(r);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	PFL_GETTIMESPEC(&ts0);

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {

		BMPCE_LOCK(e);
		/*
 		 * Waiting for I/O to complete, which could be
 		 * initiated by the read-ahead, by myself, or
 		 * by other thread.
 		 */
		while (e->bmpce_flags & BMPCEF_FAULTING) {
			DEBUG_BMPCE(PLL_DIAG, e, "waiting");
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
			perfect_ra = 0;
		}
		/*
		 * We can't read/write page in this state.
		 */
		if (e->bmpce_flags & BMPCEF_AIOWAIT) {
			msl_fsrq_aiowait_tryadd_locked(e, r);
			aiowait = 1;
			BMPCE_ULOCK(e);
			OPSTAT_INCR("msl.aio-placed");
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

		/*
 		 * Read error should be catched in the callback.
 		 */
		if (r->biorq_flags & BIORQ_READ) {
			BMPCE_ULOCK(e);
			continue;
		}

		/*
		 * Since this is a write, we clear the page to 
		 * its pristine state because a previous failure 
		 * should not cause the current write to fail.
		 */
		if (e->bmpce_flags & BMPCEF_EIO) {
			OPSTAT_INCR("msl.write_clear_rc");
			e->bmpce_rc = 0;
			e->bmpce_len = 0;
			e->bmpce_flags &= ~BMPCEF_EIO;
		}
		/*
		 * Avoid a race with readahead thread.
		 */
		e->bmpce_flags |= BMPCEF_FAULTING;
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
		if (e->bmpce_pins) {
			OPSTAT_INCR("msl.bmpce-copyin-wait");
			do {
				BMPCE_WAIT(e);
				BMPCE_LOCK(e);
			} while (e->bmpce_pins);
		}

		/*
		 * Re-check RBW sanity.  The waitq pointer within the
		 * bmpce must still be valid in order for this check to
		 * work.
		 *
		 * Set the starting buffer pointer into our cache
		 * vector.
		 */
		dest = e->bmpce_entry->page_buf;
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
		src = e->bmpce_entry->page_buf;
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

		bmpce_usecheck(e, BIORQ_READ, biorq_getaligned_off(r, i));

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

void
mfh_track_predictive_io(struct msl_fhent *mfh, size_t size, off_t off,
    enum rw rw)
{
	int delta = BMPC_BUFSZ;

	MFH_LOCK(mfh);

	if (rw == SL_WRITE) {
		if (mfh->mfh_flags & MFHF_TRACKING_RA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_RA;
			mfh->mfh_flags |= MFHF_TRACKING_WA;
			mfh->mfh_predio_off = 0;
			mfh->mfh_predio_nseq = 0;
			mfh->mfh_predio_lastoff = 0;
			mfh->mfh_predio_lastsize = 0;
		}
	} else {
		if (mfh->mfh_flags & MFHF_TRACKING_WA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_WA;
			mfh->mfh_flags |= MFHF_TRACKING_RA;
			mfh->mfh_predio_off = 0;
			mfh->mfh_predio_nseq = 0;
			mfh->mfh_predio_lastoff = 0;
			mfh->mfh_predio_lastsize = 0;
		}
	}

	/*
	 * If the first read starts from offset 0, the following will
	 * automatically trigger a read-ahead because as part of the
	 * msl_fhent structure, the fields are zeroed during allocation.
	 */
	if (off == mfh->mfh_predio_lastoff + mfh->mfh_predio_lastsize) {
		OPSTAT_INCR("msl.predio-sequential");
		mfh->mfh_predio_nseq++;
		goto out;
	}
	if (off > mfh->mfh_predio_lastoff + mfh->mfh_predio_lastsize + delta ||
	    off < mfh->mfh_predio_lastoff + mfh->mfh_predio_lastsize - delta) {
		OPSTAT_INCR("msl.predio-reset");
		mfh->mfh_predio_off = 0;
		mfh->mfh_predio_nseq = 0;
		goto out;
	}
	OPSTAT_INCR("msl.predio-semi-sequential");

 out:
	mfh->mfh_predio_lastoff = off;
	mfh->mfh_predio_lastsize = size;

	MFH_ULOCK(mfh);
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
	int bsize, tpages, rapages;
	struct fidc_membh *f;
	off_t raoff;

	f = mfh->mfh_fcmh;
	MFH_LOCK(mfh);

	if (!mfh->mfh_predio_nseq)
		PFL_GOTOERR(out, 0);

	if (mfh->mfh_flags & MFHF_TRACKING_WA) {
		if (BMPC_BUFSZ * msl_predio_pipe_size >= SLASH_BMAP_SIZE ||
		    off + npages * BMPC_BUFSZ >= 
		    SLASH_BMAP_SIZE - BMPC_BUFSZ * msl_predio_pipe_size) {
			OPSTAT_INCR("msl.predio-write-enqueue");
			predio_enqueue(&f->fcmh_fg, bno+1, rw, 0, 0);
		}
		PFL_GOTOERR(out, 0);
	}

	raoff = bno * SLASH_BMAP_SIZE + off + npages * BMPC_BUFSZ;
	if (raoff + msl_predio_pipe_size * BMPC_BUFSZ < mfh->mfh_predio_off) {
		OPSTAT_INCR("msl.predio-pipe-hit");
		PFL_GOTOERR(out, 0);
	}
	OPSTAT_INCR("msl.predio-pipe-miss");

	/* Adjust raoff based on our position in the pipe */
	if (mfh->mfh_predio_off) {
		if (mfh->mfh_predio_off > raoff) {
			OPSTAT_INCR("msl.predio-pipe-enlarge");
			raoff = mfh->mfh_predio_off;
		} else
			OPSTAT_INCR("msl.predio-pipe-overrun");
	}

	/* convert to bmap relative */
	bno = raoff / SLASH_BMAP_SIZE;
	raoff = raoff - bno * SLASH_BMAP_SIZE;
	rapages = MIN(MAX(mfh->mfh_predio_nseq*2, npages), msl_predio_max_pages);

#ifdef MYDEBUG
	psclog_max("readahead: FID = "SLPRI_FID", bno = %d, offset = %ld, size = %d", 
	    fcmh_2_fid(f), bno, raoff, rapages);
#endif

	/* 
	 * Now issue an I/O for each bmap in the prediction. This loop
	 * can handle read-ahead into multiple bmaps.
	 */
	for (; rapages && bno < fcmh_2_nbmaps(f); rapages -= tpages) {

		bsize = SLASH_BMAP_SIZE;
		if (bno == fcmh_2_nbmaps(f) - 1) {
			bsize = fcmh_2_fsz(f) % SLASH_BMAP_SIZE;
			if (bsize == 0 && fcmh_2_fsz(f))
				bsize = SLASH_BMAP_SIZE;
		}
		tpages = howmany(bsize - raoff, BMPC_BUFSZ);
		if (!tpages)
			break;
#if 0
		if (tpages > BMPC_MAXBUFSRPC)
			tpages = BMPC_MAXBUFSRPC;
#endif
		if (tpages > rapages)
			tpages = rapages;

		predio_enqueue(&f->fcmh_fg, bno, rw, raoff, tpages);

		raoff += tpages * BMPC_BUFSZ;
		if (raoff >= SLASH_BMAP_SIZE) {
			raoff = 0;
			bno++;
		}
	}

	mfh->mfh_predio_off = bno * SLASH_BMAP_SIZE + raoff;

 out:
	MFH_ULOCK(mfh);
}

__static struct msl_fsrqinfo *
msl_fsrqinfo_init(struct pscfs_req *pfr, struct msl_fhent *mfh,
    char *buf, size_t size, off_t off, enum rw rw)
{
	struct msl_fsrqinfo *q;

	/* pool is inited in msl_init() */
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
		fci->fci_etime.tv_sec = ts.tv_sec + msl_attributes_timeout;
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
 * @off: file logical offset similar to pread() and pwrite().
 * @rw: the operation type (SL_READ or SL_WRITE).
 */
void
msl_io(struct pscfs_req *pfr, struct msl_fhent *mfh, char *buf,
    size_t size, const off_t off, enum rw rw)
{
	int nr, i, rc, npages;
	size_t start, end, tlen, tsize;
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

	/* XXX EBADF if fd is not open for writing */

	if (fcmh_isdir(f)) {
		if (rw == SL_READ)
			OPSTAT_INCR("msl.fsrq-read-isdir");
		else
			OPSTAT_INCR("msl.fsrq-write-isdir");
		PFL_GOTOERR(out3, rc = EISDIR);
	}

	FCMH_LOCK(f);
	/*
	 * All I/O's block here for pending truncate requests.
	 *
	 * XXX there is a race here.  We should set CLI_TRUNC ourselves
	 * until we are done setting up the I/O to block intervening
	 * truncates.
	 */
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_CLI_TRUNC);
	fsz = fcmh_2_fsz(f);

	if (rw == SL_READ) {
		/* Catch read ops which extend beyond EOF. */
		if (size + (uint64_t)off > fsz)
			size = fsz - off;
	}
	mfh_track_predictive_io(mfh, size, off, rw);

	FCMH_ULOCK(f);

	if (size == 0)
		PFL_GOTOERR(out3, rc = 0);

	/*
	 * Get the start and end block regions from the input
	 * parameters.  We support at most 1MiB I/O that span
	 * at most one bmap boundary.
	 */
	start = off / SLASH_BMAP_SIZE;
	end = (off + size - 1) / SLASH_BMAP_SIZE;
	nr = end - start + 1;
	if (nr > MAX_BMAPS_REQ)
		PFL_GOTOERR(out3, rc = EINVAL);

	/*
	 * Initialize some state in the request to help with aio
	 * handling.
	 */
	q = msl_fsrqinfo_init(pfr, mfh, buf, size, off, rw);
	if (rw == SL_READ && off >= (off_t)fsz)
		PFL_GOTOERR(out2, rc = 0);

	pfl_opstats_grad_incr(rw == SL_READ ?
	    &slc_iosyscall_iostats_rd : &slc_iosyscall_iostats_wr,
	    size);

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
		DEBUG_FCMH(PLL_DIAG, f, "nr=%d sz=%zu tlen=%zu "
		    "off=%"PSCPRIdOFFT" roff=%"PSCPRIdOFFT" rw=%s", i,
		    tsize, tlen, off, roff,
		    rw == SL_READ ? "read" : "write");

		psc_assert(tsize);

		PFL_GETTIMESPEC(&ts0);

		rc = bmap_get(f, start + i, rw, &b);
		/* XXX got EHOSTDOWN (112) */
		if (rc)
			PFL_GOTOERR(out2, rc);

		rc = msl_bmap_lease_extend(b, 1);
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

		bmap_op_start_type(b, BMAP_OPCNT_BIORQ);
		bmap_op_done(b);

		/*
		 * No need to update roff and tsize for the last
		 * iteration.  Plus, we need them for predictive
		 * I/O work in the next step.
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
	if (!msl_predio_max_pages || b->bcm_flags & BMAPF_DIO)
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
	/*
	 * Step 4: finish up biorqs.  Copy to satisfy READ back to user
	 * occurs in this step.
	 */
	mfsrq_seterr(q, rc);
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];
		if (r)
			msl_biorq_release(r);
	}

	/*
	 * Step 5: drop our reference to the fsrq.  The last drop will
	 * reply to the userland file system interface. So we may or 
	 * may not finish the entire I/O here.
	 */
	msl_complete_fsrq(q, 0, NULL);
	return;

 out3:
	if (rw == SL_READ)
		slc_fsreply_read(f, pfr, NULL, 0, rc);
	else
		slc_fsreply_write(f, pfr, 0, rc);
}

void
msreadaheadthr_main(struct psc_thread *thr)
{
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
			/*
 			 * XXX spin when this is the last item on the list.
 			 */
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
			    &f->fcmh_waitq);
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
	    4096, NULL, "readaheadrq");
	slc_readaheadrq_pool = psc_poolmaster_getmgr(
	    &slc_readaheadrq_poolmaster);

	lc_reginit(&msl_readaheadq, struct readaheadrq, rarq_lentry,
	    "readaheadq");

	for (i = 0; i < NUM_READAHEAD_THREADS; i++) {
		thr = pscthr_init(MSTHRT_READAHEAD, msreadaheadthr_main,
		    sizeof(*mrat), "msreadaheadthr%d", i);
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
