/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2014, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/iostats.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
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

__static int	msl_biorq_complete_fsrq(struct bmpc_ioreq *);
__static size_t	msl_pages_copyin(struct bmpc_ioreq *);
__static void	msl_pages_schedflush(struct bmpc_ioreq *);

__static void	mfsrq_seterr(struct msl_fsrqinfo *, int);

__static void	msl_update_attributes(struct msl_fsrqinfo *);
__static int	msl_getra(struct msl_fhent *, int, uint32_t, int,
		    uint32_t *, int *, uint32_t *, int *);

/* Flushing fs threads wait here for I/O completion. */
struct psc_waitq	msl_fhent_aio_waitq = PSC_WAITQ_INIT;

struct timespec		msl_bmap_max_lease = { BMAP_CLI_MAX_LEASE, 0 };
struct timespec		msl_bmap_timeo_inc = { BMAP_CLI_TIMEO_INC, 0 };

psc_atomic32_t		 slc_max_readahead = PSC_ATOMIC32_INIT(MS_READAHEAD_MAXPGS);

struct pfl_iostats_rw	 slc_dio_ist;
struct pfl_iostats	 slc_rdcache_ist;
struct pfl_iostats	 slc_readahead_hit_ist;
struct pfl_iostats	 slc_readahead_issue_ist;

struct pfl_iostats_grad	 slc_iosyscall_ist[8];
struct pfl_iostats_grad	 slc_iorpc_ist[8];

struct psc_poolmaster	 slc_readaheadrq_poolmaster;
struct psc_poolmgr	*slc_readaheadrq_pool;
struct psc_listcache	 slc_readaheadq;

void
msl_update_iocounters(struct pfl_iostats_grad *ist, enum rw rw, int len)
{
	for (; ist->size && len >= ist->size; ist++)
		;
	if (rw == SL_READ)
		psc_iostats_intv_add(&ist->rw.rd, 1);
	else
		psc_iostats_intv_add(&ist->rw.wr, 1);
}

__static int
msl_biorq_page_valid(struct bmpc_ioreq *r, int idx, int checkonly)
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

		if (e->bmpce_flags & BMPCE_DATARDY) {
			if (!checkonly)
				psc_iostats_intv_add(&slc_rdcache_ist,
				    nbytes);
			return (1);
		}

		if (toff >= e->bmpce_start &&
		    toff + nbytes <= e->bmpce_start + e->bmpce_len) {
			if (!checkonly) {
				psc_iostats_intv_add(&slc_rdcache_ist,
				    nbytes);
				OPSTAT_INCR("read_part_valid");
			}
			return (1);
		}

		return (0);
	}
	psc_fatalx("biorq %p does not have page %d", r, idx);
}

void
readahead_enqueue(const struct sl_fidgen *fgp, sl_bmapno_t bno,
    uint32_t off, int npages)
{
	struct readaheadrq *rarq;

	rarq = psc_pool_get(slc_readaheadrq_pool);
	INIT_PSC_LISTENTRY(&rarq->rarq_lentry);
	rarq->rarq_fg = *fgp;
	rarq->rarq_bno = bno;
	rarq->rarq_off = off;
	rarq->rarq_npages = npages;
	lc_add(&slc_readaheadq, rarq);
}

/*
 * Construct a request structure for an I/O issued on a bmap.
 * Notes: roff is bmap aligned.
 */
__static void
msl_biorq_build(struct msl_fsrqinfo *q, struct bmap *b, char *buf,
    int rqnum, uint32_t roff, uint32_t len, int op, uint64_t fsz,
    int last)
{
	uint32_t aoff, alen, raoff, raoff2, nbmaps, bmpce_off;
	int i, bsize, npages, rapages, rapages2;
	struct msl_fhent *mfh = q->mfsrq_mfh;
	struct bmap_pagecache_entry *e;
	struct bmpc_ioreq *r;

	/*
	 * Align the offset and length to the start of a page.  Note
	 * that roff is already made relative to the start of the given
	 * bmap.
	 */
	aoff = roff & ~BMPC_BUFMASK;
	alen = len + (roff & BMPC_BUFMASK);

	DEBUG_BMAP(PLL_DIAG, b,
	    "adding req for (off=%u) (size=%u) (nbmpce=%d)", roff, len,
	    pll_nitems(&bmap_2_bmpc(b)->bmpc_lru));

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "adding req for (off=%u) (size=%u)", roff, len);

	psc_assert(len);
	psc_assert(roff + len <= SLASH_BMAP_SIZE);

	r = bmpc_biorq_new(q, b, buf, rqnum, roff, len, op);
	if (r->biorq_flags & BIORQ_DIO)
		/*
		 * The bmap is set to use directio; we may then skip
		 * cache preparation.
		 */
		return;

	/*
	 * Calculate the number of pages needed to accommodate this
	 * request.
	 */
	npages = alen / BMPC_BUFSZ;

	if (alen % BMPC_BUFSZ)
		npages++;

	/*
	 * Lock the bmap's page cache and try to locate cached pages
	 * which correspond to this request.
	 */
	for (i = 0; i < npages; i++) {
		bmpce_off = aoff + (i * BMPC_BUFSZ);

		BMAP_LOCK(b);
		e = bmpce_lookup_locked(b, bmpce_off,
		    &b->bcm_fcmh->fcmh_waitq);
		BMAP_ULOCK(b);

		psclog_diag("biorq = %p, bmpce = %p, i = %d, npages = %d, "
		    "bmpce_foff = %"PRIx64,
		    r, e, i, npages,
		    (off_t)(bmpce_off + bmap_foff(b)));

		psc_dynarray_add(&r->biorq_pages, e);
	}

	if (op == BIORQ_WRITE || rqnum != last)
		return;

	nbmaps = (fsz + SLASH_BMAP_SIZE - 1) / SLASH_BMAP_SIZE;
	if (b->bcm_bmapno < nbmaps - 1)
		bsize = SLASH_BMAP_SIZE;
	else
		bsize = fsz - (uint64_t)SLASH_BMAP_SIZE * (nbmaps - 1);

	/*
	 * XXX: Enlarge the original request to include some readhead
	 * pages within the same bmap can save extra RPCs.  And the cost
	 * of waiting them all should be minimal.
	 */
	if (!msl_getra(mfh, bsize, aoff, npages, &raoff, &rapages,
	    &raoff2, &rapages2))
		return;

	DEBUG_BIORQ(PLL_DIAG, r, "readahead raoff=%d rapages=%d "
	    "raoff2=%d rapages2=%d",
	    raoff, rapages, raoff2, rapages2);

	/*
	 * Enqueue read ahead for next sequential region of file space.
	 */
	readahead_enqueue(&b->bcm_fcmh->fcmh_fg, b->bcm_bmapno, raoff,
	    rapages);

	/*
	 * Enqueue read ahead into next bmap if our prediction would
	 * extend into that space.
	 */
	if (rapages2 && b->bcm_bmapno < nbmaps - 1)
		readahead_enqueue(&b->bcm_fcmh->fcmh_fg,
		    b->bcm_bmapno + 1, raoff2, rapages2);
}

__static void
msl_biorq_del(struct bmpc_ioreq *r)
{
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct bmap_pagecache_entry *e;
	int i;

	BIORQ_ULOCK(r);
	BMAP_LOCK(b);
	BIORQ_LOCK(r);

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		bmpce_release_locked(e, bmpc);
	}
	psc_dynarray_free(&r->biorq_pages);

	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_FLUSHRDY) {
		pll_remove(&bmpc->bmpc_new_biorqs_exp, r);
		PSC_RB_XREMOVE(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs,
		    r);
		if ((b->bcm_flags & BMAP_FLUSHQ) &&
		    RB_EMPTY(&bmpc->bmpc_new_biorqs)) {
			b->bcm_flags &= ~BMAP_FLUSHQ;
			lc_remove(&slc_bmapflushq, b);
			DEBUG_BMAP(PLL_DIAG, b,
			    "remove from slc_bmapflushq");
		}
	}

	DEBUG_BMAP(PLL_DIAG, b, "remove biorq=%p nitems_pndg(%d)",
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
		DEBUG_BMPCE(PLL_DIAG, e, "set BMPCE_EIO");
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCE_EIO;
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

	if (r->biorq_flags & BIORQ_FREEBUF)
		PSCFREE(r->biorq_buf);

	msl_biorq_del(r);

	OPSTAT_INCR("biorq_destroy");
	psc_pool_return(slc_biorq_pool, r);
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
		if (msl_biorq_complete_fsrq(r))
			msl_pages_schedflush(r);
		BIORQ_LOCK(r);
	}

	psc_assert(r->biorq_ref > 0);
	r->biorq_ref--;
	DEBUG_BIORQ(PLL_DIAG, r, "decref");
	if (r->biorq_ref) {
		BIORQ_ULOCK(r);
		return;
	}
	msl_biorq_destroy(r);
}

struct msl_fhent *
msl_fhent_new(struct pscfs_req *pfr, struct fidc_membh *f)
{
	struct msl_fhent *mfh;

	fcmh_op_start_type(f, FCMH_OPCNT_OPEN);

	mfh = psc_pool_get(slc_mfh_pool);
	memset(mfh, 0, sizeof(*mfh));
	mfh->mfh_refcnt = 1;
	mfh->mfh_fcmh = f;
	mfh->mfh_pid = pscfs_getclientctx(pfr)->pfcc_pid;
	mfh->mfh_sid = getsid(mfh->mfh_pid);
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
    struct slashrpc_cservice **csvcp)
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
		if (*csvcp)
			return (0);
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
	struct slc_async_req *car;
	struct srm_io_rep *mp;
	struct bmpc_ioreq *r;
	struct sl_resm *m;
	int i;

	mp = pscrpc_msg_buf(rq->rq_repmsg, 0, sizeof(*mp));
	m = libsl_nid2resm(rq->rq_peer.nid);

	car = psc_pool_get(slc_async_req_pool);
	memset(car, 0, sizeof(*car));
	INIT_LISTENTRY(&car->car_lentry);
	car->car_id = mp->id;
	car->car_cbf = cbf;

	/*
	 * pscfs_req has the pointers to each biorq needed for
	 * completion.
	 */
	memcpy(&car->car_argv, av, sizeof(*av));

	if (cbf == msl_read_cb) {
		int naio = 0;

		OPSTAT_INCR("read_cb_add");
		r = av->pointer_arg[MSL_CBARG_BIORQ];
		DYNARRAY_FOREACH(e, i, a) {
			BMPCE_LOCK(e);
			if (e->bmpce_flags & BMPCE_FAULTING) {
				naio++;
				e->bmpce_flags &= ~BMPCE_FAULTING;
				e->bmpce_flags |= BMPCE_AIOWAIT;
				DEBUG_BMPCE(PLL_DIAG, e, "naio=%d", naio);
			}
			BMPCE_ULOCK(e);
		}
		/* Should have found at least one aio'd page. */
		if (!naio)
			psc_fatalx("biorq %p has no AIO pages", r);

		car->car_fsrqinfo = r->biorq_fsrqi;

	} else if (cbf == msl_dio_cb) {

		OPSTAT_INCR("dio_cb_add");

		r = av->pointer_arg[MSL_CBARG_BIORQ];
		if (r->biorq_flags & BIORQ_WRITE)
			av->pointer_arg[MSL_CBARG_BIORQ] = NULL;

		car->car_fsrqinfo = r->biorq_fsrqi;
	}

	psclog_diag("get car=%p car_id=%"PRIx64" q=%p, r=%p",
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
		OPSTAT_INCR("offline_retry_clear_err");
	}
	MFH_URLOCK(q->mfsrq_mfh, lk);
}

void
mfsrq_seterr(struct msl_fsrqinfo *q, int rc)
{
	int lk;

	lk = MFH_RLOCK(q->mfsrq_mfh);
	if (q->mfsrq_err == 0 && rc) {
		q->mfsrq_err = rc;
		DPRINTF_MFSRQ(PLL_WARN, q, "setting err=%d", rc);
	}
	MFH_URLOCK(q->mfsrq_mfh, lk);
}

#define msl_complete_fsrq(q, rc, len)					\
	_msl_complete_fsrq(PFL_CALLERINFO(), (q), (rc), (len))

void
_msl_complete_fsrq(const struct pfl_callerinfo *pci,
    struct msl_fsrqinfo *q, int rc, size_t len)
{
	void *oiov = q->mfsrq_iovs;
	struct pscfs_req *pfr;
	struct bmpc_ioreq *r;
	int i;

	pfr = mfsrq_2_pfr(q);

	MFH_LOCK(q->mfsrq_mfh);
	if (!q->mfsrq_err) {
		mfsrq_seterr(q, rc);
		q->mfsrq_len += len;
		psc_assert(q->mfsrq_len <= q->mfsrq_size);
		if (q->mfsrq_flags & MFSRQ_READ)
			q->mfsrq_mfh->mfh_nbytes_rd += len;
		else
			q->mfsrq_mfh->mfh_nbytes_wr += len;
	}

	psc_assert(q->mfsrq_ref > 0);
	q->mfsrq_ref--;
	DPRINTF_MFSRQ(PLL_DIAG, q, "decref");
	if (q->mfsrq_ref) {
		MFH_ULOCK(q->mfsrq_mfh);
		return;
	}
	psc_assert((q->mfsrq_flags & MFSRQ_FSREPLIED) == 0);
	q->mfsrq_flags |= MFSRQ_FSREPLIED;
	mfh_decref(q->mfsrq_mfh);

	if (q->mfsrq_flags & MFSRQ_READ) {
		if (q->mfsrq_err) {
			OPSTAT_INCR("fsrq_read_err");
			pscfs_reply_read(pfr, NULL, 0,
			    abs(q->mfsrq_err));
		} else {
			OPSTAT_INCR("fsrq_read_ok");

			if (q->mfsrq_len == 0) {
				pscfs_reply_read(pfr, NULL, 0, 0);
			} else if (q->mfsrq_iovs) {
				psc_assert(q->mfsrq_flags & MFSRQ_COPIED);

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
				}
				pscfs_reply_read(pfr, iov, nio, 0);
			}
		}
	} else {
		if (q->mfsrq_err)
			OPSTAT_INCR("fsrq_write_err");
		else {
			OPSTAT_INCR("fsrq_write_ok");
			msl_update_attributes(q);
			psc_assert(q->mfsrq_flags & MFSRQ_COPIED);
		}
		pscfs_reply_write(pfr, q->mfsrq_len, abs(q->mfsrq_err));
	}
	PSCFREE(oiov);
}

int
msl_biorq_complete_fsrq(struct bmpc_ioreq *r)
{
	int rc, i, needflush = 0;
	struct msl_fsrqinfo *q;
	size_t len = 0;

	/* don't do anything for readahead */
	q = r->biorq_fsrqi;
	if (q == NULL)
		return (0);

	DEBUG_BIORQ(PLL_DIAG, r, "copying");

	/* ensure biorq is in fsrq */
	for (i = 0; i < MAX_BMAPS_REQ; i++)
		if (r == q->mfsrq_biorq[i])
			break;
	if (i == MAX_BMAPS_REQ)
		DEBUG_BIORQ(PLL_FATAL, r, "missing biorq in fsrq");

	MFH_LOCK(q->mfsrq_mfh);
	rc = q->mfsrq_err;
	MFH_ULOCK(q->mfsrq_mfh);

	if (rc) {
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
			MFH_LOCK(q->mfsrq_mfh);
			len = msl_pages_copyout(r, q);
			MFH_ULOCK(q->mfsrq_mfh);
		} else {
			len = msl_pages_copyin(r);
			needflush = 1;
		}
		MFH_LOCK(q->mfsrq_mfh);
		q->mfsrq_flags |= MFSRQ_COPIED;
		MFH_ULOCK(q->mfsrq_mfh);
	}
	msl_complete_fsrq(q, 0, len);
	return (needflush);
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
			if (e->bmpce_flags & BMPCE_FAULTING) {
				e->bmpce_flags &= ~BMPCE_FAULTING;
				BMPCE_ULOCK(e);
				continue;
			}
			if (e->bmpce_flags & BMPCE_AIOWAIT) {
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

#define msl_bmpce_rpc_done(e, rc)					\
	_msl_bmpce_rpc_done(PFL_CALLERINFOSS(SLSS_BMAP), (e), (rc))

__static void
_msl_bmpce_rpc_done(const struct pfl_callerinfo *pci,
    struct bmap_pagecache_entry *e, int rc)
{
	BMPCE_LOCK(e);
	psc_assert(e->bmpce_waitq);

	if (rc) {
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCE_EIO;
	} else {
		e->bmpce_flags |= BMPCE_DATARDY;
	}

	/* AIOWAIT is removed no matter what. */
	e->bmpce_flags &= ~BMPCE_AIOWAIT;
	e->bmpce_flags &= ~BMPCE_FAULTING;
	DEBUG_BMPCE(PLL_DIAG, e, "rpc_done");

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
msl_read_cb(struct pscrpc_request *rq, int rc,
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

	(void)psc_fault_here_rc(SLC_FAULT_READ_CB_EIO, &rc, EIO);

	DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b, "sbd_seq=%"PRId64,
	    bmap_2_sbd(b)->sbd_seq);
	DEBUG_BIORQ(rc ? PLL_ERROR : PLL_DIAG, r, "rc=%d", rc);

	DYNARRAY_FOREACH(e, i, a)
		msl_bmpce_rpc_done(e, rc);

	if (rc) {
		if (rc == -PFLERR_KEYEXPIRED) {
			BMAP_LOCK(b);
			b->bcm_flags |= BMAP_CLI_LEASEEXPIRED;
			BMAP_ULOCK(b);
		}
		if ((r->biorq_flags & BIORQ_READAHEAD) == 0)
			mfsrq_seterr(r->biorq_fsrqi, rc);
	} else {
		mq = pscrpc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
		msl_update_iocounters(slc_iorpc_ist, SL_READ,
		    mq->size);
		if (r->biorq_flags & BIORQ_READAHEAD)
			psc_iostats_intv_add(&slc_readahead_issue_ist,
			    mq->size);
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
 * Thin layer around msl_read_cb(), which does the real READ completion
 * processing, in case an AIOWAIT is discovered.  Upon completion of the
 * AIO, msl_read_cb() is called.
 */
int
msl_read_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	psc_assert(rq->rq_reqmsg->opc == SRMT_READ);

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_read_cb, args));

	return (msl_read_cb(rq, rc, args));
}

int
msl_dio_cb(struct pscrpc_request *rq, int rc,
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
	msl_biorq_release(r);

	MFH_LOCK(q->mfsrq_mfh);
	mfsrq_seterr(q, rc);
	psc_waitq_wakeall(&msl_fhent_aio_waitq);
	MFH_ULOCK(q->mfsrq_mfh);

	DEBUG_BIORQ(PLL_DIAG, r, "aiowait wakeup");

	//msl_update_iocounters(slc_iorpc_ist, rw, bwc->bwc_size);

	return (rc);
}

int
msl_dio_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	OPSTAT_INCR("dio_cb0");
	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_dio_cb, args));

	return (msl_dio_cb(rq, rc, args));
}

__static int
msl_pages_dio_getput(struct bmpc_ioreq *r)
{
	int i, op, n, rc;
	size_t len, off, size;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_nbreqset *nbs = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *bci;
	struct msl_fsrqinfo *q;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
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
		OPSTAT_INCR("dio_write");
	} else {
		op = SRMT_READ;
		OPSTAT_INCR("dio_read");
	}

	rc = msl_bmap_to_csvc(b, op == SRMT_WRITE, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

  retry:
	nbs = pscrpc_nbreqset_init(NULL);

	/*
	 * The buffer associated with the request hasn't been segmented
	 * into LNET_MTU-sized chunks.  Do it now.
	 */
	for (i = 0, off = 0; i < n; i++, off += len) {
		len = MIN(LNET_MTU, size - off);

		rc = SL_RSX_NEWREQ(csvc, op, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(out, rc);

		rq->rq_bulk_abortable = 1; // for aio?
		rq->rq_interpret_reply = msl_dio_cb0;
		rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
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
			OPSTAT_INCR("dio_add_req_fail");
			PFL_GOTOERR(out, rc);
		}
		rq = NULL;
	}

	/*
	 * Should be no need for a callback since this call is fully
	 * blocking.
	 */
	psc_assert(off == size);

	rc = pscrpc_nbreqset_flush(nbs);

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
		pscrpc_nbreqset_destroy(nbs);
		OPSTAT_INCR("biorq_restart");

		/*
		 * Async I/O registered by sliod; we must wait for a
		 * notification from him when it is ready.
		 */
		goto retry;
	}

	if (rc == 0) {
		psc_iostats_intv_add(op == SRMT_WRITE ?
		    &slc_dio_ist.wr : &slc_dio_ist.rd, size);
		q = r->biorq_fsrqi;
		MFH_LOCK(q->mfsrq_mfh);
		q->mfsrq_flags |= MFSRQ_COPIED;
		MFH_ULOCK(q->mfsrq_mfh);
	}

 out:
	if (rq)
		pscrpc_req_finished(rq);

	if (nbs)
		pscrpc_nbreqset_destroy(nbs);

	if (csvc)
		sl_csvc_decref(csvc);

	PSCFREE(iovs);

	DEBUG_BIORQ(PLL_DIAG, r, "rc=%d", rc);
	return (rc);
}

__static void
msl_pages_schedflush(struct bmpc_ioreq *r)
{
	struct bmap *b = r->biorq_bmap;
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);

	BMAP_LOCK(b);
	psc_assert(b->bcm_flags & BMAP_WR);

	/*
	 * The BIORQ_FLUSHRDY bit prevents the request from being
	 * processed prematurely.
	 */
	BIORQ_LOCK(r);
	biorq_incref(r);
	r->biorq_flags |= BIORQ_FLUSHRDY;
	pll_addtail(&bmpc->bmpc_new_biorqs_exp, r);
	PSC_RB_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs, r);
	DEBUG_BIORQ(PLL_DIAG, r, "sched flush");
	BIORQ_ULOCK(r);

	if (!(b->bcm_flags & BMAP_FLUSHQ)) {
		b->bcm_flags |= BMAP_FLUSHQ;
		lc_addtail(&slc_bmapflushq, b);
		DEBUG_BMAP(PLL_DIAG, b, "add to slc_bmapflushq");
	}
	bmap_flushq_wake(BMAPFLSH_TIMEOA);

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
	uint32_t off = 0;
	int rc = 0, i;

	OPSTAT_INCR("read_rpc_launch");

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);

		BMPCE_LOCK(e);
		psc_assert(e->bmpce_flags & BMPCE_FAULTING);
		psc_assert(!(e->bmpce_flags & BMPCE_DATARDY));
		DEBUG_BMPCE(PLL_DIAG, e, "page = %d", i + startpage);
		BMPCE_ULOCK(e);

		iovs[i].iov_base = e->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;

		if (!i)
			off = e->bmpce_off;
		psc_dynarray_add(a, e);
	}

	(void)psc_fault_here_rc(SLC_FAULT_READRPC_OFFLINE, &rc,
	    -ETIMEDOUT);
	if (rc)
		PFL_GOTOERR(out, rc);
	rc = msl_bmap_to_csvc(r->biorq_bmap,
	    r->biorq_bmap->bcm_flags & BMAP_WR, &csvc);
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
	rq->rq_interpret_reply = msl_read_cb0;

	biorq_incref(r);

	rc = SL_NBRQSET_ADD(csvc, rq);
	if (rc) {
		msl_biorq_release(r);
		OPSTAT_INCR("read_add_req_fail");
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

	psc_dynarray_free(a);
	PSCFREE(a);
	PSCFREE(iovs);

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(&r->biorq_pages, i + startpage);
		/* Didn't get far enough for the waitq to be removed. */
		psc_assert(e->bmpce_waitq);

		BMPCE_LOCK(e);
		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCE_EIO;
		e->bmpce_flags &= ~BMPCE_FAULTING;
		DEBUG_BMPCE(PLL_DIAG, e, "set BMPCE_EIO");
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);
	}

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
		BMPCE_LOCK(e);
		if (e->bmpce_flags & BMPCE_FAULTING ||
		    msl_biorq_page_valid(r, i, 0)) {
			BMPCE_ULOCK(e);
			OPSTAT_INCR("readahead_gratuitous");
			continue;
		}

		e->bmpce_flags |= BMPCE_FAULTING;
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
	if (needflush)
		bmpc_biorqs_flush_wait(r->biorq_bmap);

	j = 0;
	DYNARRAY_FOREACH(e, i, &pages) {
		if (i && e->bmpce_off != off) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j);
			if (rc)
				break;
			j = i;
		}
		if (i - j + 1 == BMPC_MAXBUFSRPC ||
		    i == psc_dynarray_len(&pages) - 1) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j + 1);
			if (rc)
				break;
			j = i + 1;
		}
		off = e->bmpce_off + BMPC_BUFSZ;
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
		while (e->bmpce_flags & BMPCE_FAULTING) {
			DEBUG_BMPCE(PLL_DIAG, e, "waiting");
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
			perfect_ra = 0;
		}

		if ((e->bmpce_flags & BMPCE_READAHEAD) == 0)
			perfect_ra = 0;

		if (e->bmpce_flags & BMPCE_EIO) {
			rc = e->bmpce_rc;
			BMPCE_ULOCK(e);
			break;
		}
		if (r->biorq_flags & BIORQ_WRITE) {
			BMPCE_ULOCK(e);
			continue;
		}

		if (msl_biorq_page_valid(r, i, 0)) {
			BMPCE_ULOCK(e);
			continue;
		}

		if (e->bmpce_flags & BMPCE_AIOWAIT) {
			msl_fsrq_aiowait_tryadd_locked(e, r);
			aiowait = 1;
			BMPCE_ULOCK(e);
			OPSTAT_INCR("aio_placed");
			break;
		}
		BMPCE_ULOCK(e);
	}

	PFL_GETTIMESPEC(&ts1);
	timespecsub(&ts1, &ts0, &tsd);
	OPSTAT_ADD("biorq_fetch_wait_usecs",
	    tsd.tv_sec * 1000000 + tsd.tv_nsec / 1000);

	if (rc == 0 && perfect_ra)
		psc_iostats_intv_add(&slc_readahead_hit_ist, r->biorq_len);

 out:
	DEBUG_BIORQ(PLL_DIAG, r, "aio=%d rc=%d", aiowait, rc);
	return (rc);
}

/*
 * Copy user pages into buffer cache and schedule them to be sent to the
 * ION backend.
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
		while (e->bmpce_flags & BMPCE_PINNED) {
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
			e->bmpce_flags |= BMPCE_DATARDY;
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
		    "tsize=%u nbytes=%u toff=%u, start=%u, len=%u",
		    tsize, nbytes, toff, e->bmpce_start, e->bmpce_len);

		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	return (r->biorq_len);
}

/*
 * Copy pages to the user application buffer.
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

		psc_assert(msl_biorq_page_valid(r, i, 1));

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

void
mfh_track_predictive_io(struct msl_fhent *mfh, size_t size, off_t off,
    enum rw rw)
{
	size_t prev, curr;

	MFH_LOCK(mfh);

	if (rw == SL_WRITE) {
		if (mfh->mfh_flags & MFHF_TRACKING_RA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_RA;
			mfh->mfh_flags |= MFHF_TRACKING_WA;
			mfh->mfh_predio_off = 0;
			mfh->mfh_predio_nseq = 0;
		}
	} else {
		if (mfh->mfh_flags & MFHF_TRACKING_WA) {
			mfh->mfh_flags &= ~MFHF_TRACKING_WA;
			mfh->mfh_flags |= MFHF_TRACKING_RA;
			mfh->mfh_predio_off = 0;
			mfh->mfh_predio_nseq = 0;
		}
	}

	/*
	 * If the first read starts from offset 0, the following will
	 * trigger a read-ahead.  This is because as part of the
	 * msl_fhent structure, the fields are zeroed during allocation.
	 */
	if (mfh->mfh_predio_lastoff + mfh->mfh_predio_lastsz == off) {
		mfh->mfh_predio_nseq++;
		prev = mfh->mfh_predio_lastoff / SLASH_BMAP_SIZE;
		curr = off / SLASH_BMAP_SIZE;
		if (curr > prev && mfh->mfh_predio_nseq > 1)
			/*
			 * off can go negative here.  However, we can
			 * catch the overrun and fix it later in
			 * msl_getra().
			 */
			mfh->mfh_predio_off -= SLASH_BMAP_SIZE;
	} else {
		mfh->mfh_predio_off = 0;
		mfh->mfh_predio_nseq = 0;
	}

	mfh->mfh_predio_lastoff = off;
	mfh->mfh_predio_lastsz = size;

	MFH_ULOCK(mfh);
}

void
mfh_prod_writeahead(struct msl_fhent *mfh, size_t len, off_t off)
{
	(void)mfh;
	(void)len;
	(void)off;
#if 0
	struct bmap *b;

	MFH_LOCK(mfh);
	if (mfh->mfh_flags & MFHF_TRACKING_RA)
		PFL_GOTOERR(out, 0);
	if (!mfh->mfh_predio_nseq)
		PFL_GOTOERR(out, 0);

	(void)len;
	(void)off;

	/* XXX magic number */
	if (off + npages * BMPC_BUFSZ + 4 * SLASH_SLVR_SIZE <
	    mfh->mfh_predio_off)
		PFL_GOTOERR(out, 0);

	raoff = off + npages * BMPC_BUFSZ;
	if (mfh->mfh_predio_off) {
		if (mfh->mfh_predio_off > raoff)
			raoff = mfh->mfh_predio_off;

	if (!has been sequential)
		return;
	if (already has bmap)
		return;
	MFH_ULOCK(mfh);

	if (bmap_getf(mfh->mfh_fcmh, bno,
	    SL_WRITE, BMAPGETF_ASYNC | BMAPGETF_ADV, &b) == 0)
		bmap_op_done(b);
	return;

 out:
	MFH_ULOCK(mfh);
#endif
}

/*
 * Figure out the location and size of the next readahead based on a
 * number of factors: original read size and offset, current block map
 * size which can be smaller than 128MiB at the end of a file.
 *
 * Readahead (RA) may extend beyond the current bmap as I/O reaches
 * close to the bmap boundary, in which case RA activity is split
 * between 'this' bmap and the following (the caller checks if the
 * next bmap actually exists or not).
 *
 * @mfh: file handle.
 * @bsize: size of bmap (normally SLASH_BMAP_SIZE unless it's the last
 *	bmap).
 * @off: offset into bmap of this I/O.
 * @npages: number of pages to read.
 * @raoff1: offset where RA should begin in this same bmap.
 * @rasize1: length of RA in this same bmap.
 * @raoff2: offset where RA should start in following bmap.
 * @rasize2: length of RA in the following bmap.
 */
__static int
msl_getra(struct msl_fhent *mfh, int bsize, uint32_t off, int npages,
    uint32_t *raoff1, int *rasize1, uint32_t *raoff2, int *rasize2)
{
	int rapages, rc = 0;
	uint32_t raoff;

	MFH_LOCK(mfh);
	if (mfh->mfh_flags & MFHF_TRACKING_WA)
		PFL_GOTOERR(out, 0);

	if (!mfh->mfh_predio_nseq)
		PFL_GOTOERR(out, 0);

	/* XXX magic number */
	if (off + npages * BMPC_BUFSZ + 4 * SLASH_SLVR_SIZE <
	    mfh->mfh_predio_off)
		PFL_GOTOERR(out, 0);

	/* A sudden increase in request size can overrun our window */
	raoff = off + npages * BMPC_BUFSZ;
	if (mfh->mfh_predio_off) {
		if (mfh->mfh_predio_off > raoff)
			raoff = mfh->mfh_predio_off;
		else
			OPSTAT_INCR("read_ahead_overrun");
	}

	rapages = MIN(mfh->mfh_predio_nseq * 2,
	    psc_atomic32_read(&slc_max_readahead));

	/*
	 * The readahead can be split into two parts by the bmap
	 * boundary.
	 */
	if ((int)raoff < bsize) {
		*raoff1 = raoff;
		*rasize1 = MIN((bsize - (int)raoff + BMPC_BUFSZ - 1) /
		    BMPC_BUFSZ, rapages);

		*raoff2 = 0;
		*rasize2 = rapages - *rasize1;
	} else {
		*raoff1 = 0;
		*rasize1 = 0;

		*raoff2 = (int)raoff - bsize;
		*rasize2 = rapages;
	}
	mfh->mfh_predio_off = raoff + rapages * BMPC_BUFSZ;
	rc = 1;

 out:
	MFH_ULOCK(mfh);
	return (rc);
}

void
msl_fsrqinfo_biorq_add(struct msl_fsrqinfo *q, struct bmpc_ioreq *r,
    int biorq_num)
{
	MFH_LOCK(q->mfsrq_mfh);
	psc_assert(!q->mfsrq_biorq[biorq_num]);
	q->mfsrq_biorq[biorq_num] = r;
	q->mfsrq_ref++;
	DPRINTF_MFSRQ(PLL_DIAG, q, "incref");
	MFH_ULOCK(q->mfsrq_mfh);
}

__static struct msl_fsrqinfo *
msl_fsrqinfo_init(struct pscfs_req *pfr, struct msl_fhent *mfh,
    char *buf, size_t size, off_t off, enum rw rw)
{
	struct msl_fsrqinfo *q;

	q = PSC_AGP(pfr + 1, 0);
	q->mfsrq_mfh = mfh;
	q->mfsrq_buf = buf;
	q->mfsrq_size = size;
	q->mfsrq_off = off;
	q->mfsrq_ref = 1;
	q->mfsrq_flags = (rw == SL_READ) ? MFSRQ_READ : MFSRQ_NONE;

	mfh_incref(q->mfsrq_mfh);

	if (rw == SL_READ)
		OPSTAT_INCR("fsrq_read");
	else
		OPSTAT_INCR("fsrq_write");

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
		lc_addtail(&slc_attrtimeoutq, fci);
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
	size_t start, end, tlen, tsize;
	struct bmap_pagecache_entry *e;
	struct msl_fsrqinfo *q = NULL;
	struct timespec ts0, ts1, tsd;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	struct bmap *b;
	int nr, i, j, rc;
	uint64_t fsz;
	off_t roff;

	f = mfh->mfh_fcmh;

	DEBUG_FCMH(PLL_DIAG, f, "buf=%p size=%zu off=%"PRId64" rw=%s",
	    buf, size, off, (rw == SL_READ) ? "read" : "write");

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

	FCMH_ULOCK(f);

	if (rw == SL_READ) {
		/* Catch read ops which extend beyond EOF. */
		if (size + (uint64_t)off > fsz)
			size = fsz - off;
	}
	mfh_track_predictive_io(mfh, size, off, rw);

	/*
	 * Get the start and end block regions from the input
	 * parameters.  We support at most 2 full block worth of I/O
	 * requests that span at most one block boundary.
	 */
	start = off / SLASH_BMAP_SIZE;
	end = (off + size - 1) / SLASH_BMAP_SIZE;
	nr = end - start + 1;
	if (nr > MAX_BMAPS_REQ) {
		rc = -EINVAL;
		return (rc);
	}
	mfh->mfh_retries = 0;

	/*
	 * Initialize some state in the request to help with aio
	 * handling.
	 */
	q = msl_fsrqinfo_init(pfr, mfh, buf, size, off, rw);
	if (rw == SL_READ && (!size || off >= (off_t)fsz))
		PFL_GOTOERR(out, rc = 0);

	msl_update_iocounters(slc_iosyscall_ist, rw, size);

 restart:
	rc = 0;
	tsize = size;

	/* Relativize the length and offset (roff is not aligned). */
	roff = off - (start * SLASH_BMAP_SIZE);
	psc_assert(roff < SLASH_BMAP_SIZE);

	/* Length of the first bmap request. */
	tlen = MIN(SLASH_BMAP_SIZE - (size_t)roff, tsize);

	/*
	 * Step 1: build biorqs and get bmap.
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
			PFL_GOTOERR(out, rc);

		rc = msl_bmap_lease_tryext(b, 1);
		if (rc) {
			bmap_op_done(b);
			PFL_GOTOERR(out, rc);
		}

		PFL_GETTIMESPEC(&ts1);
		timespecsub(&ts1, &ts0, &tsd);
		OPSTAT_ADD("getbmap_wait_usecs",
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
				if (e->bmpce_flags & BMPCE_EIO) {
					e->bmpce_rc = 0;
					e->bmpce_flags &= ~BMPCE_EIO;
					DEBUG_BMPCE(PLL_DIAG, e,
					    "clear BMPCE_EIO");
				}
				BMPCE_ULOCK(e);
			}
			bmap_op_start_type(b, BMAP_OPCNT_BIORQ);
			bmap_op_done_type(r->biorq_bmap,
			    BMAP_OPCNT_BIORQ);
			r->biorq_bmap = b;
		} else {
			/*
			 * roff - (i * SLASH_BMAP_SIZE) should be zero
			 * if i == 1.
			 */
			msl_biorq_build(q, b, buf, i,
			    roff - (i * SLASH_BMAP_SIZE), tlen,
			    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE,
			    fsz, nr - 1);
		}

		bmap_op_done(b);
		roff += tlen;
		tsize -= tlen;
		if (buf)
			buf += tlen;
		tlen = MIN(SLASH_BMAP_SIZE, tsize);
	}

	if (rw == SL_WRITE)
		mfh_prod_writeahead(mfh, size, off);

	/*
	 * Step 2: launch biorqs if necessary
	 *
	 * Note that the offsets used here are file-wise offsets not
	 * offsets into the buffer.
	 */
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];

		if (r->biorq_flags & BIORQ_DIO)
			rc = msl_pages_dio_getput(r);
		else
			rc = msl_pages_fetch(r);
		if (rc)
			break;
	}

 out:
	/* Step 3: retry if at least one biorq failed */
	if (rc) {
		DEBUG_FCMH(PLL_ERROR, f,
		    "q=%p bno=%zd sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%s rc=%d",
		    q, start + i, tsize, tlen, off,
		    roff, (rw == SL_READ) ? "read" : "write", rc);

		if (msl_fd_should_retry(mfh, pfr, rc)) {
			mfsrq_clrerr(q);
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

	/*
	 * Step 4: drop our reference to the fsrq.  This is done to
	 * insert any error obtained in this routine to the mfsrq.  Each
	 * biorq holds a reference to the mfsrq so reply to pscfs will
	 * only happen after each biorq finishes.  For DIO, buffers are
	 * attached to the biorqs directly so they must be used before
	 * being freed.
	 */
	msl_complete_fsrq(q, rc, 0);

	/* Step 5: finish up biorqs. */
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];
		if (r)
			msl_biorq_release(r);
	}
	return (0);
}

void
msreadaheadthr_main(struct psc_thread *thr)
{
	struct bmap_pagecache_entry *e;
	struct readaheadrq *rarq;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	struct bmap *b;
	int i;

	while (pscthr_run(thr)) {
		f = NULL;
		b = NULL;

		rarq = lc_getwait(&slc_readaheadq);
		fidc_lookup(&rarq->rarq_fg, 0, &f);
		if (f == NULL)
			goto end;
		if (bmap_get(f, rarq->rarq_bno, SL_READ, &b))
			goto end;
		if (b->bcm_flags & BMAP_DIO)
			goto end;

		r = bmpc_biorq_new(NULL, b, NULL, 0, 0, 0, BIORQ_READ);
		r->biorq_flags |= BIORQ_READAHEAD;
		for (i = 0; i < rarq->rarq_npages; i++) {
			BMAP_LOCK(b);
			e = bmpce_lookup_locked(b,
			    rarq->rarq_off + i * BMPC_BUFSZ,
			    &f->fcmh_waitq);
			BMAP_ULOCK(b);

			BMPCE_LOCK(e);
			e->bmpce_flags |= BMPCE_READAHEAD;
			BMPCE_ULOCK(e);

			psc_dynarray_add(&r->biorq_pages, e);
		}
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
	    struct readaheadrq, rarq_lentry, PPMF_AUTO, 64, 64, 0,
	    NULL, NULL, NULL, "readaheadrq");
	slc_readaheadrq_pool = psc_poolmaster_getmgr(
	    &slc_readaheadrq_poolmaster);

	lc_reginit(&slc_readaheadq, struct readaheadrq, rarq_lentry,
	    "readaheadq");

	for (i = 0; i < NUM_READAHEAD_THREADS; i++) {
		thr = pscthr_init(MSTHRT_READAHEAD, msreadaheadthr_main,
		    NULL, sizeof(*mrat), "msreadaheadthr%d", i);
		mrat = msreadaheadthr(thr);
		psc_multiwait_init(&mrat->mrat_mw, "%s",
		    thr->pscthr_name);
		pscthr_setready(thr);
	}
}
