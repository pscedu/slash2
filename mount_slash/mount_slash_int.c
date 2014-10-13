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
 * Core/internal CLI routines (badly named file).
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

psc_atomic32_t		slc_max_readahead = PSC_ATOMIC32_INIT(MS_READAHEAD_MAXPGS);

struct pscrpc_nbreqset *slc_pndgreadarqs;

struct psc_iostats	msl_diord_stat;
struct psc_iostats	msl_diowr_stat;
struct psc_iostats	msl_rdcache_stat;
struct psc_iostats	msl_racache_stat;

struct psc_iostats	msl_io_1b_stat;
struct psc_iostats	msl_io_1k_stat;
struct psc_iostats	msl_io_4k_stat;
struct psc_iostats	msl_io_16k_stat;
struct psc_iostats	msl_io_64k_stat;
struct psc_iostats	msl_io_128k_stat;
struct psc_iostats	msl_io_512k_stat;
struct psc_iostats	msl_io_1m_stat;

static void
msl_update_iocounters(int len)
{
	if (len < 1024)
		psc_iostats_intv_add(&msl_io_1b_stat, 1);
	else if (len < 4096)
		psc_iostats_intv_add(&msl_io_1k_stat, 1);
	else if (len < 16386)
		psc_iostats_intv_add(&msl_io_4k_stat, 1);
	else if (len < 65536)
		psc_iostats_intv_add(&msl_io_16k_stat, 1);
	else if (len < 131072)
		psc_iostats_intv_add(&msl_io_64k_stat, 1);
	else if (len < 524288)
		psc_iostats_intv_add(&msl_io_128k_stat, 1);
	else if (len < 1048576)
		psc_iostats_intv_add(&msl_io_512k_stat, 1);
	else
		psc_iostats_intv_add(&msl_io_1m_stat, 1);
}

__static int
msl_biorq_page_valid(struct bmpc_ioreq *r, int idx, int checkonly)
{
	int i;
	uint32_t toff, tsize, nbytes;
	struct bmap_pagecache_entry *e;

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
				psc_iostats_intv_add(&msl_rdcache_stat,
				    nbytes);
			return (1);
		}

		if (toff >= e->bmpce_start &&
		    toff + nbytes <= e->bmpce_start + e->bmpce_len) {
			if (!checkonly) {
				psc_iostats_intv_add(&msl_rdcache_stat,
				    nbytes);
				OPSTAT_INCR(SLC_OPST_READ_PART_VALID);
			}
			return (1);
		}

		return (0);
	}
	psc_fatalx("biorq %p does not have page %d", r, idx);
}

/**
 * msl_biorq_build - Construct a request structure for an I/O issued on
 *	a bmap.
 * Notes: roff is bmap aligned.
 */
__static void
msl_biorq_build(struct msl_fsrqinfo *q, struct bmap *b, char *buf,
    int rqnum, uint32_t roff, uint32_t len, int op, uint64_t fsz,
    int last)
{
	uint32_t aoff, alen, raoff, raoff2, nbmaps, bmpce_off;
	int i, bsize, npages, rapages, rapages2, readahead;
	struct msl_fhent *mfh = q->mfsrq_mfh;
	struct bmap_pagecache_entry *e;
	struct fcmh_cli_info *fci;
	struct bmpc_ioreq *r;
	struct fidc_membh *f;

	/*
	 * Align the offset and length to the start of a page. Note that
	 * roff is already made relative to the start of the given bmap.
	 */
	aoff = roff & ~BMPC_BUFMASK;
	alen = len + (roff & BMPC_BUFMASK);

	DEBUG_BMAP(PLL_DIAG, b,
	    "adding req for (off=%u) (size=%u) (nbmpce=%d)", roff, len,
	    pll_nitems(&bmap_2_bmpc(b)->bmpc_lru));

	DEBUG_FCMH(PLL_DIAG, mfh->mfh_fcmh,
	    "adding req for (off=%u) (size=%u)", roff, len);

	psc_assert(len);
	psc_assert((roff + len) <= SLASH_BMAP_SIZE);

	msl_update_iocounters(len);

	r = bmpc_biorq_new(q, b, buf, rqnum, roff, len, op);
	if (r->biorq_flags & BIORQ_DIO)
		/*
		 * The bmap is set to use directio; we may then skip
		 * cache preparation.
		 */
		return;

	/*
	 * How many pages are needed to accommodate the request?
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
		    &r->biorq_bmap->bcm_fcmh->fcmh_waitq);
		BMAP_ULOCK(b);

		psclog_diag("biorq = %p, bmpce = %p, i = %d, npages = %d, "
		    "bmpce_foff = %"PRIx64,
		    r, e, i, npages,
		    (off_t)(bmpce_off + bmap_foff(b)));

		psc_dynarray_add(&r->biorq_pages, e);
	}
	r->biorq_npages = npages;

	if (op == BIORQ_WRITE || rqnum != last)
		return;

	nbmaps = (fsz + SLASH_BMAP_SIZE - 1) / SLASH_BMAP_SIZE;
	if (b->bcm_bmapno < nbmaps - 1)
		bsize = SLASH_BMAP_SIZE;
	else
		bsize = fsz - (uint64_t)SLASH_BMAP_SIZE * (nbmaps - 1);

	readahead = msl_getra(mfh, bsize, aoff, npages, &raoff,
	    &rapages, &raoff2, &rapages2);

	f = mfh->mfh_fcmh;
	fci = fcmh_2_fci(f);
	if (!readahead) {
		FCMH_LOCK(f);
		if (f->fcmh_flags & FCMH_CLI_READA_QUEUE) {
			f->fcmh_flags &= ~FCMH_CLI_READA_QUEUE;
			lc_remove(&slc_readaheadq, fci);
			fcmh_op_done_type(f, FCMH_OPCNT_READAHEAD);
			return;
		}
		FCMH_ULOCK(f);
		return;
	}

	psclog_diag("raoff=%d rapages=%d", raoff, rapages);

	for (i = 0; i < rapages; i++) {
		bmpce_off = raoff + (i * BMPC_BUFSZ);

		BMAP_LOCK(b);
		e = bmpce_lookup_locked(b, bmpce_off,
		    &r->biorq_bmap->bcm_fcmh->fcmh_waitq);
		BMAP_ULOCK(b);

		psclog_diag("biorq = %p, bmpce = %p, i = %d, npages = %d, "
		    "bmpce_foff = %d\n", r, e, i, npages, bmpce_off);

		psc_dynarray_add(&r->biorq_pages, e);
	}
	if (rapages2 && b->bcm_bmapno < nbmaps - 1) {
		FCMH_LOCK(f);
		fci->fci_raoff = raoff2;
		fci->fci_rapages = rapages2;
		fci->fci_bmapno = b->bcm_bmapno + 1;
		if (!(f->fcmh_flags & FCMH_CLI_READA_QUEUE)) {
			f->fcmh_flags |= FCMH_CLI_READA_QUEUE;
			lc_addtail(&slc_readaheadq, fci);
			fcmh_op_start_type(f, FCMH_OPCNT_READAHEAD);
		}
		FCMH_ULOCK(f);
		psclog_diag("bmapno = %d, raoff = %d, rapages = %d",
		    fci->fci_bmapno, fci->fci_raoff, fci->fci_rapages);
		return;
	}
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
		bmpce_release_locked(e, bmpc);
	}

	if (r->biorq_flags & BIORQ_SPLAY) {
		PSC_SPLAY_XREMOVE(bmpc_biorq_tree,
		    &bmpc->bmpc_new_biorqs, r);
		r->biorq_flags &= ~BIORQ_SPLAY;
	}
	pll_remove(&bmpc->bmpc_pndg_biorqs, r);

	if (r->biorq_flags & BIORQ_FLUSHRDY) {
		bmpc->bmpc_pndgwr--;
		if (!bmpc->bmpc_pndgwr) {
			b->bcm_flags &= ~BMAP_FLUSHQ;
			lc_remove(&slc_bmapflushq, b);
			DEBUG_BMAP(PLL_DIAG, b,
			    "remove from slc_bmapflushq");
		}
	}

	DEBUG_BMAP(PLL_DIAG, b, "remove biorq=%p nitems_pndg(%d)",
		   r, pll_nitems(&bmpc->bmpc_pndg_biorqs));

	spinlock(&bmpc->bmpc_lock);
	psc_waitq_wakeall(&bmpc->bmpc_waitq);
	freelock(&bmpc->bmpc_lock);

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

void
_msl_biorq_destroy(const struct pfl_callerinfo *pci,
    struct bmpc_ioreq *r)
{
	int needflush;

	BIORQ_LOCK(r);
	psc_assert(r->biorq_ref > 0);
	r->biorq_ref--;
	DEBUG_BIORQ(PLL_DIAG, r, "destroying");
	if (r->biorq_ref) {
		BIORQ_ULOCK(r);
		return;
	}
	BIORQ_ULOCK(r);

	/* there is really no need to lock biorq from now on */
	if (!(r->biorq_flags & BIORQ_FLUSHRDY)) {
		/*
		 * A request can be split into several RPCs, so we can't
		 * declare it as complete until after its reference
		 * count drops to zero.
		 */
		needflush = msl_biorq_complete_fsrq(r);
		if (needflush) {
			msl_pages_schedflush(r);
			return;
		}
	}

	psc_assert(!(r->biorq_flags & BIORQ_DESTROY));
	r->biorq_flags |= BIORQ_DESTROY;

	msl_biorq_del(r);

	psc_dynarray_free(&r->biorq_pages);

	if (r->biorq_rqset) {
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;
	}

	OPSTAT_INCR(SLC_OPST_BIORQ_DESTROY);
	DEBUG_BIORQ(PLL_DIAG, r, "destroying");
	psc_pool_return(slc_biorq_pool, r);
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
 * @allow_nonvalid: as a hack, when READ is performed on a non-existent
 *	bmap, a lease is obtained for a newly created bmap with no VALID
 *	states.  So, we honor a flag to return a csvc to an IOS so the
 *	READ can no-op.
 *	XXX This entire approach should be changed.
 * @csvcp: value-result service handle.
 */
int
msl_try_get_replica_res(struct bmap *b, int iosidx, int allow_nonvalid,
    struct slashrpc_cservice **csvcp)
{
	struct bmap_cli_info *bci = bmap_2_bci(b);
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct rnd_iterator it;
	struct sl_resm *m;

	if (!allow_nonvalid && SL_REPL_GET_BMAP_IOS_STAT(bci->bci_repls,
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
	struct bmpc_ioreq *r = av->pointer_arg[MSL_CBARG_BIORQ];
	struct psc_dynarray *a = av->pointer_arg[MSL_CBARG_BMPCE];
	struct bmap_pagecache_entry *e, **bmpces;
	struct slc_async_req *car;
	struct srm_io_rep *mp;
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

	if (cbf == msl_readahead_cb) {
		/* readahead's are not associated with any biorq. */
		psc_assert(!r);
		bmpces = av->pointer_arg[MSL_CBARG_BMPCE];
		OPSTAT_INCR(SLC_OPST_READ_AHEAD_CB_ADD);

		for (i = 0; ; i++) {
			e = bmpces[i];
			if (!e)
				break;
			BMPCE_LOCK(e);
			e->bmpce_flags &= ~BMPCE_FAULTING;
			e->bmpce_flags |= BMPCE_AIOWAIT;
			DEBUG_BMPCE(PLL_DIAG, e, "set aio");
			BMPCE_ULOCK(e);
		}

	} else if (cbf == msl_read_cb) {
		int naio = 0;

		OPSTAT_INCR(SLC_OPST_READ_CB_ADD);
		DYNARRAY_FOREACH(e, i, a) {
			BMPCE_LOCK(e);
			if (e->bmpce_flags & BMPCE_FAULTING) {
				naio++;
				e->bmpce_flags &= ~BMPCE_FAULTING;
				e->bmpce_flags |= BMPCE_AIOWAIT;
			}
			DEBUG_BMPCE(PLL_INFO, e, "naio=%d", naio);
			BMPCE_ULOCK(e);
		}
		/* Should have found at least one aio'd page. */
		if (!naio)
			psc_fatalx("biorq %p has no AIO pages", r);

		car->car_fsrqinfo = r->biorq_fsrqi;

	} else if (cbf == msl_dio_cb) {
		OPSTAT_INCR(SLC_OPST_DIO_CB_ADD);
		if (r->biorq_flags & BIORQ_WRITE)
			av->pointer_arg[MSL_CBARG_BIORQ] = NULL;

		car->car_fsrqinfo = r->biorq_fsrqi;

		BIORQ_LOCK(r);
		if (r->biorq_flags & BIORQ_WRITE) {
			r->biorq_flags |= BIORQ_AIOWAIT;
			DEBUG_BIORQ(PLL_DIAG, r, "aiowait mark, q=%p",
			    car->car_fsrqinfo);
		}
		BIORQ_ULOCK(r);

	} else
		psc_fatalx("unknown callback");

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
		psclog_warnx("clearing rqinfo q=%p err=%d", q,
		    q->mfsrq_err);
		q->mfsrq_err = 0;
		OPSTAT_INCR(SLC_OPST_OFFLINE_RETRY_CLEAR_ERR);
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
		psclog_warnx("setting rqinfo q=%p err=%d", q, rc);
	}
	MFH_URLOCK(q->mfsrq_mfh, lk);
}

__static void
msl_complete_fsrq(struct msl_fsrqinfo *q, int rc, size_t len)
{
	struct pscfs_req *pfr;

	pfr = PSC_AGP(q, -sizeof(*pfr));

	MFH_LOCK(q->mfsrq_mfh);
	if (!q->mfsrq_err) {
		mfsrq_seterr(q, rc);
		q->mfsrq_len += len;
		if (q->mfsrq_flags & MFSRQ_READ)
			q->mfsrq_mfh->mfh_nbytes_rd += len;
		else
			q->mfsrq_mfh->mfh_nbytes_wr += len;
	}

	q->mfsrq_ref--;
	if (q->mfsrq_ref) {
		MFH_ULOCK(q->mfsrq_mfh);
		return;
	}
	MFH_ULOCK(q->mfsrq_mfh);

	psclog_diag("q=%p len=%zd error=%d pfr=%p",
	    q, q->mfsrq_len, q->mfsrq_err, pfr);

	mfh_decref(q->mfsrq_mfh);

	if (q->mfsrq_flags & MFSRQ_READ) {
		pscfs_reply_read(pfr, q->mfsrq_buf,
		    q->mfsrq_len, abs(q->mfsrq_err));
		OPSTAT_INCR(SLC_OPST_FSRQ_READ_FREE);
	} else {
		msl_update_attributes(q);
		pscfs_reply_write(pfr,
		    q->mfsrq_len, abs(q->mfsrq_err));
		OPSTAT_INCR(SLC_OPST_FSRQ_WRITE_FREE);
	}
}

__static int
msl_biorq_complete_fsrq(struct bmpc_ioreq *r0)
{
	struct msl_fsrqinfo *q;
	struct bmpc_ioreq *r;
	size_t len = 0;
	int i, rc, found = 0, needflush = 0;

	q = r0->biorq_fsrqi;
	if (q == NULL)
		return (0);

	/* Don't use me later */
	r0->biorq_fsrqi = NULL;

	MFH_LOCK(q->mfsrq_mfh);
	rc = q->mfsrq_err;
	MFH_ULOCK(q->mfsrq_mfh);

	psclog_diag("biorq=%p fsrq=%p pfr=%p", r0, q,
	    (char *)q - sizeof(struct pscfs_req));

	for (i = 0; i < MAX_BMAPS_REQ; i++) {
		r = q->mfsrq_biorq[i];

		/* only complete myself */
		if (r != r0)
			continue;

		found = 1;
		if (rc)
			continue;

		if (r->biorq_flags & BIORQ_DIO) {
			/*
			 * Support mix of dio and cached reads.  This
			 * may occur if the read request spans bmaps.
			 * The 'len' here was possibly adjusted against
			 * the tail of the file in msl_io().
			 */
			len = r->biorq_len;
		} else {
			if (q->mfsrq_flags & MFSRQ_READ)
				len = msl_pages_copyout(r);
			else {
				len = msl_pages_copyin(r);
				needflush = 1;
			}
		}
	}
	if (!found)
		psc_fatalx("missing biorq %p in fsrq %p", r0, q);

	psc_assert(len <= q->mfsrq_size);
	msl_complete_fsrq(q, 0, len);
	return (needflush);
}

/**
 * msl_bmpce_complete_biorq: Try to complete biorqs waiting on this page
 *	cache entry.
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
				DEBUG_BIORQ(PLL_NOTICE, r,
				    "still blocked on (bmpce@%p)", e);
			}
			/*
			 * Need to check any error on the page here.
			 */
			BMPCE_ULOCK(e);
		}
		DEBUG_BIORQ(PLL_NOTICE, r, "unblocked by (bmpce@%p)", e);
		msl_biorq_destroy(r);
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

/**
 * msl_read_cb - RPC callback used only for read or RBW operations.
 *	The primary purpose is to set the bmpce's to DATARDY so that
 *	other threads waiting for DATARDY may be unblocked.
 *  Note: Unref of the biorq will happen after the pages have been
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
		mfsrq_seterr(r->biorq_fsrqi, rc);
	}

	msl_biorq_destroy(r);

	/*
	 * Free the dynarray which was allocated in
	 * msl_read_rpc_launch().
	 */
	psc_dynarray_free(a);
	PSCFREE(a);

	sl_csvc_decref(csvc);
	return (rc);
}

/**
 * msl_read_cb0 - Thin layer around msl_read_cb(), which does the real
 *	READ completion processing, in case an AIOWAIT is discovered.
 *	Upon completion of the AIO, msl_read_cb() is called.
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
	int op, locked;

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
	locked = MFH_RLOCK(q->mfsrq_mfh);
	BIORQ_LOCK(r);
	r->biorq_flags &= ~BIORQ_AIOWAIT;
	if (q->mfsrq_flags & MFSRQ_AIOWAIT) {
		q->mfsrq_flags &= ~MFSRQ_AIOWAIT;
		DEBUG_BIORQ(PLL_DIAG, r, "aiowait wakeup, q=%p", q);
		psc_waitq_wakeall(&msl_fhent_aio_waitq);
	}
	BIORQ_ULOCK(r);
	mfsrq_seterr(q, rc);
	MFH_URLOCK(q->mfsrq_mfh, locked);

	msl_biorq_destroy(r);

	return (rc);
}

int
msl_dio_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	OPSTAT_INCR(SLC_OPST_DIO_CB0);
	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_dio_cb, args));

	return (msl_dio_cb(rq, rc, args));
}

__static int
msl_pages_dio_getput(struct bmpc_ioreq *r)
{
	int i, op, n, rc, locked;
	size_t len, nbytes, size;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct bmap_cli_info *bci;
	struct msl_fsrqinfo *q;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct bmap *b;
	struct iovec *iovs;
	uint64_t *v8;

	psc_assert(r->biorq_bmap);

	b = r->biorq_bmap;
	bci = bmap_2_bci(b);

	size = r->biorq_len;
	n = howmany(size, LNET_MTU);
	iovs = PSCALLOC(sizeof(*iovs) * n);

	v8 = (uint64_t *)r->biorq_buf;
	DEBUG_BIORQ(PLL_DEBUG, r, "dio req v8(%"PRIx64")", *v8);

	op = r->biorq_flags & BIORQ_WRITE ? SRMT_WRITE : SRMT_READ;

	rc = msl_bmap_to_csvc(b, op == SRMT_WRITE, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (r->biorq_rqset == NULL)
		r->biorq_rqset = pscrpc_prep_set();

	/*
	 * This buffer hasn't been segmented into LNET_MTU sized chunks.
	 * Set up buffers into LNET_MTU chunks or smaller.
	 */
	for (i = 0, nbytes = 0; i < n; i++, nbytes += len) {
		len = MIN(LNET_MTU, size - nbytes);

		rc = SL_RSX_NEWREQ(csvc, op, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(out, rc);

		rq->rq_bulk_abortable = 1;
		rq->rq_interpret_reply = msl_dio_cb0;
		rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;
		iovs[i].iov_base = r->biorq_buf + nbytes;
		iovs[i].iov_len  = len;

		rc = slrpc_bulkclient(rq, (op == SRMT_WRITE ?
		    BULK_GET_SOURCE : BULK_PUT_SINK), SRIC_BULK_PORTAL,
		    &iovs[i], 1);
		if (rc)
			PFL_GOTOERR(out, rc);

		mq->offset = r->biorq_off + nbytes;
		mq->size = len;
		mq->op = (op == SRMT_WRITE ? SRMIOP_WR : SRMIOP_RD);
		mq->flags |= SRM_IOF_DIO;

		memcpy(&mq->sbd, &bci->bci_sbd, sizeof(mq->sbd));

		authbuf_sign(rq, PSCRPC_MSG_REQUEST);
		pscrpc_set_add_new_req(r->biorq_rqset, rq);
		rc = pscrpc_push_req(rq);
		if (rc) {
			OPSTAT_INCR(SLC_OPST_RPC_PUSH_REQ_FAIL);
			pscrpc_set_remove_req(r->biorq_rqset, rq);
			PFL_GOTOERR(out, rc);
		}
		BIORQ_LOCK(r);
		r->biorq_ref++;
		DEBUG_BIORQ(PLL_DIAG, r, "dio launch");
		BIORQ_ULOCK(r);
	}

	/*
	 * Should be no need for a callback since this call is fully
	 * blocking.
	 */
	psc_assert(nbytes == size);
	rc = pscrpc_set_wait(r->biorq_rqset);
	pscrpc_set_destroy(r->biorq_rqset);
	r->biorq_rqset = NULL;

	/*
	 * Async I/O registered by sliod; we must wait for a
	 * notification from him when it is ready.
	 */
	if (rc == 0)
		psc_iostats_intv_add(op == SRMT_WRITE ?
		    &msl_diowr_stat : &msl_diord_stat, size);

	PSCFREE(iovs);
	sl_csvc_decref(csvc);

	if (rc == -SLERR_AIOWAIT) {
		DEBUG_BIORQ(PLL_DIAG, r, "aio op=%d", op);
		rc = 0;
		if (op == SRMT_WRITE) {
			q = r->biorq_fsrqi;
			/*
			 * The waitq of biorq is used for bmpce, so this
			 * hackery.
			 */
			for (;;) {
				locked = MFH_RLOCK(q->mfsrq_mfh);
				BIORQ_LOCK(r);
				if (!(r->biorq_flags & BIORQ_AIOWAIT)) {
					BIORQ_ULOCK(r);
					MFH_URLOCK(q->mfsrq_mfh, locked);
					break;
				}
				BIORQ_ULOCK(r);
				q->mfsrq_flags |= MFSRQ_AIOWAIT;
				DEBUG_BIORQ(PLL_DIAG, r,
				    "aiowait sleep, q=%p", q);
				psc_waitq_wait(&msl_fhent_aio_waitq,
				    &q->mfsrq_mfh->mfh_lock);
			}
			rc = -EAGAIN;
		}
	}

	return (rc);

 out:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		pscrpc_req_finished(rq);
	}

	if (r->biorq_rqset) {
		spinlock(&r->biorq_rqset->set_lock);
		if (psc_listhd_empty(&r->biorq_rqset->set_requests)) {
			pscrpc_set_destroy(r->biorq_rqset);
			r->biorq_rqset = NULL;
		} else
			freelock(&r->biorq_rqset->set_lock);
	}

	if (csvc)
		sl_csvc_decref(csvc);

	PSCFREE(iovs);
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
	r->biorq_ref++;
	r->biorq_flags |= BIORQ_FLUSHRDY | BIORQ_SPLAY;
	PSC_SPLAY_XINSERT(bmpc_biorq_tree, &bmpc->bmpc_new_biorqs, r);
	DEBUG_BIORQ(PLL_DIAG, r, "sched flush");
	BIORQ_ULOCK(r);

	bmpc->bmpc_pndgwr++;
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

int
msl_readahead_cb(struct pscrpc_request *rq, int rc,
    struct pscrpc_async_args *args)
{
	struct bmap_pagecache_entry *e,
	    **bmpces = args->pointer_arg[MSL_CBARG_BMPCE];
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	struct bmap *b = args->pointer_arg[MSL_CBARG_BMAP];
	struct bmap_pagecache *bmpc = bmap_2_bmpc(b);
	struct psc_waitq *wq = NULL;
	int i;

	if (rq)
		DEBUG_REQ(PLL_DIAG, rq, "bmpces=%p", bmpces);

	(void)psc_fault_here_rc(SLC_FAULT_READAHEAD_CB_EIO, &rc, EIO);

	for (i = 0, e = bmpces[0]; e; i++, e = bmpces[i]) {

		OPSTAT_INCR(SLC_OPST_READ_AHEAD_CB_PAGES);

		if (!i)
			DEBUG_BMAP(rc ? PLL_ERROR : PLL_DIAG, b,
			    "sbd_seq=%"PRId64, bmap_2_sbd(b)->sbd_seq);

		BMPCE_LOCK(e);
		if (rc) {
			e->bmpce_flags |= BMPCE_EIO;
			e->bmpce_rc = rc;
		} else
			e->bmpce_flags |= BMPCE_DATARDY;

		e->bmpce_flags &= ~BMPCE_READA;
		e->bmpce_flags &= ~BMPCE_AIOWAIT;
		e->bmpce_flags &= ~BMPCE_FAULTING;

		DEBUG_BMPCE(PLL_DIAG, e, "readahead rc=%d", rc);

		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);

		msl_bmpce_complete_biorq(e, rc);

		BMAP_LOCK(b);

		BMPCE_LOCK(e);
		bmpce_release_locked(e, bmpc);

		bmap_op_done_type(b, BMAP_OPCNT_READA);
	}

	if (wq)
		psc_waitq_wakeall(wq);

	sl_csvc_decref(csvc);

	PSCFREE(bmpces);
	return (rc);
}

int
msl_readahead_cb0(struct pscrpc_request *rq, struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc = args->pointer_arg[MSL_CBARG_CSVC];
	int rc;

	psc_assert(rq->rq_reqmsg->opc == SRMT_READ);

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_io_rep, rc);

	/* XXX should be negative errno */
	if (rc == -SLERR_AIOWAIT)
		return (msl_req_aio_add(rq, msl_readahead_cb, args));

	return (msl_readahead_cb(rq, rc, args));
}
void
msl_reada_rpc_launch(struct psc_dynarray *bmpces, int startpage,
    int npages, struct bmap *b)
{
	struct bmap_pagecache_entry *e, **bmpces_cbarg;
	struct slashrpc_cservice *csvc = NULL;
	struct pscrpc_request *rq = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	uint32_t off = 0;
	int rc, i;

	psc_assert(npages > 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	bmpces_cbarg = PSCALLOC((npages + 1) * sizeof(void *));

	iovs = PSCALLOC(npages * sizeof(*iovs));

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);
		bmpces_cbarg[i] = e;

		if (!i)
			off = e->bmpce_off;

		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCE_FAULTING | BMPCE_READA;
		DEBUG_BMPCE(PLL_DIAG, e, "page = %d", i + startpage);
		psc_atomic32_inc(&e->bmpce_ref);
		BMPCE_ULOCK(e);

		iovs[i].iov_base = e->bmpce_base;
		iovs[i].iov_len  = BMPC_BUFSZ;
		bmap_op_start_type(b, BMAP_OPCNT_READA);
	}
	bmpces_cbarg[npages] = NULL;

	rc = msl_bmap_to_csvc(b, 0, &csvc);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = SL_RSX_NEWREQ(csvc, SRMT_READ, rq, mq, mp);
	if (rc)
		PFL_GOTOERR(out, rc);

	rc = rsx_bulkclient(rq, BULK_PUT_SINK, SRIC_BULK_PORTAL, iovs,
	    npages);
	if (rc)
		PFL_GOTOERR(out, rc);

	PSCFREE(iovs);

	mq->size = BMPC_BUFSZ * npages;
	mq->op = SRMIOP_RD;
	mq->offset = off;
	psc_assert(mq->offset + mq->size <= SLASH_BMAP_SIZE);
	memcpy(&mq->sbd, bmap_2_sbd(b), sizeof(mq->sbd));

	DEBUG_BMAP(PLL_DIAG, b, "reada req off=%u, npages=%d", off,
	    npages);

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	rq->rq_timeout = 15;
	rq->rq_bulk_abortable = 1;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = bmpces_cbarg;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = NULL;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMAP] = b;
	rq->rq_interpret_reply = msl_readahead_cb0;
	pscrpc_req_setcompl(rq, &slc_rpc_compl);

	rc = pscrpc_nbreqset_add(slc_pndgreadarqs, rq);
	if (!rc)
		return;

 out:
	PSCFREE(iovs);
	PSCFREE(bmpces_cbarg);

	/* Deal with errored read ahead bmpce's. */
	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);

		BMPCE_LOCK(e);

		e->bmpce_rc = rc;
		e->bmpce_flags |= BMPCE_EIO;
		e->bmpce_flags &= ~BMPCE_FAULTING;
		BMPCE_WAKE(e);
		DEBUG_BMPCE(PLL_INFO, e, "set BMPCE_EIO");

		bmpce_release_locked(e, bmap_2_bmpc(b));
		bmap_op_done_type(b, BMAP_OPCNT_READA);
	}

	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed");
		//pscrpc_abort_bulk(rq->rq_bulk);
		pscrpc_req_finished(rq);
	}
	if (csvc)
		sl_csvc_decref(csvc);
}

/**
 * msl_read_rpc_launch - Launch a RPC for a given range of pages.  Note
 *	that a request can be satisfied by multiple RPCs because parts
 *	of the range covered by the request may have already been
 *	cached.
 */
__static int
msl_read_rpc_launch(struct bmpc_ioreq *r, struct psc_dynarray *bmpces,
    int startpage, int npages)
{
	struct slashrpc_cservice *csvc = NULL;
	struct bmap_pagecache_entry *e;
	struct pscrpc_request *rq = NULL;
	struct psc_dynarray *a = NULL;
	struct srm_io_req *mq;
	struct srm_io_rep *mp;
	struct iovec *iovs;
	uint32_t off = 0;
	int rc = 0, i;

	OPSTAT_INCR(SLC_OPST_READ_RPC_LAUNCH);

	a = PSCALLOC(sizeof(*a));
	psc_dynarray_init(a);

	psc_assert(startpage >= 0);
	psc_assert(npages <= BMPC_MAXBUFSRPC);

	iovs = PSCALLOC(sizeof(*iovs) * npages);

	for (i = 0; i < npages; i++) {
		e = psc_dynarray_getpos(bmpces, i + startpage);

		BMPCE_LOCK(e);
		e->bmpce_flags |= BMPCE_FAULTING;
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

	DEBUG_BIORQ(PLL_DIAG, r, "fid="SLPRI_FG", start=%d, pages=%d, ios=%u",
	    SLPRI_FG_ARGS(&mq->sbd.sbd_fg), startpage, npages,
	    bmap_2_ios(r->biorq_bmap));

	authbuf_sign(rq, PSCRPC_MSG_REQUEST);

	/* Setup the callback, supplying the dynarray as an argument. */
	rq->rq_async_args.pointer_arg[MSL_CBARG_BMPCE] = a;
	rq->rq_async_args.pointer_arg[MSL_CBARG_CSVC] = csvc;
	rq->rq_async_args.pointer_arg[MSL_CBARG_BIORQ] = r;

	if (!r->biorq_rqset)
		/*
		 * XXX Using a set for any type of read may be overkill.
		 */
		r->biorq_rqset = pscrpc_prep_set();

	rq->rq_interpret_reply = msl_read_cb0;
	pscrpc_set_add_new_req(r->biorq_rqset, rq);

	rc = pscrpc_push_req(rq);
	if (rc) {
		OPSTAT_INCR(SLC_OPST_RPC_PUSH_REQ_FAIL);
		pscrpc_set_remove_req(r->biorq_rqset, rq);
		PFL_GOTOERR(out, rc);
	}

	BIORQ_LOCK(r);
	r->biorq_ref++;
	DEBUG_BIORQ(PLL_DIAG, r, "RPC launch");
	BIORQ_ULOCK(r);

	PSCFREE(iovs);
	return (0);

 out:
	if (rq) {
		DEBUG_REQ(PLL_ERROR, rq, "req failed, rc = %d", rc);
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
	struct bmap_pagecache_entry *e;
	struct psc_dynarray pages = DYNARRAY_INIT;
	int rc = 0, i, j, npages, needflush = 0;
	uint32_t off = 0;

	npages = 0;
	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {
		BMPCE_LOCK(e);
		if (e->bmpce_flags & BMPCE_FAULTING) {
			BMPCE_ULOCK(e);
			continue;
		}

		if (msl_biorq_page_valid(r, i, 0)) {
			BMPCE_ULOCK(e);
			continue;
		}
		if (i < r->biorq_npages)
			npages++;
		else
			psc_iostats_intv_add(&msl_racache_stat,
			    BMPC_BUFSZ);

		e->bmpce_flags |= BMPCE_FAULTING;
		DEBUG_BMPCE(PLL_DIAG, e, "npages = %d, i = %d", npages, i);
		psc_dynarray_add(&pages, e);
		BMPCE_ULOCK(e);
		needflush = 1;
	}

	/*
	 * We must flush any pending writes first before reading from
	 * the storage.
	 */
	if (needflush)
		bmpc_biorqs_flush(r->biorq_bmap, 1);

	j = 0;
	DYNARRAY_FOREACH(e, i, &pages) {

		if (!npages)
			break;

		/*
		 * Mixing readahead pages and normal pages is tricky.
		 */
		if (i && (e->bmpce_off != off || i >= npages)) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j);
			if (rc)
				goto out;

			if (i >= npages)
				break;

			j = i;
		}
		if (i - j + 1 == BMPC_MAXBUFSRPC ||
		    i == psc_dynarray_len(&pages) - 1) {
			rc = msl_read_rpc_launch(r, &pages, j, i - j + 1);
			if (rc)
				goto out;
			j = i + 1;
		}
		off = e->bmpce_off + BMPC_BUFSZ;
	}
	j = i;
	DYNARRAY_FOREACH_CONT(e, i, &pages) {

		if (i != j && e->bmpce_off != off) {
			msl_reada_rpc_launch(&pages, j, i - j,
			    r->biorq_bmap);
			j = i;
		}
		if (i - j + 1 == BMPC_MAXBUFSRPC ||
		    i == psc_dynarray_len(&pages) - 1) {
			msl_reada_rpc_launch(&pages, j, i - j + 1,
			    r->biorq_bmap);
			j = i + 1;
		}
		off = e->bmpce_off + BMPC_BUFSZ;
	}
 out:
	psc_dynarray_free(&pages);

	return (rc);
}

/**
 * msl_pages_prefetch - Launch read RPCs for pages that are owned by the
 *	given I/O request.  This function is called to perform a pure
 *	read request or a read-before-write for a write request.  It is
 *	also used to wait for read-ahead pages to complete.
 */
int
msl_pages_prefetch(struct bmpc_ioreq *r)
{
	int i, rc = 0, aiowait = 0;
	struct bmap_pagecache_entry *e;

	psc_assert(!r->biorq_rqset);

	if (r->biorq_flags & BIORQ_READ) {
		rc = msl_launch_read_rpcs(r);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

	/*
	 * Wait for all read activities (include RBW) associated with
	 * the bioreq to complete.
	 *
	 * This set wait makes sure that pages belong to the current
	 * request are faulted in.
	 */
	if (r->biorq_rqset) {
		/*
		 * Note: This can trigger invocation of our read
		 * callback in this same thread.
		 */
		rc = pscrpc_set_wait(r->biorq_rqset);

		/*
		 * The set cb is not being used; msl_read_cb() is called
		 * on every RPC in the set.  This was causing the biorq
		 * to have its flags mod'd in an incorrect fashion.  For
		 * now, the following lines will be moved here.
		 */
		BIORQ_LOCK(r);

		if (!rc)
			DEBUG_BIORQ(PLL_DIAG, r, "read cb complete");
		BIORQ_ULOCK(r);

		/* Destroy and cleanup the set now. */
		pscrpc_set_destroy(r->biorq_rqset);
		r->biorq_rqset = NULL;

		if (rc && rc != -SLERR_AIOWAIT)
			goto out;

		/* Our caller expects a success in case of aiowait */
		rc = 0;
	}

	DYNARRAY_FOREACH(e, i, &r->biorq_pages) {

		if (i >= r->biorq_npages)
			break;

		BMPCE_LOCK(e);
		while (e->bmpce_flags & BMPCE_FAULTING) {
			DEBUG_BMPCE(PLL_DIAG, e, "waiting");
			BMPCE_WAIT(e);
			BMPCE_LOCK(e);
		}

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
			OPSTAT_INCR(SLC_OPST_AIO_PLACED);
			break;
		}
		BMPCE_ULOCK(e);
	}
 out:
	DEBUG_BIORQ(PLL_DIAG, r, "aio=%d rc=%d", aiowait, rc);
	return (rc);
}

/**
 * msl_pages_copyin - Copy user pages into buffer cache and schedule
 *	them to be sent to the ION backend.
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

		e->bmpce_flags |= BMPCE_DIRTY;
		BMPCE_WAKE(e);
		BMPCE_ULOCK(e);

		toff  += nbytes;
		src   += nbytes;
		tsize -= nbytes;
	}
	psc_assert(!tsize);
	return (r->biorq_len);
}

/**
 * msl_pages_copyout - Copy pages to the user application buffer.
 */
size_t
msl_pages_copyout(struct bmpc_ioreq *r)
{
	struct bmap_pagecache_entry *e;
	size_t nbytes, tbytes = 0, rflen;
	int i, npages, tsize;
	char *dest, *src;
	off_t toff;

	dest = r->biorq_buf;
	toff = r->biorq_off;

	rflen = fcmh_getsize(r->biorq_bmap->bcm_fcmh) -
	    bmap_foff(r->biorq_bmap);

	if (biorq_voff_get(r) > rflen) {
		/* The request goes beyond EOF. */
		tsize = rflen - r->biorq_off;
	} else
		tsize = r->biorq_len;

	DEBUG_BIORQ(PLL_DIAG, r, "tsize=%d biorq_len=%u biorq_off=%u",
		    tsize, r->biorq_len, r->biorq_off);

	if (!tsize || tsize < 0)
		return (0);

	npages = psc_dynarray_len(&r->biorq_pages);

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

		memcpy(dest, src, nbytes);

		BMPCE_ULOCK(e);

		toff   += nbytes;
		dest   += nbytes;
		tbytes += nbytes;
		tsize  -= nbytes;
	}
	psc_assert(!tsize);

	return (tbytes);
}

/*
 * Figure out the location and size of the next readahead based on a
 * number of factors: original read size and offset, current block map
 * size which can be smaller than 128MiB at the end of a file.
 *
 * Readahead (RA) may extend beyond the current bmap as I/O reaches
 * close to the bmap boundary, in which case RA activity is split
 * between 'this' bmap and the following.
 *
 * @mfh: file handle.
 * @bsize: size of bmap (normally SLASH_BMAP_SIZE unless it's the last bmap).
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
	int rapages;
	uint32_t raoff;

	MFH_LOCK(mfh);
	if (!mfh->mfh_ra.mra_nseq) {
		MFH_ULOCK(mfh);
		return (0);
	}
	if (off + npages * BMPC_BUFSZ + 4 * SLASH_SLVR_SIZE <
	    mfh->mfh_ra.mra_raoff) {
		MFH_ULOCK(mfh);
		return (0);
	}

#if 1
	if (!mfh->mfh_ra.mra_raoff)
		raoff = off + npages * BMPC_BUFSZ;
	else
		raoff = mfh->mfh_ra.mra_raoff;
#else
	raoff = off + npages * BMPC_BUFSZ;
	if (mfh->mfh_ra.mra_raoff && mfh->mfh_ra.mra_raoff > raoff)
		raoff = mfh->mfh_ra.mra_raoff;
#endif

	rapages = MIN(mfh->mfh_ra.mra_nseq * 2,
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
	mfh->mfh_ra.mra_raoff = raoff + rapages * BMPC_BUFSZ;

	MFH_ULOCK(mfh);

	return (1);
}

static void
msl_setra(struct msl_fhent *mfh, size_t size, off_t off)
{
	size_t prev, curr;
	spinlock(&mfh->mfh_lock);

	/*
	 * If the first read starts from offset 0, the following will
	 * trigger a read-ahead.  This is because as part of the
	 * msl_fhent structure, the fields are zeroed during allocation.
	 */
	if (mfh->mfh_ra.mra_loff + mfh->mfh_ra.mra_lsz == off) {
		mfh->mfh_ra.mra_nseq++;
		prev = mfh->mfh_ra.mra_loff / SLASH_BMAP_SIZE;
		curr = off / SLASH_BMAP_SIZE;
		if (curr > prev && mfh->mfh_ra.mra_nseq > 1)
			mfh->mfh_ra.mra_raoff -= SLASH_BMAP_SIZE;
	} else {
		mfh->mfh_ra.mra_raoff = 0;
		mfh->mfh_ra.mra_nseq = 0;
	}

	mfh->mfh_ra.mra_loff = off;
	mfh->mfh_ra.mra_lsz = size;

	freelock(&mfh->mfh_lock);
}

void
msl_fsrqinfo_biorq_add(struct msl_fsrqinfo *q, struct bmpc_ioreq *r,
    int biorq_num)
{
	MFH_LOCK(q->mfsrq_mfh);
	psc_assert(!q->mfsrq_biorq[biorq_num]);
	q->mfsrq_biorq[biorq_num] = r;
	q->mfsrq_ref++;
	MFH_ULOCK(q->mfsrq_mfh);
}

__static struct msl_fsrqinfo *
msl_fsrqinfo_init(struct pscfs_req *pfr, struct msl_fhent *mfh,
    char *buf, size_t size, off_t off, enum rw rw)
{
	int i;
	struct msl_fsrqinfo *q;

	q = PSC_AGP(pfr + 1, 0);
	for (i = 0; i < MAX_BMAPS_REQ; i++)
		q->mfsrq_biorq[i] = NULL;
	q->mfsrq_mfh = mfh;
	q->mfsrq_buf = buf;
	q->mfsrq_size = size;
	q->mfsrq_len = 0;
	q->mfsrq_off = off;
	q->mfsrq_err = 0;
	q->mfsrq_ref = 1;
	q->mfsrq_flags = (rw == SL_READ) ? MFSRQ_READ : MFSRQ_NONE;

	mfh_incref(q->mfsrq_mfh);

	if (rw == SL_READ)
		OPSTAT_INCR(SLC_OPST_FSRQ_READ);
	else
		OPSTAT_INCR(SLC_OPST_FSRQ_WRITE);

	psclog_diag("fsrq=%p pfr=%p rw=%d", q, pfr, rw);
	return (q);
}

void
msl_update_attributes(struct msl_fsrqinfo *q)
{
	struct timespec ts;
	struct fidc_membh *f;
	struct msl_fhent *mfh;
	struct fcmh_cli_info *fci;

	if (q->mfsrq_err)
		return;

	mfh = q->mfsrq_mfh;
	f = mfh->mfh_fcmh;

	FCMH_LOCK(f);
	PFL_GETTIMESPEC(&ts);
	f->fcmh_sstb.sst_mtime = ts.tv_sec;
	f->fcmh_sstb.sst_mtime_ns = ts.tv_nsec;
	f->fcmh_flags |= FCMH_CLI_DIRTY_MTIME;
	if (q->mfsrq_off + q->mfsrq_len > fcmh_2_fsz(f)) {
		psclog_info("fid: "SLPRI_FID", "
		    "size from %"PRId64" to %"PRId64,
		    fcmh_2_fid(f), fcmh_2_fsz(f),
		    q->mfsrq_off + q->mfsrq_len);
		fcmh_2_fsz(f) = q->mfsrq_off + q->mfsrq_len;
		f->fcmh_flags |= FCMH_CLI_DIRTY_DSIZE;
	}
	if (!(f->fcmh_flags & FCMH_CLI_DIRTY_QUEUE)) {
		fci = fcmh_2_fci(f);
		fci->fci_etime.tv_sec = ts.tv_sec;
		fci->fci_etime.tv_nsec = ts.tv_nsec;
		f->fcmh_flags |= FCMH_CLI_DIRTY_QUEUE;
		lc_addtail(&slc_attrtimeoutq, fci);
		fcmh_op_start_type(f, FCMH_OPCNT_DIRTY_QUEUE);
	}
	FCMH_ULOCK(f);
}

/**
 * msl_io - I/O gateway routine which bridges pscfs and the SLASH2
 *	client cache and backend.  msl_io() handles the creation of
 *	biorq's and the loading of bmaps (which are attached to the
 *	file's fcmh and is ultimately responsible for data being
 *	prefetched (as needed), copied into or from the cache, and (on
 *	write) being pushed to the correct I/O server.
 *
 *	The function implements the backend of mslfsop_read() and
 *	mslfsop_write().
 *
 * @pfr: file system request, used for tracking potentially asynchronous
 *	activity.
 * @mfh: file handle structure passed to us by pscfs which contains the
 *	pointer to our fcmh.
 * @buf: the application source/dest buffer.
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
	struct bmap *b;
	struct fidc_membh *f;
	struct bmpc_ioreq *r;
	uint64_t fsz;
	int nr, i, j, rc;
	off_t roff;
	char *bufp;

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
		msl_setra(mfh, size, off);
	}

	/*
	 * Get the start and end block regions from the input
	 * parameters. We support at most 2 full block worth
	 * of I/O requests that span at most one block boundary.
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
	 * Initialize some state in the pfr to help with aio requests.
	 */
	q = msl_fsrqinfo_init(pfr, mfh, buf, size, off, rw);
	if (rw == SL_READ && (!size || off >= (off_t)fsz))
		PFL_GOTOERR(out, rc = 0);

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
	for (i = 0, bufp = buf; i < nr; i++) {

		DEBUG_FCMH(PLL_DIAG, f, "sz=%zu tlen=%zu off=%"PSCPRIdOFFT" "
		    "roff=%"PSCPRIdOFFT" rw=%s", tsize, tlen, off, roff,
		    (rw == SL_READ) ? "read" : "write");

		psc_assert(tsize);

		rc = bmap_get(f, start + i, rw, &b);
		if (rc)
			PFL_GOTOERR(out, rc);

		rc = msl_bmap_lease_tryext(b, 1);
		if (rc) {
			bmap_op_done(b);
			PFL_GOTOERR(out, rc);
		}

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
			msl_biorq_build(q, b, bufp, i,
			    roff - (i * SLASH_BMAP_SIZE), tlen,
			    (rw == SL_READ) ? BIORQ_READ : BIORQ_WRITE,
			    fsz, nr - 1);
		}

		bmap_op_done(b);
		roff += tlen;
		tsize -= tlen;
		bufp += tlen;
		tlen  = MIN(SLASH_BMAP_SIZE, tsize);
	}

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
			rc = msl_pages_prefetch(r);
		if (rc)
			break;
	}
	if (r->biorq_flags & BIORQ_DIO && rc == -EAGAIN) {
		OPSTAT_INCR(SLC_OPST_BIORQ_RESTART);
		goto restart;
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
		 * Make sure we don't copy pages from biorq in
		 * case of an error.
		 */
		mfsrq_seterr(q, rc);
	}

	/* Step 4: finish up biorqs */
	for (i = 0; i < nr; i++) {
		r = q->mfsrq_biorq[i];
		if (r)
			msl_biorq_destroy(r);
	}

	/*
	 * Drop the our reference to the fsrq.  This reference acts like
	 * a barrier to multiple biorqs so that none of them can
	 * complete a fsrq prematurely.
	 *
	 * In addition, it allows us to abort the I/O if we cannot even
	 * build a biorq with bmap.
	 */
	msl_complete_fsrq(q, rc, 0);
	return (0);
}
