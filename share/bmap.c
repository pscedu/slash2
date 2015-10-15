/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2015, Pittsburgh Supercomputing Center (PSC).
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
 * SLASH2 bmap routines.  A bmap is a chunk of a file (e.g. 128MiB).
 * All activity in SLASH2 is centered around bmaps, e.g. I/O leases
 * to clients, replication work, etc.
 */

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include <limits.h>

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/thread.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"

#include "lnet/types.h"

#include "bmap.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "slerr.h"

RB_GENERATE(bmaptree, bmap, bcm_tentry, bmap_cmp)

struct psc_poolmaster	 bmap_poolmaster;
struct psc_poolmgr	*bmap_pool;

/*
 * Comparator for finding bmaps in the fcmh's tree of bmaps.
 * @a: a bmap
 * @b: another bmap
 */
int
bmap_cmp(const void *x, const void *y)
{
	const struct bmap *a = x, *b = y;

	return (CMP(a->bcm_bmapno, b->bcm_bmapno));
}

void
bmap_remove(struct bmap *b)
{
	struct fidc_membh *f = b->bcm_fcmh;

	DEBUG_BMAP(PLL_DIAG, b, "removing");

	pfl_rwlock_wrlock(&f->fcmh_rwlock);
	PSC_RB_XREMOVE(bmaptree, &f->fcmh_bmaptree, b);
	pfl_rwlock_unlock(&f->fcmh_rwlock);

	psc_pool_return(bmap_pool, b);
	fcmh_op_done_type(f, FCMH_OPCNT_BMAP);
}

void
_bmap_op_done(const struct pfl_callerinfo *pci, struct bmap *b,
    const char *fmt, ...)
{
	va_list ap;

	BMAP_LOCK_ENSURE(b);

	va_start(ap, fmt);
	_psclogv_pci(pci, SLSS_BMAP, 0, fmt, ap);
	va_end(ap);

	if (!psc_atomic32_read(&b->bcm_opcnt)) {
		b->bcm_flags |= BMAPF_TOFREE;
		DEBUG_BMAP(PLL_DIAG, b, "free bmap now");
		BMAP_ULOCK(b);

		/*
		 * Invoke service specific bmap cleanup callbacks:
		 * mds_bmap_destroy(), iod_bmap_finalcleanup(), and
		 * msl_bmap_final_cleanup().
		 */
		if (sl_bmap_ops.bmo_final_cleanupf)
			sl_bmap_ops.bmo_final_cleanupf(b);

		bmap_remove(b);
	} else {
		bmap_wake_locked(b);
		BMAP_ULOCK(b);
	}
}

/*
 * Lookup and optionally create a new bmap structure.
 * @f: file's bmap tree to search.
 * @n: bmap index number to search for.
 * @new_bmap: whether to allow creation and also value-result of whether
 * it was newly created or not.
 */
struct bmap *
bmap_lookup_cache(struct fidc_membh *f, sl_bmapno_t n, int *new_bmap)
{
	struct bmap lb, *b, *bnew = NULL;
	int doalloc;

	doalloc = *new_bmap;
	lb.bcm_bmapno = n;

 restart:
	if (doalloc && bnew)
		pfl_rwlock_wrlock(&f->fcmh_rwlock);
	else
		pfl_rwlock_rdlock(&f->fcmh_rwlock);
	b = RB_FIND(bmaptree, &f->fcmh_bmaptree, &lb);
	if (b) {
		if (!BMAP_TRYLOCK(b)) {
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			usleep(30);
			goto restart;
		}

		if (b->bcm_flags & BMAPF_TOFREE) {
			/*
			 * This bmap is going away; wait for it so we
			 * can reload it back.
			 */
			DEBUG_BMAP(PLL_DIAG, b, "wait on to-free bmap");
			BMAP_ULOCK(b);
			/*
			 * We don't want to spin if we are waiting for a
			 * flush to clear.
			 */
			psc_waitq_waitrelf_us(&f->fcmh_waitq,
			    PFL_WAITQWF_RWLOCK, &f->fcmh_rwlock, 100);
			goto restart;
		}
		bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);
	}
	if (doalloc == 0 || b) {
		pfl_rwlock_unlock(&f->fcmh_rwlock);
		if (bnew)
			psc_pool_return(bmap_pool, bnew);
		*new_bmap = 0;
		return (b);
	}
	if (bnew == NULL) {
		pfl_rwlock_unlock(&f->fcmh_rwlock);

		if (sl_bmap_ops.bmo_reapf)
			sl_bmap_ops.bmo_reapf();

		bnew = psc_pool_get(bmap_pool);
		goto restart;
	}
	b = bnew;

	*new_bmap = 1;
	memset(b, 0, bmap_pool->ppm_master->pms_entsize);
	INIT_PSC_LISTENTRY(&b->bcm_lentry);
	INIT_SPINLOCK(&b->bcm_lock);

	psc_atomic32_set(&b->bcm_opcnt, 0);
	b->bcm_fcmh = f;
	b->bcm_bmapno = n;

	/*
	 * Signify that the bmap is newly initialized and therefore may
	 * not contain certain structures.
	 */
	b->bcm_flags = BMAPF_INIT;

	bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);

	/* Perform app-specific substructure initialization. */
	sl_bmap_ops.bmo_init_privatef(b);

	/* Add to the fcmh's bmap cache */
	PSC_RB_XINSERT(bmaptree, &f->fcmh_bmaptree, b);

	pfl_rwlock_unlock(&f->fcmh_rwlock);

	fcmh_op_start_type(f, FCMH_OPCNT_BMAP);

	BMAP_LOCK(b);

	return (b);
}

/**
 * _bmap_get - Get the specified bmap.
 * @f: fcmh.
 * @n: bmap number.
 * @rw: access mode.
 * @flags: retrieval parameters.
 * @bp: value-result bmap pointer.
 * Notes: returns the bmap referenced and locked.
 */
int
_bmap_get(const struct pfl_callerinfo *pci, struct fidc_membh *f,
    sl_bmapno_t n, enum rw rw, int flags, struct bmap **bp)
{
	int rc = 0, new_bmap, bmaprw = 0;
	struct bmap *b;

	if (bp)
		*bp = NULL;
	if (rw)
		bmaprw = rw == SL_WRITE ? BMAPF_WR : BMAPF_RD;

	new_bmap = flags & BMAPGETF_CREATE;
	b = bmap_lookup_cache(f, n, &new_bmap);
	if (b == NULL) {
		rc = ENOENT;
		goto out;
	}

	if (new_bmap) {
		b->bcm_flags |= bmaprw;
		BMAP_ULOCK(b);

		/* mds_bmap_read(), iod_bmap_retrieve(), msl_bmap_retrieve() */
		if ((flags & BMAPGETF_NORETRIEVE) == 0)
			rc = sl_bmap_ops.bmo_retrievef(b, rw, flags);

		if ((flags & BMAPGETF_NONBLOCK) == 0) {
			BMAP_LOCK(b);
			b->bcm_flags &= ~BMAPF_INIT;
			bmap_wake_locked(b);
		}
	} else if ((flags & BMAPGETF_NONBLOCK) == 0) {
		/*
		 * Wait while BMAPF_INIT is set.
		 *
		 * Others wishing to access this bmap in the same mode
		 * must wait until MODECHNG ops have completed.  If the
		 * desired mode is present then a thread may proceed
		 * without blocking here so long as it only accesses
		 * structures which pertain to its mode.
		 */
		bmap_wait_locked(b, b->bcm_flags &
		    (BMAPF_INIT | BMAPF_MODECHNG));

		/*
		 * Not all lookups are done with the intent of changing
		 * the bmap mode.  bmap_lookup() does not specify a rw
		 * value.
		 *
		 * bmo_mode_chngf is currently CLI only and is
		 * msl_bmap_modeset().
		 */
		if (bmaprw && !(bmaprw & b->bcm_flags) &&
		    sl_bmap_ops.bmo_mode_chngf) {

			b->bcm_flags |= BMAPF_MODECHNG;

			DEBUG_BMAP(PLL_DIAG, b,
			    "about to mode change (rw=%d)", rw);

			BMAP_ULOCK(b);

			rc = sl_bmap_ops.bmo_mode_chngf(b, rw, 0);

			BMAP_LOCK(b);
			b->bcm_flags &= ~BMAPF_MODECHNG;
			if (!rc)
				b->bcm_flags |= bmaprw;
			bmap_wake_locked(b);
		}
	}

 out:
	if (b) {
		DEBUG_BMAP(rc && (rc != SLERR_BMAP_INVALID ||
		    (flags & BMAPGETF_NOAUTOINST) == 0) ?
		    PLL_ERROR : PLL_DIAG, b, "grabbed rc=%d", rc);
		if (rc)
			bmap_op_done(b);
		else
			*bp = b;
	}
	return (rc);
}

void
bmap_cache_init(size_t priv_size)
{
	_psc_poolmaster_init(&bmap_poolmaster,
	    sizeof(struct bmap) + priv_size,
	    offsetof(struct bmap, bcm_lentry),
	    PPMF_AUTO, 64, 64, 0, NULL, NULL, NULL, NULL, "bmap");
	bmap_pool = psc_poolmaster_getmgr(&bmap_poolmaster);
}

int
bmapdesc_access_check(struct srt_bmapdesc *sbd, enum rw rw,
    sl_ios_id_t ios_id)
{
	psc_assert(rw == SL_READ || rw == SL_WRITE);
	if (rw == SL_READ) {
		/* Read requests can get by with looser authentication. */
		if (sbd->sbd_ios != ios_id &&
		    sbd->sbd_ios != IOS_ID_ANY) {
			psclog_info("bmapdesc check failed; "
			    "type=rd ios_id sbd:%#x != %#x", sbd->sbd_ios,
			    ios_id);
			//return (EBADF);
		}
	}
	if (rw == SL_WRITE) {
		if (sbd->sbd_ios != ios_id) {
			psclog_errorx("wr ios %#x != %#x", sbd->sbd_ios,
			    ios_id);
			return (EBADF);
		}
	}
	return (0);
}

#if PFL_DEBUG > 0
void
_dump_bmap_flags_common(uint32_t *flags, int *seq)
{
	PFL_PRFLAG(BMAPF_RD, flags, seq);
	PFL_PRFLAG(BMAPF_WR, flags, seq);
	PFL_PRFLAG(BMAPF_INIT, flags, seq);
	PFL_PRFLAG(BMAPF_DIO, flags, seq);
	PFL_PRFLAG(BMAPF_DIOCB, flags, seq);
	PFL_PRFLAG(BMAPF_TOFREE, flags, seq);
	PFL_PRFLAG(BMAPF_MODECHNG, flags, seq);
	PFL_PRFLAG(BMAPF_WAITERS, flags, seq);
	PFL_PRFLAG(BMAPF_BUSY, flags, seq);
}

__weak void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}

void
_dump_bmap_common(struct bmap *b)
{
	DEBUG_BMAP(PLL_MAX, b, "");
}

__weak void
dump_bmap(struct bmap *b)
{
	_dump_bmap_common(b);
}
#endif
