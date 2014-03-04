/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2014, Pittsburgh Supercomputing Center (PSC).
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
 * The FID cache is an in-memory representation of file objects normally
 * accessed by FID (global SLASH2 file identifier).
 */

#define PSC_SUBSYS SLSS_FCMH
#include "slsubsys.h"

#include <pthread.h>
#include <stdio.h>

#include "pfl/atomic.h"
#include "pfl/cdefs.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/pool.h"
#include "pfl/thread.h"

#include "bmap.h"
#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"
#include "slutil.h"

struct psc_poolmaster	  fidcPoolMaster;
struct psc_poolmgr	 *fidcPool;
struct psc_listcache	  fidcBusyList;		/* active in use */
struct psc_listcache	  fidcIdleList;		/* identity untouched, but reapable */
struct psc_hashtbl	  fidcHtable;

#define	fcmh_get()	psc_pool_get(fidcPool)
#define	fcmh_put(f)	psc_pool_return(fidcPool, (f))

/**
 * fcmh_dtor - Destructor for FID cache member handles.
 * @f: fcmh being destroyed.
 */
void
fcmh_destroy(struct fidc_membh *f)
{
	psc_assert(SPLAY_EMPTY(&f->fcmh_bmaptree));
	psc_assert(f->fcmh_refcnt == 0);
	psc_assert(psc_hashent_disjoint(&fidcHtable, f));
	psc_assert(!psc_waitq_nwaiters(&f->fcmh_waitq));

	psc_waitq_destroy(&f->fcmh_waitq);

	/* slc_fcmh_dtor(), slm_fcmh_dtor(), sli_fcmh_dtor() */
	if (sl_fcmh_ops.sfop_dtor) {
		if (f->fcmh_flags & (FCMH_CTOR_FAILED | FCMH_NO_BACKFILE))
			DEBUG_FCMH(PLL_INFO, f,
			    "bypassing dtor() call");
		else
			sl_fcmh_ops.sfop_dtor(f);
	}

	f->fcmh_flags = FCMH_CAC_FREE;
	fcmh_put(f);
}

/**
 * fcmh_setattrf - Update the high-level app stat(2)-like attribute
 *	buffer for a FID cache member.
 * @f: FID cache member to update.
 * @sstb: incoming stat attributes.
 * @flags: behavioral flags.
 * Notes:
 *     (1) if SAVELOCAL has been specified, save local field values:
 *		(o) file size
 *		(o) mtime
 *     (2) This function should only be used by a client.
 */
void
fcmh_setattrf(struct fidc_membh *f, struct srt_stat *sstb, int flags)
{
	if (flags & FCMH_SETATTRF_HAVELOCK)
		FCMH_LOCK_ENSURE(f);
	else
		FCMH_LOCK(f);

	if (fcmh_2_gen(f) == FGEN_ANY)
	    fcmh_2_gen(f) = sstb->sst_gen;

	if (fcmh_2_fid(f) != SLFID_ROOT &&
	    fcmh_2_gen(f) > sstb->sst_gen) {
		DEBUG_FCMH(PLL_WARN, f, "attempt to set attr with "
		    "gen %"PRIu64" from old gen %"PRIu64,
		    fcmh_2_gen(f), sstb->sst_gen);
		goto out;
	}

	/*
	 * If we don't have stat attributes, how can we save our local
	 * updates?
	 */
	if ((f->fcmh_flags & FCMH_HAVE_ATTRS) == 0)
		flags &= ~FCMH_SETATTRF_SAVELOCAL;

	psc_assert(sstb->sst_gen != FGEN_ANY);
	psc_assert(f->fcmh_fg.fg_fid == sstb->sst_fid);

	/*
	 * If generation numbers match, take the highest of the values.
	 * Otherwise, disregard local values and blindly accept whatever
	 * the MDS tells us.
	 */
	if (flags & FCMH_SETATTRF_SAVELOCAL) {
		if (fcmh_2_ptruncgen(f) == sstb->sst_ptruncgen &&
		    fcmh_2_gen(f) == sstb->sst_gen &&
		    fcmh_2_fsz(f) > sstb->sst_size)
			sstb->sst_size = fcmh_2_fsz(f);
		if (fcmh_2_utimgen(f) == sstb->sst_utimgen)
			sstb->sst_mtim = f->fcmh_sstb.sst_mtim;
	}

	COPY_SSTB(sstb, &f->fcmh_sstb);
	f->fcmh_flags |= FCMH_HAVE_ATTRS;
	f->fcmh_flags &= ~FCMH_GETTING_ATTRS;

	if (sl_fcmh_ops.sfop_postsetattr)
		sl_fcmh_ops.sfop_postsetattr(f);

	DEBUG_FCMH(PLL_DEBUG, f, "attr set");

 out:
	if (!(flags & FCMH_SETATTRF_HAVELOCK))
		FCMH_ULOCK(f);
}

/**
 * fidc_reap - Reap some fcmhs from the idle list.
 */
int
fidc_reap(struct psc_poolmgr *m)
{
#define FCMH_MAX_REAP 8
	struct fidc_membh *f, *tmp, *reap[FCMH_MAX_REAP];
	int i, waslocked = 0, nreap = 0;

	psc_assert(m == fidcPool);

	LIST_CACHE_LOCK(&fidcIdleList);
	LIST_CACHE_FOREACH_SAFE(f, tmp, &fidcIdleList) {
		if (nreap == FCMH_MAX_REAP ||
		    nreap > atomic_read(&m->ppm_nwaiters))
			break;

		/* skip the root right now, no need for locking */
		if (fcmh_2_fid(f) == 1)
			continue;

		if (!FCMH_TRYRLOCK(f, &waslocked))
			continue;

		/* skip items in use */
		if (f->fcmh_refcnt)
			goto end;

		psc_assert(f->fcmh_flags & FCMH_CAC_IDLE);

		/* already victimized */
		if (f->fcmh_flags & FCMH_CAC_REAPED)
			goto end;
		DEBUG_FCMH(PLL_DEBUG, f, "reaped");

		/*
		 * Consult the context-specific callback handler before
		 *    freeing.
		 */
		f->fcmh_flags |= FCMH_CAC_REAPED | FCMH_CAC_TOFREE;
		lc_remove(&fidcIdleList, f);
		reap[nreap] = f;
		nreap++;
 end:
		FCMH_URLOCK(f, waslocked);
	}
	LIST_CACHE_ULOCK(&fidcIdleList);

	psclog_debug("reaped %d fcmhs", nreap);

	for (i = 0; i < nreap; i++) {
		psc_hashent_remove(&fidcHtable, reap[i]);
		fcmh_destroy(reap[i]);
	}
	return (i);
}

/**
 * _fidc_lookup - Search the FID cache for a member by its FID,
 *	optionally creating it.
 * @pci: thread caller information.
 * @fgp: FID and GEN #.
 * @flags: access flags.
 * @sstb: statbuf to use if populating a new entry.
 * @setattrflags: SETATTRF_* flags specifying which statbuf fields are
 *	valid.
 * @fp: value-result fcmh return.
 * @arg: argument to GETATTR.
 * Note:  Newly acquired fcmh's are ref'd with FCMH_OPCNT_NEW, reused
 *	ones are ref'd with FCMH_OPCNT_LOOKUP_FIDC.
 * Note: Returns positive errno.
 */
int
_fidc_lookup(const struct pfl_callerinfo *pci,
    const struct slash_fidgen *fgp, int flags, struct srt_stat *sstb,
    int setattrflags, struct fidc_membh **fp, void *arg)
{
	struct fidc_membh *tmp, *fcmh, *fcmh_new;
	struct psc_hashbkt *b;
	int rc = 0, try_create = 0;

	psclog_debug("fidc_lookup called for fid "SLPRI_FID, fgp->fg_fid);

	*fp = NULL;
	fcmh_new = NULL; /* gcc */

	/* sanity checks */
#ifdef _SLASH_CLIENT
	if (flags & FIDC_LOOKUP_CREATE)
		psc_assert(sstb || (flags & FIDC_LOOKUP_LOAD));
#else
	psc_assert(!(flags & FIDC_LOOKUP_EXCL));
#endif

	if (sstb)
		psc_assert((flags & FIDC_LOOKUP_LOAD) == 0);
	if (flags & FIDC_LOOKUP_LOAD)
		psc_assert(sstb == NULL);

	/* OK.  Now check if it is already in the cache. */
	b = psc_hashbkt_get(&fidcHtable, &fgp->fg_fid);
 restart:
	fcmh = NULL;
	psc_hashbkt_lock(b);
	PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, tmp, b) {
		/*
		 * Note that generation number is only used to track
		 *   truncations.
		 */
		if (fgp->fg_fid != fcmh_2_fid(tmp))
			continue;
		FCMH_LOCK(tmp);

		/* if the item is being freed, ignore it */
		if (tmp->fcmh_flags & FCMH_CAC_TOFREE) {
			FCMH_ULOCK(tmp);
			pscthr_yield();
			continue;
		}

		/*
		 * If the item is being initialized, take a reference
		 * and wait.
		 */
		if (tmp->fcmh_flags & FCMH_CAC_INITING) {
			psc_hashbkt_unlock(b);
			tmp->fcmh_flags |= FCMH_CAC_WAITING;
			fcmh_op_start_type(tmp, FCMH_OPCNT_WAIT);
			fcmh_wait_nocond_locked(tmp);
			fcmh_op_done_type(tmp, FCMH_OPCNT_WAIT);
			goto restart;
		}
		fcmh = tmp;
		break;
	}

	/*
	 * If the above lookup is a success, we hold the lock, but we
	 * haven't take a reference yet.  Also, we need to keep the
	 * bucket lock in case we need to insert a new item.
	 */
	if (fcmh) {
		psc_hashbkt_unlock(b);
		/*
		 * Test to see if we jumped here from fidcFreeList.
		 * Note an unlucky thread could find that the fid does
		 * not exist before allocation and exist after that.
		 */
		if (try_create) {
			fcmh_put(fcmh_new);
			fcmh_new = NULL;
		}

		psc_assert(fgp->fg_fid == fcmh_2_fid(fcmh));
		/* apply provided attributes to the cache */
		if (sstb)
			fcmh_setattrf(fcmh, sstb, setattrflags |
			    FCMH_SETATTRF_HAVELOCK);

		/* keep me around after unlocking later */
		fcmh_op_start_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

		if (sl_fcmh_ops.sfop_modify)
			sl_fcmh_ops.sfop_modify(fcmh, fgp);

		FCMH_ULOCK(fcmh);
		*fp = fcmh;
		return (0);
	}

	/* We have failed to find a match in the cache */
	if (flags & FIDC_LOOKUP_CREATE) {
		if (!try_create) {
			psc_hashbkt_unlock(b);
			fcmh_new = fcmh_get();
			try_create = 1;
			goto restart;
		}
	} else {
		/*
		 * FIDC_LOOKUP_CREATE was not specified and the fcmh is
		 * not present.
		 */
		psc_hashbkt_unlock(b);
		return (ENOENT);
	}

	/*
	 * OK, we've got a new fcmh.  No need to lock it since it's not
	 * yet visible to other threads.
	 */
	fcmh = fcmh_new;

	memset(fcmh, 0, fidcPoolMaster.pms_entsize);
	INIT_PSC_LISTENTRY(&fcmh->fcmh_lentry);
	SPLAY_INIT(&fcmh->fcmh_bmaptree);
	INIT_SPINLOCK(&fcmh->fcmh_lock);
	psc_hashent_init(&fidcHtable, fcmh);
	psc_waitq_init(&fcmh->fcmh_waitq);

	COPYFG(&fcmh->fcmh_fg, fgp);
	fcmh_op_start_type(fcmh, FCMH_OPCNT_NEW);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "new fcmh");

	/*
	 * Add the new item to the hash list, but mark it as INITING.
	 * If we fail to initialize it, we should mark it as TOFREE and
	 * leave it around for the reaper to free it.  Note that the
	 * item is not on any list yet.
	 */
	fcmh->fcmh_flags |= FCMH_CAC_INITING;
	if (flags & FIDC_LOOKUP_RLSBMAP)
		fcmh->fcmh_flags |= FCMH_CAC_RLSBMAP;
	psc_hashbkt_add_item(&fidcHtable, b, fcmh);
	psc_hashbkt_unlock(b);

	/*
	 * Call service specific constructor slm_fcmh_ctor(),
	 * sli_fcmh_ctor(), and slc_fcmh_ctor() to initialize their
	 * private fields that follow the main fcmh structure.  It is
	 * safe to not lock because we don't touch the state, and other
	 * thread should be waiting for us.
	 */
	if (sstb) {
		FCMH_LOCK(fcmh);
		fcmh_setattrf(fcmh, sstb, setattrflags |
		    FCMH_SETATTRF_HAVELOCK);
		rc = sl_fcmh_ops.sfop_ctor(fcmh, flags);
		if (rc)
			fcmh->fcmh_flags |= FCMH_CTOR_FAILED;
		goto finish;
	} else {
		rc = sl_fcmh_ops.sfop_ctor(fcmh, flags);
		if (rc) {
			fcmh->fcmh_flags |= FCMH_CTOR_FAILED;
			goto finish;
		}
	}

	if (flags & FIDC_LOOKUP_LOAD) {
		psc_assert(sl_fcmh_ops.sfop_getattr);
		rc = sl_fcmh_ops.sfop_getattr(fcmh, arg);	/* msl_stat() */
	}

 finish:
	(void)FCMH_RLOCK(fcmh);
	fcmh->fcmh_flags &= ~(FCMH_CAC_INITING | FCMH_CAC_RLSBMAP);
	if (fcmh->fcmh_flags & FCMH_CAC_WAITING) {
		fcmh->fcmh_flags &= ~FCMH_CAC_WAITING;
		psc_waitq_wakeall(&fcmh->fcmh_waitq);
	}

	fcmh->fcmh_flags |= FCMH_CAC_IDLE;
	lc_add(&fidcIdleList, fcmh);

	if (rc) {
		fcmh->fcmh_flags |= FCMH_CAC_TOFREE;
		fcmh_op_done_type(fcmh, FCMH_OPCNT_NEW);
	} else {
		*fp = fcmh;
		fcmh_op_start_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
		fcmh_op_done_type(fcmh, FCMH_OPCNT_NEW);
	}
	return (rc);
}

/**
 * fidc_init - Initialize the FID cache.
 */
void
fidc_init(int privsiz, int nobj)
{
	_psc_poolmaster_init(&fidcPoolMaster,
	    sizeof(struct fidc_membh) + privsiz,
	    offsetof(struct fidc_membh, fcmh_lentry),
	    PPMF_AUTO, nobj, nobj, 0, NULL,
	    NULL, fidc_reap, NULL, "fcmh");
	fidcPool = psc_poolmaster_getmgr(&fidcPoolMaster);

	lc_reginit(&fidcBusyList, struct fidc_membh,
	    fcmh_lentry, "fcmhbusy");
	lc_reginit(&fidcIdleList, struct fidc_membh,
	    fcmh_lentry, "fcmhidle");

	psc_hashtbl_init(&fidcHtable, 0, struct fidc_membh,
	    fcmh_fg, fcmh_hentry, nobj * 2, NULL, "fidc");
}

ssize_t
fcmh_getsize(struct fidc_membh *h)
{
	ssize_t size;
	int locked;

	locked = reqlock(&h->fcmh_lock);
	size = fcmh_2_fsz(h);
	ureqlock(&h->fcmh_lock, locked);
	return (size);
}

/**
 * _fcmh_op_start_type: We only move a cache item to the busy list if we
 *	know that the reference being taken is a long one.  For
 *	short-lived references, we avoid moving the cache item around.
 *	Also, we only move a cache item back to the idle list when the
 *	_last_ reference is dropped.
 */
void
_fcmh_op_start_type(const struct pfl_callerinfo *pci,
    struct fidc_membh *f, int type)
{
	int locked;

	locked = FCMH_RLOCK(f);
	psc_assert(f->fcmh_refcnt >= 0);
	f->fcmh_refcnt++;

	DEBUG_FCMH(PLL_DEBUG, f, "took ref (type=%d)", type);

	/*
	 * Only 2 types of references may be long standing,
	 * FCMH_OPCNT_OPEN and FCMH_OPCNT_BMAP.  Other ref types should
	 * not move the fcmh to the busy list.
	 */
	if ((type == FCMH_OPCNT_OPEN || type == FCMH_OPCNT_BMAP) &&
	    f->fcmh_flags & FCMH_CAC_IDLE) {
		f->fcmh_flags &= ~FCMH_CAC_IDLE;
		f->fcmh_flags |= FCMH_CAC_BUSY;
		lc_remove(&fidcIdleList, f);
		lc_add(&fidcBusyList, f);
	}
	FCMH_URLOCK(f, locked);
}

void
_fcmh_op_done_type(const struct pfl_callerinfo *pci,
    struct fidc_membh *f, int type)
{
	int rc;

	(void)FCMH_RLOCK(f);
	rc = f->fcmh_refcnt--;
	psc_assert(rc > 0);
	DEBUG_FCMH(PLL_DEBUG, f, "release ref (type=%d)", type);
	if (rc == 1) {
		psc_assert(!FCMH_HAS_BUSY(f));

		/*
		 * If we fail to initialize a fcmh, free it now.
		 * Note that the reaper won't run if there is no
		 * memory pressure, and a deprecated fcmh will
		 * cause us to spin on it.
		 */
		if (f->fcmh_flags & FCMH_CTOR_FAILED) {
			/*
			 * This won't race with _fidc_lookup because
			 * _fidc_lookup holds the bucket lock which this
			 * thread takes in psc_hashent_remove().  So
			 * _fidc_lookup is guaranteed to obtain this
			 * fcmh lock and skip the fcmh because of
			 * FCMH_CAC_TOFREE before this thread calls
			 * fcmh_destroy().
			 */
			lc_remove(&fidcIdleList, f);
			f->fcmh_flags |= FCMH_CAC_TOFREE;
			FCMH_ULOCK(f);

			psc_hashent_remove(&fidcHtable, f);
			fcmh_destroy(f);
			return;
		}

		if (f->fcmh_flags & FCMH_CAC_BUSY) {
			f->fcmh_flags &= ~FCMH_CAC_BUSY;
			f->fcmh_flags |= FCMH_CAC_IDLE;
			lc_remove(&fidcBusyList, f);
			lc_add(&fidcIdleList, f);
		}
	}
	fcmh_wake_locked(f);
	FCMH_ULOCK(f);
}

#if PFL_DEBUG > 0
void
dump_fcmh(struct fidc_membh *f)
{
	int locked;

	locked = FCMH_RLOCK(f);
	DEBUG_FCMH(PLL_MAX, f, "");
	FCMH_URLOCK(f, locked);
}

void
dump_fidcache(void)
{
	struct psc_hashbkt *bkt;
	struct fidc_membh *tmp;

	PSC_HASHTBL_FOREACH_BUCKET(bkt, &fidcHtable) {
		psc_hashbkt_lock(bkt);
		PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, tmp, bkt)
			dump_fcmh(tmp);
		psc_hashbkt_unlock(bkt);
	}
}

void
_dump_fcmh_flags_common(int *flags, int *seq)
{
	PFL_PRFLAG(FCMH_CAC_IDLE, flags, seq);
	PFL_PRFLAG(FCMH_CAC_BUSY, flags, seq);
	PFL_PRFLAG(FCMH_CAC_INITING, flags, seq);
	PFL_PRFLAG(FCMH_CAC_WAITING, flags, seq);
	PFL_PRFLAG(FCMH_CAC_TOFREE, flags, seq);
	PFL_PRFLAG(FCMH_CAC_REAPED, flags, seq);
	PFL_PRFLAG(FCMH_CAC_RLSBMAP, flags, seq);
	PFL_PRFLAG(FCMH_HAVE_ATTRS, flags, seq);
	PFL_PRFLAG(FCMH_GETTING_ATTRS, flags, seq);
	PFL_PRFLAG(FCMH_CTOR_FAILED, flags, seq);
	PFL_PRFLAG(FCMH_NO_BACKFILE, flags, seq);
	PFL_PRFLAG(FCMH_BUSY, flags, seq);
}

__weak void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	if (flags)
		printf(" unknown: %x", flags);
	printf("\n");
}
#endif
