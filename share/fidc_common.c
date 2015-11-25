/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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
#include "slconfig.h"
#include "slutil.h"

struct psc_poolmaster	  sl_fcmh_poolmaster;
struct psc_poolmgr	 *sl_fcmh_pool;
struct psc_listcache	  sl_fcmh_idle;		/* identity untouched, but reapable */
struct psc_hashtbl	  sl_fcmh_hashtbl;
struct psc_thread	 *sl_freapthr;
struct psc_waitq	  sl_freap_waitq = PSC_WAITQ_INIT;

#if PFL_DEBUG > 0
psc_spinlock_t		fcmh_ref_lock = SPINLOCK_INIT;
unsigned long		fcmh_done_type[FCMH_OPCNT_MAXTYPE + 1];
unsigned long		fcmh_start_type[FCMH_OPCNT_MAXTYPE + 1];
#endif

/*
 * Destructor for FID cache member handles.
 * @f: fcmh being destroyed.
 */
void
fcmh_destroy(struct fidc_membh *f)
{
	psc_assert(RB_EMPTY(&f->fcmh_bmaptree));
	psc_assert(f->fcmh_refcnt == 0);
	psc_assert(psc_hashent_disjoint(&sl_fcmh_hashtbl, f));
	psc_assert(!psc_waitq_nwaiters(&f->fcmh_waitq));

	psc_waitq_destroy(&f->fcmh_waitq);

	/* slc_fcmh_dtor(), slm_fcmh_dtor(), sli_fcmh_dtor() */
	if (sl_fcmh_ops.sfop_dtor) {
		if (f->fcmh_flags & FCMH_CTOR_FAILED)
			DEBUG_FCMH(PLL_INFO, f,
			    "bypassing dtor() call");
		else
			sl_fcmh_ops.sfop_dtor(f);
	}

	f->fcmh_flags = FCMH_FREE;
	psc_pool_return(sl_fcmh_pool, f);
}

#define FCMH_MAX_REAP 128

/*
 * Reap some files from the fidcache.
 * @max: max number of objects to reap.
 * @only_expired: whether to restrict reaping to only expired files.
 */
int
fidc_reap(int max, int only_expired)
{
	struct fidc_membh *f, *tmp, *reap[FCMH_MAX_REAP];
	struct timespec crtime;
	int i, nreap = 0;

	if (!max || max > FCMH_MAX_REAP)
		max = FCMH_MAX_REAP;

	LIST_CACHE_LOCK(&sl_fcmh_idle);
	LIST_CACHE_FOREACH_SAFE(f, tmp, &sl_fcmh_idle) {
		/* never reap root (/) */
		if ((FID_GET_INUM(fcmh_2_fid(f))) == SLFID_ROOT)
			continue;

		if (!FCMH_TRYLOCK(f))
			continue;

		psc_assert(!f->fcmh_refcnt);

		if (only_expired) {
			PFL_GETTIMESPEC(&crtime);
			if (timespeccmp(&crtime, &f->fcmh_etime, <)) {
				FCMH_ULOCK(f);
				continue;
			}
		}

		psc_assert(f->fcmh_flags & FCMH_IDLE);
		DEBUG_FCMH(PLL_DEBUG, f, "reaped");

		f->fcmh_flags |= FCMH_TOFREE;
		lc_remove(&sl_fcmh_idle, f);
		reap[nreap++] = f;
		FCMH_ULOCK(f);

		if (nreap >= max)
			break;
	}
	LIST_CACHE_ULOCK(&sl_fcmh_idle);

	psclog_debug("reaping %d files from fidcache", nreap);

	for (i = 0; i < nreap; i++) {
		psc_hashent_remove(&sl_fcmh_hashtbl, reap[i]);
		fcmh_destroy(reap[i]);
	}
	return (i);
}

/*
 * Reap some fcmhs from the idle list due to memory pressure.
 */
int
fidc_reaper(struct psc_poolmgr *m)
{
	return (fidc_reap(psc_atomic32_read(&m->ppm_nwaiters), 0));
}

/*
 * Search the FID cache for a member by its FID, optionally creating it.
 *
 * @pci: thread caller information.
 * @fid: FID.
 * @fgen: file GEN #.
 * @flags: access flags.
 * @fp: value-result fcmh return.
 * @arg: argument to GETATTR.
 *
 * Note: Newly acquired fcmh's are ref'd with FCMH_OPCNT_NEW, reused
 * ones are ref'd with FCMH_OPCNT_LOOKUP_FIDC.
 *
 * Note: Returns positive errno.
 */
int
_fidc_lookup(const struct pfl_callerinfo *pci, slfid_t fid,
    slfgen_t fgen, int flags, struct fidc_membh **fp, void *arg)
{
	struct fidc_membh *tmp, *f, *fnew;
	struct psc_hashbkt *b;
	int rc = 0, try_create = 0;

	psclog_debug("fidc_lookup called for fid "SLPRI_FID" "
	    "gen "SLPRI_FGEN, fid, fgen);

	*fp = NULL;
	fnew = NULL; /* gcc */

	/* sanity checks */
#ifndef _SLASH_CLIENT
	psc_assert(!(flags & FIDC_LOOKUP_EXCL));
#endif

	/* OK.  Now check if it is already in the cache. */
	b = psc_hashbkt_get(&sl_fcmh_hashtbl, &fid);
 restart:
	f = NULL;
	PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, tmp, b) {
		/*
		 * Note that generation number is only used to track
		 * truncations.
		 */
		if (fid != fcmh_2_fid(tmp))
			continue;
		FCMH_LOCK(tmp);

		/* if the item is being freed, ignore it */
		if (tmp->fcmh_flags & FCMH_TOFREE) {
			FCMH_ULOCK(tmp);
			pscthr_yield();
			continue;
		}

		/*
		 * If the item is being initialized, take a reference
		 * and wait.
		 */
		if (tmp->fcmh_flags & FCMH_INITING) {
			psc_hashbkt_unlock(b);

			tmp->fcmh_flags |= FCMH_WAITING;
			fcmh_op_start_type(tmp, FCMH_OPCNT_WAIT);
			fcmh_wait_nocond_locked(tmp);
			fcmh_op_done_type(tmp, FCMH_OPCNT_WAIT);

			psc_hashbkt_lock(b);
			goto restart;
		}
		f = tmp;
		break;
	}

	/*
	 * If the above lookup is a success, we hold the lock, but we
	 * haven't taken a reference yet.  Also, we need to keep the
	 * bucket lock in case we need to insert a new item.
	 */
	if (f) {
		/*
		 * Test to see if we jumped here from sl_fcmh_idle.
		 * Note an unlucky thread could find that the FID does
		 * not exist before allocation and exist after that.
		 */
		if (try_create) {
			psc_pool_return(sl_fcmh_pool, fnew);
			fnew = NULL;
		}

		psc_assert(fid == fcmh_2_fid(f));

		/* keep me around after unlocking later */
		fcmh_op_start_type(f, FCMH_OPCNT_LOOKUP_FIDC);

		psc_hashbkt_put(&sl_fcmh_hashtbl, b);

		/* call sli_fcmh_reopen() sliod only */
		if (sl_fcmh_ops.sfop_modify)
			rc = sl_fcmh_ops.sfop_modify(f, fgen);
		if (rc)
			fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
		else {
			if ((flags & FIDC_LOOKUP_LOCK) == 0)
				FCMH_ULOCK(f);
			*fp = f;
		}
		return (rc);
	}

	/* We have failed to find a match in the cache */
	if (flags & FIDC_LOOKUP_CREATE) {
		if (!try_create) {
			psc_hashbkt_unlock(b);

			fnew = psc_pool_get(sl_fcmh_pool);
			try_create = 1;

			psc_hashbkt_lock(b);
			goto restart;
		}
		/* fall through if no one else has created the same fcmh */
	} else {
		/*
		 * FIDC_LOOKUP_CREATE was not specified and the fcmh is
		 * not present.
		 */
		psc_hashbkt_put(&sl_fcmh_hashtbl, b);
		return (ENOENT);
	}

	/*
	 * OK, we've got a new fcmh.  No need to lock it since it's not
	 * yet visible to other threads.
	 */
	f = fnew;

	memset(f, 0, sl_fcmh_poolmaster.pms_entsize);
	INIT_PSC_LISTENTRY(&f->fcmh_lentry);
	RB_INIT(&f->fcmh_bmaptree);
	INIT_SPINLOCK(&f->fcmh_lock);
	psc_hashent_init(&sl_fcmh_hashtbl, f);
	psc_waitq_init(&f->fcmh_waitq);
	pfl_rwlock_init(&f->fcmh_rwlock);

	f->fcmh_fg.fg_fid = fid;
	f->fcmh_fg.fg_gen = fgen;
	fcmh_op_start_type(f, FCMH_OPCNT_NEW);

	DEBUG_FCMH(PLL_DEBUG, f, "new");

	/*
	 * Add the new item to the hash list, but mark it as INITING.
	 * If we fail to initialize it, we should mark it as TOFREE and
	 * leave it around for the reaper to free it.  Note that the
	 * item is not on any list yet.
	 */
	f->fcmh_flags |= FCMH_INITING;
	psc_hashbkt_add_item(&sl_fcmh_hashtbl, b, f);
	psc_hashbkt_put(&sl_fcmh_hashtbl, b);

	/*
	 * Call service specific constructor slm_fcmh_ctor(),
	 * sli_fcmh_ctor(), and slc_fcmh_ctor() to initialize their
	 * private fields that follow the main fcmh structure.  It is
	 * safe to not lock because we don't touch the state, and other
	 * thread should be waiting for us.
	 */
	rc = sl_fcmh_ops.sfop_ctor(f, flags);
	if (rc) {
		f->fcmh_flags |= FCMH_CTOR_FAILED;
		goto finish;
	}

	if (flags & FIDC_LOOKUP_LOAD) {
		psc_assert(sl_fcmh_ops.sfop_getattr);
		rc = sl_fcmh_ops.sfop_getattr(f, arg);	/* msl_stat() */
	}

 finish:
	FCMH_LOCK(f);
	f->fcmh_flags &= ~FCMH_INITING;
	if (f->fcmh_flags & FCMH_WAITING) {
		f->fcmh_flags &= ~FCMH_WAITING;
		psc_waitq_wakeall(&f->fcmh_waitq);
	}

	if (rc) {
		f->fcmh_flags |= FCMH_TOFREE;
	} else {
		*fp = f;
		fcmh_op_start_type(f, FCMH_OPCNT_LOOKUP_FIDC);
	}
	_fcmh_op_done_type(PFL_CALLERINFOSS(SLSS_FCMH), f,
	    FCMH_OPCNT_NEW, flags & FIDC_LOOKUP_LOCK);
	return (rc);
}

/*
 * Initialize the FID cache.
 */
void
fidc_init(int privsiz)
{
	int nobj;

	nobj = slcfg_local->cfg_fidcachesz;

	_psc_poolmaster_init(&sl_fcmh_poolmaster,
	    sizeof(struct fidc_membh) + privsiz,
	    offsetof(struct fidc_membh, fcmh_lentry),
	    PPMF_AUTO, nobj, nobj, 0, NULL,
	    NULL, fidc_reaper, NULL, "fcmh");
	sl_fcmh_pool = psc_poolmaster_getmgr(&sl_fcmh_poolmaster);

	lc_reginit(&sl_fcmh_idle, struct fidc_membh, fcmh_lentry,
	    "fcmhidle");

	psc_hashtbl_init(&sl_fcmh_hashtbl, 0, struct fidc_membh,
	    fcmh_fg, fcmh_hentry, 3 * nobj - 1, NULL, "fidc");
}

void
fidc_destroy(void)
{
	psc_hashtbl_destroy(&sl_fcmh_hashtbl);
	lc_unregister(&sl_fcmh_idle);
	pfl_listcache_destroy(&sl_fcmh_idle);
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

/*
 * We only move a cache item to the busy list if we know that the
 * reference being taken is a long one.  For short-lived references, we
 * avoid moving the cache item around.  Also, we only move a cache item
 * back to the idle list when the _last_ reference is dropped.
 */
void
_fcmh_op_start_type(const struct pfl_callerinfo *pci,
    struct fidc_membh *f, int type)
{
	int locked;

#if PFL_DEBUG > 0
	spinlock(&fcmh_ref_lock);
	fcmh_start_type[type]++;
	fcmh_start_type[FCMH_OPCNT_MAXTYPE]++;
	freelock(&fcmh_ref_lock);
#endif

	locked = FCMH_RLOCK(f);
	psc_assert(f->fcmh_refcnt >= 0);
	f->fcmh_refcnt++;

	DEBUG_FCMH(PLL_DEBUG, f, "took ref (type=%d)", type);

	if (f->fcmh_flags & FCMH_IDLE) {
		f->fcmh_flags &= ~FCMH_IDLE;
		lc_remove(&sl_fcmh_idle, f);
	}
	FCMH_URLOCK(f, locked);
}

void
_fcmh_op_done_type(const struct pfl_callerinfo *pci,
    struct fidc_membh *f, int type, int keep_locked)
{
	int rc;

#if PFL_DEBUG > 0
	spinlock(&fcmh_ref_lock);
	fcmh_done_type[type]++;
	fcmh_done_type[FCMH_OPCNT_MAXTYPE]++;
	freelock(&fcmh_ref_lock);
#endif

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
			 * FCMH_TOFREE before this thread calls
			 * fcmh_destroy().
			 */
			f->fcmh_flags |= FCMH_TOFREE;
			FCMH_ULOCK(f);

			psc_hashent_remove(&sl_fcmh_hashtbl, f);
			fcmh_destroy(f);
			return;
		}

		psc_assert(!(f->fcmh_flags & FCMH_IDLE));
		f->fcmh_flags |= FCMH_IDLE;
		lc_add(&sl_fcmh_idle, f);
		PFL_GETTIMESPEC(&f->fcmh_etime);
		f->fcmh_etime.tv_sec += MAX_FCMH_LIFETIME;
	}
	fcmh_wake_locked(f);
	if (!keep_locked)
		FCMH_ULOCK(f);
}

void
sl_freapthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		while (fidc_reap(0, 1))
			;
		psc_waitq_waitrel_s(&sl_freap_waitq, NULL,
		    MAX_FCMH_LIFETIME);
	}
}

void
sl_freapthr_spawn(int thrtype, const char *name)
{
	sl_freapthr = pscthr_init(thrtype, sl_freapthr_main, NULL, 0,
	    name);
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

	PSC_HASHTBL_FOREACH_BUCKET(bkt, &sl_fcmh_hashtbl) {
		psc_hashbkt_lock(bkt);
		PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, tmp, bkt)
			dump_fcmh(tmp);
		psc_hashbkt_unlock(bkt);
	}
}

void
_dump_fcmh_flags_common(int *flags, int *seq)
{
	PFL_PRFLAG(FCMH_FREE, flags, seq);
	PFL_PRFLAG(FCMH_IDLE, flags, seq);
	PFL_PRFLAG(FCMH_INITING, flags, seq);
	PFL_PRFLAG(FCMH_WAITING, flags, seq);
	PFL_PRFLAG(FCMH_TOFREE, flags, seq);
	PFL_PRFLAG(FCMH_HAVE_ATTRS, flags, seq);
	PFL_PRFLAG(FCMH_GETTING_ATTRS, flags, seq);
	PFL_PRFLAG(FCMH_CTOR_FAILED, flags, seq);
	PFL_PRFLAG(FCMH_BUSY, flags, seq);
	PFL_PRFLAG(FCMH_DELETED, flags, seq);
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
