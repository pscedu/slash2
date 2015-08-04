/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
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
#include "slconfig.h"
#include "slutil.h"

struct psc_poolmaster	  fidcPoolMaster;
struct psc_poolmgr	 *fidcPool;
struct psc_listcache	  fidcIdleList;		/* identity untouched, but reapable */
struct psc_hashtbl	  fidcHtable;

#define	fcmh_get()	psc_pool_get(fidcPool)
#define	fcmh_put(f)	psc_pool_return(fidcPool, (f))

unsigned long fcmh_done_type[FCMH_OPCNT_MAXTYPE+1];
unsigned long fcmh_start_type[FCMH_OPCNT_MAXTYPE+1];

/*
 * Destructor for FID cache member handles.
 * @f: fcmh being destroyed.
 */
void
fcmh_destroy(struct fidc_membh *f)
{
	psc_assert(RB_EMPTY(&f->fcmh_bmaptree));
	psc_assert(f->fcmh_refcnt == 0);
	psc_assert(psc_hashent_disjoint(&fidcHtable, f));
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
	fcmh_put(f);
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

	LIST_CACHE_LOCK(&fidcIdleList);
	LIST_CACHE_FOREACH_SAFE(f, tmp, &fidcIdleList) {
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
		lc_remove(&fidcIdleList, f);
		reap[nreap++] = f;
		FCMH_ULOCK(f);

		if (nreap >= max)
			break;
	}
	LIST_CACHE_ULOCK(&fidcIdleList);

	psclog_debug("reaping %d files from fidcache", nreap);

	for (i = 0; i < nreap; i++) {
		psc_hashent_remove(&fidcHtable, reap[i]);
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
 * @fgp: FID and GEN #.
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
_fidc_lookup(const struct pfl_callerinfo *pci,
    const struct sl_fidgen *fgp, int flags, struct fidc_membh **fp,
    void *arg)
{
	struct fidc_membh *tmp, *f, *fnew;
	struct psc_hashbkt *b;
	int rc = 0, try_create = 0;

	psclog_debug("fidc_lookup called for fid "SLPRI_FID,
	    fgp->fg_fid);

	*fp = NULL;
	fnew = NULL; /* gcc */

	/* sanity checks */
#ifndef _SLASH_CLIENT
	psc_assert(!(flags & FIDC_LOOKUP_EXCL));
#endif

	/* OK.  Now check if it is already in the cache. */
	b = psc_hashbkt_get(&fidcHtable, &fgp->fg_fid);
 restart:
	f = NULL;
	PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, tmp, b) {
		/*
		 * Note that generation number is only used to track
		 *   truncations.
		 */
		if (fgp->fg_fid != fcmh_2_fid(tmp))
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
		psc_hashbkt_put(&fidcHtable, b);

		/*
		 * Test to see if we jumped here from fidcIdleList.
		 * Note an unlucky thread could find that the FID does
		 * not exist before allocation and exist after that.
		 */
		if (try_create) {
			fcmh_put(fnew);
			fnew = NULL;
		}

		psc_assert(fgp->fg_fid == fcmh_2_fid(f));

		/* keep me around after unlocking later */
		fcmh_op_start_type(f, FCMH_OPCNT_LOOKUP_FIDC);

		/* call sli_fcmh_reopen() sliod only */
		if (sl_fcmh_ops.sfop_modify)
			rc = sl_fcmh_ops.sfop_modify(f, fgp);
		if (rc)
			fcmh_op_done_type(f, FCMH_OPCNT_LOOKUP_FIDC);
		else {
			FCMH_ULOCK(f);
			*fp = f;
		}
		return (rc);
	}

	/* We have failed to find a match in the cache */
	if (flags & FIDC_LOOKUP_CREATE) {
		if (!try_create) {
			psc_hashbkt_unlock(b);

			fnew = fcmh_get();
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
		psc_hashbkt_put(&fidcHtable, b);
		return (ENOENT);
	}

	/*
	 * OK, we've got a new fcmh.  No need to lock it since it's not
	 * yet visible to other threads.
	 */
	f = fnew;

	memset(f, 0, fidcPoolMaster.pms_entsize);
	INIT_PSC_LISTENTRY(&f->fcmh_lentry);
	RB_INIT(&f->fcmh_bmaptree);
	INIT_SPINLOCK(&f->fcmh_lock);
	psc_hashent_init(&fidcHtable, f);
	psc_waitq_init(&f->fcmh_waitq);
	pfl_rwlock_init(&f->fcmh_rwlock);

	COPYFG(&f->fcmh_fg, fgp);
	fcmh_op_start_type(f, FCMH_OPCNT_NEW);

	DEBUG_FCMH(PLL_DEBUG, f, "new");

	/*
	 * Add the new item to the hash list, but mark it as INITING.
	 * If we fail to initialize it, we should mark it as TOFREE and
	 * leave it around for the reaper to free it.  Note that the
	 * item is not on any list yet.
	 */
	f->fcmh_flags |= FCMH_INITING;
	psc_hashbkt_add_item(&fidcHtable, b, f);
	psc_hashbkt_put(&fidcHtable, b);

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
	fcmh_op_done_type(f, FCMH_OPCNT_NEW);
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

	_psc_poolmaster_init(&fidcPoolMaster,
	    sizeof(struct fidc_membh) + privsiz,
	    offsetof(struct fidc_membh, fcmh_lentry),
	    PPMF_AUTO, nobj, nobj, 0, NULL,
	    NULL, fidc_reaper, NULL, "fcmh");
	fidcPool = psc_poolmaster_getmgr(&fidcPoolMaster);

	lc_reginit(&fidcIdleList, struct fidc_membh, fcmh_lentry,
	    "fcmhidle");

	psc_hashtbl_init(&fidcHtable, 0, struct fidc_membh, fcmh_fg,
	    fcmh_hentry, 3 * nobj - 1, NULL, "fidc");
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

	fcmh_start_type[type]++;
	fcmh_start_type[FCMH_OPCNT_MAXTYPE]++;

	locked = FCMH_RLOCK(f);
	psc_assert(f->fcmh_refcnt >= 0);
	f->fcmh_refcnt++;

	DEBUG_FCMH(PLL_DEBUG, f, "took ref (type=%d)", type);

	if (f->fcmh_flags & FCMH_IDLE) {
		f->fcmh_flags &= ~FCMH_IDLE;
		lc_remove(&fidcIdleList, f);
	}
	FCMH_URLOCK(f, locked);
}

void
_fcmh_op_done_type(const struct pfl_callerinfo *pci,
    struct fidc_membh *f, int type)
{
	int rc;

	fcmh_done_type[type]++;
	fcmh_done_type[FCMH_OPCNT_MAXTYPE]++;

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

			psc_hashent_remove(&fidcHtable, f);
			fcmh_destroy(f);
			return;
		}

		psc_assert(!(f->fcmh_flags & FCMH_IDLE));
		f->fcmh_flags |= FCMH_IDLE;
		lc_add(&fidcIdleList, f);
		PFL_GETTIMESPEC(&f->fcmh_etime);
		f->fcmh_etime.tv_sec += MAX_FCMH_LIFETIME;
	}
	fcmh_wake_locked(f);
	FCMH_ULOCK(f);
}

void
sl_freapthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		while (fidc_reap(0, 1))
			;
		sleep(MAX_FCMH_LIFETIME);
	}
}

void
sl_freapthr_spawn(int thrtype, const char *name)
{
	pscthr_init(thrtype, sl_freapthr_main, NULL, 0, name);
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
