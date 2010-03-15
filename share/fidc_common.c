/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <pthread.h>
#include <stdio.h>

#include <fuse/fuse_lowlevel.h>

#include "pfl/cdefs.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/atomic.h"
#include "psc_util/pool.h"

#include "bmap.h"
#include "buffer.h"
#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"
#include "slutil.h"

int  (*fidcReapCb)(struct fidc_membh *);

struct psc_poolmaster	 fidcPoolMaster;
struct psc_poolmgr	*fidcPool;
struct psc_listcache	 fidcDirtyList;
struct psc_listcache	 fidcCleanList;
struct psc_hashtbl	 fidcHtable;

/**
 * fcmh_dtor - Destructor for FID cache member handles.
 * @r: fcmh being destroyed.
 */
void
fcmh_destroy(struct fidc_membh *f)
{
	/* mark the item as free, we will re-init fields when it is reused */
	f->fcmh_state = FCMH_CAC_FREE;

	psc_assert(SPLAY_EMPTY(&f->fcmh_bmaptree));
	psc_assert(!psc_waitq_nwaiters(&f->fcmh_waitq));
	psc_assert(f->fcmh_refcnt == 1);
	psc_assert(psc_hashent_disjoint(&fidcHtable, &f->fcmh_hentry));

	if (sl_fcmh_ops.sfop_dtor)
		sl_fcmh_ops.sfop_dtor(f);

	psc_pool_return(fidcPool, f);
}

/**
 * fcmh_get - Grab/Allocate a clean/unused FID cache member handle from
 *	the pool.
 */
struct fidc_membh *
fcmh_get(void)
{
	struct fidc_membh *f;

	f = psc_pool_get(fidcPool);
	memset(f, 0, fidcPoolMaster.pms_entsize);
	SPLAY_INIT(&f->fcmh_bmaptree);
	LOCK_INIT(&f->fcmh_lock);
	psc_waitq_init(&f->fcmh_waitq);
	fcmh_op_start_type(f, FCMH_OPCNT_NEW);
	return (f);
}

void
fcmh_setattr(struct fidc_membh *fcmh, const struct srt_stat *sstb,
    int flags)
{
	int locked = reqlock(&fcmh->fcmh_lock);
	uint64_t size = 0;

#if 0
	/* right now, we allow item with FIGGEN_ANY into cache, and
	 * cache lookup will trigger a getattr on the client, which
	 * lead to us.  So we can't assert here.
	 */
	psc_assert(fcmh_2_gen(fcmh) != FIDGEN_ANY);
#endif
	psc_assert(sstb->sst_ino == (ino_t)fcmh->fcmh_fg.fg_fid);

	if ((flags & FCMH_SETATTRF_SAVESIZE) &&
	    fcmh_2_ptruncgen(fcmh) >= sstb->sst_ptruncgen)
		size = fcmh_2_fsz(fcmh);

	fcmh->fcmh_sstb = *sstb;
	fcmh_2_gen(fcmh) = sstb->sst_gen;
	fcmh_refresh_age(fcmh);

	if (size)
		fcmh_2_fsz(fcmh) = size;

	if (fcmh->fcmh_state & FCMH_GETTING_ATTRS) {
		fcmh->fcmh_state &= ~FCMH_GETTING_ATTRS;
		fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
		psc_waitq_wakeall(&fcmh->fcmh_waitq);
	} else
		psc_assert(fcmh->fcmh_state & FCMH_HAVE_ATTRS);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attr set");
	ureqlock(&fcmh->fcmh_lock, locked);
}

/**
 * fidc_reap - reap some inodes from the clean list.
 */
int
fidc_reap(struct psc_poolmgr *m)
{
	struct fidc_membh *f, *tmp;
	struct psc_dynarray da = DYNARRAY_INIT;
	struct psc_hashbkt *b;
	int i=0;

	psc_assert(m == fidcPool);
 startover:
	LIST_CACHE_LOCK(&fidcCleanList);
	LIST_CACHE_FOREACH_SAFE(f, tmp, &fidcCleanList) {
		DEBUG_FCMH(PLL_INFO, f, "considering for reap");

		if (psclg_size(&m->ppm_lg) + psc_dynarray_len(&da) >=
		    atomic_read(&m->ppm_nwaiters) + 1)
			break;

		b = psc_hashbkt_get(&fidcHtable, &fcmh_2_fid(f));
		if (!psc_hashbkt_trylock(b)) {
			LIST_CACHE_ULOCK(&fidcCleanList);
			sched_yield();
			goto startover;
		}

		/* - Inode must be lockable now
		 * - Skip the root inode.
		 * - Clean inodes may have non-zero refcnts,
		 */
		if ((!trylock(&f->fcmh_lock)) ||
		    (fcmh_2_fid(f) == 1) ||
		    (f->fcmh_refcnt))
			goto end2;
		/* Make sure our clean list is 'clean' by
		 *  verifying the following conditions.
		 */
		if (!(f->fcmh_state & FCMH_CAC_CLEAN)) {
			DEBUG_FCMH(PLL_FATAL, f,
			    "Invalid fcmh state for clean list");
			psc_fatalx("Invalid state for clean list");
		}
		/* Skip inodes which already claim to be freeing
		 */
		if (f->fcmh_state & FCMH_CAC_FREEING)
			goto end1;

		/* Call into the system specific 'reap' code.
		 *  On the client this means taking the fcmh from the
		 *  parent directory fcmh.
		 */
		if (!fidcReapCb || fidcReapCb(f)) {
			f->fcmh_state |= FCMH_CAC_FREEING;
			lc_remove(&fidcCleanList, f);
			psc_dynarray_add(&da, f);
		}
 end1:
		freelock(&f->fcmh_lock);
 end2:
		psc_hashbkt_unlock(b);
	}
	LIST_CACHE_ULOCK(&fidcCleanList);

	for (i = 0; i < psc_dynarray_len(&da); i++) {
		f = psc_dynarray_getpos(&da, i);
		DEBUG_FCMH(PLL_DEBUG, f, "moving to free list");
		psc_hashent_remove(&fidcHtable, f);
		fcmh_destroy(f);
	}

	psc_dynarray_free(&da);
	return (i);
}

/**
 * fidc_lookup_fg - Wrapper for fidc_lookup().  Called when the
 *	generation number is known.
 */
struct fidc_membh *
fidc_lookup_fg(const struct slash_fidgen *fg)
{
	int rc;
	struct fidc_membh *fcmhp;

	rc = fidc_lookup(fg, 0, NULL, 0, NULL, &fcmhp);
	return rc == 0 ? fcmhp : NULL;
}

/**
 * fidc_lookup_fid - Wrapper for fidc_lookup().  Called when the
 *	generation number is not known.
 */
struct fidc_membh *
fidc_lookup_fid(slfid_t f)
{
	int rc;
	struct fidc_membh *fcmhp;
	struct slash_fidgen t = { f, FIDGEN_ANY };

	rc = fidc_lookup(&t, 0, NULL, 0, NULL, &fcmhp);
	return rc == 0 ? fcmhp : NULL;

}

/**
 * fidc_lookup - Search the FID cache for a member by its FID,
 *	optionally creating it.
 * Notes:  Newly acquired fcmh's are ref'd with FCMH_OPCNT_NEW, reused ones
 *	are ref'd with FCMH_OPCNT_LOOKUP_FIDC.
 */
int
fidc_lookup(const struct slash_fidgen *fgp, int flags,
    const struct srt_stat *sstb, int setattrflags,
    const struct slash_creds *creds, struct fidc_membh **fcmhp)
{
	int rc, try_create=0;
	struct fidc_membh *tmp, *fcmh, *fcmh_new;
	struct psc_hashbkt *b;
	struct slash_fidgen searchfg = *fgp;

#ifdef DEMOTED_INUM_WIDTHS
	searchfg.fg_fid = (fuse_ino_t)searchfg.fg_fid;
#endif

	rc = 0;
	*fcmhp = NULL;

	fcmh_new = NULL; /* gcc */

	/* sanity checks */
	if (flags & FIDC_LOOKUP_LOAD) {
		psc_assert(creds);
		psc_assert(sstb == NULL);
	}

	if (flags & FIDC_LOOKUP_CREATE)
		psc_assert(sstb || (flags & FIDC_LOOKUP_LOAD));

	if (sstb)
		psc_assert((flags & FIDC_LOOKUP_LOAD) == 0);

	/* first, check if its already in the cache */
	b = psc_hashbkt_get(&fidcHtable, &searchfg.fg_fid);
 restart:
	fcmh = NULL;
	psc_hashbkt_lock(b);
	PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, tmp, b) {
		if (searchfg.fg_fid != fcmh_2_fid(tmp))
			continue;
		FCMH_LOCK(tmp);
		/* Be sure to ignore any inodes which are freeing unless
		 *  we are removing the inode from the cache.
		 *  This is necessary to avoid a deadlock between fidc_reap()
		 *  which has the fcmh_lock before calling fidc_put_locked,
		 *  which calls this function with del==1.  This is described
		 *  in Bug #13.
		 */
		if (tmp->fcmh_state & FCMH_CAC_FREEING) {
			DEBUG_FCMH(PLL_WARN, tmp, "tmp fcmh is FREEING");
			FCMH_ULOCK(tmp);
			sched_yield();
			continue;
		}
		if (tmp->fcmh_state & FCMH_CAC_INITING) {
			psc_hashbkt_unlock(b);
			tmp->fcmh_state |= FCMH_CAC_WAITING;
			fcmh_op_start_type(tmp, FCMH_OPCNT_WAIT);
			psc_waitq_wait(&tmp->fcmh_waitq, &tmp->fcmh_lock);
			fcmh_op_done_type(tmp, FCMH_OPCNT_WAIT);
			goto restart;
		}

		if (searchfg.fg_gen == fcmh_2_gen(tmp)) {
			fcmh = tmp;
			break;
		}
		/* Look for the highest generation number.  */
		if (searchfg.fg_gen == FIDGEN_ANY) {
			if (!fcmh) {
				fcmh = tmp;
				continue;
			}
			if (fcmh_2_gen(fcmh) < fcmh_2_gen(tmp)) {
				FCMH_ULOCK(fcmh);
				fcmh = tmp;
				continue;
			}
		}
		FCMH_ULOCK(tmp);
	}

	/*
	 * If the above lookup is a success, we hold the lock, but
	 * we haven't take a reference yet.  Also, we need to keep
	 * the bucket lock in case we need to insert a new item.
	 */
	if (fcmh) {
		if (flags & FIDC_LOOKUP_REMOVE) {
			/* no reference taken here */
			psc_hashbkt_del_item(&fidcHtable, b, fcmh);
			psc_hashbkt_unlock(b);
			FCMH_ULOCK(fcmh);
			*fcmhp = fcmh;
			return (0);
		}
		psc_hashbkt_unlock(b);
		/*
		 * Test to see if we jumped here from fidcFreeList.
		 * Note an unlucky thread could find that the fid
		 * does not exist before allocation and exist after
		 * that.
		 */
		if (try_create) {
			fcmh_new->fcmh_state = FCMH_CAC_FREEING;
			fcmh_op_done_type(fcmh_new, FCMH_OPCNT_NEW);
			fcmh_new = NULL;			/* defensive */
		}
		if (flags & FIDC_LOOKUP_EXCL) {
			FCMH_ULOCK(fcmh);
			psc_warnx("FID "FIDFMT" already in cache",
			    FIDFMTARGS(fgp));
			return (EEXIST);
		}

#ifdef DEMOTED_INUM_WIDTHS
		/*
		 * Since fuse_ino_t is 'unsigned long', it will be 4
		 * bytes on some architectures.  On these machines,
		 * allow collisions since '(unsigned long)uint64_t var'
		 * will frequently be unequal to 'uint64_t var' uncasted.
		 */
		psc_assert(searchfg.fg_fid ==
		    (uint64_t)(fuse_ino_t)fcmh_2_fid(fcmh));
		if (fgp->fg_fid != fcmh_2_fid(fcmh)) {
			FCMH_ULOCK(fcmh);
			return (ENFILE);
		}
#else
		psc_assert(fgp->fg_fid == fcmh_2_fid(fcmh));
#endif
		/* keep me around after unlocking later */
		fcmh_op_start_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

		/* apply provided attributes to the cache */
		if (sstb)
			fcmh_setattr(fcmh, sstb, setattrflags);

		FCMH_ULOCK(fcmh);
		*fcmhp = fcmh;
		return (0);
	}
	/* we have failed to find a match in the cache */
	if (flags & FIDC_LOOKUP_CREATE) {
		if (!try_create) {
			/* Allocate a fidc handle and attach the
			 *   provided fcmh.
			 */
			psc_hashbkt_unlock(b);
			fcmh_new = fcmh_get();
			try_create = 1;
			goto restart;
		}
	 } else {
		/* FIDC_LOOKUP_CREATE was not specified and the fcmh
		 *  is not present.
		 */
		psc_hashbkt_unlock(b);
		return (ENOENT);
	}

	/* OK, we've got a new fcmh.  No need to lock it since
	 *  it's not yet visible to other threads.
	 */
	fcmh = fcmh_new;

	COPYFG(&fcmh->fcmh_fg, fgp);
#ifdef DEMOTED_INUM_WIDTHS
	COPYFG(&fcmh->fcmh_smallfg, &searchfg);
#endif
	/* Place the fcmh into the cache, note that the fcmh was
	 *  ref'd so no race condition exists here. We need a way
	 * to suppress this warning if on client.
	 */
	if (fcmh_2_gen(fcmh) == FIDGEN_ANY)
		DEBUG_FCMH(PLL_NOTICE, fcmh,
		    "adding FIDGEN_ANY to cache");
	/*
	 * Add the new item to the hash list, but mark it as INITING.
	 * If we fail to initialize it, we should mark it as FREEING.
	 * Also note that the item is not on any other list yet.
	 */
	tmp->fcmh_state |= FCMH_CAC_INITING;
	psc_hashbkt_add_item(&fidcHtable, b, fcmh);
	psc_hashbkt_unlock(b);

	/* Call service specific constructor slc_fcmh_ctor(), slm_fcmh_ctor(),
	 *   and sli_fcmh_ctor() to initialize their private fields that
	 *   follow the main fcmh structure.
	 */
	rc = sl_fcmh_ops.sfop_ctor(fcmh);
	if (rc) 
		goto out;

	if (sstb || (flags & FIDC_LOOKUP_LOAD))
		fcmh->fcmh_state |= FCMH_GETTING_ATTRS;
	if (sstb)
		fcmh_setattr(fcmh, sstb, setattrflags);

	DEBUG_FCMH(PLL_DEBUG, fcmh, "new fcmh");

	if (flags & FIDC_LOOKUP_LOAD) {
		psc_assert(sl_fcmh_ops.sfop_getattr);
		rc = sl_fcmh_ops.sfop_getattr(fcmh);	/* slc_fcmh_getattr() */
	}

 out:
	FCMH_LOCK(fcmh);
	if (rc) {
		fcmh->fcmh_state |= FCMH_CAC_FREEING;
		fcmh_op_done_type(fcmh, FCMH_OPCNT_NEW);
	} else {
		*fcmhp = fcmh;
		fcmh->fcmh_state |= FCMH_CAC_CLEAN;
		lc_add(&fidcCleanList, fcmh);
	}

	fcmh->fcmh_state &= ~FCMH_CAC_INITING;
	if (fcmh->fcmh_state & FCMH_CAC_WAITING) {
		fcmh->fcmh_state &= ~FCMH_CAC_WAITING;
		psc_waitq_wakeall(&fcmh->fcmh_waitq);
	}

	FCMH_ULOCK(fcmh);
	return (rc);
}

/**
 * fidc_init - Initialize the FID cache.
 */
void
fidc_init(int privsiz, int nobj, int max,
    int (*fcmh_reap_cb)(struct fidc_membh *))
{
	_psc_poolmaster_init(&fidcPoolMaster,
	    sizeof(struct fidc_membh) + privsiz,
	    offsetof(struct fidc_membh, fcmh_lentry),
	    PPMF_NONE, nobj, nobj, max, NULL,
	    NULL, fidc_reap, NULL, "fcmh");
	fidcPool = psc_poolmaster_getmgr(&fidcPoolMaster);

	lc_reginit(&fidcDirtyList, struct fidc_membh,
	    fcmh_lentry, "fcmhdirty");
	lc_reginit(&fidcCleanList, struct fidc_membh,
	    fcmh_lentry, "fcmhclean");

	psc_hashtbl_init(&fidcHtable, 0, struct fidc_membh,
	    FCMH_HASH_FIELD, fcmh_hentry, nobj * 2, NULL, "fidc");
	fidcReapCb = fcmh_reap_cb;
}

/*
 * fcmh_op_start/done_type(): we only move a cache item to the dirty list if we
 * know that the reference being taken is a long one. For short-lived references,
 * we avoid moving the cache item around.  Also, we only move a cache item back
 * to the clean list when the _last_ reference is dropped.
 */
void
fcmh_op_start_type(struct fidc_membh *f, enum fcmh_opcnt_types type)
{
	int locked=FCMH_RLOCK(f);

	psc_assert(f->fcmh_refcnt >= 0);
	f->fcmh_refcnt++;

	DEBUG_FCMH(PLL_NOTIFY, f, "took ref (type=%d)", type);

	/* Only 2 types of references may be long standing, FCMH_OPCNT_OPEN
	 *   and FCMH_OPCNT_BMAP.  Other ref types should not move the fcmh
	 *   to the dirty list.
	 */
	if (type == FCMH_OPCNT_OPEN || type == FCMH_OPCNT_BMAP) {
		if (f->fcmh_state & FCMH_CAC_CLEAN) {
			psc_assert(psclist_conjoint(&f->fcmh_lentry));
			lc_remove(&fidcCleanList, f);

			f->fcmh_state &= ~FCMH_CAC_CLEAN;
			f->fcmh_state |= FCMH_CAC_DIRTY;
			lc_add(&fidcDirtyList, f);
		}
	}
	FCMH_URLOCK(f, locked);
}

void
fcmh_op_done_type(struct fidc_membh *f, enum fcmh_opcnt_types type)
{
	int locked=FCMH_RLOCK(f);

	psc_assert(f->fcmh_refcnt > 0);

	DEBUG_FCMH(PLL_NOTIFY, (f), "release ref (type=%d)", type);

	f->fcmh_refcnt--;
	if (f->fcmh_refcnt == 0) {
		if (f->fcmh_state & FCMH_CAC_DIRTY) {
			psc_assert(psclist_conjoint(&f->fcmh_lentry));
			lc_remove(&fidcDirtyList, f);
			f->fcmh_state &= ~FCMH_CAC_DIRTY;
			lc_remove(&fidcDirtyList, f);
		}
		if (f->fcmh_state & FCMH_CAC_FREEING) {
			psc_hashent_remove(&fidcHtable, f);
			fcmh_destroy(f);
			return;
		} else {
			f->fcmh_state |= FCMH_CAC_CLEAN;
			lc_add(&fidcCleanList, f);
		}
	}
	FCMH_URLOCK(f, locked);
}

void
dump_fcmh(struct fidc_membh *f)
{
	DEBUG_FCMH(PLL_MAX, f, "");
}

void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	if (flags & FCMH_CAC_CLEAN)
		print_flag("FCMH_CAC_CLEAN", &seq);
	if (flags & FCMH_CAC_DIRTY)
		print_flag("FCMH_CAC_DIRTY", &seq);
	if (flags & FCMH_CAC_FREEING)
		print_flag("FCMH_CAC_FREEING", &seq);
	if (flags & FCMH_HAVE_ATTRS)
		print_flag("FCMH_HAVE_ATTRS", &seq);
	if (flags & FCMH_GETTING_ATTRS)
		print_flag("FCMH_GETTING_ATTRS", &seq);
	printf("\n");
}
