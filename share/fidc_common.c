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
#define fidcFreeList	 fidcPool->ppm_lc
struct psc_listcache	 fidcDirtyList;
struct psc_listcache	 fidcCleanList;
struct psc_hashtbl	 fidcHtable;
int			 fcoo_priv_size;

/**
 * fcmh_reset - Invalidate a FID cache member handle.
 * @fcmh: handle to clear.
 */
void
fcmh_reset(struct fidc_membh *f)
{
	memset(f, 0, sizeof(*f));
	LOCK_INIT(&f->fcmh_lock);
	atomic_set(&f->fcmh_refcnt, 0);
	psc_waitq_init(&f->fcmh_waitq);
	f->fcmh_state = FCMH_CAC_FREE;
	INIT_PSCLIST_ENTRY(&f->fcmh_lentry);
}

/**
 * fcmh_init - Init a newly allocated FID cache member handle.
 * @m: pool the fcmh was allocated for/from.
 * @p: object that was allocated.
 */
int
fcmh_init(__unusedx struct psc_poolmgr *m, void *p)
{
	struct fidc_membh *f = p;
	int rc = 0;

	if (sl_fcmh_ops.sfop_grow)
		rc = sl_fcmh_ops.sfop_grow();
	if (rc == 0)
		fcmh_reset(f);
	return (rc);
}

/**
 * fcmh_dtor - Destructor for FID cache member handles.
 * @p: object being destroyed.
 */
void
fcmh_dtor(__unusedx void *p)
{
	if (sl_fcmh_ops.sfop_shrink)
		sl_fcmh_ops.sfop_shrink();
	/*
	 * We don't need to reset() it here because that
	 * was done before it was returned to the pool.
	 */
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
	psc_assert(f->fcmh_state == FCMH_CAC_FREE);
	f->fcmh_state = FCMH_CAC_CLEAN;
	psc_assert(fcmh_clean_check(f));
	psc_assert(!f->fcmh_fcoo);
	fcmh_incref(f);
	return (f);
}

void
fcmh_setattr(struct fidc_membh *fcmh, const struct srt_stat *sstb,
    int flags)
{
	int locked = reqlock(&fcmh->fcmh_lock);
	uint64_t size = 0;

	psc_assert(fcmh_2_gen(fcmh) != FIDGEN_ANY);
	psc_assert(sstb->sst_ino == (ino_t)fcmh->fcmh_fg.fg_fid);

	if ((flags & FCMH_SETATTRF_SAVESIZE) &&
	    fcmh_2_ptruncgen(fcmh) >= sstb->sst_ptruncgen)
		size = fcmh_2_fsz(fcmh);

	fcmh->fcmh_sstb = *sstb;
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
 * fidc_put - release an inode onto an inode list cache.
 *
 * This routine should be called when:
 * - moving a free inode to the dirty cache.
 * - moving a dirty inode to the free cache.
 * Notes: inode is already locked.  Extensive validity checks are
 *        performed within fidc_clean_check()
 */
void
fidc_put(struct fidc_membh *f, struct psc_listcache *lc)
{
	int clean;

	/* Check for uninitialized
	 */
	if (!f->fcmh_state) {
		DEBUG_FCMH(PLL_FATAL, f, "not initialized!");
		psc_fatalx("Tried to put an uninitialized inode");
	}
	if (f->fcmh_cache_owner == lc) {
		DEBUG_FCMH(PLL_FATAL, f, "invalid list move");
		psc_fatalx("Tried to move to the same list (%p)", lc);
	}
	/* Validate the inode and check if it has some dirty blocks
	 */
	clean = fcmh_clean_check(f);

	if (lc == &fidcPool->ppm_lc) {
		psc_assert(f->fcmh_cache_owner == &fidcCleanList ||
			   f->fcmh_cache_owner == NULL);
		/* FCMH_CAC_FREEING should have already been set so that
		 *  other threads will ignore the freeing hash entry.
		 */
		psc_assert(f->fcmh_state & FCMH_CAC_FREEING);
		/* No open object allowed here.
		 */
		psc_assert(!f->fcmh_fcoo);
		/* Verify that no children are hanging about.
		 */

		psc_assert(!atomic_read(&f->fcmh_refcnt));
		if (f->fcmh_cache_owner == NULL)
			DEBUG_FCMH(PLL_WARN, f,
				   "null fcmh_cache_owner here");

		else
			psc_assert(_fidc_lookup_fg(&f->fcmh_fg, 1) == f);

		if (psc_hashent_conjoint(&fidcHtable, f))
			psc_hashent_remove(&fidcHtable, f);

		/* invalidate to reveal mismanaged refs */
		fcmh_reset(f);

	} else if (lc == &fidcCleanList) {
		psc_assert(f->fcmh_cache_owner == &fidcPool->ppm_lc ||
			   f->fcmh_cache_owner == &fidcDirtyList ||
			   f->fcmh_cache_owner == NULL);
		psc_assert(ATTR_TEST(f->fcmh_state, FCMH_CAC_CLEAN));
		psc_assert(clean);

	} else if (lc == &fidcDirtyList) {
		psc_assert(f->fcmh_cache_owner == &fidcCleanList);
		psc_assert(ATTR_TEST(f->fcmh_state, FCMH_CAC_DIRTY));
		//XXX psc_assert(clean == 0);

	} else
		psc_fatalx("lc %p is a bogus list cache", lc);

	/* Place onto the respective list.
	 */
	FCMHCACHE_PUT(f, lc);
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
	psclist_for_each_entry_safe(f, tmp, &fidcCleanList.lc_listhd,
				    fcmh_lentry) {

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
		    (atomic_read(&f->fcmh_refcnt)))
			goto end2;
		/* Make sure our clean list is 'clean' by
		 *  verifying the following conditions.
		 */
		if (!fcmh_clean_check(f)) {
			DEBUG_FCMH(PLL_FATAL, f,
			    "Invalid fcmh state for clean list");
			psc_fatalx("Invalid state for clean list");
		}
		/* Skip inodes which already claim to be freeing
		 */
		if (f->fcmh_state & FCMH_CAC_FREEING)
			goto end1;

		/* Call into the system specific 'reap' code.
		 *  On the client this means taking the fcc from the
		 *  parent directory inode.
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
		fidc_put(f, &fidcFreeList);
	}

	psc_dynarray_free(&da);
	return (i);
}

/**
 * fidc_lookup_fg - perform a lookup of a fid in the cache.
 *	If the fid is found, its refcnt is incremented and it is returned.
 */
struct fidc_membh *
_fidc_lookup_fg(const struct slash_fidgen *fg, int del)
{
	struct psc_hashbkt *b;
	struct fidc_membh *fcmh=NULL, *tmp;
	int locked[2];

	b = psc_hashbkt_get(&fidcHtable, &fg->fg_fid);

 retry:
	locked[0] = psc_hashbkt_reqlock(b);
	PSC_HASHBKT_FOREACH_ENTRY(&fidcHtable, tmp, b) {
		if (fcmh_2_fid(tmp) != fg->fg_fid)
			continue;

		locked[1] = reqlock(&tmp->fcmh_lock);
		/* Be sure to ignore any inodes which are freeing unless
		 *  we are removing the inode from the cache.
		 *  This is necessary to avoid a deadlock between fidc_reap()
		 *  which has the fcmh_lock before calling fidc_put_locked,
		 *  which calls this function with del==1.  This is described
		 *  in Bug #13.
		 */
		if (del) {
			if (fg->fg_gen == fcmh_2_gen(tmp)) {
				psc_assert(tmp->fcmh_state & FCMH_CAC_FREEING);
				fcmh = tmp;
				ureqlock(&tmp->fcmh_lock, locked[1]);
				break;
			} else {
				ureqlock(&tmp->fcmh_lock, locked[1]);
				continue;
			}
		} else {
			if (tmp->fcmh_state & FCMH_CAC_FREEING) {
				ureqlock(&tmp->fcmh_lock, locked[1]);
				continue;
			}
			if (fg->fg_gen == fcmh_2_gen(tmp)) {
				fcmh = tmp;
				ureqlock(&tmp->fcmh_lock, locked[1]);
				break;
			}
			if (fcmh_2_gen(tmp) == FIDGEN_ANY) {
				/* The generation number has yet to be obtained
				 *  from the server.  Wait for it and retry.
				 */
				psc_assert(tmp->fcmh_state & FCMH_GETTING_ATTRS);
				if (locked[1])
					atomic_inc(&tmp->fcmh_refcnt);
				psc_hashbkt_unlock(b);
				psc_waitq_wait(&tmp->fcmh_waitq, &tmp->fcmh_lock);

				if (locked[0])
					psc_hashbkt_lock(b);
				if (locked[1]) {
					spinlock(&tmp->fcmh_lock);
					atomic_dec(&tmp->fcmh_refcnt);
				}
				goto retry;
			}
			if (fg->fg_gen == FIDGEN_ANY) {
				/* Look for highest generation number.
				 */
				if (!fcmh || (fcmh_2_gen(tmp) > fcmh_2_gen(fcmh)))
					fcmh = tmp;
			}
			ureqlock(&tmp->fcmh_lock, locked[1]);
		}
	}

	if (fcmh && (del == 1))
		psc_hashent_remove(&fidcHtable, fcmh);

	psc_hashbkt_ureqlock(b, locked[0]);

	if (fcmh)
		fcmh_incref(fcmh);

	return (fcmh);
}

/**
 * fidc_lookup_simple - Wrapper for fidc_lookup_fg().  Called when the
 *  generation number is not known.
 */
struct fidc_membh *
fidc_lookup_simple(slfid_t f)
{
	struct slash_fidgen t = { f, FIDGEN_ANY };

	return (fidc_lookup_fg(&t));
}

int
fidc_lookup(const struct slash_fidgen *fgp, int flags,
    const struct srt_stat *sstb, int setattrflags,
    const struct slash_creds *creds, struct fidc_membh **fcmhp)
{
	int getting=0, rc, try_create=0;
	struct fidc_membh *fcmh, *fcmh_new;
	struct psc_hashbkt *b;
	struct slash_fidgen searchfg = *fgp;

#ifdef DEMOTED_INUM_WIDTHS
	searchfg.fg_fid = (fuse_ino_t)searchfg.fg_fid;
#endif

	rc = 0;
	*fcmhp = NULL;

	fcmh_new = NULL; /* gcc */

#if 0
	/*
	 * The original code has a bug in iod_inode_lookup().  Even though that
	 * it sets FIDC_LOOKUP_COPY, it does not pass in valid attributes (it
	 * only uses COPYFID() to initialize part of the fcmh.  Need to investigate
	 * how an I/O server uses attributes. - 12/08/2009.
	 */
	if (flags & FIDC_LOOKUP_COPY)
		psc_assert(sstb);
#endif

	if (flags & FIDC_LOOKUP_LOAD)
		psc_assert(creds);

	if (flags & FIDC_LOOKUP_CREATE)
		psc_assert(flags & (FIDC_LOOKUP_COPY | FIDC_LOOKUP_LOAD));

	b = psc_hashbkt_get(&fidcHtable, &searchfg.fg_fid);
 restart:
	psc_hashbkt_lock(b);
 trycreate:
	fcmh = fidc_lookup_fg(&searchfg);
	if (fcmh) {
		if (flags & FIDC_LOOKUP_EXCL) {
			fcmh_dropref(fcmh);
			psc_warnx("FID "FIDFMT" already in cache",
			    FIDFMTARGS(fgp));
			rc = EEXIST;
		}
		/*
		 * Test to see if we jumped here from fidcFreeList.
		 * Note an unlucky thread could find that the fid
		 * does not exist before allocation and exist after
		 * that.
		 */
		if (try_create) {
			fcmh_new->fcmh_state = FCMH_CAC_FREEING;
			fcmh_dropref(fcmh_new);
			fidc_put(fcmh_new, &fidcFreeList);
		}
		if (rc) {
			psc_hashbkt_unlock(b);
			return (rc);
		}

#ifdef DEMOTED_INUM_WIDTHS
		/*
		 * Since fuse_ino_t is 'unsigned long', it will be 4
		 * bytes on some architectures.  On these machines,
		 * allow collisions since '(unsigned long)uint64_t var'
		 * will frequently be inequal to 'uint64_t var' uncasted.
		 */
		psc_assert(searchfg.fg_fid ==
		    (uint64_t)(fuse_ino_t)fcmh_2_fid(fcmh));
		if (fgp->fg_fid != fcmh_2_fid(fcmh))
			return (ENFILE);
#else
		psc_assert(fgp->fg_fid == fcmh_2_fid(fcmh));
#endif
		fcmh_clean_check(fcmh);

		if (fcmh->fcmh_state & FCMH_CAC_FREEING) {
			DEBUG_FCMH(PLL_WARN, fcmh, "fcmh is FREEING");
			fcmh_dropref(fcmh);
			psc_hashbkt_unlock(b);
			sched_yield();
			goto restart;
		}

		/* apply provided attributes to the cache */
		if (sstb)
			fcmh_setattr(fcmh, sstb, setattrflags);

		psc_hashbkt_unlock(b);

		*fcmhp = fcmh;
		return (0);
	} else {
		if (flags & FIDC_LOOKUP_CREATE)
			if (!try_create) {
				/* Allocate a fidc handle and attach the
				 *   provided fcmh.
				 */
				psc_hashbkt_unlock(b);
				fcmh_new = fcmh_get();
				psc_hashbkt_lock(b);
				try_create = 1;
				goto trycreate;
			} else
				fcmh = fcmh_new;
		else
			/* FIDC_LOOKUP_CREATE was not specified and the fcmh
			 *  is not present.
			 */
			return (ENOENT);

		/* OK, we've got a new fcmh.  No need to lock it since
		 *  it's not yet visible to other threads.
		 */

		if (flags & FIDC_LOOKUP_COPY) {
			COPYFID(&fcmh->fcmh_fg, fgp);
#ifdef DEMOTED_INUM_WIDTHS
			COPYFID(&fcmh->fcmh_smallfg, &searchfg);
#endif
			fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
			if (sstb)
				fcmh_setattr(fcmh, sstb, setattrflags);

		} else if (flags & FIDC_LOOKUP_LOAD) {
			/* The caller has provided an incomplete
			 *  attribute set.  This fcmh will be a
			 *  placeholder and our caller will do the
			 *  stat.
			 */
			fcmh->fcmh_state &= ~FCMH_HAVE_ATTRS;
			fcmh->fcmh_state |= FCMH_GETTING_ATTRS;
			getting = 1;
			COPYFID(&fcmh->fcmh_fg, fgp);
#ifdef DEMOTED_INUM_WIDTHS
			COPYFID(&fcmh->fcmh_smallfg, &searchfg);
#endif
		} /* else is handled by the initial asserts */

		/* if there is already an fcoo, don't overwrite */
		if ((flags & FIDC_LOOKUP_FCOOSTART) &&
		    (fcmh->fcmh_state & FCMH_FCOO_ATTACH))
			flags &= ~FIDC_LOOKUP_FCOOSTART;

		if (flags & FIDC_LOOKUP_FCOOSTART) {
			/* Set an 'open' placeholder so that
			 *  only the caller may perform an open
			 *  RPC.  This is used for file creates.
			 */
			fcmh->fcmh_state |= FCMH_FCOO_STARTING;
			fcmh->fcmh_fcoo = FCOO_STARTING;
		}
		/* Place the fcmh into the cache, note that the fcmh was
		 *  ref'd so no race condition exists here.
		 */
		if (fcmh_2_gen(fcmh) == FIDGEN_ANY)
			DEBUG_FCMH(PLL_NOTICE, fcmh,
			    "adding FIDGEN_ANY to cache");

		FCMH_LOCK(fcmh);
		fidc_put(fcmh, &fidcCleanList);
		psc_hashbkt_add_item(&fidcHtable, b, fcmh);
		psc_hashbkt_unlock(b);

		/* Do this dance so that fidc_fcoo_start_locked() is not
		 *  called while holding the hash bucket lock!
		 */
		if (flags & FIDC_LOOKUP_FCOOSTART) {
			psc_assert(fcmh->fcmh_fcoo == FCOO_STARTING);
			fcmh->fcmh_fcoo = NULL;
			fidc_fcoo_start_locked(fcmh);
		}
		FCMH_ULOCK(fcmh);
		DEBUG_FCMH(PLL_DEBUG, fcmh, "new fcmh");
	}

	if ((flags & FIDC_LOOKUP_LOAD) && sl_fcmh_ops.sfop_getattr) {
		if (getting) {
			FCMH_LOCK(fcmh);
			fcmh->fcmh_state &= ~FCMH_GETTING_ATTRS;
		}
		while (fcmh->fcmh_state &
		    (FCMH_GETTING_ATTRS | FCMH_HAVE_ATTRS)) {
			psc_waitq_wait(&fcmh->fcmh_waitq,
			    &fcmh->fcmh_lock);
			FCMH_LOCK(fcmh);
		}
		if ((fcmh->fcmh_state & FCMH_HAVE_ATTRS) == 0) {
			rc = sl_fcmh_ops.sfop_getattr(fcmh, creds);
			if (rc == 0)
				fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
		}
		if (getting) {
			fcmh->fcmh_state &= ~FCMH_GETTING_ATTRS;
			FCMH_ULOCK(fcmh);
		}
		if (rc) {
			DEBUG_FCMH(PLL_DEBUG, fcmh, "getattr failure");
			return (-rc);
		}
	}

	*fcmhp = fcmh;
	return (0);
}

struct fidc_open_obj *
fidc_fcoo_init(void)
{
	struct fidc_open_obj *f;

	f = PSCALLOC(sizeof(*f) + fcoo_priv_size);
	SPLAY_INIT(&f->fcoo_bmapc);
	f->fcoo_bmap_sz = SLASH_BMAP_SIZE;
	return (f);
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
	    PPMF_NONE, nobj, nobj, max, fcmh_init,
	    fcmh_dtor, fidc_reap, NULL, "fcmh");
	fidcPool = psc_poolmaster_getmgr(&fidcPoolMaster);

	lc_reginit(&fidcDirtyList, struct fidc_membh,
	    fcmh_lentry, "fcmhdirty");
	lc_reginit(&fidcCleanList, struct fidc_membh,
	    fcmh_lentry, "fcmhclean");

	psc_hashtbl_init(&fidcHtable, 0, struct fidc_membh,
	    FCMH_HASH_FIELD, fcmh_hentry, nobj * 2, NULL, "fidc");
	fidcReapCb = fcmh_reap_cb;
}

int
fcmh_getfdbuf(struct fidc_membh *fcmh, struct srt_fd_buf *fdb)
{
	int rc, locked;

	locked = reqlock(&fcmh->fcmh_lock);

	if (!fcmh->fcmh_fcoo) {
		ureqlock(&fcmh->fcmh_lock, locked);
		return (EBADF);
	}

	rc = fidc_fcoo_wait_locked(fcmh, FCOO_NOSTART);
	if (!rc)
		*fdb = fcmh->fcmh_fcoo->fcoo_fdb;

	ureqlock(&fcmh->fcmh_lock, locked);
	return (rc ? EBADF : 0);
}

void
fcmh_setfdbuf(struct fidc_membh *fcmh, const struct srt_fd_buf *fdb)
{
	int locked;

	locked = reqlock(&fcmh->fcmh_lock);
	psc_assert(fcmh->fcmh_fcoo);
	fcmh->fcmh_fcoo->fcoo_fdb = *fdb;
	ureqlock(&fcmh->fcmh_lock, locked);
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
	if (flags & FCMH_CAC_FREE)
		print_flag("FCMH_CAC_FREE", &seq);
	if (flags & FCMH_FCOO_STARTING)
		print_flag("FCMH_FCOO_STARTING", &seq);
	if (flags & FCMH_FCOO_ATTACH)
		print_flag("FCMH_FCOO_ATTACH", &seq);
	if (flags & FCMH_FCOO_CLOSING)
		print_flag("FCMH_FCOO_CLOSING", &seq);
	if (flags & FCMH_FCOO_FAILED)
		print_flag("FCMH_FCOO_FAILED", &seq);
	if (flags & FCMH_HAVE_ATTRS)
		print_flag("FCMH_HAVE_ATTRS", &seq);
	if (flags & FCMH_GETTING_ATTRS)
		print_flag("FCMH_GETTING_ATTRS", &seq);
	printf("\n");
}
