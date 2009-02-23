/* $Id$ */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"

#include "cache_params.h"
#include "offtree.h"
#include "fid.h"
#include "fidcache.h"
#include "fidc_common.h"

#define SL_FIDCACHE_LOW_THRESHOLD 80 // 80%

int (*fidc_reap_cb)(struct fidc_membh *f);

struct psc_poolmaster fidcFreePoolMaster;
struct psc_poolmgr   *fidcFreePool;
#define fidcFreeList  fidcFreePool->ppm_lc
struct psc_listcache  fidcDirtyList;
struct psc_listcache  fidcCleanList;

struct hash_table fidcHtable;

struct sl_fsops *slFsops;

void
fidc_put_locked(struct fidc_membh *, list_cache_t *);

struct fidc_membh * 
__fidc_lookup_fg(const struct slash_fidgen *, int);

/**
 * fcmh_clean_check - verify the validity of the fcmh.
 */
static int
fcmh_clean_check(struct fidc_membh *f)
{
	int clean=0, l=reqlock(&f->fcmh_lock);

	DEBUG_FCMH(PLL_INFO, f, "clean_check");
	
	if (f->fcmh_state & FCMH_CAC_CLEAN) {
		if (f->fcmh_fcoo) {
			/* Fcoo's can exist only if the following 
			 *  true.  This phenomena exists because
			 *  we don't want to hold a lock on the fcmh
			 *  while an rpc is inflight.  So the thread
			 *  issuing the open rpc marks the fcmh as
			 *  'FCMH_FCOO_STARTING', at this time it's 
			 *  still on the clean list.
			 */
			psc_assert(f->fcmh_state & FCMH_FCOO_STARTING);
			psc_assert(atomic_read(&f->fcmh_refcnt) > 0);
		}		
		psc_assert(!(f->fcmh_state & 
			     (FCMH_CAC_DIRTY | FCMH_CAC_FREE |
			      FCMH_FCOO_ATTACH | FCMH_FCOO_CLOSING)));
		clean = 1;
	}

	ureqlock(&f->fcmh_lock, l);
	return (clean);
}

static inline int
fidc_freelist_avail_check(void)
{
        psc_assert(fidcFreePool->ppm_lc.lc_size >= 0);

        if ((fidcFreePool->ppm_lc.lc_nseen / SL_FIDCACHE_LOW_THRESHOLD) >
            (size_t)fidcFreePool->ppm_lc.lc_size)
                return 0;
        return 1;
}

/**
 * zinocache_reap - get some inodes from the clean list and free them.
 */
__static void
fidc_reap(void)
{
	struct fidc_membh *f;
	int locked, trytofree=8;

	ENTRY;

	do {
		f = lc_getnb(&fidcCleanList);
		if (f == NULL) {
			psc_warnx("fidcCleanList is empty");
			return;
		}

		locked = reqlock(&f->fcmh_lock);
		/* Skip the root inode.
		 */
		if (fcmh_2_fid(f) == 1)
			goto dontfree;

		/* Make sure our clean list is 'clean' by
		 *  verifying the following conditions.
		 */
		if (!fcmh_clean_check(f)) {
			DEBUG_FCMH(PLL_FATAL, f, 
			   "Invalid fcmh state for clean list");
			psc_fatalx("Invalid state for clean list");
		}
		/*  Clean inodes may have non-zero refcnts, skip these
		 *   inodes.
		 */
		if (atomic_read(&f->fcmh_refcnt))
			goto dontfree;

		if (fidc_reap_cb) {
			/* Call into the system specific 'reap' code.
			 *  On the client this means taking the fcc from the 
			 *  parent directory inode.
			 */
			if ((fidc_reap_cb)(f)) 
				goto dontfree;
		}		
		/* Free it but don't bother unlocking it, the fcmh was
		 *  reinitialized.
		 */
		f->fcmh_state |= FCMH_CAC_FREEING;
		DEBUG_FCMH(PLL_INFO, f, "freeing");
                fidc_put_locked(f, &fidcFreeList);
		continue;
		
	dontfree:
		DEBUG_FCMH(PLL_INFO, f, "restoring to clean list");
		lc_put(&fidcCleanList, &f->fcmh_lentry);
		ureqlock(&f->fcmh_lock, locked);

	} while(trytofree--);
}

/**
 * fidc_get - grab a clean fid from the cache.
 */
struct fidc_membh * 
fidc_get(list_cache_t *lc)
{
	struct fidc_membh *f;

 retry:
	f = lc_getnb(lc);
        if (f == NULL) {
		struct timespec ts;

                fidc_reap();        /* Try to free some fids */
		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_nsec += 100;
                f = lc_gettimed(lc, &ts);
		if (!f)
			goto retry;
        }
	if (lc == &fidcFreePool->ppm_lc) {
		f->fcmh_cache_owner = NULL;
		psc_assert(f->fcmh_state == FCMH_CAC_FREE);
		f->fcmh_state = FCMH_CAC_CLEAN;
		psc_assert(fcmh_clean_check(f));
		fidc_membh_incref(f);
	} else
		psc_fatalx("It's unwise to get inodes from %p, it's not "
			   "fidcFreePool->ppm_lc", lc);
	return (f);
}


/**
 * fidc_put_locked - release an inode onto an inode list cache.
 *
 * This routine should be called when:
 * - moving a free inode to the dirty cache.
 * - moving a dirty inode to the free cache.
 * Notes: inode is already locked.  Extensive validity checks are
 *        performed within fidc_clean_check()
 */
void
fidc_put_locked(struct fidc_membh *f, list_cache_t *lc)
{
	int clean;

	LOCK_ENSURE(&f->fcmh_lock);
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

	if (lc == &fidcFreePool->ppm_lc) {
		psc_assert(!f->fcmh_fcoo);
		/* Valid sources of this inode.
		 */
		if (!((f->fcmh_cache_owner == &fidcFreePool->ppm_lc) ||
		      (f->fcmh_cache_owner == &fidcCleanList) ||
		      (f->fcmh_cache_owner == NULL)))
			psc_fatalx("Bad inode fcmh_cache_owner %p",
			       f->fcmh_cache_owner);

		/* FCMH_CAC_FREEING should have already been set so that 
		 *  other threads will ignore the freeing hash entry.
		 */
		psc_assert(f->fcmh_state & FCMH_CAC_FREEING);

		DEBUG_FCMH(PLL_DEBUG, f, "reap fcmh");

		if (!f->fcmh_fcm)
			/* No fcm, this probably came from 
			 *  __fidc_lookup_inode() try_create.
			 */
			psc_assert(f->fcmh_cache_owner == NULL);
		else {
			psc_assert(!atomic_read(&f->fcmh_refcnt));
			if (f->fcmh_cache_owner == NULL)
				DEBUG_FCMH(PLL_WARN, f, 
					   "null fcmh_cache_owner here");
 
			freelock(&f->fcmh_lock);
			psc_assert(f == __fidc_lookup_fg(fcmh_2_fgp(f), 1));

			PSCFREE(f->fcmh_fcm);
			f->fcmh_fcm = NULL;	
		}
		/* Re-initialize it before placing onto the free list
		 */		
		fidc_membh_init(fidcFreePool, f);

	} else if (lc == &fidcCleanList) {
		psc_assert(f->fcmh_cache_owner == &fidcFreePool->ppm_lc ||
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

void
fidc_put(struct fidc_membh *f, list_cache_t *lc)
{
	int l=reqlock(&f->fcmh_lock);
	
	fidc_put_locked(f, lc);
	if (lc != &fidcFreePool->ppm_lc)
		ureqlock(&f->fcmh_lock, l);
}

/**
 * fidc_fcm_update - copy the contents of 'b' if it is newer than the 
 *   contents of h->fcmh_fcm.  Generation number is included in the 
 *   consideration.
 * @h: the fidc_membh which is already in the cache.
 * @b: prospective fidcache contents.
 */
static void 
fidc_fcm_update(struct fidc_membh *h, const struct fidc_memb *b)
{
	int l;
	struct fidc_memb *a = h->fcmh_fcm;

	l = reqlock(&h->fcmh_lock);
	
	psc_assert(SAMEFID(&a->fcm_fg, &b->fcm_fg));

	if (b->fcm_slfinfo.slf_age > a->fcm_slfinfo.slf_age)
		memcpy(a, b, sizeof(*a));

	ureqlock(&h->fcmh_lock, l);
}

#if 0
int
fidc_hash_cmp(const void *x, const void *y)
{	
	struct slash_fidgen *a=x;
	struct fidc_membh *b=y;
	
	psc_assert(a && b);

	return (a->fg_gen == fcmh_2_gen(b));
}
#endif

/**
 * fidc_lookup_simple - perform a simple lookup of a fid in the cache.  If the fid is found, its refcnt is incremented and it is returned.
 */
struct fidc_membh *
__fidc_lookup_fg(const struct slash_fidgen *fg, int del)
{
	struct hash_bucket *b;
	struct hash_entry *e, *save=NULL;
	struct fidc_membh *fcmh=NULL, *tmp;
	int l;

	b = GET_BUCKET(&fidcHtable, (u64)fg->fg_fid);

 retry:
	spinlock(&b->hbucket_lock);
	psclist_for_each_entry(e, &b->hbucket_list, hentry_lentry) {
		if ((u64)fg->fg_fid != *e->hentry_id)
			continue;
		
		tmp = e->private;

		l = reqlock(&tmp->fcmh_lock); 
		/* Be sure to ignore any inodes which are freeing unless
		 *  we are removing the inode from the cache.
		 *  This is necessary to avoid a deadlock between fidc_reap
		 *  which has the fcmh_lock before calling fidc_put_locked,
		 *  whichs calls this function with del==1.  This is described
		 *  in Bug #13.
		 */
		if ((tmp->fcmh_state & FCMH_CAC_FREEING) && !del) {
			ureqlock(&tmp->fcmh_lock, l);
			continue;
		}
		
		if (fcmh_2_gen(tmp) == FID_ANY) {
			/* The generation number has yet to be obtained
			 *  from the server.  Wait for it and retry.
			 */			
			psc_assert(tmp->fcmh_state & FCMH_GETTING_ATTRS);
			freelock(&b->hbucket_lock);
			psc_waitq_wait(&tmp->fcmh_waitq, &tmp->fcmh_lock);
			goto retry;
		}

		if ((u64)fg->fg_gen == FID_ANY) {
			/* Look for highest generation number.
			 */
			if (!fcmh || (fcmh_2_gen(tmp) > fcmh_2_gen(fcmh))) {
				fcmh = tmp;
				save = e;
			}
			
		} else if ((u64)fg->fg_gen == fcmh_2_gen(tmp)) {
			fcmh = tmp;
			save = e;
			ureqlock(&tmp->fcmh_lock, l);
			break;
		}

		ureqlock(&tmp->fcmh_lock, l);
	}

	if (fcmh && (del == 1)) {
		psc_assert(save);
                psclist_del(&save->hentry_lentry);
	}

	freelock(&b->hbucket_lock);

	if (fcmh)
		fidc_membh_incref(fcmh);

	return (fcmh);
}

struct fidc_membh *
fidc_lookup_fg(const struct slash_fidgen *fg)
{
	return (__fidc_lookup_fg(fg, 0));
}

/** 
 * fidc_lookup_simple - Wrapper for fidc_lookup_fg().  Called when the 
 *  generation number is not known.
 */
struct fidc_membh *
fidc_lookup_simple(slfid_t f) 
{
	struct slash_fidgen t = {f, FID_ANY};
	
	return (fidc_lookup_fg(&t));
}

struct fidc_membh *
__fidc_lookup_inode(const struct slash_fidgen *fg, int flags, 
		    const struct fidc_memb *fcm, 
		    const struct slash_creds *creds)
{
	int try_create=0, simple_lookup=0;
	struct fidc_membh *fcmh, *fcmh_new;

	if (flags & FIDC_LOOKUP_COPY)
		psc_assert(fcm);

	if ((flags & FIDC_LOOKUP_LOAD) || 
	    (flags & FIDC_LOOKUP_REFRESH))
		psc_assert(creds);

	if (flags & FIDC_LOOKUP_CREATE)
		psc_assert((flags & FIDC_LOOKUP_COPY) ||
			   (flags & FIDC_LOOKUP_LOAD));

 restart:
	fcmh = fidc_lookup_fg(fg);
	if (fcmh) {
		if (flags & FIDC_LOOKUP_EXCL) {
			fidc_membh_dropref(fcmh);
			psc_warnx("Fid "FIDFMT" already in cache", 
				  FIDFMTARGS(fg));
			return NULL;
		}
		/* Set to true so that we don't ref twice.
		 */
		simple_lookup = 1;
		/* Test to see if we jumped here from fidcFreeList.
		 */
		if (try_create) {
			/* Unlock and free the fcmh allocated 
			 *  from fidcFreeList.
			 */
			freelock(&fidcHtable.htable_lock);

			spinlock(&fcmh_new->fcmh_lock);
			fcmh_new->fcmh_state = FCMH_CAC_FREEING;
			fidc_put_locked(fcmh_new, &fidcFreeList);
			/* No unlock here, the inode is cleared after this.
			 */
		}
		fcmh_clean_check(fcmh);
		
		if (fcmh->fcmh_state & FCMH_CAC_FREEING) {
			DEBUG_FCMH(PLL_WARN, fcmh, "fcmh is FREEING..");
			fidc_membh_dropref(fcmh);
			goto restart;
		}
		/* These attrs may be newer than the ones in the cache.
		 */
		if (fcm)
			fidc_fcm_update(fcmh, fcm);

	} else {
		if (flags & FIDC_LOOKUP_CREATE)
			if (!try_create) {
				/* Allocate a fidc handle and attach the 
				 *   provided fcm.
				 */
				fcmh_new = fidc_get(&fidcFreeList);
				spinlock(&fidcHtable.htable_lock);
				try_create = 1;
				goto restart;
				
			} else
				fcmh = fcmh_new;
		else 
			/* FIDC_LOOKUP_CREATE was not specified and the fcmh
			 *  is not present.
			 */
			return (NULL);

		/* Ok we've got a new fcmh.  No need to lock it since 
		 *  it's not yet visible to other threads.
		 */		
		psc_assert(!fcmh->fcmh_fcm);
		fcmh->fcmh_fcm = PSCALLOC(sizeof(*fcmh->fcmh_fcm));

		if (flags & FIDC_LOOKUP_COPY) {
			psc_assert(SAMEFID(fg, fcm_2_fgp(fcm)));

			COPYFID(fcmh->fcmh_fcm, fcm);
			if (!fcmh->fcmh_state & FCMH_HAVE_ATTRS)
				fcmh->fcmh_state |= FCMH_HAVE_ATTRS;

		} else if (flags & FIDC_LOOKUP_LOAD) {
			/* The caller has provided an incomplete
			 *  attribute set.  This fcmh will be a 
			 *  placeholder and our caller will do the
			 *  stat.
			 */
			fcmh->fcmh_state &= ~FCMH_HAVE_ATTRS;
			fcmh->fcmh_state |= FCMH_GETTING_ATTRS;
			COPYFID(fcmh_2_fgp(fcmh), fg);
		} /* else is handled by the initial asserts */
		
		if (flags & FIDC_LOOKUP_FCOOSTART) {
			/* Set an 'open' placeholder so that
			 *  only the caller may perform an open
			 *  RPC.  This is used for file creates.
			 */
			fcmh->fcmh_state |= FCMH_FCOO_STARTING;
			fcmh->fcmh_fcoo = (struct fidc_open_obj *)0x1;
		}
		/* Place the fcmh into the cache, note that the fmch was
		 *  ref'd so no race condition exists here.
		 */
		if (fcmh_2_gen(fcmh) == FID_ANY)
			DEBUG_FCMH(PLL_WARN, fcmh, "adding FID_ANY to cache");
			
		fidc_put(fcmh, &fidcCleanList);
		init_hash_entry(&fcmh->fcmh_hashe, &(fcmh_2_fid(fcmh)), fcmh);
		add_hash_entry(&fidcHtable, &fcmh->fcmh_hashe);
		freelock(&fidcHtable.htable_lock);
		/* Do this dance so that fidc_fcoo_start_locked() is not 
		 *  called while holding the fidcHtable lock!
		 */
		if (flags & FIDC_LOOKUP_FCOOSTART) {
			spinlock(&fcmh->fcmh_lock);
			psc_assert(fcmh->fcmh_fcoo == (struct fidc_open_obj *)0x1);
			fcmh->fcmh_fcoo = NULL;
			fidc_fcoo_start_locked(fcmh);
			freelock(&fcmh->fcmh_lock);
		}
		DEBUG_FCMH(PLL_DEBUG, fcmh, "new fcmh");
	}

	if ((flags & FIDC_LOOKUP_LOAD) ||
	    (flags & FIDC_LOOKUP_REFRESH)) {
		if (slFsops)
			if ((slFsops->slfsop_getattr)(fcmh, creds))
				return (NULL);
	}
	return (fcmh);
}

int
fidc_membh_init(__unusedx struct psc_poolmgr *pm, void *a)
{
	struct fidc_membh *f = a;

	INIT_PSCLIST_ENTRY(&f->fcmh_lentry);
	atomic_set(&f->fcmh_refcnt, 0);	
	LOCK_INIT(&f->fcmh_lock);
	f->fcmh_state = 0;
	psc_waitq_init(&f->fcmh_waitq);
	f->fcmh_cache_owner = NULL;
	f->fcmh_fsops = slFsops;
	f->fcmh_state = FCMH_CAC_FREE;
	memset(&f->fcmh_hashe, 0, sizeof(struct hash_entry));
	return (0);	       
}

/**
 * fidc_memb_init - init a fidcache member.
 */
void
fidc_memb_init(struct fidc_memb *fcm, slfid_t f)
{
psc_fatalx("not ready");
	fcm = PSCALLOC(sizeof(*fcm));
	fcm->fcm_fg.fg_fid = f;
	fcm->fcm_fg.fg_gen = FID_ANY;
}

/**
 * fidc_membh_init - init a fidcache member handle.
 */
void
fidc_fcoo_init(struct fidc_open_obj *f)
{
	memset(f, 0, sizeof(*f));
	f->fcoo_cfd = FID_ANY;
	atomic_set(&f->fcoo_bmapc_cnt, 0);
	SPLAY_INIT(&f->fcoo_bmapc);
	lc_init(&f->fcoo_buffer_cache, struct sl_buffer, slb_fcm_lentry);
	jfi_init(&f->fcoo_jfi);
}

/**
 * fidcache_init - initialize the fid cache.
 */
void
fidcache_init(enum fid_cache_users t, __unusedx void (*fcm_init)(void *), 
	      int (*fcm_reap_cb)(struct fidc_membh *))
{
	int htsz;
	ssize_t	fcdsz, fcmsz;

	switch (t) {
	case FIDC_USER_CLI:
		htsz  = FIDC_CLI_HASH_SZ;
		fcdsz = FIDC_CLI_DEFSZ;
		fcmsz = FIDC_CLI_MAXSZ;
		break;
	case FIDC_ION_HASH_SZ:
		htsz  = FIDC_ION_HASH_SZ;
		fcdsz = FIDC_ION_DEFSZ;
		fcmsz = FIDC_ION_MAXSZ;
		break;
	case FIDC_MDS_HASH_SZ:
		htsz  = FIDC_MDS_HASH_SZ;
		fcdsz = FIDC_MDS_DEFSZ;
		fcmsz = FIDC_MDS_MAXSZ;
		break;
	default:
		psc_fatalx("Invalid fidcache user type");
	}

	psc_poolmaster_init(&fidcFreePoolMaster, struct fidc_membh, 
			    fcmh_lentry, 0, fcdsz, 0, fcmsz,
			    fidc_membh_init, NULL, NULL, "fidcFreePool");

	fidcFreePool = psc_poolmaster_getmgr(&fidcFreePoolMaster);
	
	lc_reginit(&fidcDirtyList, struct fidc_membh,
		   fcmh_lentry, "fcmdirty");
	lc_reginit(&fidcCleanList, struct fidc_membh,
		   fcmh_lentry, "fcmclean");

	init_hash_table(&fidcHtable, htsz, "fidcHtable");	
	/*fidcHtable.htcompare = fidc_hash_cmp;*/

	fidc_reap_cb = fcm_reap_cb;
}

int
fidc_fcmh2cfd(struct fidc_membh *fcmh, u64 *cfd)
{	
	int rc=0, l=reqlock(&fcmh->fcmh_lock);
	
	*cfd = FID_ANY;

	if (!fcmh->fcmh_fcoo) {
		ureqlock(&fcmh->fcmh_lock, l);
		return (-1);
	}

	rc = fidc_fcoo_wait_locked(fcmh, FCOO_NOSTART);
	if (!rc)
		*cfd = fcmh->fcmh_fcoo->fcoo_cfd;

	ureqlock(&fcmh->fcmh_lock, l);
	return (rc);
}

#if 0
int
fidc_fid2cfd(slfid_t f, u64 *cfd, struct fidc_membh **fcmh)
{
	if (!(*fcmh = fidc_lookup_inode(f)))
		return (-1);

	return (fidc_fcmh2cfd(*fcmh, cfd));
}
#endif

/**
 * bmapc_memb_init - initialize a bmap structure and create its offtree.
 * @b: the bmap struct
 * @f: the bmap's owner
 */
void
bmapc_memb_init(struct bmapc_memb *b, struct fidc_membh *f)
{
	memset(b, 0, sizeof(*b));
 	atomic_set(&b->bcm_opcnt, 0);
	psc_waitq_init(&b->bcm_waitq);
	b->bcm_oftr = offtree_create(SLASH_BMAP_SIZE, SLASH_BMAP_BLKSZ,
				     SLASH_BMAP_WIDTH, SLASH_BMAP_DEPTH,
				     f, sl_buffer_alloc, sl_oftm_addref,
				     sl_oftiov_pin_cb);
	psc_assert(b->bcm_oftr);
	f->fcmh_fcoo->fcoo_bmap_sz = SLASH_BMAP_SIZE;
}

/**
 * bmapc_cmp - bmap_cache splay tree comparator
 * @a: a bmapc_memb
 * @b: another bmapc_memb
 */
int
bmapc_cmp(const void *x, const void *y)
{
	const struct bmapc_memb *a = x, *b = y;

        if (a->bcm_blkno > b->bcm_blkno)
                return (1);
        else if (a->bcm_blkno < b->bcm_blkno)
                return (-1);
        return (0);
}

__static SPLAY_GENERATE(bmap_cache, bmapc_memb,
			bcm_tentry, bmapc_cmp);
