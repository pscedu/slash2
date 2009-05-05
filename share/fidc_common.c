/* $Id$ */

#include <stdio.h>
#define __USE_GNU 1
#include <pthread.h>

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

int (*fidcReapCb)(struct fidc_membh *f);
void (*initFcooCb)(struct fidc_open_obj *o);

struct psc_poolmaster fidcFreePoolMaster;
struct psc_poolmgr   *fidcFreePool;
#define fidcFreeList  fidcFreePool->ppm_lc
struct psc_listcache  fidcDirtyList;
struct psc_listcache  fidcCleanList;

struct hash_table fidcHtable;

struct sl_fsops *slFsops;

struct fidc_membh * 
__fidc_lookup_fg(const struct slash_fidgen *, int);


static inline int
fidc_freelist_avail_check(void)
{
        psc_assert(fidcFreePool->ppm_lc.lc_size >= 0);

        if ((fidcFreePool->ppm_lc.lc_nseen / SL_FIDCACHE_LOW_THRESHOLD) >
            (size_t)fidcFreePool->ppm_lc.lc_size)
                return 0;
        return 1;
}

void
fidc_fcm_setattr(struct fidc_membh *fcmh, const struct stat *stb)
{
	int l = reqlock(&fcmh->fcmh_lock);

	psc_assert(fcmh_2_gen(fcmh) != FID_ANY);

	memcpy(fcmh_2_stb(fcmh), stb, sizeof(*stb));
	fcmh_2_age(fcmh) = fidc_gettime() + FCMH_ATTR_TIMEO;
	
	if (fcmh->fcmh_state & FCMH_GETTING_ATTRS) {
		fcmh->fcmh_state &= ~FCMH_GETTING_ATTRS;
		fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
		psc_waitq_wakeall(&fcmh->fcmh_waitq);
	} else
		psc_assert(fcmh->fcmh_state & FCMH_HAVE_ATTRS);		

	if (fcmh_2_isdir(fcmh) && !(fcmh->fcmh_state & FCMH_ISDIR)) {
		fcmh->fcmh_state |= FCMH_ISDIR;
		INIT_PSCLIST_HEAD(&fcmh->fcmh_children);
	}

	DEBUG_FCMH(PLL_DEBUG, fcmh, "attr set");
	ureqlock(&fcmh->fcmh_lock, l);
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
fidc_put(struct fidc_membh *f, list_cache_t *lc)
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
			struct fidc_membh *tmp;

			psc_assert(!atomic_read(&f->fcmh_refcnt));
			if (f->fcmh_cache_owner == NULL)
				DEBUG_FCMH(PLL_WARN, f, 
					   "null fcmh_cache_owner here");
 
			tmp = __fidc_lookup_fg(fcmh_2_fgp(f), 1);
			if (f != tmp)
				abort();

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

/**
 * fidc_reap - reap some inodes from the clean list.
 */
int
fidc_reap(struct psc_poolmgr *m)
{
	struct fidc_membh *f, *tmp;
	static pthread_mutex_t mutex =
		PTHREAD_ERRORCHECK_MUTEX_INITIALIZER_NP;
	struct dynarray da = DYNARRAY_INIT;
	int i=0;

	ENTRY;

	psc_assert(m == fidcFreePool);
	/* Only one thread may be here.
	 */
	pthread_mutex_lock(&mutex);
 startover:
	LIST_CACHE_LOCK(&fidcCleanList);
	psclist_for_each_entry_safe(f, tmp, &fidcCleanList.lc_listhd,
				    fcmh_lentry) {

		DEBUG_FCMH(PLL_WARN, f, "considering for reap");

		if (psclg_size(&m->ppm_lg) + dynarray_len(&da) >=
                    atomic_read(&m->ppm_nwaiters) + 1) 
                        break;

		if (!trylock_hash_bucket(&fidcHtable, fcmh_2_fid(f))) {
                        LIST_CACHE_ULOCK(&fidcCleanList);
                        sched_yield();
                        goto startover;
		}		    
		/* Skip the root inode.
		 */
		if (fcmh_2_fid(f) == 1)
			goto end2;
		/*  Clean inodes may have non-zero refcnts, skip these
		 *   inodes.
		 */
		if (atomic_read(&f->fcmh_refcnt))
			goto end2;

		if (!trylock(&f->fcmh_lock))
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

		if (fidcReapCb) {
			/* Call into the system specific 'reap' code.
			 *  On the client this means taking the fcc from the 
			 *  parent directory inode.
			 */
			if ((fidcReapCb)(f)) 
				goto end1;
		}
		/* Free it but don't bother unlocking it, the fcmh was
		 *  reinitialized.
		 */
		f->fcmh_state |= FCMH_CAC_FREEING;
		lc_del(&f->fcmh_lentry, &fidcCleanList);
		dynarray_add(&da, f);
	end1:
		freelock(&f->fcmh_lock);
	end2:
		freelock_hash_bucket(&fidcHtable, fcmh_2_fid(f));
        }
        LIST_CACHE_ULOCK(&fidcCleanList);

	pthread_mutex_unlock(&mutex);
	
	for (i=0; i < dynarray_len(&da); i++) {
		f = dynarray_getpos(&da, i);
		DEBUG_FCMH(PLL_WARN, f, "moving to free list");
		fidc_put(f, &fidcFreeList);
        }
	
	dynarray_free(&da);
	return (i);
}


/**
 * fidc_get - grab a clean fid from the cache.
 */
struct fidc_membh * 
fidc_get(void)
{
	struct fidc_membh *f;

	f = psc_pool_get(fidcFreePool);
	
	f->fcmh_cache_owner = NULL;
	psc_assert(f->fcmh_state == FCMH_CAC_FREE);
	f->fcmh_state = FCMH_CAC_CLEAN;
	psc_assert(fcmh_clean_check(f));
	fidc_membh_incref(f);

	return (f);
}


/**
 * fidc_lookup_simple - perform a simple lookup of a fid in the cache.  If the fid is found, its refcnt is incremented and it is returned.
 */
struct fidc_membh *
__fidc_lookup_fg(const struct slash_fidgen *fg, int del)
{
	struct hash_bucket *b;
	struct hash_entry *e, *save=NULL;
	struct fidc_membh *fcmh=NULL, *tmp;
	int l[2];

	b = GET_BUCKET(&fidcHtable, (u64)fg->fg_fid);

 retry:
	l[0] = reqlock(&b->hbucket_lock);
	psclist_for_each_entry(e, &b->hbucket_list, hentry_lentry) {
		if ((u64)fg->fg_fid != *e->hentry_id)
			continue;
		
		tmp = e->private;

		l[1] = reqlock(&tmp->fcmh_lock); 
		/* Be sure to ignore any inodes which are freeing unless
		 *  we are removing the inode from the cache.
		 *  This is necessary to avoid a deadlock between fidc_reap
		 *  which has the fcmh_lock before calling fidc_put_locked,
		 *  whichs calls this function with del==1.  This is described
		 *  in Bug #13.
		 */
		if (del) {
			if (tmp->fcmh_state & FCMH_CAC_FREEING) {
				psc_assert((u64)fg->fg_gen == fcmh_2_gen(tmp));
				fcmh = tmp;
				save = e;
				ureqlock(&tmp->fcmh_lock, l[1]);
				break;
			} else {
				ureqlock(&tmp->fcmh_lock, l[1]);
				continue;
			}
		} else {
			if (tmp->fcmh_state & FCMH_CAC_FREEING) {
				ureqlock(&tmp->fcmh_lock, l[1]);
				continue;
			}
			if ((u64)fg->fg_gen == fcmh_2_gen(tmp)) {
				fcmh = tmp;
				save = e;
				ureqlock(&tmp->fcmh_lock, l[1]);
				break;
			}
			if (fcmh_2_gen(tmp) == FID_ANY) {
				/* The generation number has yet to be obtained
				 *  from the server.  Wait for it and retry.
				 */			
				psc_assert(tmp->fcmh_state & FCMH_GETTING_ATTRS);
				ureqlock(&b->hbucket_lock, l[0]);
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
			}
			ureqlock(&tmp->fcmh_lock, l[1]);
		}
	}

	if (fcmh && (del == 1)) {
		psc_assert(save);
                psclist_del(&save->hentry_lentry);
	}

	ureqlock(&b->hbucket_lock, l[0]);

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
        spinlock_hash_bucket(&fidcHtable, fg->fg_fid);
 trycreate:
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
			fcmh_new->fcmh_state = FCMH_CAC_FREEING;
			fidc_put(fcmh_new, &fidcFreeList);
		}

		psc_assert(fg->fg_fid == fcmh_2_fid(fcmh));		
		fcmh_clean_check(fcmh);
		
		if (fcmh->fcmh_state & FCMH_CAC_FREEING) {
			DEBUG_FCMH(PLL_WARN, fcmh, "fcmh is FREEING..");
			fidc_membh_dropref(fcmh);
			freelock_hash_bucket(&fidcHtable, fg->fg_fid);
			sched_yield();
			goto restart;
		}
		/* These attrs may be newer than the ones in the cache.
		 */
		if (fcm) 
			fidc_fcm_update(fcmh, fcm);

		freelock_hash_bucket(&fidcHtable, fg->fg_fid);

	} else {
		if (flags & FIDC_LOOKUP_CREATE)
			if (!try_create) {
				/* Allocate a fidc handle and attach the 
				 *   provided fcm.
				 */
				freelock_hash_bucket(&fidcHtable, fg->fg_fid);
				fcmh_new = fidc_get();
				spinlock_hash_bucket(&fidcHtable, fg->fg_fid);
				try_create = 1;
				goto trycreate;
				
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
			fcmh->fcmh_state |= FCMH_HAVE_ATTRS;
			fidc_fcm_setattr(fcmh, &fcm->fcm_stb);

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
			
		init_hash_entry(&fcmh->fcmh_hashe, &(fcmh_2_fid(fcmh)), fcmh);

		FCMH_LOCK(fcmh);
		fidc_put(fcmh, &fidcCleanList);
		add_hash_entry(&fidcHtable, &fcmh->fcmh_hashe);
		freelock_hash_bucket(&fidcHtable, fg->fg_fid);
		/* Do this dance so that fidc_fcoo_start_locked() is not 
		 *  called while holding the hash bucket lock!
		 */
		if (flags & FIDC_LOOKUP_FCOOSTART) {
			psc_assert(fcmh->fcmh_fcoo == (struct fidc_open_obj *)0x1);
			fcmh->fcmh_fcoo = NULL;
			fidc_fcoo_start_locked(fcmh);
		}
		FCMH_ULOCK(fcmh);
		DEBUG_FCMH(PLL_DEBUG, fcmh, "new fcmh");
	}

	if ((flags & FIDC_LOOKUP_LOAD) ||
	    (flags & FIDC_LOOKUP_REFRESH)) {
		if (slFsops)
			if ((slFsops->slfsop_getattr)(fcmh, creds)) {
				DEBUG_FCMH(PLL_DEBUG, fcmh, "getattr failure");
				return (NULL);
			}
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
	//	if (fcooInitCb)
	//	(void)(fcoo_init)(f);
	lc_init(&f->fcoo_buffer_cache, struct sl_buffer, slb_fcm_lentry);
	jfi_init(&f->fcoo_jfi);
}

/**
 * fidcache_init - initialize the fid cache.
 */
void
fidcache_init(enum fid_cache_users t, int (*fcm_reap_cb)(struct fidc_membh *))
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
			    fidc_membh_init, NULL, fidc_reap, "fidcFreePool");

	fidcFreePool = psc_poolmaster_getmgr(&fidcFreePoolMaster);
	
	lc_reginit(&fidcDirtyList, struct fidc_membh,
		   fcmh_lentry, "fcmdirty");
	lc_reginit(&fidcCleanList, struct fidc_membh,
		   fcmh_lentry, "fcmclean");

	init_hash_table(&fidcHtable, htsz, "fidcHtable");	
	/*fidcHtable.htcompare = fidc_hash_cmp;*/
	initFcooCb = NULL;
	fidcReapCb = fcm_reap_cb;
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

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);
