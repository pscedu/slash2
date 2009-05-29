/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_common.h"
#include "fidc_client.h"
#include "fidcache.h"

static struct fidc_child * 
fidc_new(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fidc_child *fcc;
	int len=strnlen(name, NAME_MAX);

	fcc = PSCALLOC(sizeof(*fcc) + (len + 1));
	atomic_set(&fcc->fcc_ref, 0);
	fcc->fcc_fg.fg_fid = fcmh_2_fid(c);
	fcc->fcc_fg.fg_gen = fcmh_2_gen(c);
	fcc->fcc_fcmh   = c;
	fcc->fcc_parent = p;
	fcc->fcc_hash   = str_hash(name);
	INIT_PSCLIST_ENTRY(&fcc->fcc_lentry);
	LOCK_INIT(&fcc->fcc_lock);
	fidc_settimeo(fcc->fcc_age);
	strncpy(fcc->fcc_name, name, len);
	fcc->fcc_name[len] = '\0';
	return (fcc);
}

static void 
fidc_child_prep_free_locked(struct fidc_membh *f)
{
	struct fidc_child *fcc, *tmp;

	LOCK_ENSURE(&f->fcmh_lock);

	if (!(f->fcmh_state & FCMH_ISDIR))
		return;

	psclist_for_each_entry_safe(fcc, tmp, &f->fcmh_children, fcc_lentry) {
		DEBUG_FCMH(PLL_WARN, f, "fcc=%p fcc_name=%s detaching", 
			   f, fcc->fcc_name);
		spinlock(&fcc->fcc_lock);
		psc_assert(fcc->fcc_parent == f);
		fcc->fcc_parent = NULL;
		freelock(&fcc->fcc_lock);
		psclist_del(&fcc->fcc_lentry);
	}
}

/**
 * fidc_child_free - release a child, parent must be locked.
 */
static void
fidc_child_free_plocked(struct fidc_child *fcc)
{
	struct fidc_membh *c=fcc->fcc_fcmh;
	int l=reqlock(&c->fcmh_lock);

	LOCK_ENSURE(&fcc->fcc_parent->fcmh_lock);
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	fidc_child_prep_free_locked(c);
	
	psclist_del(&fcc->fcc_lentry);
	psc_assert(c->fcmh_pri == fcc);
	psc_assert(!atomic_read(&fcc->fcc_ref));
	c->fcmh_pri = NULL;

	DEBUG_FCMH(PLL_WARN, c, "fcc=%p name=%s parent=%p freeing "
		   "child_empty=%d", 
		   fcc, fcc->fcc_name, fcc->fcc_parent, 
		   ((c->fcmh_state & FCMH_ISDIR) ?
                    psclist_empty(&c->fcmh_children) : -1));
	/* Verify that no children are hanging about.
	 */
	if (c->fcmh_state & FCMH_ISDIR)
		psc_assert(psclist_empty(&c->fcmh_children));

	PSCFREE(fcc);
	ureqlock(&c->fcmh_lock, l);
}

static void 
fidc_child_free_orphan_locked(struct fidc_membh *f)
{
	struct fidc_child *fcc=f->fcmh_pri;

	LOCK_ENSURE(&f->fcmh_lock);
	
	psc_assert(fcc);
	psc_assert(!(f->fcmh_state & FCMH_CAC_FREEING));
	f->fcmh_pri = NULL;

	DEBUG_FCMH(PLL_WARN, f, "fcc=%p name=%s freeing orphan",
		   fcc, fcc->fcc_name);

	if (f->fcmh_state & FCMH_ISDIR)
		psc_assert(psclist_empty(&f->fcmh_children));

	PSCFREE(fcc);
}


static struct fidc_child *
fidc_child_try_validate(struct fidc_membh *p, struct fidc_membh *c, 
			const char *name)
{
	struct fidc_child *fcc=NULL;

	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);
	
	psc_assert(!(p->fcmh_state & FCMH_CAC_FREEING));
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	if ((fcc = (struct fidc_child *)c->fcmh_pri)) {
		spinlock(&fcc->fcc_lock);
		/* Both of these must always be true.
		 */
		psc_assert(fcc->fcc_fcmh == c);
		psc_assert(SAMEFID(fcmh_2_fgp(c), &fcc->fcc_fg));
		if (strncmp(name, fcc->fcc_name, strnlen(name, NAME_MAX))) {
			/* This inode may have been renamed, remove
			 *  this fcc.
			 */
			fcc = NULL;
			fidc_child_free_plocked(fcc);

		} else {
			/* Increase the lifespan of this entry and return.
			 */
			fidc_settimeo(fcc->fcc_age);
			/* If the fcc is 'connected', then its parent inode
			 *   must be 'p'.
			 */
			if (fcc->fcc_parent) {
				psc_assert(fcc->fcc_parent == p);
				psc_assert(psclist_conjoint(&fcc->fcc_lentry));
			} else {
				fcc->fcc_parent = p;
				psclist_xadd_tail(&fcc->fcc_lentry, 
						  &p->fcmh_children);
				DEBUG_FCMH(PLL_WARN, p, "reattaching fcc=%p", 
					   fcc);
			}
		}
		freelock(&fcc->fcc_lock);
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);

	return (fcc);
}


int
fidc_child_reap_cb(struct fidc_membh *f)
{
	struct fidc_child *fcc=f->fcmh_pri;
	
	LOCK_ENSURE(&f->fcmh_lock);
	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);
	
	DEBUG_FCMH(PLL_WARN, f, "fcc=%p fcc_ref=%d fcmh_no_children=%d", 
		   fcc, (fcc ? atomic_read(&fcc->fcc_ref) : -1), 
		   ((f->fcmh_state & FCMH_ISDIR) ? 
		    psclist_empty(&f->fcmh_children) : -1));

	if ((fcc && atomic_read(&fcc->fcc_ref)) || 
	    ((f->fcmh_state & FCMH_ISDIR) && 
	     (!psclist_empty(&f->fcmh_children))))
		return (1);

	if (!fcc)
		return (0);

	if (!fcc->fcc_parent) {
		fidc_child_free_orphan_locked(f);
		return (0);

	} else {
		/* The parent needs to be unlocked after the fcc is freed, 
		 *  hence the need for the temp var 'p'.
		 */
		struct fidc_membh *p=fcc->fcc_parent;

		psc_assert(p);
		/* This tryeqlock technically violates lock ordering
		 *  (parent / child / fcc) which is why we bail if the
		 *  parent lock cannot be obtained without blocking.
		 */
		if (trylock(&p->fcmh_lock)) {
			fidc_child_free_plocked(fcc);
			freelock(&p->fcmh_lock);
			return (0);
		} 
	}
	return (1);
}


/**
 * fidc_child_get_int_locked - given a parent directory inode, try to locate a child. 
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_child *
fidc_child_lookup_int_locked(struct fidc_membh *p, const char *name)
{
	struct fidc_child *c=NULL;
	int found=0;
	int hash=str_hash(name);

	LOCK_ENSURE(&p->fcmh_lock);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(p->fcmh_state & FCMH_ISDIR);

	DEBUG_FCMH(PLL_INFO, p, "name %p (%s), hash=%d",
		   name, name, hash);

	psclist_for_each_entry(c, &p->fcmh_children, fcc_lentry) {
		
		psc_traces(PSS_OTHER, "p=fcmh@%p c=%p cname=%s hash=%d",
			   p, c, c->fcc_name, c->fcc_hash);

		if ((c->fcc_hash == hash) &&
		    (!strncmp(name, c->fcc_name, strnlen(name, NAME_MAX)))) {
			/* Pin down the fcc while the parent dir lock is held.
			 */
			found=1;
			atomic_inc(&c->fcc_ref);
			break;
		}
	}
	if (found) {
		psc_assert(c->fcc_parent == p);
		psc_assert(c->fcc_fcmh);
		psc_assert(c->fcc_fcmh->fcmh_pri == c);
		psc_assert(atomic_read(&c->fcc_ref) > 0);

		if (c->fcc_fcmh->fcmh_state & FCMH_CAC_FREEING)
			return (NULL);

		if (c->fcc_age < fidc_gettime()) {
			/* It's old, remove it.
			 */
			atomic_dec(&c->fcc_ref);
			fidc_child_free_plocked(c);
			c = NULL;
		} 
	}
	return (c);
}

#if 0
int
fidc_child_cmp(const void *x, const void *y)
{
	const struct fidc_child *a=x, *b=y;
	if ((c->fcc_hash == hash) &&
	    (!strncmp(name, c->fcc_name, strnlen(name, NAME_MAX)))) {
		/* Pin down the fcc while the parent dir lock is held.
		 */
		found=1;
		atomic_inc(&c->fcc_ref);
		break;
	}

	if (a->

        if (a->bcm_blkno > b->bcm_blkno)
                return (1);
        else if (a->bcm_blkno < b->bcm_blkno)
                return (-1);
        return (0);
}

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);
#endif

/**
 * fidc_child_lookup - search the parent inode for the child entry 'name'.
 * @p:  parent fcmh.
 * @name: the filename for which is being searched
 * Notes:  the parent fcmh is ref'd and must be decref'd by the caller.
 */
struct fidc_membh * 
fidc_child_lookup(struct fidc_membh *p, const char *name)
{
	struct fidc_child *c=NULL;	
	struct fidc_membh *m=NULL;
	int l=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
 	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	c = fidc_child_lookup_int_locked(p, name);
	if (c) { 
		m = c->fcc_fcmh;
		/* We no longer need the fcc, so decref it.  A ref on the fcmh
		 *  was taken in fidc_child_get_int_locked().
		 */
		psc_assert(atomic_read(&c->fcc_ref) > 0);
		atomic_dec(&c->fcc_ref);
	}
	ureqlock(&p->fcmh_lock, l);
	return (m);
}


void
fidc_child_unlink(struct fidc_membh *p, const char *name)
{
	struct fidc_child *fcc;
	int l=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	fcc = fidc_child_lookup_int_locked(p, name);
	if (!fcc) {
		ureqlock(&p->fcmh_lock, l);
		return;
	}
	/* Perform some sanity checks on the cached data structure.
	 */
	psc_assert(fcc->fcc_parent == p);
	psc_assert(fcc->fcc_hash == str_hash(name));
	psc_assert(!strncmp(fcc->fcc_name, name, 
			    strnlen(fcc->fcc_name, NAME_MAX)));
	
	/* The only ref on the fcc should be the one taken above in
	 *  fidc_child_lookup_int_locked()
	 */
	psc_assert(atomic_dec_and_test(&fcc->fcc_ref));
	fidc_child_free_plocked(fcc);

	ureqlock(&p->fcmh_lock, l);
}


/** fidc_child_add - add the child inode/name pair to its parent's child list.
 * @p: the parent inode handle.
 * @c: the child inode.
 * @name: the name of the child.
 */
void
fidc_child_add(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fidc_child *fcc, *tmp=NULL;

	psc_assert(p && c && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);
 	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
       
	DEBUG_FCMH(PLL_INFO, p, "name(%s)", name);
	
	if (fidc_child_try_validate(p, c, name))
		return;
	else
		/* Couldn't validate an existing namespace reference.
		 */
		fcc = fidc_new(p, c, name);

	psc_assert(fcc);

	/* Here's our atomic check+add onto the parent d_inode.
	 */
	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);
	if (!(tmp = fidc_child_lookup_int_locked(p, name))) {
		/* It doesn't yet exist, add it.
		 */
		psclist_xadd_tail(&fcc->fcc_lentry, &p->fcmh_children);	
		psc_assert(!c->fcmh_pri);
		c->fcmh_pri = fcc;
		DEBUG_FCMH(PLL_WARN, p, "fcc=%p fcc_name(%s) adding", 
			   fcc, fcc->fcc_name);
	} else {
		/* Someone beat us to the punch, do sanity checks and then
		 *  clean up.
		 */
		psc_assert(tmp->fcc_fcmh == c);
		psc_assert(tmp == c->fcmh_pri);
		atomic_dec(&tmp->fcc_ref);
		PSCFREE(fcc);
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);
}

