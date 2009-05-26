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

/**
 * fidc_child_free - release a child.
 */
static void
fidc_child_free_int_locked(struct fidc_child *fcc)
{
	struct fidc_membh *c=fcc->fcc_fcmh;
	int l=reqlock(&c->fcmh_lock);

	LOCK_ENSURE(&fcc->fcc_parent->fcmh_lock);
	
	psclist_del(&fcc->fcc_lentry);
	psc_assert(c->fcmh_pri == fcc);
	psc_assert(!atomic_read(&fcc->fcc_ref));
	c->fcmh_pri = NULL;

	DEBUG_FCMH(PLL_INFO, c, "fcc=%p name=%s parent=%p freeing", 
		   fcc, fcc->fcc_name, fcc->fcc_parent);

	PSCFREE(fcc);
	ureqlock(&c->fcmh_lock, l);
}

int
fidc_child_reap_cb(struct fidc_membh *f)
{
	struct fidc_child *c=f->fcmh_pri;
	struct fidc_membh *p;
	int locked;

	
	LOCK_ENSURE(&f->fcmh_lock);
	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);
	
	if (!c)
		return (0);
	/* Don't free directories which still have child ref's.
	 */
	if ((f->fcmh_state & FCMH_ISDIR) && !psclist_empty(&f->fcmh_children))
		return (1);

	if (atomic_read(&c->fcc_ref)) {
		DEBUG_FCMH(PLL_WARN, f, "fcc=%p ref=%d",
			   c, atomic_read(&c->fcc_ref));
		return (1);
	}
	p = c->fcc_parent;
	psc_assert(p);
	if (tryreqlock(&p->fcmh_lock, &locked)) {
		fidc_child_free_int_locked(c);
		ureqlock(&p->fcmh_lock, locked);
		return (0);
	} else
		/* The parent is busy, don't wait for the lock.
		 */
		return (1);
}


/**
 * fidc_child_get_int_locked - given a parent directory inode, try to locate a child. 
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_child *
fidc_child_lookup_int_locked(struct fidc_membh *parent, const char *name)
{
	struct fidc_child *c=NULL;
	int found=0;
	int hash=str_hash(name);

	LOCK_ENSURE(&parent->fcmh_lock);
	psc_assert(atomic_read(&parent->fcmh_refcnt) > 0);
	psc_assert(parent->fcmh_state & FCMH_ISDIR);

	DEBUG_FCMH(PLL_INFO, parent, "name %p (%s), hash=%d",
		   name, name, hash);

	psclist_for_each_entry(c, &parent->fcmh_children, fcc_lentry) {
		
		psc_traces(PSS_OTHER, "parent=fcmh@%p c=%p cname=%s hash=%d",
			   parent, c, c->fcc_name, c->fcc_hash);

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
		psc_assert(atomic_read(&c->fcc_ref) > 0);
		psc_assert(c->fcc_fcmh);
		if (c->fcc_age < fidc_gettime()) {
			/* It's old, remove it.
			 */
			atomic_dec(&c->fcc_ref);
			fidc_child_free_int_locked(c);
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
	struct fidc_membh *c;
	int l=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	fcc = fidc_child_lookup_int_locked(p, name);
	if (!fcc) {
		freelock(&p->fcmh_lock);
		return;
	}
	
	c = fcc->fcc_fcmh;
	/* Note the locking order here - parent then child.
	 */
	spinlock(&c->fcmh_lock);
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
	/* Remove the fcc from the list parent's list and delete its
	 *  reference in the in child's inode.
	 */
	psclist_del(&fcc->fcc_lentry);	
	c->fcmh_pri = NULL;

	freelock(&c->fcmh_lock);

	PSCFREE(fcc);

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
	size_t len=strnlen(name, NAME_MAX);

	psc_assert(p && c && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);
 	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
       
	DEBUG_FCMH(PLL_INFO, p, "name(%s)", name);

	/* Does this fcmh already have an fcc assigned to it?
	 */
	spinlock(&c->fcmh_lock);
	if ((tmp = (struct fidc_child *)c->fcmh_pri)) {
		psc_assert(tmp->fcc_fcmh == c);
		psc_assert(tmp->fcc_parent == p);
		/* Increase the lifespan of this entry and return.
		 */
		fidc_settimeo(tmp->fcc_age);
	}
	freelock(&c->fcmh_lock);
	if (tmp)
		return;
	    
	fcc = PSCALLOC(sizeof(*fcc) + (len + 1));
	atomic_set(&fcc->fcc_ref, 0);
	fcc->fcc_fcmh   = c;
	fcc->fcc_parent = p;
	fcc->fcc_hash   = str_hash(name);
	fidc_settimeo(fcc->fcc_age);
	strncpy(fcc->fcc_name, name, len);
	fcc->fcc_name[len] = '\0';

	DEBUG_FCMH(PLL_INFO, p, "fcc=%p fcc_name(%s) try add", 
		   fcc, fcc->fcc_name);
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
		DEBUG_FCMH(PLL_INFO, p, "fcc=%p fcc_name(%s) adding", 
			   fcc, fcc->fcc_name);
	} else {
		/* Someone beat us to the punch, do sanity checks and then
		 *  clean up.
		 */
		psc_assert(tmp->fcc_fcmh == c);
		psc_assert(tmp->fcc_parent == p);
		psc_assert(tmp == c->fcmh_pri);
		atomic_dec(&tmp->fcc_ref);
		PSCFREE(fcc);
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);
}

