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
void
fidc_child_free(struct fidc_child *c)
{
	int l;

	l = reqlock(&c->fcc_parent->fcmh_lock);
	spinlock(c->fcc_fcmh->fcmh_lock);
	psc_assert(c->fcc_fcmh->fcmh_pri == c);
	c->fcc_fcmh->fcmh_pri = NULL;
	freelock(c->fcc_fcmh->fcmh_lock);

	psclist_del(&c->fcc_lentry);
	DEBUG_FCMH(PLL_INFO, p, "fcc=%p name=%s parent=%p", 
		   c, c->fcc_name, c->fcc_parent);
	ureqlock(&c->fcc_parent->fcmh_lock, l);

	PSCFREE(c->fcc_name);
	PSCFREE(c);	
}

int
fidc_child_reap_cb(struct fidc_membh *f)
{
	struct fidc_child *c=f->fcmh_pri;
	int locked;

	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);
	
	if (!c)
		return (0);

	if (atomic_read(&f->fcc_ref)) {
		DEBUG_FCMH(PLL_WARN, "fcc=%p ref=%d", 
			   c, atomic_read(&f->fcc_ref));
		return (1);
	}       
	/* Don't free directories which still have child ref's.
	 */
	if ((f->fcmh_state & FCMH_ISDIR) && !psclist_empty(&f->fcmh_children))
		return (1);

	if (tryreqlock(&c->fcc_parent->fcmh_lock, &locked)) {
		/* Save parent's fcmh pointer, it will be NULL'd
		 *  in fidc_child_free().
		 */
		struct fidc_membh *tmp=c->fcc_parent;
		
		fidc_child_free(c);
		ureqlock(&tmp->fcmh_lock, locked);
		return (0);
	} else
		/* The parent is busy, don't wait for the lock.
		 */
		return (1);
}

int
fidc_child_wait_locked(struct fidc_membh *p, struct fidc_child *fcc)
{
	int lock=0;

	LOCK_ENSURE(&p->fcmh_lock);
	psc_assert(atomic_read(&fcc->fcc_ref) >= 0);
 retry:
	if (lock)
		spinlock(&p->fcmh_lock);
	
	if (fcc->fcc_len < 0) {
		abort();		       
		/* The fcc is bad.
		 */
		psc_warnx("bad fcc=%p, ref=%d", 
			  fcc, atomic_read(&fcc->fcc_ref));
		if (atomic_dec_and_test(&fcc->fcc_ref))
			fidc_child_free(fcc);
		return (-1);
	}

	if (!fcc->fcc_fcmh) {
		lock = 1;
		/* Another create is in progress.  Remember, psc_waitq_wait()
		 *  will release fcmh_lock.
		 */
		psc_waitq_wait(&p->fcmh_waitq, &p->fcmh_lock);
		goto retry;
	}
	/* Reaquire the lock.
	 */
	spinlock(&p->fcmh_lock);
	return (0);
}

void
fidc_child_fail(struct fidc_child *fcc)
{
	/* Signify that the fcc is bad.
	 */
	psc_assert(atomic_read(&fcc->fcc_ref) >= 0);
	if (!atomic_read(&fcc->fcc_ref))
		fidc_child_free(fcc);	

	else {		
		fcc->fcc_len = -1;
		psc_waitq_wakeall(&fcc->fcc_parent->fcmh_waitq);
	}	
}

void
fidc_child_add_fcmh(struct fidc_child *fcc, struct fidc_membh *c)
{
	/* Verify that our ref counts are non-zero, both should 
	 *  be pinned for this call.
	 */
	psc_assert(atomic_read(&fcc->fcc_ref) > 0);
	psc_assert(atomic_read(&c->fcmh_ref) > 0);

	c->fcmh_pri = fcc;
	fcc->fcc_fcmh = c;
	atomic_dec(&c->fcc_ref);

	psc_waitq_wakeall(&fcc->fcc_parent->fcmh_waitq);
}

/**
 * fidc_child_get_int_locked - given a parent directory inode, try to locate a child. 
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_child * 
fidc_child_get_int_locked(struct fidc_membh *parent, const char *name, 
			  size_t len)
{
	struct fidc_child *c=NULL;
	struct fidc_membh *m=NULL;
	int found=0;

	LOCK_ENSURE(&parent->fcmh_lock);

	psc_assert(parent->fcmh_state & FCMH_ISDIR);
	DEBUG_FCMH(PLL_INFO, parent, "name %p (%s), len %zu", 
		   name, name, len);

	psclist_for_each_entry(c, &parent->fcmh_children, fcc_lentry) {
		
		psc_traces(PSS_OTHER, "parent=fcmh@%p c=%p cname=%s clen=%zd",
			   parent, c, c->fcc_name, c->fcc_len);

		if ((c->fcc_len == (ssize_t)len) &&
		    (!strncmp(name, c->fcc_name, len))) {
			found=1;
			/* Pin down the fcc while the parent dir lock is held.
			 */
			atomic_inc(&c->fcc_ref);
			break;
		}
	}
	if (found) {
		/* The ref take above guarantees that no one will free us.
		 *  fidc_child_wait_locked() ensures that the fcmh is present.
		 */
		psc_assert(!fidc_child_wait_locked(parent, c));
		psc_assert(c->fcc_fcmh);
		psc_assert(atomic_read(&c->fcc_ref >= 0));
	}
	ureqlock(&parent->fcmh_lock, l);

	return (m);
}

struct fidc_membh *
fidc_child_get(struct fidc_membh *parent, const char *name, size_t len)
{
	struct fidc_child *c=NULL;	
	struct fidc_membh *m=NULL;

	c = fidc_child_get_int(parent, name, len);
	if (c) { 
		m = c->fcc_fcmh;
		/* Ref the inode and then decref the fcc so that the fcc
		 *  is not freed from under us.
		 */
		fidc_membh_incref(m);
		atomic_dec(&c->fcc_ref);
		psc_assert(atomic_read(&c->fcc_ref >= 0));
	}

	return (m);
}

/** fidc_child_add - add the child inode / name pair to its parent's child list.
 * @p: the parent inode handle.
 * @c: the child inode.
 * @name: the name of the child.
 */
struct fidc_child *
fidc_child_add(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fidc_child *fcc, *tmp;
	struct fidc_membh *m;

	psc_assert(p && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);

	fcc = PSCALLOC(sizeof(*fcc));
	atomic_set(&fcc->fcc_ref, 0);
	fcc->fcc_fcmh   = c; /* Note: 'c' may be NULL */
	fcc->fcc_parent = p;
	fcc->fcc_len    = strnlen(name, NAME_MAX);
	fcc->fcc_name   = PSCALLOC(fcc->fcc_len + 1);

	strncpy(fcc->fcc_name, name, strlen(name));
	fcc->fcc_name[fcc->fcc_len] = '\0';

	DEBUG_FCMH(PLL_INFO, parent, "fcc=%p fcc_name(%s)", 
		   fcc, fcc->fcc_name);
	/* Here's our atomic check / add onto the parent d_inode.
	 */
	spinlock(&p->fcmh_lock);
	tmp = fidc_child_get_int_locked(p, name, strnlen(name, NAME_MAX));
	if (tmp) {
		fcc = tmp;
		psc_assert(!fcc->fcc_fcmh);
		/* It doesn't yet exist, add it.
		 */
		psclist_xadd_tail(&fcc->fcc_lentry, &p->fcmh_children);	

	} else {
		/* Someone beat us to the punch.
		 */
		PSCFREE(fcc->fcc_name);
		PSCFREE(fcc);
		fcc = tmp;
	}
	freelock(&p->fcmh_lock);
	psc_assert(atomic_read(&fcc->fcc_ref > 0));

	return (fcc);
}


/* fidc_child_add - add the child inode / name pair to its parent's child list.
 * @p: the parent inode handle.
 * @c: the child inode.
 * @name: the name of the child.
 */
int
fidc_child_rename(slfid_t op, const char *oldname, 
		  slfid_t np, const char *newname)
{
	int rc=0;
	struct fidc_membh *o, *n;
	struct fidc_child *fcc=NULL;

	psc_assert(strlen(oldname) && strlen(newname));
	psc_assert(strncmp(newname, ".", 1));
	psc_assert(strncmp(oldname, "..", 2));	       

	o = fidc_lookup_inode(op);
	if (!o)
		/* If there's no old directory then just punt.
		 */
		return -ENOENT;

	/* The destination dir is optional.
	 */
	n = fidc_lookup_inode(np);
	
	for (;;) {
		spinlock(&o->fcmh_lock);
		if (!fcc)
			fcc = fidc_child_get(o, oldname, strlen(oldname));
		if (fcc) {
			if (n) {
				if (trylock(&n->fcmh_lock)) {
					fidc_child_add(n, fcc->fcc_fcmh, 
						       newname);
					freelock(&n->fcmh_lock);	
				} else {
					freelock(&o->fcmh_lock);
					continue;
				}
			}			
			fidc_child_free(fcc);
			fidc_membh_dropref(fcc->fcc_fcmh);
			freelock(&o->fcmh_lock);
			break;

		} else {
			rc = -ENOENT;
			break;
		}
	}
	return (rc);
}

