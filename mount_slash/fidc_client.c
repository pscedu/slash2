/* $Id: fidcache.c 4191 2008-09-17 19:27:30Z yanovich $ */

#include <stdio.h>
#include <sys/stat.h>
#define __USE_GNU
#define _GNU_SOURCE
#include <string.h>
#undef __USE_GNU
#undef  _GNU_SOURCE

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

void
fidc_fcm_setattr(struct fidc_membh *fcmh, struct stat *stb)
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
 * fidc_child_free - release a child.
 */
void
fidc_child_free(struct fidc_membh *p, struct fidc_child *c)
{
	int l=reqlock(&p->fcmh_lock);

	psc_assert(c->fcc_fcmh->fcmh_pri == c);
	c->fcc_fcmh->fcmh_pri = NULL;
	/* reqlock p->fcmh_lock */
	psclist_del(&c->fcc_lentry); /* XXX lock the parent? */
	ureqlock(&p->fcmh_lock, l);

	DEBUG_FCMH(PLL_INFO, p, "<-- p fidc_child_free()!!");

	PSCFREE(c->fcc_name);
	PSCFREE(c);	
}

int
fidc_child_reap_cb(struct fidc_membh *f)
{
	struct fidc_child *c=f->fcmh_pri;

	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);
	
	if (!c)
		return (0);
	
	/* Don't free directories which still have child ref's.
	 */
	if ((f->fcmh_state & FCMH_ISDIR) && 
	    !psclist_empty(&f->fcmh_children))
		return (1);

	if (trylock(&c->fcc_parent->fcmh_lock)) {
		struct fidc_membh *tmp=c->fcc_parent;
		
		fidc_child_free(c->fcc_parent, c);
		freelock(&tmp->fcmh_lock);
		return (0);
	} else
		/* The parent is busy, don't wait for the lock.
		 */
		return (1);
}

int
fidc_child_wait_locked(struct fidc_membh *p, struct fidc_child *fcc)
{
	int lock=0, rc=-1;

	psc_assert(atomic_read(&fcc->fcc_ref) >= 0);
	atomic_inc(&fcc->fcc_ref);
 retry:
	if (lock)
		spinlock(&p->fcmh_lock);
	
	if (fcc->fcc_len < 0) {
		/* The fcc is bad.
		 */
		psc_warnx("bad fcc=%p, ref=%d", 
			  fcc, atomic_read(&fcc->fcc_ref));
		if (atomic_dec_and_test(&fcc->fcc_ref))
			fidc_child_free(p, fcc);
		goto out;
	}

	if (!fcc->fcc_fcmh) {
		lock = 1;
		/* Another create is in progress.  Remember, psc_waitq_wait()
		 *  will release fcmh_lock.
		 */		
		psc_waitq_wait(&p->fcmh_waitq, &p->fcmh_lock);
		goto retry;
	} 
	rc = 0;
	atomic_dec(&fcc->fcc_ref);
 out:
	spinlock(&p->fcmh_lock);
	return (rc);
}

void
fidc_child_fail(struct fidc_child *fcc)
{
	/* Signify that the fcc is bad.
	 */
	psc_assert(atomic_read(&fcc->fcc_ref) >= 0);
	if (!atomic_read(&fcc->fcc_ref))
		fidc_child_free(fcc->fcc_parent, fcc);	

	else {		
		fcc->fcc_len = -1;
		psc_waitq_wakeall(&fcc->fcc_parent->fcmh_waitq);
	}	
}

void
fidc_child_add_fcmh(struct fidc_child *fcc, struct fidc_membh *c)
{
	spinlock(&c->fcmh_lock);
	c->fcmh_pri = fcc;
	freelock(&c->fcmh_lock);
	fcc->fcc_fcmh = c;
	psc_waitq_wakeall(&fcc->fcc_parent->fcmh_waitq);
}

/**
 * fidc_child_get - given a parent directory inode, try to locate a child. 
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
struct fidc_child * 
fidc_child_get(struct fidc_membh *parent, const char *name, size_t len)
{
	struct fidc_child *c=NULL;
	int found=0, l=reqlock(&parent->fcmh_lock);
	
	psc_assert(parent->fcmh_state & FCMH_ISDIR);

	psc_traces(PSS_OTHER, "parent=fcmh@%p name %p (%s), len %zu", 
		   parent, name, name, len);

	psclist_for_each_entry(c, &parent->fcmh_children, fcc_lentry) {
		
		psc_traces(PSS_OTHER, "parent=fcmh@%p c=%p cname=%s clen=%zd",
			   parent, c, c->fcc_name, c->fcc_len);

		if ((c->fcc_len == (ssize_t)len) &&
		    (!strncmp(name, c->fcc_name, len))) {
			found = 1;
			fidc_membh_incref(c->fcc_fcmh);
			break;
		}
	}
	ureqlock(&parent->fcmh_lock, l);	
	return (found ? c : NULL);
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
	int l;

	psc_assert(p && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);

	fcc = PSCALLOC(sizeof(*fcc));	
	atomic_set(&fcc->fcc_ref, 0);
	fcc->fcc_fcmh = c;
	fcc->fcc_parent = p;
	fcc->fcc_len = strnlen(name, NAME_MAX);
	fcc->fcc_name = PSCALLOC(fcc->fcc_len + 1);

	strncpy(fcc->fcc_name, name, strlen(name));
	fcc->fcc_name[fcc->fcc_len] = '\0';
	if (c)
		c->fcmh_pri = fcc;

	psc_infos(PSS_OTHER, "parent=fcmh@%p name (%s) fcc=%p fcc_name(%s)", 
		  p, name, fcc, fcc->fcc_name);

	l = reqlock(&p->fcmh_lock);
	tmp = fidc_child_get(p, name, strnlen(name, NAME_MAX));
	if (!tmp)
		psclist_xadd_tail(&fcc->fcc_lentry, &p->fcmh_children);	
	else {
		psc_assert(c == tmp->fcc_fcmh);
		/* XXX this may not work correctly for hardlinked files!!
		 */
		psc_assert(p == tmp->fcc_parent);
	}
	ureqlock(&p->fcmh_lock, l);
	
	if (tmp) {
		/* Cleanup.
		 */
		PSCFREE(fcc->fcc_name);
		PSCFREE(fcc);
	}

	return (tmp ? tmp : fcc);
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
			fidc_child_free(o, fcc);
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

