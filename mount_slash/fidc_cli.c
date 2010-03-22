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

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "pfl/cdefs.h"
#include "psc_ds/hash2.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rsx.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/strlcpy.h"
#include "psc_util/time.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"

#define FCMH_FOREACH_CHILD(c, p)						\
	psclist_for_each_entry2((c), &fcmh_2_fci(p)->fci_children,		\
	    sizeof(struct fidc_membh) +	offsetof(struct fcmh_cli_info, fci_sibling))

#define FCMH_FOREACH_CHILD_SAFE(c0, cn, p)					\
	psclist_for_each_entry2_safe((c0), (cn),				\
	    &fcmh_2_fci(p)->fci_children, sizeof(struct fidc_membh) +		\
	    offsetof(struct fcmh_cli_info, fci_sibling))			\


/**
 * fidc_child_reap_cb - the callback handler for fidc_reap() is
 *	responsible for notifying the fcmh reaper if the fcmh is
 *	eligible for reaping.
 * @f: the fcmh which is trying to be freed.
 * Returns: '1' if reapable, '0' if unreapable.
 */
int
fidc_child_reap_cb(struct fidc_membh *f)
{
	struct fcmh_cli_info *fci;

	LOCK_ENSURE(&f->fcmh_lock);

	fci = fcmh_get_pri(f);

	/*
	 * Keep a directory around if it has any children.  We can do
	 * better by checking any of its children (and grandchildren)
	 * is actually in use.  But doing that requires a global lock.
	 */
	if (fcmh_isdir(f) && !psclist_empty(&fci->fci_children))
		return (0);

	DEBUG_FCMH(PLL_WARN, f, "reaping: %s (%s)", fci->fci_name,
	    fcmh_isdir(f) ? "dir": "file");

	return (1);
}

/**
 * fidc_child_get_int_locked - given a parent directory inode, try to
 *	locate a child.  If the child is too old then it is freed and NULL
 *	is returned.
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_membh *
fidc_child_lookup_int_locked(struct fidc_membh *p, const char *name)
{
	int found=0, hash=psc_str_hashify(name);
	struct fcmh_cli_info *fci;
	struct fidc_membh *c;
	struct timeval now;

	LOCK_ENSURE(&p->fcmh_lock);
	psc_assert(p->fcmh_refcnt > 0);
	psc_assert(fcmh_isdir(p));

	DEBUG_FCMH(PLL_INFO, p, "name %p (%s), hash=%d",
	    name, name, hash);

	/* 
	 * Note we can be racing with the reaper because we don't
	 * use reference count to protect parent-child relationship.
	 */
	FCMH_FOREACH_CHILD(c, p) {
		spinlock(&c->fcmh_lock);
		if (c->fcmh_state & FCMH_CAC_FREEING) {
			freelock(&c->fcmh_lock);
			continue;
		}
		fci = fcmh_get_pri(c);

		psc_traces(PSS_GEN, "p=fcmh@%p c=%p cname=%s hash=%d",
		    p, c, fci->fci_name, fci->fci_hash);

		if (fci->fci_hash == hash &&
		    strcmp(name, fci->fci_name) == 0) {
			found = 1;
			psc_assert(fci->fci_parent == p);
			break;
		}
		freelock(&c->fcmh_lock);
	}
	if (c == NULL)
		return (NULL);

	PFL_GETTIME(&now);
	if (timercmp(&now, &c->fcmh_age, <)) {
		fcmh_op_start_type(c, FCMH_OPCNT_LOOKUP_PARENT);
		freelock(&c->fcmh_lock);
	} else {
		freelock(&c->fcmh_lock);
		c = NULL;
	}
	return (c);
}

/**
 * fidc_child_lookup - search the parent inode for the child entry 'name'.
 * @p:  parent fcmh.
 * @name: the filename for which is being searched
 * Notes:  the parent fcmh is ref'd and must be decref'd by the caller.
 */
struct fidc_membh *
fidc_child_lookup(struct fidc_membh *p, const char *name)
{
	struct fidc_membh *m=NULL;
	int locked=reqlock(&p->fcmh_lock);

	psc_assert(fcmh_isdir(p));
	psc_assert(p->fcmh_refcnt > 0);

	m = fidc_child_lookup_int_locked(p, name);
	ureqlock(&p->fcmh_lock, locked);
	return (m);
}

void
fidc_child_unlink(struct fidc_membh *p, const char *name)
{
	struct fcmh_cli_info *fci;
	struct fidc_membh *c;
	struct fidc_membh *tmp1;
	struct fidc_membh *tmp2;

	FCMH_LOCK(p);
	c = fidc_child_lookup_int_locked(p, name);
	FCMH_ULOCK(p);

	if (!c)
		return;

	spinlock(&c->fcmh_lock);

	/* Perform some sanity checks on the cached data structure. */
	fci = fcmh_get_pri(c);
	psc_assert(fci->fci_parent == p);
	psc_assert(fci->fci_hash == psc_str_hashify(name));
	psc_assert(strcmp(fci->fci_name, name) == 0);

	/* detach myself from my parent */
	fci->fci_parent = NULL;
	psclist_del(&fci->fci_sibling);
	
	/*
	 * If we can unlink this directory, then it should not have any children.
	 * Perhaps a unlink can bypass our cache.
	 */
	if (fcmh_isdir(c)) {
		FCMH_FOREACH_CHILD_SAFE(tmp1, tmp2, c) {
			spinlock(&tmp1->fcmh_lock);
			if (tmp1->fcmh_state & FCMH_CAC_FREEING) {
				freelock(&tmp1->fcmh_lock);
				continue;
			}
			fci = fcmh_get_pri(tmp1);
			DEBUG_FCMH(PLL_WARN, tmp1, "fidc_membh=%p name=%s detaching", 
			    tmp1, fci->fci_name);
			psc_assert(fci->fci_parent == c);
			fci->fci_parent = NULL;
			psclist_del(&fci->fci_sibling);
			freelock(&tmp1->fcmh_lock);
		}
	}

	/* The only ref on the child should be the one taken above in
	 *  fidc_child_lookup_int_locked()
	 */
	fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_PARENT);
	psc_assert(!c->fcmh_refcnt);

	freelock(&c->fcmh_lock);
}

/**
 * fidc_child_add - add the child inode/name pair to its parent's child list.
 * @p: the parent inode handle.
 * @c: the child inode.
 * @name: the name of the child.
 */
void
fidc_child_add(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fcmh_cli_info *fci, *pci;

	DEBUG_FCMH(PLL_INFO, p, "name(%s)", name);

	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);

	fci = fcmh_get_pri(c);
	if (fci->fci_parent == NULL) {
		/* It doesn't yet exist, add it. */
		fci->fci_parent = p;
		fci->fci_name = psc_strdup(name);
		fci->fci_hash = psc_str_hashify(name);

		pci = fcmh_get_pri(p);
		psclist_xadd_tail(&fci->fci_sibling, &pci->fci_children);
		DEBUG_FCMH(PLL_WARN, p, "adding name: %s", fci->fci_name);
	} else 
		psc_assert(fci->fci_parent == p);

	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);
}

void
fidc_child_rename(struct fidc_membh *op, const char *oldname,
    struct fidc_membh *np, const char *newname)
{
	struct fcmh_cli_info *fci, *pci;
	struct fidc_membh *ch;
	size_t len;

	pci = fcmh_get_pri(np);

	len = strlen(newname);
	if (len > NAME_MAX)
		psc_fatalx("name too long");
	len++;

	spinlock(&op->fcmh_lock);
	ch = fidc_child_lookup_int_locked(op, oldname);
	fci = fcmh_get_pri(ch);
	if (ch) {
		spinlock(&ch->fcmh_lock);
		fci->fci_parent = NULL;
		psclist_del(&fci->fci_sibling);
	}
	freelock(&op->fcmh_lock);

	/*
	 * At this point, the rename RPC has been successful and we somehow
	 * screw up locally. How could this happen?
	 */
	if (ch == NULL) {
		psc_warnx("missing source file %s in a rename", oldname);
		return;
	}

	/* overwrite the old name with the new one in place */
	psc_assert(fci->fci_name);
	fci->fci_name = psc_realloc(fci->fci_name, len, 0);
	fci->fci_hash = psc_str_hashify(newname);
	strlcpy(fci->fci_name, newname, len);

	spinlock(&np->fcmh_lock);
	fci->fci_parent = np;
	psclist_xadd_tail(&fci->fci_sibling, &pci->fci_children);
	freelock(&np->fcmh_lock);

	fcmh_op_done_type(ch, FCMH_OPCNT_LOOKUP_PARENT);
	freelock(&ch->fcmh_lock);

	DEBUG_FCMH(PLL_WARN, ch, "rename file: "
	    "%s op(i+g:%"PRId64"+""%"PRId64") --> "
	    "%s np(i+g:%"PRId64"+""%"PRId64")",
	    oldname, fcmh_2_fid(op), fcmh_2_gen(op),
	    newname, fcmh_2_fid(np), fcmh_2_gen(np));
}

/**
 * fcmh_setlocalsize - Apply a local WRITE update to a fid cache member
 *	handle.
 */
void
fcmh_setlocalsize(struct fidc_membh *h, uint64_t size)
{
	int locked;

	locked = reqlock(&h->fcmh_lock);
	if (size > fcmh_2_fsz(h))
		fcmh_2_fsz(h) = size;
	ureqlock(&h->fcmh_lock, locked);
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

int
slc_fcmh_ctor(struct fidc_membh *fcmh)
{
	struct fcmh_cli_info *fci;

	fci = fcmh_get_pri(fcmh);
	memset(fci, 0, sizeof(struct fcmh_cli_info));
	INIT_PSCLIST_ENTRY(&fci->fci_sibling);
	INIT_PSCLIST_HEAD(&fci->fci_children);
	return (0);
}

/* destructor function called by fcmh_destroy() */
void
slc_fcmh_dtor(struct fidc_membh *fcmh)
{
	struct fcmh_cli_info *fci;
	struct fidc_membh *p;

	fci = fcmh_2_fci(fcmh);
	p = fci->fci_parent;
	/*
	 * The reaper should never reclaim a directory that
	 * has one or more children.
	 */
	if (p) {
		spinlock(&p->fcmh_lock);
		fci->fci_parent = NULL;
		psclist_del(&fci->fci_sibling);
		freelock(&p->fcmh_lock);
	}
	psc_assert(psclist_empty(&fci->fci_children));
	PSCFREE(fci->fci_name);
}

int
slc_fcmh_getattr(struct fidc_membh *fcmh)
{
	return (slash2fuse_stat(fcmh, &rootcreds));
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */	slc_fcmh_ctor,
/* dtor */	slc_fcmh_dtor,
/* getattr */	slc_fcmh_getattr
};
