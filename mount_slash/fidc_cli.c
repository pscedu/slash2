/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "pfl/cdefs.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "psc_util/strlcpy.h"
#include "psc_util/time.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"

/**
 * fidc_new - create a new fni structure and initialize it using provided
 *     parameters.
 * @p: parent fcmh
 * @c: child fcmh
 * @name: name of child fcmh
 */
static struct fidc_nameinfo *
fidc_new(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fidc_nameinfo *fni;
	int len;

	len = strlen(name);
	if (len > NAME_MAX)
		psc_fatalx("name too long");
	len++;

	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	fni = PSCALLOC(sizeof(*fni) + len);
	fni->fni_hash   = str_hash(name);
	INIT_PSCLIST_ENTRY(&c->fcmh_sibling);
	fidc_gettime(&fni->fni_age);
	strlcpy(fni->fni_name, name, len);
	return (fni);
}

/**
 * fidc_child_prep_free_locked - if the fcmh is a directory then detach
 *	any cached fni's from fcmh_children.  This ensures that the
 *	children's fni_parent backpointer reference is properly erased.
 * @f:  the fcmh object to be freed.
 */
static void
fidc_child_prep_free_locked(struct fidc_membh *f)
{
	struct fidc_membh *c, *tmp;

	psclist_for_each_entry_safe(c, tmp, &f->fcmh_children, fcmh_sibling) {
		DEBUG_FCMH(PLL_WARN, c, "fidc_membh=%p name=%s detaching",
			   c, c->fcmh_name->fni_name);
		psc_assert(c->fcmh_parent == f);
		c->fcmh_parent = NULL;
		psclist_del(&c->fcmh_sibling);
	}
}

/**
 * fidc_child_free - release a child fni.  The parent must already be
 *	locked (plocked == 'parent locked') so that the fni may be freed
 *	from the parent's fcmh_children list.
 * @fni: the fni to be freed.
 */
static void
fidc_child_free_plocked(struct fidc_membh *c)
{
	int			 locked;
	struct fidc_nameinfo	*fni;

	locked = reqlock(&c->fcmh_lock);

	LOCK_ENSURE(&c->fcmh_lock);
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	if (c->fcmh_state & FCMH_ISDIR) {
		fidc_child_prep_free_locked(c);
		psc_assert(psclist_empty(&c->fcmh_children));
	}

	fni = c->fcmh_name;
	DEBUG_FCMH(PLL_WARN, c, "fni=%p name=%s parent=%p freeing "
		   "child_empty=%d",
		   fni, fni->fni_name, c->fcmh_parent,
		   ((c->fcmh_state & FCMH_ISDIR) ?
		    psclist_empty(&c->fcmh_children) : -1));

	c->fcmh_name = NULL;
	c->fcmh_parent = NULL;
	psclist_del(&c->fcmh_sibling);

	PSCFREE(fni);
	ureqlock(&c->fcmh_lock, locked);
}

/**
 * fidc_child_free_orphan_locked - free an fni which has no parent
 *	pointer (and hence, is an 'orphan').  The freeing process here
 *	is less involved than fidc_child_free_plocked() because no
 *	parent data structure needs to be managed.
 * @f: fcmh to be freed.
 */
static void
fidc_child_free_orphan_locked(struct fidc_membh *f)
{
	struct fidc_nameinfo *fni=f->fcmh_name;

	LOCK_ENSURE(&f->fcmh_lock);

	psc_assert(fni);
	psc_assert(!f->fcmh_parent);
	psc_assert(!(f->fcmh_state & FCMH_CAC_FREEING));
	f->fcmh_name = NULL;

	DEBUG_FCMH(PLL_WARN, f, "fni=%p name=%s freeing orphan",
		   fni, fni->fni_name);

	if (f->fcmh_state & FCMH_ISDIR)
		psc_assert(psclist_empty(&f->fcmh_children));

	PSCFREE(fni);
}

/**
 * fidc_child_try_validate - given a parent fcmh, child, and a name, try
 *	to validate the child's fni if one exists.  On success the fni
 *	timeout is increased and if the fni was orphaned then is
 *	reattached to the parent 'p'.
 * @p: parent fcmh
 * @c: child fcmh
 * @name: name of the fni entry associated with child.
 * Note: the locking order is [parent, child, child fni]
 */
static struct fidc_nameinfo *
fidc_child_try_validate(struct fidc_membh *p, struct fidc_membh *c,
			const char *name)
{
	struct fidc_nameinfo *fni;

	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(!(p->fcmh_state & FCMH_CAC_FREEING));
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	fni = c->fcmh_name;
	if (fni) {
		/* Both of these must always be true.
		 */
		if (strncmp(name, fni->fni_name, strnlen(name, NAME_MAX))) {
			/* This inode may have been renamed, remove
			 *  this fni.
			 */
			fidc_child_free_plocked(c);
			fni = NULL;
		} else {
			/* Increase the lifespan of this entry and return.
			 */
			fidc_gettime(&fni->fni_age);
			/* If the fni is 'connected', then its parent inode
			 *   must be 'p'.
			 */
			if (c->fcmh_parent) {
				psc_assert(c->fcmh_parent == p);
				psc_assert(psclist_conjoint(&c->fcmh_sibling));
			} else {
				c->fcmh_parent = p;
				psclist_xadd_tail(&c->fcmh_sibling, &p->fcmh_children);
				DEBUG_FCMH(PLL_WARN, p, "reattaching fni=%p",
					   fni);
			}
		}
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);

	return (fni);
}

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
	struct fidc_nameinfo *fni=f->fcmh_name;

	LOCK_ENSURE(&f->fcmh_lock);
	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);

	DEBUG_FCMH(PLL_WARN, f, "fni=%p fcmh_no_children=%d",
		   fni,
		   ((f->fcmh_state & FCMH_ISDIR) ?
		    psclist_empty(&f->fcmh_children) : -1));

	if (((f->fcmh_state & FCMH_ISDIR) &&
	     (!psclist_empty(&f->fcmh_children))))
		return (0);

	else if (!fni)
		return (1);

	else if (!f->fcmh_parent) {
		fidc_child_free_orphan_locked(f);
		return (1);

	} else {
		/* The parent needs to be unlocked after the fni is freed,
		 *  hence the need for the temp var 'p'.
		 */
		struct fidc_membh *p=f->fcmh_parent;

		psc_assert(p);
		/* This trylock technically violates lock ordering
		 *  (parent / child / fni) which is why we bail if the
		 *  parent lock cannot be obtained without blocking.
		 */
		if (trylock(&p->fcmh_lock)) {
			fidc_child_free_plocked(f);
			freelock(&p->fcmh_lock);
			return (1);
		}
	}
	return (0);
}

/**
 * fidc_child_get_int_locked - given a parent directory inode, try to
 *	locate a child.  If the fni is too old then it is freed and NULL
 *	is returned.
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_membh *
fidc_child_lookup_int_locked(struct fidc_membh *p, const char *name)
{
	struct fidc_membh *c=NULL;
	int found=0;
	int hash=str_hash(name);
	struct timespec	now;

	LOCK_ENSURE(&p->fcmh_lock);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(p->fcmh_state & FCMH_ISDIR);

	DEBUG_FCMH(PLL_INFO, p, "name %p (%s), hash=%d",
		   name, name, hash);

	psclist_for_each_entry(c, &p->fcmh_children, fcmh_sibling) {

		psc_traces(PSS_GEN, "p=fcmh@%p c=%p cname=%s hash=%d",
			   p, c, c->fcmh_name->fni_name, c->fcmh_name->fni_hash);

		if ((c->fcmh_name->fni_hash == hash) &&
		    (!strncmp(name, c->fcmh_name->fni_name, strnlen(name, NAME_MAX)))) {
			found = 1;
			psc_assert(c->fcmh_parent == p);
			fidc_membh_incref(c);
			break;
		}
	}
	if (!found || (c->fcmh_state & FCMH_CAC_FREEING))
		return (NULL);

	clock_gettime(CLOCK_REALTIME, &now);
	
	if (timespeccmp(&c->fcmh_name->fni_age, &now, <)) {
		/* It's old, remove it.
		 */
		fidc_child_free_plocked(c);
		fidc_membh_dropref(c);
		/* this will force an RPC to do the lookup */
		c = NULL;
	}
	return (c);
}

#if 0
int
fidc_child_cmp(const void *x, const void *y)
{
	const struct fidc_nameinfo *a=x, *b=y;
	if ((c->fni_hash == hash) &&
	    (!strncmp(name, c->fni_name, strnlen(name, NAME_MAX)))) {
		/* Pin down the fni while the parent dir lock is held.
		 */
		found=1;
		atomic_inc(&c->fni_ref);
		break;
	}

	if (a->

	if (a->bcm_blkno > b->bcm_blkno)
		return (1);
	else if (a->bcm_blkno < b->bcm_blkno)
		return (-1);
	return (0);
}

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
	struct fidc_membh *m=NULL;
	int locked=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	m = fidc_child_lookup_int_locked(p, name);
	ureqlock(&p->fcmh_lock, locked);
	return (m);
}

void
fidc_child_unlink(struct fidc_membh *p, const char *name)
{
	struct fidc_membh *c;
	int locked=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	c = fidc_child_lookup_int_locked(p, name);
	if (!c) {
		ureqlock(&p->fcmh_lock, locked);
		return;
	}
	/* Perform some sanity checks on the cached data structure.
	 */
	psc_assert(c->fcmh_parent == p);
	psc_assert(c->fcmh_name->fni_hash == str_hash(name));
	psc_assert(!strncmp(c->fcmh_name->fni_name, name,
			    strnlen(c->fcmh_name->fni_name, NAME_MAX)));

	/* The only ref on the fni should be the one taken above in
	 *  fidc_child_lookup_int_locked()
	 */
	psc_assert(atomic_dec_and_test(&c->fcmh_refcnt));
	fidc_child_free_plocked(c);

	ureqlock(&p->fcmh_lock, locked);
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
	struct fidc_nameinfo *fni;
	struct fidc_membh *tmp=NULL;

	psc_assert(p && c && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	DEBUG_FCMH(PLL_INFO, p, "name(%s)", name);

	if (fidc_child_try_validate(p, c, name))
		return;

	/* Couldn't validate an existing namespace reference.
	 */
	fni = fidc_new(p, c, name);

	psc_assert(fni);

	/* Here's our atomic check+add onto the parent d_inode.
	 */
	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);
	if (!(tmp = fidc_child_lookup_int_locked(p, name))) {
		/* It doesn't yet exist, add it.
		 */
		c->fcmh_name = fni;
		c->fcmh_parent = p;
		psclist_xadd_tail(&c->fcmh_sibling, &p->fcmh_children);
		DEBUG_FCMH(PLL_WARN, p, "fni=%p, adding name: %s", fni, fni->fni_name);
	} else {
		/* Someone beat us to the punch, do sanity checks and then
		 *  clean up.
		 */
		fidc_membh_dropref(tmp);
		PSCFREE(fni);
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);
}

void
fidc_child_rename(struct fidc_membh *op, const char *oldname,
    struct fidc_membh *np, const char *newname)
{
	struct fidc_membh *ch;
	struct fidc_nameinfo *fni;
	size_t len;

	len = strlen(newname);
	if (len > NAME_MAX)
		psc_fatalx("name too long");
	len++;

	spinlock(&op->fcmh_lock);
	ch = fidc_child_lookup_int_locked(op, oldname);
	if (ch) {
		spinlock(&ch->fcmh_lock);
		ch->fcmh_parent = NULL;
		psclist_del(&ch->fcmh_sibling);
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

	fni = ch->fcmh_name;

	/* overwrite the old name with the new one in place */
	psc_assert(ch->fcmh_name != NULL);
	fni = ch->fcmh_name = psc_realloc(fni, sizeof(*fni) + len, 0);
	fni->fni_hash = str_hash(newname);
	strlcpy(fni->fni_name, newname, len);

	spinlock(&np->fcmh_lock);
	ch->fcmh_parent = np;
	psclist_xadd_tail(&ch->fcmh_sibling, &np->fcmh_children);
	freelock(&np->fcmh_lock);

	freelock(&ch->fcmh_lock);

	DEBUG_FCMH(PLL_WARN, ch, "fni=%p, rename file: %s -->  %s", fni, oldname, newname);
}
