/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/time.h"
#include "psc_util/alloc.h"
#include "psc_util/atomic.h"
#include "pfl/cdefs.h"
#include "psc_util/strlcpy.h"

#include "cache_params.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"

/**
 * fidc_new - create a new fcc structure and initialize it using provided
 *     parameters.
 * @p: parent fcmh
 * @c: child fcmh
 * @name: name of child fcmh
 */
static struct fidc_private *
fidc_new(struct fidc_membh *p, struct fidc_membh *c, const char *name)
{
	struct fidc_private *fcc;
	int len;

	len = strlen(name);
	if (len > NAME_MAX)
		psc_fatalx("name too long");
	len++;

	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	fcc = PSCALLOC(sizeof(*fcc) + len);
	fcc->fcc_fg.fg_fid = fcmh_2_fid(c);
	fcc->fcc_fg.fg_gen = fcmh_2_gen(c);
	fcc->fcc_fcmh   = c;
	fcc->fcc_parent = p;
	fcc->fcc_hash   = str_hash(name);
	INIT_PSCLIST_ENTRY(&fcc->fcc_lentry);
	fidc_gettime(&fcc->fcc_age);
	strlcpy(fcc->fcc_name, name, len);
	return (fcc);
}

/**
 * fidc_child_prep_free_locked - if the fcmh is a directory then detach
 *	any cached fcc's from fcmh_children.  This ensures that the
 *	children's fcc_parent backpointer reference is properly erased.
 * @f:  the fcmh object to be freed.
 */
static void
fidc_child_prep_free_locked(struct fidc_membh *f)
{
	struct fidc_private *fcc, *tmp;

	psclist_for_each_entry_safe(fcc, tmp, &f->fcmh_children, fcc_lentry) {
		DEBUG_FCMH(PLL_WARN, f, "fcc=%p fcc_name=%s detaching",
			   f, fcc->fcc_name);
		psc_assert(fcc->fcc_parent == f);
		fcc->fcc_parent = NULL;
		psclist_del(&fcc->fcc_lentry);
	}
}

/**
 * fidc_child_free - release a child fcc.  The parent must already be
 *	locked (plocked == 'parent locked') so that the fcc may be freed
 *	from the parent's fcmh_children list.
 * @fcc: the fcc to be freed.
 */
static void
fidc_child_free_plocked(struct fidc_private *fcc)
{
	struct fidc_membh	*c;
	int			 locked;

	c = fcc->fcc_fcmh;
	locked = reqlock(&c->fcmh_lock);

	LOCK_ENSURE(&fcc->fcc_parent->fcmh_lock);
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	if (c->fcmh_state & FCMH_ISDIR) {
		fidc_child_prep_free_locked(c);
		psc_assert(psclist_empty(&c->fcmh_children));
	}

	psclist_del(&fcc->fcc_lentry);
	psc_assert(c->fcmh_pri == fcc);
	c->fcmh_pri = NULL;

	DEBUG_FCMH(PLL_WARN, c, "fcc=%p name=%s parent=%p freeing "
		   "child_empty=%d",
		   fcc, fcc->fcc_name, fcc->fcc_parent,
		   ((c->fcmh_state & FCMH_ISDIR) ?
		    psclist_empty(&c->fcmh_children) : -1));

	PSCFREE(fcc);
	ureqlock(&c->fcmh_lock, locked);
}

/**
 * fidc_child_free_orphan_locked - free an fcc which has no parent
 *	pointer (and hence, is an 'orphan').  The freeing process here
 *	is less involved than fidc_child_free_plocked() because no
 *	parent data structure needs to be managed.
 * @f: fcmh to be freed.
 */
static void
fidc_child_free_orphan_locked(struct fidc_membh *f)
{
	struct fidc_private *fcc=f->fcmh_pri;

	LOCK_ENSURE(&f->fcmh_lock);

	psc_assert(fcc);
	psc_assert(!fcc->fcc_parent);
	psc_assert(!(f->fcmh_state & FCMH_CAC_FREEING));
	f->fcmh_pri = NULL;

	DEBUG_FCMH(PLL_WARN, f, "fcc=%p name=%s freeing orphan",
		   fcc, fcc->fcc_name);

	if (f->fcmh_state & FCMH_ISDIR)
		psc_assert(psclist_empty(&f->fcmh_children));

	PSCFREE(fcc);
}

/**
 * fidc_child_try_validate - given a parent fcmh, child, and a name, try
 *	to validate the child's fcc if one exists.  On success the fcc
 *	timeout is increased and if the fcc was orphaned then is
 *	reattached to the parent 'p'.
 * @p: parent fcmh
 * @c: child fcmh
 * @name: name of the fcc entry associated with child.
 * Note: the locking order is [parent, child, child fcc]
 */
static struct fidc_private *
fidc_child_try_validate(struct fidc_membh *p, struct fidc_membh *c,
			const char *name)
{
	struct fidc_private *fcc;

	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

	spinlock(&p->fcmh_lock);
	spinlock(&c->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(!(p->fcmh_state & FCMH_CAC_FREEING));
	psc_assert(!(c->fcmh_state & FCMH_CAC_FREEING));

	fcc = c->fcmh_pri;
	if (fcc) {
		/* Both of these must always be true.
		 */
		psc_assert(fcc->fcc_fcmh == c);
		psc_assert(SAMEFID(fcmh_2_fgp(c), &fcc->fcc_fg));
		if (strncmp(name, fcc->fcc_name, strnlen(name, NAME_MAX))) {
			/* This inode may have been renamed, remove
			 *  this fcc.
			 */
			fidc_child_free_plocked(fcc);
			fcc = NULL;
		} else {
			/* Increase the lifespan of this entry and return.
			 */
			fidc_gettime(&fcc->fcc_age);
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
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);

	return (fcc);
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
	struct fidc_private *fcc=f->fcmh_pri;

	LOCK_ENSURE(&f->fcmh_lock);
	/* Don't free the root inode.
	 */
	psc_assert(fcmh_2_fid(f) != 1);

	DEBUG_FCMH(PLL_WARN, f, "fcc=%p fcmh_no_children=%d",
		   fcc,
		   ((f->fcmh_state & FCMH_ISDIR) ?
		    psclist_empty(&f->fcmh_children) : -1));

	if (((f->fcmh_state & FCMH_ISDIR) &&
	     (!psclist_empty(&f->fcmh_children))))
		return (0);

	else if (!fcc)
		return (1);

	else if (!fcc->fcc_parent) {
		fidc_child_free_orphan_locked(f);
		return (1);

	} else {
		/* The parent needs to be unlocked after the fcc is freed,
		 *  hence the need for the temp var 'p'.
		 */
		struct fidc_membh *p=fcc->fcc_parent;

		psc_assert(p);
		/* This trylock technically violates lock ordering
		 *  (parent / child / fcc) which is why we bail if the
		 *  parent lock cannot be obtained without blocking.
		 */
		if (trylock(&p->fcmh_lock)) {
			fidc_child_free_plocked(fcc);
			freelock(&p->fcmh_lock);
			return (1);
		}
	}
	return (0);
}

/**
 * fidc_child_get_int_locked - given a parent directory inode, try to
 *	locate a child.  If the fcc is too old then it is freed and NULL
 *	is returned.
 * @parent: the parent directory inode.
 * @name: name of the child.
 * @len: the length of the child name string.
 */
static struct fidc_private *
fidc_child_lookup_int_locked(struct fidc_membh *p, const char *name)
{
	struct fidc_private *c=NULL;
	int found=0;
	int hash=str_hash(name);
	struct timespec	now;

	LOCK_ENSURE(&p->fcmh_lock);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(p->fcmh_state & FCMH_ISDIR);

	DEBUG_FCMH(PLL_INFO, p, "name %p (%s), hash=%d",
		   name, name, hash);

	psclist_for_each_entry(c, &p->fcmh_children, fcc_lentry) {

		psc_traces(PSS_GEN, "p=fcmh@%p c=%p cname=%s hash=%d",
			   p, c, c->fcc_name, c->fcc_hash);

		if ((c->fcc_hash == hash) &&
		    (!strncmp(name, c->fcc_name, strnlen(name, NAME_MAX)))) {
			/* Pin down the fcc while the parent dir lock is held.
			 */
			found=1;
			fidc_membh_incref(c->fcc_fcmh);
			break;
		}
	}

	if (!found)
		return (NULL);

	psc_assert(c->fcc_fcmh);
	psc_assert(c->fcc_parent == p);
	psc_assert(c->fcc_fcmh->fcmh_pri == c);

	if (c->fcc_fcmh->fcmh_state & FCMH_CAC_FREEING)
		return (NULL);

	clock_gettime(CLOCK_REALTIME, &now);
	if (timespeccmp(&c->fcc_age, &now, <)) {
		/* It's old, remove it.
		 */
		fidc_membh_dropref(c->fcc_fcmh);							\
		fidc_child_free_plocked(c);
		c = NULL;
	}
	return (c);
}

#if 0
int
fidc_child_cmp(const void *x, const void *y)
{
	const struct fidc_private *a=x, *b=y;
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
	struct fidc_private *c=NULL;
	struct fidc_membh *m=NULL;
	int locked=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	c = fidc_child_lookup_int_locked(p, name);
	if (c) {
		psc_assert(c->fcc_fcmh);
		m = c->fcc_fcmh;
	}
	ureqlock(&p->fcmh_lock, locked);
	return (m);
}

void
fidc_child_unlink(struct fidc_membh *p, const char *name)
{
	struct fidc_private *fcc;
	int locked=reqlock(&p->fcmh_lock);

	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);

	fcc = fidc_child_lookup_int_locked(p, name);
	if (!fcc) {
		ureqlock(&p->fcmh_lock, locked);
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
	psc_assert(atomic_dec_and_test(&fcc->fcc_fcmh->fcmh_refcnt));
	fidc_child_free_plocked(fcc);

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
	struct fidc_private *fcc, *tmp=NULL;

	psc_assert(p && c && name);
	psc_assert(p->fcmh_state & FCMH_ISDIR);
	psc_assert(atomic_read(&p->fcmh_refcnt) > 0);
	psc_assert(atomic_read(&c->fcmh_refcnt) > 0);

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
		DEBUG_FCMH(PLL_WARN, p, "fcc=%p, adding name: %s", fcc, fcc->fcc_name);
	} else {
		/* Someone beat us to the punch, do sanity checks and then
		 *  clean up.
		 */
		psc_assert(tmp->fcc_fcmh == c);
		psc_assert(tmp == c->fcmh_pri);
		fidc_membh_dropref(tmp->fcc_fcmh);
		PSCFREE(fcc);
	}
	freelock(&c->fcmh_lock);
	freelock(&p->fcmh_lock);
}

void
fidc_child_rename(struct fidc_membh *op, const char *oldname,
    struct fidc_membh *np, const char *newname)
{
	struct fidc_private *ch;
	struct fidc_membh *c;
	size_t len;

	c = NULL; /* gcc */
	len = strlen(newname);
	if (len > NAME_MAX)
		psc_fatalx("name too long");
	len++;

	spinlock(&op->fcmh_lock);
	ch = fidc_child_lookup_int_locked(op, oldname);
	if (ch) {
		c = ch->fcc_fcmh;
		spinlock(&c->fcmh_lock);
		psclist_del(&ch->fcc_lentry);
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

	/* sanity check to ensure we don't lose fcmh_pri */
	psc_assert(ch == c->fcmh_pri);

	/* overwrite the old name with the new one in place */
	psc_assert(c->fcmh_pri != NULL);
	ch = c->fcmh_pri = psc_realloc(ch, sizeof(*ch) + len, 0);
	ch->fcc_hash = str_hash(newname);
	strlcpy(ch->fcc_name, newname, len);

	spinlock(&np->fcmh_lock);
	ch->fcc_parent = np;
	psclist_xadd_tail(&ch->fcc_lentry, &np->fcmh_children);
	freelock(&np->fcmh_lock);

	freelock(&c->fcmh_lock);
}
