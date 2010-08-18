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
#include <sys/time.h>

#include <stdint.h>
#include <string.h>

#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/listcache.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "dircache.h"
#include "fidcache.h"
#include "sltypes.h"

/* Note:  These macros must match the ones used by the server in
 *  zfs_operations_slash.c
 */
#define SRT_NAME_OFFSET ((unsigned long)((struct srt_dirent *) 0)->name)
#define SRT_DIRENT_ALIGN(x) (((x) + sizeof(uint64_t) - 1) &	\
			     ~(sizeof(uint64_t) - 1))
#define SRT_DIRENT_SIZE(d)					\
	SRT_DIRENT_ALIGN(SRT_NAME_OFFSET + (d)->namelen)

static __inline size_t
srt_dirent_size(size_t namelen)
{
	return SRT_DIRENT_ALIGN(SRT_NAME_OFFSET + namelen);
}

void
dircache_init(struct dircache_mgr *m, const char *name, size_t maxsz)
{
	m->dcm_maxsz = maxsz;
	m->dcm_alloc = 0;

	LOCK_INIT(&m->dcm_lock);
	lc_reginit(&m->dcm_lc, struct dircache_ents, de_lentry_lc, "%s", name);
}

void
dircache_init_info(struct dircache_info *i, struct fidc_membh *f,
	   struct dircache_mgr *m)
{
	i->di_dcm = m;
	i->di_fcmh = f;
	INIT_PSCLIST_HEAD(&i->di_list);
	LOCK_INIT(&i->di_lock);
}

static void
dircache_rls_ents(struct dircache_ents *e)
{
	struct dircache_info *i = e->de_info;
	struct dircache_mgr *m = i->di_dcm;
	int locked;

	DEBUG_FCMH(PLL_DEBUG, i->di_fcmh,
	    "rls dircache_ents %p cachesz=%zu", e, m->dcm_alloc);

	locked = reqlock(&m->dcm_lock);
	m->dcm_alloc -= e->de_sz;
	lc_remove(&i->di_dcm->dcm_lc, e);

	spinlock(&i->di_lock);
	psc_assert(e->de_flags & DIRCE_FREEING);
	psclist_del(&e->de_lentry);
	freelock(&i->di_lock);

	ureqlock(&m->dcm_lock, locked);

	PSCFREE(e);

	fcmh_op_done_type(i->di_fcmh, FCMH_OPCNT_DIRENTBUF);
}

void
dircache_setfreeable_ents(struct dircache_ents *e)
{
	dircache_ent_lock(e);
	if (!(e->de_flags & DIRCE_FREEABLE))
		e->de_flags |= DIRCE_FREEABLE;

	if (!e->de_remlookup && !(e->de_flags & DIRCE_FREEING)) {
		e->de_flags |= DIRCE_FREEING;
		dircache_ent_ulock(e);
		dircache_rls_ents(e);
	} else
		dircache_ent_ulock(e);
}

slfid_t
dircache_lookup(struct dircache_info *i, const char *name, int flag)
{
	struct dircache_ents *e;
	struct dircache_desc desc, *d;
	struct srt_dirent *dirent;
	slfid_t ino = FID_ANY;
	int found=0, pos, freeit=0;

	desc.dd_hash = psc_str_hashify(name);
	desc.dd_len  = strnlen(name, NAME_MAX);
	desc.dd_name = name;

	/* This lock is equiv to dircache_ent_lock()
	 */
	spinlock(&i->di_lock);
	psclist_for_each_entry(e, &i->di_list, de_lentry) {
		/* 
 		 * The return code for psc_dynarray_bsearch() tells us
 		 * the position where our name should be to keep the
 		 * the list sorted.  If it is one after the last item,
 		 * then we know for sure the item is not there. Otherwise,
 		 * we still need one more comparison to be sure.
		 */
		pos = psc_dynarray_bsearch(&e->de_dents,
		    &desc, dirent_cmp);
		if (pos >= psc_dynarray_len(&e->de_dents))
			continue;
		d = psc_dynarray_getpos(&e->de_dents, pos);
		dirent = (void *)(e->de_base + d->dd_offset);

		psc_dbg("ino=%"PRIx64" off=%"PRId64" nlen=%u "
		    "type=%#o name=%.*s lkname=%.*s off=%d d=%p",
		    dirent->ino, dirent->off, dirent->namelen,
		    dirent->type, dirent->namelen, dirent->name,
		    NAME_MAX, name, d->dd_offset, d);

		if (d->dd_hash == desc.dd_hash &&
		    d->dd_len  == desc.dd_len &&
		    strncmp(name, dirent->name, d->dd_len) == 0) {
			/* Map the dirent from the desc's offset.
			 */
			found = 1;

			if (!(d->dd_flags & DC_LOOKUP)) {
				e->de_remlookup--;
				d->dd_flags |= DC_LOOKUP;
			}

			if (!(d->dd_flags & DC_STALE))
				ino = dirent->ino;

			if (flag & DC_STALE)
				d->dd_flags |= DC_STALE;

			break;
		}
	}

	if (found && !e->de_remlookup && (e->de_flags & DIRCE_FREEABLE) &&
	    !(e->de_flags & DIRCE_FREEING)) {
		/* If all of the items have been accessed via lookup then
		 *   assume that fuse has an entry cached for each and free
		 *   the buffer.
		 */
		e->de_flags |= DIRCE_FREEING;
		freeit = 1;
	}

	if (found && (e->de_flags & DIRCE_FREEING)) {
		freelock(&i->di_lock);
		if (freeit)
			dircache_rls_ents(e);
	} else
		freelock(&i->di_lock);

	return (ino);
}

struct dircache_ents *
dircache_new_ents(struct dircache_info *i, size_t size)
{
	struct dircache_mgr *m = i->di_dcm;
	struct dircache_ents *e, *tmp;
	struct timeval now;

	PFL_GETTIMEVAL(&now);

	spinlock(&m->dcm_lock);
	m->dcm_alloc += size;

	/* Remove old entries from the top of the list.
	 */
	LIST_CACHE_FOREACH_SAFE(e, tmp, &m->dcm_lc) {
		dircache_ent_lock(e);
		if (timercmp(&now, &e->de_age, >) &&
		    (e->de_flags & DIRCE_FREEABLE) &&
		    !(e->de_flags & DIRCE_FREEING)) {
			e->de_flags |= DIRCE_FREEING;
			dircache_ent_ulock(e);
			dircache_rls_ents(e);

		} else {
			dircache_ent_ulock(e);
			break;
		}
	}

	/* Clear more space if needed.
	 */
	while ((m->dcm_alloc + size) > m->dcm_maxsz &&
	       !lc_empty(&m->dcm_lc)) {
		e = lc_peekhead(&m->dcm_lc);
		if (!e)
			break;

		dircache_ent_lock(e);
		if ((e->de_flags & DIRCE_FREEABLE) &&
		    !(e->de_flags & DIRCE_FREEING)) {
			e->de_flags |= DIRCE_FREEING;
			dircache_ent_ulock(e);
			dircache_rls_ents(e);
		} else
			dircache_ent_ulock(e);
	}
	freelock(&m->dcm_lock);

	e = PSCALLOC(sizeof(*e) + size);
	e->de_sz = size;
	e->de_info = i;
	return (e);
}

/* dircache_reg_ents
 */
void
dircache_reg_ents(struct dircache_ents *e, size_t nents)
{
	struct dircache_info *i = e->de_info;
	struct dircache_mgr  *m = i->di_dcm;
	struct dircache_desc *c;
	struct srt_dirent *d;
	unsigned char *b;
	off_t off;
	int j;

	psc_assert(e->de_sz);
	psc_assert(e->de_info);
	psc_assert(!e->de_desc);

	PFL_GETTIMEVAL(&e->de_age);
	e->de_age.tv_sec += dirent_timeo;
	e->de_remlookup = nents - 2; /* subtract "." and ".." */
	e->de_flags = 0; /* remain '0' until readdir req has completed */

	c = e->de_desc = PSCALLOC(sizeof(struct dircache_desc) * nents);
	psc_dynarray_ensurelen(&e->de_dents, nents);

	for (j=0, b=e->de_base, off=0; j < (int)nents; j++, c++) {
		d = (void *)(b + off);

		psc_dbg("ino=%"PRIx64" off=%"PRId64" "
		    "nlen=%u type=%#o name=%.*s d=%p off=%"PRId64,
		    d->ino, d->off, d->namelen, d->type,
		    d->namelen, d->name, d, off);

		c->dd_len    = d->namelen;
		c->dd_hash   = psc_strn_hashify(d->name, d->namelen);
		c->dd_flags  = 0;
		c->dd_offset = off;
		c->dd_name   = d->name;

		psc_dynarray_add(&e->de_dents, c);
		off += srt_dirent_size((size_t)d->namelen);
	}

	/* Sort the desc items by their hash.
	 */
	psc_dynarray_sort(&e->de_dents, qsort, dirent_sort_cmp);
	DYNARRAY_FOREACH(c, j, &e->de_dents)
		psc_dbg("c=%p hash=%d len=%u name=%.*s",
		    c, c->dd_hash, c->dd_len, c->dd_len, c->dd_name);

	lc_addtail(&m->dcm_lc, e);

	spinlock(&i->di_lock);
	/* New entries are considered to be more accurate so place them
	 *   at the beginning of the list.
	 */
	psclist_xadd(&e->de_lentry, &i->di_list);
	freelock(&i->di_lock);

	fcmh_op_start_type(i->di_fcmh, FCMH_OPCNT_DIRENTBUF);
}
