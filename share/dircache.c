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
	lc_reginit(&m->dcm_lc, struct dircache_ents, de_lentry2, name);
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

	DEBUG_FCMH(PLL_WARN, i->di_fcmh,
		   "rls dircache_ents %p cachesz=%"PRId64, e, m->dcm_alloc);

	locked = reqlock(&m->dcm_lock);
	m->dcm_alloc -= e->de_sz;
	lc_remove(&i->di_dcm->dcm_lc, e);

	spinlock(&i->di_lock);
	psclist_del(&e->de_lentry1);
	freelock(&i->di_lock);

	ureqlock(&m->dcm_lock, locked);

	PSCFREE(e);

	fcmh_op_done_type(i->di_fcmh, FCMH_OPCNT_DIRENTBUF);
}

slfid_t
dircache_lookup(struct dircache_info *i, const char *name, int flag)
{
	struct dircache_ents *e;
	struct dircache_desc desc, *d;
	struct srt_dirent *dirent;
	slfid_t ino = FID_ANY;
	int found=0, pos;

	desc.dd_hash = psc_str_hashify(name);
	desc.dd_len  = strnlen(name, NAME_MAX);
	desc.dd_name = name;

	spinlock(&i->di_lock);
	psclist_for_each_entry(e, &i->di_list, de_lentry1) {
#ifdef BSEARCH
		/* The return code for psc_dynarray_bsearch() isn't quite
		 *    right for our purposes but either way the strings
		 *    must still be compared.
		 */
		pos = psc_dynarray_bsearch(&e->de_dents,
		    &desc, dirent_cmp);
d = psc_dynarray_getpos(&e->de_dents, pos);
printf("pos=%d key=%s found=%s\n", pos, name, d->dd_name);
		if (pos >= psc_dynarray_len(&e->de_dents))
			break;
		d = psc_dynarray_getpos(&e->de_dents, pos);
#else
		DYNARRAY_FOREACH(d, pos, &e->de_dents) {
#endif
			dirent = (void *)(e->de_base + d->dd_offset);

			psc_warnx("ino=%"PRIx64" off=%"PRId64" nlen=%u "
				  "type=%o name=%s lkname=%s off=%d d=%p",
				  dirent->ino, dirent->off, dirent->namelen,
				  dirent->type, dirent->name, name,
				  d->dd_offset, d);

			if (d->dd_hash == desc.dd_hash &&
			    d->dd_len  == desc.dd_len) {
				/* Map the dirent from the desc's offset.
				 */
				if (!strncmp(name, dirent->name,
					     desc.dd_len)) {
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
#ifndef BSEARCH
		}

		if (found)
			break;
#endif
	}
	freelock(&i->di_lock);
	/* If all of the items have been accessed via lookup then
	 *   assume that fuse has an entry cached for each and free
	 *   the buffer.
	 */
	if (found && !e->de_remlookup) {
		/* de_freeable should only be set prior to the completion
		 *   of the readdir.  The ents are fair game for freeing
		 *   any time after that.
		 */
		psc_assert(e->de_freeable);
		dircache_rls_ents(e);
	}

	return (ino);
}

struct dircache_ents *
dircache_new_ents(struct dircache_info *i, size_t size)
{
	struct dircache_mgr *m = i->di_dcm;
	struct dircache_ents *e, *tmp;
	struct timeval now;

	PFL_GETTIME(&now);

	spinlock(&m->dcm_lock);
	m->dcm_alloc += size;

	/* Remove old entries from the top of the list.
	 */
	LIST_CACHE_FOREACH_SAFE(e, tmp, &m->dcm_lc) {
		if (timercmp(&now, &e->de_age, >) && e->de_freeable)
			dircache_rls_ents(e);
		else
			break;
	}
	/* Clear more space if needed.
	 */
	while ((m->dcm_alloc + size) > m->dcm_maxsz &&
	       !lc_empty(&m->dcm_lc)) {
		e = lc_peekhead(&m->dcm_lc);
		if (!e)
			break;

		if (e->de_freeable)
			dircache_rls_ents(e);
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

	PFL_GETTIME(&e->de_age);
	e->de_age.tv_sec += dirent_timeo;
	e->de_remlookup = nents - 2; /* subtract "." and ".." */
	e->de_freeable = 0; /* remain '0' until readdir req has completed */

	c = e->de_desc = PSCALLOC(sizeof(struct dircache_desc) * nents);
	psc_dynarray_ensurelen(&e->de_dents, nents);

	for (j=0, b=e->de_base, off=0; j < (int)nents; j++, c++) {
		d = (struct srt_dirent *)((unsigned char *)(b + off));

		psc_warnx("ino=%"PRIx64" off=%"PRId64
			  " nlen=%u type=%o name=%s d=%p off=%"PRId64,
			  d->ino, d->off, d->namelen, d->type, d->name,
			  d, off);

		c->dd_len    = d->namelen;
		c->dd_hash   = psc_str_hashify(d->name);
		c->dd_flags  = 0;
		c->dd_offset = off;
		c->dd_name   = d->name;

		psc_dynarray_add(&e->de_dents, c);
		off += srt_dirent_size((size_t)d->namelen);
	}
	/* Sort the desc items by their hash.
	 */
#ifdef BSEARCH
		psc_dynarray_sort(&e->de_dents, qsort, dirent_sort_cmp);
		DYNARRAY_FOREACH(c, j, &e->de_dents) {
			psc_warnx("c=%p hash=%d len=%u",
				  c, c->dd_hash, c->dd_len);
		}
#endif
	lc_addtail(&m->dcm_lc, e);

	spinlock(&i->di_lock);
	/* New entries are considered to be more accurate so place them
	 *   at the beginning of the list.
	 */
	psclist_xadd(&e->de_lentry1, &i->di_list);
	freelock(&i->di_lock);

	fcmh_op_start_type(i->di_fcmh, FCMH_OPCNT_DIRENTBUF);
}

/* dircache_release_early - called when a dircache_ents was not registered.
 */
void
dircache_earlyrls_ents(struct dircache_ents *e)
{
	psc_assert(psclist_disjoint(&e->de_lentry1));
	psc_assert(psclist_disjoint(&e->de_lentry2));

	spinlock(&e->de_info->di_dcm->dcm_lock);
	e->de_info->di_dcm->dcm_alloc -= e->de_sz;
	freelock(&e->de_info->di_dcm->dcm_lock);

	PSCFREE(e);
}
