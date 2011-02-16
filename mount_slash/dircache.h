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

/*
 * When a file directory file listing is requested, FUSE sends out a
 * READDIR RPC on the directory to get a listing of the names, then it
 * looks up each name for its inode, and requests attributes on each
 * inode.  Our code brings in all the name to inode translations and
 * attributes in one go.  This interface implements a simple,
 * non-coherent cache to avoid lookup and getattr RPCs on each
 * individual file.
 */

#ifndef _DIRCACHE_H_
#define _DIRCACHE_H_

#include <sys/types.h>

#include <time.h>

#include "pfl/str.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"

#include "sltypes.h"

/*
 * When you do a file listing under a directory, FUSE sends out a readdir RPC
 * on the directory to get a listing of the names, then it looks up each name
 * for its inode, and requests attributes on each inode.  Our code brings in
 * all the name to inode translations and attributes in one go.  This file
 * implements a simple, non-coherent cache to avoid lookup and getattr RPCs on
 * each individual files.
 */

#define dirent_timeo 4

struct fidc_membh;

/*
 * Single global manager.
 */
struct dircache_mgr {
	size_t			 dcm_maxsz;
	size_t			 dcm_alloc;
	psc_spinlock_t		 dcm_lock;
	struct psc_listcache	 dcm_lc;
};

/*
 * This is attached to a directory fcmh and contains all dircache data.
 */
struct dircache_info {
	struct dircache_mgr	*di_dcm;
	struct fidc_membh	*di_fcmh;
	struct psc_lockedlist	 di_list;
	psc_spinlock_t		 di_lock;
};

/*
 * This consitutes a block of 'struct dirent' members (dircache_desc)
 * belonging to a READDIR request, which will be one of many for
 * directories with many entries.
 */
struct dircache_ents {
	int			 de_remlookup;
	int			 de_flags;	/* see DIRCE_* below */
	size_t			 de_sz;
	struct timeval		 de_age;
	struct psc_listentry	 de_lentry;	/* chain on info  */
	struct psc_listentry	 de_lentry_lc;	/* chain in mgr */
	struct psc_dynarray	 de_dents;
	struct dircache_desc	*de_desc;	/* contains dircache_descs */
	struct dircache_info	*de_info;
	void			*de_base;	/* contains pscfs_dirents */
};

/* de_flags */
#define DIRCE_FREEABLE		(1 << 0)
#define DIRCE_FREEING		(1 << 1)

#define dircache_ent_lock(e)	spinlock(&(e)->de_info->di_lock)
#define dircache_ent_ulock(e)	freelock(&(e)->de_info->di_lock)

/* This is synonymous with 'struct dirent'. */
struct dircache_desc {
	int			 dd_hash;
	int			 dd_namelen;
	int			 dd_offset;
	int			 dd_flags;	/* see DC_* below */
	const char		*dd_name;
};

/* dd_flags */
#define DC_STALE		(1 << 0)	/* Set on rename or unlink */
#define	DC_LOOKUP		(1 << 1)	/* Item was accessed via lookup */

/*
 * This is also a sort comparison.  We need dirent_cmp() and dirent_sort_cmp()
 * for different interfaces.
 */
static __inline int
dirent_cmp(const void *a, const void *b)
{
	const struct dircache_desc *x = a, *y = b;
	int rc;

	rc = CMP(x->dd_hash, y->dd_hash);
	if (rc)
		return (rc);
	rc = CMP(x->dd_namelen, y->dd_namelen);
	if (rc)
		return (rc);
	return (strncmp(x->dd_name, y->dd_name, y->dd_namelen));
}

static __inline int
dirent_sort_cmp(const void *x, const void *y)
{
	const void * const *pa = x, *a = *pa;
	const void * const *pb = y, *b = *pb;

	return (dirent_cmp(a, b));
}

#define DIRCACHE_INITIALIZED(fcmh)	((fcmh)->fcmh_flags & FCMH_CLI_INITDCI)

struct dircache_ents *
	dircache_new_ents(struct dircache_info *, size_t);
void	dircache_init(struct dircache_mgr *, const char *, size_t);
slfid_t	dircache_lookup(struct dircache_info *, const char *, int);
void	dircache_reg_ents(struct dircache_ents *, size_t);
void	dircache_rls_ents(struct dircache_ents *, int);
void	dircache_setfreeable_ents(struct dircache_ents *);
void	dircache_walk(struct dircache_info *, void (*)(struct dircache_desc *, void *), void *);

#define DCFREEF_RELEASE	(1 << 0)	/* fcmh may be released */
#define DCFREEF_EARLY	(1 << 1)	/* dircache ent not attached */

extern struct dircache_mgr dircacheMgr;

#endif /* _DIRCACHE_H_ */
