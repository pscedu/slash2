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

#ifndef _DIRCACHE_H_
#define _DIRCACHE_H_

#include <sys/types.h>

#include <time.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"

#include "sltypes.h"

#define dirent_timeo 4

struct fidc_membh;

struct dircache_mgr {
	size_t			dcm_maxsz;
	size_t			dcm_alloc;
	psc_spinlock_t		dcm_lock;
	struct psc_listcache	dcm_lc;
};

struct dircache_info {
	struct dircache_mgr	*di_dcm;
	struct fidc_membh	*di_fcmh;
	struct psclist_head	 di_list;
	psc_spinlock_t		 di_lock;
};

struct dircache_ents {
	int			 de_remlookup;
	int			 de_freeable;
	int			 de_idx;
	size_t			 de_sz;
	struct timeval		 de_age;
	struct psclist_head	 de_lentry1;   /* Chain on info  */
	struct psclist_head	 de_lentry2;   /* Chain in mgr */
	struct psc_dynarray	 de_dents;
	struct dircache_desc	*de_desc;
	struct dircache_info	*de_info;
	unsigned char		 de_base[0];
};

struct dircache_desc {
	int			 dd_hash;
	int			 dd_len;
	int			 dd_offset;
	int			 dd_flags;
	const char		*dd_name;
};

enum dircache_flags {
	DC_STALE  = (1 << 0),	/* Set on rename or unlink */
	DC_LOOKUP = (1 << 1)	/* Item was accessed via lookup */
};

static __inline void
dircache_setfreeable_ents(struct dircache_ents *e)
{
	e->de_freeable = 1;
}

static __inline int
dirent_cmp(const void *a, const void *b)
{
	const struct dircache_desc *x = a, *y = b;

	if (x->dd_hash == y->dd_hash) {
		if (x->dd_hash == y->dd_hash)
			return (strcmp(x->dd_name, y->dd_name));
		return (CMP(x->dd_len, y->dd_len));
	}
	return (CMP(x->dd_hash, y->dd_hash));
}

static __inline int
dirent_sort_cmp(const void *x, const void *y)
{
	const void * const *pa = x, *a = *pa;
	const void * const *pb = y, *b = *pb;

	return (dirent_cmp(a, b));
}

#define DIRCACHE_INIT(fcmh, mgr)					\
	do {								\
		if (!fcmh_2_fci(fcmh)->fci_dci.di_fcmh)			\
			dircache_init_info(&fcmh_2_fci(fcmh)->fci_dci,	\
			    (fcmh), (mgr));				\
	} while (0)

#define DIRCACHE_INITIALIZED(fcmh)					\
	(fcmh_2_fci(fcmh)->fci_dci.di_fcmh ? 1 : 0)

void
dircache_init(struct dircache_mgr *, const char *, size_t);

void
dircache_init_info(struct dircache_info *, struct fidc_membh *,
	   struct dircache_mgr *);

slfid_t
dircache_lookup(struct dircache_info *, const char *, int);

struct dircache_ents *
dircache_new_ents(struct dircache_info *, size_t);

void
dircache_reg_ents(struct dircache_ents *, size_t);

void
dircache_earlyrls_ents(struct dircache_ents *);

#endif
