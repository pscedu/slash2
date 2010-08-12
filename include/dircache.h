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

#include "pfl/str.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"

#include "sltypes.h"

#define dirent_timeo 4

struct fidc_membh;

struct dircache_mgr {
	size_t			 dcm_maxsz;
	size_t			 dcm_alloc;
	psc_spinlock_t		 dcm_lock;
	struct psc_listcache	 dcm_lc;
};

struct dircache_info {
	struct dircache_mgr	*di_dcm;
	struct fidc_membh	*di_fcmh;
	struct psclist_head	 di_list;
	psc_spinlock_t		 di_lock;
};

struct dircache_ents {
	int			 de_remlookup;
	int			 de_flags;	/* see DIRCE_* below */
	int			 de_idx;
	size_t			 de_sz;
	struct timeval		 de_age;
	struct psclist_head	 de_lentry;	/* Chain on info  */
	struct psclist_head	 de_lentry_lc;	/* Chain in mgr */
	struct psc_dynarray	 de_dents;
	struct dircache_desc	*de_desc;
	struct dircache_info	*de_info;
	unsigned char		 de_base[0];
};

/* de_flags */
#define DIRCE_FREEABLE		(1 << 0)
#define DIRCE_FREEING		(1 << 1)

#define dircache_ent_lock(e)	spinlock(&(e)->de_info->di_lock)
#define dircache_ent_ulock(e)	freelock(&(e)->de_info->di_lock)

struct dircache_desc {
	int			 dd_hash;
	int			 dd_len;
	int			 dd_offset;
	int			 dd_flags;	/* see DC_* below */
	const char		*dd_name;
};

/* dd_flags */
#define DC_STALE		(1 << 0)	/* Set on rename or unlink */
#define	DC_LOOKUP		(1 << 1)	/* Item was accessed via lookup */

static __inline int
dirent_sort_cmp(const void *a, const void *b)
{
	const void * const *pa = a;
	const void * const *pb = b;
	const struct dircache_desc *x = *pa, *y = *pb;

	return (pfl_strncmp2(x->dd_name, x->dd_len, y->dd_name, y->dd_len));
}

#define DIRCACHE_INIT(fcmh, mgr)					\
	do {								\
		if (!fcmh_2_fci(fcmh)->fci_init) {			\
			dircache_init_info(&fcmh_2_fci(fcmh)->fci_dci,	\
			    (fcmh), (mgr));				\
			fcmh_2_fci(fcmh)->fci_init = 1;			\
		}							\
	} while (0)

#define DIRCACHE_INITIALIZED(fcmh)					\
	fcmh_2_fci(fcmh)->fci_init

struct dircache_ents *
	dircache_new_ents(struct dircache_info *, size_t);
void	dircache_earlyrls_ents(struct dircache_ents *);
void	dircache_init(struct dircache_mgr *, const char *, size_t);
void	dircache_init_info(struct dircache_info *, struct fidc_membh *, struct dircache_mgr *);
slfid_t	dircache_lookup(struct dircache_info *, const char *, int);
void	dircache_reg_ents(struct dircache_ents *, size_t);
void	dircache_setfreeable_ents(struct dircache_ents *);

#endif /* _DIRCACHE_H_ */
