/* $Id: dircache.h $*/
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

#include <time.h>

#include "psc_ds/list.h"
#include "psc_util/lock.h"

#include "sltypes.h"
#include "fidcache.h"

#define dirent_timeo 8

struct dircache_mgr {
	size_t                   dcm_maxsz;
        size_t                   dcm_alloc;
        psc_spinlock_t           dcm_lock;
	struct psc_listcache     dcm_lc;
};

struct dircache_info {
	struct dircache_mgr     *di_dcm;
	struct fidc_membh       *di_fcmh;
	struct psclist_head      di_list;
	struct psclist_head      di_lentry;
	psc_spinlock_t           di_lock;
};

struct dircache_ents {
	int                      de_remlookup:31;
	int                      de_freeable:1;
	size_t                   de_sz;
	struct timeval           de_age;
	struct psclist_head      de_lentry1;   /* Chain on info  */
	struct psclist_head      de_lentry2;   /* Chain onto mgr */
	struct psc_dynarray      de_dents;	
	struct dircache_info    *de_info;
	unsigned char           *de_base;
};

struct dircache_desc {
	int                      dd_hash;
	int                      dd_len;
	int                      dd_offset:30;
	int                      dd_flags:2;
};

enum dircache_flags {
	DC_STALE  = (1 << 0),      /* Set on rename or unlink */
	DC_LOOKUP = (1 << 1)       /* Item was accessed via lookup */
};

static inline void
dircache_setfreeable_ents(struct dircache_ents *e)
{
        e->de_freeable = 1;
}

/* Note:  These macros must match the ones used by the server in 
 *  zfs_operations_slash.c
 */
#define SRT_NAME_OFFSET ((unsigned long) ((struct str_dirent *) 0)->name)
#define SRT_DIRENT_ALIGN(x) (((x) + sizeof(uint64_t) - 1) &	\
			     ~(sizeof(uint64_t) - 1))
#define SRT_DIRENT_SIZE(d)					\
	SRT_DIRENT_ALIGN(SRT_NAME_OFFSET + (d)->namelen)

static __inline size_t 
srt_dirent_size(size_t namelen)
{
	return SRT_DIRENT_ALIGN(SRT_NAME_OFFSET + namelen);
}

static __inline int
dirent_desc_sort_cmp(const void *x, const void *y)
{
	const struct dircache_desc * const *pa = x, *a = *pa;
	const struct dircache_desc * const *pb = y, *b = *pb;

	return (CMP(a->bmpce_hash, b->bmpce_hash));
}

#endif
