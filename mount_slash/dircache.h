/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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

#include "pfl/dynarray.h"
#include "pfl/fs.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/str.h"
#include "pfl/time.h"

#include "sltypes.h"
#include "fidc_cli.h"

/*
 * dircache - directory entry (dent) caching layer.
 *
 * When readdir(3) is issued, FUSE invokes the READDIR handler to fetch
 * directory entries.  An RPC is sent that bulks together stat(2)'s in a
 * READDIR+ fashion as some libraries and applications shortly perform
 * stat(2) operations on each entry after readdir(3) returns.
 *
 * This implementation fosters a compromise between read-ahead for
 * larger directories as well as memory exhaustion for much larger
 * directories in the form of a simple, non-coherent cache.
 */

struct fidc_membh;

#define DIRCACHEPG_TIMEO	4	/* expiration after page read */
#define DIRCACHEPG_MAXTIMEO	30	/* expiration regardless if read */

/*
 * This consitutes a block of 'struct dirent' members (dircache_ent)
 * belonging to a READDIR request, which may be one of many for
 * directories with many entries.
 */
struct dircache_page {
	int			 dcp_flags;	/* see DIRCACHEPGF_* below */
	int			 dcp_rc;	/* readdir(3) error */
	size_t			 dcp_size;
	off_t			 dcp_off;
	off_t			 dcp_nextoff;
	struct pfl_timespec	 dcp_tm;
	struct psc_listentry	 dcp_lentry;	/* chain on dci  */
	struct psc_dynarray	*dcp_dents_name;/* dircache_ents sorted by hash(basename) */
	struct psc_dynarray	*dcp_dents_off;	/* dircache_ents sorted by d_off */
	void			*dcp_base;	/* pscfs_dirents */
	void			*dcp_base0;	/* dircache_ents */
	int			 dcp_refcnt;
};

/* dcp_flags */
#define DIRCACHEPGF_LOADING	(1 << 0)	/* stub is waiting for network load */
#define DIRCACHEPGF_LOADED	(1 << 1)
#define DIRCACHEPGF_EOF		(1 << 2)	/* denotes last page */
#define DIRCACHEPGF_READ	(1 << 3)	/* page has been used */
#define DIRCACHEPGF_FREEING	(1 << 4)	/* a thread is trying to free */

/*
 * This is NOT the expire timestamp of a cache page. It is used to calculate 
 * the largest timestamp that a cache page can have in order to expire right now.
 */
struct dircache_expire {
	struct pfl_timespec	 dexp_def;
	struct pfl_timespec	 dexp_max;
};

#define DIRCACHEPG_EXPIRED(d, p, dexp)					\
	((p)->dcp_refcnt == 0 &&					\
	 ((timespeccmp(&(dexp)->dexp_def, &(p)->dcp_tm, >) &&		\
	    (p)->dcp_flags & DIRCACHEPGF_READ) ||			\
	  timespeccmp(&(dexp)->dexp_max, &(p)->dcp_tm, >) ||		\
	  timespeccmp(&(d)->fcmh_sstb.sst_mtim, &p->dcp_tm, >)))

#define DIRCACHEPG_INITEXP(dexp)					\
	do {								\
		PFL_GETPTIMESPEC(&(dexp)->dexp_def);			\
		(dexp)->dexp_max = (dexp)->dexp_def;			\
		(dexp)->dexp_def.tv_sec -= DIRCACHEPG_TIMEO;		\
		(dexp)->dexp_max.tv_sec -= DIRCACHEPG_MAXTIMEO;		\
	} while (0)

#define DBGPR_DIRCACHEPG(lvl, dcp, fmt, ...)				\
	psclog((lvl), "dcp@%p off %"PSCPRIdOFFT" rf %d sz %zu "		\
	    "fl %#x nextoff %"PSCPRIdOFFT": " fmt,			\
	    (dcp), (dcp)->dcp_off, (dcp)->dcp_refcnt, (dcp)->dcp_size,	\
	    (dcp)->dcp_flags, (dcp)->dcp_nextoff, ## __VA_ARGS__)

/* This is analogous to 'struct dirent' many of which reside in a page. */
struct dircache_ent {
	int			 dce_hash;
	int			 dce_namelen;
	off_t			 dce_len;
	const char		*dce_name;
	int64_t			 dce_off;
};

/* The different interfaces below are used for searching and sorting. */
static __inline int
dce_cmp_name(const void *a, const void *b)
{
	const struct dircache_ent *x = a, *y = b;
	int rc;

	rc = CMP(x->dce_hash, y->dce_hash);
	if (rc)
		return (rc);
	rc = CMP(x->dce_namelen, y->dce_namelen);
	if (rc)
		return (rc);
	return (strncmp(x->dce_name, y->dce_name, y->dce_namelen));
}

static __inline int
dce_sort_cmp_name(const void *x, const void *y)
{
	const void * const *pa = x, *a = *pa;
	const void * const *pb = y, *b = *pb;

	return (dce_cmp_name(a, b));
}

/* The different interfaces below are used for searching and sorting. */
static __inline int
dce_cmp_off(const void *a, const void *b)
{
	const struct dircache_ent *x = a, *y = b;

	return (CMP(x->dce_off, y->dce_off));
}

static __inline int
dce_sort_cmp_off(const void *x, const void *y)
{
	const void * const *pa = x, *a = *pa;
	const void * const *pb = y, *b = *pb;

	return (dce_cmp_off(a, b));
}

#define dircache_free_page(d, p)					\
	_dircache_free_page(PFL_CALLERINFO(), (d), (p), 1)

#define dircache_free_page_nowait(d, p)					\
	_dircache_free_page(PFL_CALLERINFO(), (d), (p), 0)

struct dircache_page *
	dircache_new_page(struct fidc_membh *, off_t, int);
int	dircache_hasoff(struct dircache_page *, off_t);
int	_dircache_free_page(const struct pfl_callerinfo *,
	    struct fidc_membh *, struct dircache_page *, int);
slfid_t	dircache_lookup(struct fidc_membh *, const char *, off_t *, int);
void	dircache_mgr_init(void);
void	dircache_purge(struct fidc_membh *);
void	dircache_reg_ents(struct fidc_membh *, struct dircache_page *,
	    size_t, void *, size_t, int);
void	dircache_walk(struct fidc_membh *, void (*)(struct dircache_page *,
	    struct dircache_ent *, void *), void *);

#endif /* _DIRCACHE_H_ */
