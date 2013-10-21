/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2013, Pittsburgh Supercomputing Center (PSC).
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
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/str.h"
#include "pfl/time.h"
#include "pfl/lock.h"

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

#define DIRENT_TIMEO 4

/*
 * This consitutes a block of 'struct dirent' members (dircache_ent)
 * belonging to a READDIR request, which may be one of many for
 * directories with many entries.
 */
struct dircache_page {
	int			 dcp_flags;	/* see DCPF_* below */
	int			 dcp_rc;	/* readdir(3) error */
	size_t			 dcp_size;
	off_t			 dcp_off;
	off_t			 dcp_nextoff;
	struct pfl_timespec	 dcp_tm;
	struct psc_listentry	 dcp_lentry;	/* chain on dci  */
	struct psc_dynarray	 dcp_dents;
	void			*dcp_base;	/* pscfs_dirents */
	void			*dcp_base0;	/* dircache_ents */
};

/* dcp_flags */
#define DCPF_LOADING		(1 << 0)
#define DCPF_EOF		(1 << 1)	/* denotes last page */
#define DCPF_READAHEAD		(1 << 2)	/* was loaded by readahead */

#define DIRCACHE_PAGE_EXPIRED(d, p, exp)				\
	(((p)->dcp_flags & DCPF_LOADING) == 0 &&			\
	 (timespeccmp((exp), &(p)->dcp_tm, >) ||			\
	  timespeccmp(&(d)->fcmh_sstb.sst_mtim, &p->dcp_tm, >)))

#define DPRINTF_DIRCACHEPG(lvl, dcp, fmt, ...)				\
	psclog((lvl), "dcp@%p off %"PSCPRIdOFFT" sz %zu fl %#x "	\
	    "nextoff %"PSCPRIdOFFT": " fmt,				\
	    (dcp), (dcp)->dcp_off, (dcp)->dcp_size, (dcp)->dcp_flags,	\
	    (dcp)->dcp_nextoff, ## __VA_ARGS__)

/* This is analogous to 'struct dirent'. */
struct dircache_ent {
	int			 dce_hash;
	int			 dce_namelen;
	off_t			 dce_len;
	const char		*dce_name;
};

/*
 * This is also a sort comparison.  We need dirent_cmp() and
 * dirent_sort_cmp() for different interfaces.
 */
static __inline int
dirent_cmp(const void *a, const void *b)
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
dirent_sort_cmp(const void *x, const void *y)
{
	const void * const *pa = x, *a = *pa;
	const void * const *pb = y, *b = *pb;

	return (dirent_cmp(a, b));
}

struct dircache_page *
	dircache_new_page(struct fidc_membh *, off_t, int);
void	dircache_free_page(struct fidc_membh *, struct dircache_page *);
slfid_t	dircache_lookup(struct fidc_membh *, const char *);
void	dircache_mgr_init(void);
void	dircache_purge(struct fidc_membh *);
void	dircache_reg_ents(struct fidc_membh *, struct dircache_page *, size_t, void *);
void	dircache_walk(struct fidc_membh *, void (*)(struct dircache_page *,
    	    struct dircache_ent *, void *), void *);

#endif /* _DIRCACHE_H_ */
