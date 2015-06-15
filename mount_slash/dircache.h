/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2010-2015, Pittsburgh Supercomputing Center (PSC).
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
 * This API is akin to READDIRPLUS (which fetches READDIR entries plus
 * 'struct stat' for each entry in anticipation that an application such
 * as ls(1) will soon stat(2) each entry) but also provides:
 *	- LRU for entries in case another READDIR for the same region
 *	  occurs again soon.
 *	- 'name to inode number' lookup cache (see namecache API)
 * Since the file metadata attributes may differ in this cache versus
 * the 'truth' (i.e. the MDS), this cache is not strictly coherent.
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

#define DIRCACHEPG_SOFT_TIMEO	4	/* expiration after page read */
#define DIRCACHEPG_HARD_TIMEO	30	/* expiration regardless if read */

/*
 * This consitutes a block of 'struct dirent' members (dircache_ent)
 * belonging to a READDIR request, which may be one of many for
 * directories with many entries.
 */
struct dircache_page {
	int			 dcp_flags;	/* see DIRCACHEPGF_* below */
	int			 dcp_rc;	/* readdir(3) error */
	size_t			 dcp_size;	/* length of page (# dirents in page) */
	off_t			 dcp_off;	/* getdents(2) 'offset' cookie of first dirent */
	off_t			 dcp_nextoff;	/* next getdents(2) 'offset' cookie */
	struct pfl_timespec	 dcp_local_tm;	/* local clock when populated */
	struct pfl_timespec	 dcp_remote_tm;	/* remote clock when populated */
	struct psc_listentry	 dcp_lentry;	/* chain on dci  */
	struct psc_dynarray	*dcp_dents_off;	/* dircache_ents sorted by pfd_off */
	void			*dcp_base;	/* pscfs_dirents */
	slfgen_t		 dcp_dirgen;	/* directory generation; used to detect stale pages */
	int			 dcp_refcnt;
};

/* dcp_flags */
#define DIRCACHEPGF_LOADING	(1 << 0)	/* stub is waiting for network load */
#define DIRCACHEPGF_EOF		(1 << 1)	/* denotes last page */
#define DIRCACHEPGF_READ	(1 << 2)	/* page has been used */
#define DIRCACHEPGF_FREEING	(1 << 3)	/* a thread is trying to free */

/*
 * This structure is used to decide when to evict a page.  It is
 * assigned the 'now' timestamp minus the page timeout intervals then
 * these values are checked against the time stored when the page was
 * populated.
 */
struct dircache_expire {
	struct pfl_timespec	 dexp_soft;	/* used if page was read */
	struct pfl_timespec	 dexp_hard;	/* max */
};

/*
 * Determine if a page of dirents should be evicted.
 * Conditions:
 *   (1) no current references to this page
 *   (2) page was READ and is older than soft timeout: evict.
 *   (3) page is older than hard timeout: evict.
 *   (4) page is older than directory's mtime: evict.
 *   (5) page references an older directory generation: evict.
 */
#define DIRCACHEPG_EXPIRED(d, p, dexp)					\
	((p)->dcp_refcnt == 0 &&					\
	 (((p)->dcp_flags & DIRCACHEPGF_READ &&				\
	  timespeccmp(&(dexp)->dexp_soft, &(p)->dcp_local_tm, >)) ||	\
	  timespeccmp(&(dexp)->dexp_hard, &(p)->dcp_local_tm, >) ||	\
	  memcmp(&(d)->fcmh_sstb.sst_mtim, &(p)->dcp_remote_tm,		\
	    sizeof((p)->dcp_remote_tm)) ||				\
	  (p)->dcp_dirgen != fcmh_2_gen(d)))

#define DIRCACHEPG_INITEXP(dexp)					\
	do {								\
		PFL_GETPTIMESPEC(&(dexp)->dexp_soft);			\
		(dexp)->dexp_hard = (dexp)->dexp_soft;			\
		(dexp)->dexp_soft.tv_sec -= DIRCACHEPG_SOFT_TIMEO;	\
		(dexp)->dexp_hard.tv_sec -= DIRCACHEPG_HARD_TIMEO;	\
	} while (0)

#define PFLOG_DIRCACHEPG(lvl, p, fmt, ...)				\
	psclog((lvl), "dcp@%p off %"PSCPRIdOFFT" rf %d gen %"PRId64" "	\
	    "sz %zu fl %#x nextoff %"PSCPRIdOFFT": " fmt,		\
	    (p), (p)->dcp_off, (p)->dcp_refcnt, (p)->dcp_dirgen,	\
	    (p)->dcp_size, (p)->dcp_flags, (p)->dcp_nextoff, ## __VA_ARGS__)

/* This is analogous to 'struct dirent' many of which reside in a page. */
struct dircache_ent {
	uint64_t		 dce_key;
	uint64_t		 dce_pfid;
	int			 dce_index;
	struct dircache_page	*dce_page;
	struct pscfs_dirent	*dce_pfd;
	struct psc_hashentry	 dce_hentry;
#define dce_lentry dce_hentry.phe_lentry
};

struct dircache_ent_query {
	uint64_t		 dcq_key;
	uint64_t		 dcq_pfid;
	uint32_t		 dcq_namelen;
	const char		*dcq_name;
};

/* The different interfaces below are used for searching and sorting. */
static __inline int
dce_cmp_off_search(const void *a, const void *b)
{
	const struct dircache_ent *y = b;
	const off_t *xoff = a;

	return (CMP((uint64_t)*xoff, y->dce_pfd->pfd_off));
}

static __inline int
dce_cmp_off(const void *a, const void *b)
{
	const struct dircache_ent *x = a, *y = b;

	return (CMP(x->dce_pfd->pfd_off, y->dce_pfd->pfd_off));
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
void	dircache_mgr_init(void);
void	dircache_purge(struct fidc_membh *);
void	dircache_reg_ents(struct fidc_membh *, struct dircache_page *,
	    size_t, void *, size_t, int);
void	dircache_walk(struct fidc_membh *, void (*)(struct dircache_page *,
	    struct dircache_ent *, void *), void *);
int	dircache_ent_cmp(const void *, const void *);

#define NAMECACHELOOKUPF_PEEK		0
#define NAMECACHELOOKUPF_DELETE		1
#define NAMECACHELOOKUPF_UPDATE		2

#define namecache_delete(p, name)	_namecache_lookup((p), (name), 0, NAMECACHELOOKUPF_DELETE)
#define namecache_lookup(p, name)	_namecache_lookup((p), (name), 0, NAMECACHELOOKUPF_PEEK)
#define namecache_update(p, name, fid)	_namecache_lookup((p), (name), (fid), NAMECACHELOOKUPF_UPDATE)

void	 namecache_purge(struct fidc_membh *);
slfid_t	_namecache_lookup(struct fidc_membh *, const char *, uint64_t, int);
void	 namecache_insert(struct fidc_membh *, const char *, uint64_t);

struct psc_hashtbl msl_namecache_hashtbl;

#endif /* _DIRCACHE_H_ */
