/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2010-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

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
 *
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

struct psc_compl;

struct fidc_membh;

#define DIRCACHE_NPAGES		64		/* initial number of pages in pool*/

#define DIRCACHEPG_SOFT_TIMEO	4		/* expiration after page read */
#define DIRCACHEPG_HARD_TIMEO	30		/* expiration regardless if read */

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

#define DIRCACHE_WRLOCK(d)	pfl_rwlock_wrlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_REQWRLOCK(d)	pfl_rwlock_reqwrlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_UREQLOCK(d, l)	pfl_rwlock_ureqlock(fcmh_2_dc_rwlock(d), (l))
#define DIRCACHE_RDLOCK(d)	pfl_rwlock_rdlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_ULOCK(d)	pfl_rwlock_unlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_WR_ENSURE(d)	psc_assert(pfl_rwlock_haswrlock(fcmh_2_dc_rwlock(d)))

#define DIRCACHE_WAKE(d)						\
	do {								\
		int _waslocked;						\
									\
		_waslocked = DIRCACHE_REQWRLOCK(d);			\
		psc_waitq_wakeall(&(d)->fcmh_waitq);			\
		DIRCACHE_UREQLOCK((d), _waslocked);			\
	} while (0)

#define DIRCACHE_WAIT(d)						\
	do {								\
		DIRCACHE_WR_ENSURE(d);					\
		psc_waitq_waitf(&(d)->fcmh_waitq, PFL_LOCKPRIMT_RWLOCK,	\
		    fcmh_2_dc_rwlock(d));				\
		DIRCACHE_WRLOCK(d);					\
	} while (0)

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

#define PFLOG_DIRCACHENT(lvl, e, fmt, ...)				\
	psclog((lvl), "dce@%p pfd=%p page=%p pfid="SLPRI_FID" "		\
	    "fid="SLPRI_FID" off=%"PRId64" "				\
	    "type=%#o flags=%#x name='%.*s' " fmt,			\
	    (e), (e)->dce_pfd, (e)->dce_page,				\
	    (e)->dce_pfid, (e)->dce_pfd->pfd_ino,			\
	    (e)->dce_pfd->pfd_off, (e)->dce_pfd->pfd_type,		\
	    (e)->dce_flags, (e)->dce_pfd->pfd_namelen,			\
	    (e)->dce_pfd->pfd_name, ## __VA_ARGS__)

/*
 * This is essentially a pointer to a pscfs_dirent.  Many of these
 * reside in one dircache_page but may exist totally independently if
 * brought in through certain namespace operations.
 */
struct dircache_ent {
	uint64_t		 dce_key;	/* hash table key */
	slfid_t			 dce_pfid;	/* parent dir FID+GEN, for hashtbl cmp */
	uint32_t		 dce_flags;	/* see DCEF_* flags below */
	struct dircache_page	*dce_page;	/* back pointer to READDIR page */
	struct pscfs_dirent	*dce_pfd;	/* actual dirent */
	struct psc_hashentry	 dce_hentry;	/* hash table linkage */
#define dce_lentry dce_hentry.phe_lentry
};

/* dce_flags */
#define DCEF_HOLD		(1 << 0)	/* being updated via RPC */
#define DCEF_DESTROYED		(1 << 1)	/* garbage collected */
#define DCEF_ACTIVE		(1 << 2)	/* in hash table */
#define DCEF_FREEME		(1 << 3)	/* HOLD'er thread must free */
#define DCEF_DETACHED		(1 << 4)	/* not on fcid_ents list */

/*
 * This structure is almost identical to dircache_ent but slightly
 * different solely to accommodate hash table lookups.
 */
struct dircache_ent_query {
	uint64_t		 dcq_key;	/* hash table key */
	slfid_t			 dcq_pfid;	/* parent dir FID+GEN */
	uint32_t		 dcq_namelen;	/* strlen(dcq_name) */
	const char		*dcq_name;	/* entry basename */
};

/* struct to simplify updating entries */
struct dircache_ent_update {
	struct dircache_ent	 *dcu_dce;	/* dirent */
	struct psc_hashbkt	 *dcu_bkt;	/* namecache hashtable */
	struct fidc_membh	 *dcu_d;	/* parent directory */
};

#define DCE_UPD_INIT		{ NULL, NULL, NULL }

struct slc_wkdata_dircache {
	struct fidc_membh	 *d;
	void			(*cbf)(struct dircache_page *,
				    struct dircache_ent *, void *);
	void			 *cbarg;
	struct psc_compl	 *compl;
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
void	dircache_mgr_destroy(void);
void	dircache_mgr_init(void);
void	dircache_init(struct fidc_membh *);
void	dircache_purge(struct fidc_membh *);
int	dircache_reg_ents(struct fidc_membh *, struct dircache_page *,
	    int *, void *, size_t, int);
void	dircache_walk_async(struct fidc_membh *, void (*)(
	    struct dircache_page *, struct dircache_ent *, void *),
	    void *, struct psc_compl *);
int	dircache_ent_cmp(const void *, const void *);
void	dircache_walk(struct fidc_membh *, void (*)(struct dircache_page *,
	    struct dircache_ent *, void *), void *);

#define namecache_get_entry(dcu, d, name)				\
	_namecache_get_entry(PFL_CALLERINFO(), (dcu), (d), (name), 0)

#define namecache_hold_entry(dcu, d, name)				\
	_namecache_get_entry(PFL_CALLERINFO(), (dcu), (d), (name), 1)

/* XXX capitalize this name to inform it's a macro */
#define namecache_update(dcu, fid, rc)					\
	_namecache_update(PFL_CALLERINFO(), (dcu),			\
	    (rc) ? FID_ANY : (fid), (rc))

#define namecache_fail(dcu)						\
	namecache_update((dcu), FID_ANY, -1)

int	_namecache_get_entry(const struct pfl_callerinfo *,
	    struct dircache_ent_update *, struct fidc_membh *,
	    const char *, int);
void	 namecache_get_entries(struct dircache_ent_update *,
	    struct fidc_membh *, const char *,
	    struct dircache_ent_update *,
	    struct fidc_membh *, const char *);
void	_namecache_update(const struct pfl_callerinfo *,
	    struct dircache_ent_update *, uint64_t, int);
void	 namecache_delete(struct dircache_ent_update *, int);
void	 namecache_purge(struct fidc_membh *);

extern struct psc_hashtbl msl_namecache_hashtbl;

#endif /* _DIRCACHE_H_ */
