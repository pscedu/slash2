/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2010-2018, Pittsburgh Supercomputing Center
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

#define DIRCACHE_NPAGES		64		/* initial number of pages in pool */
#define DIRCACHE_NAMECACHE	2048		/* initial number of name cache enties in pool */

#define DIRCACHEPG_SOFT_TIMEO	30		/* expiration regardless if read or not */

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
	long			 dcp_expire;	/* expiration time */
	struct psc_listentry	 dcp_lentry;	/* chain on dci  */
	void			*dcp_base;	/* pscfs_dirents */
	slfgen_t		 dcp_dirgen;	/* directory generation; used to detect stale pages */
	int			 dcp_refcnt;
	int			 dcp_nents;
};

/* dcp_flags */
#define DIRCACHEPGF_LOADING	(1 << 0)	/* stub is waiting for network load */
#define DIRCACHEPGF_EOF		(1 << 1)	/* denotes last page */
#define DIRCACHEPGF_READ	(1 << 2)	/* page has been used */
#define DIRCACHEPGF_WAIT	(1 << 3)	/* someone is waiting */
#define DIRCACHEPGF_ASYNC	(1 << 4)	/* asynchronous readdir */
#define DIRCACHEPGF_FREEING	(1 << 5)	/* a thread is trying to free */

#define DIRCACHE_WRLOCK(d)	pfl_rwlock_wrlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_RDLOCK(d)	pfl_rwlock_rdlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_ULOCK(d)	pfl_rwlock_unlock(fcmh_2_dc_rwlock(d))
#define DIRCACHE_WR_ENSURE(d)	psc_assert(pfl_rwlock_haswrlock(fcmh_2_dc_rwlock(d)))

#define DIRCACHE_WAKE(d)						\
	do {								\
		DIRCACHE_WR_ENSURE(d);					\
		psc_waitq_wakeall(&(d)->fcmh_waitq);			\
		DIRCACHE_ULOCK(d);					\
	} while (0)

#define DIRCACHE_WAIT(d)						\
	do {								\
		DIRCACHE_WR_ENSURE(d);					\
		psc_waitq_waitf(&(d)->fcmh_waitq, PFL_LOCKPRIMT_RWLOCK,	\
		    fcmh_2_dc_rwlock(d));				\
		DIRCACHE_WRLOCK(d);					\
	} while (0)

/*
 * Determine if a page of dirents should be evicted.
 */
#define DIRCACHEPG_EXPIRED(d, p, expire)				\
	((expire) > (p)->dcp_expire ||					\
	  (p)->dcp_dirgen != fcmh_2_gen(d))

#define PFLOG_DIRCACHEPG(lvl, p, fmt, ...)				\
	psclog((lvl), "dcp@%p off %"PSCPRIdOFFT" rf %d gen %"PRId64" "	\
	    "sz %zu fl %#x nextoff %"PSCPRIdOFFT": " fmt,		\
	    (p), (p)->dcp_off, (p)->dcp_refcnt, (p)->dcp_dirgen,	\
	    (p)->dcp_size, (p)->dcp_flags, (p)->dcp_nextoff, ## __VA_ARGS__)

#define	SL_MAX_SHORT_NAME	32

#define	DIRCACHE_F_NONE 	0x00
#define	DIRCACHE_F_SHORT	0x01
#define	DIRCACHE_F_LIST		0x02		/* debug */
#define	DIRCACHE_F_HASH		0x04		/* debug */

struct dircache_ent {
	uint64_t		 dce_key;	/* hash table key */
	struct psc_hashentry     dce_hentry;    /* hash table linkage */
#define dce_lentry dce_hentry.phe_lentry

	struct psc_listentry	 dce_entry;     /* per directory linkage */
	uint64_t		 dce_pino;
	uint64_t		 dce_ino;
	uint32_t		 dce_namelen;
	long			 dce_expire;
	int			 dce_flag;
	char			 dce_short[SL_MAX_SHORT_NAME];
	char			*dce_name;	/* NOT null-terminated */
};

struct dircache_page *
	dircache_new_page(struct fidc_membh *, off_t, int);
int	dircache_hasoff(struct dircache_page *, off_t);
void	dircache_free_page(struct fidc_membh *, struct dircache_page *);
void	dircache_mgr_destroy(void);
void	dircache_mgr_init(void);
void	dircache_init(struct fidc_membh *);
void	dircache_purge(struct fidc_membh *);
void	dircache_reg_ents(struct fidc_membh *, struct dircache_page *, 
	    int, void *, size_t, int, int32_t);
void	dircache_walk_async(struct fidc_membh *, void (*)(
	    struct dircache_page *, struct dircache_ent *, void *),
	    void *, struct psc_compl *);
int	dircache_ent_cmp(const void *, const void *);
void	dircache_walk(struct fidc_membh *, void (*)(struct dircache_page *,
	    struct dircache_ent *, void *), void *);

int	dircache_ent_cmp(const void *, const void *);

void	dircache_lookup(struct fidc_membh *, const char *, uint64_t *);
void	dircache_insert(struct fidc_membh *, const char *, uint64_t, int32_t);
void	dircache_delete(struct fidc_membh *, const char *);
void	dircache_trim(struct fidc_membh *, int);

extern struct psc_hashtbl msl_namecache_hashtbl;

#endif /* _DIRCACHE_H_ */
