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
 * This API implements the core READDIR dirents caching into buffers
 * called 'dircache_page'.
 *
 * TODO
 *  - use the fuse async API to automatically insert entries into the
 *    name cache after READDIR instead of waiting for LOOKUPs
 */

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fs.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/opstats.h"
#include "pfl/pool.h"
#include "pfl/str.h"
#include "pfl/workthr.h"

#include "dircache.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "sltypes.h"
#include "slutil.h"

struct psc_poolmaster	 dircache_page_poolmaster;
struct psc_poolmgr	*dircache_page_pool;

struct psc_poolmaster	 dircache_ent_poolmaster;
struct psc_poolmgr	*dircache_ent_pool;

struct psc_lockedlist	 msl_dircache_pages_lru;

/*
 * Initialize per-fcmh dircache structures.
 */
void
dircache_init(struct fidc_membh *d)
{
	struct fcmh_cli_info *fci = fcmh_2_fci(d);

	psc_assert(!(d->fcmh_flags & FCMHF_INIT_DIRCACHE));
	d->fcmh_flags |= FCMHF_INIT_DIRCACHE;

	pll_init(&fci->fci_dc_pages, struct dircache_page, dcp_lentry,
	    &d->fcmh_lock);
	pfl_rwlock_init(&fci->fcid_dircache_rwlock);
}

/*
 * Initialize global dircache structures.
 */
void
dircache_mgr_init(void)
{
	psc_poolmaster_init(&dircache_page_poolmaster,
	    struct dircache_page, dcp_lentry, PPMF_AUTO,
	    DIRCACHE_NPAGES, DIRCACHE_NPAGES, 0, NULL,
	    "dircachepg");
	dircache_page_pool = psc_poolmaster_getmgr(
	    &dircache_page_poolmaster);

	psc_poolmaster_init(&dircache_ent_poolmaster,
	    struct dircache_ent, dce_lentry, PPMF_AUTO, DIRCACHE_NPAGES,
	    DIRCACHE_NPAGES, 0, NULL, "dircachent");
	dircache_ent_pool = psc_poolmaster_getmgr(
	    &dircache_ent_poolmaster);
}

void
dircache_mgr_destroy(void)
{
	pfl_poolmaster_destroy(&dircache_page_poolmaster);
	pfl_poolmaster_destroy(&dircache_ent_poolmaster);
}

/*
 * Perform a dircache_ent comparison for use by the hash table API to
 * disambiguate entries with the same hash key.
 */
int
dircache_ent_cmp(const void *a, const void *b)
{
	const struct dircache_ent *da = a, *db = b;

	return (da->dce_pfid == db->dce_pfid &&
	    da->dce_pfd->pfd_namelen == db->dce_pfd->pfd_namelen &&
	    strncmp(da->dce_pfd->pfd_name, db->dce_pfd->pfd_name,
	    da->dce_pfd->pfd_namelen) == 0);
}

/*
 * Perform a comparison for use by the hash table search to disambiguate
 * entries with the same hash key against a query structures.
 */
int
dircache_entq_cmp(const void *a, const void *b)
{
	const struct dircache_ent_query *da = a;
	const struct dircache_ent *db = b;

	return (da->dcq_pfid == db->dce_pfid &&
	    da->dcq_namelen == db->dce_pfd->pfd_namelen &&
	    strncmp(da->dcq_name, db->dce_pfd->pfd_name,
	    da->dcq_namelen) == 0);
}

#define dircache_ent_destroy_locked(d, dce)				\
	_dircache_ent_destroy((d), (dce), 1)
#define dircache_ent_destroy(d, dce)					\
	_dircache_ent_destroy((d), (dce), 0)

/*
 * Release memory for a dircache_ent.
 *
 * If the entry came from a READDIR page, it is marked as stale and the
 * entire page is marked as expired so it is not used.  The entry itself
 * is not released so as to not screw with the page dcp_dents_off list
 * (although the page shouldn't be used).
 *
 * If the entry came from an individual population (ent_pool instead of
 * page_pool), it is removed and memory released.
 */
void
_dircache_ent_destroy(struct fidc_membh *d, struct dircache_ent *dce,
    int locked)
{
	struct fcmh_cli_info *fci;
	struct dircache_page *p;

	OPSTAT_INCR("msl.dircache-ent-destroy");

	PFLOG_DIRCACHENT(PLL_DEBUG, dce, "delete");

	fci = fcmh_2_fci(d);
	if (!locked)
		DIRCACHE_WRLOCK(d);
	p = dce->dce_page;
	dce->dce_pfd->pfd_ino = FID_ANY;
	psc_assert(!(dce->dce_flags & DCEF_ACTIVE));
	psc_assert(!(dce->dce_flags & DCEF_DESTROYED));
	dce->dce_flags |= DCEF_DESTROYED;
	dce->dce_flags &= ~DCEF_HOLD;
	if (p) {
		/* force expiration of the entire page */
		PFL_GETPTIMESPEC(&p->dcp_local_tm);
		p->dcp_local_tm.tv_sec -= DIRCACHEPG_HARD_TIMEO;
	} else if (!(dce->dce_flags & DCEF_DETACHED)) {
		dce->dce_flags |= DCEF_DETACHED;
		psc_dynarray_removeitem(&fci->fcid_ents, dce);
	}
	if (!locked)
		DIRCACHE_ULOCK(d);

	if (p == NULL) {
		PSCFREE(dce->dce_pfd);
		psc_pool_return(dircache_ent_pool, dce);
	}
}

/*
 * Release a page of dirents from cache.
 * @d: directory handle.
 * @p: page to release.
 * @block: whether to block for all other references to release.
 */
int
dircache_free_page(struct fidc_membh *d, struct dircache_page *p)
{
	struct fcmh_cli_info *fci;
	struct pscfs_dirent *pfd;
	struct dircache_ent *dce;
	struct psc_hashbkt *b;
	int i;

	DIRCACHE_WR_ENSURE(d);
	fci = fcmh_2_fci(d);

	psc_assert(!p->dcp_refcnt);
	psc_assert(!(p->dcp_flags & DIRCACHEPGF_FREEING));

	p->dcp_flags |= DIRCACHEPGF_FREEING;

	if ((p->dcp_flags & DIRCACHEPGF_READ) == 0)
		OPSTAT_INCR("msl.dircache-unused-page");

	pll_remove(&fci->fci_dc_pages, p);

	if (p->dcp_dents_off) {
		DYNARRAY_FOREACH(dce, i, p->dcp_dents_off) {
			PFLOG_DIRCACHENT(PLL_DEBUG, dce, "free_page "
			    "fcmh=%p", d);
			pfd = dce->dce_pfd;
			if (pfd->pfd_ino != FID_ANY) {
				b = psc_hashent_getbucket(
				    &msl_namecache_hashtbl, dce);
				dce->dce_pfd = NULL;
				if (dce->dce_flags & DCEF_ACTIVE) {
					psc_hashbkt_del_item(
					    &msl_namecache_hashtbl, b,
					    dce);
					dce->dce_flags &= ~DCEF_ACTIVE;
				}
				if (dce->dce_flags & DCEF_HOLD) {
					dce->dce_flags |= DCEF_TOFREE;
					dce = NULL;
				}
				psc_hashbkt_put(&msl_namecache_hashtbl,
				    b);
			}
			if (dce)
				psc_pool_return(dircache_ent_pool, dce);
		}
		psc_dynarray_free(p->dcp_dents_off);
		PSCFREE(p->dcp_dents_off);
	}
	PSCFREE(p->dcp_base);
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "free dir=%p", d);
	psc_pool_return(dircache_page_pool, p);

	DIRCACHE_WAKE(d);

	return (1);
}

/*
 * Perform an operation on each cached dirent referenced by an fcmh.
 *
 * @d: directory handle.
 * @cbf: callback to run.
 * @cbarg: callback argument.
 */
void
dircache_walk(struct fidc_membh *d, void (*cbf)(struct dircache_page *,
    struct dircache_ent *, void *), void *cbarg)
{
	struct fcmh_cli_info *fci;
	struct dircache_page *p, *np;
	struct dircache_ent *dce;
	int n;

	fci = fcmh_2_fci(d);
	DIRCACHE_RDLOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_rc || p->dcp_flags & DIRCACHEPGF_LOADING)
			continue;
		DYNARRAY_FOREACH(dce, n, p->dcp_dents_off)
			cbf(p, dce, cbarg);
	}
	DYNARRAY_FOREACH(dce, n, &fci->fcid_ents)
		cbf(NULL, dce, cbarg);
	DIRCACHE_ULOCK(d);
}

int
dircache_walk_async_wkcb(void *arg)
{
	struct slc_wkdata_dircache *wk = arg;

	dircache_walk(wk->d, wk->cbf, wk->cbarg);

	fcmh_op_done_type(wk->d, FCMH_OPCNT_DIRCACHE);

	if (wk->compl)
		psc_compl_ready(wk->compl, 1);

	return (0);
}

/*
 * Perform an operation on each cached dirent referenced by an fcmh
 * asynchronously.
 */
void
dircache_walk_async(struct fidc_membh *d, void (*cbf)(
    struct dircache_page *, struct dircache_ent *, void *), void *cbarg,
    struct psc_compl *compl)
{
	struct slc_wkdata_dircache *wk;

	wk = pfl_workq_getitem(dircache_walk_async_wkcb,
	    struct slc_wkdata_dircache);
	fcmh_op_start_type(d, FCMH_OPCNT_DIRCACHE);
	wk->d = d;
	wk->cbf = cbf;
	wk->cbarg = cbarg;
	wk->compl = compl;
	pfl_workq_putitem(wk);
}

/*
 * Destroy all dirent pages belonging to a directory.
 * @d: directory handle.
 */
void
dircache_purge(struct fidc_membh *d)
{
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;

	DIRCACHE_WR_ENSURE(d);

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		dircache_free_page(d, p);
}

/*
 * Determine if a dircache page contains an entry at the specified
 * directory 'offset' (which is more like a cookie/ID).
 * @d: directory handle.
 * @off: offset of desired dirent.
 */
int
dircache_hasoff(struct dircache_page *p, off_t off)
{
	struct dircache_ent *dce;
	int n;

	if (off == p->dcp_off)
		return (1);
	if (off == p->dcp_nextoff)
		return (0);

	n = psc_dynarray_bsearch(p->dcp_dents_off, &off,
	    dce_cmp_off_search);
	if (n >= psc_dynarray_len(p->dcp_dents_off))
		return (0);
	dce = psc_dynarray_getpos(p->dcp_dents_off, n);
	return (dce->dce_pfd->pfd_off == (uint64_t)off);
}

/*
 * Allocate a new page of dirents.
 * @d: directory handle.
 * @off: offset into directory for this slew of dirents.
 * @block: whether this call should be non-blocking or not.
 */
struct dircache_page *
dircache_new_page(struct fidc_membh *d, off_t off, int block)
{
	struct dircache_page *p;
	struct fcmh_cli_info *fci;

	if (block)
		p = psc_pool_get(dircache_page_pool);
	else
		p = psc_pool_tryget(dircache_page_pool);

	if (p == NULL)
		return (NULL);

	fci = fcmh_2_fci(d);
	memset(p, 0, sizeof(*p));
	INIT_PSC_LISTENTRY(&p->dcp_lentry);
	p->dcp_off = off;
	pll_addtail(&fci->fci_dc_pages, p);
	p->dcp_flags |= DIRCACHEPGF_LOADING;
	p->dcp_refcnt++;
	p->dcp_dirgen = fcmh_2_gen(d);
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "incref");

	return (p);
}

/*
 * Compute a hash key for a dircache_ent based on the parent
 * directory's FID and the entry's basename.
 * @dce: entry to compute hash for.
 */
uint64_t
dircache_ent_hash(uint64_t pfid, const char *name, size_t namelen)
{
	return (pfid ^ psc_strn_hashify(name, namelen));
}

/*
 * Register directory entries with our cache.
 * @d: directory.
 * @p: buffer of dirent objects.
 * @nents: number of dirent objects in @p.
 * @base: pointer to buffer of pscfs_dirents from RPC.
 * @size: size of @base buffer.
 * @eof: whether this signifies the last READDIR for this directory.
 */
int
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    int *nents, void *base, size_t size, int eof)
{
	struct pscfs_dirent *dirent = NULL;
	struct dircache_ent *dce, *dce2;
	struct psc_dynarray *da_off;
	struct psc_hashbkt *b;
	off_t adj;
	int i;

	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "registering");

	if (p->dcp_dirgen != fcmh_2_gen(d)) {
		OPSTAT_INCR("msl.readdir-all-stale");
		return (-ESTALE);
	}
	p->dcp_base = base;
	p->dcp_size = size;
	PFL_GETPTIMESPEC(&p->dcp_local_tm);
	p->dcp_remote_tm = d->fcmh_sstb.sst_mtim;
	if (eof)
		p->dcp_flags |= DIRCACHEPGF_EOF;
	DIRCACHE_ULOCK(d);
	return (0);
}
