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
 * Release a page of dirents from cache.
 * @d: directory handle.
 * @p: page to release.
 * @block: whether to block for all other references to release.
 */
void
dircache_free_page(struct fidc_membh *d, struct dircache_page *p)
{
	struct fcmh_cli_info *fci;

	DIRCACHE_WR_ENSURE(d);
	fci = fcmh_2_fci(d);

	psc_assert(!p->dcp_refcnt);
	psc_assert(!(p->dcp_flags & DIRCACHEPGF_FREEING));

	p->dcp_flags |= DIRCACHEPGF_FREEING;

	if ((p->dcp_flags & DIRCACHEPGF_READ) == 0)
		OPSTAT_INCR("msl.dircache-unused-page");

	pll_remove(&fci->fci_dc_pages, p);

	PSCFREE(p->dcp_base);
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "free dir=%p", d);
	psc_pool_return(dircache_page_pool, p);
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
	struct pscfs_dirent *dirent;
	off_t adj;
	int i;

	if (off == p->dcp_off)
		return (1);
	if (off == p->dcp_nextoff)
		return (0);

	for (i = 0, adj = 0; i < p->dcp_nents; i++) {
		dirent = PSC_AGP(p->dcp_base, adj);
		if (dirent->pfd_off == (uint64_t)off)
			return (1);
		adj += PFL_DIRENT_SIZE(dirent->pfd_namelen);
	}
	return (0);
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
