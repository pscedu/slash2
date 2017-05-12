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

int	psc_disable_namecache = 0;

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
 * Compute a hash key for a dircache_ent based on the parent
 * directory's FID and the entry's basename.
 * @dce: entry to compute hash for.
 */
uint64_t
dircache_hash(uint64_t pfid, const char *name, size_t namelen)
{
	return (pfid ^ psc_strn_hashify(name, namelen));
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

/*
 * Destroy all dirent pages belonging to a directory.
 * @d: directory handle.
 */
void
dircache_purge(struct fidc_membh *d)
{
	int i;
	struct psc_hashbkt *b;
	struct dircache_ent *dce;
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;

	DIRCACHE_WR_ENSURE(d);

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		dircache_free_page(d, p);

	/* (gdb) p fci.u.d.ents */
	DYNARRAY_FOREACH(dce, i, &fci->fcid_ents) {
		b = psc_hashent_getbucket(&msl_namecache_hashtbl, dce);
		psc_hashbkt_del_item(&msl_namecache_hashtbl, b, dce);
		psc_hashbkt_put(&msl_namecache_hashtbl, b);
		if (!(dce->dce_flag & DIRCACHE_F_SHORT))
			PSCFREE(dce->dce_name);
		psc_pool_return(dircache_ent_pool, dce);
	}
	psc_dynarray_free(&fci->fcid_ents);
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


/*
 * Perform a dircache_ent comparison for use by the hash table API to
 * disambiguate entries with the same hash key.
 */
int
dircache_ent_cmp(const void *a, const void *b) 
{
	const struct dircache_ent *da = a, *db = b;

	return (da->dce_pino == db->dce_pino &&
		da->dce_namelen == db->dce_namelen &&
		strncmp(da->dce_name, db->dce_name, da->dce_namelen) == 0); 
}

int
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    int nents, void *base, size_t size, int eof)
{
	struct dircache_ent *dce, *tmpdce;
	int i;
	off_t adj;
	struct pscfs_dirent *dirent = NULL;
	struct fcmh_cli_info *fci;
	struct psc_hashbkt *b;

	/*
 	 * Stop name cache changes while we populating it.
 	 *
 	 * We should limit the number of name cache entries
 	 * per directory or system wide here. However, when
 	 * the name is found in the look up path, it must
 	 * be created for possibly silly renaming support.
 	 */
	FCMH_LOCK(d);
	FCMH_WAIT_BUSY(d, 1);

	if (p->dcp_dirgen != fcmh_2_gen(d)) {
		OPSTAT_INCR("msl.readdir-all-stale");
		FCMH_UNBUSY(d, 1);
		return (-ESTALE);
	}

	fci = fcmh_get_pri(d);
	DIRCACHE_WRLOCK(d);

	/*
 	 * We used to allow an entry to point to a dirent inside the
 	 * readdir page or allocate its own memory. It is tricky and
 	 * we can't let the entry to last longer than its associated
 	 * page.  So let us keep it as simple as possible.
 	 */
	for (i = 0, adj = 0; i < nents; i++) {
		dirent = PSC_AGP(base, adj);
		adj += PFL_DIRENT_SIZE(dirent->pfd_namelen);

		if (dirent->pfd_namelen >= SL_SHORT_NAME) {
			OPSTAT_INCR("msl.dircache-longname");
			continue;
		}
		dce = psc_pool_shallowget(dircache_ent_pool);
		if (!dce) {
			OPSTAT_INCR("msl.dircache-pool-low");
			continue;
		}

		dce->dce_ino = dirent->pfd_ino;
		dce->dce_pino = fcmh_2_fid(d);
		dce->dce_namelen = dirent->pfd_namelen;
		dce->dce_flag = DIRCACHE_F_SHORT;
		dce->dce_name = &dce->dce_short[0];
		strncpy(dce->dce_name, dirent->pfd_name, dce->dce_namelen);
		dce->dce_key = dircache_hash(dce->dce_pino, dce->dce_name, 
		    dce->dce_namelen);

		psc_hashent_init(&msl_namecache_hashtbl, dce);
		b = psc_hashbkt_get(&msl_namecache_hashtbl, &dce->dce_key);

		tmpdce = _psc_hashbkt_search(&msl_namecache_hashtbl, b, 0,
			dircache_ent_cmp, dce, NULL, NULL, &dce->dce_key);
		if (!tmpdce)
			psc_hashbkt_add_item(&msl_namecache_hashtbl, b, dce);
		psc_hashbkt_put(&msl_namecache_hashtbl, b);
	
		if (strncmp(dirent->pfd_name, "dt-bindings", 11) == 0)
			psclog_warn("insert2");

		if (!tmpdce) {
			psc_dynarray_add(&fci->fcid_ents, dce);
			OPSTAT_INCR("msl.dircache-insert");
		} else {
			OPSTAT_INCR("msl.dircache-discard");
			psc_pool_return(dircache_ent_pool, dce);
		}
	}

	p->dcp_nents = nents;
	p->dcp_base = base;
	p->dcp_size = size;
	PFL_GETPTIMESPEC(&p->dcp_local_tm);
	p->dcp_remote_tm = d->fcmh_sstb.sst_mtim;
	p->dcp_flags |= eof ? DIRCACHEPGF_EOF : 0;
	p->dcp_nextoff = dirent ? (off_t)dirent->pfd_off : p->dcp_off;

	DIRCACHE_ULOCK(d);
	FCMH_UNBUSY(d, 1);

	return (0);
}


int
dircache_lookup(struct fidc_membh *d, const char *name, uint64_t *ino)
{
	int rc = 1, len;
	struct psc_hashbkt *b;
	struct dircache_ent *dce, tmpdce;

	if (psc_disable_namecache)
		return (rc);

	DIRCACHE_WRLOCK(d);
	len = strlen(name);
	tmpdce.dce_name = (char *) name;
	tmpdce.dce_namelen = len;
	tmpdce.dce_pino = fcmh_2_fid(d);
	tmpdce.dce_key = dircache_hash(tmpdce.dce_pino, name, len);

	b = psc_hashbkt_get(&msl_namecache_hashtbl, &tmpdce.dce_key);

	dce = _psc_hashbkt_search(&msl_namecache_hashtbl, b, 0,
		dircache_ent_cmp, &tmpdce, NULL, NULL, &tmpdce.dce_key);

	if (dce) {
		rc = 0;
		*ino = dce->dce_ino;
	}
	psc_hashbkt_put(&msl_namecache_hashtbl, b);

	if (dce && strncmp(dce->dce_name, "dt-bindings", 11) == 0)
		psclog_warn("lookup");

	DIRCACHE_ULOCK(d);

	return (rc);
}

/*
 * Add a name after a successful lookup.
 */
void
dircache_insert(struct fidc_membh *d, const char *name, uint64_t ino)
{
	int len;
	struct psc_hashbkt *b;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce, *tmpdce;

	if (psc_disable_namecache)
		return;

	fci = fcmh_get_pri(d);
	DIRCACHE_WRLOCK(d);

	len = strlen(name);
	dce = psc_pool_get(dircache_ent_pool);

	dce->dce_flag = 0;
	dce->dce_namelen = len;
	if (len < SL_SHORT_NAME) {
		OPSTAT_INCR("msl.dircache-insert-short");
		dce->dce_flag |= DIRCACHE_F_SHORT;
		dce->dce_name = &dce->dce_short[0];
	} else {
		OPSTAT_INCR("msl.dircache-insert-long");
		dce->dce_name = PSCALLOC(len);
	}

	strncpy(dce->dce_name, name, dce->dce_namelen);

	dce->dce_ino = ino;
	dce->dce_pino = fcmh_2_fid(d);
	dce->dce_key = dircache_hash(dce->dce_pino, dce->dce_name, 
	    dce->dce_namelen);

	b = psc_hashbkt_get(&msl_namecache_hashtbl, &dce->dce_key);

	tmpdce = _psc_hashbkt_search(&msl_namecache_hashtbl, b, 0,
	    dircache_ent_cmp, dce, NULL, NULL, &dce->dce_key);

	if (tmpdce) {
		OPSTAT_INCR("msl.dircache-update");
		psc_hashbkt_del_item(&msl_namecache_hashtbl, b, tmpdce);
		if (!(tmpdce->dce_flag & DIRCACHE_F_SHORT))
			PSCFREE(tmpdce->dce_name);
		psc_pool_return(dircache_ent_pool, tmpdce);
	}

	if (strncmp(name, "dt-bindings", 11) == 0)
		psclog_warn("insert (%d)", (int)strlen(name));

	psc_hashbkt_add_item(&msl_namecache_hashtbl, b, dce);
	psc_hashbkt_put(&msl_namecache_hashtbl, b);
	psc_dynarray_add(&fci->fcid_ents, dce);

	DIRCACHE_ULOCK(d);
}

void
dircache_delete(struct fidc_membh *d, const char *name)
{
	int len;
	uint64_t key;
	struct fcmh_cli_info *fci;
	struct psc_hashbkt *b;
	struct dircache_ent *dce, tmpdce;

	if (psc_disable_namecache)
		return;

	fci = fcmh_get_pri(d);
	DIRCACHE_WRLOCK(d);

	len = strlen(name);
	tmpdce.dce_name = (char *) name;
	tmpdce.dce_namelen = len;
	tmpdce.dce_pino = fcmh_2_fid(d);
	tmpdce.dce_key = dircache_hash(tmpdce.dce_pino, name, len);

	b = psc_hashbkt_get(&msl_namecache_hashtbl, &key);
	dce = _psc_hashbkt_search(&msl_namecache_hashtbl, b, 0,
		dircache_ent_cmp, &tmpdce, NULL, NULL, &tmpdce.dce_key);
	if (dce) {
		OPSTAT_INCR("msl.dircache-delete");
		psc_dynarray_removeitem(&fci->fcid_ents, dce);
		psc_hashbkt_del_item(&msl_namecache_hashtbl, b, dce);
	} else
		OPSTAT_INCR("msl.dircache-delete-nop");

	psc_hashbkt_put(&msl_namecache_hashtbl, b);

	if (strncmp(name, "dt-bindings", 11) == 0)
		psclog_warn("delete");

	DIRCACHE_ULOCK(d);
}
