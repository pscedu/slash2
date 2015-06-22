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
 * This API implements the core READDIR dirents caching into buffers
 * called 'dircache_page'.
 */

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>
#include <string.h>

#include "pfl/alloc.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fs.h"
#include "pfl/iostats.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
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

void
dircache_init(struct fidc_membh *d)
{
	struct fcmh_cli_info *fci = fcmh_2_fci(d);

	if ((d->fcmh_flags & FCMH_CLI_INITDIRCACHE) == 0) {
		pll_init(&fci->fci_dc_pages, struct dircache_page,
		    dcp_lentry, &d->fcmh_lock);
		psc_rwlock_init(&fci->fcid_dircache_rwlock);
		d->fcmh_flags |= FCMH_CLI_INITDIRCACHE;
	}
}

/*
 * Reap old dircache_pages.
 */
int
dircache_page_reap(__unusedx struct psc_poolmgr *m)
{
#if 0
	for () {
		if ()
			break;
	}

	foreach
		free
#endif
	return (0);
}

int
dircache_ent_reap(__unusedx struct psc_poolmgr *m)
{
#if 0
	for () {
		if ()
			break;
	}

	foreach
		free
#endif
	return (0);
}

/*
 * Initialize dircache API.
 */
void
dircache_mgr_init(void)
{
	//pll_init(&msl_dircache_pages_lru, struct dircache_page);
	//pll_init(&msl_dircache_ents_lru, struct dircache_ent);

#define DCP_DEF 64
	psc_poolmaster_init(&dircache_page_poolmaster,
	    struct dircache_page, dcp_lentry, PPMF_AUTO, DCP_DEF,
	    DCP_DEF, 0, NULL, NULL, dircache_page_reap, "dircachepg");
	dircache_page_pool = psc_poolmaster_getmgr(&dircache_page_poolmaster);

	psc_poolmaster_init(&dircache_ent_poolmaster,
	    struct dircache_ent, dce_lentry, PPMF_AUTO, DCP_DEF,
	    DCP_DEF, 0, NULL, NULL, dircache_ent_reap, "dircachent");
	dircache_ent_pool = psc_poolmaster_getmgr(&dircache_ent_poolmaster);
}

/*
 * Perform a dircache_ent_query to dircache_ent comparison for use by
 * the hash table API to disambiguate entries with the same hash key.
 */
int
dircache_ent_cmp(const void *a, const void *b)
{
	const struct dircache_ent_query *da = a;
	const struct dircache_ent *db = b;

	return (da->dcq_pfid == db->dce_pfid &&
	    da->dcq_namelen == db->dce_pfd->pfd_namelen &&
	    strncmp(da->dcq_name, db->dce_pfd->pfd_name,
	    da->dcq_namelen) == 0);
}

void
dircache_ent_zap(struct fidc_membh *d, struct dircache_ent *dce)
{
	void *freedent = NULL;
	struct fcmh_cli_info *fci;
	struct dircache_page *p;
	struct psc_dynarray *a;
	struct dircache_ent *dce2;

	if (dce->dce_page == NULL)
		freedent = dce->dce_pfd;

	fci = fcmh_2_fci(d);
	p = dce->dce_page;
	DIRCACHE_WRLOCK(d);
	if (p) {
		a = p->dcp_dents_off;

		/* force expiration */
		PFL_GETPTIMESPEC(&p->dcp_local_tm);
		p->dcp_local_tm.tv_sec -= DIRCACHEPG_HARD_TIMEO;
	} else {
		a = &fci->fcid_ents;
	}
	psc_dynarray_removepos(a, dce->dce_index);
	if (psc_dynarray_len(a) &&
	    dce->dce_index != psc_dynarray_len(a)) {
		dce2 = psc_dynarray_getpos(a, dce->dce_index);
		dce2->dce_index = dce->dce_index;
	}
	DIRCACHE_ULOCK(d);

	if (freedent)
		PSCFREE(dce->dce_pfd);
	psc_pool_return(dircache_ent_pool, dce);
}

/*
 * Release a page of dirents from cache.
 * @d: directory handle.
 * @p: page to release.
 * @wait: whether to wait for all other references to release.
 */
int
_dircache_free_page(const struct pfl_callerinfo *pci,
    struct fidc_membh *d, struct dircache_page *p, int wait)
{
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	int i;

	DIRCACHE_WR_ENSURE(d);
	fci = fcmh_2_fci(d);

	if (p->dcp_flags & DIRCACHEPGF_FREEING)
		return (0);

	if (p->dcp_refcnt && !wait)
		return (0);

	p->dcp_flags |= DIRCACHEPGF_FREEING;

	if ((p->dcp_flags & DIRCACHEPGF_READ) == 0)
		OPSTAT_INCR("dircache-unused-page");

	while (p->dcp_refcnt)
		DIRCACHE_WAIT(d);

	// XXX this conjoint conditional should not be here
	if (psclist_conjoint(&p->dcp_lentry,
	    psc_lentry_hd(&fci->fci_dc_pages.pll_listhd)))
		pll_remove(&fci->fci_dc_pages, p);

	if (p->dcp_dents_off) {
		DYNARRAY_FOREACH(dce, i, p->dcp_dents_off) {
			psc_hashent_remove(&msl_namecache_hashtbl, dce);
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
 * Perform a batch operation across all cached dirents.
 * @d: directory handle.
 * @cbf: callback to run.
 * @cbarg: callback argument.
 */
int
dircache_walk_wkcb(void *arg)
{
	struct slc_wkdata_dircache *wk = arg;
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	int n;

	fci = fcmh_2_fci(wk->d);
	DIRCACHE_RDLOCK(wk->d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_rc || p->dcp_flags & DIRCACHEPGF_LOADING)
			continue;
		DYNARRAY_FOREACH(dce, n, p->dcp_dents_off)
			wk->cbf(p, dce, wk->cbarg);
	}
	DYNARRAY_FOREACH(dce, n, &fci->fcid_ents)
		wk->cbf(NULL, dce, wk->cbarg);
	DIRCACHE_ULOCK(wk->d);

	fcmh_op_done_type(wk->d, FCMH_OPCNT_DIRCACHE);

	if (wk->compl)
		psc_compl_ready(wk->compl, 1);

	return (0);
}

void
dircache_walk_async(struct fidc_membh *d, void (*cbf)(
    struct dircache_page *, struct dircache_ent *, void *), void *cbarg,
    struct psc_compl *compl)
{
	struct slc_wkdata_dircache *wk;

	wk = pfl_workq_getitem(dircache_walk_wkcb,
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

	fci = fcmh_2_fci(d);
	DIRCACHE_WRLOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		dircache_free_page(d, p);
	DIRCACHE_ULOCK(d);
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
	if (p->dcp_rc)
		return (0);
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
 * @wait: whether this call should be non-blocking or not.
 */
struct dircache_page *
dircache_new_page(struct fidc_membh *d, off_t off, int wait)
{
	struct dircache_page *p, *np, *newp;
	struct dircache_expire dexp;
	struct fcmh_cli_info *fci;

	newp = psc_pool_get(dircache_page_pool);

	DIRCACHE_WRLOCK(d);

 restart:
	DIRCACHEPG_INITEXP(&dexp);

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (DIRCACHEPG_EXPIRED(d, p, &dexp)) {
			dircache_free_page(d, p);
			continue;
		}
		if (p->dcp_flags & DIRCACHEPGF_LOADING) {
			if (p->dcp_off == off) {
				/* Page is waiting for us; use it. */
				p->dcp_flags &= ~DIRCACHEPGF_LOADING;
				break;
			}

			if (wait) {
				DIRCACHE_WAIT(d);
				goto restart;
			}
			continue;
		}
		if (dircache_hasoff(p, off)) {
			/* Stale page in cache; purge and refresh. */
			if (wait)
				dircache_free_page(d, p);
			else if (!dircache_free_page_nowait(d, p)) {
				p = NULL;
				goto out;
			}
			p = NULL;
			break;
		}
	}

	if (p == NULL) {
		memset(newp, 0, sizeof(*newp));
		INIT_PSC_LISTENTRY(&newp->dcp_lentry);
		newp->dcp_off = off;
		pll_addtail(&fci->fci_dc_pages, newp);
		p = newp;
		newp = NULL;
	}
	psc_assert((p->dcp_flags & DIRCACHEPGF_LOADING) == 0);
	p->dcp_flags |= DIRCACHEPGF_LOADING;
	p->dcp_refcnt++;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "incref");

 out:
	DIRCACHE_ULOCK(d);

	if (newp)
		psc_pool_return(dircache_page_pool, newp);

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
void
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    size_t nents, void *base, size_t size, int eof)
{
	struct pscfs_dirent *dirent = NULL;
	struct dircache_ent *dce, *dce2;
	struct psc_dynarray *da_off;
	struct psc_hashbkt *b;
	off_t adj;
	int i;

	OPSTAT_INCR("dircache-reg-ents");
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "registering");

	da_off = PSCALLOC(sizeof(*da_off));
	psc_dynarray_init(da_off);

	psc_dynarray_ensurelen(da_off, nents);

	for (i = 0, adj = 0; i < (int)nents; i++, dce++) {
		dirent = PSC_AGP(base, adj);

		psclog_debug("fid="SLPRI_FID" pfd_off=%"PRId64" "
		    "nlen=%u type=%#o "
		    "name=%.*s dirent=%p adj=%"PRId64,
		    dirent->pfd_ino, dirent->pfd_off,
		    dirent->pfd_namelen, dirent->pfd_type,
		    dirent->pfd_namelen, dirent->pfd_name, dirent, adj);

		dce = psc_pool_get(dircache_ent_pool);
		memset(dce, 0, sizeof(*dce));
		INIT_PSC_LISTENTRY(&dce->dce_lentry);
		dce->dce_page = p;
		dce->dce_pfd = dirent;
		dce->dce_pfid = fcmh_2_fid(d);
		dce->dce_key = dircache_ent_hash(dce->dce_pfid,
		    dce->dce_pfd->pfd_name, dce->dce_pfd->pfd_namelen);
		dce->dce_index = psc_dynarray_len(da_off);
		psc_dynarray_add(da_off, dce);

		adj += PFL_DIRENT_SIZE(dirent->pfd_namelen);
	}

	psc_dynarray_sort(da_off, qsort, dce_sort_cmp_off);

	DIRCACHE_WRLOCK(d);

	if ((p->dcp_flags & DIRCACHEPGF_LOADING) == 0) {
		PFLOG_DIRCACHEPG(PLL_DEBUG, p, "already loaded");
		p->dcp_refcnt--;
		DIRCACHE_ULOCK(d);

		DYNARRAY_FOREACH(dce, i, da_off)
			psc_pool_return(dircache_ent_pool, dce);
		psc_dynarray_free(da_off);
		PSCFREE(da_off);
		PSCFREE(base);
		return;
	}

	DYNARRAY_FOREACH(dce, i, da_off) {
		psc_hashent_init(&msl_namecache_hashtbl, dce);
		b = psc_hashbkt_get(&msl_namecache_hashtbl,
		    &dce->dce_key);
		dce2 = psc_hashbkt_search_cmp(&msl_namecache_hashtbl, b,
		    dce, &dce->dce_key);
		if (dce2) {
			psc_hashbkt_del_item(&msl_namecache_hashtbl, b,
			    dce2);
			dircache_ent_zap(d, dce2);
		}
		psc_hashbkt_add_item(&msl_namecache_hashtbl, b, dce);
		psc_hashbkt_put(&msl_namecache_hashtbl, b);
	}

	psc_assert(p->dcp_dents_off == NULL);

	if (dirent)
		p->dcp_nextoff = dirent->pfd_off;
	else
		p->dcp_nextoff = p->dcp_off;
	p->dcp_dents_off = da_off;
	p->dcp_base = base;
	p->dcp_size = size;
	p->dcp_dirgen = fcmh_2_gen(d);
	PFL_GETPTIMESPEC(&p->dcp_local_tm);
	p->dcp_remote_tm = d->fcmh_sstb.sst_mtim;
	psc_assert(p->dcp_flags & DIRCACHEPGF_LOADING);
	p->dcp_flags &= ~DIRCACHEPGF_LOADING;
	if (eof)
		p->dcp_flags |= DIRCACHEPGF_EOF;
	p->dcp_refcnt--;
	PFLOG_DIRCACHEPG(PLL_DEBUG, p, "decref");
	DIRCACHE_WAKE(d);
	DIRCACHE_ULOCK(d);
}

/*
 * Eradicate all entries in the namecache hashtable cache belonging to
 * the given directory.  We loop through each dircache page and remove
 * all entries and all anonymous (i.e. not referenced by a dircache
 * page) entries referenced by fcid_ents.
 * @d: directory to remove entries for.
 */
void
namecache_purge(struct fidc_membh *d)
{
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	int i;

	fci = fcmh_get_pri(d);
	DYNARRAY_FOREACH(dce, i, &fci->fcid_ents) {
		psc_hashent_remove(&msl_namecache_hashtbl, dce);
		PSCFREE(dce->dce_pfd);
		psc_pool_return(dircache_ent_pool, dce);
	}
	psc_dynarray_free(&fci->fcid_ents);
}

void
dircache_ent_update(void *p, void *arg)
{
	struct dircache_ent *dce = p;

	dce->dce_pfd->pfd_ino = *(uint64_t *)arg;
}

void
dircache_ent_peek(void *p, void *arg)
{
	struct dircache_ent *dce = p;

	*(uint64_t *)arg = dce->dce_pfd->pfd_ino;
}

/*
 * Perform a search in the namecache hash table for a FID based on
 * the parent directory FID and file basename.
 * @d: parent directory.
 * @name: basename to lookup.
 * @cfid: for UPDATE type, new child FID for entry.
 * @op: operation type (LOOKUP, DELETE, etc.).
 */
slfid_t
_namecache_lookup(int op, struct fidc_membh *d, const char *name,
    uint64_t cfid)
{
	void *cbf = NULL, *arg = NULL;
	uint64_t pfid, key, rc = FID_ANY;
	struct dircache_ent *dce, *new_dce;
	struct dircache_ent_query q;
	struct fcmh_cli_info *fci;
	struct psc_hashbkt *b;
	size_t entsz, namelen;
	int flags = 0;

	pfid = fcmh_2_fid(d);
	if (pfid == SLFID_NS)
		return (FID_ANY);
	psc_assert(pfid == SLFID_ROOT || pfid >= SLFID_MIN);

	switch (op) {
	case NAMECACHELOOKUPF_UPDATE:
	case NAMECACHELOOKUPF_CLOBBER:
		cbf = dircache_ent_update;
		arg = &cfid;
		break;
	case NAMECACHELOOKUPF_DELETE:
		flags |= PHLF_DEL;
		break;
	case NAMECACHELOOKUPF_PEEK:
		cbf = dircache_ent_peek;
		arg = &rc;
		/* FALLTHRU */
	default:
		break;
	}

	q.dcq_pfid = pfid;
	q.dcq_name = name;
	q.dcq_namelen = namelen = strlen(name);
	q.dcq_key = key = dircache_ent_hash(pfid, name, namelen);
	dce = _psc_hashtbl_search(&msl_namecache_hashtbl, flags, &q,
	    cbf, arg, &key);

	switch (op) {
	case NAMECACHELOOKUPF_PEEK:
		if (dce)
			OPSTAT_INCR("namecache-hit");
		return (rc);
	case NAMECACHELOOKUPF_DELETE:
		if (dce) {
			dircache_ent_zap(d, dce);
			OPSTAT_INCR("namecache-delete");
		} else
			OPSTAT_INCR("namecache-delete-miss");
		return (rc);
	case NAMECACHELOOKUPF_UPDATE:
		if (dce)
			OPSTAT_INCR("namecache-update");
		return (rc);
	case NAMECACHELOOKUPF_CLOBBER:
		if (dce) {
			OPSTAT_INCR("namecache-update");
			return (rc);
		}
		break;
	case NAMECACHELOOKUPF_INSERT:
		if (dce) {
			OPSTAT_INCR("namecache-insert-collision");
			return (rc);
		}
		break;
	}

	entsz = PFL_DIRENT_SIZE(namelen);
	new_dce = psc_pool_get(dircache_ent_pool);
	memset(new_dce, 0, sizeof(*new_dce));
	INIT_PSC_LISTENTRY(&new_dce->dce_lentry);
	new_dce->dce_pfd = PSCALLOC(entsz);

	b = psc_hashbkt_get(&msl_namecache_hashtbl, &key);
	dce = _psc_hashbkt_search(&msl_namecache_hashtbl, b, flags, &q,
	    NULL, NULL, &key);
	if (dce) {
		OPSTAT_INCR("namecache-insert-race");
	} else {
		OPSTAT_INCR("namecache-insert");

		dce = new_dce;
		new_dce = NULL;

		dce->dce_pfd->pfd_ino = cfid;
		dce->dce_pfd->pfd_namelen = namelen;
		dce->dce_key = key;
		dce->dce_pfid = pfid;
		strncpy(dce->dce_pfd->pfd_name, name, namelen);
		psc_hashent_init(&msl_namecache_hashtbl, dce);
		psc_hashbkt_add_item(&msl_namecache_hashtbl, b,
		    dce);
		fci = fcmh_2_fci(d);
		DIRCACHE_WRLOCK(d);
		dce->dce_index = psc_dynarray_len(&fci->fcid_ents);
		psc_dynarray_add(&fci->fcid_ents, dce);
		DIRCACHE_ULOCK(d);
	}
	psc_hashbkt_put(&msl_namecache_hashtbl, b);
	if (new_dce) {
		PSCFREE(new_dce->dce_pfd);
		psc_pool_return(dircache_ent_pool, new_dce);
	}
	return (rc);
}

void
dircache_ent_dbgpr(struct dircache_page *p, struct dircache_ent *e,
    void *a)
{
	struct dircache_page **pp = a;

	if (*pp != p) {
		PFLOG_DIRCACHEPG(PLL_MAX, p, "");
		*pp = p;
	}

	fprintf(stderr, "   ent %.*s\n", e->dce_pfd->pfd_namelen,
	    e->dce_pfd->pfd_name);
}

void
dircache_dbgpr(struct fidc_membh *d)
{
	void *p = NULL;

	printf("pages %d\n", pll_nitems(&fcmh_2_fci(d)->fci_dc_pages));
	//dircache_walk(d, dircache_ent_dbgpr, &p);
}
