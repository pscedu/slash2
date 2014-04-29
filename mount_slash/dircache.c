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
 * The dircache interface caches READDIR bufs after reception from the
 * MDS via RPC for the consequent LOOKUPs on each item by pscfs that
 * follow.
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
#include "pfl/pool.h"
#include "pfl/str.h"

#include "dircache.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "sltypes.h"
#include "slutil.h"

struct psc_poolmaster	 dircache_poolmaster;
struct psc_poolmgr	*dircache_pool;

void
dircache_mgr_init(void)
{
	/* XXX have a reclaim cb to force release some pages */
#define DCP_DEF 64
	psc_poolmaster_init(&dircache_poolmaster, struct dircache_page,
	    dcp_lentry, PPMF_AUTO, DCP_DEF, DCP_DEF, 0, NULL, NULL,
	    NULL, "dircache");
	dircache_pool = psc_poolmaster_getmgr(&dircache_poolmaster);
}

/**
 * dircache_free_page: Release a page of dirents from cache.
 * @d: directory handle.
 * @p: page to release.
 */
int
_dircache_free_page(const struct pfl_callerinfo *pci,
    struct fidc_membh *d, struct dircache_page *p, int wait)
{
	struct fcmh_cli_info *fci;

	FCMH_LOCK_ENSURE(d);
	fci = fcmh_2_fci(d);

//	psc_assert((p->dcp_flags & DIRCACHEPGF_LOADING) == 0);

	if (p->dcp_flags & DIRCACHEPGF_FREEING)
		return (0);

	if (p->dcp_refcnt && !wait)
		return (0);

	p->dcp_flags |= DIRCACHEPGF_FREEING;

	if ((p->dcp_flags & DIRCACHEPGF_READ) == 0)
		OPSTAT_INCR(SLC_OPST_DIRCACHE_UNUSED);

	while (p->dcp_refcnt)
		fcmh_wait_nocond_locked(d);

	if (psclist_conjoint(&p->dcp_lentry,
	    psc_lentry_hd(&fci->fci_dc_pages.pll_listhd)))
		pll_remove(&fci->fci_dc_pages, p);

	if (p->dcp_dents_name) {
		psc_dynarray_free(p->dcp_dents_name);
		psc_dynarray_free(p->dcp_dents_off);
	}
	PSCFREE(p->dcp_dents_name);
	PSCFREE(p->dcp_dents_off);
	PSCFREE(p->dcp_base);
	PSCFREE(p->dcp_base0);
	DBGPR_DIRCACHEPG(PLL_DEBUG, p, "free dir=%p", d);
	psc_pool_return(dircache_pool, p);

	fcmh_wake_locked(d);

	return (1);
}

/**
 * dircache_walk: Perform a batch operation across all cached dirents.
 * @d: directory handle.
 * @cbf: callback to run.
 * @cbarg: callback argument.
 */
void
dircache_walk(struct fidc_membh *d, void (*cbf)(struct dircache_page *,
    struct dircache_ent *, void *), void *cbarg)
{
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	int lk, n;

	fci = fcmh_2_fci(d);
	lk = FCMH_RLOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		DYNARRAY_FOREACH(dce, n, p->dcp_dents_name)
			cbf(p, dce, cbarg);
	FCMH_URLOCK(d, lk);
}

/**
 * dircache_lookup: Perform a search across cached pages for a file base
 *	name.  Entries are sorted by their hash(basename), with
 *	identical hash values side-by-side in the dynarray.
 *	We use a bsearch to find an entry with our hash, move backwards
 *	until we find the first occurance of the hash, then scan all
 *	matches hashes performing a full strcmp for our key and return
 *	failure when the iterating item's hash changes.
 * @d: directory handle.
 * @name: name to lookup.
 */
slfid_t
dircache_lookup(struct fidc_membh *d, const char *name, off_t *nextoffp, int invalidate)
{
	struct dircache_page *p, *np;
	struct dircache_ent q, *dce;
	struct dircache_expire dexp;
	struct pscfs_dirent *dirent;
	struct fcmh_cli_info *fci;
	slfid_t ino = FID_ANY;
	int found, pos;

	FCMH_LOCK_ENSURE(d);

	DIRCACHEPG_INITEXP(&dexp);

	q.dce_namelen = strlen(name);
	q.dce_hash = psc_strn_hashify(name, q.dce_namelen);
	q.dce_name = name;

	if (nextoffp)
		*nextoffp = 0;

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DIRCACHEPGF_LOADING)
			continue;

		if (DIRCACHEPG_EXPIRED(d, p, &dexp)) {
			dircache_free_page(d, p);
			continue;
		}

		if (p->dcp_rc)
			continue;

		if (nextoffp)
			*nextoffp = p->dcp_nextoff;

		/*
		 * The return code for psc_dynarray_bsearch() tells us
		 * the position where our name should be to keep the
		 * list sorted.  If it is one after the last item, then
		 * we know for sure the item is not there.  Otherwise,
		 * we still need one more comparison to be sure.
		 */
		pos = psc_dynarray_bsearch(p->dcp_dents_name, &q,
		    dce_cmp_name);
		if (pos >= psc_dynarray_len(p->dcp_dents_name))
			continue;

		/*
		 * Find the first entry with an equivalent hash then
		 * loop through all.
		 */
		for (; pos > 0; pos--) {
			dce = psc_dynarray_getpos(p->dcp_dents_name,
			    pos - 1);
			if (dce->dce_hash != q.dce_hash)
				break;
		}
		found = 0;
		DYNARRAY_FOREACH_CONT(dce, pos, p->dcp_dents_name) {
			if (dce->dce_hash != q.dce_hash)
				break;

			dirent = PSC_AGP(p->dcp_base, dce->dce_len);

			if (dce->dce_hash == q.dce_hash &&
			    dce->dce_namelen == q.dce_namelen &&
			    strncmp(dce->dce_name, q.dce_name,
			    q.dce_namelen) == 0) {
				ino = dirent->pfd_ino;
				found = 1;
				OPSTAT_INCR(SLC_OPST_DIRCACHE_LOOKUP_HIT);
					
				if (invalidate) {
				    found = 0;
				    dce->dce_hash++;
				    OPSTAT_INCR(SLC_OPST_DIRCACHE_LOOKUP_DEL);
				}
			}

			psclog_debug("fid="SLPRI_FID" off=%"PRId64" "
			    "nlen=%u type=%#o dname=%.*s lookupname=%s "
			    "len=%"PSCPRIdOFFT" dce=%p found=%d",
			    dirent->pfd_ino, dirent->pfd_off,
			    dirent->pfd_namelen, dirent->pfd_type,
			    dirent->pfd_namelen, dirent->pfd_name, name,
			    dce->dce_len, dce, found);

			if (found)
				break;
		}
		if (found)
			break;
	}

	return (ino);
}

/**
 * dircache_purge: Destroy all dirent pages belonging to a directory.
 * @d: directory handle.
 */
void
dircache_purge(struct fidc_membh *d)
{
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;

	fci = fcmh_2_fci(d);
	FCMH_LOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		dircache_free_page(d, p);
	FCMH_ULOCK(d);
}

int
dircache_cmp(const void *a, const void *b)
{
	const struct dircache_page *pa = a, *pb = b;

	return (CMP(pa->dcp_off, pb->dcp_off));
}

int
dircache_hasoff(struct dircache_page *p, off_t off)
{
	struct dircache_ent q, *dce;
	int n;

	if (p->dcp_rc)
		return (0);

	if (off == p->dcp_off)
		return (1);
	if (off == p->dcp_nextoff)
		return (0);

	q.dce_off = off;
	n = psc_dynarray_bsearch(p->dcp_dents_off, &q, dce_cmp_off);
	if (n >= psc_dynarray_len(p->dcp_dents_off))
		return (0);
	dce = psc_dynarray_getpos(p->dcp_dents_name, n);
	return (dce->dce_off == off);
}

/**
 * Allocate a new page of dirents.
 * @d: directory handle.
 * @off: offset into directory for this slew of dirents.
 */
struct dircache_page *
dircache_new_page(struct fidc_membh *d, off_t off, int wait)
{
	struct dircache_page *p, *np, *newp;
	struct dircache_expire dexp;
	struct fcmh_cli_info *fci;

	newp = psc_pool_get(dircache_pool);

	FCMH_LOCK(d);

 restart:
	DIRCACHEPG_INITEXP(&dexp);

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DIRCACHEPGF_LOADING) {
			if (p->dcp_off == off) {
				/* Page is waiting for us; use it. */
				p->dcp_flags &= ~DIRCACHEPGF_LOADING;
				break;
			}

			if (wait) {
				fcmh_wait_nocond_locked(d);
				goto restart;
			}
			continue;
		}
		if (DIRCACHEPG_EXPIRED(d, p, &dexp)) {
			dircache_free_page(d, p);
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
	DBGPR_DIRCACHEPG(PLL_DEBUG, p, "incref");

 out:
	FCMH_ULOCK(d);

	if (newp)
		psc_pool_return(dircache_pool, newp);

	return (p);
}

/**
 * dircache_reg_ents: Register directory entries with our cache.
 * @d: directory handle.
 * @p: buffer of dirent objects.
 * @nents: number of dirent objects in @p.
 * @base: pointer to buffer of pscfs_dirents from RPC.
 */
void
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    size_t nents, void *base, size_t size, int eof)
{
	struct psc_dynarray *da_name, *da_off;
	struct pscfs_dirent *dirent = NULL;
	struct dircache_ent *dce;
	void *base0;
	off_t adj;
	int i;

	OPSTAT_INCR(SLC_OPST_DIRCACHE_REG_ENTRY);

	dce = base0 = PSCALLOC(nents * sizeof(*dce));

	da_name = PSCALLOC(sizeof(*da_name));
	da_off = PSCALLOC(sizeof(*da_off));
	psc_dynarray_init(da_name);
	psc_dynarray_init(da_off);

	psc_dynarray_ensurelen(da_name, nents);
	psc_dynarray_ensurelen(da_off, nents);

	for (i = 0, adj = 0; i < (int)nents; i++, dce++) {
		dirent = PSC_AGP(base, adj);

		psclog_debug("fid="SLPRI_FID" d_off=%"PRId64" "
		    "nlen=%u type=%#o "
		    "name=%.*s dirent=%p adj=%"PRId64,
		    dirent->pfd_ino, dirent->pfd_off,
		    dirent->pfd_namelen, dirent->pfd_type,
		    dirent->pfd_namelen, dirent->pfd_name, dirent, adj);

		dce->dce_len = adj;
		dce->dce_namelen = dirent->pfd_namelen;
		dce->dce_name = dirent->pfd_name;
		dce->dce_off = dirent->pfd_off;
		dce->dce_hash = psc_strn_hashify(dirent->pfd_name,
		    dirent->pfd_namelen);

		/* XXX ensure this off doesnt show up in another page? */

		psc_dynarray_add(da_name, dce);
		psc_dynarray_add(da_off, dce);

		adj += PFL_DIRENT_SIZE(dirent->pfd_namelen);
	}

	psc_dynarray_sort(da_name, qsort, dce_sort_cmp_name);
	psc_dynarray_sort(da_off, qsort, dce_sort_cmp_off);

	FCMH_LOCK(d);

	if ((p->dcp_flags & DIRCACHEPGF_LOADED) ||
	    (p->dcp_flags & DIRCACHEPGF_LOADING) == 0) {
		psc_dynarray_free(da_name);
		psc_dynarray_free(da_off);
		PSCFREE(da_name);
		PSCFREE(da_off);
		PSCFREE(base0);
		PSCFREE(base);
		p->dcp_refcnt--;
		DBGPR_DIRCACHEPG(PLL_DEBUG, p, "already loaded");
		FCMH_ULOCK(d);
		return;
	}

	psc_assert(p->dcp_dents_name == NULL);

	if (dirent)
		p->dcp_nextoff = dirent->pfd_off;
	else
		p->dcp_nextoff = p->dcp_off;
	p->dcp_dents_name = da_name;
	p->dcp_dents_off = da_off;
	p->dcp_base0 = base0;
	p->dcp_base = base;
	p->dcp_size = size;
	PFL_GETPTIMESPEC(&p->dcp_tm);
	psc_assert(p->dcp_flags & DIRCACHEPGF_LOADING);
	p->dcp_flags &= ~DIRCACHEPGF_LOADING;
	p->dcp_flags |= DIRCACHEPGF_LOADED;
	if (eof)
		p->dcp_flags |= DIRCACHEPGF_EOF;
	p->dcp_refcnt--;
	DBGPR_DIRCACHEPG(PLL_DEBUG, p, "decref");
	fcmh_wake_locked(d);
	FCMH_ULOCK(d);
}

void
dircache_ent_dbgpr(struct dircache_page *p, struct dircache_ent *e,
    void *a)
{
	struct dircache_page **pp = a;

	if (*pp != p) {
		DBGPR_DIRCACHEPG(PLL_MAX, p, "");
		*pp = p;
	}

	fprintf(stderr, "   ent %.*s\n", e->dce_namelen, e->dce_name);
}

void
dircache_dbgpr(struct fidc_membh *d)
{
	void *p = NULL;

	printf("pages %d\n", pll_nitems(&fcmh_2_fci(d)->fci_dc_pages));
	dircache_walk(d, dircache_ent_dbgpr, &p);
}
