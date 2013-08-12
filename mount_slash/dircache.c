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
 * The dircache interface caches READDIR bufs after reception from the
 * MDS via RPC for the consequent LOOKUPs on each item by pscfs that
 * follow.
 */

#include <sys/types.h>
#include <sys/time.h>

#include <stdint.h>
#include <string.h>

#include "pfl/dynarray.h"
#include "pfl/fs.h"
#include "pfl/listcache.h"
#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/lock.h"
#include "psc_util/pool.h"

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
void
dircache_free_page(struct fidc_membh *d, struct dircache_page *p)
{
	struct fcmh_cli_info *fci;

	FCMH_LOCK_ENSURE(d);
	fci = fcmh_2_fci(d);

	if (psclist_conjoint(&p->dcp_lentry,
	    psc_lentry_hd(&fci->fci_dc_pages.pll_listhd)))
		pll_remove(&fci->fci_dc_pages, p);

	fci->fci_dc_nents -= psc_dynarray_len(&p->dcp_dents);
	psc_dynarray_free(&p->dcp_dents);
	PSCFREE(p->dcp_base);
	PSCFREE(p->dcp_base0);
	psc_pool_return(dircache_pool, p);

	fcmh_wake_locked(d);

	/* XXX move this to generic pool stats */
	OPSTAT_INCR(SLC_OPST_DIRCACHE_REL_ENTRY);
}

/**
 * dircache_walk: Perform a batch operation across all cached dirents.
 * @d: directory handle.
 * @cbf: callback to run.
 * @cbarg: callback argument.
 */
void
dircache_walk(struct fidc_membh *d, void (*cbf)(struct dircache_page *p,
    struct dircache_ent *, void *), void *cbarg)
{
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	struct pfl_timespec expire;
	int lk, n;

	PFL_GETPTIMESPEC(&expire);
	expire.tv_sec -= DIRENT_TIMEO;

	fci = fcmh_2_fci(d);
	lk = FCMH_RLOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DCPF_LOADING)
			continue;

		DYNARRAY_FOREACH(dce, n, &p->dcp_dents)
			cbf(p, dce, cbarg);
	}
	FCMH_URLOCK(d, lk);
}

/**
 * dircache_lookup: Perform a search across cached pages for a base
 *	name.
 * @d: directory handle.
 * @name: name to lookup.
 */
slfid_t
dircache_lookup(struct fidc_membh *d, const char *name)
{
	struct dircache_ent dcent, *dce;
	struct dircache_page *p, *np;
	struct pscfs_dirent *dirent;
	struct pfl_timespec expire;
	struct fcmh_cli_info *fci;
	slfid_t ino = FID_ANY;
	int found, pos;

	FCMH_LOCK_ENSURE(d);

	PFL_GETPTIMESPEC(&expire);
	expire.tv_sec -= DIRENT_TIMEO;

	dcent.dce_namelen = strlen(name);
	dcent.dce_hash = psc_strn_hashify(name, dcent.dce_namelen);
	dcent.dce_name = name;

	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DCPF_LOADING)
			continue;

		if (DIRCACHE_PAGE_EXPIRED(d, p, &expire)) {
			dircache_free_page(d, p);
			continue;
		}

		/*
		 * The return code for psc_dynarray_bsearch() tells us
		 * the position where our name should be to keep the
		 * list sorted.  If it is one after the last item, then
		 * we know for sure the item is not there.  Otherwise,
		 * we still need one more comparison to be sure.
		 */
		pos = psc_dynarray_bsearch(&p->dcp_dents,
		    &dcent, dirent_cmp);
		if (pos >= psc_dynarray_len(&p->dcp_dents))
			continue;

		/*
		 * Find the first entry with an equivalent hash then
		 * loop through all.
		 */
		for (; pos > 0; pos--) {
			dce = psc_dynarray_getpos(&p->dcp_dents, pos - 1);
			if (dce->dce_hash != dcent.dce_hash)
				break;
		}
		found = 0;
		DYNARRAY_FOREACH_CONT(dce, pos, &p->dcp_dents) {
			if (dce->dce_hash != dcent.dce_hash)
				break;

			dirent = PSC_AGP(p->dcp_base, dce->dce_len);

			if (dce->dce_hash == dcent.dce_hash &&
			    dce->dce_namelen == dcent.dce_namelen &&
			    strncmp(dce->dce_name, dcent.dce_name,
			    dcent.dce_namelen) == 0) {
				ino = dirent->pfd_ino;
				found = 1;
				OPSTAT_INCR(SLC_OPST_DIRCACHE_LOOKUP_HIT);
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

/**
 * dircache_new_page: Allocate a new page of dirents.
 * @d: directory handle.
 * @off: offset into directory for this slew of dirents.
 */
struct dircache_page *
dircache_new_page(struct fidc_membh *d, off_t off, int ra)
{
	struct dircache_page *p, *np, *newp;
	struct fcmh_cli_info *fci;
	struct pfl_timespec expire;

	newp = psc_pool_get(dircache_pool);

	FCMH_LOCK(d);
	PFL_GETPTIMESPEC(&expire);
	expire.tv_sec -= DIRENT_TIMEO;
	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DCPF_LOADING) {
			FCMH_ULOCK(d);
			OPSTAT_INCR(SLC_OPST_DIRCACHE_RACE);
			psc_pool_return(dircache_pool, newp);
			return (NULL);
		}
		if (DIRCACHE_PAGE_EXPIRED(d, p, &expire) ||
		    (off >= p->dcp_off &&
		     off < p->dcp_nextoff))
			dircache_free_page(d, p);
	}

	memset(newp, 0, sizeof(*newp));
	psc_dynarray_init(&newp->dcp_dents);
	INIT_PSC_LISTENTRY(&newp->dcp_lentry);
	newp->dcp_flags = DCPF_LOADING;
	newp->dcp_off = off;
	if (ra)
		newp->dcp_flags |= DCPF_READAHEAD;
	pll_add_sorted(&fci->fci_dc_pages, newp, dircache_cmp);
	FCMH_ULOCK(d);
	return (newp);
}

/**
 * dircache_reg_ents: Register directory entries with our cache.
 * @d: directory handle.
 * @p: buffer of dirent objects.
 * @nents: number of dirent objects in @p.
 */
void
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    size_t nents, void *base)
{
	struct pscfs_dirent *dirent = NULL;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	unsigned char *b;
	off_t off;
	int i;

	OPSTAT_INCR(SLC_OPST_DIRCACHE_REG_ENTRY);

	psc_assert(psc_dynarray_len(&p->dcp_dents) == 0);

	PFL_GETPTIMESPEC(&p->dcp_tm);

	dce = p->dcp_base0 = PSCALLOC(nents * sizeof(*dce));
	psc_dynarray_ensurelen(&p->dcp_dents, nents);

	for (i = 0, b = base, off = 0; i < (int)nents; i++, dce++) {
		dirent = PSC_AGP(b, off);

		psclog_debug("fid="SLPRI_FID" off=%"PRId64" "
		    "nlen=%u type=%#o "
		    "name=%.*s dirent=%p off=%"PRId64,
		    dirent->pfd_ino, dirent->pfd_off,
		    dirent->pfd_namelen, dirent->pfd_type,
		    dirent->pfd_namelen, dirent->pfd_name, dirent, off);

		dce->dce_len = off;
		dce->dce_namelen = dirent->pfd_namelen;
		dce->dce_name = dirent->pfd_name;
		dce->dce_hash = psc_strn_hashify(dirent->pfd_name,
		    dirent->pfd_namelen);

		psc_dynarray_add(&p->dcp_dents, dce);
		off += PFL_DIRENT_SIZE(dirent->pfd_namelen);
	}

	psc_dynarray_sort(&p->dcp_dents, qsort, dirent_sort_cmp);
	DYNARRAY_FOREACH(dce, i, &p->dcp_dents)
		psclog_debug("dce=%p hash=%#x namelen=%u name=%.*s",
		    dce, dce->dce_hash, dce->dce_namelen,
		    dce->dce_namelen, dce->dce_name);

	fci = fcmh_2_fci(d);
	FCMH_LOCK(d);
	if (dirent)
		p->dcp_nextoff = dirent->pfd_off;
	else
		p->dcp_nextoff = p->dcp_off;
	p->dcp_base = base;
	p->dcp_flags &= ~DCPF_LOADING;
	fci->fci_dc_nents += nents;
	fcmh_wake_locked(d);
	FCMH_ULOCK(d);
}

void
dircache_ent_dprintf(struct dircache_page *p, struct dircache_ent *e,
    void *a)
{
	struct dircache_page **pp = a;

	if (*pp != p) {
		DPRINTF_DIRCACHEPG(PLL_MAX, p, "");
		*pp = p;
	}

	printf("   ent %.*s\n", e->dce_namelen, e->dce_name);
}

void
dircache_dprintf(struct fidc_membh *d)
{
	void *p = NULL;

	dircache_walk(d, dircache_ent_dprintf, &p);
}
