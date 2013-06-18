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
	struct dircache_ent *dce;

	psc_assert(p->dcp_flags == 0);

	FCMH_LOCK_ENSURE(d);
	fci = fcmh_2_fci(d);
	pll_remove(&fci->fci_dc_pages, p);

	dce = psc_dynarray_getpos(&p->dcp_dents, 0);
	PSCFREE(dce);
	psc_dynarray_free(&p->dcp_dents);
	PSCFREE(p->dcp_base);
	psc_pool_return(dircache_pool, p);

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
dircache_walk(struct fidc_membh *d,
    void (*cbf)(struct dircache_ent *, void *), void *cbarg)
{
	struct dircache_page *p, *np;
	struct fcmh_cli_info *fci;
	struct dircache_ent *dce;
	struct pfl_timespec expire;
	int n;

	PFL_GETPTIMESPEC(&expire);
	expire.tv_sec -= DIRENT_TIMEO;

	fci = fcmh_2_fci(d);
	FCMH_LOCK(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages) {
		if (p->dcp_flags & DCPF_LOADING)
			continue;

		if (DIRCACHE_PAGE_EXPIRED(d, p, &expire)) {
			dircache_free_page(d, p);
			continue;
		}

		DYNARRAY_FOREACH(dce, n, &p->dcp_dents)
			cbf(dce, cbarg);
	}
	FCMH_ULOCK(d);
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

	psc_assert(FCMH_HAS_LOCK(d));

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

			/* Map the dirent from the dcent's offset. */
			dirent = PSC_AGP(p->dcp_base, dce->dce_offset);

			if (dce->dce_hash == dcent.dce_hash &&
			    dce->dce_namelen == dcent.dce_namelen &&
			    strncmp(dce->dce_name, dcent.dce_name,
			    dcent.dce_namelen) == 0) {
				if (!(dce->dce_flags & DCEF_LOOKUP))
					dce->dce_flags |= DCEF_LOOKUP;

				ino = dirent->pfd_ino;
				found = 1;
				OPSTAT_INCR(SLC_OPST_DIRCACHE_HIT);
			}

			psclog_dbg("fid="SLPRI_FID" off=%"PRId64" nlen=%u "
			   "type=%#o dname=%.*s lookupname=%s off=%d dce=%p "
			   "found=%d",
			   dirent->pfd_ino, dirent->pfd_off,
			   dirent->pfd_namelen, dirent->pfd_type,
			   dirent->pfd_namelen, dirent->pfd_name, name,
			   dce->dce_offset, dce, found);

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

/**
 * dircache_new_page: Allocate a new page of dirents.
 * @d: directory handle.
 * @size: size of buffer for holding dirents.
 * @off: offset into directory for this slew of dirents.
 * @base: buffer to write dirents into.
 */
struct dircache_page *
dircache_new_page(struct fidc_membh *d, size_t size, off_t off,
    void *base)
{
	struct dircache_page *p, *np;
	struct pfl_timespec expire;
	struct fcmh_cli_info *fci;

	psc_assert(FCMH_HAS_LOCK(d));

	p = psc_pool_get(dircache_pool);
	memset(p, 0, sizeof(*p));
	psc_dynarray_init(&p->dcp_dents);
	INIT_PSC_LISTENTRY(&p->dcp_lentry);
	p->dcp_flags = DCPF_LOADING;
	p->dcp_size = size;
	p->dcp_off = off;
	p->dcp_base = base;

	PFL_GETPTIMESPEC(&expire);
	expire.tv_sec -= DIRENT_TIMEO;

	/*
	 * Release expired pages and any pages that overlap with the new
	 * contents.
	 */
	fci = fcmh_2_fci(d);
	PLL_FOREACH_SAFE(p, np, &fci->fci_dc_pages)
		if (DIRCACHE_PAGE_EXPIRED(d, p, &expire) ||
		    (off < p->dcp_off + p->dcp_size &&
		     off + size > p->dcp_off)) {
			dircache_free_page(d, p);
			continue;
		}

	/*
	 * New entries are considered to be more accurate so place them
	 * at the beginning of the list.
	 */
	pll_addtail(&fci->fci_dc_pages, p);

	return (p);
}

/**
 * dircache_reg_ents: Register directory entries with our cache.
 * @d: directory handle.
 * @p: buffer of dirent objects.
 * @nents: number of dirent objects in @p.
 */
void
dircache_reg_ents(struct fidc_membh *d, struct dircache_page *p,
    size_t nents)
{
	struct pscfs_dirent *dirent;
	struct dircache_ent *dce;
	unsigned char *b;
	off_t off;
	int j;

	OPSTAT_INCR(SLC_OPST_DIRCACHE_REG_ENTRY);

	psc_assert(p->dcp_size);
	psc_assert(psc_dynarray_len(&p->dcp_dents) == 0);

	PFL_GETPTIMESPEC(&p->dcp_tm);

	dce = PSCALLOC(nents * sizeof(*dce));
	psc_dynarray_ensurelen(&p->dcp_dents, nents);

	for (j = 0, b = p->dcp_base, off = 0;
	    j < (int)nents; j++, dce++) {
		dirent = PSC_AGP(b, off);

		psclog_dbg("fid="SLPRI_FID" off=%"PRId64" "
		    "nlen=%u type=%#o "
		    "name=%.*s dirent=%p off=%"PRId64,
		    dirent->pfd_ino, dirent->pfd_off,
		    dirent->pfd_namelen, dirent->pfd_type,
		    dirent->pfd_namelen, dirent->pfd_name, dirent, off);

		dce->dce_offset = off;
		dce->dce_namelen = dirent->pfd_namelen;
		dce->dce_name = dirent->pfd_name;
		dce->dce_hash = psc_strn_hashify(dirent->pfd_name,
		    dirent->pfd_namelen);

		psc_dynarray_add(&p->dcp_dents, dce);
		off += PFL_DIRENT_SIZE(dirent->pfd_namelen);
	}

	psc_dynarray_sort(&p->dcp_dents, qsort, dirent_sort_cmp);
	DYNARRAY_FOREACH(dce, j, &p->dcp_dents)
		psclog_dbg("dce=%p hash=%#x namelen=%u name=%.*s",
		    dce, dce->dce_hash, dce->dce_namelen,
		    dce->dce_namelen, dce->dce_name);

	FCMH_LOCK(d);
	p->dcp_flags &= ~DCPF_LOADING;
	fcmh_wake_locked(d);
	FCMH_ULOCK(d);
}
