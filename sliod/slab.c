/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
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

#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/dynarray.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/pool.h"
#include "pfl/rpc.h"
#include "pfl/vbitmap.h"

#include "cache_params.h"
#include "fidcache.h"
#include "slab.h"
#include "sliod.h"
#include "slvr.h"

struct psc_poolmaster	 slab_poolmaster;
struct psc_poolmgr	*slab_pool;

struct slab *
slab_alloc(void)
{
	struct slab *slb;

	slb = psc_pool_get(slab_pool);
	slb->slb_base = PSCALLOC(SLASH_SLVR_SIZE);
	INIT_LISTENTRY(&slb->slb_mgmt_lentry);

	return (slb);
}

void
slab_free(struct slab *slb)
{
	PSCFREE(slb->slb_base);
	psc_pool_return(slab_pool, slb);
}

void
slibreapthr_main(struct psc_thread *thr)
{
	while (pscthr_run(thr)) {
		psc_pool_reap(slab_pool, 0);
		thr->pscthr_waitq = "sleep 10";
		sleep(10);
		thr->pscthr_waitq = NULL;
	}
}

void
slab_cache_init(void)
{
	size_t nbuf;

	psc_assert(SLASH_SLVR_SIZE <= LNET_MTU);

	if (slcfg_local->cfg_slab_cache_size < SLAB_MIN_CACHE)
		psc_fatalx("invalid slab_cache_size setting; "
		    "minimum allowed is %zu", SLAB_MIN_CACHE);

	nbuf = slcfg_local->cfg_slab_cache_size / SLASH_SLVR_SIZE;
	psc_poolmaster_init(&slab_poolmaster, struct slab,
	    slb_mgmt_lentry, PPMF_AUTO, nbuf, nbuf, nbuf,
	    slab_cache_reap, "slab", NULL);
	slab_pool = psc_poolmaster_getmgr(&slab_poolmaster);

	pscthr_init(SLITHRT_BREAP, slibreapthr_main, 0, "slibreapthr");

	psclogs_info(SLISS_INFO, "Slab cache size is %zd bytes or %zd bufs", 
	    slcfg_local->cfg_slab_cache_size, nbuf);
}
