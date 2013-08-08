/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <errno.h>
#include <inttypes.h>
#include <time.h>

#include "pfl/cdefs.h"
#include "pfl/dynarray.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/rpc.h"
#include "pfl/vbitmap.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"

#include "buffer.h"
#include "cache_params.h"
#include "fidcache.h"
#include "sliod.h"
#include "slvr.h"

struct psc_poolmaster	 sl_bufs_poolmaster;
struct psc_poolmgr	*sl_bufs_pool;

void
sl_buffer_fresh_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
	psc_assert(b->slb_base);
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert((!atomic_read(&b->slb_inflight)) &&
		   (!atomic_read(&b->slb_inflpndg)));
}

void
sl_buffer_clear(struct sl_buffer *b, size_t size)
{
	memset(b->slb_base, 0, size);
}

int
sl_buffer_init(__unusedx struct psc_poolmgr *m, void *pri)
{
	struct sl_buffer *slb = pri;

	slb->slb_blksz = SLB_BLKSZ;
	slb->slb_nblks = SLB_NBLK;
	slb->slb_base  = PSCALLOC(SLB_NBLK * SLB_BLKSZ);
	atomic_set(&slb->slb_ref, 0);
	atomic_set(&slb->slb_inflight, 0);
	INIT_SPINLOCK(&slb->slb_lock);

	slb->slb_flags = SLB_FRESH;
	INIT_LISTENTRY(&slb->slb_mgmt_lentry);

	DEBUG_SLB(PLL_TRACE, slb, "new slb");
	return (0);
}

void
sl_buffer_destroy(void *pri)
{
	struct sl_buffer *slb = pri;

	PSCFREE(slb->slb_base);
}

void
slibreapthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		psc_mutex_lock(&sl_bufs_pool->ppm_reclaim_mutex);
		sl_bufs_pool->ppm_reclaimcb(sl_bufs_pool);
		psc_mutex_unlock(&sl_bufs_pool->ppm_reclaim_mutex);
		sleep(10);
	}
}

void
sl_buffer_cache_init(void)
{
	psc_assert(SLB_SIZE <= LNET_MTU);

	psc_poolmaster_init(&sl_bufs_poolmaster, struct sl_buffer,
	    slb_mgmt_lentry, PPMF_AUTO, SLB_NDEF, SLB_MIN, SLB_MAX,
	    sl_buffer_init, sl_buffer_destroy, slvr_buffer_reap, "slab",
	    NULL);
	sl_bufs_pool = psc_poolmaster_getmgr(&sl_bufs_poolmaster);

//	lc_reginit(&slBufsLru,  struct sl_buffer, slb_mgmt_lentry, "slabBufLru");
//	lc_reginit(&slBufsPin,  struct sl_buffer, slb_mgmt_lentry, "slabBufPin");

	pscthr_init(SLITHRT_BREAP, 0, slibreapthr_main, NULL, 0,
	    "slibreapthr");
}
