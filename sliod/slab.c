/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
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
#include "pfl/vbitmap.h"
#include "pfl/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"

#include "buffer.h"
#include "cache_params.h"
#include "fidcache.h"
#include "sliod.h"

struct psc_poolmaster	 sl_bufs_poolmaster;
struct psc_poolmgr	*sl_bufs_pool;

void
sl_buffer_fresh_assertions(struct sl_buffer *b)
{
	psc_assert(b->slb_flags == SLB_FRESH);
	psc_vbitmap_printbin1(b->slb_inuse);
	psc_assert(psc_vbitmap_nfree(b->slb_inuse) == b->slb_nblks);
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	psc_assert(!b->slb_lc_owner); /* Not on any cache mgmt lists */
	psc_assert(psc_listhd_empty(&b->slb_iov_list));
	psc_assert(b->slb_base);
	psc_assert(!atomic_read(&b->slb_ref));
	psc_assert(!atomic_read(&b->slb_unmapd_ref));
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

	slb->slb_inuse = psc_vbitmap_new(SLB_NBLK);
	slb->slb_blksz = SLB_BLKSZ;
	slb->slb_nblks = SLB_NBLK;
	slb->slb_base  = PSCALLOC(SLB_NBLK * SLB_BLKSZ);
	atomic_set(&slb->slb_ref, 0);
	atomic_set(&slb->slb_unmapd_ref, 0);
	atomic_set(&slb->slb_inflight, 0);
	INIT_SPINLOCK(&slb->slb_lock);
	//ATTR_SET  (slb->slb_flags, SLB_FREEING);
	slb->slb_flags = SLB_FRESH;
	INIT_LISTHEAD(&slb->slb_iov_list);
	INIT_LISTENTRY(&slb->slb_mgmt_lentry);
	INIT_LISTENTRY(&slb->slb_fcmh_lentry);

	DEBUG_SLB(PLL_TRACE, slb, "new slb");
	return (0);
}

void
sl_buffer_destroy(void *pri)
{
	struct sl_buffer *slb = pri;

//	psc_assert(psc_listhd_empty(&slb->slb_iov_list));
//	psc_assert(psclist_disjoint(&slb->slb_fcmh_lentry));

	PSCFREE(slb->slb_base);
	psc_vbitmap_free(slb->slb_inuse);
}

void
slibreapthr_main(__unusedx struct psc_thread *thr)
{
	while (pscthr_run()) {
		psc_mutex_lock(&sl_bufs_pool->ppm_reclaim_mutex);
		sl_bufs_pool->ppm_reclaimcb(sl_bufs_pool);
		psc_mutex_unlock(&sl_bufs_pool->ppm_reclaim_mutex);
		sleep(30);
	}
}

void
sl_buffer_cache_init(void)
{
	int slvr_buffer_reap(struct psc_poolmgr *);

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
