/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include "pfl/cdefs.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "fidcache.h"
#include "inode.h"

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmap_cmp);

struct psc_poolmaster	 bmap_poolmaster;
struct psc_poolmgr	*bmap_pool;

/**
 * bmap_cmp - comparator for bmapc_membs in the splay cache.
 * @a: a bmapc_memb
 * @b: another bmapc_memb
 */
int
bmap_cmp(const void *x, const void *y)
{
	const struct bmapc_memb *a = x, *b = y;

	return (CMP(a->bcm_blkno, b->bcm_blkno));
}

void
bmap_remove(struct bmapc_memb *b)
{
	struct fidc_membh *f=b->bcm_fcmh;
	int locked;

	BMAP_RLOCK(b);

	DEBUG_BMAP(PLL_INFO, b, "removing");

	psc_assert(b->bcm_mode & BMAP_CLOSING);
	psc_assert(!(b->bcm_mode & BMAP_DIRTY));
	psc_assert(!atomic_read(&b->bcm_wr_ref) &&
		   !atomic_read(&b->bcm_rd_ref));
	psc_assert(!atomic_read(&b->bcm_opcnt));

	BMAP_ULOCK(b);

	locked = FCMH_RLOCK(f);
	PSC_SPLAY_XREMOVE(bmap_cache, &f->fcmh_bmaptree, b);
	psc_waitq_wakeall(&f->fcmh_waitq);
	FCMH_URLOCK(f, locked);

	psc_pool_return(bmap_pool, b);
}

void
_bmap_op_done(struct bmapc_memb *b)
{
	BMAP_RLOCK(b);

	atomic_dec(&b->bcm_opcnt);

	DEBUG_BMAP(PLL_INFO, b, "bmap_op_done");

	psc_assert(atomic_read(&b->bcm_opcnt) >= 0);
	psc_assert(atomic_read(&b->bcm_wr_ref) >= 0);
	psc_assert(atomic_read(&b->bcm_rd_ref) >= 0);

	if (!atomic_read(&b->bcm_rd_ref) &&
	    !atomic_read(&b->bcm_wr_ref) &&
	    !atomic_read(&b->bcm_opcnt)) {
		b->bcm_mode |= BMAP_CLOSING;
		/* XXX remove from fcmh tree? */
		BMAP_ULOCK(b);

		if (bmap_final_cleanupf)
			bmap_final_cleanupf(b);

		bmap_remove(b);
		return;
	}
	psc_waitq_wakeall(&b->bcm_waitq);
	BMAP_ULOCK(b);
}

__static struct bmapc_memb *
bmap_lookup_cache(struct fidc_membh *f, sl_blkno_t n)
{
	struct bmapc_memb lb, *b;

 restart:
	lb.bcm_blkno = n;
	b = SPLAY_FIND(bmap_cache, &f->fcmh_bmaptree, &lb);
	if (b) {
		BMAP_LOCK(b);
		if (b->bcm_mode & BMAP_CLOSING) {
			/*
			 * This bmap is going away; wait for
			 * it so we can reload it back.
			 */
			BMAP_ULOCK(b);
			psc_waitq_wait(&f->fcmh_waitq, &f->fcmh_lock);
			FCMH_LOCK(f);
			goto restart;
		}
		bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);
	}
	return (b);
}

/**
 * bmap_get - Get the specified bmap.
 * @f: fcmh.
 * @n: bmap number.
 * @rw: bmap access mode.
 * @flags: retrieval parameters.
 * @arg: optional retrieval parameter.
 * Notes: returns the bmap referenced via bcm_opcnt.
 */
int
_bmap_get(struct fidc_membh *f, sl_blkno_t n, enum rw rw, int flags,
    struct bmapc_memb **bp, void *arg)
{
	int rc = 0, do_load = 0, locked;
	struct bmapc_memb *b;

	*bp = NULL;

	locked = reqlock(&f->fcmh_lock);
	b = bmap_lookup_cache(f, n);
	if (b == NULL) {
		if ((flags & BMAPGETF_LOAD) == 0) {
			ureqlock(&f->fcmh_lock, locked);
			return (ENOENT);
		}
		b = psc_pool_get(bmap_pool);
		memset(b, 0, bmap_pool->ppm_master->pms_entsize);
		LOCK_INIT(&b->bcm_lock);

		atomic_set(&b->bcm_opcnt, 0);
		atomic_set(&b->bcm_rd_ref, 0);
		atomic_set(&b->bcm_wr_ref, 0);
		psc_waitq_init(&b->bcm_waitq);
		b->bcm_fcmh = f;
		b->bcm_bmapno = n;
		b->bcm_pri = b + 1;

		/* Signify that the bmap is newly initialized and therefore
		 *  may not contain certain structures.
		 */
		b->bcm_mode = BMAP_INIT;

		bmap_op_start_type(b, BMAP_OPCNT_LOOKUP);
		/* Perform app-specific substructure initialization. */
		bmap_init_privatef(b);

		/* Add to the fcmh's bmap cache */
		SPLAY_INSERT(bmap_cache, &f->fcmh_bmaptree, b);
		do_load = 1;
	}
	ureqlock(&f->fcmh_lock, locked);
	if (do_load) {
		rc = bmap_retrievef(b, rw, arg);

		BMAP_LOCK(b);
		b->bcm_mode &= ~BMAP_INIT;
		psc_waitq_wakeall(&b->bcm_waitq);
		if (rc) {
			bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
			return (rc);
		}
	} else {
		while (b->bcm_mode & BMAP_INIT) {
			psc_waitq_wait(&b->bcm_waitq, &b->bcm_lock);
			BMAP_LOCK(b);
		}
	}
	if (b) {
		BMAP_ULOCK(b);
		*bp = b;
	}
	return (rc);
}

void
bmap_cache_init(size_t priv_size)
{
	_psc_poolmaster_init(&bmap_poolmaster, sizeof(struct bmapc_memb) +
	    priv_size, offsetof(struct bmapc_memb, bcm_lentry), PPMF_AUTO,
	    64, 64, 0, NULL, NULL, NULL, NULL, "bmap");
	bmap_pool = psc_poolmaster_getmgr(&bmap_poolmaster);
}

void
_debug_bmapod(struct bmapc_memb *bmap, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	DEBUG_BMAPODV(PLL_MAX, bmap, fmt, ap);
	va_end(ap);
}
