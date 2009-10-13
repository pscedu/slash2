/* $Id$ */

#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "fidcache.h"
#include "inode.h"

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

struct psc_poolmaster	 bmap_poolmaster;
struct psc_poolmgr	*bmap_pool;

/**
 * bmapc_cmp - bmap_cache splay tree comparator
 * @a: a bmapc_memb
 * @b: another bmapc_memb
 */
int
bmapc_cmp(const void *x, const void *y)
{
	const struct bmapc_memb *a = x, *b = y;

	if (a->bcm_blkno > b->bcm_blkno)
		return (1);
	else if (a->bcm_blkno < b->bcm_blkno)
		return (-1);
	return (0);
}

struct bmapc_memb *
bmap_lookup_locked(struct fidc_open_obj *fcoo, sl_blkno_t n)
{
	struct bmapc_memb lb, *b;

	lb.bcm_blkno=n;
	b = SPLAY_FIND(bmap_cache, &fcoo->fcoo_bmapc, &lb);
	if (b)
		atomic_inc(&b->bcm_opcnt);

	return (b);
}

struct bmapc_memb *
bmap_lookup(struct fidc_membh *f, sl_blkno_t n)
{
	struct bmapc_memb *b;
	int locked;

	locked = reqlock(&f->fcmh_lock);
	b = bmap_lookup_locked(f->fcmh_fcoo, n);
	ureqlock(&f->fcmh_lock, locked);
	return (b);
}

struct bmapc_memb *
bmap_lookup_add(struct fidc_membh *f, sl_blkno_t n,
    void (*bmap_init_fn)(struct bmapc_memb *))
{
	int locked;
	struct bmapc_memb *b;

	psc_assert(f->fcmh_fcoo);

	locked = reqlock(&f->fcmh_lock);

	b = bmap_lookup_locked(f->fcmh_fcoo, n);
	if (!b) {
		b = psc_pool_get(bmap_pool);
		memset(b, 0, bmap_pool->ppm_master->pms_entsize);
		LOCK_INIT(&b->bcm_lock);
		atomic_set(&b->bcm_opcnt, 0);
		psc_waitq_init(&b->bcm_waitq);
		b->bcm_fcmh = f;
		b->bcm_blkno = n;
		b->bcm_pri = b + sizeof(*b);

		/* Note that the bmap is newly initialized and therefore
		 *  may not contain certain structures.
		 */
		b->bcm_mode = BMAP_INIT;
		/* Call the provided init function.
		 */
		bmap_init_fn(b);
		/* Add to the fcmh's bmap cache but first up the opcnt.
		 */
		atomic_inc(&b->bcm_opcnt);
		SPLAY_INSERT(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, b);
	}
	ureqlock(&f->fcmh_lock, locked);
	return (b);
}
