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


void
bmap_remove(struct bmapc_memb *b)
{
	struct fidc_membh *f=b->bcm_fcmh;
	int locked;

	BMAP_LOCK(b);

	DEBUG_BMAP(PLL_WARN, b, "removing");

	psc_assert(b->bcm_mode & BMAP_CLOSING);
	psc_assert(!(b->bcm_mode & BMAP_DIRTY));
        psc_assert(!atomic_read(&b->bcm_waitq.wq_nwaitors));
        psc_assert(!atomic_read(&b->bcm_wr_ref) &&
                   !atomic_read(&b->bcm_rd_ref));
        psc_assert(!atomic_read(&b->bcm_opcnt));
	BMAP_ULOCK(b);

	locked = FCMH_RLOCK(f);

	if (!SPLAY_REMOVE(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, b))
                DEBUG_BMAP(PLL_FATAL, b, "failed to locate bmap in fcoo");
	
	FCMH_URLOCK(f, locked);

	atomic_dec(&f->fcmh_fcoo->fcoo_bmapc_cnt);
	psc_assert(atomic_read(&f->fcmh_fcoo->fcoo_bmapc_cnt) >= 0);

        psc_pool_return(bmap_pool, b);
}


int
bmap_try_release_locked(struct bmapc_memb *b) {
	
	BMAP_LOCK_ENSURE(b);
	
	DEBUG_BMAP(PLL_WARN, b, " ");

	if (!atomic_read(&b->bcm_rd_ref) &&
            !atomic_read(&b->bcm_wr_ref) &&
            !atomic_read(&b->bcm_opcnt)) {
                b->bcm_mode |= BMAP_CLOSING;
                BMAP_ULOCK(b);

                bmap_remove(b);

		return (1);
	}

	return (0);
}


void
bmap_try_release(struct bmapc_memb *b) {
	BMAP_LOCK(b);
	bmap_try_release_locked(b);
	
	if (!bmap_try_release_locked(b))
		/* It wasn't freed.
		 */
		BMAP_ULOCK(b);
}

void
bmap_op_done(struct bmapc_memb *b)
{
	atomic_dec(&b->bcm_opcnt);
	bmap_try_release(b);
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
	struct bmapc_memb *b;
	int locked;

	psc_assert(f->fcmh_fcoo);

	locked = reqlock(&f->fcmh_lock);
	//psc_assert(n < fcmh_2_nbmaps(f));

	b = bmap_lookup_locked(f->fcmh_fcoo, n);
	if (!b) {
		b = psc_pool_get(bmap_pool);
		memset(b, 0, bmap_pool->ppm_master->pms_entsize);
		LOCK_INIT(&b->bcm_lock);
		atomic_set(&b->bcm_opcnt, 0);
		atomic_set(&b->bcm_rd_ref, 0);
		atomic_set(&b->bcm_wr_ref, 0);
		psc_waitq_init(&b->bcm_waitq);
		b->bcm_fcmh = f;
		b->bcm_blkno = n;
		b->bcm_pri = b + 1;

		/* Note that the bmap is newly initialized and therefore
		 *  may not contain certain structures.
		 */
		b->bcm_mode = BMAP_INIT;
		/* Call the provided init function.
		 */
		bmap_init_fn(b);
		/* Add to the fcmh's bmap cache but first up the opcnt. 
		 *   If 'b' was found the opcnt was incremented within
		 *   bmap_lookup_locked().
		 */
		atomic_inc(&b->bcm_opcnt);
		atomic_inc(&f->fcmh_fcoo->fcoo_bmapc_cnt);
		SPLAY_INSERT(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, b);
	}
	ureqlock(&f->fcmh_lock, locked);
	return (b);
}
