/* $Id$ */

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "fidcache.h"
#include "inode.h"

__static SPLAY_GENERATE(bmap_cache, bmapc_memb, bcm_tentry, bmapc_cmp);

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
        int locked = reqlock(&f->fcmh_lock);
        struct bmapc_memb *b = bmap_lookup_locked(f->fcmh_fcoo, n);

        ureqlock(&f->fcmh_lock, locked);
        return (b);
}

struct bmapc_memb *
bmap_lookup_add(struct fidc_membh *f, sl_blkno_t n, void (*bmap_init_fn)(struct bmapc_memb *, struct fidc_membh *, sl_blkno_t))
{	
        int locked;
	struct bmapc_memb *b;

	psc_assert(f->fcmh_fcoo);

	locked = reqlock(&f->fcmh_lock);

        b = bmap_lookup_locked(f->fcmh_fcoo, n);
        if (!b) {
                b = PSCALLOC(sizeof(struct bmapc_memb));
		/* Note that the bmap is newly initialized and therefore
		 *  may not contain certain structures.
		 */
		b->bcm_mode = BMAP_INIT;
		/* Call the provided init function.
		 */
                (bmap_init_fn)(b, f, n);
		/* Add to the fcmh's bmap cache.
		 */
		SPLAY_INSERT(bmap_cache, &f->fcmh_fcoo->fcoo_bmapc, b);
        }
        ureqlock(&f->fcmh_lock, locked);
        return (b);
}
