/* $Id$ */

#include "psc_types.h"

#include "psc_rpc/rpc.h"
#include "psc_ds/listcache.h"
#include "psc_util/lock.h"

#include "slvr.h"
#include "iod_bmap.h"
#include "offtree.h"

struct list_cache dirtySlvrs;

__static SPLAY_GENERATE(iobd_slvrtree, slvr_ref, slvr_tentry, slvr_cmp);

/**
 * slvr_do_crc - given a sliver reference, take the crc of the respective
 *   data and attach the ref to an srm_bmap_crcup structure.
 * @s: the sliver reference.
 * Notes:  Don't hold the lock while taking the crc.
 */
__static void 
slvr_do_crc(struct slvr_ref *s)
{
	struct offtree_req *r = PSCALLOC(sizeof(*r));
	
	psc_assert(s->slvr_flags == SLVR_CRCING);
	psc_assert(!s->slvr_updates);
	psc_assert(psclist_disjoint(&s->slvr_lentry));
	
	sliod_oftrq_build(r, SLVR_2_BMAP(s), SLVR_2_BLK(s), 
		  (SLASH_SLVR_SIZE/SLASH_BMAP_BLKSZ), OFTREQ_OP_READ);
	
	//XXX take CRC here.

	PSCFREE(r);
}

__static void
slvr_release(struct slvr_ref *s)
{
        struct iodbmap_data *iobd = s->slvr_pri;

        psc_assert(iobd);
	/* Lock the iobd which protects the sliver and iobd
	 *   tree.  If no one has updated the sliver during the 
	 *   crc and rpc procedures then free it.
	 */
        spinlock(&iobd->iobd_lock);
        if (s->slvr_updates) {
                freelock(&iobd->iobd_lock);
                return;
        }

        if (SPLAY_REMOVE(iobd_slvrtree, &iobd->iobd_slvrs, s))
                PSCFREE(s);
        else
                psc_assert("Could not locate sliver for removal");

        freelock(&iobd->iobd_lock);
}


struct slvr_ref *
slvr_lookup(uint16_t num, struct bmap_iod_info *b, int add)
{
        struct slvr_ref *s, ts;

        psc_assert(b->biod_bmap);
        ts.slvr_num = num;

        spinlock(&b->biod_lock);

        s = SPLAY_FIND(biod_slvrtree, &b->biod_slvrs, &ts);
        if (!s && add) {
                s = PSCALLOC(sizeof(*s));
                slvr_init(s);
                SPLAY_INSERT(biod_slvrtree, &b->biod_slvrs, s);
        }
        freelock(&b->biod_lock);

        return (s);
}


void
slvr_update(struct slvr_ref *s)
{
	SLVR_LOCK(s);
	s->slvr_updates++;
	clock_gettime(&s->slvr_ts);
	SLVR_ULOCK(s);
	
	if (s->slvr_flags & SLVR_NEW)
		lc_queue(&dirtySlvrs, &s->slvr_lentry);
	else 
		lc_requeue(&dirtySlvrs, &s->slvr_lentry);
}

void
slvr_cache_init(void)
{
	lc_reginit(&dirtySlvrs,  struct slvr_ref, slvr_lentry, "dirtySlvrs");
}
