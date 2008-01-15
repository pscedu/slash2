#ifndef MDSEXPC_H
#define MDSEXPC_H 1

/* These structures provide back pointers into the Fid Cache to facilitate client-wise operations within the cache.  Such ops would include dereferencing the entire tree of client bmap references on connection close.
 */

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_ds/list.h"
#include "fidcache.h"

struct pscrpc_export;

/*
 * mexpbcm (mds_export_bmap_cache_member) -
 *
 */
struct mexpbcm {
        u32                     mexpbcm_flags;
        struct bmap_cache_memb *mexpbcm_bmap;
	struct pscrpc_export   *mexpbcm_export;
        SPLAY_ENTRY(mexpbcm)    mexpbcm_tentry;
};

SPLAY_HEAD(exp_bmaptree, mexpbcm);
SPLAY_PROTOTYPE(exp_bmaptree, mexpbcm, mexpbcm_tentry, bmap_cache_cmp);
/*
 * mexpfcm (mds_export_fidcache_memb) - 
 *
 */
struct mexpfcm {
        fcache_memb_t        *mecm_fcm;        /* point to the fcm*/
        psc_spinlock_t        mecm_lock;
        struct exp_bmaptree   mecm_bmaps;      /* my tree of bmap pointers */
        struct pscrpc_export *mecm_export;     /* backpointer to our export */
        SPLAY_ENTRY(mexpfcm)  mecm_exp_tentry; /* export tree entry */
        SPLAY_ENTRY(mexpfcm)  mecm_fcm_tentry; /* fcm tree entry    */
};

SPLAY_HEAD(exp_fidtree, mexpfcm);
SPLAY_PROTOTYPE(exp_fidtree, exp_fid_memb, mecm_exp_tentry, bmap_cache_cmp);
/*  
 * mexpfc (mds_export_fidcache) - stored in the export's private data, bmec_set is an array of bmap_cache_memb pointers.  This structure is traversed if the export is closed or if the file object is truncated.  
*/
struct mexpfc {
        struct pscrpc_export *mec_export; /* backpointer to our export */
        struct exp_fidtree    mec_fids;   /* reference all fcm's via the
                                           * export */
        struct psclist_head   mec_lentry; /* sched export for revocations */
        psc_spinlock_t        mec_lock;	
};


#endif
