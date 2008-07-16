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
 * mexpbcm (mds_export_bmap_cache_member) - mexpbcm references bmaps stored in the GFC (global fid cache) and acts as a bridge between GFC bmaps and the export(s) which reference them.  mexpbcm is tracked by the global fidcache (through the fcm's bmap export tree (at the bottom of the GFC's tree chain).  The bmexpcr struct (fidcache.h) points to it.
 * 
 * mexpbcm_lentry is used for scheduling revocation of the bmap via a listcache based queue.
 * 
 * mexpbcm is the lowest member of the exp fidcache chain and corresponds to the GFC's bmap tier.
 */
struct mexpbcm {
        u32                     mexpbcm_mode;
        struct bmap_cache_memb *mexpbcm_bmap;
	struct pscrpc_export   *mexpbcm_export;
	struct psclist_head     mexpbcm_lentry; /* ref for client cb */
        SPLAY_ENTRY(mexpbcm)    mexpbcm_exp_tentry;
        SPLAY_ENTRY(mexpbcm)    mexpbcm_bmap_tentry;
};

enum mexpbcm_modes {
	MEXPBCM_WR  = (1<<0), /* otherwise read */
        MEXPBCM_DIO = (1<<1)  /* otherwise caching mode */ 
};

static inline int
mexpbmap_cache_cmp(const void *x, const void *y)
{
        return (bmap_cache_cmp(((struct mexpbcm *)x)->mexpbcm_bmap,
                               ((struct mexpbcm *)y)->mexpbcm_bmap));
}

SPLAY_HEAD(exp_bmaptree, mexpbcm);
SPLAY_PROTOTYPE(exp_bmaptree, mexpbcm, mexpbcm_exp_tentry, mexpbmap_cache_cmp);
/*
 * mexpfcm (mds_export_fidcache_memb) - this structure interacts with the mds fid cache and the clients cache by mediation within the export.  It tracks which bmaps the client has cached, the operation (lamport) clocks of the fcm and the client.  mexpfcm is tracked by the export's fidcache and the fcm's bmap_lessees tree.
 * mecm_fcm_opcnt is used to detect changes in the fcm who keeps his update count in fcm_slfinfo.slf_opcnt.  At no time may mecm_fcm_opcnt be higher than the fcm (unless the fcm counter has flipped).  mecm_loc_opcnt and mecm_rem_opcnt serve a similar purpose but the update detection is between the client and the export.  So if client A updates the ctime or mode, exp(clientA) will inc mecm_fcm->fcm_slfinfo.slf_opcnt and desync the opcnt between the fcm and the other exports prompting the other exports to update their client's caches.  It may be the case that incrementing  mecm_fcm->fcm_slfinfo.slf_opcnt will cause rpc's to be sent to the set of clients using that fcm.
 * 
 * mexpfcm is in the middle of the exp fc chain and corresponds with the GFC fcm tier.
 */
struct mexpfcm {
        struct fidcache_memb_handle *mecm_fcm; /* point to the fcm*/
	u64                   mecm_fcm_opcnt;  /* detect fcm updates */
	u64                   mecm_loc_opcnt;  /* count outstanding updates */
	u64                   mecm_rem_opcnt;  /* detect remote updates */
        psc_spinlock_t        mecm_lock;
        struct exp_bmaptree   mecm_bmaps;      /* my tree of bmap pointers */
        struct pscrpc_export *mecm_export;     /* backpointer to our export */
        SPLAY_ENTRY(mexpfcm)  mecm_exp_tentry; /* export tree entry */
        SPLAY_ENTRY(mexpfcm)  mecm_fcm_tentry; /* fcm tree entry */
};

//XXX Should this be based on the CFD?
static inline int
mexpfcm_cache_cmp(const void *x, const void *y)
{
	slfid_t a, b;
	
	a = ((struct fidcache_memb_handle *)x)->fcmh_memb->fcm_fg->fg_fid;
	b = ((struct fidcache_memb_handle *)y)->fcmh_memb->fcm_fg->fg_fid;

        if (a > b)
                return 1;
        else if (a < b)
                return -1;
        return 0;
}

SPLAY_HEAD(exp_fidtree, mexpfcm);
SPLAY_PROTOTYPE(exp_fidtree, mexpfcm, mecm_exp_tentry, mexpfcm_cache_cmp);

struct mexp_cli {
        struct exp_fidtree    mc_fids;        /* track all fids on export    */
        struct psclist_head   mc_lentry;      /* sched export for revocation */
	list_cache_t          mc_pndg_invals; /* pending invalidations       */
};

/* This data structure will be used to handle ion failover and 
 *   the reassignment of bmaps to other ions.
 */
struct mexp_ion {
	struct dynarray       mi_bmaps;  /* array of struct mexpbcm  */
	struct dynarray       mi_bmaps_deref; /* dereferencing bmaps */
	struct psclist_head   mi_lentry; /* chain ion's              */
	atomic_t              mi_refcnt; /* num cli's using this ion */
	int                   mi_alive;
	struct timespec       mi_lastping;
};

/*
 * mexp (mds_export_private) - stored in the export's private data, bmec_set is an array of bmap_cache_memb pointers.  This structure is traversed if the export is closed or if the file object is truncated.
 *
 * mexpfc is the upper most level of the exp fidcache.
 */
struct mexp {
	u64                   mexp_magic;  /* detect misapplied reconnects? */
        struct pscrpc_export *mexp_export; /* backpointer to our export */
        psc_spinlock_t        mexp_lock;	
	int                   mexp_type;   /* the export flavor (cli, ion) */
	union mexp_types {
		struct mexp_cli *mexptype_cli; 
		struct mexp_ion *mexpype_ion; 
	};
#define mexp_cli mexp_types.mexptype_cli
#define mexp_ion mexp_types.mexptype_ion
};

enum mds_exp_types {
	MDS_EXP_ION = (1<<0),
	MDS_EXP_CLI = (1<<1)
};

SPLAY_HEAD(bmap_exports, mexpbcm);
SPLAY_PROTOTYPE(bmap_exports, mexpbcm, mexpbcm_bmap_tentry,mexpbmap_cache_cmp);
/*
 * bmap_mds_info - associate the fcache block to its respective export bmap caches.
 * Notes: both read and write clients are stored to bmdsi_exports, the ref counts are used to determine the number of both and hence the caching mode used at the clients.   bmdsi_wr_ion is a shortcut pointer used only when the bmap has client writers - all writers (and readers) are directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
        atomic_t              bmdsi_rd_ref;  /* count our cli read refs     */
        atomic_t              bmdsi_wr_ref;  /* count our cli write refs    */
	struct mexpfch       *bmdsi_wr_ion;  /* ion export assigned to bmap */
        struct bmap_exports   bmdsi_exports; /* tree of mexpbcm's           */
};

enum bmap_mds_modes {
	BMAP_MDS_WR = (1<<0), 
	BMAP_MDS_RD = (1<<1)
};

SPLAY_HEAD(fcm_exports, mexpfcm);
SPLAY_PROTOTYPE(fcm_exports, mexpfcm, mexpfcm_fcm_tentry, mexpbmap_cache_cmp);

struct fidc_mds_info {	
	struct fcm_exports fmdsi_exports; /* tree of mexpfcm */
	atomic fmdsi_ref;
};

void
fidc_mds_handle_init(void *p)
{
        struct fidc_memb_handle *f=p;
	struct fidc_mds_info *i;

        /* Private data must have already been freed and the pointer nullified.
         */
        psc_assert(!f->fcmh_pri);
	/* Call the common initialization handler.
	 */
	fidc_gen_handle_init(f);
	/* Here are the 'mds' specific calls.
	 */
        f->fcmh_pri = PSCALLOC(sizeof(*i));
        SPLAY_INIT(&f->fmdsi_exports);
	atomic_set(&f->fmdsi_ref, 0);
}

#endif
