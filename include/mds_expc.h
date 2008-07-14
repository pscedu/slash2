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
        SPLAY_ENTRY(mexpbcm)    mexpbcm_tentry;				
};

enum mexpbcm_modes {
	MEXPBCM_WR  = (1<<0),  /* otherwise READ */
        MEXPBCM_DIO = (1<<2)
};

SPLAY_HEAD(exp_bmaptree, mexpbcm);
SPLAY_PROTOTYPE(exp_bmaptree, mexpbcm, mexpbcm_tentry, bmap_cache_cmp);
/*
 * mexpfcm (mds_export_fidcache_memb) - this structure interacts with the mds fid cache and the clients cache by mediation within the export.  It tracks which bmaps the client has cached, the operation (lamport) clocks of the fcm and the client.  mexpfcm is tracked by the export's fidcache and the fcm's bmap_lessees tree.

\ * mecm_fcm_opcnt is used to detect changes in the fcm who keeps his update count in fcm_slfinfo.slf_opcnt.  At no time may mecm_fcm_opcnt be higher than the fcm (unless the fcm counter has flipped).  mecm_loc_opcnt and mecm_rem_opcnt serve a similar purpose but the update detection is between the client and the export.  So if client A updates the ctime or mode, exp(clientA) will inc mecm_fcm->fcm_slfinfo.slf_opcnt and desync the opcnt between the fcm and the other exports prompting the other exports to update their client's caches.  It may be the case that incrementing  mecm_fcm->fcm_slfinfo.slf_opcnt will cause rpc's to be sent to the set of clients using that fcm.
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
        SPLAY_ENTRY(mexpfcm)  mecm_fcm_tentry; /* fcm tree entry    */
};

SPLAY_HEAD(exp_fidtree, mexpfcm);
SPLAY_PROTOTYPE(exp_fidtree, mexpfcm, mecm_exp_tentry, bmap_cache_cmp);


struct mexpfc_cli {
        struct exp_fidtree    mc_fids;   /* reference all fcm's via the
					  * export */
        struct psclist_head   mc_lentry; /* sched export for revocations */
	list_cache_t          mc_pndg_invals; /* pending invalidations   */
};

/* This data structure will be used to handle ion failover and 
 *   the reassignment of bmaps to other ions.
 */
struct mexpfc_ion {
	struct dynarray       mi_bmaps;  /* array of struct mexpbcm  */
	struct dynarray       mi_bmaps_deref; /* dereferencing bmaps */
	struct psclist_head   mi_lentry; /* chain ion's              */
	atomic_t              mi_refcnt; /* num cli's using this ion */
	int                   mi_alive;
	struct timespec       mi_lastping;
};

/*
 * mexpfc (mds_export_fidcache_handle) - stored in the export's private data, bmec_set is an array of bmap_cache_memb pointers.  This structure is traversed if the export is closed or if the file object is truncated.
 *
 * mexpfc is the upper most level of the exp fidcache.
 */
struct mexpfch {
	u64                   mec_magic;  /* detect misapplied reconnects? */
        struct pscrpc_export *mec_export; /* backpointer to our export */
        psc_spinlock_t        mec_lock;	
	int                   mec_type;   /* the export flavor (cli, ion) */
	union mec_types {
		struct mexpfc_cli *mectype_cli; 
		struct mexpfc_ion *mectype_ion; 
	};
#define mec_cli mec_types.mectype_cli
#define mec_ion mec_types.mectype_ion
};

enum mds_exp_types {
	MDS_EXP_ION = (1<<0),
	MDS_EXP_CLI = (1<<1)
};

/* Tree definition for the fcm to enable tracking of leased bmaps */
SPLAY_HEAD(bmap_lessees, mexpfcm);
SPLAY_PROTOTYPE(bmap_lessees, mexpfcm, mecm_fcm_tentry, bmap_cache_cmp);

struct fcmh_info_mds {
	struct bmap_lessees fcimds_lessees;  /* mds only, client leases array 
					      * of bmap_mds_export_cache     */
};

/* 
 * struct bmexpcr is the leaf entity of the bmap_exports tree.
 * XXX GET RID of THIS!!! move the splay into mexpbcm.
 */
struct bmexpcr {
        struct mexpbcm       *bmexpcr_ref;
        SPLAY_ENTRY(bmexpcr)  bmexpcr_tentry;
};

/* Tree of bmexpcr's held by bmap_mds_info.
 */
SPLAY_HEAD(bmap_exports, bmexpcr);
SPLAY_PROTOTYPE(bmap_exports, bmexpcr, bmexpcr_tentry, bmap_cache_cmp);

/*
 * bmap_mds_info - associate the fcache block to its respective export bmap caches.
 * Notes: both read and write clients are stored to bmdsi_exports, the ref counts are used to determine the number of both and hence the caching mode used at the clients.   bmdsi_wr_ion is a shortcut pointer used only when the bmap has client writers - all writers (and readers) are directed to this ion once a client has invoked write mode on the bmap.
 */
struct bmap_mds_info {
        atomic_t              bmdsi_rd_ref;  /* count our cli read refs     */
        atomic_t              bmdsi_wr_ref;  /* count our cli write refs    */
	struct mexpfch       *bmdsi_wr_ion;  /* ion export assigned to bmap */
        struct bmap_exports   bmdsi_exports; /* tree of our client exports  */
};

enum bmap_mds_modes {
	BMAP_MDS_WR = (1<<0), 
	BMAP_MDS_RD = (1<<1)
};
#endif
