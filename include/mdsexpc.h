/* $Id$ */

#ifndef _MDSEXPC_H_
#define _MDSEXPC_H_

/*
 * These structures provide back pointers into the FID Cache to
 * facilitate client-wise operations within the cache.  Such ops would
 * include dereferencing the entire tree of client bmap references on
 * connection close.
 */

#include <time.h>

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"
#include "psc_util/odtable.h"

#include "jflush.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "inodeh.h"
#include "bmap.h"
#include "mds_bmap.h"

struct pscrpc_export;

/*
 * mexpbcm (mds_export_bmapc_member) - mexpbcm references bmaps stored in
 *   the GFC (global fid cache) and acts as a bridge between GFC bmaps and
 *   the export(s) which reference them.  Mexpbcm is tracked by the global
 *   fidcache (through the fcm's bmap export tree (at the bottom of the GFC's
 *   tree chain).
 *
 * mexpbcm_lentry is used for scheduling revocation of the bmap via a
 *   listcache based queue.
 *
 * mexpbcm is the lowest member of the exp fidcache chain and corresponds to
 *   the GFC's bmap tier.
 */
struct mexpbcm {
	sl_blkno_t              mexpbcm_blkno;
        u32                     mexpbcm_mode;
	u32                     mexpbcm_net_cmd;
	u32                     mexpbcm_net_inf;
        struct bmapc_memb      *mexpbcm_bmap;
	struct pscrpc_export   *mexpbcm_export;
	struct psclist_head     mexpbcm_lentry;      /* ref for client cb   */
	atomic_t                mexpbcm_msgcnt;      /* put to zero after rpc
						      * has been issued */
        SPLAY_ENTRY(mexpbcm)    mexpbcm_exp_tentry;  /* ref from mexpfcm    */
	SPLAY_ENTRY(mexpbcm)    mexpbcm_bmap_tentry; /* ref from bmap tree  */
};

#define mexpbcm2nid(b) (b)->mexpbcm_export->exp_connection->c_peer.nid
/* Borrow the export lock.
 */
#define MEXPBCM_LOCK(m)  spinlock(&(m)->mexpbcm_export->exp_lock)
#define MEXPBCM_ULOCK(m) freelock(&(m)->mexpbcm_export->exp_lock)
#define MEXPBCM_REQLOCK(m)  reqlock(&(m)->mexpbcm_export->exp_lock)
#define MEXPBCM_UREQLOCK(m, l) ureqlock(&(m)->mexpbcm_export->exp_lock, (l))
#define MEXPBCM_LOCK_ENSURE(m) LOCK_ENSURE(&(m)->mexpbcm_export->exp_lock)

enum mexpbcm_modes {
	MEXPBCM_DIO_REQD = (1<<0),  /* dio callback outstanding             */
	MEXPBCM_CIO_REQD = (1<<1),  /* cached-io callback outstanding       */
	MEXPBCM_RD       = (1<<2),
	MEXPBCM_WR       = (1<<3),
	MEXPBCM_CDIO     = (1<<4),  /* client requested directio            */
        MEXPBCM_DIO      = (1<<5),  /* otherwise caching mode               */
	MEXPBCM_INIT     = (1<<6),  /* on it's way, block on the fcmh waitq */
	MEXPBCM_RPC_CANCEL = (1<<7)
};

static inline int
mexpbmapc_cmp(const void *x, const void *y)
{
	const struct mexpbcm *a = x, *b = y;

	if (a->mexpbcm_blkno > b->mexpbcm_blkno)
		return (1);
	if (a->mexpbcm_blkno < b->mexpbcm_blkno)
		return (-1);
	return (0);
}

static inline int
mexpbmapc_exp_cmp(const void *x, const void *y)
{
	const struct mexpbcm *a = x, *b = y;
	struct pscrpc_export *e, *f;

	e = a->mexpbcm_export;
	f = b->mexpbcm_export;

	if (e->exp_connection->c_peer.nid > f->exp_connection->c_peer.nid)
		return (1);
	if (e->exp_connection->c_peer.nid < f->exp_connection->c_peer.nid)
		return (-1);
	return (0);
}

SPLAY_HEAD(exp_bmaptree, mexpbcm);
SPLAY_PROTOTYPE(exp_bmaptree, mexpbcm, mexpbcm_exp_tentry, mexpbmapc_cmp);

/*
 * mexpfcm (mds_export_fidc_memb) - this structure interacts with the mds
 *   fid cache and the clients cache by mediation within the export.  It
 *   tracks which bmaps the client has cached.  Mexpfcm is tracked by the
 *   export's fidcache and the fcm's bmap_lessees tree.
 *
 * mexpfcm is in the middle of the exp fc chain and corresponds with the
 *   GFC fcm tier.
 */
struct mexpfcm {
        struct fidc_membh    *mexpfcm_fcmh;       /* point to the fcm        */
	int                   mexpfcm_flags;
        psc_spinlock_t        mexpfcm_lock;
	union {
		struct exp_bmaptree f_bmaps;      /* tree of bmap pointers   */
	} mexpfcm_ford;
        struct pscrpc_export *mexpfcm_export;     /* backpointer to export   */
        SPLAY_ENTRY(mexpfcm)  mexpfcm_fcm_tentry; /* fcm tree entry          */
#define mexpfcm_bmaps mexpfcm_ford.f_bmaps
};

#define MEXPFCM_LOCK(m)  spinlock(&(m)->mexpfcm_lock)
#define MEXPFCM_ULOCK(m) freelock(&(m)->mexpfcm_lock)
#define MEXPFCM_LOCK_ENSURE(m) LOCK_ENSURE(&(m)->mexpfcm_lock)

enum mexpfcm_states {
	MEXPFCM_CLOSING   = (1<<0),
	MEXPFCM_REGFILE   = (1<<1),
	MEXPFCM_DIRECTORY = (1<<2)
};

#define mexpfcm2fid(m)		fcmh_2_fid((m)->mexpfcm_fcmh)
#define mexpfcm2fidgen(m)	fcmh_2_fgp((m)->mexpfcm_fcmh)

static inline int
mexpfcm_cache_cmp(const void *x, const void *y)
{
	const struct mexpfcm *a=x, *b=y;

        if (a->mexpfcm_export > b->mexpfcm_export)
                return 1;
        else if (a->mexpfcm_export < b->mexpfcm_export)
                return -1;
        return 0;
}

struct mexp_cli {
	struct timespec           mc_lastping;
	struct slashrpc_cservice *mc_csvc;
};

/*
 * mexp_ion - will be used to handle ion failover and the reassignment
 *   of bmaps to other ions.  Mexp_ion is accessed from
 *  (sl_resm_t *)->resm_pri.
 */
struct mexp_ion {
	struct dynarray       mi_bmaps;       /* array of struct mexpbcm     */
	struct dynarray       mi_bmaps_deref; /* dereferencing bmaps         */
	struct psclist_head   mi_lentry;      /* chain ion's                 */
	atomic_t              mi_refcnt;      /* num cli's using this ion    */
	int                   mi_alive;
	struct timespec       mi_lastping;
	struct slashrpc_cservice *mi_csvc;
	sl_resm_t            *mi_resm;
};

/*
 * bmi_assign - the structure used for tracking the mds's bmap / ion
 *   assignments.  These structures are stored in a odtable.
 */
struct bmi_assign {
	lnet_nid_t   bmi_ion_nid;
	sl_ios_id_t  bmi_ios;
	slfid_t      bmi_fid;
	sl_blkno_t   bmi_bmapno;
	time_t       bmi_start;
};

/*
 * bmap_exports is used to reference the exports which are accessing this bmap.
 */
SPLAY_PROTOTYPE(bmap_exports, mexpbcm, mexpbcm_bmap_tentry, mexpbmapc_exp_cmp);

SPLAY_HEAD(fcm_exports, mexpfcm);
SPLAY_PROTOTYPE(fcm_exports, mexpfcm, mexpfcm_fcm_tentry, mexpfcm_cache_cmp);

static inline void
bmdsi_sanity_locked(struct bmapc_memb *bmap, int dio_check, int *wr)
{
	struct bmap_mds_info *mdsi = bmap->bcm_pri;

	wr[0] = atomic_read(&mdsi->bmdsi_wr_ref);
        wr[1] = atomic_read(&mdsi->bmdsi_rd_ref);
        psc_assert(wr[0] >= 0 && wr[1] >= 0);
	if (dio_check && (wr[0] > 1 || (wr[0] && wr[1])))
		psc_assert(bmap->bcm_mode & BMAP_MDS_DIO);
}

struct fidc_mds_info {
	struct fcm_exports        fmdsi_exports; /* tree of mexpfcm */
	struct slash_inode_handle fmdsi_inodeh; // MDS sl_inodeh_t goes here
	atomic_t                  fmdsi_ref;
	u32                       fmdsi_xid;
	void                     *fmdsi_data;
};

#define fcmh_2_inoh(f)							\
	(&((struct fidc_mds_info *)(&(f)->fcmh_fcoo->fcoo_pri))->fmdsi_inodeh)

static inline void
fmdsi_init(struct fidc_mds_info *mdsi, struct fidc_membh *fcmh, void *pri)
{
	SPLAY_INIT(&mdsi->fmdsi_exports);
	atomic_set(&mdsi->fmdsi_ref, 0);
	mdsi->fmdsi_xid = 0;
	mdsi->fmdsi_data = pri;

	slash_inode_handle_init(&mdsi->fmdsi_inodeh, fcmh);
}

/* IOS round-robin counter for assigning IONs.  Attaches at res_pri.
 */
struct resprof_mds_info {
	int rmi_cnt;
	psc_spinlock_t rmi_lock;
};

extern void 
mexpfcm_release_brefs(struct mexpfcm *);

#endif /* _MDSEXPC_H_ */
