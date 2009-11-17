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

#include "psc_ds/tree.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"
#include "psc_util/odtable.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fidcache.h"
#include "inodeh.h"
#include "jflush.h"
#include "slashrpc.h"

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
	uint32_t                mexpbcm_mode;
	uint32_t                mexpbcm_net_cmd;
	uint32_t                mexpbcm_net_inf;
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

static __inline int
mexpbmapc_cmp(const void *x, const void *y)
{
	const struct mexpbcm *a = x, *b = y;

	return (CMP(a->mexpbcm_blkno, b->mexpbcm_blkno));
}

static __inline int
mexpbmapc_exp_cmp(const void *x, const void *y)
{
	const struct mexpbcm *a = x, *b = y;

	return (CMP(a->mexpbcm_export->exp_connection->c_peer.nid,
	    b->mexpbcm_export->exp_connection->c_peer.nid));
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

static __inline int
mexpfcm_cache_cmp(const void *x, const void *y)
{
	const struct mexpfcm *a=x, *b=y;

	return (CMP(a->mexpfcm_export, b->mexpfcm_export));
}

struct mexp_cli {
	struct timespec           mc_lastping;
	struct slashrpc_cservice *mc_csvc;
	psc_spinlock_t		  mc_lock;
};

/*
 * mexp_ion - will be used to handle ion failover and the reassignment
 *   of bmaps to other ions.  mexp_ion is accessed from
 *  (struct sl_resm *)->resm_pri.
 */
struct mexp_ion {
	struct dynarray       mi_bmaps;       /* array of struct mexpbcm     */
	struct dynarray       mi_bmaps_deref; /* dereferencing bmaps         */
	atomic_t              mi_refcnt;      /* num cli's using this ion    */
	struct sl_resm       *mi_resm;
};

/*
 * bmap_exports is used to reference the exports which are accessing this bmap.
 */
SPLAY_PROTOTYPE(bmap_exports, mexpbcm, mexpbcm_bmap_tentry, mexpbmapc_exp_cmp);

extern void
mexpfcm_release_brefs(struct mexpfcm *);

#endif /* _MDSEXPC_H_ */
