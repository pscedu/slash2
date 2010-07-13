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

/*
 * Routines implementing replication features in the MDS.
 *
 * This ranges from tracking the replication state of each bmap's copy on
 * each ION and managing replication requests and persistent behavior.
 */

#include <sys/param.h>

#include <linux/fuse.h>

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "pfl/str.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/alloc.h"
#include "psc_util/crc.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_util/pool.h"
#include "psc_util/pthrutil.h"
#include "psc_util/waitq.h"

#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "inodeh.h"
#include "mdsio.h"
#include "mdslog.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slerr.h"
#include "up_sched_res.h"

struct upschedtree	 upsched_tree = SPLAY_INITIALIZER(&upsched_tree);
struct psc_poolmgr	*upsched_pool;
psc_spinlock_t		 upsched_tree_lock = LOCK_INITIALIZER;

struct psc_listcache	 slm_replst_workq;

struct psc_vbitmap	*repl_busytable;
psc_spinlock_t		 repl_busytable_lock = LOCK_INITIALIZER;
int			 repl_busytable_nents;
sl_ino_t		 mds_repldir_inum;

__static int
iosidx_cmp(const void *a, const void *b)
{
	const int *x = a, *y = b;

	return (CMP(*x, *y));
}

__static int
iosidx_in(int idx, const int *iosidx, int nios)
{
	if (bsearch(&idx, iosidx, nios,
	    sizeof(iosidx[0]), iosidx_cmp))
		return (1);
	return (0);
}

int
uswi_cmp(const void *a, const void *b)
{
	const struct up_sched_work_item *x = a, *y = b;

	return (CMP(USWI_FID(x), USWI_FID(y)));
}

SPLAY_GENERATE(upschedtree, up_sched_work_item, uswi_tentry, uswi_cmp);

void
mds_repl_enqueue_sites(struct up_sched_work_item *wk,
    const sl_replica_t *iosv, int nios)
{
	struct site_mds_info *smi;
	struct sl_site *site;
	int locked, n;

	locked = psc_pthread_mutex_reqlock(&wk->uswi_mutex);
	wk->uswi_gen++;
	for (n = 0; n < nios; n++) {
		site = libsl_resid2site(iosv[n].bs_id);
		smi = site->site_pri;

		spinlock(&smi->smi_lock);
		if (!psc_dynarray_exists(&smi->smi_upq, wk)) {
			psc_dynarray_add(&smi->smi_upq, wk);
			smi->smi_flags |= SMIF_DIRTYQ;
			psc_atomic32_inc(&wk->uswi_refcnt);
		}
		psc_multiwaitcond_wakeup(&smi->smi_mwcond);
		freelock(&smi->smi_lock);
	}
	psc_pthread_mutex_ureqlock(&wk->uswi_mutex, locked);
}

int
_mds_repl_ios_lookup(struct slash_inode_handle *i, sl_ios_id_t ios, int add,
	     int journal)
{
	uint32_t j=0, k;
	int rc = -ENOENT;
	sl_replica_t *repl;

	INOH_LOCK(i);
	if (!i->inoh_ino.ino_nrepls) {
		if (!add)
			goto out;
		else
			goto add_repl;
	}

	for (j = 0, k = 0, repl = i->inoh_ino.ino_repls;
	    j < i->inoh_ino.ino_nrepls; j++, k++) {
		if (j >= SL_DEF_REPLICAS) {
			/* The first few replicas are in the inode itself,
			 *   the rest are in the extras block.
			 */
			if (!(i->inoh_flags & INOH_HAVE_EXTRAS))
				if (!(rc = mds_inox_load_locked(i)))
					goto out;

			repl = i->inoh_extras->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_INFO, i, "rep%u[%u] == %u",
			   k, repl[k].bs_id, ios);
		if (repl[k].bs_id == ios) {
			rc = j;
			goto out;
		}
	}
	/* It does not exist, add the replica to the inode if 'add' was
	 *   specified, else return.
	 */
	if (rc == -ENOENT && add) {
 add_repl:
		psc_assert(i->inoh_ino.ino_nrepls == j);

		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
			rc = -ENOSPC;
			goto out;
		}

		if (j > SL_DEF_REPLICAS) {
			/* Note that both the inode structure and replication
			 *  table must be synced.
			 */
			psc_assert(i->inoh_extras);
			i->inoh_flags |= (INOH_EXTRAS_DIRTY | INOH_INO_DIRTY);
			repl = i->inoh_extras->inox_repls;
			k = j - SL_DEF_REPLICAS;
		} else {
			i->inoh_flags |= INOH_INO_DIRTY;
			repl = i->inoh_ino.ino_repls;
			k = j;
		}

		repl[k].bs_id = ios;
		i->inoh_ino.ino_nrepls++;

		DEBUG_INOH(PLL_INFO, i, "add IOS(%u) to repls, replica %d",
			   ios, i->inoh_ino.ino_nrepls-1);

		if (journal)
			mds_inode_addrepl_update(i, ios, j);

		rc = j;
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

#define mds_repl_iosv_lookup(ih, ios, iosidx, nios)	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 0)
#define mds_repl_iosv_lookup_add(ih, ios, iosidx, nios)	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 1)

__static int
_mds_repl_iosv_lookup(struct slash_inode_handle *ih,
    const sl_replica_t iosv[], int iosidx[], int nios, int add)
{
	int k, last;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(ih, iosv[k].bs_id, add, add)) < 0)
			return (-iosidx[k]);

	qsort(iosidx, nios, sizeof(iosidx[0]), iosidx_cmp);
	/* check for dups */
	last = -1;
	for (k = 0; k < nios; k++, last = iosidx[k])
		if (iosidx[k] == last)
			return (EINVAL);
	return (0);
}

#if 0
__static int
mds_repl_xattr_load_locked(struct slash_inode_handle *i)
{
	char fidfn[FID_MAX_PATH];
	size_t sz;
	int rc;

	DEBUG_INOH(PLL_INFO, i, "trying to load replica table");

	INOH_LOCK_ENSURE(i);
	psc_assert(i->inoh_ino.ino_nrepls);
	psc_assert(!i->inoh_replicas);
	psc_assert(!(i->inoh_flags & INOH_HAVE_REPS));

	fid_makepath(i->inoh_ino.ino_fg.fg_fid, fidfn);
	sz = (sizeof(sl_replica_t) * i->inoh_ino.ino_nrepls);

	if (fid_getxattr(fidfn, SFX_REPLICAS,  i->inoh_replicas, sz)) {
		psc_warnx("fid_getxattr failed to get %s", SFX_REPLICAS);
		rc = -errno;
		goto fail;

	} else if (mds_repl_crc_check(i)) {
		rc = -EIO;
		goto fail;

	} else {
		i->inoh_flags |= INOH_HAVE_REPS;
		DEBUG_INOH(PLL_INFO, i, "replica table loaded");
	}
	return (0);

 fail:
	DEBUG_INOH(PLL_INFO, i, "replica table load failed");
	return (rc);
}
#endif

int
_mds_repl_bmap_apply(struct bmapc_memb *bcm, const int *tract,
    const int *retifset, int flags, int off, int *scircuit)
{
	struct slash_bmap_od *bmapod = bcm->bcm_od;
	struct bmap_mds_info *bmdsi = bmap_2_bmdsi(bcm);
	int locked, val, rc = 0;

	/* Take a write lock on the bmapod.
	 */
	BMAPOD_WRLOCK(bmdsi);

	if (scircuit)
		*scircuit = 0;
	else
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);

	val = SL_REPL_GET_BMAP_IOS_STAT(bmapod->bh_repls, off);

	if (val >= SL_NREPLST)
		psc_fatalx("corrupt bmap");

	/* Check for return values
	 */
	if (retifset && retifset[val]) {
		/* Assign here instead of above to prevent
		 *   overwriting a zero return value.
		 */
		rc = retifset[val];
		if (flags & REPL_WALKF_SCIRCUIT) {
			*scircuit = 1;
			goto out;
		}
	}

	/* Apply any translations */
	if (tract && tract[val] != -1) {
		SL_REPL_SET_BMAP_IOS_STAT(bmapod->bh_repls,
		    off, tract[val]);
		locked = BMAP_RLOCK(bcm);
		bmdsi->bmdsi_flags |= BMIM_LOGCHG;
		BMAP_URLOCK(bcm, locked);
	}

 out:
	BMAPOD_ULOCK(bmdsi);
	return (rc);
}

/*
 * mds_repl_bmap_walk - walk the bmap replication bits, performing any
 *	specified translations and returning any queried states.
 * @b: bmap.
 * @tract: translation actions; for each array slot, set states of the type
 *	corresponding to the array index to the array value.  For example:
 *
 *		tract[SL_REPLST_INACTIVE] = SL_REPLST_ACTIVE
 *
 *	This changes any SL_REPLST_INACTIVE states into SL_REPLST_ACTIVE.
 * @retifset: return the value of the slot in this array corresponding to
 *	the state value as the slot index, if the array value is nonzero;
 *	the last replica always gets priority unless SCIRCUIT is specified.
 * @flags: operational flags.
 * @iosidx: indexes of I/O systems to exclude or query, or NULL for everyone.
 * @nios: # I/O system indexes specified.
 */
int
mds_repl_bmap_walk(struct bmapc_memb *bcm, const int *tract,
    const int *retifset, int flags, const int *iosidx, int nios)
{
	struct slash_bmap_od *bmapod = bcm->bcm_od;
	int scircuit, nr, off, k, rc, trc;

	scircuit = rc = 0;
	nr = fcmh_2_inoh(bcm->bcm_fcmh)->inoh_ino.ino_nrepls;
	bmapod = bcm->bcm_od;

	if (nios == 0)
		/* no one specified; apply to all */
		for (k = 0, off = 0; k < nr;
		    k++, off += SL_BITS_PER_REPLICA) {
			trc = _mds_repl_bmap_apply(bcm, tract,
			    retifset, flags, off, &scircuit);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}
	else if (flags & REPL_WALKF_MODOTH) {
		/* modify sites all sites except those specified */
		for (k = 0, off = 0; k < nr; k++,
		    off += SL_BITS_PER_REPLICA)
			if (!iosidx_in(k, iosidx, nios)) {
				trc = _mds_repl_bmap_apply(bcm, tract,
				    retifset, flags, off, &scircuit);
				if (trc)
					rc = trc;
				if (scircuit)
					break;
			}
	} else
		/* modify only the sites specified */
		for (k = 0; k < nios; k++) {
			trc = _mds_repl_bmap_apply(bcm, tract,
			    retifset, flags, iosidx[k] *
			    SL_BITS_PER_REPLICA, &scircuit);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}

	return (rc);
}

/*
 * mds_repl_inv_except - For the given bmap, change the status of
 *	all its replicas marked "active" to "old" except for the replica
 *	specified.
 *
 *	This is a high-level convenience call provided to easily update
 *	status after an ION has received some new I/O, which would make
 *	all other existing copies of the bmap on any other replicas old.
 * @bcm: the bmap.
 * @ios: the ION resource that should stay marked "active".
 */
int
mds_repl_inv_except(struct bmapc_memb *bcm, sl_ios_id_t ios)
{
	int rc, iosidx, tract[SL_NREPLST], retifset[SL_NREPLST];
	uint32_t policy;
	struct up_sched_work_item *wk;
	sl_replica_t repl;

	/* Find/add our replica's IOS ID but instruct mds_repl_ios_lookup_add()
	 *   not to journal this operation because the inode's repl table
	 *   will be journaled with this bmap's updated repl bitmap.
	 * This saves a journal I/O.
	 */
	iosidx = mds_repl_ios_lookup_add(fcmh_2_inoh(bcm->bcm_fcmh), ios, 1);
	if (iosidx < 0)
		psc_fatalx("lookup ios %d: %s", ios, slstrerror(iosidx));

	/* If this bmap is marked for persistent replication,
	 * the repl request must exist and should be marked such
	 * that the replication monitors do not release it in the
	 * midst of processing it as this activity now means they
	 * have more to do.
	 */
	BHREPL_POLICY_GET(bcm, policy);
	if (policy == BRP_PERSIST) {
		wk = uswi_find(&bcm->bcm_fcmh->fcmh_fg, NULL);
		repl.bs_id = ios;
		mds_repl_enqueue_sites(wk, &repl, 1);
		uswi_unref(wk);
	}

	/* Ensure this replica is marked active
	 */
	tract[SL_REPLST_INACTIVE] = SL_REPLST_ACTIVE;
	tract[SL_REPLST_OLD] = -1;
	tract[SL_REPLST_SCHED] = -1;
	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_TRUNCPNDG] = -1;
	tract[SL_REPLST_GARBAGE] = -1;
	tract[SL_REPLST_GARBAGE_SCHED] = -1;

	retifset[SL_REPLST_INACTIVE] = 0;
	retifset[SL_REPLST_OLD] = EINVAL;
	retifset[SL_REPLST_SCHED] = EINVAL;
	retifset[SL_REPLST_ACTIVE] = 0;
	retifset[SL_REPLST_TRUNCPNDG] = EINVAL;
	retifset[SL_REPLST_GARBAGE] = EINVAL;
	retifset[SL_REPLST_GARBAGE_SCHED] = EINVAL;

	rc = mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	if (rc)
		psc_error("bh_repls is marked OLD or SCHED for fid %"PRIx64" "
		    "bmap %d iosidx %d", fcmh_2_fid(bcm->bcm_fcmh),
		    bcm->bcm_blkno, iosidx);

	/*
	 * Invalidate all other replicas.
	 * Note: if the status is SCHED here, don't do anything; once
	 * the replication status update comes from the ION, we will know
	 * he copied an old bmap and mark it OLD then.
	 */
	tract[SL_REPLST_INACTIVE] = -1;
	tract[SL_REPLST_OLD] = -1;
	tract[SL_REPLST_SCHED] = -1;
	tract[SL_REPLST_ACTIVE] = SL_REPLST_OLD;
	tract[SL_REPLST_TRUNCPNDG] = -1;
	tract[SL_REPLST_GARBAGE] = -1;
	tract[SL_REPLST_GARBAGE_SCHED] = -1;

	retifset[SL_REPLST_INACTIVE] = 0;
	retifset[SL_REPLST_OLD] = 0;
	retifset[SL_REPLST_SCHED] = 0;
	retifset[SL_REPLST_ACTIVE] = 1;
	retifset[SL_REPLST_TRUNCPNDG] = 0;
	retifset[SL_REPLST_GARBAGE] = 0;
	retifset[SL_REPLST_GARBAGE_SCHED] = 0;

	if (mds_repl_bmap_walk(bcm, tract, retifset,
	    REPL_WALKF_MODOTH, &iosidx, 1))
		BHGEN_INCREMENT(bcm);

	/* Write changes to disk
	 */
	mds_bmap_repl_update(bcm);

	return (0);
}

void
mds_repl_bmap_rel(struct bmapc_memb *bcm)
{
	mds_bmap_repl_update(bcm);
	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
}

int
mds_repl_loadino(const struct slash_fidgen *fgp, struct fidc_membh **fp)
{
//	struct slash_inode_handle *ih;
	struct fidc_membh *fcmh;
	int rc;

	*fp = NULL;

	rc = slm_fcmh_get(fgp, &fcmh);
	if (rc)
		return (rc);

//	ih = fcmh_2_inoh(fcmh);
//	rc = mds_inox_ensure_loaded(ih);
//	if (rc)
//		psc_fatalx("mds_inox_ensure_loaded: %s", slstrerror(rc));
	*fp = fcmh;

	if (rc)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (rc);
}

int
mds_repl_addrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    const sl_replica_t *iosv, int nios)
{
	int tract[SL_NREPLST], retifset[SL_NREPLST], retifzero[SL_NREPLST];
	int iosidx[SL_MAX_REPLICAS], rc, locked;
	struct up_sched_work_item *newrq, *wk;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	char fn[FID_MAX_PATH];

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (EINVAL);

	newrq = psc_pool_get(upsched_pool);

	rc = 0;
 restart:
	spinlock(&upsched_tree_lock);
	wk = uswi_find(fgp, &locked);
	if (wk == NULL) {
		/*
		 * If the tree stayed locked, the request
		 * exists but we can't use it e.g. because it
		 * is going away.
		 */
		if (!locked)
			goto restart;

		/* Not found, add it and its persistent link. */
		rc = snprintf(fn, sizeof(fn), "%016"PRIx64, fgp->fg_fid);
		if (rc == -1)
			rc = errno;
		else if (rc >= (int)sizeof(fn))
			rc = ENAMETOOLONG;
		else if ((rc = mds_repl_loadino(fgp, &fcmh)) != ENOENT) {
			if (rc)
				psc_fatalx("mds_repl_loadino: %s",
				    slstrerror(rc));

			/* Find/add our replica's IOS ID */
			rc = mds_repl_iosv_lookup_add(fcmh_2_inoh(fcmh),
			    iosv, iosidx, nios);
			if (rc)
				goto bail;

			rc = uswi_init(newrq, fcmh);
			if (rc == 0) {
				wk = newrq;
				newrq = NULL;

				/*
				 * Refcnt is 1 for the tree's ref after
				 * return here; bump again though because
				 * we will unrefrq() when we're done.
				 */
				psc_atomic32_inc(&wk->uswi_refcnt);
			}
		}
	} else {
		/* Find/add our replica's IOS ID */
		rc = mds_repl_iosv_lookup_add(USWI_INOH(wk),
		    iosv, iosidx, nios);
	}
 bail:
	if (locked)
		freelock(&upsched_tree_lock);

	if (newrq)
		psc_pool_return(upsched_pool, newrq);

	if (rc) {
		if (wk)
			uswi_unref(wk);
		return (rc);
	}

	/*
	 * Check inode's bmap state.  INACTIVE and ACTIVE states
	 * become OLD, signifying that replication needs to happen.
	 */
	tract[SL_REPLST_INACTIVE] = SL_REPLST_OLD;
	tract[SL_REPLST_SCHED] = SL_REPLST_OLD;
	tract[SL_REPLST_OLD] = -1;
	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_TRUNCPNDG] = -1;
	tract[SL_REPLST_GARBAGE] = -1;
	tract[SL_REPLST_GARBAGE_SCHED] = -1;

	retifzero[SL_REPLST_INACTIVE] = 0;
	retifzero[SL_REPLST_ACTIVE] = 1;
	retifzero[SL_REPLST_OLD] = 0;
	retifzero[SL_REPLST_SCHED] = 0;
	retifzero[SL_REPLST_TRUNCPNDG] = 0;
	retifzero[SL_REPLST_GARBAGE] = 0;
	retifzero[SL_REPLST_GARBAGE_SCHED] = 0;

	if (bmapno == (sl_bmapno_t)-1) {
		int repl_some_act = 0, repl_all_act = 1;
		int ret_if_inact[SL_NREPLST];

		/* check if all bmaps are already old/queued */
		retifset[SL_REPLST_INACTIVE] = 1;
		retifset[SL_REPLST_SCHED] = 0;
		retifset[SL_REPLST_OLD] = 0;
		retifset[SL_REPLST_ACTIVE] = 1;
		retifset[SL_REPLST_TRUNCPNDG] = 0;
		retifset[SL_REPLST_GARBAGE] = 0;
		retifset[SL_REPLST_GARBAGE_SCHED] = 0;

		/* check if all bmaps are already active */
		ret_if_inact[SL_REPLST_INACTIVE] = 1;
		ret_if_inact[SL_REPLST_SCHED] = 1;
		ret_if_inact[SL_REPLST_OLD] = 1;
		ret_if_inact[SL_REPLST_ACTIVE] = 0;
		ret_if_inact[SL_REPLST_TRUNCPNDG] = 1;
		ret_if_inact[SL_REPLST_GARBAGE] = 1;
		ret_if_inact[SL_REPLST_GARBAGE_SCHED] = 1;

		for (bmapno = 0; bmapno < USWI_NBMAPS(wk); bmapno++) {
			if (mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm))
				continue;

			BMAP_LOCK(bcm);

			/*
			 * If no ACTIVE replicas exist, the bmap must be
			 * uninitialized/all zeroes.  Skip it.
			 */
			if (mds_repl_bmap_walk_all(bcm, NULL, retifzero,
			    REPL_WALKF_SCIRCUIT) == 0) {
				bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
				continue;
			}

			repl_some_act |= mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			if (repl_all_act && mds_repl_bmap_walk_all(bcm, NULL,
			    ret_if_inact, REPL_WALKF_SCIRCUIT))
				repl_all_act = 0;
			mds_repl_bmap_rel(bcm);
		}
		if (bmapno && repl_some_act == 0)
			rc = EALREADY;
		else if (repl_all_act)
			rc = SLERR_REPL_ALREADY_ACT;
	} else if (mds_bmap_exists(wk->uswi_fcmh, bmapno)) {
		/*
		 * If this bmap is already being
		 * replicated, return EALREADY.
		 */
		retifset[SL_REPLST_INACTIVE] = 0;
		retifset[SL_REPLST_SCHED] = EALREADY;
		retifset[SL_REPLST_OLD] = EALREADY;
		retifset[SL_REPLST_ACTIVE] = 0;
		retifset[SL_REPLST_TRUNCPNDG] = SLERR_REPL_NOT_ACT;
		retifset[SL_REPLST_GARBAGE] = SLERR_REPL_NOT_ACT;
		retifset[SL_REPLST_GARBAGE_SCHED] = SLERR_REPL_NOT_ACT;

		rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
		if (rc == 0) {
			BMAP_LOCK(bcm);

			/*
			 * If no ACTIVE replicas exist, the bmap must be
			 * uninitialized/all zeroes.  Skip it.
			 */
			if (mds_repl_bmap_walk_all(bcm, NULL, retifzero,
			    REPL_WALKF_SCIRCUIT) == 0) {
				bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
				rc = SLERR_BMAP_ZERO;
			} else {
				rc = mds_repl_bmap_walk(bcm,
				    tract, retifset, 0, iosidx, nios);
				mds_repl_bmap_rel(bcm);
			}
		}
	} else
		rc = SLERR_BMAP_INVALID;

	if (rc == 0)
		mds_repl_enqueue_sites(wk, iosv, nios);
	else if (rc == SLERR_BMAP_ZERO)
		rc = 0;

	uswi_unref(wk);
	return (rc);
}

int
mds_repl_delrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    const sl_replica_t *iosv, int nios)
{
	int rc, tract[SL_NREPLST], retifset[SL_NREPLST], iosidx[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (EINVAL);

	wk = uswi_find(fgp, NULL);
	if (wk == NULL)
		return (SLERR_REPL_NOT_ACT);

	/* Find replica IOS indexes */
	rc = mds_repl_iosv_lookup_add(USWI_INOH(wk),
	    iosv, iosidx, nios);
	if (rc) {
		uswi_unref(wk);
		return (rc);
	}

	tract[SL_REPLST_INACTIVE] = -1;
	tract[SL_REPLST_ACTIVE] = -1;
	tract[SL_REPLST_OLD] = SL_REPLST_INACTIVE;
	tract[SL_REPLST_SCHED] = SL_REPLST_INACTIVE;
	tract[SL_REPLST_TRUNCPNDG] = -1;
	tract[SL_REPLST_GARBAGE] = -1;
	tract[SL_REPLST_GARBAGE_SCHED] = -1;

	if (bmapno == (sl_bmapno_t)-1) {
		retifset[SL_REPLST_INACTIVE] = 0;
		retifset[SL_REPLST_ACTIVE] = 1;
		retifset[SL_REPLST_OLD] = 1;
		retifset[SL_REPLST_SCHED] = 1;
		retifset[SL_REPLST_TRUNCPNDG] = 0;
		retifset[SL_REPLST_GARBAGE] = 0;
		retifset[SL_REPLST_GARBAGE_SCHED] = 0;

		rc = SLERR_REPLS_ALL_INACT;
		for (bmapno = 0; bmapno < USWI_NBMAPS(wk); bmapno++) {
			if (mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm))
				continue;

			BMAP_LOCK(bcm);
			if (mds_repl_bmap_walk(bcm, tract,
			    retifset, 0, iosidx, nios))
				rc = 0;
			mds_repl_bmap_rel(bcm);
		}
	} else if (mds_bmap_exists(wk->uswi_fcmh, bmapno)) {
		retifset[SL_REPLST_INACTIVE] = SLERR_REPL_ALREADY_INACT;
		retifset[SL_REPLST_ACTIVE] = 0;
		retifset[SL_REPLST_OLD] = 0;
		retifset[SL_REPLST_SCHED] = 0;
		retifset[SL_REPLST_TRUNCPNDG] = 0; /* XXX EINVAL? */
		retifset[SL_REPLST_GARBAGE] = EINVAL;
		retifset[SL_REPLST_GARBAGE_SCHED] = EINVAL;

		rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
		if (rc == 0) {
			BMAP_LOCK(bcm);
			rc = mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			mds_repl_bmap_rel(bcm);
		}
	} else
		rc = SLERR_BMAP_INVALID;

	uswi_unref(wk);
	return (rc);
}

void
mds_repl_scandir(void)
{
	sl_replica_t iosv[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	int rc, tract[SL_NREPLST];
	char *buf, fn[NAME_MAX];
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;
	struct slash_fidgen fg;
	struct fuse_dirent *d;
	off64_t off, toff;
	size_t siz, tsiz;
	uint32_t j;
	void *data;

	rc = mdsio_opendir(mds_repldir_inum, &rootcreds, NULL, &data);
	if (rc)
		psc_fatalx("mdsio_opendir %s: %s", SL_PATH_REPLS,
		    slstrerror(rc));

	off = 0;
	siz = 8 * 1024;
	buf = PSCALLOC(siz);

	for (;;) {
		rc = mdsio_readdir(&rootcreds, siz,
			   off, buf, &tsiz, NULL, NULL, 0, data);
		if (rc)
			psc_fatalx("mdsio_readdir %s: %s", SL_PATH_REPLS,
			    slstrerror(rc));
		if (tsiz == 0)
			break;
		for (toff = 0; toff < (off64_t)tsiz;
		    toff += FUSE_DIRENT_SIZE(d)) {
			d = (void *)(buf + toff);
			off = d->off;

			if (strlcpy(fn, d->name, sizeof(fn)) > sizeof(fn))
				psc_assert("impossible");
			if (d->namelen < sizeof(fn))
				fn[d->namelen] = '\0';

			if (fn[0] == '.')
				continue;

			rc = mdsio_lookup(mds_repldir_inum, fn, &fg,
			    NULL, &rootcreds, NULL);
			if (rc)
				/* XXX if ENOENT, remove from repldir and continue */
				psc_fatalx("mdsio_lookup %s/%s: %s",
				    SL_PATH_REPLS, fn, slstrerror(rc));

			rc = mds_repl_loadino(&fg, &fcmh);
			if (rc)
				/* XXX if ENOENT, remove from repldir and continue */
				psc_fatal("mds_repl_loadino: %s",
				    slstrerror(rc));

			wk = psc_pool_get(upsched_pool);
			rc = uswi_initf(wk, fcmh, USWI_INITF_NOPERSIST);
			if (rc)
				psc_fatal("uswi_initf: %s",
				    slstrerror(rc));

			psc_pthread_mutex_lock(&wk->uswi_mutex);
			wk->uswi_flags &= ~USWIF_BUSY;
			psc_pthread_mutex_unlock(&wk->uswi_mutex);

			tract[SL_REPLST_INACTIVE] = -1;
			tract[SL_REPLST_ACTIVE] = -1;
			tract[SL_REPLST_OLD] = -1;
			tract[SL_REPLST_SCHED] = SL_REPLST_OLD;
			tract[SL_REPLST_TRUNCPNDG] = -1;
			tract[SL_REPLST_GARBAGE] = -1;
			tract[SL_REPLST_GARBAGE_SCHED] = -1;

			/*
			 * If we crashed, revert all inflight SCHED'ed
			 * bmaps to OLD.
			 */
			for (j = 0; j < USWI_NBMAPS(wk); j++) {
				if (mds_bmap_load(wk->uswi_fcmh, j, &bcm))
					continue;

				BMAP_LOCK(bcm);
				mds_repl_bmap_walk(bcm, tract,
				    NULL, 0, NULL, 0);
				mds_repl_bmap_rel(bcm);
			}

			/*
			 * Requeue pending replications on all sites.
			 * If there is no work to do, it will be promptly
			 * removed by the slmupschedthr.
			 */
			for (j = 0; j < USWI_NREPLS(wk); j++)
				iosv[j].bs_id = USWI_GETREPL(wk, j).bs_id;
			mds_repl_enqueue_sites(wk, iosv, USWI_NREPLS(wk));
		}
		off += tsiz;
	}
	rc = mdsio_release(&rootcreds, data);
	if (rc)
		psc_fatalx("mdsio_release %s: %s", SL_PATH_REPLS,
		    slstrerror(rc));

	free(buf);
}

/*
 * The replication busy table is a bitmap to allow quick lookups of
 * communication status between arbitrary IONs.  Each resm has a unique
 * busyid:
 *
 *	     A    B    C    D    E    F    G		n | off (sz=6)
 *	  +----+----+----+----+----+----+----+		--+-------------
 *	A |    |  0 |  1 |  3 |  6 | 10 | 15 |		0 |  0
 *	  +----+----+----+----+----+----+----+		1 |  6
 *	B |    |    |  2 |  4 |  7 | 11 | 16 |		2 | 11
 *	  +----+----+----+----+----+----+----+		3 | 15
 *	C |    |    |    |  5 |  8 | 12 | 17 |		4 | 18
 *	  +----+----+----+----+----+----+----+		5 | 20
 *	D |    |    |    |    |  9 | 13 | 18 |		--+-------------
 *	  +----+----+----+----+----+----+----+		n | n(n+1)/2
 *	E |    |    |    |    |    | 14 | 19 |
 *	  +----+----+----+----+----+----+----+
 *	F |    |    |    |    |    |    | 20 |
 *	  +----+----+----+----+----+----+----+
 *	G |    |    |    |    |    |    |    |
 *	  +----+----+----+----+----+----+----+
 *
 * For checking if communication exists between resources with busyid
 * `min' and `max', we test the bit:
 *
 *	(max - 1) * (max) / 2 + min
 */
#define MDS_REPL_BUSYNODES(min, max)					\
	(((max) - 1) * (max) / 2 + (min))

int
_mds_repl_nodes_setbusy(struct resm_mds_info *ma,
    struct resm_mds_info *mb, int set, int busy)
{
	const struct resm_mds_info *min, *max;
	int rc, locked;

	psc_assert(ma->rmmi_busyid != mb->rmmi_busyid);

	if (ma->rmmi_busyid < mb->rmmi_busyid) {
		min = ma;
		max = mb;
	} else {
		min = mb;
		max = ma;
	}

	locked = reqlock(&repl_busytable_lock);
	if (set)
		rc = psc_vbitmap_xsetval(repl_busytable,
		    MDS_REPL_BUSYNODES(min->rmmi_busyid, max->rmmi_busyid), busy);
	else
		rc = psc_vbitmap_get(repl_busytable,
		    MDS_REPL_BUSYNODES(min->rmmi_busyid, max->rmmi_busyid));
	ureqlock(&repl_busytable_lock, locked);

	/*
	 * If we set the status to "not busy", alert anyone
	 * waiting to utilize the new connection slots.
	 */
	if (set && busy == 0 && rc) {
		locked = reqlock(&ma->rmmi_lock);
		psc_multiwaitcond_wakeup(&ma->rmmi_mwcond);
		ureqlock(&ma->rmmi_lock, locked);

		locked = reqlock(&mb->rmmi_lock);
		psc_multiwaitcond_wakeup(&mb->rmmi_mwcond);
		ureqlock(&mb->rmmi_lock, locked);
	}
	return (rc);
}

void
mds_repl_node_clearallbusy(struct resm_mds_info *rmmi)
{
	int n, j, locked, locked2;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;

	PLL_LOCK(&globalConfig.gconf_sites);
	locked = reqlock(&repl_busytable_lock);
	locked2 = reqlock(&rmmi->rmmi_lock);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, n, &s->site_resources)
			DYNARRAY_FOREACH(resm, j, &r->res_members)
				if (resm->resm_pri != rmmi)
					mds_repl_nodes_setbusy(rmmi,
					    resm->resm_pri, 0);
	ureqlock(&rmmi->rmmi_lock, locked2);
	ureqlock(&repl_busytable_lock, locked);
	PLL_ULOCK(&globalConfig.gconf_sites);
}

void
mds_repl_buildbusytable(void)
{
	struct resm_mds_info *rmmi;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int n, j;

	/* count # resm's (IONs) and assign each a busy identifier */
	PLL_LOCK(&globalConfig.gconf_sites);
	spinlock(&repl_busytable_lock);
	repl_busytable_nents = 0;
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, n, &s->site_resources)
			DYNARRAY_FOREACH(resm, j, &r->res_members) {
				rmmi = resm->resm_pri;
				rmmi->rmmi_busyid = repl_busytable_nents++;
			}
	PLL_ULOCK(&globalConfig.gconf_sites);

	if (repl_busytable)
		psc_vbitmap_free(repl_busytable);
	repl_busytable = psc_vbitmap_new(repl_busytable_nents *
	    (repl_busytable_nents + 1) / 2);
	freelock(&repl_busytable_lock);
}

void
mds_repl_reset_scheduled(sl_ios_id_t resid)
{
	int tract[SL_NREPLST], rc, iosidx;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;
	sl_replica_t repl;
	sl_bmapno_t n;

	repl.bs_id = resid;

	spinlock(&upsched_tree_lock);
	SPLAY_FOREACH(wk, upschedtree, &upsched_tree) {
		psc_atomic32_inc(&wk->uswi_refcnt);
		if (!uswi_access(wk))
			continue;

		rc = mds_inox_ensure_loaded(USWI_INOH(wk));
		if (rc) {
			psc_warnx("couldn't load inoh repl table: %s",
			    slstrerror(rc));
			goto end;
		}

		iosidx = mds_repl_ios_lookup(USWI_INOH(wk), resid);
		if (iosidx < 0)
			goto end;

		tract[SL_REPLST_INACTIVE] = -1;
		tract[SL_REPLST_SCHED] = SL_REPLST_OLD;
		tract[SL_REPLST_OLD] = -1;
		tract[SL_REPLST_ACTIVE] = -1;
		tract[SL_REPLST_TRUNCPNDG] = -1;
		tract[SL_REPLST_GARBAGE] = -1;
		tract[SL_REPLST_GARBAGE_SCHED] = -1;

		for (n = 0; n < USWI_NBMAPS(wk); n++) {
			if (mds_bmap_load(wk->uswi_fcmh, n, &bcm))
				continue;

			BMAP_LOCK(bcm);
			mds_repl_bmap_walk(bcm, tract,
			    NULL, 0, &iosidx, 1);
			mds_repl_bmap_rel(bcm);
		}
 end:
		mds_repl_enqueue_sites(wk, &repl, 1);
		uswi_unref(wk);
	}
	freelock(&upsched_tree_lock);
}

void
mds_repl_init(void)
{
	int rc;

	rc = mdsio_lookup(MDSIO_FID_ROOT, SL_PATH_REPLS, NULL,
	    &mds_repldir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup repldir: %s", slstrerror(rc));

	mds_repl_buildbusytable();
	mds_repl_scandir();
}
