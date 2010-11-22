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

struct psc_listcache	 slm_replst_workq;

struct psc_vbitmap	*repl_busytable;
psc_spinlock_t		 repl_busytable_lock = SPINLOCK_INIT;
int			 repl_busytable_nents;
sl_ino_t		 mds_upschdir_inum;

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

int
_mds_repl_ios_lookup(struct slash_inode_handle *i, sl_ios_id_t ios, int add,
	     int journal)
{
	uint32_t j=0, k;
	int rc = -ENOENT;
	sl_replica_t *repl;

	INOH_LOCK(i);

	/* 
	 * Search the existing replicas to make sure the given ios is not
	 *   already there.
	 */
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
	/*
	 * It does not exist, add the replica to the inode if 'add' was
	 *   specified, else return.
	 */
	if (rc == -ENOENT && add) {
		psc_assert(i->inoh_ino.ino_nrepls == j);

		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
			rc = -ENOSPC;
			goto out;
		}

		if (j >= SL_DEF_REPLICAS) {
			/*
			 * Note that both the inode structure and replication
			 *  table must be synced.
			 */
			psc_assert(i->inoh_extras);
			i->inoh_flags |= INOH_EXTRAS_DIRTY | INOH_INO_DIRTY;
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

#define mds_repl_iosv_lookup(ih, ios, iosidx, nios)			\
	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 0)

#define mds_repl_iosv_lookup_add(ih, ios, iosidx, nios)			\
	_mds_repl_iosv_lookup((ih), (ios), (iosidx), (nios), 1)

__static int
_mds_repl_iosv_lookup(struct slash_inode_handle *ih,
    const sl_replica_t iosv[], int iosidx[], int nios, int add)
{
	int k, last;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(ih,
		    iosv[k].bs_id, add, add)) < 0)
			return (-iosidx[k]);

	qsort(iosidx, nios, sizeof(iosidx[0]), iosidx_cmp);
	/* check for dups */
	last = -1;
	for (k = 0; k < nios; k++, last = iosidx[k])
		if (iosidx[k] == last)
			return (EINVAL);
	return (0);
}

int
_mds_repl_bmap_apply(struct bmapc_memb *bcm, const int *tract,
    const int *retifset, int flags, int off, int *scircuit,
    brepl_walkcb_t cbf, void *cbarg)
{
	struct bmap_mds_info *bmdsi = bmap_2_bmdsi(bcm);
	int val, rc = 0;

	/* Take a write lock on the bmapod. */
	BMAPOD_WRLOCK(bmdsi);

	if (scircuit)
		*scircuit = 0;
	else
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);

	val = SL_REPL_GET_BMAP_IOS_STAT(bcm->bcm_repls, off);

	if (val >= NBREPLST)
		psc_fatalx("corrupt bmap");

	if (cbf)
		cbf(bcm, off / SL_BITS_PER_REPLICA, val, cbarg);

	/* Check for return values */
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
		SL_REPL_SET_BMAP_IOS_STAT(bcm->bcm_repls,
		    off, tract[val]);
		BMDSI_LOGCHG_SET(bcm);
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
 *		tract[BREPLST_INVALID] = BREPLST_VALID
 *
 *	This changes any BREPLST_INVALID states into BREPLST_VALID.
 * @retifset: return the value of the slot in this array corresponding to
 *	the state value as the slot index, if the array value is nonzero;
 *	the last replica always gets priority unless SCIRCUIT is specified.
 * @flags: operational flags.
 * @iosidx: indexes of I/O systems to exclude or query, or NULL for everyone.
 * @nios: # I/O system indexes specified.
 */
int
_mds_repl_bmap_walk(struct bmapc_memb *bcm, const int *tract,
    const int *retifset, int flags, const int *iosidx, int nios,
    brepl_walkcb_t cbf, void *cbarg)
{
	int scircuit, nr, off, k, rc, trc;

	scircuit = rc = 0;
	nr = fcmh_2_inoh(bcm->bcm_fcmh)->inoh_ino.ino_nrepls;

	if (nios == 0)
		/* no one specified; apply to all */
		for (k = 0, off = 0; k < nr;
		    k++, off += SL_BITS_PER_REPLICA) {
			trc = _mds_repl_bmap_apply(bcm, tract,
			    retifset, flags, off, &scircuit, cbf, cbarg);
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
				    retifset, flags, off, &scircuit, cbf,
				    cbarg);
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
			    SL_BITS_PER_REPLICA, &scircuit, cbf,
			    cbarg);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}

	return (rc);
}

/*
 * mds_repl_inv_except - For the given bmap, change the status of
 *	all its replicas marked "valid" to "invalid" except for the
 *	replica specified.
 *
 *	This is a high-level convenience call provided to easily update
 *	status after an ION has received some new I/O, which would make
 *	all other existing copies of the bmap on any other replicas old.
 * @bcm: the bmap.
 * @ios: the ION resource that should stay marked "valid".
 *
 * XXX this should mark others as GARBAGE instead of INVALID to avoid garbage leaks.
 */
int
mds_repl_inv_except(struct bmapc_memb *bcm, sl_ios_id_t ios)
{
	int rc, iosidx, tract[NBREPLST], retifset[NBREPLST];
	struct up_sched_work_item *wk;
	uint32_t policy;

	/*
	 * Find/add our replica's IOS ID but instruct mds_repl_ios_lookup_add()
	 *   not to journal this operation because the inode's repl table
	 *   will be journaled with this bmap's updated repl bitmap.
	 * This saves a journal I/O.
	 */
	iosidx = mds_repl_ios_lookup_add(fcmh_2_inoh(bcm->bcm_fcmh), ios, 1);
	if (iosidx < 0)
		psc_fatalx("lookup ios %d: %s", ios, slstrerror(iosidx));

	BHREPL_POLICY_GET(bcm, policy);

	/* Ensure replica on active IOS is marked valid. */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_VALID;

	brepls_init(retifset, EINVAL);
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;

	rc = mds_repl_bmap_walk(bcm, tract, retifset, 0, &iosidx, 1);
	if (rc)
		psc_error("bcs_repls is marked OLD or SCHED for fid %"PRIx64" "
		    "bmap %d iosidx %d", fcmh_2_fid(bcm->bcm_fcmh),
		    bcm->bcm_bmapno, iosidx);

	/*
	 * Invalidate all other replicas.
	 * Note: if the status is SCHED here, don't do anything; once
	 * the replication status update comes from the ION, we will know
	 * he copied an old bmap and mark it OLD then.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_VALID] = policy == BRP_PERSIST ?
	    BREPLST_REPL_QUEUED : BREPLST_INVALID;

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;

	if (mds_repl_bmap_walk(bcm, tract, retifset, REPL_WALKF_MODOTH,
	    &iosidx, 1))
		BHGEN_INCREMENT(bcm);

	/* Write changes to disk. */
	mds_bmap_repl_update(bcm);

	/*
	 * If this bmap is marked for persistent replication,
	 * the repl request must exist and should be marked such
	 * that the replication monitors do not release it in the
	 * midst of processing it as this activity now means they
	 * have more to do.
	 */
	if (policy == BRP_PERSIST) {
		sl_replica_t repl;

		wk = uswi_find(&bcm->bcm_fcmh->fcmh_fg, NULL);
		repl.bs_id = ios;
		uswi_enqueue_sites(wk, &repl, 1);
		uswi_unref(wk);
	}

	return (0);
}

/*
 * mds_repl_bmap_rel - Release a bmap after use.
 */
void
mds_repl_bmap_rel(struct bmapc_memb *bcm)
{
	mds_bmap_repl_update(bcm);
	bmap_op_done_type(bcm, BMAP_OPCNT_LOOKUP);
}

/* XXX remove this routine */
int
mds_repl_loadino(const struct slash_fidgen *fgp, struct fidc_membh **fp)
{
	struct slash_inode_handle *ih;
	struct fidc_membh *fcmh;
	int rc;

	*fp = NULL;

	rc = slm_fcmh_get(fgp, &fcmh);
	if (rc)
		return (rc);

	if (fcmh_2_nrepls(fcmh) > SL_DEF_REPLICAS) {
		ih = fcmh_2_inoh(fcmh);
		rc = mds_inox_ensure_loaded(ih);
		if (rc)
			psc_fatalx("mds_inox_ensure_loaded: %s", slstrerror(rc));
	}
	*fp = fcmh;

	if (rc)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
}


int
mds_repl_addrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    const sl_replica_t *iosv, int nios)
{
	int tract[NBREPLST], retifset[NBREPLST], retifzero[NBREPLST];
	int iosidx[SL_MAX_REPLICAS], rc;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (EINVAL);

	rc = uswi_findoradd(fgp, &wk);
	if (rc)
		return (rc);

	/* Find/add our replica's IOS ID */
	rc = mds_repl_iosv_lookup_add(USWI_INOH(wk),
	    iosv, iosidx, nios);
	if (rc) {
		uswi_unref(wk);
		return (rc);
	}

	/*
	 * Check inode's bmap state.  INVALID and VALID states become
	 * OLD, signifying that replication needs to happen.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_REPL_QUEUED;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE] = BREPLST_REPL_QUEUED;

	brepls_init(retifzero, 0);
	retifzero[BREPLST_VALID] = 1;

	if (bmapno == (sl_bmapno_t)-1) {
		int repl_some_act = 0, repl_all_act = 1;
		int ret_if_inact[NBREPLST];

		/* check if all bmaps are already old/queued */
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = 1;
		retifset[BREPLST_VALID] = 1;

		/* check if all bmaps are already valid */
		brepls_init(ret_if_inact, 1);
		ret_if_inact[BREPLST_VALID] = 0;

		for (bmapno = 0; bmapno < USWI_NBMAPS(wk); bmapno++) {
			if (mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm))
				continue;

			BMAP_LOCK(bcm);

			/*
			 * If no VALID replicas exist, the bmap must be
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
		brepls_init(retifset, SLERR_REPL_NOT_ACT);
		retifset[BREPLST_INVALID] = 0;
		retifset[BREPLST_REPL_SCHED] = EALREADY;
		retifset[BREPLST_REPL_QUEUED] = EALREADY;
		retifset[BREPLST_VALID] = 0;

		rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
		if (rc == 0) {
			BMAP_LOCK(bcm);

			/*
			 * If no VALID replicas exist, the bmap must be
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
		uswi_enqueue_sites(wk, iosv, nios);
	else if (rc == SLERR_BMAP_ZERO)
		rc = 0;

	uswi_unref(wk);
	return (rc);
}

int
mds_repl_delrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    const sl_replica_t *iosv, int nios)
{
	int rc, tract[NBREPLST], retifset[NBREPLST], iosidx[SL_MAX_REPLICAS];
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

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_INVALID;
	tract[BREPLST_REPL_SCHED] = BREPLST_INVALID;

	if (bmapno == (sl_bmapno_t)-1) {
		brepls_init(retifset, 0);
		retifset[BREPLST_VALID] = 1;
		retifset[BREPLST_REPL_QUEUED] = 1;
		retifset[BREPLST_REPL_SCHED] = 1;

		rc = SLERR_REPLS_ALL_INACT;
		for (bmapno = 0; bmapno < USWI_NBMAPS(wk); bmapno++) {
			if (mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm))
				continue;

			if (mds_repl_bmap_walk(bcm, tract,
			    retifset, 0, iosidx, nios))
				rc = 0;
			mds_repl_bmap_rel(bcm);
		}
	} else if (mds_bmap_exists(wk->uswi_fcmh, bmapno)) {
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = SLERR_REPL_ALREADY_INACT;
		/* XXX BREPLST_TRUNCPNDG] = EINVAL? */
		retifset[BREPLST_GARBAGE] = EINVAL;
		retifset[BREPLST_GARBAGE_SCHED] = EINVAL;

		rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
		if (rc == 0) {
			rc = mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			mds_repl_bmap_rel(bcm);
		}
	} else
		rc = SLERR_BMAP_INVALID;

	uswi_unref(wk);
	return (rc);
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
		locked = RMMI_RLOCK(ma);
		psc_multiwaitcond_wakeup(&ma->rmmi_mwcond);
		RMMI_URLOCK(ma, locked);

		locked = RMMI_RLOCK(mb);
		psc_multiwaitcond_wakeup(&mb->rmmi_mwcond);
		RMMI_URLOCK(mb, locked);
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
	locked2 = RMMI_RLOCK(rmmi);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, n, &s->site_resources)
			DYNARRAY_FOREACH(resm, j, &r->res_members)
				if (resm->resm_pri != rmmi)
					mds_repl_nodes_setbusy(rmmi,
					    resm->resm_pri, 0);
	RMMI_URLOCK(rmmi, locked2);
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
	int tract[NBREPLST], iosidx;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;
	sl_replica_t repl;
	sl_bmapno_t n;

	repl.bs_id = resid;

	PLL_LOCK(&upsched_listhd);
	PLL_FOREACH(wk, &upsched_listhd) {
		USWI_INCREF(wk, USWI_REFT_LOOKUP);
		if (!uswi_access(wk))
			continue;
		PLL_ULOCK(&upsched_listhd);

		iosidx = mds_repl_ios_lookup(USWI_INOH(wk), resid);
		if (iosidx < 0)
			goto end;

		brepls_init(tract, -1);
		tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;

		for (n = 0; n < USWI_NBMAPS(wk); n++) {
			if (mds_bmap_load(wk->uswi_fcmh, n, &bcm))
				continue;

			mds_repl_bmap_walk(bcm, tract,
			    NULL, 0, &iosidx, 1);
			mds_repl_bmap_rel(bcm);
		}
 end:
		uswi_enqueue_sites(wk, &repl, 1);
		PLL_LOCK(&upsched_listhd);
		uswi_unref(wk);
	}
	PLL_ULOCK(&upsched_listhd);
}

void
mds_repl_init(void)
{
	int rc;

	rc = mdsio_lookup(MDSIO_FID_ROOT, SL_PATH_UPSCH,
	    &mds_upschdir_inum, &rootcreds, NULL);
	if (rc)
		psc_fatalx("lookup repldir: %s", slstrerror(rc));

	mds_repl_buildbusytable();
	upsched_scandir();
}
