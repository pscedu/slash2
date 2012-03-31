/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2012, Pittsburgh Supercomputing Center (PSC).
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
 * This ranges from tracking the replication state of each bmap's copy
 * on each ION and managing replication requests and persistent
 * behavior.
 */

#include <sys/param.h>

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "pfl/str.h"
#include "psc_ds/tree.h"
#include "psc_ds/treeutil.h"
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
#include "mdsio.h"
#include "mdslog.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slerr.h"
#include "up_sched_res.h"

struct psc_listcache	 slm_replst_workq;

struct slm_resmlink	*repl_busytable;
int			 repl_busytable_nents;
psc_spinlock_t		 repl_busytable_lock = SPINLOCK_INIT;

__static int
iosidx_cmp(const void *a, const void *b)
{
	const int *x = a, *y = b;

	return (CMP(*x, *y));
}

__static int
iosid_cmp(const void *a, const void *b)
{
	const sl_replica_t *x = a, *y = b;

	return (CMP(x->bs_id, y->bs_id));
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
_mds_repl_ios_lookup(struct slash_inode_handle *ih, sl_ios_id_t ios,
    int add, int log)
{
	int locked, rc = -ENOENT, inox_rc = 0;
	struct sl_resource *res;
	sl_replica_t *repl;
	uint32_t j, k;

	locked = INOH_RLOCK(ih);
	/*
	 * Search the existing replicas to see if the given IOS is
	 * already there.
	 */
	for (j = 0, k = 0, repl = ih->inoh_ino.ino_repls;
	    j < ih->inoh_ino.ino_nrepls; j++, k++) {
		if (j == SL_DEF_REPLICAS) {
			/*
			 * The first few replicas are in the inode
			 * itself, the rest are in the extras block.
			 */
			if ((inox_rc = mds_inox_ensure_loaded(ih)))
				goto out;

			repl = ih->inoh_extras->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_INFO, ih, "rep%u[%u] == %u",
		    k, repl[k].bs_id, ios);

		if (repl[k].bs_id == ios) {
			rc = j;
			goto out;
		}
	}

	res = libsl_id2res(ios);
	if (res == NULL || !RES_ISFS(res))
		PFL_GOTOERR(out, rc = -SLERR_RES_BADTYPE);

	/*
	 * It does not exist; add the replica to the inode if 'add' was
	 *   specified, else return.
	 */
	if (add) {
		psc_assert(ih->inoh_ino.ino_nrepls <= SL_MAX_REPLICAS);
		if (ih->inoh_ino.ino_nrepls == SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, ih, "too many replicas");
			PFL_GOTOERR(out, rc = -ENOSPC);

		} else if (ih->inoh_ino.ino_nrepls >= SL_DEF_REPLICAS) {
			if ((inox_rc = mds_inox_ensure_loaded(ih)))
				goto out;

			repl = ih->inoh_extras->inox_repls;
			k = j - SL_DEF_REPLICAS;

		} else {
			repl = ih->inoh_ino.ino_repls;
			k = j;
		}

		repl[k].bs_id = ios;
		ih->inoh_ino.ino_nrepls++;

		DEBUG_INOH(PLL_INFO, ih, "add IOS(%u) to repls, index %d",
		    ios, j);

		mds_inodes_odsync(ih->inoh_fcmh, mdslog_ino_repls);

		rc = j;
	}
 out:
	INOH_URLOCK(ih, locked);
	return (inox_rc ? inox_rc : rc);
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

void
mds_brepls_check(uint8_t *repls, int nr)
{
	int val, off, i;

	psc_assert(nr > 0 && nr <= SL_MAX_REPLICAS);
	for (i = 0, off = 0; i < nr; i++, off += SL_BITS_PER_REPLICA) {
		val = SL_REPL_GET_BMAP_IOS_STAT(repls, off);
		switch (val) {
		case BREPLST_VALID:
		case BREPLST_GARBAGE:
		case BREPLST_GARBAGE_SCHED:
		case BREPLST_TRUNCPNDG:
		case BREPLST_TRUNCPNDG_SCHED:
			return;
		}
	}
	psc_fatalx("no valid replica states exist");
}

int
_mds_repl_bmap_apply(struct bmapc_memb *bcm, const int *tract,
    const int *retifset, int flags, int off, int *scircuit,
    brepl_walkcb_t cbf, void *cbarg)
{
	struct bmap_mds_info *bmdsi = bmap_2_bmi(bcm);
	int locked, val, rc = 0;

	/* Take a write lock on the bmapod. */
	if (tract)
		locked = BMAPOD_REQWRLOCK(bmdsi);
	else
		locked = BMAPOD_REQRDLOCK(bmdsi);

	if (scircuit)
		*scircuit = 0;
	else
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);

	/* retrieve IOS status given a bit offset into the map */
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
	if (tract && tract[val] != -1)
		SL_REPL_SET_BMAP_IOS_STAT(bcm->bcm_repls,
		    off, tract[val]);

 out:
	BMAPOD_UREQLOCK(bmdsi, locked);
	return (rc);
}

/**
 * mds_repl_bmap_walk - Walk the bmap replication bits, performing any
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
			trc = _mds_repl_bmap_apply(bcm, tract, retifset,
			    flags, off, &scircuit, cbf, cbarg);
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
				    retifset, flags, off, &scircuit,
				    cbf, cbarg);
				if (trc)
					rc = trc;
				if (scircuit)
					break;
			}
	} else
		/* modify only the sites specified */
		for (k = 0; k < nios; k++) {
			trc = _mds_repl_bmap_apply(bcm, tract, retifset,
			    flags, iosidx[k] * SL_BITS_PER_REPLICA,
			    &scircuit, cbf, cbarg);
			if (trc)
				rc = trc;
			if (scircuit)
				break;
		}

	return (rc);
}

struct iosidv {
	sl_replica_t	iosv[SL_MAX_REPLICAS];
	int		nios;
};

void
mds_repl_inv_requeue(struct bmapc_memb *b, int idx, int val, void *arg)
{
	struct iosidv *qv = arg;

	if (val == BREPLST_VALID)
		qv->iosv[qv->nios++].bs_id = fcmh_2_repl(b->bcm_fcmh, idx);
}

/**
 * mds_repl_inv_except - For the given bmap, change the status of
 *	all its replicas marked "valid" to "invalid" except for the
 *	replica specified.
 *
 *	This is a high-level convenience call provided to easily update
 *	status after an ION has received some new I/O, which would make
 *	all other existing copies of the bmap on any other replicas old.
 * @b: the bmap.
 * @iosidx: the index of the only ION resource in the inode replica
 *	table that should be marked "valid".
 *
 * Note: All callers must journal log these bmap replica changes
 *	themselves. In addition, they must log any changes to the inode
 *	_before_ the bmap changes.  Otherwise, we could end up actually
 *	having bmap replicas that are not recognized by the information
 *	stored in the inode during log replay.
 */
int
mds_repl_inv_except(struct bmapc_memb *b, int iosidx)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	struct up_sched_work_item *wk;
	struct iosidv qv;
	uint32_t policy;

	BHREPL_POLICY_GET(b, &policy);

	/* Ensure replica on active IOS is marked valid. */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_VALID;
	tract[BREPLST_GARBAGE] = BREPLST_VALID;

	brepls_init(retifset, EINVAL);
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;

	rc = mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx, 1);
	if (rc)
		psclog_error("bcs_repls is marked OLD or SCHED for "
		    "fid "SLPRI_FID" bmap %d iosidx %d",
		    fcmh_2_fid(b->bcm_fcmh), b->bcm_bmapno, iosidx);

	/*
	 * Invalidate all other replicas.
	 * Note: if the status is SCHED here, don't do anything; once
	 * the replication status update comes from the ION, we will
	 * know he copied an old bmap and mark it OLD then.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_VALID] = policy == BRPOL_PERSIST ?
	    BREPLST_REPL_QUEUED : BREPLST_GARBAGE;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;

	qv.nios = 0;
	if (_mds_repl_bmap_walk(b, tract, retifset, REPL_WALKF_MODOTH,
	    &iosidx, 1, mds_repl_inv_requeue, &qv))
		BHGEN_INCREMENT(b);

	rc = mds_bmap_write(b, 0, NULL, NULL);

	/*
	 * If this bmap is marked for persistent replication, the repl
	 * request must exist and should be marked such that the
	 * replication monitors do not release it in the midst of
	 * processing it as this activity now means they have more to
	 * do.
	 */
	if (policy == BRPOL_PERSIST) {
		wk = uswi_find(&b->bcm_fcmh->fcmh_fg);
		uswi_enqueue_sites(wk, qv.iosv, qv.nios);
		uswi_unref(wk);
	}
	return (rc);
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
			psc_fatalx("mds_inox_ensure_loaded: %s",
			    slstrerror(rc));
	}
	*fp = fcmh;

	if (rc)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (rc);
}

void
slm_iosv_setbusy(sl_replica_t *iosv, int nios)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *r;
	int n;

	/* this must be sorted to avoid deadlocks */
	qsort(iosv, nios, sizeof(iosv[0]), iosid_cmp);
	for (n = 0; n < nios; n++) {
		r = libsl_id2res(iosv[n].bs_id);
		rpmi = res2rpmi(r);
		si = res2iosinfo(r);

		RPMI_LOCK(rpmi);
		while (si->si_flags & SIF_BUSY) {
			psc_waitq_wait(&rpmi->rpmi_waitq,
			    &rpmi->rpmi_lock);
			RPMI_LOCK(rpmi);
		}
		si->si_flags |= SIF_BUSY;
		RPMI_ULOCK(rpmi);
	}
}

void
slm_iosv_clearbusy(const sl_replica_t *iosv, int nios)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *r;
	int n;

	for (n = 0; n < nios; n++) {
		r = libsl_id2res(iosv[n].bs_id);
		rpmi = res2rpmi(r);
		si = res2iosinfo(r);

		RPMI_LOCK(rpmi);
		psc_assert(si->si_flags & SIF_BUSY);
		si->si_flags &= ~SIF_BUSY;
		RPMI_ULOCK(rpmi);
		psc_waitq_wakeall(&rpmi->rpmi_waitq);
	}
}

int
mds_repl_addrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int tract[NBREPLST], retifset[NBREPLST], retifzero[NBREPLST];
	int iosidx[SL_MAX_REPLICAS], rc;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (-EINVAL);

	rc = uswi_findoradd(fgp, &wk);
	if (rc)
		return (rc);

	slm_iosv_setbusy(iosv, nios);

	/* Find/add our replica's IOS ID */
	rc = mds_repl_iosv_lookup_add(USWI_INOH(wk), iosv, iosidx,
	    nios);
	if (rc)
		goto out;

	/*
	 * Check inode's bmap state.  INVALID and VALID states become
	 * OLD, signifying that replication needs to happen.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_REPL_QUEUED;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED; /* XXX check gen */
	tract[BREPLST_GARBAGE] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_REPL_QUEUED;

	brepls_init(retifzero, 0);
	retifzero[BREPLST_VALID] = 1;

	if (bmapno == (sl_bmapno_t)-1) {
		int repl_some_act = 0, repl_all_act = 1;
		int ret_if_inact[NBREPLST];

		/* check if all bmaps are already old/queued */
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = 1;
		retifset[BREPLST_VALID] = 1;
		retifset[BREPLST_GARBAGE] = 1;
		retifset[BREPLST_GARBAGE_SCHED] = 1;

		/* check if all bmaps are already valid */
		brepls_init(ret_if_inact, 1);
		ret_if_inact[BREPLST_VALID] = 0;

		for (bmapno = 0; bmapno <
		    fcmh_nvalidbmaps(wk->uswi_fcmh); bmapno++) {
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
			if (repl_all_act && mds_repl_bmap_walk(bcm,
			    NULL, ret_if_inact, REPL_WALKF_SCIRCUIT,
			    iosidx, nios))
				repl_all_act = 0;
			mds_bmap_write_repls_rel(bcm);
		}
		if (bmapno && repl_some_act == 0)
			rc = EALREADY;
		else if (bmapno && repl_all_act)
			rc = SLERR_REPL_ALREADY_ACT;
	} else if (mds_bmap_exists(wk->uswi_fcmh, bmapno)) {
		/*
		 * If this bmap is already being replicated, return
		 * EALREADY.
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
				mds_bmap_write_repls_rel(bcm);
			}
		}
	} else
		rc = SLERR_BMAP_INVALID;

	if (rc == 0)
		uswi_enqueue_sites(wk, iosv, nios);
	else if (rc == SLERR_BMAP_ZERO)
		rc = 0;

 out:
	uswi_unref(wk);
	slm_iosv_clearbusy(iosv, nios);
	return (rc);
}

int
mds_repl_delrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int rc, tract[NBREPLST], retifset[NBREPLST], iosidx[SL_MAX_REPLICAS];
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (-EINVAL);

	rc = uswi_findoradd(fgp, &wk);
	if (rc)
		return (rc);

	slm_iosv_setbusy(iosv, nios);

	/* Find replica IOS indexes */
	rc = mds_repl_iosv_lookup(USWI_INOH(wk), iosv, iosidx, nios);
	if (rc)
		goto out;

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE;
	tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_VALID] = BREPLST_GARBAGE;

	if (bmapno == (sl_bmapno_t)-1) {
		brepls_init(retifset, 0);
		retifset[BREPLST_VALID] = 1;
		retifset[BREPLST_REPL_QUEUED] = 1;
		retifset[BREPLST_REPL_SCHED] = 1;

		rc = SLERR_REPLS_ALL_INACT;
		for (bmapno = 0; bmapno <
		    fcmh_nvalidbmaps(wk->uswi_fcmh); bmapno++) {
			if (mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm))
				continue;

			if (mds_repl_bmap_walk(bcm, tract,
			    retifset, 0, iosidx, nios))
				rc = 0;
			mds_bmap_write_repls_rel(bcm);
		}
	} else if (mds_bmap_exists(wk->uswi_fcmh, bmapno)) {
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = SLERR_REPL_ALREADY_INACT;
		/* XXX BREPLST_TRUNCPNDG -> EINVAL? */
		retifset[BREPLST_GARBAGE] = EINVAL;
		retifset[BREPLST_GARBAGE_SCHED] = EINVAL;

		rc = mds_bmap_load(wk->uswi_fcmh, bmapno, &bcm);
		if (rc == 0) {
			rc = mds_repl_bmap_walk(bcm,
			    tract, retifset, 0, iosidx, nios);
			mds_bmap_write_repls_rel(bcm);
		}
	} else
		rc = SLERR_BMAP_INVALID;

	uswi_enqueue_sites(wk, iosv, nios);

 out:
	uswi_unref(wk);
	slm_iosv_clearbusy(iosv, nios);
	return (rc);
}

/**
 * mds_repl_nodes_adjbusy - Adjust the bandwidth estimate between two
 *	IONs.
 * @ma: resm #1.
 * @mb: resm #2.
 * @amt: adjustment amount.
 * Returns: if @amt is positive, return value is the amount that has
 *	been reserved or zero if none could be allocated.
 */
int64_t
mds_repl_nodes_adjbusy(struct resm_mds_info *ma,
    struct resm_mds_info *mb, int64_t amt)
{
	int wake = 0, minid, maxid, locked;
	struct slm_resmlink *srl;

	psc_assert(ma->rmmi_busyid != mb->rmmi_busyid);
	minid = MIN(ma->rmmi_busyid, mb->rmmi_busyid);
	maxid = MAX(ma->rmmi_busyid, mb->rmmi_busyid);

	locked = reqlock(&repl_busytable_lock);
	srl = repl_busytable + MDS_REPL_BUSYNODES(minid, maxid);
	if (srl->srl_used + amt > srl->srl_avail) {
		amt = srl->srl_avail - srl->srl_used;
		srl->srl_used = srl->srl_avail;
	} else {
		srl->srl_used += amt;
		if (srl->srl_used < 0) {
			srl->srl_used = 0;
			wake = 1;
		}
		amt = srl->srl_used;
	}
	ureqlock(&repl_busytable_lock, locked);

	/*
	 * If we reset the amount, alert anyone waiting to utilize the
	 * new connection slots.
	 */
	if (wake) {
		locked = RMMI_RLOCK(ma);
		psc_multiwaitcond_wakeup(&ma->rmmi_mwcond);
		RMMI_URLOCK(ma, locked);

		locked = RMMI_RLOCK(mb);
		psc_multiwaitcond_wakeup(&mb->rmmi_mwcond);
		RMMI_URLOCK(mb, locked);
	}
	return (amt);
}

void
mds_repl_node_clearallbusy(struct resm_mds_info *rmmi)
{
	int n, j, locked[3], dummy;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;

	PLL_TRYRLOCK(&globalConfig.gconf_sites, locked);
	tryreqlock(&repl_busytable_lock, locked + 1);
	RMMI_TRYRLOCK(rmmi, locked + 2);

	if (0) {
 retry:
		if (psc_spin_haslock(&repl_busytable_lock))
			freelock(&repl_busytable_lock);
		if (PLL_HASLOCK(&globalConfig.gconf_sites))
			PLL_ULOCK(&globalConfig.gconf_sites);
	}

	if (!PLL_TRYRLOCK(&globalConfig.gconf_sites, &dummy))
		goto retry;
	if (!tryreqlock(&repl_busytable_lock, &dummy))
		goto retry;
	if (!RMMI_TRYRLOCK(rmmi, &dummy))
		goto retry;

	CONF_FOREACH_RESM(s, r, n, resm, j) {
		if (!RES_ISFS(r))
			continue;
		if (resm2rmmi(resm) != rmmi)
			mds_repl_nodes_clearbusy(rmmi,
			    resm2rmmi(resm));
	}
	RMMI_URLOCK(rmmi, locked[2]);
	ureqlock(&repl_busytable_lock, locked[1]);
	PLL_URLOCK(&globalConfig.gconf_sites, locked[0]);
}

void
mds_repl_buildbusytable(void)
{
	struct resm_mds_info *rmmi;
	struct slm_resmlink *srl;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int n, j;

	/* count # resm's (IONs) and assign each a busy identifier */
	PLL_LOCK(&globalConfig.gconf_sites);
	spinlock(&repl_busytable_lock);
	repl_busytable_nents = 0;
	CONF_FOREACH_SITE(s)
		DYNARRAY_FOREACH(r, n, &s->site_resources) {
			if (!RES_ISFS(r))
				continue;
			DYNARRAY_FOREACH(resm, j, &r->res_members) {
				rmmi = resm2rmmi(resm);
				rmmi->rmmi_busyid = repl_busytable_nents++;
			}
		}
	PLL_ULOCK(&globalConfig.gconf_sites);

	if (repl_busytable)
		PSCFREE(repl_busytable);
	repl_busytable = psc_calloc(sizeof(*repl_busytable),
	    repl_busytable_nents * (repl_busytable_nents + 1) / 2, 0);
	for (n = 0; n < repl_busytable_nents; n++)
		for (j = n + 1; j < repl_busytable_nents; j++) {
			srl = repl_busytable + MDS_REPL_BUSYNODES(n, j);
			srl->srl_avail = SLM_RESMLINK_DEF_BANDWIDTH;
		}
	freelock(&repl_busytable_lock);
}

void
mds_repl_reset_scheduled(sl_ios_id_t resid)
{
	int tract[NBREPLST], retifset[NBREPLST], iosidx;
	struct up_sched_work_item *wk;
	struct bmapc_memb *bcm;
	sl_replica_t repl;
	sl_bmapno_t n;

	repl.bs_id = resid;

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_TRUNCPNDG_SCHED] = BREPLST_TRUNCPNDG;

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;

	slm_iosv_setbusy(&repl, 1);

	PLL_LOCK(&upsched_listhd);
	PLL_FOREACH(wk, &upsched_listhd) {
		USWI_INCREF(wk, USWI_REFT_LOOKUP);
		if (!uswi_access(wk))
			continue;
		PLL_ULOCK(&upsched_listhd);

		iosidx = mds_repl_ios_lookup(USWI_INOH(wk), resid);
		if (iosidx < 0)
			goto end;

		for (n = 0; n < fcmh_nvalidbmaps(wk->uswi_fcmh); n++) {
			if (mds_bmap_load(wk->uswi_fcmh, n, &bcm))
				continue;

			if (mds_repl_bmap_walk(bcm, tract, retifset, 0,
			    &iosidx, 1))
				mds_bmap_write_repls_rel(bcm);
			else
				bmap_op_done(bcm);
		}
 end:
		uswi_enqueue_sites(wk, &repl, 1);
		PLL_LOCK(&upsched_listhd);
		uswi_unref(wk);
	}
	PLL_ULOCK(&upsched_listhd);
	slm_iosv_clearbusy(&repl, 1);
}

void
mds_repl_init(void)
{
	mds_repl_buildbusytable();
	upsched_scandir();
}
