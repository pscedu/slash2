/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
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

#include "pfl/alloc.h"
#include "pfl/crc.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/pool.h"
#include "pfl/pthrutil.h"
#include "pfl/str.h"
#include "pfl/tree.h"
#include "pfl/treeutil.h"
#include "pfl/waitq.h"

#include "bmap_mds.h"
#include "fid.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "odtable_mds.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slconn.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

struct slm_resmlink	*repl_busytable;
int			 repl_busytable_nents;
psc_spinlock_t		 repl_busytable_lock = SPINLOCK_INIT;
struct sl_mds_iosinfo	 slm_null_si;

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

void
_slm_repl_bmap_rel_type(struct bmapc_memb *b, int type)
{
	if (BMAPOD_HASWRLOCK(bmap_2_bmi(b)) &&
	    !(b->bcm_flags & BMAP_MDS_REPLMODWR)) {
		/* we took a write lock but did not modify; undo */
		BMAPOD_MODIFY_DONE(b, 0);
		BMAP_UNBUSY(b);
		FCMH_UNBUSY(b->bcm_fcmh);
	}
	bmap_op_done_type(b, type);
}

int
_mds_repl_ios_lookup(int vfsid, struct slash_inode_handle *ih,
    sl_ios_id_t ios, int add)
{
	int locked, rc = -SLERR_REPL_NOT_ACT, inox_rc = 0;
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
			inox_rc = mds_inox_ensure_loaded(ih);
			if (inox_rc)
				goto out;

			repl = ih->inoh_extras->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_DEBUG, ih, "is rep[%u](=%u) == %u ?",
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
		int waslk, wasbusy;

		psc_assert(ih->inoh_ino.ino_nrepls <= SL_MAX_REPLICAS);
		if (ih->inoh_ino.ino_nrepls == SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, ih, "too many replicas");
			PFL_GOTOERR(out, rc = -ENOSPC);

		} else if (ih->inoh_ino.ino_nrepls >= SL_DEF_REPLICAS) {
			inox_rc = mds_inox_ensure_loaded(ih);
			if (inox_rc)
				goto out;

			repl = ih->inoh_extras->inox_repls;
			k = j - SL_DEF_REPLICAS;

		} else {
			repl = ih->inoh_ino.ino_repls;
			k = j;
		}

		wasbusy = FCMH_REQ_BUSY(inoh_2_fcmh(ih), &waslk);

		repl[k].bs_id = ios;
		ih->inoh_ino.ino_nrepls++;

		DEBUG_INOH(PLL_INFO, ih, "add IOS(%u) to repls, index %d",
		    ios, j);

		mds_inodes_odsync(vfsid, inoh_2_fcmh(ih),
		    mdslog_ino_repls);

		FCMH_UREQ_BUSY(inoh_2_fcmh(ih), wasbusy, waslk);

		rc = j;
	}
 out:
	INOH_URLOCK(ih, locked);
	return (inox_rc ? inox_rc : rc);
}

int
_mds_repl_iosv_lookup(int vfsid, struct slash_inode_handle *ih,
    const sl_replica_t iosv[], int iosidx[], int nios, int add)
{
	int k, last;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(vfsid, ih,
		    iosv[k].bs_id, add)) < 0)
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

/**
 * mds_repl_bmap_apply - Apply a translation matrix of residency states
 *	to a bmap.
 * @b: bmap.
 * @tract: translation actions, indexed by current bmap state with
 *	corresponding values to the new state that should be assigned.
 *	For example, index BREPLST_VALID in the array with the value
 *	BREPLST_INVALID would render a VALID state to an INVALID.
 * @retifset: return value, indexed in the same manner as @tract.
 * @flags: behavioral flags.
 * @off: offset int bmap residency table for IOS intended to be
 *	changed/queried.
 * @scircuit: value-result for batch operations.
 * @cbf: callback routine for more detailed processing.
 * @cbarg: argument to callback.
 *
 * Notes: the locks are acquired in the following order:
 *	(*) FCMH_BUSY
 *	(*) BMAP_BUSY
 *	(*) BMAP_WRLOCK
 */
int
_mds_repl_bmap_apply(struct bmapc_memb *b, const int *tract,
    const int *retifset, int flags, int off, int *scircuit,
    brepl_walkcb_t cbf, void *cbarg)
{
	int locked = 0, unlock = 0, relock = 0, val, rc = 0, dummy;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct fidc_membh *f = b->bcm_fcmh;

	if (tract) {
		if (BMAPOD_HASWRLOCK(bmi))
			FCMH_BUSY_ENSURE(f);

		if (FCMH_HAS_BUSY(f)) {
			if (FCMH_HAS_LOCK(f))
				FCMH_ULOCK(f);
		} else {
			if (BMAP_HASLOCK(b)) {
				locked = 1;
				BMAP_ULOCK(b);
			}
			(void)FCMH_REQ_BUSY(f, &dummy);
			FCMH_ULOCK(f);
			if (locked)
				BMAP_LOCK(b);
		}

		if (BMAPOD_HASWRLOCK(bmi)) {
			BMAP_LOCK(b);
			BMAP_BUSY_ENSURE(b);
			psc_assert((b->bcm_flags &
			    BMAP_MDS_REPLMODWR) == 0);
			BMAP_ULOCK(b);
		} else {
			BMAP_WAIT_BUSY(b);
			psc_assert((b->bcm_flags &
			    BMAP_MDS_REPLMODWR) == 0);
			BMAP_ULOCK(b);
			BMAPOD_MODIFY_START(b);
			memcpy(bmi->bmi_orepls, b->bcm_repls,
			    sizeof(bmi->bmi_orepls));
		}
	} else if (!BMAPOD_HASWRLOCK(bmi) && !BMAPOD_HASRDLOCK(bmi)) {
		relock = BMAP_HASLOCK(b);
		BMAP_WAIT_BUSY(b);
		BMAPOD_RDLOCK(bmi);
		BMAP_UNBUSY(b);
		unlock = 1;
	}

	if (scircuit)
		*scircuit = 0;
	else
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);

	/* retrieve IOS status given a bit offset into the map */
	val = SL_REPL_GET_BMAP_IOS_STAT(b->bcm_repls, off);

	if (val >= NBREPLST)
		psc_fatalx("corrupt bmap");

	if (cbf)
		cbf(b, off / SL_BITS_PER_REPLICA, val, cbarg);

	/* check for & apply return values */
	if (retifset && retifset[val]) {
		rc = retifset[val];
		if (flags & REPL_WALKF_SCIRCUIT) {
			*scircuit = 1;
			goto out;
		}
	}

	/* apply any translations */
	if (tract && tract[val] != -1) {
		DEBUG_BMAPOD(PLL_DIAG, b, "before modification");
		SL_REPL_SET_BMAP_IOS_STAT(b->bcm_repls, off,
		    tract[val]);
		DEBUG_BMAPOD(PLL_DIAG, b, "after modification");
	}

 out:
	if (unlock)
		BMAPOD_ULOCK(bmi);
	if (relock)
		BMAP_LOCK(b);
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
_mds_repl_bmap_walk(struct bmapc_memb *b, const int *tract,
    const int *retifset, int flags, const int *iosidx, int nios,
    brepl_walkcb_t cbf, void *cbarg)
{
	int scircuit, nr, off, k, rc, trc;

	scircuit = rc = 0;
	nr = fcmh_2_inoh(b->bcm_fcmh)->inoh_ino.ino_nrepls;

	if (nios == 0)
		/* no one specified; apply to all */
		for (k = 0, off = 0; k < nr;
		    k++, off += SL_BITS_PER_REPLICA) {
			trc = _mds_repl_bmap_apply(b, tract, retifset,
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
				trc = _mds_repl_bmap_apply(b, tract,
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
			trc = _mds_repl_bmap_apply(b, tract, retifset,
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
		qv->iosv[qv->nios++].bs_id = fcmh_2_repl(b->bcm_fcmh,
		    idx);
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
_mds_repl_inv_except(struct bmapc_memb *b, int iosidx, int defer)
{
	int rc, tract[NBREPLST], retifset[NBREPLST];
	struct iosidv qv;
	uint32_t policy;

	/* Ensure replica on active IOS is marked valid. */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_VALID;
	tract[BREPLST_GARBAGE] = BREPLST_VALID;

	brepls_init_idx(retifset);
	retifset[BREPLST_INVALID] = 0;
	retifset[BREPLST_VALID] = 0;

	/*
	 * XXX on full truncate, the metafile will exist, which means
	 * the bmap states will exist, which means a new IOS will be
	 * selected which will probably be GARBAGE after truncate
	 * happens a few times.
	 */
	rc = mds_repl_bmap_walk(b, tract, retifset, 0, &iosidx, 1);
	if (rc)
		psclog_errorx("bcs_repls has active IOS marked in a "
		    "weird state while invalidating other replicas; "
		    "fid="SLPRI_FID" bmap=%d iosidx=%d state=%d",
		    fcmh_2_fid(b->bcm_fcmh), b->bcm_bmapno, iosidx, rc);

	BHREPL_POLICY_GET(b, &policy);

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

	if (defer)
		return (rc);

	rc = mds_bmap_write(b, 0, NULL, NULL);

	/*
	 * If this bmap is marked for persistent replication, the repl
	 * request must exist and should be marked such that the
	 * replication monitors do not release it in the midst of
	 * processing it as this activity now means they have more to
	 * do.
	 */
	if (policy == BRPOL_PERSIST)
		upsch_enqueue(&bmap_2_bmi(b)->bmi_upd);
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
			psc_waitq_wait_mutex(&rpmi->rpmi_waitq,
			    &rpmi->rpmi_mutex);
			RPMI_LOCK(rpmi);
		}
		si->si_flags |= SIF_BUSY;
		si->si_owner = pthread_self();
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
		si->si_owner = 0;
		RPMI_ULOCK(rpmi);
		psc_waitq_wakeall(&rpmi->rpmi_waitq);
	}
}

#define PUSH_IOS(b, a, id)						\
	do {								\
		(a)->iosv[(a)->nios++].bs_id = (id);			\
		DEBUG_BMAP(PLL_DEBUG, (b),				\
		    "registering resid %d with %s", (id), #a);		\
	} while (0)

void
slm_repl_upd_write(struct bmapc_memb *b)
{
	struct {
		sl_replica_t	iosv[SL_MAX_REPLICAS];
		unsigned	nios;
	} add, del, sch, deq;
	int locked, off, vold, vnew;
	struct slm_update_data *upd;
	struct sl_mds_iosinfo *si;
	struct bmap_mds_info *bmi;
	struct fidc_membh *f;
	struct sl_resource *r;
	sl_ios_id_t resid;
	pthread_t pthr;
	unsigned n;

	bmi = bmap_2_bmi(b);
	upd = &bmi->bmi_upd;
	f = b->bcm_fcmh;

	/* Transfer ownership to us. */
	pthr = pthread_self();
	f->fcmh_owner = pthr;
	b->bcm_owner = pthr;

	locked = BMAPOD_READ_START(b);

	add.nios = 0;
	del.nios = 0;
	sch.nios = 0;
	deq.nios = 0;
	for (n = 0, off = 0; n < fcmh_2_nrepls(f);
	    n++, off += SL_BITS_PER_REPLICA) {
		resid = fcmh_2_repl(f, n);
		vold = SL_REPL_GET_BMAP_IOS_STAT(
		    bmi->bmi_orepls, off);
		vnew = SL_REPL_GET_BMAP_IOS_STAT(
		    b->bcm_repls, off);

		r = libsl_id2res(resid);
		si = r ? res2iosinfo(r) : &slm_null_si;

		if (vold == vnew)
			;
		else if (vnew == BREPLST_REPL_QUEUED ||
		    (vnew == BREPLST_GARBAGE &&
		     (si->si_flags & SIF_PRECLAIM_NOTSUP) == 0))
			PUSH_IOS(b, &add, resid);
		else if ((vold == BREPLST_REPL_QUEUED ||
		     vold == BREPLST_REPL_SCHED ||
		     vold == BREPLST_TRUNCPNDG_SCHED ||
		     vold == BREPLST_TRUNCPNDG ||
		     vold == BREPLST_GARBAGE_SCHED ||
		     vold == BREPLST_VALID) &&
		    (vnew == BREPLST_GARBAGE ||
		     vnew == BREPLST_VALID ||
		     vnew == BREPLST_INVALID))
			PUSH_IOS(b, &del, resid);
		else if ((vold == BREPLST_REPL_SCHED &&
		     vnew != BREPLST_REPL_SCHED) ||
		    (vold == BREPLST_TRUNCPNDG_SCHED &&
		     vnew != BREPLST_TRUNCPNDG_SCHED))
			PUSH_IOS(b, &deq, resid);
		else if ((vold != BREPLST_REPL_QUEUED &&
		      vnew == BREPLST_REPL_QUEUED) ||
		     (vold != BREPLST_TRUNCPNDG &&
		      vnew == BREPLST_TRUNCPNDG))
			PUSH_IOS(b, &sch, resid);
	}

	if (add.nios)
		for (n = 0; n < add.nios; n++)
			slm_upsch_insert(b, add.iosv[n].bs_id);
	if (deq.nios)
		for (n = 0; n < deq.nios; n++)
			dbdo(NULL, NULL,
			    " UPDATE	upsch"
			    " SET	status = ?"
			    " WHERE	resid = ?"
			    "	AND	fid = ?"
			    "	AND	bno = ?",
			    SQLITE3_TEXT, "Q",
			    SQLITE_INTEGER, deq.iosv[n].bs_id,
			    SQLITE_INTEGER64, bmap_2_fid(b),
			    SQLITE_INTEGER, b->bcm_bmapno);
	if (sch.nios)
		for (n = 0; n < sch.nios; n++)
			dbdo(NULL, NULL,
			    " UPDATE	upsch"
			    " SET	status = ?"
			    " WHERE	resid = ?"
			    "	AND	fid = ?"
			    "	AND	bno = ?",
			    SQLITE3_TEXT, "S",
			    SQLITE_INTEGER, sch.iosv[n].bs_id,
			    SQLITE_INTEGER64, bmap_2_fid(b),
			    SQLITE_INTEGER, b->bcm_bmapno);
	if (del.nios)
		for (n = 0; n < del.nios; n++)
			dbdo(NULL, NULL,
			    " DELETE FROM upsch"
			    " WHERE	resid = ?"
			    "   AND	fid = ?"
			    "   AND	bno = ?",
			    SQLITE_INTEGER, del.iosv[n].bs_id,
			    SQLITE_INTEGER64, bmap_2_fid(b),
			    SQLITE_INTEGER, b->bcm_bmapno);
	BMAPOD_READ_DONE(b, locked);

	FCMH_UNBUSY(b->bcm_fcmh);

	BMAP_LOCK(b);
	b->bcm_flags &= ~BMAP_MDS_REPLMODWR;
	BMAP_UNBUSY(b);

	UPD_UNBUSY(upd);

	bmap_op_done_type(b, BMAP_OPCNT_WORK);
}

int
mds_repl_addrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int tract[NBREPLST], retifset[NBREPLST], retifzero[NBREPLST];
	int iosidx[SL_MAX_REPLICAS], rc, i;
	struct fidc_membh *f;
	struct bmapc_memb *b;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (-EINVAL);

	rc = slm_fcmh_get(fgp, &f);
	if (rc)
		return (-rc);

	slm_iosv_setbusy(iosv, nios);

	/* Find/add our replica's IOS ID */
	rc = -mds_repl_iosv_lookup_add(current_vfsid, fcmh_2_inoh(f),
	    iosv, iosidx, nios);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fcmh_isdir(f)) 
		PFL_GOTOERR(out, 0);

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

#define F_LOG		(1 << 0)
#define F_REPORT	(1 << 1)
#define F_ALREADY	(1 << 2)

		/* check if all bmaps are already old/queued */
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = F_LOG | F_REPORT;
		retifset[BREPLST_REPL_SCHED] = F_LOG;
		retifset[BREPLST_VALID] = F_REPORT;
		retifset[BREPLST_GARBAGE] = F_LOG | F_REPORT;
		retifset[BREPLST_GARBAGE_SCHED] = F_LOG | F_REPORT;

		/* check if all bmaps are already valid */
		brepls_init(ret_if_inact, 1);
		ret_if_inact[BREPLST_VALID] = 0;

		for (bmapno = 0; bmapno < fcmh_nvalidbmaps(f);
		    bmapno++) {
			if (bmap_get(f, bmapno, SL_WRITE, &b))
				continue;

			BMAP_LOCK(b);

			/*
			 * If no VALID replicas exist, the bmap must be
			 * uninitialized/all zeroes.  Skip it.
			 */
			if (mds_repl_bmap_walk_all(b, NULL, retifzero,
			    REPL_WALKF_SCIRCUIT) == 0) {
				bmap_op_done(b);
				continue;
			}

			i = mds_repl_bmap_walk(b, tract, retifset, 0,
			    iosidx, nios);
			if (i & F_REPORT)
				repl_some_act |= 1;
			if (repl_all_act && mds_repl_bmap_walk(b, NULL,
			    ret_if_inact, REPL_WALKF_SCIRCUIT, iosidx,
			    nios))
				repl_all_act = 0;
			if (i & F_LOG) {
				struct slm_update_data *upd;

				upd = &bmap_2_bmi(b)->bmi_upd;
				if (pfl_memchk(upd, 0,
				    sizeof(*upd)) == 1)
					upd_init(upd, UPDT_BMAP);
				mds_bmap_write_logrepls(b);
			}
			slm_repl_bmap_rel(b);
		}
		if (bmapno && repl_some_act == 0)
			rc = -PFLERR_ALREADY;
		else if (bmapno && repl_all_act)
			rc = -SLERR_REPL_ALREADY_ACT;
	} else if (mds_bmap_exists(f, bmapno)) {
		brepls_init(retifset, 0);
		retifset[BREPLST_INVALID] = F_LOG;
		retifset[BREPLST_REPL_SCHED] = F_LOG;
		retifset[BREPLST_REPL_QUEUED] = F_ALREADY;
		retifset[BREPLST_VALID] = F_ALREADY;
		retifset[BREPLST_GARBAGE] = F_LOG;
		retifset[BREPLST_GARBAGE_SCHED] = F_LOG;

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc == 0) {
			BMAP_LOCK(b);

			/*
			 * If no VALID replicas exist, the bmap must be
			 * uninitialized/all zeroes.  Skip it.
			 */
			if (mds_repl_bmap_walk_all(b, NULL, retifzero,
			    REPL_WALKF_SCIRCUIT) == 0) {
				bmap_op_done(b);
				rc = -SLERR_BMAP_ZERO;
			} else {
				rc = mds_repl_bmap_walk(b, tract,
				    retifset, 0, iosidx, nios);
				if (rc & F_LOG) {
					struct slm_update_data *upd;

					upd = &bmap_2_bmi(b)->bmi_upd;
					if (pfl_memchk(upd, 0,
					    sizeof(*upd)) == 1)
						upd_init(upd,
						    UPDT_BMAP);
					else {
						UPD_WAIT(upd);
						upd->upd_flags |= UPDF_BUSY;
						upd->upd_owner = pthread_self();
						UPD_ULOCK(upd);
					}
					mds_bmap_write_logrepls(b);
					UPD_UNBUSY(upd);
					rc = 0;
				} else if (rc & F_ALREADY)
					rc = -PFLERR_ALREADY;
				else
					rc = -SLERR_REPL_NOT_ACT;
				slm_repl_bmap_rel(b);
			}
		}
	} else
		rc = -SLERR_BMAP_INVALID;

	if (rc == -SLERR_BMAP_ZERO)
		rc = 0;

 out:
	if (f)
		fcmh_op_done(f);
	slm_iosv_clearbusy(iosv, nios);
	return (rc);
}

struct slm_repl_valid {
	int  n;
	int  nios;
	int *idx;
};

/**
 * slm_repl_countvalid_cb - Count the number of replicas that would
 *	exist after a potential DELRQ operation, to ensure the last
 *	replicas aren't removed.
 */
void
slm_repl_countvalid_cb(__unusedx struct bmapc_memb *b, int iosidx,
    int val, void *arg)
{
	struct slm_repl_valid *t = arg;
	int j;

	/* If the state isn't VALID, nothing to count. */
	if (val != BREPLST_VALID)
		return;

	/*
	 * If we find an IOS that was specified, we can't factor it into
	 * our count since it won't be here much longer.
	 */
	for (j = 0; j < t->nios; j++)
		if (iosidx == t->idx[j])
			return;
	t->n++;
}

int
mds_repl_delrq(const struct slash_fidgen *fgp, sl_bmapno_t bmapno,
    sl_replica_t *iosv, int nios)
{
	int rc, empty_tract[NBREPLST], tract[NBREPLST],
	    retifset[NBREPLST], iosidx[SL_MAX_REPLICAS];
	struct slm_repl_valid replv;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b;

	if (nios < 1 || nios > SL_MAX_REPLICAS)
		return (-EINVAL);

	rc = slm_fcmh_get(fgp, &f);
	if (rc)
		return (-rc);

	slm_iosv_setbusy(iosv, nios);

	/* Find replica IOS indexes */
	rc = -mds_repl_iosv_lookup(current_vfsid, fcmh_2_inoh(f), iosv,
	    iosidx, nios);
	if (rc)
		PFL_GOTOERR(out, rc);

	replv.nios = nios;
	replv.idx = iosidx;

	brepls_init(empty_tract, -1);

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE;
	tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_VALID] = BREPLST_GARBAGE;

	if (bmapno == (sl_bmapno_t)-1) {
		sl_bmapno_t all_invalid = 0;

		brepls_init(retifset, 0);
		retifset[BREPLST_VALID] = 1;
		retifset[BREPLST_REPL_QUEUED] = 1;
		retifset[BREPLST_REPL_SCHED] = 1;

		rc = -SLERR_REPL_NOT_ACT;
		for (bmapno = 0; bmapno < fcmh_nvalidbmaps(f);
		    bmapno++) {
			if (bmap_get(f, bmapno, SL_WRITE, &b))
				continue;

			replv.n = 0;
			mds_repl_bmap_walkcb(b, empty_tract, NULL, 0,
			    slm_repl_countvalid_cb, &replv);
			if (replv.n > 0) {
				if (mds_repl_bmap_walk(b, tract,
				    retifset, 0, iosidx, nios)) {
					mds_bmap_write_logrepls(b);
					rc = 0;
				}
			} else
				all_invalid++;
			slm_repl_bmap_rel(b);
		}
		if (all_invalid == bmapno)
			rc = -SLERR_LASTREPL;
	} else if (mds_bmap_exists(f, bmapno)) {
		brepls_init(retifset, 0);
		/* XXX BREPLST_TRUNCPNDG : F_REPORT ? */
		retifset[BREPLST_GARBAGE] = F_REPORT;
		retifset[BREPLST_GARBAGE_SCHED] = F_REPORT;
		retifset[BREPLST_REPL_QUEUED] = F_LOG;
		retifset[BREPLST_REPL_SCHED] = F_LOG;
		retifset[BREPLST_VALID] = F_LOG;

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc == 0) {
			replv.n = 0;
			mds_repl_bmap_walkcb(b, empty_tract, NULL, 0,
			    slm_repl_countvalid_cb, &replv);
			if (replv.n > 0) {
				rc = mds_repl_bmap_walk(b, tract,
				    retifset, 0, iosidx, nios);
				if (rc & F_LOG) {
					mds_bmap_write_logrepls(b);
					rc = 0;
				} else if (rc & F_REPORT)
					rc = -EINVAL;
				else
					rc = -SLERR_REPL_NOT_ACT;
			} else
				rc = -SLERR_LASTREPL;
			slm_repl_bmap_rel(b);
		}
	} else
		rc = -SLERR_BMAP_INVALID;

 out:
	if (f)
		fcmh_op_done(f);
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
mds_repl_nodes_adjbusy(struct sl_resm *rma, struct sl_resm *rmb,
    int64_t amt)
{
	int wake = 0, minid, maxid, locked;
	struct resm_mds_info *ma, *mb;
	struct slm_resmlink *srl;

	ma = resm2rmmi(rma);
	mb = resm2rmmi(rmb);

	psc_assert(ma->rmmi_busyid != mb->rmmi_busyid);
	minid = MIN(ma->rmmi_busyid, mb->rmmi_busyid);
	maxid = MAX(ma->rmmi_busyid, mb->rmmi_busyid);

	locked = reqlock(&repl_busytable_lock);
	srl = repl_busytable + MDS_REPL_BUSYNODES(minid, maxid);
/*
	if (psc_dynarray_len(&a->rpmi_upschq) == 0 &&
	    psc_dynarray_len(&b->rpmi_upschq) == 0 && srl->srl_used)
		srl->srl_used = 0;
*/
	if (srl->srl_used + amt >= srl->srl_avail) {
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
		CSVC_WAKE(rma->resm_csvc);
		CSVC_WAKE(rmb->resm_csvc);
	}
	return (amt);
}

void
mds_repl_node_clearallbusy(struct sl_resm *m)
{
	int n, j, locked[2], dummy;
	struct resm_mds_info *rmmi;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;

	rmmi = resm2rmmi(m);
	(void)CONF_TRYRLOCK(locked);
	(void)tryreqlock(&repl_busytable_lock, locked + 1);

	if (0) {
 retry:
		if (CONF_HASLOCK())
			CONF_ULOCK();
	}

	if (!CONF_TRYRLOCK(&dummy))
		goto retry;
	if (!tryreqlock(&repl_busytable_lock, &dummy))
		goto retry;

	CONF_FOREACH_RESM(s, r, n, resm, j) {
		if (!RES_ISFS(r) || resm->resm_csvc == NULL)
			continue;
		if (resm2rmmi(resm) != rmmi)
			mds_repl_nodes_clearbusy(m, resm);
	}
	ureqlock(&repl_busytable_lock, locked[1]);
	CONF_URLOCK(locked[0]);
}

void
mds_repl_buildbusytable(void)
{
	struct resm_mds_info *rmmi;
	struct slm_resmlink *srl;
	struct sl_resource *r;
	struct sl_resm *m;
	struct sl_site *s;
	int n, j;

	/* count # resm's (IONs) and assign each a busy identifier */
	CONF_LOCK();
	spinlock(&repl_busytable_lock);
	repl_busytable_nents = 0;
	CONF_FOREACH_RESM(s, r, n, m, j) {
		if (!RES_ISFS(r))
			continue;
		rmmi = resm2rmmi(m);
		rmmi->rmmi_busyid = repl_busytable_nents++;
	}
	CONF_ULOCK();

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

	slm_null_si.si_flags |= SIF_PRECLAIM_NOTSUP;
}
