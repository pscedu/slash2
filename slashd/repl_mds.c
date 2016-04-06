/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2008-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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
#include "pathnames.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slconn.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

struct psc_poolmgr *slm_repl_status_pool;

struct sl_mds_iosinfo	 slm_null_iosinfo = {
	.si_flags = SIF_PRECLAIM_NOTSUP
};

/*
 * Max number of allowable bandwidth units (BW_UNITSZ) in any sliod's
 * bwqueue.
 */
int slm_bwqueuesz = 8 * 32 * 1024;

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
_slm_repl_bmap_rel_type(struct bmap *b, int type)
{
	if (BMAPOD_HASWRLOCK(bmap_2_bmi(b)) &&
	    !(b->bcm_flags & BMAPF_REPLMODWR)) {
		/* we took a write lock but did not modify; undo */
		BMAPOD_MODIFY_DONE(b, 0);
		BMAP_UNBUSY(b);
		FCMH_UNBUSY(b->bcm_fcmh);
	}
	bmap_op_done_type(b, type);
}

int
_mds_repl_ios_lookup(int vfsid, struct slash_inode_handle *ih,
    sl_ios_id_t ios, int flags)
{
	int locked, rc = -SLERR_REPL_NOT_ACT, inox_rc = 0;
	struct slm_inox_od *ix = NULL;
	struct sl_resource *res;
	struct fidc_membh *f;
	sl_replica_t *repl;
	uint32_t j, k, *nr;

	f = inoh_2_fcmh(ih);
	nr = &ih->inoh_ino.ino_nrepls;
	locked = INOH_RLOCK(ih);
	/*
	 * Search the existing replicas to see if the given IOS is
	 * already there.
	 */
	for (j = 0, k = 0, repl = ih->inoh_ino.ino_repls;
	    j < *nr; j++, k++) {
		if (j == SL_DEF_REPLICAS) {
			/*
			 * The first few replicas are in the inode
			 * itself, the rest are in the extras block.
			 */
			inox_rc = mds_inox_ensure_loaded(ih);
			if (inox_rc)
				goto out;
			ix = ih->inoh_extras;
			repl = ix->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_DEBUG, ih, "is rep[%u](=%u) == %u ?",
		    k, repl[k].bs_id, ios);

		if (repl[k].bs_id == ios) {
			if (flags == IOSV_LOOKUPF_DEL) {
				if (*nr > SL_DEF_REPLICAS) {
					mds_inox_ensure_loaded(ih);
					ix = ih->inoh_extras;
				}
				if (j < SL_DEF_REPLICAS - 1) {
					memmove(&repl[k], &repl[k + 1],
					    (SL_DEF_REPLICAS - k - 1) *
					    sizeof(*repl));
				}
				if (j < SL_DEF_REPLICAS) {
					if (*nr > SL_DEF_REPLICAS)
						repl[SL_DEF_REPLICAS - 1].bs_id =
						    ix->inox_repls[0].bs_id;
					k = 0;
				}
				if (*nr > SL_DEF_REPLICAS &&
				    j < SL_MAX_REPLICAS - 1) {
					repl = ix->inox_repls;
					memmove(&repl[k], &repl[k + 1],
					    (SL_INOX_NREPLICAS - k - 1) *
					    sizeof(*repl));
				}
				--*nr;
				mds_inodes_odsync(vfsid, f,
				    mdslog_ino_repls);
			}
			rc = j;
			goto out;
		}
	}

	res = libsl_id2res(ios);
	if (res == NULL || !RES_ISFS(res))
		PFL_GOTOERR(out, rc = -SLERR_RES_BADTYPE);

	/* It doesn't exist; add to inode replica table if requested. */
	if (flags == IOSV_LOOKUPF_ADD) {
		int waslk, wasbusy;

		psc_assert(*nr <= SL_MAX_REPLICAS);
		if (*nr == SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, ih, "too many replicas");
			PFL_GOTOERR(out, rc = -ENOSPC);

		} else if (*nr >= SL_DEF_REPLICAS) {
			inox_rc = mds_inox_ensure_loaded(ih);
			if (inox_rc)
				goto out;

			repl = ih->inoh_extras->inox_repls;
			k = j - SL_DEF_REPLICAS;

		} else {
			repl = ih->inoh_ino.ino_repls;
			k = j;
		}

		wasbusy = FCMH_REQ_BUSY(f, &waslk);

		repl[k].bs_id = ios;
		++*nr;

		DEBUG_INOH(PLL_DIAG, ih, "add IOS(%u) to repls, index %d",
		    ios, j);

		mds_inodes_odsync(vfsid, f, mdslog_ino_repls);

		FCMH_UREQ_BUSY(f, wasbusy, waslk);

		rc = j;
	}

 out:
	INOH_URLOCK(ih, locked);
	return (inox_rc ? inox_rc : rc);
}

int
_mds_repl_iosv_lookup(int vfsid, struct slash_inode_handle *ih,
    const sl_replica_t iosv[], int iosidx[], int nios, int flags)
{
	int k;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(vfsid, ih,
		    iosv[k].bs_id, flags)) < 0)
			return (-iosidx[k]);

	qsort(iosidx, nios, sizeof(iosidx[0]), iosidx_cmp);
	/* check for dups */
	for (k = 1; k < nios; k++)
		if (iosidx[k] == iosidx[k - 1])
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

/*
 * Apply a translation matrix of residency states to a bmap.
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
 *	(*) BMAPF_BUSY
 *	(*) BMAPOD_WRLOCK
 */
int
_mds_repl_bmap_apply(struct bmap *b, const int *tract,
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
			    BMAPF_REPLMODWR) == 0);
			BMAP_ULOCK(b);
		} else {
			BMAP_WAIT_BUSY(b);
			psc_assert((b->bcm_flags &
			    BMAPF_REPLMODWR) == 0);
			BMAP_ULOCK(b);
			BMAPOD_MODIFY_START(b);
			memcpy(bmi->bmi_orepls, bmi->bmi_repls,
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
	val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);

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
		SL_REPL_SET_BMAP_IOS_STAT(bmi->bmi_repls, off,
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

/*
 * Walk the bmap replication bits, performing any specified translations
 * and returning any queried states.
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
_mds_repl_bmap_walk(struct bmap *b, const int *tract,
    const int *retifset, int flags, const int *iosidx, int nios,
    brepl_walkcb_t cbf, void *cbarg)
{
	int scircuit, nr, off, k, rc, trc;

	scircuit = rc = 0;
	nr = fcmh_2_nrepls(b->bcm_fcmh);

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
mds_repl_inv_requeue(struct bmap *b, int idx, int val, void *arg)
{
	struct iosidv *qv = arg;

	if (val == BREPLST_VALID)
		qv->iosv[qv->nios++].bs_id = fcmh_2_repl(b->bcm_fcmh,
		    idx);
}

/*
 * For the given bmap, change the status of all its replicas marked
 * "valid" to "invalid" except for the replica specified.
 *
 * This is a high-level convenience call provided to easily update
 * status after an ION has received some new I/O, which would make all
 * other existing copies of the bmap on any other replicas old.
 * @b: the bmap.
 * @iosidx: the index of the only ION resource in the inode replica
 *	table that should be marked "valid".
 *
 * Note: All callers must journal log these bmap replica changes
 *	themselves.  In addition, they must log any changes to the inode
 *	_before_ the bmap changes.  Otherwise, we could end up actually
 *	having bmap replicas that are not recognized by the information
 *	stored in the inode during log replay.
 */
int
_mds_repl_inv_except(struct bmap *b, int iosidx, int defer)
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

	rc = mds_bmap_write(b, NULL, NULL);

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

#define PUSH_IOS(b, a, id, st)						\
	do {								\
		(a)->stat[(a)->nios] = (st);				\
		(a)->iosv[(a)->nios++].bs_id = (id);			\
		DEBUG_BMAP(PLL_DEBUG, (b),				\
		    "registering resid %d with %s", (id), #a);		\
	} while (0)

void
slm_repl_upd_write(struct bmap *b, int rel)
{
	struct {
		sl_replica_t	 iosv[SL_MAX_REPLICAS];
		char		*stat[SL_MAX_REPLICAS];
		unsigned	 nios;
	} add, del, chg;
	int locked, off, vold, vnew, sprio, uprio, rc;
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
	sprio = bmi->bmi_sys_prio;
	uprio = bmi->bmi_usr_prio;

	/* Transfer ownership to us. */
	pthr = pthread_self();
	f->fcmh_owner = pthr;
	b->bcm_owner = pthr;

	locked = BMAPOD_READ_START(b);

	memset(&chg, 0, sizeof(chg));

	add.nios = 0;
	del.nios = 0;
	chg.nios = 0;
	for (n = 0, off = 0; n < fcmh_2_nrepls(f);
	    n++, off += SL_BITS_PER_REPLICA) {
		resid = fcmh_2_repl(f, n);
		vold = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_orepls, off);
		vnew = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);

		r = libsl_id2res(resid);
		si = r ? res2iosinfo(r) : &slm_null_iosinfo;

		if (vold == vnew)
			;

		/* Work was added. */
		else if ((vold != BREPLST_REPL_SCHED &&
		    vold != BREPLST_GARBAGE &&
		    vold != BREPLST_GARBAGE_SCHED &&
		    vnew == BREPLST_REPL_QUEUED) ||
		    (vold != BREPLST_GARBAGE_SCHED &&
		     vnew == BREPLST_GARBAGE &&
		     (si->si_flags & SIF_PRECLAIM_NOTSUP) == 0))
			PUSH_IOS(b, &add, resid, NULL);

		/* Work has finished. */
		else if ((vold == BREPLST_REPL_QUEUED ||
		     vold == BREPLST_REPL_SCHED ||
		     vold == BREPLST_TRUNCPNDG_SCHED ||
		     vold == BREPLST_TRUNCPNDG ||
		     vold == BREPLST_GARBAGE_SCHED ||
		     vold == BREPLST_VALID) &&
		    (((si->si_flags & SIF_PRECLAIM_NOTSUP) &&
		      vnew == BREPLST_GARBAGE) ||
		     vnew == BREPLST_VALID ||
		     vnew == BREPLST_INVALID))
			PUSH_IOS(b, &del, resid, NULL);

		/*
		 * Work that was previously scheduled failed so requeue
		 * it.
		 */
		else if (vold == BREPLST_REPL_SCHED ||
		    vold == BREPLST_GARBAGE_SCHED ||
		    vold == BREPLST_TRUNCPNDG_SCHED)
			PUSH_IOS(b, &chg, resid, "Q");

		/* Work was scheduled. */
		else if (vnew == BREPLST_REPL_SCHED ||
		    vnew == BREPLST_GARBAGE_SCHED ||
		    vnew == BREPLST_TRUNCPNDG_SCHED)
			PUSH_IOS(b, &chg, resid, "S");

		/* Work was reprioritized. */
		else if (sprio != -1 || uprio != -1)
			PUSH_IOS(b, &chg, resid, NULL);
	}

	for (n = 0; n < add.nios; n++) {
		rc = slm_upsch_insert(b, add.iosv[n].bs_id, sprio,
		    uprio);
		if (rc)
			DEBUG_BMAPOD(PLL_WARN, b,
			    "unable to insert into upsch database; "
			    "ios=%#x rc=%d",
			    add.iosv[n].bs_id, rc);
	}

	for (n = 0; n < del.nios; n++)
		dbdo(NULL, NULL,
		    " DELETE FROM upsch"
		    " WHERE	resid = ?"
		    "   AND	fid = ?"
		    "   AND	bno = ?",
		    SQLITE_INTEGER, del.iosv[n].bs_id,
		    SQLITE_INTEGER64, bmap_2_fid(b),
		    SQLITE_INTEGER, b->bcm_bmapno);

	for (n = 0; n < chg.nios; n++)
		dbdo(NULL, NULL,
		    " UPDATE	upsch"
		    " SET	status = IFNULL(?, status),"
		    "		sys_prio = IFNULL(?, sys_prio),"
		    "		usr_prio = IFNULL(?, usr_prio)"
		    " WHERE	resid = ?"
		    "	AND	fid = ?"
		    "	AND	bno = ?",
		    chg.stat[n] ? SQLITE_TEXT : SQLITE_NULL,
		    chg.stat[n] ? chg.stat[n] : 0,
		    sprio == -1 ? SQLITE_NULL : SQLITE_INTEGER,
		    sprio == -1 ? 0 : sprio,
		    uprio == -1 ? SQLITE_NULL : SQLITE_INTEGER,
		    uprio == -1 ? 0 : uprio,
		    SQLITE_INTEGER, chg.iosv[n].bs_id,
		    SQLITE_INTEGER64, bmap_2_fid(b),
		    SQLITE_INTEGER, b->bcm_bmapno);

	bmap_2_bmi(b)->bmi_sys_prio = -1;
	bmap_2_bmi(b)->bmi_usr_prio = -1;

	if (rel) {
		BMAPOD_READ_DONE(b, locked);

		FCMH_UNBUSY(b->bcm_fcmh);

		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAPF_REPLMODWR;
		BMAP_UNBUSY(b);

		UPD_UNBUSY(upd);

		bmap_op_done_type(b, BMAP_OPCNT_WORK);
	}
}

#define FLAG_DIRTY			(1 << 0)	/* bmap was modified and must be saved */
#define FLAG_REPLICA_STATE_INVALID	(1 << 1)	/* return SLERR_REPLICA_STATE_INVALID */

/*
 * Change operation state depending on replica state.
 *
 * Flag dirty if replicas get enqueued for replication so the bmap can
 * be written to persistent storage.
 */
void
slm_repl_addrq_cb(__unusedx struct bmap *b, __unusedx int iosidx,
    int val, void *arg)
{
	int *flags = arg;

	switch (val) {
	case BREPLST_REPL_QUEUED:
	case BREPLST_REPL_SCHED:
	case BREPLST_VALID:
		break;

	case BREPLST_GARBAGE:
	case BREPLST_GARBAGE_SCHED:
	case BREPLST_INVALID:
		 *flags |= FLAG_DIRTY;
		 break;

	default:
		 /* Report that the replica will not be made valid. */
		 *flags |= FLAG_REPLICA_STATE_INVALID;
		 break;
	}
}

/*
 * Handle a request to do replication from a client.  May also
 * reinitialize some parameters of the replication, such as priority, if
 * the request already exists in the system.
 */
int
mds_repl_addrq(const struct sl_fidgen *fgp, sl_bmapno_t bmapno,
    sl_bmapno_t *nbmaps, sl_replica_t *iosv, int nios, int sys_prio,
    int usr_prio)
{
	int tract[NBREPLST], ret_hasvalid[NBREPLST];
	int iosidx[SL_MAX_REPLICAS], rc, flags;
	sl_bmapno_t nbmaps_processed = 0;
	struct fidc_membh *f;
	struct bmap *b;

	/* Perform sanity checks on request. */
	if (nios < 1 || nios > SL_MAX_REPLICAS || *nbmaps == 0)
		return (-EINVAL);

	rc = slm_fcmh_get(fgp, &f);
	if (rc)
		return (-rc);

	if (!fcmh_isdir(f) && !fcmh_isreg(f))
		PFL_GOTOERR(out, rc = -PFLERR_NOTSUP);

	/* Lookup replica(s)' indexes in our replica table. */
	rc = -mds_repl_iosv_lookup_add(current_vfsid, fcmh_2_inoh(f),
	    iosv, iosidx, nios);
	if (rc)
		PFL_GOTOERR(out, rc);

	/*
	 * If we are modifying a directory, we are done as just the
	 * replica table needs to be updated.
	 */
	if (fcmh_isdir(f))
		PFL_GOTOERR(out, 0);

	/*
	 * Setup structure to ensure at least one VALID replica exists.
	 */
	brepls_init(ret_hasvalid, 0);
	ret_hasvalid[BREPLST_VALID] = 1;

	/*
	 * Setup transitions to enqueue a replication.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_REPL_QUEUED;

	/* Wildcards shouldn't result in errors on zero-length files. */
	if (*nbmaps != (sl_bmapno_t)-1)
		rc = -SLERR_BMAP_INVALID;
	for (; *nbmaps && bmapno < fcmh_nvalidbmaps(f);
	    bmapno++, --*nbmaps, nbmaps_processed++) {
		if (nbmaps_processed >= SLM_REPLRQ_NBMAPS_MAX)
			PFL_GOTOERR(out, rc = -PFLERR_WOULDBLOCK);

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc) {
			if (rc == -SLERR_BMAP_ZERO) {
				rc = 0;
				break;
			}
			PFL_GOTOERR(out, rc);
		}

		/*
		 * If no VALID replicas exist, the bmap must be
		 * uninitialized/all zeroes; skip it.
		 */
		if (mds_repl_bmap_walk_all(b, NULL, ret_hasvalid,
		    REPL_WALKF_SCIRCUIT) == 0) {
			bmap_op_done(b);
			continue;
		}

		/*
		 * We do not follow the standard "retifset" API here
		 * because we need to preserve DIRTY if it gets set
		 * instead of some other state getting returned.
		 */
		flags = 0;
		_mds_repl_bmap_walk(b, tract, NULL, 0, iosidx, nios,
		    slm_repl_addrq_cb, &flags);
		bmap_2_bmi(b)->bmi_sys_prio = sys_prio;
		bmap_2_bmi(b)->bmi_usr_prio = usr_prio;
		if (flags & FLAG_DIRTY) {
			struct slm_update_data *upd;

			upd = &bmap_2_bmi(b)->bmi_upd;
			if (pfl_memchk(upd, 0, sizeof(*upd)) == 1) {
				upd_init(upd, UPDT_BMAP);
			} else {
				UPD_WAIT(upd);
				upd->upd_flags |= UPDF_BUSY;
				upd->upd_owner = pthread_self();
				UPD_ULOCK(upd);
			}
			mds_bmap_write_logrepls(b);
			UPD_UNBUSY(upd);
		} else if (sys_prio != -1 || usr_prio != -1)
			slm_repl_upd_write(b, 0);
		slm_repl_bmap_rel(b);
		if (flags & FLAG_REPLICA_STATE_INVALID)
			PFL_GOTOERR(out,
			    rc = -SLERR_REPLICA_STATE_INVALID);
	}

 out:
	if (f)
		fcmh_op_done(f);
	*nbmaps = nbmaps_processed;
	return (rc);
}

struct slm_repl_valid {
	int  n;
	int  nios;
	int *idx;
};

/*
 * Count the number of replicas that would exist after a potential DELRQ
 * operation, to ensure the last replicas aren't removed.
 */
void
slm_repl_countvalid_cb(__unusedx struct bmap *b, int iosidx, int val,
    void *arg)
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

void
slm_repl_delrq_cb(__unusedx struct bmap *b, __unusedx int iosidx,
    int val, void *arg)
{
	int *flags = arg;

	switch (val) {
	case BREPLST_INVALID:
	case BREPLST_REPL_QUEUED:
	case BREPLST_REPL_SCHED:
		break;

	case BREPLST_GARBAGE:
	case BREPLST_GARBAGE_SCHED:
	case BREPLST_VALID:
		 *flags |= FLAG_DIRTY;
		 break;

	default:
		 /* Report that the replica will not be made invalid. */
		 *flags |= FLAG_REPLICA_STATE_INVALID;
		 break;
	}
}

int
mds_repl_delrq(const struct sl_fidgen *fgp, sl_bmapno_t bmapno,
    sl_bmapno_t *nbmaps, sl_replica_t *iosv, int nios)
{
	int tract[NBREPLST], rc, iosidx[SL_MAX_REPLICAS], flags = 0;
	sl_bmapno_t nbmaps_processed = 0;
	struct slm_repl_valid replv;
	struct fidc_membh *f = NULL;
	struct bmap *b;

	if (nios < 1 || nios > SL_MAX_REPLICAS || *nbmaps == 0)
		return (-EINVAL);

	rc = slm_fcmh_get(fgp, &f);
	if (rc)
		return (-rc);

	if (fcmh_isdir(f))
		flags = IOSV_LOOKUPF_DEL;

	/* Find replica IOS indexes. */
	rc = -_mds_repl_iosv_lookup(current_vfsid, fcmh_2_inoh(f), iosv,
	    iosidx, nios, flags);
	if (rc)
		PFL_GOTOERR(out, rc);

	if (fcmh_isdir(f))
		PFL_GOTOERR(out, 0);

	replv.nios = nios;
	replv.idx = iosidx;

	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE;
	tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE;
	tract[BREPLST_VALID] = BREPLST_GARBAGE;

	/* Wildcards shouldn't result in errors on zero-length files. */
	if (*nbmaps != (sl_bmapno_t)-1)
		rc = -SLERR_BMAP_INVALID;
	for (; *nbmaps && bmapno < fcmh_nvalidbmaps(f);
	    bmapno++, --*nbmaps, nbmaps_processed++) {
		if (nbmaps_processed >= SLM_REPLRQ_NBMAPS_MAX)
			PFL_GOTOERR(out, rc = -PFLERR_WOULDBLOCK);

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc) {
			if (rc == -SLERR_BMAP_ZERO) {
				rc = 0;
				break;
			}
			PFL_GOTOERR(out, rc);
		}

		/*
		 * Before blindly doing the transition, we have to check
		 * to ensure this operation would retain at least one
		 * valid replica.
		 */
		replv.n = 0;
		mds_repl_bmap_walkcb(b, NULL, NULL, 0,
		    slm_repl_countvalid_cb, &replv);
		if (replv.n == 0)
			PFL_GOTOERR(bmap_done, rc = -SLERR_LASTREPL);

		flags = 0;
		rc = _mds_repl_bmap_walk(b, tract, NULL, 0, iosidx,
		    nios, slm_repl_delrq_cb, &flags);
		if (flags & FLAG_DIRTY)
			mds_bmap_write_logrepls(b);

 bmap_done:
		slm_repl_bmap_rel(b);
		if (flags & FLAG_REPLICA_STATE_INVALID)
			PFL_GOTOERR(out,
			    rc = -SLERR_REPLICA_STATE_INVALID);
	}

 out:
	if (f)
		fcmh_op_done(f);
	*nbmaps = nbmaps_processed;
	return (rc);
}

#define HAS_BW(bwd, amt)						\
	((bwd)->bwd_queued + (bwd)->bwd_inflight < slm_bwqueuesz)

#define ADJ_BW(bwd, amt)						\
	do {								\
		if ((amt) > 0)						\
			(bwd)->bwd_inflight += (amt);			\
		(bwd)->bwd_assigned += (amt);				\
		psc_assert((bwd)->bwd_assigned >= 0);			\
	} while (0)

#define SIGN(x)	((x) >= 0 ? 1 : -1)

/*
 * Adjust the bandwidth estimate between two IONs.
 * @src: source resm.
 * @dst: destination resm.
 * @amt_bytes: adjustment amount, in octets.
 * @moreavail: whether there is still bandwidth left after this
 * reservation.
 */
int
resmpair_bw_adj(struct sl_resm *src, struct sl_resm *dst,
    int64_t amt_bytes, int *moreavail)
{
	struct resprof_mds_info *r_min, *r_max;
	struct rpmi_ios *is, *id;
	int32_t amt;
	int rc = 0;

	amt = SIGN(amt_bytes) * howmany(labs(amt_bytes), BW_UNITSZ);

	if (moreavail)
		*moreavail = 0;

	/* sort by addr to avoid deadlock */
	r_min = MIN(res2rpmi(src->resm_res), res2rpmi(dst->resm_res));
	r_max = MAX(res2rpmi(src->resm_res), res2rpmi(dst->resm_res));
	RPMI_LOCK(r_min);
	RPMI_LOCK(r_max);

	is = res2rpmi_ios(src->resm_res);
	id = res2rpmi_ios(dst->resm_res);

	if (amt < 0 ||
	    (HAS_BW(&is->si_bw_egress, amt) &&
	     HAS_BW(&is->si_bw_aggr, amt) &&
	     HAS_BW(&id->si_bw_ingress, amt) &&
	     HAS_BW(&id->si_bw_aggr, amt))) {
		ADJ_BW(&is->si_bw_egress, amt);
		ADJ_BW(&is->si_bw_aggr, amt);
		ADJ_BW(&id->si_bw_ingress, amt);
		ADJ_BW(&id->si_bw_aggr, amt);

		psclog_diag("adjust bandwidth; src=%s dst=%s amt=%d",
		    src->resm_name, dst->resm_name, amt);

		if (moreavail &&
		    HAS_BW(&is->si_bw_egress, 1) &&
		    HAS_BW(&is->si_bw_aggr, 1) &&
		    HAS_BW(&id->si_bw_ingress, 1) &&
		    HAS_BW(&id->si_bw_aggr, 1))
			*moreavail = 1;

		rc = 1;
	}

	/* XXX upsch page in? */
	if (amt < 0) {
		/*
		 * We released some bandwidth; wake anyone waiting for
		 * some.
		 */
		CSVC_WAKE(src->resm_csvc);
		CSVC_WAKE(dst->resm_csvc);
	}

	RPMI_ULOCK(r_max);
	RPMI_ULOCK(r_min);

	return (rc);
}
