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
 *
 * We have the ability to throttle at batch RPC layer. So disable this
 * for now.
 */
int slm_upsch_bandwidth = 0;		/* used to be 1024 */

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

/*
 * Return the index of the given IOS ID or a negative error code on failure.
 */
int
_mds_repl_ios_lookup(int vfsid, struct slash_inode_handle *ih,
    sl_ios_id_t ios, int flag)
{
	int locked, rc;
	struct slm_inox_od *ix = NULL;
	struct sl_resource *res;
	struct fidc_membh *f;
	sl_replica_t *repl;
	uint32_t i, j, nr;
	char buf[LINE_MAX];

	switch (flag) {
	    case IOSV_LOOKUPF_ADD:
		OPSTAT_INCR("replicate-add");
		break;
	    case IOSV_LOOKUPF_DEL:
		OPSTAT_INCR("replicate-del");
		break;
	    case IOSV_LOOKUPF_LOOKUP:
		OPSTAT_INCR("replicate-lookup");
		break;
	    default:
		psc_fatalx("Invalid IOS lookup flag %d", flag);
	}

	/*
 	 * Can I assume that IOS ID are non-zeros?  If so, I can use
 	 * zero to mark a free slot.  See sl_global_id_build().
 	 */
	f = inoh_2_fcmh(ih);
	nr = ih->inoh_ino.ino_nrepls;
	repl = ih->inoh_ino.ino_repls;
	locked = INOH_RLOCK(ih);

	psc_assert(nr <= SL_MAX_REPLICAS);

	res = libsl_id2res(ios);
	if (res == NULL)
		PFL_GOTOERR(out, rc = -SLERR_RES_UNKNOWN);
	if (!RES_ISFS(res))
		PFL_GOTOERR(out, rc = -SLERR_RES_BADTYPE);

	/*
	 * 09/29/2016: Hit SLERR_SHORTIO in the function. Need more investigation.
	 */

	/*
 	 * Return ENOENT by default for IOSV_LOOKUPF_DEL & IOSV_LOOKUPF_LOOKUP.
 	 */
	rc = -ENOENT;

	/*
	 * Search the existing replicas to see if the given IOS is
	 * already there.
	 *
	 * The following code can step through zero IOS IDs just fine.
	 *
	 */
	for (i = 0, j = 0; i < nr; i++, j++) {
		if (i == SL_DEF_REPLICAS) {
			/*
			 * The first few replicas are in the inode
			 * itself, the rest are in the extra inode
			 * block.
			 */
			rc = mds_inox_ensure_loaded(ih);
			if (rc)
				goto out;
			ix = ih->inoh_extras;
			repl = ix->inox_repls;
			j = 0;
		}

		DEBUG_INOH(PLL_DEBUG, ih, buf, "is rep[%u](=%u) == %u ?",
		    j, repl[j].bs_id, ios);

		if (repl[j].bs_id == ios) {
			/*
 			 * Luckily, this code is only called by mds_repl_delrq() 
 			 * for directories.
 			 *
 			 * Make sure that the logic works for at least the following 
 			 * edge cases:
 			 *
 			 *    (1) There is only one item in the basic array.
 			 *    (2) There is only one item in the extra array.
 			 *    (3) The number of items is SL_DEF_REPLICAS.
 			 *    (4) The number of items is SL_MAX_REPLICAS.
 			 */
			if (flag == IOSV_LOOKUPF_DEL) {
				/*
				 * Compact the array if the IOS is not the last
				 * one. The last one will be either overwritten
				 * or zeroed.  Note that we might move extra 
				 * garbage at the end if the total number is less 
				 * than SL_DEF_REPLICAS.
				 */
				if (i < SL_DEF_REPLICAS - 1) {
					memmove(&repl[j], &repl[j + 1],
					    (SL_DEF_REPLICAS - j - 1) *
					    sizeof(*repl));
				}
				/*
				 * All items in the basic array, zero the last
				 * one and we are done.
				 */
				if (nr <= SL_DEF_REPLICAS) {
					repl[nr-1].bs_id = 0;
					goto syncit;
				}
				/*
				 * Now we know we have more than SL_DEF_REPLICAS
				 * items.  However, if we are in the basic array,
				 * we have not read the extra array yet. In this
				 * case, we should also move the first item from 
				 * the extra array to the last one in the basic 
				 * array (overwrite).
				 */
				if (i < SL_DEF_REPLICAS) {
					rc = mds_inox_ensure_loaded(ih);
					if (rc)
						goto out;
					ix = ih->inoh_extras;

					repl[SL_DEF_REPLICAS - 1].bs_id =
					    ix->inox_repls[0].bs_id;

					repl = ix->inox_repls;
					j = 0;
				}
				/*
				 * Compact the extra array unless the IOS is
				 * the last one, which will be zeroed.
				 */
				if (i < SL_MAX_REPLICAS - 1) {
					memmove(&repl[j], &repl[j + 1],
					    (SL_INOX_NREPLICAS - j - 1) * 
					    sizeof(*repl));
				}

				repl[nr-SL_DEF_REPLICAS-1].bs_id = 0;
 syncit:
				ih->inoh_ino.ino_nrepls = nr - 1;
				rc = mds_inodes_odsync(vfsid, f, mdslog_ino_repls);
				if (rc)
					goto out;
			}
			if (flag == IOSV_LOOKUPF_ADD)
				OPSTAT_INCR("ios-add-noop");
			rc = i; 
			goto out;
		}
	}

	/* It doesn't exist; add to inode replica table if requested. */
	if (flag == IOSV_LOOKUPF_ADD) {

		/* paranoid */
		psc_assert(i == nr);

		if (nr == SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, ih, buf, "too many replicas");
			PFL_GOTOERR(out, rc = -ENOSPC);
		}

		if (nr >= SL_DEF_REPLICAS) {
			/* be careful with the case of nr = SL_DEF_REPLICAS */
			rc = mds_inox_ensure_loaded(ih);
			if (rc)
				goto out;
			repl = ih->inoh_extras->inox_repls;
			j = i - SL_DEF_REPLICAS;

		} else {
			repl = ih->inoh_ino.ino_repls;
			j = i;
		}

		repl[j].bs_id = ios;

		DEBUG_INOH(PLL_DIAG, ih, buf, "add IOS(%u) at idx %d", ios, i);

		ih->inoh_ino.ino_nrepls = nr + 1;
		rc = mds_inodes_odsync(vfsid, f, mdslog_ino_repls);
		if (!rc)
			rc = i;
	}

 out:
	INOH_URLOCK(ih, locked);
	return (rc);
}

/*
 * Given a vector of IOS IDs, return their indexes.
 */
int
_mds_repl_iosv_lookup(int vfsid, struct slash_inode_handle *ih,
    const sl_replica_t iosv[], int iosidx[], int nios, int flag)
{
	int k;

	for (k = 0; k < nios; k++)
		if ((iosidx[k] = _mds_repl_ios_lookup(vfsid, ih,
		    iosv[k].bs_id, flag)) < 0)
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
		case BREPLST_GARBAGE_QUEUED:
		case BREPLST_GARBAGE_SCHED:
		case BREPLST_TRUNC_QUEUED:
		case BREPLST_TRUNC_SCHED:
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
 */
int
_mds_repl_bmap_apply(struct bmap *b, const int *tract,
    const int *retifset, int flags, int off, int *scircuit,
    brepl_walkcb_t cbf, void *cbarg)
{
	int val, rc = 0;
	struct timeval tv1, tv2, tvd;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	BMAP_LOCK_ENSURE(b);
	if (tract) {
		/*
		 * The caller must set the flag if modifications are made.
		 *
		 * With bmap locked, we will save old replication info.
		 * The change will be updated by a worker thread, which
		 * is protected by the BMAPF_REPLMODWR flag.
		 */
		PFL_GETTIMEVAL(&tv1);
		bmap_wait_locked(b, b->bcm_flags & BMAPF_REPLMODWR);
		PFL_GETTIMEVAL(&tv2);
		timersub(&tv2, &tv1, &tvd);
		OPSTAT_ADD("bmap-wait-usecs", tvd.tv_sec * 1000000 + tvd.tv_usec);

		memcpy(bmi->bmi_orepls, bmi->bmi_repls,
		    sizeof(bmi->bmi_orepls));
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);
	}

	if (scircuit)
		*scircuit = 0;
	else
		psc_assert((flags & REPL_WALKF_SCIRCUIT) == 0);

	/* retrieve IOS status given a bit offset into the map */
	val = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);

	if (val >= NBREPLST)
		psc_fatalx("corrupt bmap, val = %d, bno = %d, fid="SLPRI_FID,
			 val, b->bcm_bmapno, fcmh_2_fid(b->bcm_fcmh));

	/* callback can also be used to track if we did make any changes */
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

	/* apply any translations - this must be done after retifset */
	if (tract && tract[val] != -1) {
		DEBUG_BMAPOD(PLL_DEBUG, b, "before modification");
		SL_REPL_SET_BMAP_IOS_STAT(bmi->bmi_repls, off,
		    tract[val]);
		DEBUG_BMAPOD(PLL_DEBUG, b, "after modification");
	}

 out:
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

	/* 
 	 * gdb help:
 	 *
	 * ((struct fcmh_mds_info *)
	 * (b->bcm_fcmh + 1))->fmi_inodeh.inoh_ino.ino_nrepls 
	 *
 	 * ((struct bmap_mds_info*)(b+1))->bmi_corestate.bcs_repls
 	 *
	 */ 
	nr = fcmh_2_nrepls(b->bcm_fcmh);

	if (nios == 0) {
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
		return (rc);
	}
	if (flags & REPL_WALKF_MODOTH) {
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
		return (rc);
	} 

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
mds_repl_inv_except(struct bmap *b, int iosidx)
{
	int rc, logit = 0, tract[NBREPLST], retifset[NBREPLST];
	uint32_t policy;

	/* Ensure replica on active IOS is marked valid. */
	brepls_init(tract, -1);
	tract[BREPLST_INVALID] = BREPLST_VALID;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_VALID;
	tract[BREPLST_GARBAGE_QUEUED] = BREPLST_VALID;

	/*
	 * The old state for this bmap on the given IOS is
	 * either valid or invalid.
	 */
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
	if (rc) {
		psclog_errorx("bcs_repls has active IOS marked in a "
		    "weird state while invalidating other replicas; "
		    "fid="SLPRI_FID" bmap=%d iosidx=%d state=%d",
		    fcmh_2_fid(b->bcm_fcmh), b->bcm_bmapno, iosidx, rc);
	}

	policy = bmap_2_replpol(b);

	/*
	 * Invalidate all other replicas.
	 * Note: if the status is SCHED here, don't do anything; once
	 * the replication status update comes from the ION, we will
	 * know he copied an old bmap and mark it OLD then.
	 */
	brepls_init(tract, -1);
	tract[BREPLST_VALID] = policy == BRPOL_PERSIST ?
	    BREPLST_REPL_QUEUED : BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;
	retifset[BREPLST_REPL_SCHED] = 1;

	if (_mds_repl_bmap_walk(b, tract, retifset, REPL_WALKF_MODOTH,
	    &iosidx, 1, NULL, NULL)) {
		logit = 1;
		BHGEN_INCREMENT(b);
	}
	if (logit)
		rc = mds_bmap_write_logrepls(b);
	else
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

/* b is for debug only */
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

	int off, vold, vnew, sprio, uprio, rc;
	struct sl_mds_iosinfo *si;
	struct bmap_mds_info *bmi;
	struct fidc_membh *f;
	struct sl_resource *r;
	sl_ios_id_t resid;
	unsigned n, nrepls;

	bmi = bmap_2_bmi(b);
	f = b->bcm_fcmh;
	sprio = bmi->bmi_sys_prio;
	uprio = bmi->bmi_usr_prio;

	add.nios = 0;
	del.nios = 0;
	chg.nios = 0;
	nrepls = fcmh_2_nrepls(f);
	for (n = 0, off = 0; n < nrepls; n++, off += SL_BITS_PER_REPLICA) {

		if (n == SL_DEF_REPLICAS)
			mds_inox_ensure_loaded(fcmh_2_inoh(f));

		resid = fcmh_2_repl(f, n);
		vold = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_orepls, off);
		vnew = SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls, off);

		r = libsl_id2res(resid);
		si = r ? res2iosinfo(r) : &slm_null_iosinfo;

		if (vold == vnew)
			;

		/* Work was added. */
		else if ((vold != BREPLST_REPL_SCHED &&
		    vold != BREPLST_GARBAGE_QUEUED &&
		    vold != BREPLST_GARBAGE_SCHED &&
		    vnew == BREPLST_REPL_QUEUED) ||
		    (vold != BREPLST_GARBAGE_SCHED &&
		     vnew == BREPLST_GARBAGE_QUEUED &&
		     (si->si_flags & SIF_PRECLAIM_NOTSUP) == 0)) {
			OPSTAT_INCR("repl-work-add");
			PUSH_IOS(b, &add, resid, NULL);
		}

		/* Work has finished. */
		else if ((vold == BREPLST_REPL_QUEUED ||
		     vold == BREPLST_REPL_SCHED ||
		     vold == BREPLST_TRUNC_SCHED ||
		     vold == BREPLST_TRUNC_QUEUED ||
		     vold == BREPLST_GARBAGE_SCHED ||
		     vold == BREPLST_VALID) &&
		    (((si->si_flags & SIF_PRECLAIM_NOTSUP) &&
		      vnew == BREPLST_GARBAGE_QUEUED) ||
		     vnew == BREPLST_VALID ||
		     vnew == BREPLST_INVALID)) {
			OPSTAT_INCR("repl-work-del");
			PUSH_IOS(b, &del, resid, NULL);
		}

		/*
		 * Work that was previously scheduled failed so 
		 * requeue it.
		 */
		else if (vold == BREPLST_REPL_SCHED ||
		    vold == BREPLST_GARBAGE_SCHED ||
		    vold == BREPLST_TRUNC_SCHED)
			PUSH_IOS(b, &chg, resid, "Q");

		/* Work was scheduled. */
		else if (vnew == BREPLST_REPL_SCHED ||
		    vnew == BREPLST_GARBAGE_SCHED ||
		    vnew == BREPLST_TRUNC_SCHED)
			PUSH_IOS(b, &chg, resid, "S");

		/* Work was reprioritized. */
		else if (sprio != -1 || uprio != -1)
			PUSH_IOS(b, &chg, resid, NULL);
	}

	for (n = 0; n < add.nios; n++) {
		rc = slm_upsch_insert(b, add.iosv[n].bs_id, sprio,
		    uprio);
		if (!rc)
			continue;
		psclog_warnx("upsch insert failed: bno = %d, "
		    "fid=%"PRId64", ios= %d, rc = %d",
		    b->bcm_bmapno, bmap_2_fid(b), 
		    add.iosv[n].bs_id, rc);
	}

	for (n = 0; n < del.nios; n++) {
		dbdo(NULL, NULL,
		    " DELETE FROM upsch"
		    " WHERE	resid = ?"
		    "   AND	fid = ?"
		    "   AND	bno = ?",
		    SQLITE_INTEGER, del.iosv[n].bs_id,
		    SQLITE_INTEGER64, bmap_2_fid(b),
		    SQLITE_INTEGER, b->bcm_bmapno);
	}

	for (n = 0; n < chg.nios; n++) {
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
	}

	bmap_2_bmi(b)->bmi_sys_prio = -1;
	bmap_2_bmi(b)->bmi_usr_prio = -1;

	if (rel) {
		BMAP_LOCK(b);
		b->bcm_flags &= ~BMAPF_REPLMODWR;
		bmap_wake_locked(b);
		bmap_op_done_type(b, BMAP_OPCNT_WORK);
	}
}

#define FLAG_REPLICA_STATE_DIRTY	(1 << 0)	/* bmap was modified and must be saved */
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

	case BREPLST_GARBAGE_QUEUED:
	case BREPLST_GARBAGE_SCHED:
	case BREPLST_INVALID:
		 *flags |= FLAG_REPLICA_STATE_DIRTY;
		 break;

	case BREPLST_TRUNC_QUEUED:
	case BREPLST_TRUNC_SCHED:
		 /* Report that the replica will not be made valid. */
		 *flags |= FLAG_REPLICA_STATE_INVALID;
		 break;

	default:
		psc_fatalx("Invalid replication state %d", val);
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
	struct fidc_membh *f = NULL;
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
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_QUEUED] = BREPLST_REPL_QUEUED;

	/* Wildcards shouldn't result in errors on zero-length files. */
	if (*nbmaps != (sl_bmapno_t)-1)
		rc = -SLERR_BMAP_INVALID;

	for (; *nbmaps && bmapno < fcmh_nvalidbmaps(f);
	    bmapno++, --*nbmaps, nbmaps_processed++) {

		if (nbmaps_processed >= SLM_REPLRQ_NBMAPS_MAX) {
			rc = -PFLERR_WOULDBLOCK;
			break;
		}

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc)
			PFL_GOTOERR(out, rc);

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

		/* both default to -1 in parse_replrq() */
		bmap_2_bmi(b)->bmi_sys_prio = sys_prio;
		bmap_2_bmi(b)->bmi_usr_prio = usr_prio;
		if (flags & FLAG_REPLICA_STATE_DIRTY)
			mds_bmap_write_logrepls(b);
		else if (sys_prio != -1 || usr_prio != -1)
			slm_repl_upd_write(b, 0);

		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
		if (flags & FLAG_REPLICA_STATE_INVALID) {
			/* See pfl_register_errno() */
			rc = -SLERR_REPLICA_STATE_INVALID;
			break;
		}
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
slm_repl_countvalid_cb(struct bmap *b, int iosidx, int val,
    void *arg)
{
	struct slm_repl_valid *t = arg;
	struct slash_inode_handle *ih;
	struct fidc_membh *f;
	sl_replica_t *repl;
	int i, rc;

	/* If the state isn't VALID, nothing to count. */
	if (val != BREPLST_VALID)
		return;

	/*
	 * If we find an IOS that was specified, we can't factor it into
	 * our count since it won't be here much longer.
	 */
	for (i = 0; i < t->nios; i++)
		if (iosidx == t->idx[i])
			return;

	/*
 	 * If the "valid" replica is hold by an unknown I/Os, don't take
 	 * it into account.
 	 */
	f = b->bcm_fcmh;
	ih = fcmh_2_inoh(f);
	if (iosidx < SL_DEF_REPLICAS) {
		i = iosidx;
		repl = ih->inoh_ino.ino_repls;
	} else {
		rc = mds_inox_ensure_loaded(ih);
		if (rc)
			return;
		i = iosidx - SL_DEF_REPLICAS;
		repl = ih->inoh_extras->inox_repls;
	} 
	if (libsl_id2res(repl[i].bs_id))
		t->n++;
	else
		OPSTAT_INCR("unknown-valid");
}

void
slm_repl_delrq_cb(__unusedx struct bmap *b, __unusedx int iosidx,
    int val, void *arg)
{
	int *flags = arg;

	switch (val) {
	case BREPLST_REPL_QUEUED:
	case BREPLST_REPL_SCHED:
	case BREPLST_TRUNC_QUEUED:
	case BREPLST_TRUNC_SCHED:
	case BREPLST_GARBAGE_SCHED:
	case BREPLST_VALID:
		*flags |= FLAG_REPLICA_STATE_DIRTY;
		break;
	default:
		 break;
	}
}

int
mds_repl_delrq(const struct sl_fidgen *fgp, sl_bmapno_t bmapno,
    sl_bmapno_t *nbmaps, sl_replica_t *iosv, int nios)
{
	int tract[NBREPLST], rc, iosidx[SL_MAX_REPLICAS], flags;
	sl_bmapno_t nbmaps_processed = 0;
	struct slm_repl_valid replv;
	struct fidc_membh *f = NULL;
	struct bmap *b;
	sl_bmapno_t nvalidbmaps;

	if (nios < 1 || nios > SL_MAX_REPLICAS || *nbmaps == 0)
		return (-EINVAL);

	rc = slm_fcmh_get(fgp, &f);
	if (rc)
		return (-rc);

	FCMH_LOCK(f);
	if (fcmh_isdir(f))
		flags = IOSV_LOOKUPF_DEL;
	else
		flags = IOSV_LOOKUPF_LOOKUP;

	/* Find replica IOS indexes. */
	rc = -_mds_repl_iosv_lookup(current_vfsid, fcmh_2_inoh(f), iosv,
	    iosidx, nios, flags);

	if (fcmh_isdir(f) || rc)
		PFL_GOTOERR(out, rc);

	replv.nios = nios;
	replv.idx = iosidx;

	/*
 	 * In theory, we should only have VALID --> GARBASE transition.
 	 * However, if the MDS or IOS crashes, we could be stuck in
 	 * other states as well.
 	 */
	brepls_init(tract, -1);
	tract[BREPLST_REPL_QUEUED] = BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_REPL_SCHED] = BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_TRUNC_QUEUED] = BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_TRUNC_SCHED] = BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_QUEUED;
	tract[BREPLST_VALID] = BREPLST_GARBAGE_QUEUED;

	/* Wildcards shouldn't result in errors on zero-length files. */
	if (*nbmaps != (sl_bmapno_t)-1)
		rc = -SLERR_BMAP_INVALID;

	nvalidbmaps = fcmh_nvalidbmaps(f);
	FCMH_ULOCK(f);

	/*
 	 * The following loop will bail out on the very first error. 
 	 * However,  its previous action, if any, has already taken
 	 * effect.
 	 */
	for (; *nbmaps && bmapno < nvalidbmaps;
	    bmapno++, --*nbmaps, nbmaps_processed++) {
		if (nbmaps_processed >= SLM_REPLRQ_NBMAPS_MAX)
			PFL_GOTOERR(out, rc = -PFLERR_WOULDBLOCK);

		rc = -bmap_get(f, bmapno, SL_WRITE, &b);
		if (rc)
			PFL_GOTOERR(out, rc);
		/*
		 * Before blindly doing the transition, we have to check
		 * to ensure this operation would retain at least one
		 * valid replica.
		 */
		replv.n = 0;
		mds_repl_bmap_walkcb(b, NULL, NULL, 0,
		    slm_repl_countvalid_cb, &replv);

		flags = 0;
		if (replv.n == 0)
			rc = -SLERR_LASTREPL;
		else {
			rc = _mds_repl_bmap_walk(b, tract, NULL, 0, iosidx,
			    nios, slm_repl_delrq_cb, &flags);
			psc_assert(!rc);

			/* schedule a call to slm_upsch_trypreclaim() */
			if (flags & FLAG_REPLICA_STATE_DIRTY)
				rc = mds_bmap_write_logrepls(b);
		}
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
		if (rc)
			PFL_GOTOERR(out, rc);
	}

 out:
	if (f)
		fcmh_op_done(f);
	*nbmaps = nbmaps_processed;
	return (rc);
}

#define HAS_BW(bwd, amt)						\
	((bwd)->bwd_queued + (bwd)->bwd_inflight < 			\
	slm_upsch_bandwidth * BW_UNITSZ)				

#define ADJ_BW(bwd, amt)						\
	do {								\
		(bwd)->bwd_inflight += (amt);				\
		(bwd)->bwd_assigned += (amt);				\
		psc_assert((bwd)->bwd_assigned >= 0);			\
	} while (0)

#define SIGN(x)	((x) >= 0 ? 1 : -1)

/*
 * Adjust the bandwidth estimate between two IONs.
 * @src: source resm.
 * @dst: destination resm.
 * @amt: adjustment amount in bytes.
 */
int
resmpair_bw_adj(struct sl_resm *src, struct sl_resm *dst,
    int64_t amt, int rc)
{
	int ret = 1;
	struct resprof_mds_info *r_min, *r_max;
	struct rpmi_ios *is, *id;
	int64_t src_total, dst_total;
	int64_t cap = (int64_t)slm_upsch_bandwidth;

	/* sort by addr to avoid deadlock */
	r_min = MIN(res2rpmi(src->resm_res), res2rpmi(dst->resm_res));
	r_max = MAX(res2rpmi(src->resm_res), res2rpmi(dst->resm_res));
	RPMI_LOCK(r_min);
	RPMI_LOCK(r_max);

	is = res2rpmi_ios(src->resm_res);
	id = res2rpmi_ios(dst->resm_res);

	psc_assert(amt);

	/* reserve */
	if (amt > 0) {
		if (cap) {
			src_total = is->si_repl_ingress_pending + 
			    is->si_repl_egress_pending + amt;
			dst_total = is->si_repl_ingress_pending + 
			    is->si_repl_egress_pending + amt;
			if ((src_total > cap * BW_UNITSZ) || 
			     dst_total > cap * BW_UNITSZ) { 
				ret = 0;
				goto out;
			}
		}
		is->si_repl_egress_pending += amt;
		id->si_repl_ingress_pending += amt;

		psclog_diag("adjust bandwidth; src=%s dst=%s amt=%"PRId64,
		    src->resm_name, dst->resm_name, amt);
	}

	/* unreserve */
	if (amt < 0) {
		is->si_repl_egress_pending += amt;
		id->si_repl_ingress_pending += amt;
		psc_assert(is->si_repl_egress_pending >= 0);
		psc_assert(id->si_repl_ingress_pending >= 0);
		if (!rc) {
			is->si_repl_egress_aggr += -amt;
			id->si_repl_ingress_aggr += -amt;
		}
		/*
		 * We released some bandwidth; wake anyone waiting for
		 * some.
		 */
#if 0
		CSVC_WAKE(src->resm_csvc);
		CSVC_WAKE(dst->resm_csvc);
#endif
	}

 out:
	RPMI_ULOCK(r_max);
	RPMI_ULOCK(r_min);

	return (ret);
}
