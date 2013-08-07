/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "psc_util/log.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "mdsio.h"
#include "repl_mds.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

static __inline void *
bmap_2_mdsio_data(struct bmap *b)
{
	struct fcmh_mds_info *fmi;

	fmi = fcmh_2_fmi(b->bcm_fcmh);
	psc_assert(fmi->fmi_mdsio_data);
	return (fmi->fmi_mdsio_data);
}

/**
 * mds_bmap_initnew - Called when a read request offset exceeds the
 *	bounds of the file causing a new bmap to be created.
 * Notes:  Bmap creation race conditions are prevented because the bmap
 *	handle already exists at this time with
 *	bcm_flags == BMAP_INIT.
 *
 *	This causes other threads to block on the waitq until
 *	read/creation has completed.
 * More Notes:  this bmap is not written to disk until a client actually
 *	writes something to it.
 */
__static void
mds_bmap_initnew(struct bmap *b)
{
	struct bmap_ondisk *bod = bmap_2_ondisk(b);
	struct fidc_membh *f = b->bcm_fcmh;
	uint32_t pol;
	int i;

	for (i = 0; i < SLASH_CRCS_PER_BMAP; i++)
		bod->bod_crcs[i] = BMAP_NULL_CRC;

	INOH_LOCK(fcmh_2_inoh(f));
	pol = fcmh_2_ino(f)->ino_replpol;
	INOH_ULOCK(fcmh_2_inoh(f));

	BHREPL_POLICY_SET(b, pol);

	b->bcm_flags |= BMAP_NEW;
}

void
mds_bmap_ensure_valid(struct bmap *b)
{
	int rc, retifset[NBREPLST];

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;
	retifset[BREPLST_GARBAGE] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	retifset[BREPLST_TRUNCPNDG] = 1;
	retifset[BREPLST_TRUNCPNDG_SCHED] = 1;
	rc = mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT);
	if (!rc)
		DEBUG_BMAP(PLL_FATAL, b, "bmap has no valid replicas");
}

struct bmap_nonce_cbarg {
	struct bmap	*b;
	int		 update;
};

int
slm_bmap_resetnonce_cb(struct slm_sth *sth, void *p)
{
	struct bmap_nonce_cbarg *a = p;
	int idx, tract[NBREPLST];
	uint32_t nonce;

	nonce = sqlite3_column_int(sth->sth_sth, 0);
	if (nonce == sys_upnonce)
		return (0);

	a->update = 1;
	idx = mds_repl_ios_lookup(current_vfsid,
	    fcmh_2_inoh(a->b->bcm_fcmh),
	    sqlite3_column_int(sth->sth_sth, 1));
	psc_assert(idx >= 0);

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE;
	mds_repl_bmap_walk(a->b, tract, NULL, 0, &idx, 1);

	return (0);
}

void
slm_bmap_resetnonce(struct bmap *b)
{
	struct bmap_nonce_cbarg a;

	memset(&a, 0, sizeof(a));
	a.b = b;

	dbdo(slm_bmap_resetnonce_cb, &a,
	    " SELECT	nonce,"
	    "		resid"
	    " FROM	upsch"
	    " WHERE	fid = ?"
	    "   AND	bno = ?",
	    SQLITE_INTEGER64, bmap_2_fid(b),
	    SQLITE_INTEGER, b->bcm_bmapno);

	if (a.update) {
		dbdo(NULL, NULL,
		    " UPDATE	upsch "
		    " SET	nonce = ?"
		    " WHERE	fid = ?"
		    "   AND	bno = ?",
		    SQLITE_INTEGER, sys_upnonce,
		    SQLITE_INTEGER64, bmap_2_fid(b),
		    SQLITE_INTEGER, b->bcm_bmapno);
		mds_bmap_write_logrepls(b);
	}
}

/**
 * mds_bmap_read - Retrieve a bmap from the ondisk inode file.
 * @b: bmap.
 * Returns zero on success, negative errno code on failure.
 */
int
mds_bmap_read(struct bmap *b, __unusedx enum rw rw, int flags)
{
	int rc, vfsid, retifset[NBREPLST];
	uint64_t crc, od_crc = 0;
	struct slm_update_data *upd;
	struct fidc_membh *f;
	struct iovec iovs[2];
	size_t nb;

	upd = bmap_2_upd(b);
	upd_init(upd, UPDT_BMAP);
	UPD_UNBUSY(upd);

	f = b->bcm_fcmh;

	iovs[0].iov_base = bmap_2_ondisk(b);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    (off_t)BMAP_OD_SZ * b->bcm_bmapno + SL_BMAP_START_OFF,
	    bmap_2_mdsio_data(b));

	if (rc == 0 && nb == 0 && (flags & BMAPGETF_NOAUTOINST))
		return (SLERR_BMAP_INVALID);

	/*
	 * Check for a NULL CRC if we had a good read.  NULL CRC can
	 * happen when bmaps are gaps that have not been written yet.
	 * Note that a short read is tolerated as long as the bmap is
	 * zeroed.
	 */
	if (rc == 0) {
		if (nb == 0 || (nb == BMAP_OD_SZ && od_crc == 0 &&
		    pfl_memchk(bmap_2_ondisk(b), 0, BMAP_OD_CRCSZ))) {
			    mds_bmap_initnew(b);
			    DEBUG_BMAPOD(PLL_INFO, b,
				"initialized new bmap");
			    return (0);
		    }

		if (nb == BMAP_OD_SZ) {
			psc_crc64_calc(&crc, bmap_2_ondisk(b),
			    BMAP_OD_CRCSZ);
			if (od_crc != crc)
				rc = SLERR_BADCRC;
		}
	}

	/*
	 * At this point, the short I/O is an error since the bmap isn't
	 * zeros.
	 */
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "mdsio_read: rc=%d", rc);
		return (rc);
	}

	mds_bmap_ensure_valid(b);

	DEBUG_BMAPOD(PLL_INFO, b, "successfully loaded from disk");

	if (slm_opstate == SLM_OPSTATE_REPLAY)
		return (0);

	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	if (mds_repl_bmap_walk_all(b, NULL, retifset,
	    REPL_WALKF_SCIRCUIT))
		slm_bmap_resetnonce(b);
	return (0);
}

/**
 * mds_bmap_write - Update the on-disk data of bmap.  Note we must
 *	reserve journal log space if logf is given.
 * @update_mtime: we used to allow CRC update code path to update mtime
 *	if the generation number it carries matches what we have.  This
 *	is no longer used.  Now, only the client can update the mtime.
 *	The code will be removed after things are proven to be
 *	stabilized.
 */
int
mds_bmap_write(struct bmap *b, int update_mtime, void *logf,
    void *logarg)
{
	struct fidc_membh *f;
	struct iovec iovs[2];
	int rc, new, vfsid;
	uint64_t crc;
	size_t nb;

	BMAPOD_REQRDLOCK(bmap_2_bmi(b));
	mds_bmap_ensure_valid(b);

	psc_crc64_calc(&crc, bmap_2_ondisk(b), BMAP_OD_CRCSZ);

	iovs[0].iov_base = bmap_2_ondisk(b);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	if (logf)
		mds_reserve_slot(1);
	f = b->bcm_fcmh;
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	rc = mdsio_pwritev(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    (off_t)BMAP_OD_SZ * b->bcm_bmapno + SL_BMAP_START_OFF,
	    update_mtime, bmap_2_mdsio_data(b), logf, logarg);
	if (logf)
		mds_unreserve_slot(1);

	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;
	if (rc)
		DEBUG_BMAP(PLL_ERROR, b,
		    "mdsio_write: error (rc=%d)", rc);
	else
		DEBUG_BMAP(PLL_INFO, b, "written successfully");
	if (BMAPOD_HASRDLOCK(bmap_2_bmi(b)))
		BMAPOD_READ_DONE(b, 0);

	BMAP_LOCK(b);
//	psc_assert(b->bcm_flags & BMAP_BUSY);
	new = b->bcm_flags & BMAP_NEW;
	b->bcm_flags &= ~BMAP_NEW;
	BMAP_ULOCK(b);

	if (new)
		mdsio_fcmh_refreshattr(b->bcm_fcmh, NULL);
	return (rc);
}

void
mds_bmap_init(struct bmap *b)
{
	struct bmap_mds_info *bmi;

	bmi = bmap_2_bmi(b);
	pll_init(&bmi->bmi_leases, struct bmap_mds_lease,
	    bml_bmi_lentry, &b->bcm_lock);
	psc_rwlock_init(&bmi->bmi_rwlock);
}

void
mds_bmap_destroy(struct bmap *b)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	psc_assert(bmi->bmi_writers == 0);
	psc_assert(bmi->bmi_readers == 0);
	psc_assert(bmi->bmi_assign == NULL);
	psc_assert(pll_empty(&bmi->bmi_leases));
	psc_rwlock_destroy(&bmi->bmi_rwlock);
	upd_destroy(&bmi->bmi_upd);
}

/**
 * mds_bmap_crc_update - Handle CRC updates for one bmap by pushing
 *	the updates to ZFS and then log it.
 */
int
mds_bmap_crc_update(struct bmap *bmap, sl_ios_id_t iosid,
    struct srm_bmap_crcup *crcup)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);
	struct slash_inode_handle *ih;
	struct sl_mds_crc_log crclog;
	struct fidc_membh *f;
	struct srt_stat sstb;
	int rc, fl, idx, vfsid;
	uint32_t i;

	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	f = bmap->bcm_fcmh;
	ih = fcmh_2_inoh(f);

	rc = slfid_to_vfsid(fcmh_2_fid(f), &vfsid);
	if (rc)
		return (-rc);
	if (vfsid != current_vfsid)
		return (EINVAL);

	FCMH_WAIT_BUSY(f);
	idx = mds_repl_ios_lookup(vfsid, ih, iosid);
	if (idx < 0)
		psc_fatal("not found");
	sstb.sst_blocks = fcmh_2_nblks(f) + crcup->nblks -
	    fcmh_2_repl_nblks(f, idx);
	fl = SL_SETATTRF_NBLKS;

	fcmh_set_repl_nblks(f, idx, crcup->nblks);

	/* use nolog because mdslog_bmap_crc() will cover this */
	rc = mds_fcmh_setattr_nolog(vfsid, f, fl, &sstb);
	if (rc)
		psclog_error("unable to setattr: rc=%d", rc);

	FCMH_LOCK(f);

	if (idx >= SL_DEF_REPLICAS)
		mds_inox_write(vfsid, ih, NULL, NULL);
	else
		mds_inode_write(vfsid, ih, NULL, NULL);

	if (_mds_repl_inv_except(bmap, idx, 1)) {
		mds_bmap_write_logrepls(bmap);
	} else {
		BMAPOD_MODIFY_DONE(bmap, 0);
		BMAP_UNBUSY(bmap);
		FCMH_UNBUSY(f);
	}

	crclog.scl_bmap = bmap;
	crclog.scl_crcup = crcup;
	crclog.scl_iosid = iosid;

	BMAPOD_REQWRLOCK(bmi);
	for (i = 0; i < crcup->nups; i++) {
		bmap_2_crcs(bmap, crcup->crcs[i].slot) =
		    crcup->crcs[i].crc;
		bmap->bcm_crcstates[crcup->crcs[i].slot] =
		    BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_DIAG, bmap, "slot=%d crc=%"PSCPRIxCRC64,
		    crcup->crcs[i].slot, crcup->crcs[i].crc);
	}
	return (mds_bmap_write(bmap, 0, mdslog_bmap_crc, &crclog));
}

/**
 * mds_bmap_write_rel - Release a bmap after use.
 */
int
_mds_bmap_write_rel(const struct pfl_callerinfo *pci,
    struct bmap *b, void *logf)
{
	int rc;

	rc = mds_bmap_write(b, 0, logf, b);
	bmap_op_done(b);
	return (rc);
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	PFL_PRFLAG(BMAP_MDS_CRC_UP, &flags, &seq);
	PFL_PRFLAG(BMAP_MDS_CRCWRT, &flags, &seq);
	PFL_PRFLAG(BMAP_MDS_NOION, &flags, &seq);
	PFL_PRFLAG(BMAP_MDS_DIO, &flags, &seq);
	PFL_PRFLAG(BMAP_MDS_SEQWRAP, &flags, &seq);
	PFL_PRFLAG(BMAP_MDS_REPLMODWR, &flags, &seq);
	if (flags)
		printf(" unknown: %#x", flags);
	printf("\n");
}

void
dump_bml_flags(uint32_t flags)
{
	int seq = 0;

	PFL_PRFLAG(BML_READ, &flags, &seq);
	PFL_PRFLAG(BML_WRITE, &flags, &seq);
	PFL_PRFLAG(BML_DIO, &flags, &seq);
	PFL_PRFLAG(BML_DIOCB, &flags, &seq);
	PFL_PRFLAG(BML_TIMEOQ, &flags, &seq);
	PFL_PRFLAG(BML_BMI, &flags, &seq);
	PFL_PRFLAG(BML_RECOVER, &flags, &seq);
	PFL_PRFLAG(BML_CHAIN, &flags, &seq);
	PFL_PRFLAG(BML_FREEING, &flags, &seq);
	PFL_PRFLAG(BML_ASSFAIL, &flags, &seq);
	PFL_PRFLAG(BML_RECOVERFAIL, &flags, &seq);
	printf("\n");
}
#endif

struct bmap_ops bmap_ops = {
	mds_bmap_init,
	mds_bmap_read,
	NULL,
	mds_bmap_destroy
};
