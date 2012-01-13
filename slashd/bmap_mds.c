/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

static __inline void *
bmap_2_mdsio_data(struct bmapc_memb *b)
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
mds_bmap_initnew(struct bmapc_memb *b)
{
	struct bmap_ondisk *bod = bmap_2_ondisk(b);
	struct fidc_membh *fcmh = b->bcm_fcmh;
	uint32_t pol;
	int i;

	for (i = 0; i < SLASH_CRCS_PER_BMAP; i++)
		bod->bod_crcs[i] = BMAP_NULL_CRC;

	INOH_LOCK(fcmh_2_inoh(fcmh));
	pol = fcmh_2_ino(fcmh)->ino_replpol;
	INOH_ULOCK(fcmh_2_inoh(fcmh));

	BHREPL_POLICY_SET(b, pol);

	b->bcm_flags |= BMAP_NEW;
}

void
mds_bmap_ensure_valid(struct bmapc_memb *b)
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

/**
 * mds_bmap_read - Retrieve a bmap from the ondisk inode file.
 * @b: bmap.
 * Returns zero on success, negative errno code on failure.
 */
int
mds_bmap_read(struct bmapc_memb *b, __unusedx enum rw rw, int flags)
{
	uint64_t crc, od_crc = 0;
	struct iovec iovs[2];
	size_t nb;
	int rc;

	iovs[0].iov_base = bmap_2_ondisk(b);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);
	rc = mdsio_preadv(&rootcreds, iovs, nitems(iovs), &nb,
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
			    DEBUG_BMAPOD(PLL_INFO, b, "initialized new bmap");
			    return (0);
		    }

		if (nb == BMAP_OD_SZ) {
			psc_crc64_calc(&crc, bmap_2_ondisk(b), BMAP_OD_CRCSZ);
			if (od_crc != crc)
				rc = SLERR_BADCRC;
		}
	}

	/*
	 * At this point, the short I/O is an error since the bmap isn't
	 *    zeros.
	 */
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "mdsio_read: rc=%d", rc);
		return (rc);
	}

	mds_bmap_ensure_valid(b);

	DEBUG_BMAPOD(PLL_INFO, b, "successfully loaded from disk");
	return (0);
}

/**
 * mds_bmap_write - Update a bmap of an inode.  Note we must reserve log
 *     space if logf is given.
 *
 * update_mtime: we used to allow CRC update code path to update mtime if
 * the generation number it carries matches what we have.  This is no
 * long used.  Now, only the client can update the mtime.  The code will
 * be removed after things are proven to be stablized.
 */
int
mds_bmap_write(struct bmapc_memb *b, int update_mtime, void *logf,
    void *logarg)
{
	struct iovec iovs[2];
	int locked, rc, new;
	uint64_t crc;
	size_t nb;

	BMAPOD_REQRDLOCK(bmap_2_bmi(b));
	if (BMAP_HASLOCK(b))
		BMAP_ULOCK(b);
	mds_bmap_ensure_valid(b);

	psc_crc64_calc(&crc, bmap_2_ondisk(b), BMAP_OD_CRCSZ);

	iovs[0].iov_base = bmap_2_ondisk(b);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	if (logf)
		mds_reserve_slot(1);
	rc = mdsio_pwritev(&rootcreds, iovs, nitems(iovs), &nb,
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
	BMAPOD_READ_DONE(b, 0);

	locked = BMAP_RLOCK(b);
	new = b->bcm_flags & BMAP_NEW;
	b->bcm_flags &= ~BMAP_NEW;
	BMAP_URLOCK(b, locked);

	if (new)
		mdsio_fcmh_refreshattr(b->bcm_fcmh, NULL);
	return (rc);
}

void
mds_bmap_init(struct bmapc_memb *b)
{
	struct bmap_mds_info *bmi;

	bmi = bmap_2_bmi(b);
	pll_init(&bmi->bmdsi_leases, struct bmap_mds_lease,
	    bml_bmdsi_lentry, &b->bcm_lock);
	bmi->bmdsi_xid = 0;
	psc_rwlock_init(&bmi->bmdsi_rwlock);
}

void
mds_bmap_destroy(struct bmapc_memb *b)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	psc_assert(bmi->bmdsi_writers == 0);
	psc_assert(bmi->bmdsi_readers == 0);
	psc_assert(bmi->bmdsi_assign == NULL);
	psc_assert(pll_empty(&bmi->bmdsi_leases));
	psc_rwlock_destroy(&bmi->bmdsi_rwlock);
}

/**
 * mds_bmap_crc_update - Handle CRC updates for one bmap by pushing
 *	the updates to ZFS and then log it.
 */
int
mds_bmap_crc_update(struct bmapc_memb *bmap,
    struct srm_bmap_crcup *crcup)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);
	struct slash_inode_handle *ih;
	struct sl_mds_crc_log crclog;
	struct fidc_membh *f;
	struct srt_stat sstb;
	uint32_t i;
	sl_ios_id_t iosid;
	int fl, idx;

	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	f = bmap->bcm_fcmh;
	ih = fcmh_2_inoh(f);

	FCMH_LOCK(f);
	fcmh_wait_locked(f, f->fcmh_flags & FCMH_IN_SETATTR);

	iosid = bmi->bmdsi_wr_ion->rmmi_resm->resm_res_id;
	idx = mds_repl_ios_lookup(ih, iosid);
	if (idx < 0)
		psc_fatal("not found");
	sstb.sst_blocks = fcmh_2_nblks(f) + crcup->nblks -
	    fcmh_2_repl_nblks(f, idx);
	fl = SL_SETATTRF_NBLKS;

	/* use nolog because mdslog_bmap_crc will cover this */
	mds_fcmh_setattr_nolog(f, fl, &sstb);

	fcmh_set_repl_nblks(f, idx, crcup->nblks);
	if (idx >= SL_DEF_REPLICAS)
		mds_inox_write(ih, NULL, NULL);
	else
		mds_inode_write(ih, NULL, NULL);

	FCMH_ULOCK(f);

	crclog.scl_bmap = bmap;
	crclog.scl_crcup = crcup;

	BMAPOD_REQWRLOCK(bmi);
	for (i = 0; i < crcup->nups; i++) {
		bmap_2_crcs(bmap, crcup->crcs[i].slot) =
		    crcup->crcs[i].crc;
		bmap->bcm_crcstates[crcup->crcs[i].slot] =
		    BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_INFO, bmap, "slot(%d) crc(%"PSCPRIxCRC64")",
		    crcup->crcs[i].slot, crcup->crcs[i].crc);
	}
	return (mds_bmap_write(bmap, 0, mdslog_bmap_crc, &crclog));
}

/**
 * mds_bmap_write_rel - Release a bmap after use.
 */
int
_mds_bmap_write_rel(const struct pfl_callerinfo *pci,
    struct bmapc_memb *b, void *logf)
{
	int rc;

	/* XXX check that we set this */
//	psc_assert(b->bcm_flags & BMAP_BUSY);

	rc = mds_bmap_write(b, 0, logf, b);
	bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
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
	PFL_PRFLAG(BML_CDIO, &flags, &seq);
	PFL_PRFLAG(BML_COHRLS, &flags, &seq);
	PFL_PRFLAG(BML_COHDIO, &flags, &seq);
	PFL_PRFLAG(BML_EXP, &flags, &seq);
	PFL_PRFLAG(BML_TIMEOQ, &flags, &seq);
	PFL_PRFLAG(BML_BMDSI, &flags, &seq);
	PFL_PRFLAG(BML_COH, &flags, &seq);
	PFL_PRFLAG(BML_RECOVER, &flags, &seq);
	PFL_PRFLAG(BML_CHAIN, &flags, &seq);
	PFL_PRFLAG(BML_UPGRADE, &flags, &seq);
	PFL_PRFLAG(BML_EXPFAIL, &flags, &seq);
	PFL_PRFLAG(BML_FREEING, &flags, &seq);
	PFL_PRFLAG(BML_ASSFAIL, &flags, &seq);
	PFL_PRFLAG(BML_RECOVERPNDG, &flags, &seq);
	PFL_PRFLAG(BML_REASSIGN, &flags, &seq);
	printf("\n");
}
#endif

struct bmap_ops bmap_ops = {
	mds_bmap_init,
	mds_bmap_read,
	NULL,
	mds_bmap_destroy
};
