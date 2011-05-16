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

#include "pfl/cdefs.h"
#include "psc_util/log.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "mdsio.h"
#include "repl_mds.h"
#include "slerr.h"

static __inline void *
bmap_2_mdsio_data(struct bmapc_memb *bmap)
{
	struct fcmh_mds_info *fmi;

	fmi = fcmh_2_fmi(bmap->bcm_fcmh);
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
 * @bcm: bmap.
 * Returns zero on success, negative errno code on failure.
 */
int
mds_bmap_read(struct bmapc_memb *bcm, __unusedx enum rw rw, int flags)
{
	size_t nb;
	int rc;

	rc = mdsio_read(&rootcreds, bmap_2_ondisk(bcm), BMAP_OD_SZ,
	    &nb, (off_t)BMAP_OD_SZ * bcm->bcm_bmapno +
	    SL_BMAP_START_OFF, bmap_2_mdsio_data(bcm));

	if (rc == 0 && nb == 0 && (flags & BMAPGETF_NOAUTOINST))
		return (SLERR_BMAP_INVALID);

	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;

	/*
	 * Check for a NULL CRC if we had a good read.  NULL CRC can
	 * happen when bmaps are gaps that have not been written yet.
	 * Note that a short read is tolerated as long as the bmap is
	 * zeroed.
	 */
	if (!rc || rc == SLERR_SHORTIO) {
		if (bmap_2_ondiskcrc(bcm) == 0 &&
		    pfl_memchk(bmap_2_ondisk(bcm), 0, BMAP_OD_SZ)) {
			mds_bmap_initnew(bcm);
			DEBUG_BMAPOD(PLL_INFO, bcm, "initialized");
			return (0);
		}
	}

	/*
	 * At this point, the short I/O is an error since the bmap isn't
	 *    zeros.
	 */
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, bcm, "mdsio_read: rc=%d", rc);
		return (rc);
	}

	mds_bmap_ensure_valid(bcm);

	DEBUG_BMAPOD(PLL_INFO, bcm, "successfully loaded from disk");
	return (0);
}

int
mds_bmap_write(struct bmapc_memb *bmap, int update_mtime, void *logf,
    void *logarg)
{
	struct iovec iovs[2];
	uint64_t crc;
	size_t nb;
	int rc;

	BMAPOD_REQRDLOCK(bmap_2_bmi(bmap));
	mds_bmap_ensure_valid(bmap);

	psc_crc64_calc(&crc, bmap_2_ondisk(bmap), BMAP_OD_CRCSZ);

	iovs[0].iov_base = bmap_2_ondisk(bmap);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	if (logf)
		mds_reserve_slot();
	rc = mdsio_pwritev(&rootcreds, iovs, nitems(iovs), &nb,
	    (off_t)((BMAP_OD_SZ * bmap->bcm_bmapno) +
	    SL_BMAP_START_OFF), update_mtime, bmap_2_mdsio_data(bmap),
	    logf, logarg);
	if (logf)
		mds_unreserve_slot();

	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;
	if (rc)
		DEBUG_BMAP(PLL_ERROR, bmap,
		    "mdsio_write: error (rc=%d)", rc);
	else
		DEBUG_BMAP(PLL_INFO, bmap, "written successfully");
	BMAPOD_READ_DONE(bmap, 0);
	return (rc);
}

void
mds_bmap_init(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmi;

	bmi = bmap_2_bmi(bcm);
	pll_init(&bmi->bmdsi_leases, struct bmap_mds_lease,
	    bml_bmdsi_lentry, &bcm->bcm_lock);
	bmi->bmdsi_xid = 0;
	psc_rwlock_init(&bmi->bmdsi_rwlock);
}

void
mds_bmap_destroy(struct bmapc_memb *bcm)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(bcm);

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
	struct sl_mds_crc_log crclog;
	struct fidc_membh *f;
	uint32_t utimgen, i;
	int extend = 0;

	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	f = bmap->bcm_fcmh;
	FCMH_LOCK(f);
	if (crcup->fsize > fcmh_2_fsz(f))
		extend = 1;
	mds_fcmh_increase_fsz(f, crcup->fsize);
	utimgen = f->fcmh_sstb.sst_utimgen;
	FCMH_ULOCK(f);

	if (utimgen < crcup->utimgen)
		DEBUG_FCMH(PLL_ERROR, f,
		   "utimgen %d < crcup->utimgen %d",
		   utimgen, crcup->utimgen);

	crcup->extend = extend;
	crclog.scl_bmap = bmap;
	crclog.scl_crcup = crcup;

	BMAPOD_WRLOCK(bmi);
	for (i = 0; i < crcup->nups; i++) {
		bmap_2_crcs(bmap, crcup->crcs[i].slot) =
		    crcup->crcs[i].crc;
		bmap->bcm_crcstates[crcup->crcs[i].slot] =
		    BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_INFO, bmap, "slot(%d) crc(%"PSCPRIxCRC64")",
		    crcup->crcs[i].slot, crcup->crcs[i].crc);
	}
	return (mds_bmap_write(bmap, utimgen == crcup->utimgen,
	    mds_bmap_crc_log, &crclog));
}

/**
 * mds_bmap_repl_update - We update bmap replication status in two cases:
 *	(1) An MDS issues a write lease to a client.
 *	(2) An MDS performs a replicate request.
 */
int
mds_bmap_repl_update(struct bmapc_memb *bmap, int log)
{
	return (mds_bmap_write(bmap, 0,
	    log ? mds_bmap_repl_log : NULL, bmap));
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags(&flags, &seq);
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
	printf("\n");
}
#endif

struct bmap_ops bmap_ops = {
	mds_bmap_init,
	mds_bmap_read,
	NULL,
	mds_bmap_destroy
};
