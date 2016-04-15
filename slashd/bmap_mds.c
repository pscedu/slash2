/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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

#define PSC_SUBSYS SLSS_BMAP
#include "slsubsys.h"

#include "pfl/cdefs.h"
#include "pfl/fs.h"
#include "pfl/log.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "mdsio.h"
#include "repl_mds.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

static __inline void *
bmap_2_mfh(struct bmap *b)
{
	struct fcmh_mds_info *fmi;
	void *fh;

	fmi = fcmh_2_fmi(b->bcm_fcmh);
	fh = fmi->fmi_mfh.fh;
	psc_assert(fh);
	return (fh);
}

/*
 * Called when a read request offset exceeds the bounds of the file
 * causing a new bmap to be created.
 *
 * This causes other threads to block on the waitq until read/creation
 * has completed.
 *
 * Note: this bmap is not written to disk until a client actually writes
 * something to it.
 */
__static void
mds_bmap_initnew(struct bmap *b)
{
	struct fidc_membh *f = b->bcm_fcmh;
	struct bmap_mds_info *bmi;
	struct bmap_ondisk *bod;
	uint32_t pol;
	int i;

	bmi = bmap_2_bmi(b);
	bod = bmi_2_ondisk(bmi);
	for (i = 0; i < SLASH_SLVRS_PER_BMAP; i++)
		bod->bod_crcs[i] = BMAP_NULL_CRC;

	INOH_LOCK(fcmh_2_inoh(f));
	pol = fcmh_2_ino(f)->ino_replpol;
	INOH_ULOCK(fcmh_2_inoh(f));

	BHREPL_POLICY_SET(b, pol);

	bmi->bmi_sys_prio = -1;
	bmi->bmi_usr_prio = -1;
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
	/* 
	 * 04/13/2016: Hit this during log replay.
	 * 04/15/2016: Hit during bmap relay (B_REPLAY_OP_CRC).
	 */
	if (!rc) {
		if (slm_opstate == SLM_OPSTATE_NORMAL)
			DEBUG_BMAP(PLL_FATAL, b, "bmap has no valid replicas");
		else
			DEBUG_BMAP(PLL_WARN, b, "bmap has no valid replicas");
	}
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
	if (nonce == sl_sys_upnonce)
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
		    " UPDATE	upsch"
		    " SET	nonce = ?"
		    " WHERE	fid = ?"
		    "   AND	bno = ?",
		    SQLITE_INTEGER, sl_sys_upnonce,
		    SQLITE_INTEGER64, bmap_2_fid(b),
		    SQLITE_INTEGER, b->bcm_bmapno);
		mds_bmap_write_logrepls(b);
	}
}

/*
 * Retrieve a bmap from the on-disk inode file.
 * @b: bmap.
 * Returns zero on success, negative errno code on failure.
 */
int
mds_bmap_read(struct bmap *b, int flags)
{
	int rc, vfsid, retifset[NBREPLST];
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct slm_update_data *upd;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc = 0;
	size_t nb;

	upd = bmap_2_upd(b);
	upd_init(upd, UPDT_BMAP);
	UPD_UNBUSY(upd);

	if (flags & BMAPGETF_NODISKREAD) {
		mds_bmap_initnew(b);
		goto out2;
	}

	f = b->bcm_fcmh;

	iovs[0].iov_base = bmi_2_ondisk(bmi);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &od_crc;
	iovs[1].iov_len = sizeof(od_crc);

	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);

	psclog_diag("read bmap: handle=%p fid="SLPRI_FID" bmapno=%d",
	    bmap_2_mfh(b), f->fcmh_sstb.sst_fg.fg_fid, b->bcm_bmapno);

	rc = mdsio_preadv(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    (off_t)BMAP_OD_SZ * b->bcm_bmapno + SL_BMAP_START_OFF,
	    bmap_2_mfh(b));
	if (rc)
		goto out1;

	if (nb == 0 && (flags & BMAPGETF_NOAUTOINST))
		return (SLERR_BMAP_INVALID);

	/*
	 * Check for a NULL CRC if we had a good read.  NULL CRC can
	 * happen when bmaps are gaps that have not been written yet.
	 * Note that a short read is tolerated as long as the bmap is
	 * zeroed.
	 */
	if (nb == 0 || (nb == BMAP_OD_SZ && od_crc == 0 &&
	    pfl_memchk(bmi_2_ondisk(bmi), 0, BMAP_OD_CRCSZ))) {
		mds_bmap_initnew(b);
		DEBUG_BMAPOD(PLL_DIAG, b, "initialized new bmap, nb=%d",
		    nb);
		return (0);
	}

	if (nb == BMAP_OD_SZ) {
		psc_crc64_calc(&crc, bmi_2_ondisk(bmi), BMAP_OD_CRCSZ);
		if (od_crc != crc) {
			OPSTAT_INCR("badcrc");
			rc = PFLERR_BADCRC;
		}
	}

 out1:
	/*
	 * At this point, the short I/O is an error since the bmap isn't
	 * zeros.
	 */
	if (rc) {
		DEBUG_BMAP(PLL_ERROR, b, "mdsio_read: rc=%d", rc);
		return (rc);
	}

	mds_bmap_ensure_valid(b);

	DEBUG_BMAPOD(PLL_DIAG, b, "successfully loaded from disk");

 out2:
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

/*
 * Update the on-disk data of bmap.  Note we must reserve journal log
 * space if @logf is given.
 */
int
mds_bmap_write(struct bmap *b, void *logf, void *logarg)
{
	struct fidc_membh *f;
	struct iovec iovs[2];
	int rc, vfsid;
	uint64_t crc;
	size_t nb;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	BMAPOD_REQRDLOCK(bmi);
	mds_bmap_ensure_valid(b);

	psc_crc64_calc(&crc, bmi_2_ondisk(bmi), BMAP_OD_CRCSZ);

	iovs[0].iov_base = bmi_2_ondisk(bmi);
	iovs[0].iov_len = BMAP_OD_CRCSZ;
	iovs[1].iov_base = &crc;
	iovs[1].iov_len = sizeof(crc);

	f = b->bcm_fcmh;
	slfid_to_vfsid(fcmh_2_fid(f), &vfsid);

	psclog_diag("write bmap: handle=%p fid="SLPRI_FID" bmapno=%d",
	    bmap_2_mfh(b), f->fcmh_sstb.sst_fg.fg_fid, b->bcm_bmapno);

	if (logf)
		mds_reserve_slot(1);
	rc = mdsio_pwritev(vfsid, &rootcreds, iovs, nitems(iovs), &nb,
	    (off_t)BMAP_OD_SZ * b->bcm_bmapno + SL_BMAP_START_OFF,
	    bmap_2_mfh(b), logf, logarg);
	if (logf)
		mds_unreserve_slot(1);

	if (rc == 0 && nb != BMAP_OD_SZ)
		rc = SLERR_SHORTIO;
	if (rc)
		DEBUG_BMAP(PLL_ERROR, b,
		    "mdsio_write: error (rc=%d)", rc);
	else
		DEBUG_BMAP(PLL_DIAG, b, "written successfully");
	if (BMAPOD_HASRDLOCK(bmap_2_bmi(b)))
		BMAPOD_READ_DONE(b, 0);

	return (rc);
}

void
mds_bmap_init(struct bmap *b)
{
	struct bmap_mds_info *bmi;

	bmi = bmap_2_bmi(b);
	pll_init(&bmi->bmi_leases, struct bmap_mds_lease,
	    bml_bmi_lentry, &b->bcm_lock);
	pfl_rwlock_init(&bmi->bmi_rwlock);
}

void
mds_bmap_destroy(struct bmap *b)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	psc_assert(bmi->bmi_writers == 0);
	psc_assert(bmi->bmi_readers == 0);
	psc_assert(bmi->bmi_assign == NULL);
	psc_assert(pll_empty(&bmi->bmi_leases));
	pfl_rwlock_destroy(&bmi->bmi_rwlock);
	upd_destroy(&bmi->bmi_upd);
}

/*
 * Handle CRC updates for one bmap by pushing the updates to ZFS and
 * then log it.
 */
int
mds_bmap_crc_update(struct bmap *bmap, sl_ios_id_t iosid,
    struct srt_bmap_crcup *crcup)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);
	struct slash_inode_handle *ih;
	struct sl_mds_crc_log crclog;
	struct fidc_membh *f;
	struct srt_stat sstb;
	int rc, fl, idx, vfsid;
	uint32_t i;

	psc_assert(bmap->bcm_flags & BMAPF_CRC_UP);

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

	/*
	 * Only update the block usage when there is a real change.
	 */
	if (crcup->nblks != fcmh_2_repl_nblks(f, idx)) {
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
	}

	if (_mds_repl_inv_except(bmap, idx, 1)) {
		/* XXX why are we writing the bmap twice??? */
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
		bmi->bmi_crcstates[crcup->crcs[i].slot] =
		    BMAP_SLVR_DATA | BMAP_SLVR_CRC;

		DEBUG_BMAP(PLL_DIAG, bmap, "slot=%d crc=%"PSCPRIxCRC64,
		    crcup->crcs[i].slot, crcup->crcs[i].crc);
	}
	return (mds_bmap_write(bmap, mdslog_bmap_crc, &crclog));
}

void
dump_bmapod(struct bmap *bmap)
{
	DEBUG_BMAPOD(PLL_MAX, bmap, "");
}

void
_dump_bmapod(const struct pfl_callerinfo *pci, int level,
    struct bmap *bmap, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	_dump_bmapodv(pci, level, bmap, fmt, ap);
	va_end(ap);
}

#define DUMP_BMAP_REPLS(repls, buf)					\
	do {								\
		int _k, off, ch[NBREPLST];				\
									\
		ch[BREPLST_INVALID] = '-';				\
		ch[BREPLST_REPL_SCHED] = 's';				\
		ch[BREPLST_REPL_QUEUED] = 'q';				\
		ch[BREPLST_VALID] = '+';				\
		ch[BREPLST_TRUNCPNDG] = 't';				\
		ch[BREPLST_TRUNCPNDG_SCHED] = 'p';			\
		ch[BREPLST_GARBAGE] = 'g';				\
		ch[BREPLST_GARBAGE_SCHED] = 'x';			\
									\
		for (_k = 0, off = 0; _k < SL_MAX_REPLICAS;		\
		    _k++, off += SL_BITS_PER_REPLICA)			\
			(buf)[_k] = ch[SL_REPL_GET_BMAP_IOS_STAT(repls,	\
			    off)];					\
		while (_k > 1 && (buf)[_k - 1] == '-')			\
			_k--;						\
		(buf)[_k] = '\0';					\
	} while (0)

void
_dump_bmap_repls(FILE *fp, uint8_t *repls)
{
	char rbuf[SL_MAX_REPLICAS + 1];

	DUMP_BMAP_REPLS(repls, rbuf);
	fprintf(fp, "%s\n", rbuf);
}

void
dump_bmap_repls(uint8_t *repls)
{
	_dump_bmap_repls(stderr, repls);
}

void
_dump_bmapodv(const struct pfl_callerinfo *pci, int level,
    struct bmap *bmap, const char *fmt, va_list ap)
{
	char mbuf[LINE_MAX], rbuf[SL_MAX_REPLICAS + 1],
	     cbuf[SLASH_SLVRS_PER_BMAP + 1];
	int k;
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);

	vsnprintf(mbuf, sizeof(mbuf), fmt, ap);

	DUMP_BMAP_REPLS(bmi->bmi_repls, rbuf);

	for (k = 0; k < SLASH_SLVRS_PER_BMAP; k++)
		if (bmi->bmi_crcstates[k] > 9)
			cbuf[k] = 'a' + bmi->bmi_crcstates[k] - 10;
		else
			cbuf[k] = '0' + bmi->bmi_crcstates[k];
	while (k > 1 && cbuf[k - 1] == '0')
		k--;
	cbuf[k] = '\0';

	_DEBUG_BMAP(pci, level, bmap, "repls={%s} crcstates=[0x%s] %s",
	    rbuf, cbuf, mbuf);
}

#if PFL_DEBUG > 0
void
dump_bmap_flags(uint32_t flags)
{
	int seq = 0;

	_dump_bmap_flags_common(&flags, &seq);
	PFL_PRFLAG(BMAPF_CRC_UP, &flags, &seq);
	PFL_PRFLAG(BMAPF_NOION, &flags, &seq);
	PFL_PRFLAG(BMAPF_REPLMODWR, &flags, &seq);
	PFL_PRFLAG(BMAPF_IOSASSIGNED, &flags, &seq);
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

struct bmap_ops sl_bmap_ops = {
	NULL,				/* bmo_reapf() */
	mds_bmap_init,			/* bmo_init_privatef() */
	mds_bmap_read,			/* bmo_retrievef() */
	NULL,				/* bmo_mode_chngf() */
	mds_bmap_destroy		/* bmo_final_cleanupf() */
};
