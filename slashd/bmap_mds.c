/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2017, Pittsburgh Supercomputing Center
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
#include "slashd.h"
#include "slerr.h"
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

extern int debug_ondisk_inode;

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

	bmap_2_replpol(b) = pol;
	BHGEN_SET(b, &sl_sys_upnonce);

	bmi->bmi_sys_prio = -1;
	bmi->bmi_usr_prio = -1;
	OPSTAT_INCR("bmap-init");
}

void
mds_bmap_ensure_valid(struct bmap *b)
{
	int rc, retifset[NBREPLST];

	brepls_init(retifset, 0);
	retifset[BREPLST_VALID] = 1;
	retifset[BREPLST_GARBAGE_QUEUED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;
	retifset[BREPLST_TRUNC_QUEUED] = 1;
	retifset[BREPLST_TRUNC_SCHED] = 1;

	/* Caller should busy fcmh and bmap. */
	rc = mds_repl_bmap_walk_all(b, NULL, retifset, REPL_WALKF_SCIRCUIT);

	/* 
	 * 04/13/2016 & 04/15/2016: Hit during bmap relay (B_REPLAY_OP_CRC).
	 *
	 * 06/16/2016: Hit when called by slm_rmc_handle_getbmap() and the
	 *             number of replicas is zero even if the first bmap has 
	 *             a valid state.
	 *
	 * ((struct fcmh_mds_info *)(b->bcm_fcmh + 1))->fmi_inodeh.inoh_ino.ino_nrepls
	 *
	 * ((struct bmap_mds_info *)(b+1))->bmi_corestate.bcs_repls
	 *
	 */
	if (rc) 
		return;

	/*
 	 * See this during normal operation.  However, it can recover if we
 	 * request a bmap from an IOS. So let us warn instead of crash.
 	 */
	DEBUG_BMAP(PLL_WARN, b, "no valid replicas, bno = %d, fid = "SLPRI_FID,
	    b->bcm_bmapno, fcmh_2_fid(b->bcm_fcmh)); 
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
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_QUEUED;
	mds_repl_bmap_walk(a->b, tract, NULL, 0, &idx, 1);

	return (0);
}

/* Introduced by commit 18a5f376d02847e075461819d4b315d228bcfde6 (07/13) */
static void
slm_bmap_resetnonce(struct bmap *b)
{
	int tract[NBREPLST];
	int rc, retifset[NBREPLST];
#if 0
	struct bmap_nonce_cbarg a;

	memset(&a, 0, sizeof(a));
	a.b = b;

	/*
 	 * XXX we have to do this each time the bmap is evicted
 	 * from the cache or we reboot.
 	 *
 	 * Instead of associating a nonce with each bmap, we can
 	 * associating it with the whole table.
 	 *
 	 */
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
#endif

	brepls_init(tract, -1);
	tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
	tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_QUEUED;
	brepls_init(retifset, 0);
	retifset[BREPLST_REPL_SCHED] = 1;
	retifset[BREPLST_GARBAGE_SCHED] = 1;

	rc = mds_repl_bmap_walk_all(b, tract, retifset, 0);
	if (rc) {
		OPSTAT_INCR("bmap-requeue-normal");
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
	int rc, new, vfsid;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);
	struct slm_update_data *upd;
	struct fidc_membh *f;
	struct iovec iovs[2];
	uint64_t crc, od_crc = 0;
	size_t nb;
	sl_bmapgen_t bgen;

	upd = bmap_2_upd(b);
	upd_init(upd);

	new = 0;
	f = b->bcm_fcmh;

	if (flags & BMAPGETF_NODISKREAD) {
		new = 1;
		mds_bmap_initnew(b);
		goto out2;
	}
	OPSTAT_INCR("bmap-read");

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
		DEBUG_BMAPOD(PLL_DIAG, b, "initialized new bmap, nb=%d", nb);
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

	OPSTAT_INCR("bmap-load");
	DEBUG_BMAPOD(PLL_DIAG, b, "successfully loaded from disk");

 out2:

	BMAP_LOCK(b);
	if (!new)
		/* 
		 * gdb help:
		 *
		 * ((struct bmap_mds_info*)(b+1))->bmi_corestate.bcs_repls 
		 */
		mds_bmap_ensure_valid(b);

	/*
 	 * During the REPLAY stage, we rely on slm_upsch_revert_cb() to 
 	 * do the work.
 	 *
 	 * During the NORMAL stage, we rely on the generation number to
 	 * do the work.
 	 *
	 * (gdb) p ((struct bmap_mds_info *)(b+1))->bmi_extrastate.bes_gen
 	 *
 	 */
	BHGEN_GET(b, &bgen);
	if (bgen == sl_sys_upnonce) {
		OPSTAT_INCR("bmap-gen-same");
	} else {
		OPSTAT_INCR("bmap-gen-diff");
		BHGEN_SET(b, &sl_sys_upnonce);
		if (slm_opstate != SLM_OPSTATE_REPLAY) {
			/*
 			 * If we were scheduled by a previous incarnation 
 			 * of MDS, revert SCHED to QUEUED.
 			 */
			slm_bmap_resetnonce(b);
		}
	}

	BMAP_ULOCK(b);
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
	int rc, vfsid, level;
	uint64_t crc;
	size_t nb;
	struct slm_wkdata_wr_brepl *wk;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	OPSTAT_INCR("bmap-write");
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

	level = debug_ondisk_inode ? PLL_MAX : (rc ? PLL_ERROR : PLL_DIAG);
	DEBUG_BMAP(level, b, "mdsio_pwritev: bno = %d, rc=%d", 
	    b->bcm_bmapno, rc);

	if (!rc && logf == (void *)mdslog_bmap_repls) {
		/*
		 * Schedule an update of the SQLite database.
		 */
		BMAP_LOCK_ENSURE(b);
		b->bcm_flags |= BMAPF_REPLMODWR;
		psc_assert(slm_opstate == SLM_OPSTATE_NORMAL);
		wk = pfl_workq_getitem(slm_wkcb_wr_brepl,
		    struct slm_wkdata_wr_brepl);
		wk->b = b;
		bmap_op_start_type(b, BMAP_OPCNT_WORK);
		/*
 		 * Under massive deletion workload, we might be
 		 * starved, which causes delay on replication work.
 		 */
		pfl_workq_putitem(wk);
		OPSTAT_INCR("bmap-write-log");
	}

	return (rc);
}

void
mds_bmap_init(struct bmap *b)
{
	struct bmap_mds_info *bmi;

	bmi = bmap_2_bmi(b);
	pll_init(&bmi->bmi_leases, struct bmap_mds_lease,
	    bml_bmi_lentry, &b->bcm_lock);

	bmi->bmi_sys_prio = -1;
	bmi->bmi_usr_prio = -1;
}

void
mds_bmap_destroy(struct bmap *b)
{
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	psc_assert(bmi->bmi_writers == 0);
	psc_assert(bmi->bmi_readers == 0);
	psc_assert(pll_empty(&bmi->bmi_leases));
	upd_destroy(&bmi->bmi_upd);
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

/* Keep the symbols in sync with fnstat_prdat() */
#define DUMP_BMAP_REPLS(repls, buf)					\
	do {								\
		int _k, off, ch[NBREPLST];				\
									\
		ch[BREPLST_INVALID] = '-';				\
		ch[BREPLST_REPL_SCHED] = 's';				\
		ch[BREPLST_REPL_QUEUED] = 'q';				\
		ch[BREPLST_VALID] = '+';				\
		ch[BREPLST_TRUNC_QUEUED] = 't';				\
		ch[BREPLST_TRUNC_SCHED] = 'p';				\
		ch[BREPLST_GARBAGE_QUEUED] = 'g';			\
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
	PFL_PRFLAG(BML_RECOVERFAIL, &flags, &seq);
	printf("\n");
}
#endif

struct bmap_ops sl_bmap_ops = {
	mds_bmap_init,			/* bmo_init_privatef() */
	mds_bmap_read,			/* bmo_retrievef() */
	NULL,				/* bmo_mode_chngf() */
	mds_bmap_destroy		/* bmo_final_cleanupf() */
};
