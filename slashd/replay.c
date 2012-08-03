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

#include <errno.h>

#include "pfl/fs.h"

#include "bmap_mds.h"
#include "fidc_mds.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "namespace.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "slerr.h"

#define B_REPLAY_OP_CRC		0
#define B_REPLAY_OP_REPLS	1

#define I_REPLAY_OP_REPLS	0

/**
 * mds_replay_bmap - Replay an operation on a bmap.
 */
static int
mds_replay_bmap(void *jent, int op)
{
	struct slmds_jent_bmap_repls *sjbr = jent;
	struct slmds_jent_bmap_crc *sjbc = jent;
	struct srt_bmap_crcwire *bmap_wire;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	struct slash_fidgen fg;
	int i, rc;
	struct {
		slfid_t		fid;
		sl_bmapno_t	bno;
	} *cp = jent;

	fg.fg_fid = cp->fid;
	fg.fg_gen = FGEN_ANY;
	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		goto out;

	rc = mds_bmap_load(f, cp->bno, &b);
	if (rc)
		goto out;

	switch (op) {
	case B_REPLAY_OP_REPLS:
		mds_brepls_check(sjbr->sjbr_repls, sjbr->sjbr_nrepls);
		bmap_2_replpol(b) = sjbr->sjbr_replpol;
		memcpy(b->bcm_repls, sjbr->sjbr_repls,
		    SL_REPLICA_NBYTES);
		/* XXX update db entries */
		break;
	case B_REPLAY_OP_CRC: {
		struct slash_inode_handle *ih;
		struct srt_stat sstb;
		int fl, idx;

		FCMH_LOCK(f);
		ih = fcmh_2_inoh(f);
		idx = mds_repl_ios_lookup(ih, sjbc->sjbc_iosid);
		if (idx < 0) {
			psclog_errorx("iosid %d not found in repl "
			    "table", sjbc->sjbc_iosid);
			goto out;
		}
		sstb.sst_blocks = sjbc->sjbc_aggr_nblks;
		fcmh_set_repl_nblks(f, idx, sjbc->sjbc_repl_nblks);
		if (idx >= SL_DEF_REPLICAS)
			rc = mds_inox_write(ih, NULL, NULL);
		else
			rc = mds_inode_write(ih, NULL, NULL);
		if (rc)
			goto out;

		fl = SL_SETATTRF_NBLKS;

		/* Apply the filesize from the journal entry.
		 */
		if (sjbc->sjbc_extend) {
			sstb.sst_size = sjbc->sjbc_fsize;
			fl |= PSCFS_SETATTRF_DATASIZE;
		}
		rc = mds_fcmh_setattr_nolog(f, fl, &sstb);
		if (rc)
			goto out;

		for (i = 0; i < sjbc->sjbc_ncrcs; i++) {
			bmap_wire = &sjbc->sjbc_crc[i];
			bmap_2_crcs(b, bmap_wire->slot) = bmap_wire->crc;
			b->bcm_crcstates[bmap_wire->slot] |=
			    BMAP_SLVR_DATA | BMAP_SLVR_CRC;
		}
		break;
	    }
	}

	DEBUG_BMAPOD(PLL_NOTIFY, b, "replayed bmap op=%d", op);

	rc = mds_bmap_write(b, 0, NULL, NULL);

 out:
	if (b)
		bmap_op_done_type(b, BMAP_OPCNT_LOOKUP);
	if (f)
		fcmh_op_done(f);
	return (rc);
}

static int
mds_replay_bmap_repls(struct psc_journal_enthdr *pje)
{
	return (mds_replay_bmap(PJE_DATA(pje),
	    B_REPLAY_OP_REPLS));
}

/**
 * mds_replay_bmap_crc - Replay a CRC update.  Because we only log
 *     CRCs that have been changed in the bmap, this has to be a
 *     read-modify-write process.
 */
static int
mds_replay_bmap_crc(struct psc_journal_enthdr *pje)
{
	return (mds_replay_bmap(PJE_DATA(pje), B_REPLAY_OP_CRC));
}

static int
mds_replay_bmap_seq(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_bmapseq *sjsq;

	sjsq = PJE_DATA(pje);

	if (mds_cursor.pjc_seqno_hwm < sjsq->sjbsq_high_wm)
		mds_cursor.pjc_seqno_hwm = sjsq->sjbsq_high_wm;
	if (mds_cursor.pjc_seqno_lwm < sjsq->sjbsq_low_wm)
		mds_cursor.pjc_seqno_lwm = sjsq->sjbsq_low_wm;

	return (0);
}

/**
 * mds_replay_ino - Replay an inode update.
 */
static int
mds_replay_ino(void *jent, int op)
{
	struct slmds_jent_ino_repls *sjir = jent;
	struct slash_inode_handle *ih = NULL;
	struct slash_fidgen fg;
	struct fidc_membh *f;
	int j, rc;

	if (sjir->sjir_fid == FID_ANY) {
		psclog_errorx("cannot replay on FID_ANY");
		return (EINVAL);
	}

	fg.fg_fid = sjir->sjir_fid;
	fg.fg_gen = FGEN_ANY;
	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		goto out;

	/* It's possible this replay created this inode. */
	ih = fcmh_2_inoh(f);
	INOH_LOCK(ih);

	switch (op) {
	case I_REPLAY_OP_REPLS:
		for (j = 0; j < SL_MAX_REPLICAS; j++)
			if (sjir->sjir_repls[j])
				break;
		psc_assert(j != SL_MAX_REPLICAS);

		/*
		 * We always update the inode itself because the number of
		 * replicas is stored there.
		 */
		if (ih->inoh_flags & INOH_INO_NEW) {
			if (sjir->sjir_repls[0] == 0)
				psclog_errorx("ino_repls[0] should be set for "
				    "newly created inode");
			if (sjir->sjir_nrepls != 1)
				psclog_errorx("ino_nrepls (%d) in "
				    "should be 1 for newly created inode",
				    sjir->sjir_nrepls);
		}

		psc_assert(sjir->sjir_nrepls <= SL_MAX_REPLICAS);

		if (sjir->sjir_nrepls > SL_DEF_REPLICAS) {
			mds_inox_ensure_loaded(ih);
			memcpy(ih->inoh_extras->inox_repls,
			    &sjir->sjir_repls[SL_DEF_REPLICAS],
			    sizeof(ih->inoh_extras->inox_repls));
			rc = mds_inox_write(ih, NULL, NULL);
			if (rc)
				goto out;

			DEBUG_INOH(PLL_DEBUG, ih, "replayed inox_repls");
		}

		ih->inoh_ino.ino_replpol = sjir->sjir_replpol;
		ih->inoh_ino.ino_nrepls = sjir->sjir_nrepls;
		memcpy(ih->inoh_ino.ino_repls, sjir->sjir_repls,
		    sizeof(ih->inoh_ino.ino_repls));
		break;
	default:
		psc_fatalx("unknown op");
	}

	rc = mds_inode_write(ih, NULL, NULL);

 out:
	if (ih)
		INOH_ULOCK(ih);
	psclog(rc ? PLL_ERROR : PLL_DEBUG,
	    "fid="SLPRI_FID" rc=%d", fg.fg_fid, rc);
	if (f)
		fcmh_op_done(f);
	return (rc);
}

static int
mds_replay_ino_repls(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_ino_repls *sjir;
	int rc;

	sjir = PJE_DATA(pje);
	rc = mdsio_redo_fidlink(sjir->sjir_fid, &rootcreds);
	if (!rc)
		rc = mds_replay_ino(sjir, I_REPLAY_OP_REPLS);
	return (rc);
}

/**
 * mds_replay_bmap_assign - Replay a bmap assignment update.
 */
static int
mds_replay_bmap_assign(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_assign_rep *logentry;
	struct slmds_jent_bmap_assign *sjba;
	struct bmap_ios_assign *bia;
	struct odtable_entftr *odtf;
	struct odtable_hdr odth;
	size_t nb, len, elem;
	void *p, *handle;
	mdsio_fid_t mf;
	uint64_t crc;
	int rc;

	logentry = PJE_DATA(pje);
	elem = logentry->sjar_elem;
	if (logentry->sjar_flags & SLJ_ASSIGN_REP_FREE)
		psclog_info("free item %zd", elem);
	else {
		sjba = &logentry->sjar_bmap;
		psclog_info("replay item %zd, fid="SLPRI_FID", flags=%d",
		    elem, sjba->sjba_fid, logentry->sjar_flags);
	}
	if (logentry->sjar_flags & SLJ_ASSIGN_REP_INO)
		mds_replay_ino(&logentry->sjar_ino, I_REPLAY_OP_REPLS);
	if (logentry->sjar_flags & SLJ_ASSIGN_REP_REP)
		mds_replay_bmap(&logentry->sjar_rep,
		    B_REPLAY_OP_REPLS);
	rc = mdsio_lookup(mds_metadir_inum, SL_FN_BMAP_ODTAB, &mf,
	    &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(mf, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &handle, NULL, NULL, 0);
	psc_assert(!rc && handle);

	rc = mdsio_read(&rootcreds, &odth, sizeof(odth), &nb, 0,
	    handle);
	psc_assert(rc == 0 && nb == sizeof(odth));

	psc_assert((odth.odth_magic == ODTBL_MAGIC) &&
		   (odth.odth_version == ODTBL_VERS));

	p = PSCALLOC(odth.odth_slotsz);
	odtf = p + odth.odth_elemsz;
	odtf->odtf_magic = ODTBL_MAGIC;
	odtf->odtf_slotno = elem;

	if (logentry->sjar_flags & SLJ_ASSIGN_REP_BMAP) {
		bia = p;
		sjba = &logentry->sjar_bmap;
		bia->bia_lastcli.pid = sjba->sjba_lastcli.pid;
		bia->bia_lastcli.nid = sjba->sjba_lastcli.nid;
		bia->bia_ios = sjba->sjba_ios;
		bia->bia_fid = sjba->sjba_fid;
		bia->bia_seq = sjba->sjba_seq;
		bia->bia_bmapno = sjba->sjba_bmapno;
		bia->bia_start = sjba->sjba_start;
		bia->bia_flags = sjba->sjba_flags;

		/* I don't think memset() does any good, anyway... */
		len = sizeof(struct bmap_ios_assign);
		if (len < odth.odth_elemsz)
			memset(p + len, 0, odth.odth_elemsz - len);
		psc_crc64_calc(&crc, p, odth.odth_elemsz);

		odtf->odtf_crc = crc;
		odtf->odtf_inuse = ODTBL_INUSE;
	}
	if (logentry->sjar_flags & SLJ_ASSIGN_REP_FREE)
		odtf->odtf_inuse = ODTBL_FREE;

	rc = mdsio_write(&rootcreds, p, odth.odth_slotsz,
	   &nb, odth.odth_start + elem * odth.odth_slotsz,
	   0, handle, NULL, NULL);
	psc_assert(!rc && nb == odth.odth_slotsz);

	PSCFREE(p);
	mdsio_release(&rootcreds, handle);
	return (0);
}

/**
 * mds_replay_namespace - Replay a NAMESPACE modification operation.
 *	Note: this may not be a replay but could also be a namespace
 *	update from a remote MDS.
 * @sjnm: journal entry.
 * @replay: whether this is a replay or remote MDS update.
 */
int
mds_replay_namespace(struct slmds_jent_namespace *sjnm, int replay)
{
	char name[SL_NAME_MAX + 1], newname[SL_NAME_MAX + 1];
	struct fidc_membh *fcmh = NULL;
	struct srt_stat sstb;
	int rc;

	memset(&sstb, 0, sizeof(sstb));
	sstb.sst_fid = sjnm->sjnm_target_fid,
	sstb.sst_gen = sjnm->sjnm_target_gen;
//	sstb.sst_ptruncgen = sjnm->sjnm_uid;
	sstb.sst_uid = sjnm->sjnm_uid;
	sstb.sst_gid = sjnm->sjnm_gid;
	sstb.sst_mode = sjnm->sjnm_mode;
	sstb.sst_size = sjnm->sjnm_size;

	sstb.sst_atime = sjnm->sjnm_atime;
	sstb.sst_atime_ns = sjnm->sjnm_atime_ns;
	sstb.sst_mtime = sjnm->sjnm_mtime;
	sstb.sst_mtime_ns = sjnm->sjnm_mtime_ns;
	sstb.sst_ctime = sjnm->sjnm_ctime;
	sstb.sst_ctime_ns = sjnm->sjnm_ctime_ns;

	if (!sstb.sst_fid) {
//	 || fid == FID_ANY
		psclog_errorx("unexpected zero SLASH2 FID");
		return (EINVAL);
	}

	name[0] = '\0';
	newname[0] = '\0';
	if (sjnm->sjnm_namelen) {
		memcpy(name, sjnm->sjnm_name, sjnm->sjnm_namelen);
		name[sjnm->sjnm_namelen] = '\0';
	}
	if (sjnm->sjnm_namelen2) {
		memcpy(newname, sjnm->sjnm_name + sjnm->sjnm_namelen,
		    sjnm->sjnm_namelen2);
		newname[sjnm->sjnm_namelen2] = '\0';
	}
	/*
	 * If the receiving MDS is the one that initiated the namespace
	 * operation, then we should have already propagated the remote
	 * operation back to us.  If so, then seeing ENOENT for unlink
	 * and EXIST for creates should be fine.
	 */
	switch (sjnm->sjnm_op) {
	    case NS_OP_RECLAIM:
		rc = 0;
		break;
	    case NS_OP_CREATE:
		rc = mdsio_redo_create(sjnm->sjnm_parent_fid, name,
		    &sstb);
		break;
	    case NS_OP_MKDIR:
		rc = mdsio_redo_mkdir(sjnm->sjnm_parent_fid, name,
		    &sstb);
		break;
	    case NS_OP_LINK:
		rc = mdsio_redo_link(sjnm->sjnm_parent_fid,
		    sjnm->sjnm_target_fid, name, &sstb);
		break;
	    case NS_OP_SYMLINK:
		rc = mdsio_redo_symlink(sjnm->sjnm_parent_fid,
		    sjnm->sjnm_target_fid, name, newname, &sstb);
		break;
	    case NS_OP_RENAME:
		rc = mdsio_redo_rename(sjnm->sjnm_parent_fid, name,
		    sjnm->sjnm_new_parent_fid, newname, &sstb);
		break;
	    case NS_OP_UNLINK:
		rc = mdsio_redo_unlink(sjnm->sjnm_parent_fid,
		    sjnm->sjnm_target_fid, name);
		break;
	    case NS_OP_RMDIR:
		rc = mdsio_redo_rmdir(sjnm->sjnm_parent_fid,
		    sjnm->sjnm_target_fid, name);
		break;
	    case NS_OP_SETSIZE:
	    case NS_OP_SETATTR:
		if (!replay) {
			/*
			 * Make sure that we propagate attributes
			 * to the fcmh layer if work is done at
			 * the ZFS layer.
			 */
			rc = slm_fcmh_peek(&sstb.sst_fg, &fcmh);
			if (fcmh)
				FCMH_LOCK(fcmh);
		}
		rc = mdsio_redo_setattr(sjnm->sjnm_target_fid,
		    sjnm->sjnm_mask, &sstb);
		slm_setattr_core(fcmh, &sstb,
		    mdsio_setattrmask_2_slflags(sjnm->sjnm_mask));
		if (!replay) {
			if (fcmh) {
				/* setattr() above has filled sstb */
				COPY_SSTB(&sstb, &fcmh->fcmh_sstb);
				fcmh_op_done(fcmh);
			}
		}
		break;
	    default:
		psclog_errorx("Unexpected opcode %d", sjnm->sjnm_op);
		rc = EINVAL;
		break;
	}
	if (rc)
		psclog_errorx("Redo namespace log: "
		    "op=%d name=%s newname=%s "
		    "fid="SLPRI_FID" rc=%d",
		    sjnm->sjnm_op, name, newname,
		    sjnm->sjnm_target_fid, rc);
	return (rc);
}

/**
 * mds_replay_handler - Handle journal replay events.
 */
int
mds_replay_handler(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_namespace *sjnm;
	int rc = 0, type;
	uint64_t fid;

	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	switch (type) {
	    case MDS_LOG_BMAP_REPLS:
		rc = mds_replay_bmap_repls(pje);
		break;
	    case MDS_LOG_BMAP_CRC:
		rc = mds_replay_bmap_crc(pje);
		break;
	    case MDS_LOG_BMAP_SEQ:
		rc = mds_replay_bmap_seq(pje);
		break;
	    case MDS_LOG_INO_REPLS:
		rc = mds_replay_ino_repls(pje);
		break;
	    case MDS_LOG_BMAP_ASSIGN:
		rc = mds_replay_bmap_assign(pje);
		break;
	    case MDS_LOG_NAMESPACE:
		sjnm = PJE_DATA(pje);
		psc_assert(sjnm->sjnm_magic == SJ_NAMESPACE_MAGIC);
		rc = mds_replay_namespace(sjnm, 1);

		/*
		 * If we fail above, we still skip these SLASH2 FIDs here
		 * in case a client gets confused.
		 *
		 * 04/12/2012:
		 *
		 * Alternatively, we can just set it to be one beyond the last
		 * fid stored in a journal entry.
		 */
		if (sjnm->sjnm_op == NS_OP_CREATE ||
		    sjnm->sjnm_op == NS_OP_MKDIR ||
		    sjnm->sjnm_op == NS_OP_LINK ||
		    sjnm->sjnm_op == NS_OP_SYMLINK)
			slm_get_next_slashfid(&fid);
		break;
	    default:
		psc_fatalx("invalid log entry type %d", pje->pje_type);
	}
	psclog_info("replayed journal optype=%d xid=%#"PRIx64" "
	    "txg=%#"PRIx64" rc=%d",
	    type, pje->pje_xid, pje->pje_txg, rc);
	return (rc);
}
