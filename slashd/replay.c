/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2011, Pittsburgh Supercomputing Center (PSC).
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
#include "mdsio.h"
#include "namespace.h"
#include "pathnames.h"
#include "slerr.h"
#include "sljournal.h"

/**
 * mds_redo_bmap_repl - Replay a replication update on a bmap.  This has
 *	to be a read-modify-write process because we don't touch the CRC
 *	tables.
 */
static int
mds_redo_bmap_repl_common(struct slmds_jent_repgen *jrpg)
{
	struct bmap_ondisk bmap_disk;
	void *mdsio_data;
	mdsio_fid_t fid;
	size_t nb;
	int rc;

	memset(&bmap_disk, 0, sizeof(struct bmap_ondisk));

	rc = mdsio_lookup_slfid(jrpg->sjp_fid, &rootcreds, NULL, &fid);
	if (rc) {
		if (rc == ENOENT) {
			psclog_warnx("mdsio_lookup_slfid: %s", slstrerror(rc));
			return (rc);
		}
		psc_fatalx("mdsio_lookup_slfid: %s", slstrerror(rc));
	}

	rc = mdsio_opencreate(fid, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &mdsio_data, NULL, NULL);
	if (rc)
		psc_fatalx("mdsio_opencreate: %s", slstrerror(rc));

	rc = mdsio_read(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * jrpg->sjp_bmapno) + SL_BMAP_START_OFF),
	    mdsio_data);

	/*
	 * We allow a short read here because it is possible
	 * that the file was just created by our own replay.
	 */
	if (rc)
		goto out;

	psc_assert(!nb || nb == BMAP_OD_SZ);

	memcpy(bmap_disk.bod_repls, jrpg->sjp_reptbl, SL_REPLICA_NBYTES);

	/* XXX recalculate CRC!! */

	rc = mdsio_write(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * jrpg->sjp_bmapno) + SL_BMAP_START_OFF),
	    0, mdsio_data, NULL, NULL);

	if (!rc && nb != BMAP_OD_SZ)
		rc = EIO;
 out:
	mdsio_release(&rootcreds, mdsio_data);
	return (rc);
}

static int
mds_redo_bmap_repl(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_repgen *jrpg;
	int rc;

	jrpg = PJE_DATA(pje);
	rc = mds_redo_bmap_repl_common(jrpg);
	return (rc);
}

/**
 * mds_redo_bmap_crc - Replay a CRC update.  Because we only log
 *     CRCs that have been changed in the bmap, this has to be a
 *     read-modify-write process.
 */
static int
mds_redo_bmap_crc(struct psc_journal_enthdr *pje)
{
	struct srm_bmap_crcwire *bmap_wire;
	struct bmap_ondisk bmap_disk;
	struct slmds_jent_crc *jcrc;
	struct srt_stat sstb;
	void *mdsio_data;
	mdsio_fid_t mf;
	int i, rc;
	size_t nb;

	jcrc = PJE_DATA(pje);
	memset(&bmap_disk, 0, sizeof(struct bmap_ondisk));

	psclog_info("pje_xid=%"PRIx64" pje_txg=%"PRIx64" fid="SLPRI_FID" "
	    "bmapno=%u ncrcs=%d crc[0]=%"PSCPRIxCRC64,
	    pje->pje_xid, pje->pje_txg, jcrc->sjc_fid, jcrc->sjc_bmapno,
	    jcrc->sjc_ncrcs, jcrc->sjc_crc[0].crc);

	rc = mdsio_lookup_slfid(jcrc->sjc_fid, &rootcreds, NULL, &mf);
	if (rc == ENOENT) {
		psclog_warnx("mdsio_lookup_slfid: %s", slstrerror(rc));
		return (rc);
	}
	if (rc)
		psc_fatalx("mdsio_lookup_slfid: %s", slstrerror(rc));

	rc = mdsio_opencreate(mf, &rootcreds, O_RDWR, 0, NULL,
	    NULL, NULL, &mdsio_data, NULL, NULL);
	if (rc)
		psc_fatalx("mdsio_opencreate: %s", slstrerror(rc));

	/* Apply the filesize from the journal entry.
	 */
	sstb.sst_size = jcrc->sjc_fsize;
	rc = mdsio_setattr(mf, &sstb, PSCFS_SETATTRF_DATASIZE,
	    &rootcreds, NULL, mdsio_data, NULL);
	if (rc)
		goto out;

	rc = mdsio_read(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * jcrc->sjc_bmapno) +
	    SL_BMAP_START_OFF), mdsio_data);

	if (!rc && nb != BMAP_OD_SZ)
		rc = EIO;
	if (rc)
		goto out;

	for (i = 0 ; i < jcrc->sjc_ncrcs; i++) {
		bmap_wire = &jcrc->sjc_crc[i];
		bmap_disk.bod_crcs[bmap_wire->slot] = bmap_wire->crc;
		bmap_disk.bod_crcstates[bmap_wire->slot] |=
		    BMAP_SLVR_DATA | BMAP_SLVR_CRC;
	}
	psc_crc64_calc(&bmap_disk.bod_crc, &bmap_disk, BMAP_OD_CRCSZ);

	rc = mdsio_write(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
	    (off_t)((BMAP_OD_SZ * jcrc->sjc_bmapno) +
	    SL_BMAP_START_OFF), 0, mdsio_data, NULL, NULL);

	if (!rc && nb != BMAP_OD_SZ)
		rc = EIO;

 out:
	mdsio_release(&rootcreds, mdsio_data);
	return (rc);
}

static int
mds_redo_bmap_seq(struct psc_journal_enthdr *pje)
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
 * mds_redo_ino_addrepl - Replay an inode replication table update.
 */
static int
mds_redo_ino_addrepl_common(struct slmds_jent_ino_addrepl *jrir)
{
	struct slash_inode_extras_od inoh_extras;
	struct slash_inode_od inoh_ino;
	void *mdsio_data;
	mdsio_fid_t fid;
	int pos, j, rc;
	size_t nb;

	memset(&inoh_ino, 0, sizeof(inoh_ino));

	pos = jrir->sjir_pos;
	if (pos >= SL_MAX_REPLICAS || pos < 0) {
		psclog_errorx("ino_nrepls index (%d) is out of range",
		    pos);
		return (EINVAL);
	}

	rc = mdsio_lookup_slfid(jrir->sjir_fid, &rootcreds, NULL, &fid);
	if (rc)
		//psc_fatalx("mdsio_lookup_slfid: %s", slstrerror(rc));
		return (rc);

	rc = mdsio_opencreate(fid, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &mdsio_data, NULL, NULL);
	if (rc)
		psc_fatalx("mdsio_opencreate: %s", slstrerror(rc));

	/*
	 * We allow a short read of the inode here because it is possible
	 * that the file was just created by our own replay process.
	 */
	if (pos >= SL_DEF_REPLICAS) {
		memset(&inoh_extras, 0, sizeof(inoh_extras));

		rc = mdsio_read(&rootcreds, &inoh_extras, INOX_OD_SZ,
		    &nb, SL_EXTRAS_START_OFF, mdsio_data);
		if (rc)
			goto out;

		j = pos - SL_DEF_REPLICAS;
		inoh_extras.inox_repls[j].bs_id = jrir->sjir_ios;
		psc_crc64_calc(&inoh_extras.inox_crc, &inoh_extras,
		    INOX_OD_CRCSZ);

		rc = mdsio_write(&rootcreds, &inoh_extras, INOX_OD_SZ,
		    &nb, SL_EXTRAS_START_OFF, 0, mdsio_data, NULL,
		    NULL);

		if (!rc && nb != INO_OD_SZ)
			rc = EIO;
		if (rc)
			goto out;
	}
	/*
	 * We always update the inode itself because the number of
	 * replicas is stored there.
	 */
	rc = mdsio_read(&rootcreds, &inoh_ino, INO_OD_SZ, &nb,
		SL_INODE_START_OFF, mdsio_data);
	if (rc)
		goto out;

	/* initialize newly replay-created inode */
	if (!nb && inoh_ino.ino_crc == 0 &&
	    memcmp(&inoh_ino, &null_inode_od, INO_OD_CRCSZ) == 0) {
		inoh_ino.ino_bsz = SLASH_BMAP_SIZE;
		inoh_ino.ino_version = INO_VERSION;
		if (pos != 0)
			psclog_errorx("ino_nrepls index (%d) in "
			    "should be 0 for newly created inode", pos);
		if (jrir->sjir_nrepls != 1)
			psclog_errorx("ino_nrepls (%d) in "
			    "should be 1 for newly created inode",
			    jrir->sjir_nrepls);
	}

	if (jrir->sjir_nrepls > SL_MAX_REPLICAS ||
	    jrir->sjir_nrepls <= jrir->sjir_pos)
		abort();
	inoh_ino.ino_nrepls = jrir->sjir_nrepls;

	if (pos < SL_DEF_REPLICAS)
		inoh_ino.ino_repls[pos].bs_id = jrir->sjir_ios;

	psc_crc64_calc(&inoh_ino.ino_crc, &inoh_ino, INO_OD_CRCSZ);

	rc = mdsio_write(&rootcreds, &inoh_ino, INO_OD_SZ, &nb,
	    SL_INODE_START_OFF, 0, mdsio_data, NULL, NULL);

	if (!rc && nb != INO_OD_SZ)
		rc = EIO;

 out:
	mdsio_release(&rootcreds, mdsio_data);
	return (rc);
}

static int
mds_redo_ino_addrepl(struct psc_journal_enthdr *pje)
{
	int rc;
	struct slmds_jent_ino_addrepl *jrir;

	jrir = PJE_DATA(pje);
	rc = mds_redo_ino_addrepl_common(jrir);
	return (rc);
}

/**
 * mds_redo_bmap_assign - Replay a bmap assignment update.
 */
static int
mds_redo_bmap_assign(struct psc_journal_enthdr *pje)
{
	int rc;
	void *p, *handle;
	mdsio_fid_t mf;
	uint64_t crc;
	size_t nb, len, elem;
	struct odtable_hdr odth;
	struct odtable_entftr *odtf;
	struct slmds_jent_ino_addrepl *jrir;
	struct slmds_jent_repgen *jrpg;
	struct slmds_jent_bmap_assign *jrba;
	struct slmds_jent_assign_rep *logentry;

	logentry = PJE_DATA(pje);
	if (logentry->sjar_flag & SLJ_ASSIGN_REP_INO) {
		jrir = &logentry->sjar_ino;
		mds_redo_ino_addrepl_common(jrir);
	}

	psc_assert(logentry->sjar_flag & SLJ_ASSIGN_REP_REP);
	jrpg = &logentry->sjar_rep;
	mds_redo_bmap_repl_common(jrpg);

	psc_assert(logentry->sjar_flag & SLJ_ASSIGN_REP_BMAP);
	jrba = &logentry->sjar_bmap;

	rc = mdsio_lookup(MDSIO_FID_ROOT, SL_PATH_BMAP, &mf, &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(mf, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &handle, NULL, NULL);
	psc_assert(!rc && handle);

	rc = mdsio_read(&rootcreds, &odth, sizeof(odth), &nb, 0,
	    handle);
	psc_assert(rc == 0 && nb == sizeof(odth));

	psc_assert((odth.odth_magic == ODTBL_MAGIC) &&
		   (odth.odth_version == ODTBL_VERS));

	elem = logentry->sjar_elem;
	len = sizeof(struct slmds_jent_bmap_assign);

	p = PSCALLOC(odth.odth_slotsz);
	memcpy(p, jrba, len);
	if (len < odth.odth_elemsz)
		memset(p + len, 0, odth.odth_elemsz - len);
	psc_crc64_calc(&crc, p, odth.odth_elemsz);

	odtf = p + odth.odth_elemsz;
	odtf->odtf_crc = crc;
	odtf->odtf_inuse = ODTBL_INUSE;
	odtf->odtf_slotno = elem;
	odtf->odtf_magic = ODTBL_MAGIC;

	rc = mdsio_write(&rootcreds, p, odth.odth_slotsz,
	   &nb, odth.odth_start + elem * odth.odth_slotsz,
	   0, handle, NULL, NULL);
	psc_assert(!rc && nb == odth.odth_slotsz);

	PSCFREE(p);
	mdsio_release(&rootcreds, handle);
	return (0);
}

/**
 * mds_redo_namespace - Replay a NAMESPACE modification operation.
 *	Note: this may not be a replay but could also be a namespace
 *	update from a remote MDS.
 * @sjnm: journal entry.
 * @replay: whether this is a replay or remote MDS update.
 */
int
mds_redo_namespace(struct slmds_jent_namespace *sjnm, int replay)
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
		psclog_errorx("Unexpected zero SLASH2 FID.");
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

	switch (sjnm->sjnm_op) {
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
		    sjnm->sjnm_target_fid, name, &sstb);
		break;
	    case NS_OP_RMDIR:
		rc = mdsio_redo_rmdir(sjnm->sjnm_parent_fid,
		    sjnm->sjnm_target_fid, name, &sstb);
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
				fcmh->fcmh_sstb = sstb;
				fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
			}
		}
		break;
	    default:
		psclog_errorx("Unexpected opcode %d", sjnm->sjnm_op);
		rc = EINVAL;
		break;
	}
	psclog_info("Redo namespace log: op=%d name=%s "
	    "newname=%s fid="SLPRI_FID" rc=%d",
	    sjnm->sjnm_op, name, newname, sjnm->sjnm_target_fid, rc);
	return (rc);
}

/**
 * mds_replay_handler - Handle journal replay events.
 */
int
mds_replay_handler(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_namespace *sjnm;
	int rc = 0;

	psclog_info("pje=%p pje_xid=%"PRIx64" pje_txg=%"PRIx64,
	    pje, pje->pje_xid, pje->pje_txg);

	switch (pje->pje_type & ~(_PJE_FLSHFT - 1)) {
	    case MDS_LOG_BMAP_REPL:
		rc = mds_redo_bmap_repl(pje);
		break;
	    case MDS_LOG_BMAP_CRC:
		rc = mds_redo_bmap_crc(pje);
		break;
	    case MDS_LOG_BMAP_SEQ:
		rc = mds_redo_bmap_seq(pje);
		break;
	    case MDS_LOG_INO_ADDREPL:
		rc = mds_redo_ino_addrepl(pje);
		break;
	    case MDS_LOG_BMAP_ASSIGN:
		rc = mds_redo_bmap_assign(pje);
		break;
	    case MDS_LOG_NAMESPACE:
		sjnm = PJE_DATA(pje);
		psc_assert(sjnm->sjnm_magic == SJ_NAMESPACE_MAGIC);
		rc = mds_redo_namespace(sjnm, 1);
		/*
		 * If we fail above, we still skip these SLASH2 FIDs here
		 * in case a client gets confused.
		 */
		if (sjnm->sjnm_op == NS_OP_CREATE ||
		    sjnm->sjnm_op == NS_OP_MKDIR ||
		    sjnm->sjnm_op == NS_OP_LINK ||
		    sjnm->sjnm_op == NS_OP_SYMLINK)
			slm_get_next_slashid();
		break;
	    default:
		psc_fatalx("invalid log entry type %d", pje->pje_type);
	}
	return (rc);
}
