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
#include "up_sched_res.h"

#include "zfs-fuse/zfs_slashlib.h"

#define B_REPLAY_OP_CRC		0
#define B_REPLAY_OP_REPLS	1

#define I_REPLAY_OP_REPLS	0

/*
 * Alternatively, we could add vfsid to each log entry in the journal.
 * But doing so can break backward compatibility.  It also reduces the
 * space of each log entry.  We could infer vfsid from the fid stored in
 * a log entry.  But some entries such as bmap assignment log entries do
 * not have one.
 *
 * Actually, we can use the FSUUID stored in the log header to do the
 * matching.
 */

/*
 * Replay an operation on a bmap.
 */
static int
mds_replay_bmap(void *jent, int op)
{
	int resid, off, rc, tract[NBREPLST];
	struct slmds_jent_bmap_repls *sjbr = jent;
	struct fidc_membh *f = NULL;
	struct bmapc_memb *b = NULL;
	struct bmap_mds_info *bmi;
	struct sl_fidgen fg;
	uint8_t bmi_orepls[SL_REPLICA_NBYTES];
	struct {
		slfid_t		fid;
		sl_bmapno_t	bno;
	} *cp = jent;
	uint32_t n;

	fg.fg_fid = cp->fid;
	fg.fg_gen = FGEN_ANY;
	rc = slm_fcmh_get(&fg, &f);
	if (rc)
		goto out;

	rc = bmap_getf(f, cp->bno, SL_WRITE, BMAPGETF_CREATE, &b);
	if (rc)
		goto out;

	BMAP_ULOCK(b);
	bmi = bmap_2_bmi(b);

	DEBUG_BMAPOD(PLL_DIAG, b, "before bmap replay op=%d", op);

	switch (op) {
	case B_REPLAY_OP_REPLS:

		OPSTAT_INCR("replay-repls");
		mds_brepls_check(sjbr->sjbr_repls, sjbr->sjbr_nrepls);

		bmap_op_start_type(b, BMAP_OPCNT_WORK);

		/*
		 * So we have some changes in the journal, but not
		 * in the sql table.
		 */
		BMAP_LOCK(b);
		memcpy(bmi_orepls, bmi->bmi_repls,
		    sizeof(bmi->bmi_orepls));

		/*
 		 * Here, we assume that if we replay the log entry, the
 		 * correponding changes have not be made to the file
 		 * system.
 		 */
		bmap_2_replpol(b) = sjbr->sjbr_replpol;
		memcpy(bmi->bmi_repls, sjbr->sjbr_repls,
		    SL_REPLICA_NBYTES);

		/* revert inflight to reissue */
		brepls_init(tract, -1);
		tract[BREPLST_REPL_SCHED] = BREPLST_REPL_QUEUED;
		tract[BREPLST_GARBAGE_SCHED] = BREPLST_GARBAGE_QUEUED;
		tract[BREPLST_TRUNC_SCHED] = BREPLST_TRUNC_QUEUED;
		mds_repl_bmap_walk_all(b, tract, NULL, 0);

		BMAP_ULOCK(b);

		memcpy(bmi->bmi_orepls, bmi_orepls, sizeof(bmi->bmi_orepls));

		slm_repl_upd_write(b, 1);

		/*
 		 * The following seems to make sure those replicas already
 		 * marked BREPLST_REPL_QUEUED and BREPLST_GARBAGE before
 		 * log replay are inserted into the table.
 		 *
 		 * I got some not-unique warning (rc=19) after the locking 
 		 * revamp.
 		 */
		for (n = 0, off = 0; n < fcmh_2_nrepls(f);
		    n++, off += SL_BITS_PER_REPLICA)
			switch (SL_REPL_GET_BMAP_IOS_STAT(bmi->bmi_repls,
			    off)) {
			case BREPLST_REPL_QUEUED:
			case BREPLST_TRUNC_QUEUED:
			case BREPLST_GARBAGE_QUEUED:
				resid = fcmh_2_repl(f, n);

				rc = slm_upsch_insert(b, resid, 
				    bmi->bmi_sys_prio, bmi->bmi_usr_prio);
				if (rc)
					psclog_warnx("upsch insert failed: bno=%d, "
					    "fid=%"PRId64", ios=%d, rc=%d",
					    b->bcm_bmapno, bmap_2_fid(b), 
					    resid, rc);
				break;
			}

		break;
	}

	DEBUG_BMAPOD(PLL_DIAG, b, "replayed bmap op=%d", op);

	BMAP_LOCK(b);
	rc = mds_bmap_write(b, NULL, NULL);
	BMAP_ULOCK(b);

 out:
	if (b)
		bmap_op_done(b);
	if (f)
		fcmh_op_done(f);
	return (rc);
}

/*
 * Replay a CRC update.  Because we only log CRCs that have been changed
 * in the bmap, this has to be a read-modify-write process.
 */
static int
mds_replay_update(struct psc_journal_enthdr *pje)
{
	return (0);
}

static int
mds_replay_bmap_seq(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_bmapseq *sjsq;

	sjsq = PJE_DATA(pje);

	/*
 	 * We update slm_bmap_leases directly. Otherwise, cursor updates
 	 * during replay will pick up stale values.  However, this is
 	 * just one way to make sure watermarks are bumped correctly.
 	 */
	if (slm_bmap_leases.btt_maxseq < sjsq->sjbsq_high_wm)
		slm_bmap_leases.btt_maxseq = sjsq->sjbsq_high_wm;
	if (slm_bmap_leases.btt_minseq < sjsq->sjbsq_low_wm)
		slm_bmap_leases.btt_minseq = sjsq->sjbsq_low_wm;

	return (0);
}

/*
 * Replay an inode update.
 *
 * Note that the replica table can be stored for a directory for inheritance purposes,
 * and it is named according to /deployment_s2md/.slmd/fidns/a/b/c/d/$fid.ino. 
 * See slm_fcmh_ctor() for details.
 */
static int
mds_replay_ino(void *jent, int op)
{
	struct slmds_jent_ino_repls *sjir = jent;
	struct slash_inode_handle *ih = NULL;
	struct sl_fidgen fg;
	struct fidc_membh *f;
	char buf[LINE_MAX];
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

	/* XXX ih is only set up for regular files */
	INOH_LOCK(ih);

	switch (op) {
	case I_REPLAY_OP_REPLS:
		if (!fcmh_isdir(f)) {
			/* sanity check against null buffer */
			for (j = 0; j < SL_MAX_REPLICAS; j++)
				if (sjir->sjir_repls[j])
					break;
			psc_assert(j != SL_MAX_REPLICAS);
		}

		/*
		 * We always update the inode itself because the number
		 * of replicas is stored there.
		 */
		if (ih->inoh_flags & INOH_INO_NEW) {
			if (sjir->sjir_repls[0] == 0)
				psclog_errorx("ino_repls[0] should be set for "
				    "newly created inode");
			/*
 			 * 10/05/2016: I hit this today. However, a new inode
 			 * can inherit the value from its parent.
 			 */
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
			rc = mds_inox_write(current_vfsid, ih, NULL, NULL);
			if (rc)
				goto out;

			DEBUG_INOH(PLL_DEBUG, ih, buf, "replayed inox_repls");
		}

		ih->inoh_ino.ino_replpol = sjir->sjir_replpol;
		ih->inoh_ino.ino_nrepls = sjir->sjir_nrepls;
		memcpy(ih->inoh_ino.ino_repls, sjir->sjir_repls,
		    sizeof(ih->inoh_ino.ino_repls));
		break;
	default:
		psc_fatalx("unknown op");
	}

	rc = mds_inode_write(current_vfsid, ih, NULL, NULL);

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
	rc = mds_replay_ino(sjir, I_REPLAY_OP_REPLS);
	return (rc);
}

/*
 * Replay a bmap assignment update.
 */
static int
mds_replay_bmap_assign(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_assign_rep *sjar;
	struct slmds_jent_bmap_assign *sjba;
	struct bmap_ios_assign *bia;
	size_t item;

	sjar = PJE_DATA(pje);
	item = sjar->sjar_item;
	if (sjar->sjar_flags & SLJ_ASSIGN_REP_FREE)
		psclog_diag("free item %zd", item);
	if (sjar->sjar_flags & SLJ_ASSIGN_REP_INO)
		mds_replay_ino(&sjar->sjar_ino, I_REPLAY_OP_REPLS);
	if (sjar->sjar_flags & SLJ_ASSIGN_REP_REP)
		mds_replay_bmap(&sjar->sjar_rep, B_REPLAY_OP_REPLS);

	pfl_odt_allocitem(slm_bia_odt, (void **)&bia);

	if (sjar->sjar_flags & SLJ_ASSIGN_REP_BMAP) {
		sjba = &sjar->sjar_bmap;
		psclog_diag("replay item %zd, fid="SLPRI_FID", flags=%d",
		    item, sjba->sjba_fid, sjar->sjar_flags);
		bia->bia_lastcli.pid = sjba->sjba_lastcli.pid;
		bia->bia_lastcli.nid = sjba->sjba_lastcli.nid;
		bia->bia_ios = sjba->sjba_ios;
		bia->bia_fid = sjba->sjba_fid;
		bia->bia_seq = sjba->sjba_seq;
		bia->bia_bmapno = sjba->sjba_bmapno;
		bia->bia_start = sjba->sjba_start;
		bia->bia_flags = sjba->sjba_flags;
	}

	pfl_odt_putitem(slm_bia_odt, item, bia,
	    sjar->sjar_flags & SLJ_ASSIGN_REP_FREE ? 0 : 1);

	PSCFREE(bia);

	return (0);
}

/*
 * Replay a NAMESPACE modification operation.
 *
 * Note: this may not be a replay but could also be a namespace update
 * from a remote MDS.
 *
 * @sjnm: journal entry.
 * @replay: whether this is a replay or remote MDS update.
 */
int
mds_replay_namespace(struct slmds_jent_namespace *sjnm)
{
	char name[SL_NAME_MAX + 1], newname[SL_NAME_MAX + 1];
	struct srt_stat sstb;
	int rc;
	struct fidc_membh *f = NULL;

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
		rc = mdsio_redo_create(current_vfsid,
		    sjnm->sjnm_parent_fid, name, &sstb);
		break;
	    case NS_OP_MKDIR:
		rc = mdsio_redo_mkdir(current_vfsid,
		    sjnm->sjnm_parent_fid, name, &sstb);
		break;
	    case NS_OP_LINK:
		rc = mdsio_redo_link(current_vfsid,
		    sjnm->sjnm_parent_fid, sjnm->sjnm_target_fid, name,
		    &sstb);
		break;
	    case NS_OP_SYMLINK:
		rc = mdsio_redo_symlink(current_vfsid,
		    sjnm->sjnm_parent_fid, sjnm->sjnm_target_fid, name,
		    newname, &sstb);
		break;
	    case NS_OP_RENAME:
		rc = mdsio_redo_rename(current_vfsid,
		    sjnm->sjnm_parent_fid, name,
		    sjnm->sjnm_new_parent_fid, newname, &sstb);
		break;
	    case NS_OP_UNLINK:
		rc = mdsio_redo_unlink(current_vfsid,
		    sjnm->sjnm_parent_fid, sjnm->sjnm_target_fid, name);
		break;
	    case NS_OP_RMDIR:
		rc = mdsio_redo_rmdir(current_vfsid,
		    sjnm->sjnm_parent_fid, sjnm->sjnm_target_fid, name);

		snprintf(name, sizeof(name), "%016"PRIx64".ino",
		    sjnm->sjnm_target_fid);
		mdsio_unlink(current_vfsid, mdsio_getfidlinkdir(
		    sjnm->sjnm_target_fid), NULL, name, &rootcreds,
		    NULL, NULL);

		break;
	    case NS_OP_SETSIZE:
	    case NS_OP_SETATTR:
		rc = mdsio_redo_setattr(current_vfsid,
		    sjnm->sjnm_target_fid, sjnm->sjnm_mask, &sstb);
		if (rc)
			break;
		/*
		 * Throw away a cached copy to force a reload.
		 */
		rc = sl_fcmh_peek_fg(&sstb.sst_fg, &f);
		if (!rc) {
			FCMH_LOCK(f);
			f->fcmh_flags |= FCMH_TOFREE;
			fcmh_op_done(f);
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

/*
 * Handle journal replay events. It is called from pjournal_replay().
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
		rc = mds_replay_bmap(PJE_DATA(pje), B_REPLAY_OP_REPLS);
		break;
	    case MDS_LOG_BMAP_CRC:
		psclog_warnx("unexpected log entry type %d", pje->pje_type);
		break;
	    case MDS_LOG_UPDATE:
		rc = mds_replay_update(pje);
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
		rc = mds_replay_namespace(sjnm);

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
