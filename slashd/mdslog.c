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
 * mdslog - Logic supporting transmission of various kinds of updates to
 * peers.
 *   (o) namespace updates to other MDS nodes ("update")
 *   (o) garbage reclamation to IOS nodes ("reclaim")
 */

#include <sys/param.h>

#include <inttypes.h>
#include <string.h>

#include "pfl/crc.h"
#include "pfl/ctlsvr.h"
#include "pfl/dynarray.h"
#include "pfl/fcntl.h"
#include "pfl/fs.h"
#include "pfl/journal.h"
#include "pfl/lock.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "pfl/workthr.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "journal_mds.h"
#include "mdsio.h"
#include "mdslog.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "subsys_mds.h"

#include "zfs-fuse/zfs_slashlib.h"

#define R_ENTSZ			sizeof(struct srt_reclaim_entry)
#define U_ENTSZ			sizeof(struct srt_update_entry)

#define RP_ENTSZ		sizeof(struct reclaim_prog_entry)
#define UP_ENTSZ		sizeof(struct update_prog_entry)

/* MDS -> IOS RECLAIM RPC async callback argument indexes */
#define CBARG_CSVC		0
#define CBARG_RARG		1
#define CBARG_RES		2

/* max # MDS records in one progress file */
#define MAX_UPDATE_PROG_ENTRY	1024

/* namespace update progress tracker to peer MDSes */
struct update_prog_entry {
	uint64_t		 upe_xid;
	uint64_t		 upe_batchno;
	sl_ios_id_t		 upe_id;
	int32_t			 _pad;
};

/* max # IOS records in one progress file */
#define MAX_RECLAIM_PROG_ENTRY	1024

/* garbage reclaim progress tracker to IOSes */
struct reclaim_prog_entry {
	uint64_t		 rpe_xid;
	uint64_t		 rpe_batchno;
	sl_ios_id_t		 rpe_id;
	int32_t			 _pad;
};

struct slm_progress		 nsupd_prg;
struct slm_progress		 reclaim_prg;

/* max # of seconds to wait for updates before hard retry */
#define SL_UPDATE_MAX_AGE	 30
#define SL_RECLAIM_MAX_AGE	 30

struct psc_journal		*slm_journal;

static psc_spinlock_t		 mds_distill_lock = SPINLOCK_INIT;

static void			*mds_cursor_handle;
struct psc_journal_cursor	 mds_cursor;

psc_spinlock_t			 mds_txg_lock = SPINLOCK_INIT;

struct psc_waitq		 slm_cursor_waitq = PSC_WAITQ_INIT("cursor");
psc_spinlock_t			 slm_cursor_lock = SPINLOCK_INIT;
int				 slm_cursor_update_inprog;
int				 slm_cursor_update_needed;

uint64_t			 slm_reclaim_proc_batchno;

int
mds_open_file(char *fn, int flags, void **handle)
{
	mdsio_fid_t mf;
	int rc;

	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], fn, &mf, &rootcreds, NULL);
	if (rc == ENOENT && (flags & O_CREAT)) {
		rc = mdsio_opencreatef(current_vfsid,
		    mds_metadir_inum[current_vfsid], &rootcreds, flags,
		    MDSIO_OPENCRF_NOLINK, 0600, fn, NULL, NULL, handle,
		    NULL, NULL, 0);
	} else if (!rc) {
		flags &= ~O_CREAT;
		rc = mdsio_opencreate(current_vfsid, mf, &rootcreds,
		    flags, 0, NULL, NULL, NULL, handle, NULL, NULL, 0);
	}
	return (rc);
}

int
mds_read_file(void *h, void *buf, uint64_t size, size_t *nb, off_t off)
{
	int rc;

	rc = mdsio_read(current_vfsid, &rootcreds, buf, size, nb, off, h);
	return (rc);
}

int
mds_write_file(void *h, void *buf, uint64_t size, size_t *nb, off_t off)
{
	int rc;

	rc = mdsio_write(current_vfsid, &rootcreds, buf, size, nb, off,
	    h, NULL, NULL);
	return (rc);
}

int
mds_release_file(void *handle)
{
	int rc;

	rc = mdsio_release(current_vfsid, &rootcreds, handle);
	return rc;
}

static void
mds_record_update_prog(void)
{
	struct update_prog_entry *up, *ubase;
	struct sl_mds_peerinfo *sp;
	struct sl_resm *resm;
	size_t size;
	int i, rc;

	ubase = nsupd_prg.prg_buf;

	i = 0;
	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		sp = res2rpmi(resm->resm_res)->rpmi_info;
		up = &ubase[i];
		up->upe_id = resm->resm_res_id;
		up->upe_xid = sp->sp_xid;
		up->upe_batchno = sp->sp_batchno;
		i++;
	);
	rc = mds_write_file(nsupd_prg.prg_handle, nsupd_prg.prg_buf,
	    i * UP_ENTSZ, &size, 0);
	psc_assert(rc == 0);
	psc_assert(size == i * UP_ENTSZ);
}

static void
mds_record_reclaim_prog(void)
{
	int ri, rc, idx, lastindex = 0;
	struct reclaim_prog_entry *rp, *rbase;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct sl_site *s;
	size_t size;

	rbase = reclaim_prg.prg_buf;

	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;

		idx = si->si_index;
		rp = &rbase[idx];
		RPMI_LOCK(rpmi);
		if (si->si_flags & SIF_NEW_PROG_ENTRY) {
			si->si_flags &= ~SIF_NEW_PROG_ENTRY;
			rp->rpe_id = res->res_id;
		}
		RPMI_ULOCK(rpmi);
		psc_assert(rp->rpe_id == res->res_id);

		rp->rpe_xid = si->si_xid;
		rp->rpe_batchno = si->si_batchno;

		if (lastindex < idx)
			lastindex = idx;
	}
	lastindex++;
	rc = mds_write_file(reclaim_prg.prg_handle,
	    reclaim_prg.prg_buf, lastindex * RP_ENTSZ, &size, 0);
	psc_assert(rc == 0);
	psc_assert(size == (size_t)lastindex * RP_ENTSZ);
}

/*
 * Tie system journal with ZFS transaction groups.
 */
void
mds_txg_handler(__unusedx uint64_t *txgp, __unusedx void *data, int op)
{
	psc_assert(op == PJRNL_TXG_GET || op == PJRNL_TXG_PUT);
}

int
mds_remove_logfile(uint64_t batchno, int update, __unusedx int cleanup)
{
	char logfn[PATH_MAX];
	int rc;

	if (update)
		xmkfn(logfn, "%s.%d", SL_FN_UPDATELOG, batchno);
	else
		xmkfn(logfn, "%s.%d", SL_FN_RECLAIMLOG, batchno);

	rc = mdsio_unlink(current_vfsid,
	    mds_metadir_inum[current_vfsid], NULL, logfn, &rootcreds,
	    NULL, NULL);

	if (rc && rc != ENOENT)
		psc_fatalx("Failed to remove log file %s: %s", logfn,
		    sl_strerror(rc));
	if (!rc) {
		psclog_info("Log file %s has been removed", logfn);
		OPSTAT_INCR("logfile-remove");
	}
	return (rc);
}
void
mds_remove_logfiles(uint64_t batchno, int update)
{
	int rc, notfound = 0;
	struct timeval tv1, tv2;
	int64_t i, nfound = 0;

	PFL_GETTIMEVAL(&tv1);
	for (i = (int64_t) batchno - 2; i >= 0; i--) {
		rc = mds_remove_logfile(i, update, 1);
		if (rc) {
			if (++notfound > 10)
				break;
		} else
			nfound++;
	}
	PFL_GETTIMEVAL(&tv2);
	if (nfound)
		psclog_info("%"PRId64" log file(s) have been removed "
		    "in %"PSCPRI_TIMET" second(s); LWM is %"PRId64,
		    nfound, tv2.tv_sec - tv1.tv_sec, batchno);
}

int
mds_open_logfile(uint64_t batchno, int update, int readonly,
    void **handle)
{
	char logfn[PATH_MAX];
	int rc;

	if (update) {
		xmkfn(logfn, "%s.%d", SL_FN_UPDATELOG, batchno);
	} else {
		xmkfn(logfn, "%s.%d", SL_FN_RECLAIMLOG, batchno);
	}
	if (readonly) {
		/* The caller should check the return value. */
		return (mds_open_file(logfn, O_RDONLY, handle));
	}

	/*
	 * Note we use different file descriptors for read and write.
	 *
	 * During replay, we need to read the file first to find out
	 * the right position, so we can't use O_WRONLY.
	 */
	rc = mds_open_file(logfn, O_RDWR, handle);
	if (rc == 0) {
		/*
		 * During replay, the offset will be determined by the
		 * xid.
		 */
		return (rc);
	}
	rc = mds_open_file(logfn, O_CREAT | O_TRUNC | O_WRONLY, handle);
	if (rc)
		psc_fatalx("Failed to create log file %s: %s", logfn,
		    sl_strerror(rc));
	OPSTAT_INCR("logfile-create");
	return (rc);
}

void
mds_write_logentry(uint64_t xid, uint64_t fid, uint64_t gen)
{
	struct srt_reclaim_entry re, *r;
	struct srt_stat sstb;
	int rc, count, total;
	void *reclaimbuf;
	size_t size;

	if (!xid) {
		PJ_LOCK(slm_journal);
		xid = pjournal_next_xid(slm_journal);
		PJ_ULOCK(slm_journal);
	}

	if (reclaim_prg.log_handle)
		goto skip;

	rc = mds_open_logfile(reclaim_prg.cur_batchno, 0, 0,
	    &reclaim_prg.log_handle);
	psc_assert(rc == 0);

	rc = mdsio_getattr(current_vfsid, 0, reclaim_prg.log_handle,
	    &rootcreds, &sstb);

	psc_assert(rc == 0);

	reclaim_prg.log_offset = 0;
	/*
	 * Even if there is no need to replay after a startup, we should
	 * still skip existing entries.
	 */
	if (sstb.sst_size) {
		/* add to existing log; check magic at beginning */
		reclaimbuf = PSCALLOC(sstb.sst_size);
		rc = mds_read_file(reclaim_prg.log_handle, reclaimbuf,
		    sstb.sst_size, &size, 0);
		if (rc || size != sstb.sst_size)
			psc_fatalx("Failed to read reclaim log file, "
			    "batchno=%"PRId64": %s",
			    reclaim_prg.cur_batchno, sl_strerror(rc));

		r = reclaimbuf;
		if (r->xid != RECLAIM_MAGIC_VER ||
		    r->fg.fg_gen != RECLAIM_MAGIC_GEN ||
		    r->fg.fg_fid != RECLAIM_MAGIC_FID)
			psc_fatalx("Reclaim log corrupted, "
			    "batchno=%"PRId64, reclaim_prg.cur_batchno);

		size -= R_ENTSZ;
		r++;
		reclaim_prg.log_offset += R_ENTSZ;

		/* this should never happen, but we have seen bitten */
		if (size > R_ENTSZ * SLM_RECLAIM_BATCH_NENTS ||
		    size % R_ENTSZ) {
			psclog_warnx("Reclaim log corrupted! "
			    "batch=%"PRId64" size=%zd",
			    reclaim_prg.cur_batchno, size);
			size = R_ENTSZ * (SLM_RECLAIM_BATCH_NENTS - 1);
		}
		total = size / R_ENTSZ;
		for (count = 0; count < total; r++, count++) {
			if (r->xid == xid)
				psclog_warnx("Reclaim xid %"PRId64" "
				    "already in use! batch = %"PRId64,
				    xid, reclaim_prg.cur_batchno);
			reclaim_prg.log_offset += R_ENTSZ;
		}
		PSCFREE(reclaimbuf);
		psc_assert(reclaim_prg.log_offset ==
		    (off_t)sstb.sst_size);
	} else {
		/* starting new logfile: write a magic header */
		re.xid = RECLAIM_MAGIC_VER;
		re.fg.fg_fid = RECLAIM_MAGIC_FID;
		re.fg.fg_gen = RECLAIM_MAGIC_GEN;

		rc = mds_write_file(reclaim_prg.log_handle, &re,
		    R_ENTSZ, &size, reclaim_prg.log_offset);
		if (rc || size != R_ENTSZ)
			psc_fatal("Failed to write reclaim log "
			    "file, batchno=%"PRId64,
			    reclaim_prg.cur_batchno);
		reclaim_prg.log_offset += R_ENTSZ;
	}

 skip:
	re.xid = xid;
	re.fg.fg_fid = fid;
	re.fg.fg_gen = gen;

	rc = mds_write_file(reclaim_prg.log_handle, &re, R_ENTSZ,
	    &size, reclaim_prg.log_offset);
	if (size != R_ENTSZ)
		psc_fatal("Failed to write reclaim log file, batchno=%"PRId64,
		    reclaim_prg.cur_batchno);

	spinlock(&mds_distill_lock);
	reclaim_prg.cur_xid = xid;
	freelock(&mds_distill_lock);

	reclaim_prg.log_offset += R_ENTSZ;
	if (reclaim_prg.log_offset == SLM_RECLAIM_BATCH_NENTS *
	    (off_t)R_ENTSZ) {

		mds_release_file(reclaim_prg.log_handle);
		reclaim_prg.log_handle = NULL;

		reclaim_prg.cur_batchno++;
		OPSTAT_INCR("reclaim-batchno");

		spinlock(&mds_distill_lock);
		reclaim_prg.sync_xid = xid;
		freelock(&mds_distill_lock);

		spinlock(&reclaim_prg.lock);
		psc_waitq_wakeall(&reclaim_prg.waitq);
		freelock(&reclaim_prg.lock);

		psclog_info("Next batchno=%"PRId64", "
		    "current reclaim XID=%"PRId64,
		    reclaim_prg.cur_batchno, reclaim_prg.cur_xid);
	}
	psc_assert(reclaim_prg.log_offset <= SLM_RECLAIM_BATCH_NENTS *
	    (off_t)R_ENTSZ);

	psclog_diag("reclaim_prg.cur_xid=%"PRIu64" batchno=%"PRIu64" "
	    "fg="SLPRI_FG,
	    reclaim_prg.cur_xid, reclaim_prg.cur_batchno,
	    SLPRI_FG_ARGS(&re.fg));
}

/*
 * Distill information from the system journal and write into namespace
 * update or garbage reclaim logs.
 *
 * Writing the information to secondary logs allows us to recycle the
 * space in the main system log as quickly as possible.  The distill
 * process is continuous in order to make room for system logs.  Once in
 * a secondary log, we can process them as we see fit.  Sometimes these
 * secondary log files can hang over a long time because a peer MDS or
 * an IO server is down or slow.
 *
 * We encode the cursor creation time and hostname into the log file
 * names to minimize collisions.  If undetected, these collisions can
 * lead to insidious bugs, especially when on-disk format changes.
 *
 * It is called via pj->pj_distill_handler().
 */
int
mds_distill_handler(struct psc_journal_enthdr *pje,
    __unusedx uint64_t xid, int npeers, int action)
{
	struct slmds_jent_namespace *sjnm = NULL;
	struct srt_update_entry ue, *u;
	int rc, count, total;
	uint16_t type;
	size_t size;

	psc_assert(pje->pje_magic == PJE_MAGIC);

	/*
	 * The following can only be executed by the singleton distill
	 * thread.
	 */
	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	if (type != MDS_LOG_NAMESPACE)
		return (0);

	sjnm = PJE_DATA(pje);
	psc_assert(sjnm->sjnm_magic == SJ_NAMESPACE_MAGIC);

	/*
	 * Note that we distill reclaim before update.  This is the same
	 * order we use in recovery.
	 *
	 * If the namespace operation needs to reclaim disk space on I/O
	 * servers, write the information into the reclaim log.
	 */
	if (!(sjnm->sjnm_flag & SJ_NAMESPACE_RECLAIM))
		goto check_update;

	psc_assert(
	    sjnm->sjnm_op == NS_OP_RECLAIM ||
	    sjnm->sjnm_op == NS_OP_SETATTR ||
	    sjnm->sjnm_op == NS_OP_UNLINK ||
	    sjnm->sjnm_op == NS_OP_SETSIZE);

	mds_write_logentry(pje->pje_xid, sjnm->sjnm_target_fid,
	    sjnm->sjnm_target_gen);

 check_update:
	if (!npeers)
		return (0);

	if (nsupd_prg.log_handle == NULL) {
		nsupd_prg.log_offset = 0;
		mds_open_logfile(nsupd_prg.cur_batchno, 1, 0,
		    &nsupd_prg.log_handle);

		if (action == 1) {
			rc = mds_read_file(nsupd_prg.log_handle,
			    nsupd_prg.log_buf, SLM_UPDATE_BATCH_NENTS *
			    U_ENTSZ, &size, 0);
			if (rc)
				psc_fatalx("Failed to read update log "
				    "file, batchno=%"PRId64": %s",
				    nsupd_prg.cur_batchno,
				    sl_strerror(rc));

			total = size / U_ENTSZ;
			u = nsupd_prg.log_buf;
			for (count = 0; count < total; u++, count++) {
				if (u->xid == pje->pje_xid)
					break;
				nsupd_prg.log_offset += U_ENTSZ;
			}
		}
	}

	memset(&ue, 0, U_ENTSZ);
	ue.xid = pje->pje_xid;

#if 0
	/*
	 * Fabricate a setattr update entry to change the size.
	 */
	if (type == MDS_LOG_BMAP_CRC) {
		ue.op = NS_OP_SETSIZE;
		ue.mask = mdsio_slflags_2_setattrmask(
		    PSCFS_SETATTRF_DATASIZE);
		ue.size = sjbc->sjbc_fsize;
		ue.target_fid = sjbc->sjbc_fid;
		goto write_update;
	}
#endif

	ue.op = sjnm->sjnm_op;
	ue.target_gen = sjnm->sjnm_target_gen;
	ue.parent_fid = sjnm->sjnm_parent_fid;
	ue.target_fid = sjnm->sjnm_target_fid;
	ue.new_parent_fid = sjnm->sjnm_new_parent_fid;

	ue.mode = sjnm->sjnm_mode;
	ue.mask = sjnm->sjnm_mask;
	ue.uid = sjnm->sjnm_uid;
	ue.gid = sjnm->sjnm_gid;

	ue.size = sjnm->sjnm_size;

	ue.atime = sjnm->sjnm_atime;
	ue.mtime = sjnm->sjnm_mtime;
	ue.ctime = sjnm->sjnm_ctime;
	ue.atime_ns = sjnm->sjnm_atime_ns;
	ue.mtime_ns = sjnm->sjnm_mtime_ns;
	ue.ctime_ns = sjnm->sjnm_ctime_ns;

	ue.namelen = sjnm->sjnm_namelen;
	ue.namelen2 = sjnm->sjnm_namelen2;
	memcpy(ue.name, sjnm->sjnm_name,
	    sjnm->sjnm_namelen + sjnm->sjnm_namelen2);
#if 0

 write_update:

#endif
	rc = mds_write_file(nsupd_prg.log_handle, &ue, U_ENTSZ, &size,
	    nsupd_prg.log_offset);
	if (size != U_ENTSZ)
		psc_fatal("failed to write update log file, "
		    "batchno=%"PRId64" rc=%d",
		    nsupd_prg.cur_batchno, rc);

	/* see if we need to close the current update log file */
	nsupd_prg.log_offset += U_ENTSZ;
	if (nsupd_prg.log_offset == SLM_UPDATE_BATCH_NENTS * U_ENTSZ) {
		mds_release_file(nsupd_prg.log_handle);

		nsupd_prg.log_handle = NULL;
		nsupd_prg.cur_batchno++;

		spinlock(&mds_distill_lock);
		nsupd_prg.sync_xid = pje->pje_xid;
		freelock(&mds_distill_lock);

		spinlock(&nsupd_prg.lock);
		psc_waitq_wakeall(&nsupd_prg.waitq);
		freelock(&nsupd_prg.lock);
	}

	spinlock(&mds_distill_lock);
	nsupd_prg.cur_xid = pje->pje_xid;
	freelock(&mds_distill_lock);

	return (0);
}

const char *slm_ns_opnames[] = {
	"create",
	"link",
	"mkdir",
	"rename",
	"rmdir",
	"setsize",
	"setattr",
	"symlink",
	"unlink",
	"reclaim"
};

int
slm_wk_rmdir_ino(void *p)
{
	struct slm_wkdata_rmdir_ino *wk = p;
	char name[24];

	snprintf(name, sizeof(name), "%016"PRIx64".ino", wk->fid);
	mdsio_unlink(current_vfsid, mdsio_getfidlinkdir(wk->fid),
	    NULL, name, &rootcreds, NULL, NULL);
	return (0);
}

/*
 * Log a namespace operation before we attempt it.  This makes sure that
 * it will be propagated towards other MDSes and made permanent before
 * we reply to the client.
 */
void
mdslog_namespace(int op, uint64_t txg, uint64_t pfid, uint64_t npfid,
    const struct srt_stat *sstb, int mask, const char *name,
    const char *newname, void *arg)
{
	struct slmds_jent_namespace *sjnm;
	int chg, distill = 0;
	size_t siz;

	if (op == NS_OP_SETATTR)
		psc_assert(mask);

	if (op == NS_OP_CREATE || op == NS_OP_MKDIR)
		psc_assert(sstb->sst_fid);

	sjnm = pjournal_get_buf(slm_journal, sizeof(*sjnm));
	memset(sjnm, 0, sizeof(*sjnm));
	sjnm->sjnm_magic = SJ_NAMESPACE_MAGIC;
	sjnm->sjnm_op = op;
	sjnm->sjnm_parent_fid = pfid;
	sjnm->sjnm_target_fid = sstb->sst_fid;
	sjnm->sjnm_new_parent_fid = npfid;
	sjnm->sjnm_mask = mask;

	sjnm->sjnm_uid = sstb->sst_uid;
	sjnm->sjnm_gid = sstb->sst_gid;
	sjnm->sjnm_mode = sstb->sst_mode;
	sjnm->sjnm_atime = sstb->sst_atime;
	sjnm->sjnm_atime_ns = sstb->sst_atime_ns;
	sjnm->sjnm_mtime = sstb->sst_mtime;
	sjnm->sjnm_mtime_ns = sstb->sst_mtime_ns;
	sjnm->sjnm_ctime = sstb->sst_ctime;
	sjnm->sjnm_ctime_ns = sstb->sst_ctime_ns;
	sjnm->sjnm_size = sstb->sst_size;

	siz = sstb->sst_size;

	/*
	 * We need distill if we have a peer MDS or we need to do
	 * garbage reclamation.
	 */
	distill = pjournal_has_peers(slm_journal);
	if ((op == NS_OP_RECLAIM) ||
	    (op == NS_OP_UNLINK && sstb->sst_nlink == 1) ||
	    (op == NS_OP_SETSIZE && sstb->sst_size == 0)) {
		/*
		 * We want to reclaim the space taken by the previous
		 * generation.  Note that changing the attributes of a
		 * zero-length file should NOT trigger this code.
		 *
		 * Add 1000 to differentiate the reason for distilling
		 * in the log messages.
		 *
		 * 10/20/2017: It is not likely we have 1000 peers.
		 */
		distill += 1000;
		sjnm->sjnm_flag |= SJ_NAMESPACE_RECLAIM;
		sjnm->sjnm_target_gen = sstb->sst_gen;
		if (op == NS_OP_SETSIZE) {
			psc_assert(sstb->sst_gen >= 1);
			sjnm->sjnm_target_gen--;
		}
	}

	if (name) {
		sjnm->sjnm_namelen = strlen(name);
		memcpy(sjnm->sjnm_name, name, sjnm->sjnm_namelen);
	}
	if (newname) {
		sjnm->sjnm_namelen2 = strlen(newname);
		memcpy(sjnm->sjnm_name + sjnm->sjnm_namelen, newname,
		    sjnm->sjnm_namelen2);
	}
	psc_assert(sjnm->sjnm_namelen + sjnm->sjnm_namelen2 <=
	    sizeof(sjnm->sjnm_name));

	pjournal_add_entry(slm_journal, txg, MDS_LOG_NAMESPACE, distill,
	    sjnm, offsetof(struct slmds_jent_namespace, sjnm_name) +
	    sjnm->sjnm_namelen + sjnm->sjnm_namelen2);

	if (!distill)
		pjournal_put_buf(slm_journal, sjnm);

	psclog_info("namespace op %s (%d): distill=%d "
	    "fid="SLPRI_FID" name='%s%s%s' mask=%#x size=%"PRId64" "
	    "link=%"PRId64" pfid="SLPRI_FID" npfid="SLPRI_FID" txg=%"PRId64,
	    slm_ns_opnames[op], op, distill, sstb->sst_fid, name,
	    newname ? "' newname='" : "", newname ? newname : "",
	    mask, sstb->sst_size, sstb->sst_nlink, pfid, npfid, txg);

	switch (op) {
	case NS_OP_UNLINK:
		if (sstb->sst_nlink > 1)
			COPYFG((struct sl_fidgen *)arg, &sstb->sst_fg);
		break;
	case NS_OP_RENAME: {

		/* filled in by zfs_rename() */
		struct zfs_rename_log_arg {
			struct sl_fidgen clfg;
			struct sl_fidgen *fgp;
		} *aa;

		aa = arg;
		COPYFG(&aa->fgp[0], &sstb->sst_fg);
		COPYFG(&aa->fgp[1], &aa->clfg);
		break;
	    }
	case NS_OP_SETSIZE:
		siz = *(size_t *)arg;
		break;
	}

	chg = 0;
	if (op == NS_OP_SETATTR) {
		int sl_mask;

		sl_mask = mdsio_setattrmask_2_slflags(mask);
		if (sl_mask & (PSCFS_SETATTRF_UID | PSCFS_SETATTRF_GID))
			chg = 1;
	}

	if (chg ||
	    op == NS_OP_RECLAIM ||
	    (op == NS_OP_UNLINK && sstb->sst_nlink == 1) ||
	    (op == NS_OP_SETSIZE && sstb->sst_size == 0))
		psclogs(PLL_INFO, SLMSS_INFO,
		    "file data %s fg="SLPRI_FG" "
		    "uid=%u gid=%u "
		    "fsize=%zu op=%d",
		    chg ? "changed" : "removed",
		    SLPRI_FG_ARGS(&sstb->sst_fg),
		    sstb->sst_uid, sstb->sst_gid,
		    siz, op);

	/*
 	 * XXX The purpose of ZFS callback is to link a transaction ID with
 	 * the corresponding journal log entry which lives outside of the ZFS.
 	 * So anything other than writing the log entry should go away.
 	 */
	if (op == NS_OP_RECLAIM ||
	    (op == NS_OP_UNLINK && sstb->sst_nlink == 1)) {
		struct slm_wkdata_upsch_purge *wk;

		wk = pfl_workq_getitem(slm_wk_upsch_purge,
		    struct slm_wkdata_upsch_purge);
		wk->fid = sstb->sst_fid;
		wk->bno = BMAPNO_ANY;
		pfl_workq_putitemq(&slm_db_lopri_workq, wk);
	}

	if (op == NS_OP_RMDIR) {
		struct slm_wkdata_upsch_purge *wk;

		wk = pfl_workq_getitem(slm_wk_rmdir_ino,
		    struct slm_wkdata_rmdir_ino);
		wk->fid = sstb->sst_fid;
		pfl_workq_putitem(wk);
	}
}

/*
 * Find the lowest garbage reclamation watermark of all IOSes.
 */
__static uint64_t
mds_reclaim_lwm(int batchno)
{
	uint64_t value = UINT64_MAX;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct sl_site *s;
	int ri;

	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		/*
		 * Prevents reading old log file repeatedly only to
		 * find out that an IOS is down.
		 */
		if (si->si_flags & SIF_DISABLE_GC) {
			RPMI_ULOCK(rpmi);
			continue;
		}
		if (batchno) {
			if (si->si_batchno < value)
				value = si->si_batchno;
		} else {
			if (si->si_xid < value)
				value = si->si_xid;
		}
		RPMI_ULOCK(rpmi);
	}
	psc_assert(value != UINT64_MAX);
	return (value);
}

__static uint64_t
mds_reclaim_hwm(int batchno)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct sl_site *s;
	uint64_t value = 0;
	int ri;

	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (si->si_batchno > value)
				value = si->si_batchno;
		} else {
			if (si->si_xid > value)
				value = si->si_xid;
		}
		RPMI_ULOCK(rpmi);
	}
	return (value);
}

/*
 * Find the lowest namespace update watermark of all peer MDSes.
 */
__static uint64_t
mds_update_lwm(int batchno)
{
	uint64_t value = UINT64_MAX;
	struct resprof_mds_info *rpmi;
	struct sl_mds_peerinfo *sp;
	struct sl_resm *resm;

	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		sp = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (sp->sp_batchno < value)
				value = sp->sp_batchno;
		} else {
			if (sp->sp_xid < value)
				value = sp->sp_xid;
		}
		RPMI_ULOCK(rpmi);
	);
	psc_assert(value != UINT64_MAX);
	return (value);
}

__static uint64_t
mds_update_hwm(int batchno)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_peerinfo *sp;
	struct sl_resm *resm;
	uint64_t value = 0;

	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		sp = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (sp->sp_batchno > value)
				value = sp->sp_batchno;
		} else {
			if (sp->sp_xid > value)
				value = sp->sp_xid;
		}
		RPMI_ULOCK(rpmi);
	);
	return (value);
}

void
mds_skip_reclaim_batch(uint64_t batchno)
{
	int ri, nios = 0, record = 0, didwork = 0;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct sl_site *s;

	if (batchno >= reclaim_prg.cur_batchno)
		return;

	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		nios++;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;
		if (si->si_batchno < batchno)
			continue;
		if (si->si_batchno > batchno) {
			didwork++;
			continue;
		}
		record = 1;
		didwork++;
		si->si_batchno++;
	}
	if (record)
		mds_record_reclaim_prog();

	psclog_warnx("Skipping reclaim log file, batchno=%"PRId64,
	    batchno);

	if (didwork == nios && batchno >= 1)
		mds_remove_logfile(batchno - 1, 0, 0);
}

/*
 * Send a batch of updates to peer MDSes that want them.
 */
int
mds_send_batch_update(uint64_t batchno)
{
	int siter, i, rc, npeers, count, total, didwork = 0, record = 0;
	struct srt_update_entry *u, *nu;
	struct slrpc_cservice *csvc;
	struct sl_mds_peerinfo *sp;
	struct srm_update_rep *mp;
	struct srm_update_req *mq;
	struct pscrpc_request *rq;
	struct sl_resource *res;
	struct sl_resm *resm;
	struct sl_site *site;
	struct iovec iov;
	uint64_t xid;
	size_t size;
	void *handle;

	rc = mds_open_logfile(batchno, 1, 1, &handle);
	if (rc) {
		/*
		 * It is fine that the distill process hasn't written
		 * the next log file after closing the old one.
		 */
		if (rc != ENOENT)
			psc_fatalx("failed to open update log file, "
			    "batchno=%"PRId64": %s",
			    batchno, sl_strerror(rc));
		return (didwork);
	}
	rc = mds_read_file(handle, nsupd_prg.log_buf,
	    SLM_UPDATE_BATCH_NENTS * U_ENTSZ, &size, 0);
	mds_release_file(handle);

	if (size == 0)
		return (didwork);

	psc_assert(size % U_ENTSZ == 0);
	count = (int)size / (int)U_ENTSZ;

	/* Find the xid associated with the last log entry. */
	u = PSC_AGP(nsupd_prg.log_buf, (count - 1) * U_ENTSZ);
	xid = u->xid;

	/* Trim padding from buffer to reduce RPC traffic. */
	u = nu = nsupd_prg.log_buf;
	size = UPDATE_ENTRY_LEN(u);
	for (i = 1; i < count; i++) {
		u++;
		nu = PSC_AGP(nu, UPDATE_ENTRY_LEN(nu));
		memmove(nu, u, UPDATE_ENTRY_LEN(u));
		size += UPDATE_ENTRY_LEN(u);
	}

	npeers = 0;

	CONF_LOCK();
	CONF_FOREACH_SITE(site)
	    SITE_FOREACH_RES(site, res, siter) {
		if (res->res_type != SLREST_MDS)
			continue;
		resm = psc_dynarray_getpos(&res->res_members, 0);
		if (resm == nodeResm)
			continue;
		npeers++;
		sp = resm2rpmi(resm)->rpmi_info;

		/*
		 * A simplistic backoff strategy to avoid CPU spinning.
		 * A better way could be to let the ping thread handle
		 * this.
		 */
		if (sp->sp_fails >= 3) {
			if (sp->sp_skips == 0) {
				sp->sp_skips = 3;
				continue;
			}
			sp->sp_skips--;
			if (sp->sp_skips)
				continue;
		}
		if (sp->sp_batchno < batchno)
			continue;

		/*
		 * Note that the update xid we can see is not
		 * necessarily contiguous.
		 */
		if (sp->sp_batchno > batchno ||
		    sp->sp_xid > xid) {
			didwork++;
			continue;
		}

		/* Find out which part of the buffer should be sent out */
		i = count;
		total = size;
		u = nsupd_prg.log_buf;
		do {
			if (u->xid >= sp->sp_xid)
				break;
			i--;
			total -= UPDATE_ENTRY_LEN(u);
			u = PSC_AGP(u, UPDATE_ENTRY_LEN(u));
		} while (total);

		psc_assert(total);

		iov.iov_len = total;
		iov.iov_base = u;

		csvc = slm_getmcsvc_wait(resm, 0);
		if (csvc == NULL) {
			sp->sp_fails++;
			continue;
		}
		rc = SL_RSX_NEWREQ(csvc, SRMT_NAMESPACE_UPDATE, rq, mq,
		    mp);
		if (rc) {
			sl_csvc_decref(csvc);
			continue;
		}
		mq->count = i;
		mq->size = iov.iov_len;
		mq->siteid = nodeSite->site_id;

		slrpc_bulkclient(rq, BULK_GET_SOURCE, SRMM_BULK_PORTAL,
		    &iov, 1);

		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
		else
			sp->sp_fails++;

		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);

		if (rc == 0) {
			record++;
			didwork++;
			sp->sp_fails = 0;
			sp->sp_xid = xid + 1;
			if (count == SLM_UPDATE_BATCH_NENTS)
				sp->sp_batchno++;
		}
	}
	CONF_ULOCK();

	/*
	 * Record the progress first before potentially removing an old
	 * log file.
	 */
	if (record)
		mds_record_update_prog();
	if (didwork == npeers &&
	    count == SLM_UPDATE_BATCH_NENTS &&
	    batchno >= 1)
		mds_remove_logfile(batchno - 1, 1, 0);
	return (didwork);
}

/*
 * Write some system information into our cursor file.  Note that every
 * field must be protected by a spinlock. It is called from zfs_write().
 */
void
mds_update_cursor(void *buf, uint64_t txg, int flag)
{
	static uint64_t start_txg = 0;
	struct psc_journal_cursor *cursor = buf;

	if (flag == 1) {
		start_txg = txg;
		pjournal_update_txg(slm_journal, txg);
		return;
	}
	psc_assert(start_txg == txg);

	/*
	 * During the replay, actually as soon as ZFS starts, its group
	 * transaction number starts to increase.  If we crash in the
	 * middle of a replay, we can miss replaying some entries if we
	 * update the txg at this point.
	 */
	if ((slm_journal->pj_flags & PJF_REPLAYINPROG) == 0) {
		spinlock(&mds_txg_lock);
		cursor->pjc_commit_txg = txg;
		freelock(&mds_txg_lock);
	} else {
		/*
		 * Paranoid.  Until I am done, no one can make progress
		 * in ZFS.  Still let us wait a bit to make sure the
		 * replay thread has updated the replay xid.
		 */
		sleep(1);
		cursor->pjc_replay_xid = pjournal_next_replay(
		    slm_journal);
	}

	/*
	 * Be conservative.  We are willing to do extra work than
	 * missing some.
	 */
	spinlock(&mds_distill_lock);
	if (nsupd_prg.sync_xid < reclaim_prg.sync_xid)
		cursor->pjc_distill_xid = nsupd_prg.sync_xid;
	else
		cursor->pjc_distill_xid = reclaim_prg.sync_xid;
	freelock(&mds_distill_lock);

	cursor->pjc_fid = slm_get_curr_slashfid();

	mds_bmap_getcurseq(&cursor->pjc_seqno_hwm, &cursor->pjc_seqno_lwm);
}

/*
 * Update the cursor file in the ZFS that records the current
 * transaction group number and other system log status.  If there is no
 * activity in system other than this write to update the cursor, our
 * customized ZFS will extend the lifetime of the transaction group.
 */
void
slmjcursorthr_main(struct psc_thread *thr)
{
	int rc;

	while (pscthr_run(thr)) {
		spinlock(&slm_cursor_lock);
		if (!slm_cursor_update_needed) {
			slm_cursor_update_inprog = 0;
			psc_waitq_wait(&slm_cursor_waitq,
			    &slm_cursor_lock);
			spinlock(&slm_cursor_lock);
		}
		slm_cursor_update_inprog = 1;
		freelock(&slm_cursor_lock);

		/* Use SLASH2_CURSOR_UPDATE to write cursor file */
		rc = mdsio_write_cursor(current_vfsid, &mds_cursor,
		    sizeof(mds_cursor), mds_cursor_handle,
		    mds_update_cursor);
		if (rc)
			psclog_warnx("failed to update cursor, rc=%d",
			    rc);
		else
			psclog_diag("cursor updated: txg=%"PRId64" "
			    "xid=%"PRId64" fid="SLPRI_FID" "
			    "seqno=(lo=%"PRIx64" hi=%"PRIx64")",
			    mds_cursor.pjc_commit_txg,
			    mds_cursor.pjc_distill_xid,
			    mds_cursor.pjc_fid,
			    mds_cursor.pjc_seqno_lwm,
			    mds_cursor.pjc_seqno_hwm);
		OPSTAT_INCR("slm-cursor-update");
	}
}

void
mds_open_cursor(void)
{
	char *p, tmbuf[26];
	mdsio_fid_t mf;
	size_t nb;
	time_t tm;
	int rc;

	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], SL_FN_CURSOR, &mf,
	    &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(current_vfsid, mf, &rootcreds, O_RDWR, 0,
	    NULL, NULL, NULL, &mds_cursor_handle, NULL, NULL, 0);
	psc_assert(!rc && mds_cursor_handle);

	rc = mdsio_read(current_vfsid, &rootcreds, &mds_cursor,
	    sizeof(struct psc_journal_cursor), &nb, 0, mds_cursor_handle);
	psc_assert(rc == 0 && nb == sizeof(struct psc_journal_cursor));

	psc_assert(mds_cursor.pjc_magic == PJRNL_CURSOR_MAGIC);
	psc_assert(mds_cursor.pjc_version == PJRNL_CURSOR_VERSION);
	psc_assert(mds_cursor.pjc_fid >= SLFID_MIN);

#if 0
	if (FID_GET_SITEID(mds_cursor.pjc_fid) == 0)
		mds_cursor.pjc_fid |= (uint64_t)nodeSite->site_id <<
		    SLASH_FID_MDSID_SHFT;
	if (FID_GET_SITEID(mds_cursor.pjc_fid) != nodeSite->site_id)
		psc_fatal("Mismatched site ID in the FID, expected %d",
		    nodeSite->site_id);
#endif
	/* old utility does not set fsid, so we fill it here */
	if (FID_GET_SITEID(mds_cursor.pjc_fid) == 0)
		FID_SET_SITEID(mds_cursor.pjc_fid,
		    zfs_mounts[current_vfsid].zm_siteid);

#if 0
	/* backward compatibility */
	if (mount_index == 1) {
		psc_assert(current_vfsid == 0);
		zfs_mounts[current_vfsid].fsid = FID_GET_SITEID(mds_cursor.pjc_fid);
	}
#endif

	slm_set_curr_slashfid(mds_cursor.pjc_fid);

	psclog_info("SLFID prior to replay="SLPRI_FID,
	    mds_cursor.pjc_fid);

	mds_bmap_setcurseq(mds_cursor.pjc_seqno_hwm, mds_cursor.pjc_seqno_lwm);

	psclogs_info(SLMSS_INFO, "bmap sequence number LWM before replay is %"PRId64,
	    mds_cursor.pjc_seqno_lwm);
	psclogs_info(SLMSS_INFO, "bmap sequence number HWM before replay is %"PRId64,
	    mds_cursor.pjc_seqno_hwm);

	tm = mds_cursor.pjc_timestamp;
	ctime_r(&tm, tmbuf);
	p = strchr(tmbuf, '\n');
	if (p)
		*p = '\0';
	psclog_info("file system was formatted on %s "
	    "(%"PSCPRI_TIMET")", tmbuf, tm);
}

struct reclaim_arg {
	struct psc_spinlock	lock;
	struct psc_waitq	wq;
	uint64_t		xid;
	int			count;
	int			ndone;
	int			record;
};

int
slm_rim_reclaim_cb(struct pscrpc_request *rq,
    struct pscrpc_async_args *av)
{
	struct slrpc_cservice *csvc = av->pointer_arg[CBARG_CSVC];
	struct reclaim_arg *ra = av->pointer_arg[CBARG_RARG];
	struct sl_resource *res = av->pointer_arg[CBARG_RES];
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	int rc;

	SL_GET_RQ_STATUS_TYPE(csvc, rq, struct srm_reclaim_rep, rc);

	rpmi = res2rpmi(res);
	si = rpmi->rpmi_info;

	if (rc)
		OPSTAT_INCR("reclaim-rpc-fail");
	else {
		OPSTAT_INCR("reclaim-rpc-send");

		RPMI_LOCK(rpmi);
		si->si_xid = ra->xid + 1;
		if (ra->count == SLM_RECLAIM_BATCH_NENTS)
			si->si_batchno++;
		RPMI_ULOCK(rpmi);

		spinlock(&ra->lock);
		ra->record = 1;
		ra->ndone++;
		freelock(&ra->lock);
	}

	/*
 	 * Saw rc = -110 below during bigfile.sh test.  The problem
 	 * is lime is every slow with only two disks. It can't keep
 	 * up.  So we timed out here.  However, we should not drop
 	 * the connection with pscrpc_fail_import() just because a
 	 * single RPC fails. As a result, the MDS will tell the client
 	 * to write elsewhere even if lime is its preferred IOS.
 	 * The good news is that lime pings MDS and come back online
 	 * shortly.  Also, the bigfile.sh test passes as well.
 	 *
 	 * We need to make this scenario more robust.
 	 */

	psclog(rc ? PLL_ERROR : PLL_DIAG,
	    "reclaim batchno=%"PRId64" res=%s rc=%d",
	    si->si_batchno, res->res_name, rc);

	sl_csvc_decref(csvc);

	return (0);
}

/*
 * Send a batch of RECLAIM messages to IOS nodes.  A RECLAIM message
 * contains a list of FIDs which are no longer active in the file system
 * and may be deleted on the IOS backend.
 *
 * The FIDs are bunched into a batch group with an associated number
 * @batchno.  On entry, this batch group is sent to each online IOS.
 * On exit, the value-result pointer is set to the next batch group
 * considering which IOS nodes are online and which batch they are
 * currently "on".
 */
int
mds_send_batch_reclaim(uint64_t *pbatchno)
{
	int i, ri, rc, total, nios;
	uint64_t batchno, next_batchno;
	struct slrpc_cservice *csvc;
	struct pscrpc_request_set *set;
	struct resprof_mds_info *rpmi;
	struct srt_reclaim_entry *r;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct pscrpc_request *rq;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct reclaim_arg rarg;
	struct srt_stat sstb;
	struct sl_site *s;
	struct sl_resm *m;
	struct iovec iov;
	void *handle;
	size_t size;

	slm_reclaim_proc_batchno = batchno = (*pbatchno)++;

	rc = mds_open_logfile(batchno, 0, 1, &handle);
	if (rc) {
		/*
		 * It is fine that the distill process hasn't written
		 * the next log file after closing the old one.
		 */
		if (rc != ENOENT)
			psc_fatalx("Failed to open reclaim log file, "
			    "batchno=%"PRId64": %s",
			    batchno, sl_strerror(rc));

		/*
		 * However, if the log file is missing for some reason,
		 * we skip it so that we can make progress.
		 */
		if (batchno < reclaim_prg.cur_batchno) {
			mds_skip_reclaim_batch(batchno);
			return (1);
		}
		return (0);
	}
	rc = mdsio_getattr(current_vfsid, 0, handle, &rootcreds, &sstb);
	psc_assert(rc == 0);

	if (sstb.sst_size == 0) {
		mds_release_file(handle);
		psclog_warnx("Zero size reclaim log file, "
		    "batchno=%"PRId64, batchno);
		return (0);
	}
	reclaim_prg.log_buf = psc_realloc(reclaim_prg.log_buf,
	    sstb.sst_size, 0);

	rc = mds_read_file(handle, reclaim_prg.log_buf, sstb.sst_size,
	    &size, 0);
	psc_assert(rc == 0 && sstb.sst_size == size);
	mds_release_file(handle);

	if (size == R_ENTSZ) {
		psclog_warnx("Empty reclaim log file, batchno=%"PRId64,
		    batchno);
		return (0);
	}

	/*
	 * XXX XXX XXX XXX hack
	 * We have seen odd file size (> 600MB) without any clue.
	 * To avoid confusing other code on the MDS and sliod, pretend
	 * we have done the job and move on.
	 */
	if (size > R_ENTSZ * SLM_RECLAIM_BATCH_NENTS ||
	    size % R_ENTSZ) {
		psclog_warnx("Reclaim log corrupted! "
		    "batch=%"PRIx64" size=%zd",
		    batchno, size);
		mds_skip_reclaim_batch(batchno);
		return (1);
	}
	memset(&rarg, 0, sizeof(rarg));
	INIT_SPINLOCK(&rarg.lock);
	rarg.count = size / R_ENTSZ;

	set = pscrpc_prep_set();

	/* Find the xid associated with the last log entry. */
	r = PSC_AGP(reclaim_prg.log_buf, (rarg.count - 1) * R_ENTSZ);
	rarg.xid = r->xid;

	size -= R_ENTSZ;

	next_batchno = UINT64_MAX;

	nios = 0;
	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		nios++;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;
		m = psc_dynarray_getpos(&res->res_members, 0);

		csvc = slm_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);

		RPMI_LOCK(rpmi);

		/*
		 * We won't need this if the IOS is actually down.
		 * But we need to shortcut it for testing purposes.
		 */
		if (si->si_flags & SIF_DISABLE_GC) {
			RPMI_ULOCK(rpmi);
			if (csvc)
				sl_csvc_decref(csvc);
			continue;
		}

		if (csvc) {
			next_batchno = MIN(next_batchno,
			    si->si_batchno);
			sl_csvc_decref(csvc);
		} else {
			RPMI_ULOCK(rpmi);
			continue;
		}

		if (si->si_batchno < batchno) {
			RPMI_ULOCK(rpmi);
			continue;
		}

		/*
		 * Note that the reclaim xid we can see is not
		 * necessarily contiguous.
		 *
		 * We only check for xid when the log file is not full
		 * to get around some internally corrupted log file (xid
		 * is not increasing all the way).
		 *
		 * Note that this workaround only applies when the
		 * batchno already matches.
		 */
		if (si->si_batchno > batchno ||
		    (rarg.count < SLM_RECLAIM_BATCH_NENTS &&
		     si->si_xid > rarg.xid)) {
			RPMI_ULOCK(rpmi);

			spinlock(&rarg.lock);
			rarg.ndone++;
			freelock(&rarg.lock);
			continue;
		}

		RPMI_ULOCK(rpmi);

		/*
		 * Find out which part of the buffer should be sent out.
		 */
		i = rarg.count - 1;
		total = size;
		r = reclaim_prg.log_buf;
		r++;

		/*
		 * In a perfect world, si_xid <= xid is always true.
		 * This is because batchno and xid are related.
		 * But I was met with cold reality and couldn't explain
		 * why it happened.  Anyway, resending requests is not
		 * the end of the world.
		 */
		if (si->si_xid <= rarg.xid) {
			do {
				if (r->xid >= si->si_xid)
					break;
				i--;
				total -= R_ENTSZ;
				r++;
			} while (total);
		} else
			psclog_warnx("batch (%"PRId64") versus xids "
			    "(%"PRId64":%"PRId64")",
			    batchno, si->si_xid, rarg.xid);

		psc_assert(total);

		rq = NULL;
		csvc = slm_geticsvcf(m, CSVCF_NONBLOCK | CSVCF_NORECON, 0);
		if (csvc == NULL)
			PFL_GOTOERR(out, rc = SLERR_ION_OFFLINE);
		rc = SL_RSX_NEWREQ(csvc, SRMT_RECLAIM, rq, mq, mp);
		if (rc)
			PFL_GOTOERR(out, rc);

		iov.iov_len = total;
		iov.iov_base = r;

		mq->batchno = si->si_batchno;
		mq->xid = rarg.xid;
		mq->size = iov.iov_len;
		mq->count = i;

		slrpc_bulkclient(rq, BULK_GET_SOURCE, SRIM_BULK_PORTAL,
		    &iov, 1);

		rq->rq_interpret_reply = slm_rim_reclaim_cb;
		rq->rq_async_args.pointer_arg[CBARG_CSVC] = csvc;
		rq->rq_async_args.pointer_arg[CBARG_RARG] = &rarg;
		rq->rq_async_args.pointer_arg[CBARG_RES] = res;
		rc = SL_NBRQSETX_ADD(set, csvc, rq);
		if (!rc) 
			continue;
 out:
		if (rq)
			pscrpc_req_finished(rq);
		if (csvc)
			sl_csvc_decref(csvc);

		OPSTAT_INCR("reclaim-rpc-fail");
		psclog(rc == SLERR_ION_OFFLINE ? PLL_INFO :
		    PLL_WARN, "reclaim RPC failure: "
		    "batchno=%"PRId64" dst=%s rc=%d",
		    batchno, res->res_name, rc);
	}

	pscrpc_set_wait(set);
	pscrpc_set_destroy(set);

	/*
	 * Record the progress first before potentially removing old log
	 * files.
	 */
	if (rarg.record)
		mds_record_reclaim_prog();

	/*
	 * XXX if the log file is never filled to its capacity for some
	 * reason, then we are stuck!  Perhaps we should check for the
	 * existence of the next log file and skip it.  But we must make
	 * sure all IOS have seen this log file.
	 *
	 * If this log file is full and all I/O servers have applied its
	 * contents, remove an old log file (keep the previous one so
	 * that we can figure out the last distill xid upon recovery).
	 */
	if (rarg.ndone == nios &&
	    rarg.count == SLM_RECLAIM_BATCH_NENTS &&
	    batchno >= 1)
		mds_remove_logfile(batchno - 1, 0, 0);

	if (next_batchno != UINT64_MAX &&
	    next_batchno != batchno)
		*pbatchno = next_batchno;

	return (rarg.ndone);
}

/*
 * Send garbage collection to I/O servers.
 */
void
slmjreclaimthr_main(struct psc_thread *thr)
{
	int didwork, cleanup = 1;
	uint64_t batchno;

	/*
	 * Instead of tracking precisely which reclaim log record has
	 * been sent to an I/O node, we track the batch number.  A
	 * receiving I/O node can safely ignore any resent records.
	 */
	while (pscthr_run(thr)) {
		batchno = mds_reclaim_lwm(1);
		if (cleanup) {
			cleanup = 0;
			mds_remove_logfiles(batchno, 0);
		}
		do {
			spinlock(&mds_distill_lock);
			if (!reclaim_prg.cur_xid ||
			    mds_reclaim_lwm(0) > reclaim_prg.cur_xid) {
				freelock(&mds_distill_lock);
				break;
			}
			freelock(&mds_distill_lock);
			didwork = mds_send_batch_reclaim(&batchno);
		} while (didwork && mds_reclaim_hwm(1) >= batchno);

		spinlock(&reclaim_prg.lock);
		psc_waitq_waitrel_s(&reclaim_prg.waitq,
		    &reclaim_prg.lock, SL_RECLAIM_MAX_AGE);
	}
}

/*
 * Send local namespace updates to peer MDSes.
 */
void
slmjnsthr_main(struct psc_thread *thr)
{
	uint64_t batchno;
	int didwork;

	/*
	 * This thread scans the batches of updates between the low and
	 * high water marks and sends them to peer MDSes.  Although
	 * different MDSes have different paces, we send updates in
	 * order within one MDS.
	 */
	while (pscthr_run(thr)) {
		batchno = mds_update_lwm(1);
		do {
			spinlock(&mds_distill_lock);
			if (!nsupd_prg.cur_xid ||
			    mds_update_lwm(0) > nsupd_prg.cur_xid) {
				freelock(&mds_distill_lock);
				break;
			}
			freelock(&mds_distill_lock);
			didwork = mds_send_batch_update(batchno);
			batchno++;
		} while (didwork && mds_update_hwm(1) >= batchno);

		spinlock(&nsupd_prg.lock);
		psc_waitq_waitrel_s(&nsupd_prg.waitq, &nsupd_prg.lock,
		    SL_UPDATE_MAX_AGE);
	}
}

void
mdslogfill_ino_repls(struct fidc_membh *f,
    struct slmds_jent_ino_repls *sjir)
{
	struct slash_inode_handle *ih;
	int locked;

	locked = FCMH_RLOCK(f);
	ih = fcmh_2_inoh(f);

	if (!fcmh_isdir(f))
		psc_assert(fcmh_2_nrepls(f));

	sjir->sjir_fid = fcmh_2_fid(f);
	sjir->sjir_nrepls = fcmh_2_nrepls(f);
	sjir->sjir_replpol = fcmh_2_replpol(f);
	memcpy(sjir->sjir_repls, ih->inoh_ino.ino_repls,
	    sizeof(ih->inoh_ino.ino_repls));
	if (fcmh_2_nrepls(f) > SL_DEF_REPLICAS)
		memcpy(&sjir->sjir_repls[SL_DEF_REPLICAS],
		    ih->inoh_extras->inox_repls,
		    sizeof(ih->inoh_extras->inox_repls));
	FCMH_URLOCK(f, locked);

	DEBUG_FCMH(PLL_DEBUG, f, "filled ino_repls journal log");
}

void
mdslog_ino_repls(void *datap, uint64_t txg, __unusedx int flag)
{
	struct slmds_jent_ino_repls *sjir;
	struct fidc_membh *f = datap;

	sjir = pjournal_get_buf(slm_journal, sizeof(*sjir));
	mdslogfill_ino_repls(f, sjir);
	pjournal_add_entry(slm_journal, txg, MDS_LOG_INO_REPLS, 0, sjir,
	    sizeof(*sjir));
	pjournal_put_buf(slm_journal, sjir);
}

void
mdslogfill_bmap_repls(struct bmapc_memb *b,
    struct slmds_jent_bmap_repls *sjbr)
{
	struct fidc_membh *f = b->bcm_fcmh;
	struct bmap_mds_info *bmi = bmap_2_bmi(b);

	BMAP_LOCK_ENSURE(b);

	sjbr->sjbr_fid = fcmh_2_fid(f);
	sjbr->sjbr_bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &sjbr->sjbr_bgen);

	sjbr->sjbr_nrepls = fcmh_2_nrepls(f);
	sjbr->sjbr_replpol = fcmh_2_replpol(f);

	memcpy(sjbr->sjbr_repls, bmi->bmi_repls, SL_REPLICA_NBYTES);

	DEBUG_BMAPOD(PLL_DEBUG, b, "filled bmap_repls journal log entry");
}

/*
 * Perform the work scheduled by mdslog_bmap_repls().
 */
int
slm_wkcb_wr_brepl(void *p)
{
	struct slm_wkdata_wr_brepl *wk = p;

	slm_repl_upd_write(wk->b, 1);
	return (0);
}

/*
 * Write a recently modified replication table to the journal.
 * Note: bmap must be locked to prevent further changes from sneaking in
 * before the repl table is committed to the journal.
 */
void
mdslog_bmap_repls(void *datap, uint64_t txg, __unusedx int flag)
{
	struct slmds_jent_bmap_repls *sjbr;
	struct bmap *b = datap;

	sjbr = pjournal_get_buf(slm_journal, sizeof(*sjbr));
	mdslogfill_bmap_repls(b, sjbr);
	pjournal_add_entry(slm_journal, txg, MDS_LOG_BMAP_REPLS, 0,
	    sjbr, sizeof(*sjbr));
	pjournal_put_buf(slm_journal, sjbr);
}

void
mds_journal_init(uint64_t fsuuid)
{
	void *handle, *reclaimbuf;
	char *journalfn, fn[PATH_MAX];
	int i, ri, rc, max, nios, count, stale, total, idx, npeers;
	uint64_t last_update_xid = 0, last_distill_xid = 0;
	uint64_t lwm, batchno, last_reclaim_xid = 0;
	struct reclaim_prog_entry *rbase, *rp;
	struct update_prog_entry *ubase, *up;
	struct resprof_mds_info *rpmi;
	struct srt_reclaim_entry *r;
	struct srt_update_entry *u;
	struct sl_mds_peerinfo *sp;
	struct sl_mds_iosinfo *si;
	struct sl_resource *res;
	struct sl_resm *resm;
	struct srt_stat	sstb;
	struct sl_site *s;
	size_t size;

	OPSTAT_INCR("reclaim-cursor");

	psc_assert(_MDS_LOG_LAST_TYPE <= (1 << 15));
	psc_assert(U_ENTSZ == 512);

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	CONF_FOREACH_RES(s, res, ri)
		if (RES_ISFS(res))
			nios++;
	if (!nios)
		psc_fatalx("missing I/O servers at site %s",
		    nodeSite->site_name);

	if (nios > MAX_RECLAIM_PROG_ENTRY)
		psc_fatalx("number of I/O servers (%d) exceeds %d",
		    nios, MAX_RECLAIM_PROG_ENTRY);

	/* Count the number of peer MDSes we have */
	npeers = 0;
	SL_MDS_WALK(resm, npeers++);
	npeers--;
	if (npeers > MAX_UPDATE_PROG_ENTRY)
		psc_fatalx("number of metadata servers (%d) "
		    "exceeds %d", npeers,
		    MAX_UPDATE_PROG_ENTRY);

	journalfn = slcfg_local->cfg_journal;
	if (journalfn == '\0') {
		xmkfn(fn, "%s/%s", sl_datadir, SL_FN_OPJOURNAL);
		journalfn = fn;
	}
	psclog_info("Journal device is %s", journalfn);

	slm_journal = pjournal_open("metadata", journalfn);
	if (slm_journal == NULL)
		psc_fatalx("failed to open log file %s",
		    journalfn);

#if 0
	/*
	 * We should specify the uuid in the journal when creating it.
	 * Currently, we allow a random number to be used.
	 */
	if (fsuuid && slm_journal->pj_hdr->pjh_fsuuid != fsuuid)
		psc_fatalx("UUID mismatch FS=%"PRIx64" JRNL=%"PRIx64".  "
		    "The journal needs to be reinitialized.",
		  fsuuid, slm_journal->pj_hdr->pjh_fsuuid);
#endif

	mds_open_cursor();

	/*
	 * We need the cursor thread to start any potential log replay.
	 * Also, without this thread, we can't write anything into ZFS.
	 * We can't even read from ZFS because a read changes atime.
	 */
	pscthr_init(SLMTHRT_CURSOR, slmjcursorthr_main, 0, "slmjcursorthr");

	psc_waitq_init(&nsupd_prg.waitq,"suspd");
	psc_waitq_init(&reclaim_prg.waitq, "reclaim");

	INIT_SPINLOCK(&nsupd_prg.lock);
	INIT_SPINLOCK(&reclaim_prg.lock);

	xmkfn(fn, "%s", SL_FN_RECLAIMPROG);
	rc = mds_open_file(fn, O_RDWR, &reclaim_prg.prg_handle);
	psc_assert(rc == 0);

	/*
	 * Allocate maximum amount of memory to avoid dealing with
	 * the coming and going of IOS as compared with the records
	 * in the progress files.
	 */
	reclaim_prg.prg_buf = PSCALLOC(MAX_RECLAIM_PROG_ENTRY * RP_ENTSZ);
	rc = mds_read_file(reclaim_prg.prg_handle, reclaim_prg.prg_buf,
	    MAX_RECLAIM_PROG_ENTRY * RP_ENTSZ, &size, 0);
	psc_assert(rc == 0);

	/* Find out the highest reclaim batchno and xid */

	rbase = reclaim_prg.prg_buf;

	stale = 0;
	batchno = 0;
	count = idx = size / RP_ENTSZ;
	for (i = 0; i < count; i++) {
		rp = &rbase[i];

		res = libsl_id2res(rp->rpe_id);
		if (res == NULL || !RES_ISFS(res)) {
			psclog_warnx("Bad or non-FS resource ID %u "
			    "found in reclaim progress file",
			    rp->rpe_id);

			stale++;
			rp->rpe_xid = 0;
			rp->rpe_batchno = 0;

			continue;
		}

		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;
		RPMI_LOCK(rpmi);
		si->si_xid = rp->rpe_xid;
		si->si_batchno = rp->rpe_batchno;
		si->si_flags &= ~SIF_NEED_JRNL_INIT;
		si->si_index = i;
		if (si->si_batchno > batchno)
			batchno = si->si_batchno;
		si->si_batchmeter.pm_maxp = &reclaim_prg.cur_batchno;
		RPMI_ULOCK(rpmi);
	}
	if (stale) {
		rc = mds_write_file(reclaim_prg.prg_handle, rbase, size,
		    &size, 0);
		psc_assert(rc == 0);
		psc_assert(size == count * RP_ENTSZ);
		psclog_warnx("%d stale entry(s) have been zeroed from the "
		    "reclaim progress file", stale);
	}

	rc = ENOENT;
	lwm = batchno;
	while (batchno < UINT64_MAX) {
		rc = mds_open_logfile(batchno, 0, 1, &handle);
		if (rc) {
			psc_assert(rc == ENOENT);
			if (batchno > lwm) {
				batchno--;
				rc = mds_open_logfile(batchno, 0, 1,
				    &handle);
			}
			break;
		}
		mds_release_file(handle);
		batchno++;
	}
	if (rc)
		psc_fatalx("Failed to open reclaim log file, "
		    "batchno=%"PRId64": %s", batchno, sl_strerror(rc));

	rc = mdsio_getattr(current_vfsid, 0, handle, &rootcreds, &sstb);
	psc_assert(rc == 0);

	reclaim_prg.cur_batchno = batchno;
	OPSTAT_INCR("reclaim-batchno");

	if (sstb.sst_size) {
		reclaimbuf = PSCALLOC(sstb.sst_size);

		rc = mds_read_file(handle, reclaimbuf, sstb.sst_size,
		    &size, 0);
		psc_assert(rc == 0 && size == sstb.sst_size);

		max = SLM_RECLAIM_BATCH_NENTS;
		r = reclaimbuf;

		if (r->xid != RECLAIM_MAGIC_VER ||
		    r->fg.fg_gen != RECLAIM_MAGIC_GEN ||
		    r->fg.fg_fid != RECLAIM_MAGIC_FID)
			psc_fatalx("Reclaim log corrupted, batchno=%"PRId64,
			    reclaim_prg.cur_batchno);

		size -= R_ENTSZ;
		max = SLM_RECLAIM_BATCH_NENTS - 1;
		r++;

		psc_assert(size % R_ENTSZ == 0);

		total = size / R_ENTSZ;
		psclog_info("scanning the last reclaim log, batchno=%"PRId64,
		    reclaim_prg.cur_batchno);
		for (count = 0; count < total; count++, r++)
			last_reclaim_xid = r->xid;
		if (total > max) {
			psclog_warnx("the last reclaim log has %d "
			    "entry(s) - more than it should have!",
			    total);
			total = max;
		}
		if (total == max)
			reclaim_prg.cur_batchno++;
		PSCFREE(reclaimbuf);
	}

	reclaim_prg.cur_xid = last_reclaim_xid;
	OPSTAT_INCR("reclaim-xid");

	last_distill_xid = last_reclaim_xid;

	mds_release_file(handle);

	/* search for newly-added I/O servers */
	CONF_FOREACH_RES(s, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		si = rpmi->rpmi_info;

		if (si->si_xid == 0 && si->si_batchno == 0) {
			si->si_xid = reclaim_prg.cur_xid;
			si->si_batchno = reclaim_prg.cur_batchno;
			psclog_info("Fast forward batchno/xid for "
			    "resource ID %u", res->res_id);
		}
		if (!(si->si_flags & SIF_NEED_JRNL_INIT))
			continue;

		RPMI_LOCK(rpmi);
		si->si_index = idx++;
		si->si_flags &= ~SIF_NEED_JRNL_INIT;
		si->si_flags |= SIF_NEW_PROG_ENTRY;
		si->si_batchmeter.pm_maxp = &reclaim_prg.cur_batchno;
		RPMI_ULOCK(rpmi);
	}

	psclog_info("reclaim_prg.cur_batchno=%"PRId64" "
	    "reclaim_prg.cur_xid=%"PRId64,
	    reclaim_prg.cur_batchno, reclaim_prg.cur_xid);

	/* We are done if we don't have any peer MDSes */
	if (!npeers)
		goto replay_log;

	xmkfn(fn, "%s", SL_FN_UPDATEPROG);
	rc = mds_open_file(fn, O_RDWR, &nsupd_prg.prg_handle);
	psc_assert(rc == 0);

	ubase = nsupd_prg.prg_buf = PSCALLOC(MAX_UPDATE_PROG_ENTRY *
	    UP_ENTSZ);
	rc = mds_read_file(nsupd_prg.prg_handle, ubase,
	    MAX_UPDATE_PROG_ENTRY * UP_ENTSZ, &size, 0);
	psc_assert(rc == 0);

	/* Find out the highest update batchno and xid */
	batchno = UINT64_MAX;
	count = size / UP_ENTSZ;
	for (i = 0; i < count; i++) {
		up = &ubase[i];
		res = libsl_id2res(up->upe_id);
		if (res->res_type != SLREST_MDS) {
			psclog_warnx("non-MDS resource ID %u "
			    "in update file", res->res_id);
			continue;
		}
		sp = res2rpmi(res)->rpmi_info;
		sp->sp_flags &= ~SPF_NEED_JRNL_INIT;
		sp->sp_xid = up->upe_xid;
		sp->sp_batchno = up->upe_batchno;
		if (sp->sp_batchno < batchno)
			batchno = sp->sp_batchno;
		sp->sp_batchmeter.pm_maxp = &nsupd_prg.cur_batchno;
	}
	nsupd_prg.prg_buf = psc_realloc(nsupd_prg.prg_buf,
	    npeers * UP_ENTSZ, 0);

	if (batchno == UINT64_MAX)
		batchno = 0;

	rc = mds_open_logfile(batchno, 1, 1, &handle);
	if (rc && batchno) {
		batchno--;
		rc = mds_open_logfile(batchno, 1, 1, &handle);
	}
	if (rc)
		psc_fatalx("Failed to open update log file, "
		    "batchno=%"PRId64": %s", batchno, sl_strerror(rc));

	nsupd_prg.cur_batchno = batchno;
	nsupd_prg.log_buf = PSCALLOC(SLM_UPDATE_BATCH_NENTS * U_ENTSZ);

	rc = mds_read_file(handle, nsupd_prg.log_buf,
	    SLM_UPDATE_BATCH_NENTS * U_ENTSZ, &size, 0);
	mds_release_file(handle);

	psc_assert(rc == 0);
	psc_assert(size % U_ENTSZ == 0);

	total = size / U_ENTSZ;
	u = nsupd_prg.log_buf;
	for (count = 0; count < total; u++, count++)
		last_update_xid = u->xid;
	nsupd_prg.cur_xid = last_update_xid;
	if (total == SLM_UPDATE_BATCH_NENTS)
		nsupd_prg.cur_batchno++;

	if (last_distill_xid < last_update_xid)
		last_distill_xid = last_update_xid;

	/* search for newly-added metadata servers */
	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		sp = rpmi->rpmi_info;
		if (!(sp->sp_flags & SPF_NEED_JRNL_INIT))
			continue;

		sp->sp_xid = nsupd_prg.cur_xid;
		sp->sp_batchno = nsupd_prg.cur_batchno;
		sp->sp_flags &= ~SPF_NEED_JRNL_INIT;
		sp->sp_batchmeter.pm_maxp = &nsupd_prg.cur_batchno;
	);

	psclog_info("nsupd_prg.cur_batchno = %"PRId64", "
	    "nsupd_prg.cur_xid = %"PRId64,
	    nsupd_prg.cur_batchno, nsupd_prg.cur_xid);

 replay_log:
	slm_journal->pj_npeers = npeers;
	slm_journal->pj_distill_xid = last_distill_xid;
	slm_journal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	slm_journal->pj_replay_xid = mds_cursor.pjc_replay_xid;

	psclog_info("Last SLASH2 FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psclog_info("Last synced ZFS transaction group number is %"PRId64,
	    slm_journal->pj_commit_txg);
	psclog_info("Last replayed SLASH2 transaction ID is %"PRId64,
	    slm_journal->pj_replay_xid);

	pjournal_replay(slm_journal, SLMTHRT_JRNL, "slmjthr",
	    mds_replay_handler, mds_distill_handler);

	psclog_info("Last used SLASH2 transaction ID is %"PRId64,
	   slm_journal->pj_lastxid);

	psclog_info("The next FID will be %"PRId64, slm_get_curr_slashfid());

	psclogs_info(SLMSS_INFO, "bmap sequence number LWM after replay is %"PRId64,
	    slm_bmap_leases.btt_minseq);
	psclogs_info(SLMSS_INFO, "bmap sequence number HWM after replay is %"PRId64,
	    slm_bmap_leases.btt_maxseq);

	psclog_info("Journal UUID=%"PRIx64" MDS UUID=%"PRIx64,
	    slm_journal->pj_hdr->pjh_fsuuid, fsuuid);

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, slmjreclaimthr_main, 0, "slmjreclaimthr");
	if (!npeers)
		return;

#if 0
	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	pscthr_init(SLMTHRT_JNAMESPACE, slmjnsthr_main, 0, "slmjnsthr");
#endif
}

void
mds_reserve_slot(int count)
{
	int nwaits;

	nwaits = pjournal_reserve_slot(slm_journal, count);
	while (nwaits > 0) {
		nwaits--;
		OPSTAT_INCR("journal-wait");
	}
}

void
mds_unreserve_slot(int count)
{
	pjournal_unreserve_slot(slm_journal, count);
}

int
mds_update_boot_file(void)
{
	int rc = 0;
	void *h;
	int32_t boot;
	uint64_t size;
	char *fn = "boot.log";

	rc = mds_open_file(fn, O_RDWR|O_CREAT, &h);
	if (rc)
		return (rc);
	rc = mds_read_file(h, &boot, sizeof(boot), &size, 0);
	if (rc) {
		mds_release_file(h);
		return (rc);
	}
	if (size)
		boot++;
	else {
		psclogs_info(SLMSS_INFO, "File boot.log has been created successfully.");
		boot = 1;
	}
	rc = mds_write_file(h, &boot, sizeof(boot), &size, 0);
	mds_release_file(h);
	return (rc);
}
