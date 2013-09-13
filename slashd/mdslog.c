/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2008-2013, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/param.h>

#include <inttypes.h>
#include <string.h>

#include "pfl/dynarray.h"
#include "pfl/fcntl.h"
#include "pfl/fs.h"
#include "pfl/rpc.h"
#include "pfl/rsx.h"
#include "psc_util/crc.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/hostname.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

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
#include "worker.h"

#include "zfs-fuse/zfs_slashlib.h"

#define SLM_CBARG_SLOT_CSVC	0
#define SLM_CBARG_SLOT_RESPROF	1

struct psc_journal		*mdsJournal;

static psc_spinlock_t		 mds_distill_lock = SPINLOCK_INIT;

uint64_t			 current_update_batchno;
uint64_t			 current_reclaim_batchno;

uint64_t			 current_update_xid;
uint64_t			 current_reclaim_xid;

uint64_t			 sync_update_xid;
uint64_t			 sync_reclaim_xid;

static void			*update_logfile_handle;
static void			*reclaim_logfile_handle;

static off_t			 update_logfile_offset;
static off_t			 reclaim_logfile_offset;

static void			*update_progfile_handle;
static void			*reclaim_progfile_handle;

#define	MAX_UPDATE_PROG_ENTRY	1024

/* namespace update progress tracker to peer MDSes */
struct update_prog_entry {
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
	sl_ios_id_t		 res_id;
	int32_t			 _pad;
};

#define	MAX_RECLAIM_PROG_ENTRY	1024

/* garbage reclaim progress tracker to IOSes */
struct reclaim_prog_entry {
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
	sl_ios_id_t		 res_id;
	int32_t			 _pad;
};

struct update_prog_entry	*update_prog_buf;
struct reclaim_prog_entry	*reclaim_prog_buf;

struct psc_waitq		 mds_update_waitq = PSC_WAITQ_INIT;
psc_spinlock_t			 mds_update_waitqlock = SPINLOCK_INIT;

struct psc_waitq		 mds_reclaim_waitq = PSC_WAITQ_INIT;
psc_spinlock_t			 mds_reclaim_waitqlock = SPINLOCK_INIT;

/* max # of buffers used to decrease I/O in namespace updates */
#define	SL_UPDATE_MAX_BUF	 8

/* we only have a few buffers (SL_UPDATE_MAX_BUF), so a list is fine */
__static PSCLIST_HEAD(mds_update_buflist);

/* max # of buffers used to decrease I/O in garbage collection */
#define	SL_RECLAIM_MAX_BUF	 4

/* we only have a few buffers (SL_RECLAIM_MAX_BUF), so a list is fine */
__static PSCLIST_HEAD(mds_reclaim_buflist);

/* max # of seconds before an update is propagated */
#define SL_UPDATE_MAX_AGE	 30

/* max # of seconds before a reclaim is propagated */
#define SL_RECLAIM_MAX_AGE	 30

/* a buffer used to read on-disk update log file */

/* this buffer is used for read for RPC and read during distill, need
 * synchronize or use different buffers */
static void			*updatebuf;

static void			*mds_cursor_handle;
struct psc_journal_cursor	 mds_cursor;

psc_spinlock_t			 mds_txg_lock = SPINLOCK_INIT;

struct psc_waitq		 cursorWaitq = PSC_WAITQ_INIT;
psc_spinlock_t			 cursorWaitLock = SPINLOCK_INIT;

static int			 cursor_update_inprog = 0;
static int			 cursor_update_needed = 0;

static int
mds_open_file(char *fn, int flags, void **handle)
{
	mdsio_fid_t mf;
	int rc;

	mds_note_update(1);
	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], fn, &mf, &rootcreds, NULL);
	if (rc == ENOENT && (flags & O_CREAT)) {
		rc = mdsio_opencreatef(current_vfsid,
		    mds_metadir_inum[current_vfsid], &rootcreds, flags,
		    MDSIO_OPENCRF_NOLINK, 0600, fn, NULL, NULL, handle,
		    NULL, NULL, 0);
	} else if (!rc) {
		rc = mdsio_opencreate(current_vfsid, mf, &rootcreds,
		    flags, 0, NULL, NULL, NULL, handle, NULL, NULL, 0);
	}
	mds_note_update(-1);
	return (rc);
}

static inline int
mds_read_file(void *h, void *buf, uint64_t size, size_t *nb, off_t off)
{
	int rc;
	mds_note_update(1);
	rc = mdsio_read(current_vfsid, &rootcreds, buf, size, nb, off, h);
	mds_note_update(-1);
	return rc;
}

static inline int
mds_write_file(void *h, void *buf, uint64_t size, size_t *nb, off_t off)
{
	int rc;
	mds_note_update(1);
	rc = mdsio_write(current_vfsid, &rootcreds, buf, size, nb, off, 0, h, NULL, NULL);
	mds_note_update(-1);
	return rc;
}

static inline int
mds_release_file(void *handle)
{
	int rc;
	mds_note_update(1);
	rc = mdsio_release(current_vfsid, &rootcreds, handle);
	mds_note_update(-1);
	return rc;
}

static void
mds_record_update_prog(void)
{
	struct sl_mds_peerinfo *peerinfo;
	struct sl_resm *resm;
	size_t size;
	int i, rc;

	i = 0;
	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		peerinfo = res2rpmi(resm->resm_res)->rpmi_info;
		update_prog_buf[i].res_id = resm->resm_res_id;
		update_prog_buf[i].res_xid = peerinfo->sp_xid;
		update_prog_buf[i].res_batchno = peerinfo->sp_batchno;
		i++;
	);
	rc = mds_write_file(update_progfile_handle, update_prog_buf,
	    i * sizeof(struct update_prog_entry), &size, 0);
	psc_assert(rc == 0);
	psc_assert(size == (size_t)i * sizeof(struct update_prog_entry));
}

static void
mds_record_reclaim_prog(void)
{
	struct sl_mds_iosinfo *iosinfo;
	struct sl_resource *res;
	int ri, rc, index, lastindex = 0;
	size_t size;

	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		iosinfo = res2rpmi(res)->rpmi_info;

		index = iosinfo->si_index;
		if (iosinfo->si_flags & SIF_NEW_PROG_ENTRY) {
			iosinfo->si_flags &= ~SIF_NEW_PROG_ENTRY;
			reclaim_prog_buf[index].res_id = res->res_id;
		}
		psc_assert(reclaim_prog_buf[index].res_id == res->res_id);

		reclaim_prog_buf[index].res_xid = iosinfo->si_xid;
		reclaim_prog_buf[index].res_batchno = iosinfo->si_batchno;

		if (lastindex < index)
			lastindex = index;
	}
	lastindex++;
	rc = mds_write_file(reclaim_progfile_handle, reclaim_prog_buf,
	    lastindex * sizeof(struct reclaim_prog_entry), &size, 0);
	psc_assert(rc == 0);
	psc_assert(size == (size_t)lastindex * sizeof(struct reclaim_prog_entry));
}

/**
 * mds_txg_handler - Tie system journal with ZFS transaction groups.
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

	mds_note_update(1);
	rc = mdsio_unlink(current_vfsid,
	    mds_metadir_inum[current_vfsid], NULL, logfn, &rootcreds,
	    NULL, NULL);
	mds_note_update(-1);

	if (rc && rc != ENOENT)
		psc_fatalx("Failed to remove log file %s: %s", logfn,
		    slstrerror(rc));
	if (!rc) {
		psclog_warnx("Log file %s has been removed", logfn);
		OPSTAT_INCR(SLM_OPST_LOGFILE_REMOVE);
	}
	return rc;
}
void
mds_remove_logfiles(uint64_t batchno, int update)
{
	int64_t i;
	int rc, notfound = 0;
	struct timeval tv1, tv2;

	gettimeofday(&tv1, NULL);
	for (i = (int64_t) batchno - 2; i >= 0; i--) {
		rc = mds_remove_logfile(i, update, 1);
		if (rc)
			notfound++;
		if (notfound > 10)
			break;
	}
	gettimeofday(&tv2, NULL);
	psclog_info("%"PRId64" log file(s) have been removed in %d "
	    "second(s), LWM is %"PRId64,
	    OPSTAT_CURR(SLM_OPST_LOGFILE_REMOVE),
	    (int)(tv2.tv_sec - tv1.tv_sec), batchno);
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
		/*
		 * The caller should check the return value.
		 */
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
		    slstrerror(rc));
	OPSTAT_INCR(SLM_OPST_LOGFILE_CREATE);
	return (rc);
}

/**
 * mds_distill_handler - Distill information from the system journal and
 *	write into namespace update or garbage reclaim logs.
 *
 *	Writing the information to secondary logs allows us to recycle
 *	the space in the main system log as quickly as possible.  The
 *	distill process is continuous in order to make room for system
 *	logs.  Once in a secondary log, we can process them as we see
 *	fit.  Sometimes these secondary log files can hang over a long
 *	time because a peer MDS or an IO server is down or slow.
 *
 *	We encode the cursor creation time and hostname into the log
 *	file names to minimize collisions.  If undetected, these
 *	collisions can lead to insidious bugs, especially when on-disk
 *	format changes.
 */
int
mds_distill_handler(struct psc_journal_enthdr *pje,
    __unusedx uint64_t xid, int npeers, int action)
{
	struct srt_reclaim_entry reclaim_entry, *reclaim_entryp;
	struct srt_update_entry update_entry, *update_entryp;
	struct slmds_jent_namespace *sjnm = NULL;
	struct slmds_jent_bmap_crc *sjbc = NULL;
	struct srt_stat sstb;
	void *reclaimbuf = NULL;
	int rc, count, total;
	uint16_t type;
	size_t size;

	psc_assert(pje->pje_magic == PJE_MAGIC);

	/*
	 * The following can only be executed by the singleton distill
	 * thread.
	 */
	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	if (type == MDS_LOG_BMAP_CRC) {
		sjbc = PJE_DATA(pje);
		goto check_update;
	}

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

	if (reclaim_logfile_handle == NULL) {

		rc = mds_open_logfile(current_reclaim_batchno, 0, 0,
		    &reclaim_logfile_handle);
		psc_assert(rc == 0);

		mds_note_update(1);
		rc = mdsio_getattr(current_vfsid, 0,
		    reclaim_logfile_handle, &rootcreds, &sstb);
		mds_note_update(-1);

		psc_assert(rc == 0);

		reclaim_logfile_offset = 0;
		/*
		 * Even if there is no need to replay after a startup,
		 * we should still skip existing entries.
		 */
		if (sstb.sst_size) {
			reclaimbuf = PSCALLOC(sstb.sst_size);
			rc = mds_read_file(reclaim_logfile_handle,
			    reclaimbuf, sstb.sst_size, &size, 0);
			if (rc || size != sstb.sst_size)
				psc_fatalx("Failed to read reclaim log "
				    "file, batchno=%"PRId64": %s",
				    current_reclaim_batchno,
				    slstrerror(rc));

			reclaim_entryp = (struct srt_reclaim_entry *)reclaimbuf;
			if (reclaim_entryp->xid != RECLAIM_MAGIC_VER ||
			    reclaim_entryp->fg.fg_gen != RECLAIM_MAGIC_GEN ||
			    reclaim_entryp->fg.fg_fid != RECLAIM_MAGIC_FID)
				psc_fatalx("Reclaim log corrupted, batchno=%"PRId64,
				    current_reclaim_batchno);

			size -= sizeof(struct srt_reclaim_entry);
			reclaim_entryp = PSC_AGP(reclaim_entryp,
			    sizeof(struct srt_reclaim_entry));
			reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);

			/* this should never happen, but we have seen bitten */
			if (size > sizeof(struct srt_reclaim_entry) * SLM_RECLAIM_BATCH ||
			    ((size % sizeof(struct srt_reclaim_entry)) != 0)) {
				psclog_warnx("Reclaim log corrupted! "
				    "batch=%"PRId64" size=%zd",
				    current_reclaim_batchno, size);
				size = sizeof(struct srt_reclaim_entry) *
				    (SLM_RECLAIM_BATCH - 1);
			}
			count = 0;
			total = size / sizeof(struct srt_reclaim_entry);
			while (count < total) {
				if (reclaim_entryp->xid == pje->pje_xid) {
					psclog_warnx("Reclaim xid %"PRId64" "
					    "already in use! batch = %"PRId64,
					    pje->pje_xid, current_reclaim_batchno);
				}
				reclaim_entryp = PSC_AGP(reclaim_entryp,
				    sizeof(struct srt_reclaim_entry));
				count++;
				reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);
			}
			PSCFREE(reclaimbuf);
			reclaimbuf = NULL;
			psc_assert(reclaim_logfile_offset == (off_t)sstb.sst_size);
		} else {
			reclaim_entry.xid = RECLAIM_MAGIC_VER;
			reclaim_entry.fg.fg_fid = RECLAIM_MAGIC_FID;
			reclaim_entry.fg.fg_gen = RECLAIM_MAGIC_GEN;

			rc = mds_write_file(reclaim_logfile_handle,
			    &reclaim_entry, sizeof(struct srt_reclaim_entry),
			    &size, reclaim_logfile_offset);
			if (rc || size != sizeof(struct srt_reclaim_entry))
				psc_fatal("Failed to write reclaim log file, batchno=%"PRId64,
				    current_reclaim_batchno);
			reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);
		}
	}

	reclaim_entry.xid = pje->pje_xid;
	reclaim_entry.fg.fg_fid = sjnm->sjnm_target_fid;
	reclaim_entry.fg.fg_gen = sjnm->sjnm_target_gen;

	rc = mds_write_file(reclaim_logfile_handle, &reclaim_entry,
	    sizeof(struct srt_reclaim_entry), &size, reclaim_logfile_offset);
	if (size != sizeof(struct srt_reclaim_entry))
		psc_fatal("Failed to write reclaim log file, batchno=%"PRId64,
		    current_reclaim_batchno);

	spinlock(&mds_distill_lock);
	current_reclaim_xid = pje->pje_xid;
	OPSTAT_ASSIGN(SLM_OPST_RECLAIM_XID, current_reclaim_xid);
	freelock(&mds_distill_lock);

	reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);
	if (reclaim_logfile_offset ==
	    SLM_RECLAIM_BATCH * (off_t)sizeof(struct srt_reclaim_entry)) {

		mds_release_file(reclaim_logfile_handle);

		reclaim_logfile_handle = NULL;

		current_reclaim_batchno++;
		OPSTAT_INCR(SLM_OPST_RECLAIM_BATCHNO);

		spinlock(&mds_distill_lock);
		sync_reclaim_xid = pje->pje_xid;
		freelock(&mds_distill_lock);

		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_wakeall(&mds_reclaim_waitq);
		freelock(&mds_reclaim_waitqlock);
		psclog_info("Next batchno=%"PRId64", "
		    "current reclaim XID=%"PRId64,
		    current_reclaim_batchno, current_reclaim_xid);
	}
	psc_assert(reclaim_logfile_offset <=
	    SLM_RECLAIM_BATCH * (off_t)sizeof(struct srt_reclaim_entry));

	psclog_diag("current_reclaim_xid=%"PRIu64" batchno=%"PRIu64" "
	    "fg="SLPRI_FG,
	    current_reclaim_xid, current_reclaim_batchno,
	    SLPRI_FG_ARGS(&reclaim_entry.fg));

 check_update:

	if (!npeers)
		return (0);

	if (update_logfile_handle == NULL) {

		update_logfile_offset = 0;
		mds_open_logfile(current_update_batchno, 1, 0,
		    &update_logfile_handle);

		if (action == 1) {
			rc = mds_read_file(update_logfile_handle,
			    updatebuf, SLM_UPDATE_BATCH *
			    sizeof(struct srt_update_entry), &size, 0);
			if (rc)
				psc_fatalx("Failed to read update log "
				    "file, batchno=%"PRId64": %s",
				    current_update_batchno,
				    slstrerror(rc));
			total = size / sizeof(struct srt_update_entry);

			count = 0;
			update_entryp = updatebuf;
			while (count < total) {
				if (update_entryp->xid == pje->pje_xid)
					break;
				update_entryp++;
				count++;
				update_logfile_offset +=
				    sizeof(struct srt_update_entry);
			}
		}
	}

	memset(&update_entry, 0, sizeof(update_entry));
	update_entry.xid = pje->pje_xid;

	/*
	 * Fabricate a setattr update entry to change the size.
	 */
	if (type == MDS_LOG_BMAP_CRC) {
		update_entry.op = NS_OP_SETSIZE;
		update_entry.mask = mdsio_slflags_2_setattrmask(
		    PSCFS_SETATTRF_DATASIZE);
		update_entry.size = sjbc->sjbc_fsize;
		update_entry.target_fid = sjbc->sjbc_fid;
		goto write_update;
	}

	update_entry.op = sjnm->sjnm_op;
	update_entry.target_gen = sjnm->sjnm_target_gen;
	update_entry.parent_fid = sjnm->sjnm_parent_fid;
	update_entry.target_fid = sjnm->sjnm_target_fid;
	update_entry.new_parent_fid = sjnm->sjnm_new_parent_fid;

	update_entry.mode = sjnm->sjnm_mode;
	update_entry.mask = sjnm->sjnm_mask;
	update_entry.uid = sjnm->sjnm_uid;
	update_entry.gid = sjnm->sjnm_gid;

	update_entry.size = sjnm->sjnm_size;

	update_entry.atime = sjnm->sjnm_atime;
	update_entry.mtime = sjnm->sjnm_mtime;
	update_entry.ctime = sjnm->sjnm_ctime;
	update_entry.atime_ns = sjnm->sjnm_atime_ns;
	update_entry.mtime_ns = sjnm->sjnm_mtime_ns;
	update_entry.ctime_ns = sjnm->sjnm_ctime_ns;

	update_entry.namelen = sjnm->sjnm_namelen;
	update_entry.namelen2 = sjnm->sjnm_namelen2;
	memcpy(update_entry.name, sjnm->sjnm_name,
	    sjnm->sjnm_namelen + sjnm->sjnm_namelen2);

 write_update:

	rc = mds_write_file(update_logfile_handle, &update_entry,
	    sizeof(struct srt_update_entry), &size,
	    update_logfile_offset);
	if (size != sizeof(struct srt_update_entry))
		psc_fatal("failed to write update log file, "
		    "batchno=%"PRId64" rc=%d",
		    current_update_batchno, rc);

	/* see if we need to close the current update log file */
	update_logfile_offset += sizeof(struct srt_update_entry);
	if (update_logfile_offset ==
	    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry)) {

		mds_release_file(update_logfile_handle);

		update_logfile_handle = NULL;
		current_update_batchno++;

		spinlock(&mds_distill_lock);
		sync_update_xid = pje->pje_xid;
		freelock(&mds_distill_lock);

		spinlock(&mds_update_waitqlock);
		psc_waitq_wakeall(&mds_update_waitq);
		freelock(&mds_update_waitqlock);
	}

	spinlock(&mds_distill_lock);
	current_update_xid = pje->pje_xid;
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

/**
 * mdslog_namespace - Log a namespace operation before we attempt it.
 *	This makes sure that it will be propagated towards other MDSes
 *	and made permanent before we reply to the client.
 */
void
mdslog_namespace(int op, uint64_t txg, uint64_t pfid,
    uint64_t npfid, const struct srt_stat *sstb, int mask,
    const char *name, const char *newname, void *arg)
{
	struct slmds_jent_namespace *sjnm;
	int chg, distill = 0;
	size_t siz;

	if (op == NS_OP_SETATTR)
		psc_assert(mask);

	if (op == NS_OP_CREATE || op == NS_OP_MKDIR)
		psc_assert(sstb->sst_fid);

	sjnm = pjournal_get_buf(mdsJournal, sizeof(*sjnm));
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
	distill = pjournal_has_peers(mdsJournal);
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

	pjournal_add_entry(mdsJournal, txg,
	    MDS_LOG_NAMESPACE, distill, sjnm,
	    offsetof(struct slmds_jent_namespace, sjnm_name) +
	    sjnm->sjnm_namelen + sjnm->sjnm_namelen2);

	if (!distill)
		pjournal_put_buf(mdsJournal, sjnm);

	psclog_info("namespace op %s (%d): distill=%d "
	    "fid="SLPRI_FID" name='%s%s%s' mask=%#x size=%"PRId64" "
	    "link=%"PRId64" pfid="SLPRI_FID" npfid="SLPRI_FID" txg=%"PRId64,
	    slm_ns_opnames[op], op, distill, sstb->sst_fid, name,
	    newname ? "' newname='" : "", newname ? newname : "",
	    mask, sstb->sst_size, sstb->sst_nlink, pfid, npfid, txg);

	switch (op) {
	case NS_OP_UNLINK:
		if (sstb->sst_nlink > 1)
			COPYFG((struct slash_fidgen *)arg, &sstb->sst_fg);
		break;
	case NS_OP_RENAME:
		COPYFG((struct slash_fidgen *)arg, &sstb->sst_fg);
		break;
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
	    ((op == NS_OP_RECLAIM) ||
	     (op == NS_OP_UNLINK && sstb->sst_nlink == 1) ||
	     (op == NS_OP_SETSIZE && sstb->sst_size == 0)))
		psclogs(PLL_INFO, SLMSS_INFO,
		    "file data %s fid="SLPRI_FID" "
		    "uid=%u gid=%u "
		    "fsize=%zu op=%d",
		    chg ? "changed" : "removed",
		    sstb->sst_fid,
		    sstb->sst_uid, sstb->sst_gid,
		    siz, op);

	if (((op == NS_OP_RECLAIM) ||
	     (op == NS_OP_UNLINK && sstb->sst_nlink == 1))) {
		struct slm_wkdata_upsch_purge *wk;

		wk = pfl_workq_getitem(slm_wk_upsch_purge,
		    struct slm_wkdata_upsch_purge);
		wk->fid = sstb->sst_fid;
		pfl_workq_putitem(wk);
	}
}

/**
 * mds_reclaim_lwm - Find the lowest garbage reclamation water mark of
 *	all IOSes.
 */
__static uint64_t
mds_reclaim_lwm(int batchno)
{
	uint64_t value = UINT64_MAX;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;
	int ri;

	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		/*
		 * Prevents reading old log file repeatedly only to
		 * find out that an IOS is down.
		 */
		if (iosinfo->si_flags & SIF_DISABLE_GC) {
			RPMI_ULOCK(rpmi);
			continue;
		}
		if (batchno) {
			if (iosinfo->si_batchno < value)
				value = iosinfo->si_batchno;
		} else {
			if (iosinfo->si_xid < value)
				value = iosinfo->si_xid;
		}
		RPMI_ULOCK(rpmi);
	}
	psc_assert(value != UINT64_MAX);
	return (value);
}

__static uint64_t
mds_reclaim_hwm(int batchno)
{
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;
	uint64_t value = 0;
	int ri;

	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (iosinfo->si_batchno > value)
				value = iosinfo->si_batchno;
		} else {
			if (iosinfo->si_xid > value)
				value = iosinfo->si_xid;
		}
		RPMI_ULOCK(rpmi);
	}
	return (value);
}

/**
 * mds_update_lwm - Find the lowest namespace update water mark of all
 *	peer MDSes.
 */
__static uint64_t
mds_update_lwm(int batchno)
{
	uint64_t value = UINT64_MAX;
	struct sl_mds_peerinfo *peerinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;

	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (peerinfo->sp_batchno < value)
				value = peerinfo->sp_batchno;
		} else {
			if (peerinfo->sp_xid < value)
				value = peerinfo->sp_xid;
		}
		RPMI_ULOCK(rpmi);
	);
	psc_assert(value != UINT64_MAX);
	return (value);
}

__static uint64_t
mds_update_hwm(int batchno)
{
	struct sl_mds_peerinfo *peerinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;
	uint64_t value = 0;

	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (batchno) {
			if (peerinfo->sp_batchno > value)
				value = peerinfo->sp_batchno;
		} else {
			if (peerinfo->sp_xid > value)
				value = peerinfo->sp_xid;
		}
		RPMI_ULOCK(rpmi);
	);
	return (value);
}

void
mds_skip_reclaim_batch(uint64_t batchno)
{
	int ri, nios = 0, record = 0, didwork = 0;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;

	if (batchno >= current_reclaim_batchno)
		return;

	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		nios++;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;
		if (iosinfo->si_batchno < batchno)
			continue;
		if (iosinfo->si_batchno > batchno) {
			didwork++;
			continue;
		}
		record = 1;
		didwork++;
		iosinfo->si_batchno++;
	}
	if (record)
		mds_record_reclaim_prog();

	psclog_warnx("Skipping reclaim log file, batchno=%"PRId64,
		batchno);
	if (didwork == nios) {
		if (batchno >= 1)
			mds_remove_logfile(batchno - 1, 0, 0);
	}

}

/**
 * mds_send_batch_update - Send a batch of updates to peer MDSes
 *	that want them.
 */
int
mds_send_batch_update(uint64_t batchno)
{
	int siter, i, rc, npeers, count, total, didwork = 0, record = 0;
	struct srt_update_entry *entryp, *next_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct slashrpc_cservice *csvc;
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
			    batchno, slstrerror(rc));
		return (didwork);
	}
	rc = mds_read_file(handle, updatebuf,
	    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry), &size, 0);
	mds_release_file(handle);

	if (size == 0)
		return (didwork);

	psc_assert((size % sizeof(struct srt_update_entry)) == 0);
	count = (int)size / (int)sizeof(struct srt_update_entry);

	/* find the xid associated with the last log entry */
	entryp = PSC_AGP(updatebuf, (count - 1) *
	    sizeof(struct srt_update_entry));
	xid = entryp->xid;

	/*
	 * Trim padding from buffer to reduce RPC traffic.
	 */
	entryp = next_entryp = updatebuf;
	size = UPDATE_ENTRY_LEN(entryp);
	for (i = 1; i < count; i++) {
		entryp++;
		next_entryp = PSC_AGP(next_entryp,
		    UPDATE_ENTRY_LEN(next_entryp));
		memmove(next_entryp, entryp, UPDATE_ENTRY_LEN(entryp));
		size += UPDATE_ENTRY_LEN(entryp);
	}

	npeers = 0;

	CONF_LOCK();
	CONF_FOREACH_SITE(site)
	    SITE_FOREACH_RES(site, res, siter) {
		if (res->res_type != SLREST_MDS)
			continue;
		resm = psc_dynarray_getpos(
		    &res->res_members, 0);
		if (resm == nodeResm)
			continue;
		npeers++;
		peerinfo = resm2rpmi(resm)->rpmi_info;

		/*
		 * A simplistic backoff strategy to avoid CPU spinning.
		 * A better way could be to let the ping thread handle
		 * this.
		 */
		if (peerinfo->sp_fails >= 3) {
			if (peerinfo->sp_skips == 0) {
				peerinfo->sp_skips = 3;
				continue;
			}
			peerinfo->sp_skips--;
			if (peerinfo->sp_skips)
				continue;
		}
		if (peerinfo->sp_batchno < batchno)
			continue;
		/*
		 * Note that the update xid we can see is not
		 * necessarily contiguous.
		 */
		if (peerinfo->sp_batchno > batchno ||
		    peerinfo->sp_xid > xid) {
			didwork++;
			continue;
		}

		/* Find out which part of the buffer should be sent out */
		i = count;
		total = size;
		entryp = updatebuf;
		do {
			if (entryp->xid >= peerinfo->sp_xid)
				break;
			i--;
			total -= UPDATE_ENTRY_LEN(entryp);
			entryp = PSC_AGP(entryp,
			    UPDATE_ENTRY_LEN(entryp));
		} while (total);

		psc_assert(total);

		iov.iov_len = total;
		iov.iov_base = entryp;

		csvc = slm_getmcsvc_wait(resm);
		if (csvc == NULL) {
			peerinfo->sp_fails++;
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
		psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

		rsx_bulkclient(rq, BULK_GET_SOURCE, SRMM_BULK_PORTAL,
		    &iov, 1);

		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;
		else
			peerinfo->sp_fails++;

		pscrpc_req_finished(rq);
		rq = NULL;
		sl_csvc_decref(csvc);

		if (rc == 0) {
			record++;
			didwork++;
			peerinfo->sp_fails = 0;
			peerinfo->sp_xid = xid + 1;
			if (count == SLM_UPDATE_BATCH)
				peerinfo->sp_batchno++;
		}
	}
	CONF_ULOCK();
	/*
	 * Record the progress first before potentially removing an old
	 * log file.
	 */
	if (record)
		mds_record_update_prog();
	if (didwork == npeers && count == SLM_UPDATE_BATCH) {
		if (batchno >= 1)
			mds_remove_logfile(batchno - 1, 1, 0);
	}
	return (didwork);
}

/**
 * mds_update_cursor - Write some system information into our cursor
 *	file.  Note that every field must be protected by a spinlock.
 */
void
mds_update_cursor(void *buf, uint64_t txg, int flag)
{
	static uint64_t start_txg = 0;
	struct psc_journal_cursor *cursor = buf;
	uint64_t hwm, lwm;

	if (flag == 1) {
		start_txg = txg;
		pjournal_update_txg(mdsJournal, txg);
		return;
	}
	psc_assert(start_txg == txg);

	/*
	 * During the replay, actually as soon as ZFS starts, its group
	 * transaction number starts to increase.  If we crash in the
	 * middle of a relay, we can miss replaying some entries if we
	 * update the txg at this point.
	 */
	if ((mdsJournal->pj_flags & PJF_REPLAYINPROG) == 0) {
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
		cursor->pjc_replay_xid = pjournal_next_replay(mdsJournal);
	}

	/*
	 * Be conservative.  We are willing to do extra work than
	 * missing some.
	 */
	spinlock(&mds_distill_lock);
	if (sync_update_xid < sync_reclaim_xid)
		cursor->pjc_distill_xid = sync_update_xid;
	else
		cursor->pjc_distill_xid = sync_reclaim_xid;
	freelock(&mds_distill_lock);

	cursor->pjc_fid = slm_get_curr_slashfid();

	mds_bmap_getcurseq(&hwm, &lwm);
	cursor->pjc_seqno_lwm = lwm;
	cursor->pjc_seqno_hwm = hwm;
}

/**
 * mds_cursor_thread - Update the cursor file in the ZFS that records
 *	the current transaction group number and other system log
 *	status.  If there is no activity in system other that this write
 *	to update the cursor, our customized ZFS will extend the
 *	lifetime of the transaction group.
 */
void
mds_cursor_thread(struct psc_thread *thr)
{
	int rc;

	while (pscthr_run(thr)) {

		spinlock(&cursorWaitLock);
		if (!cursor_update_needed) {
			cursor_update_inprog = 0;
			psc_waitq_wait(&cursorWaitq, &cursorWaitLock);
		} else {
			cursor_update_inprog = 1;
			freelock(&cursorWaitLock);
		}

		/* Use SLASH2_CURSOR_FLAG to write cursor file */
		rc = mdsio_write_cursor(current_vfsid, &mds_cursor,
		    sizeof(mds_cursor), mds_cursor_handle,
		    mds_update_cursor);
		if (rc)
			psclog_warnx("failed to update cursor, rc=%d", rc);
		else
			psclog_diag("cursor updated: txg=%"PRId64", xid=%"PRId64
			    ", fid="SLPRI_FID", seqno=(%"PRIx64", %"PRIx64")",
			    mds_cursor.pjc_commit_txg,
			    mds_cursor.pjc_distill_xid,
			    mds_cursor.pjc_fid,
			    mds_cursor.pjc_seqno_lwm,
			    mds_cursor.pjc_seqno_hwm);
	}
}

void
mds_note_update(int val)
{
	spinlock(&cursorWaitLock);
	cursor_update_needed += val;
	if (!cursor_update_inprog && cursor_update_needed)
		psc_waitq_wakeall(&cursorWaitq);
	freelock(&cursorWaitLock);
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
		    SLASH_FID_SITE_SHFT;
	if (FID_GET_SITEID(mds_cursor.pjc_fid) != nodeSite->site_id)
		psc_fatal("Mismatched site ID in the FID, expected %d",
		    nodeSite->site_id);
#endif
	/* old utility does not set fsid, so we fill it here */
	if (FID_GET_SITEID(mds_cursor.pjc_fid) == 0)
		FID_SET_SITEID(mds_cursor.pjc_fid,
		    zfsMount[current_vfsid].siteid);

#if 0
	/* backward compatibility */
	if (mount_index == 1) {
		psc_assert(current_vfsid == 0);
		zfsMount[current_vfsid].fsid = FID_GET_SITEID(mds_cursor.pjc_fid);
	}
#endif

	slm_set_curr_slashfid(mds_cursor.pjc_fid);

	psclog_info("SLFID prior to replay="SLPRI_FID,
	    mds_cursor.pjc_fid);

	mds_bmap_setcurseq(mds_cursor.pjc_seqno_hwm, mds_cursor.pjc_seqno_lwm);

	psclog_info("Last bmap sequence number LWM prior to replay is %"PRId64,
	    mds_cursor.pjc_seqno_lwm);
	psclog_info("Last bmap sequence number HWM prior to replay is %"PRId64,
	    mds_cursor.pjc_seqno_hwm);

	tm = mds_cursor.pjc_timestamp;
	ctime_r(&tm, tmbuf);
	p = strchr(tmbuf, '\n');
	if (p)
		*p = '\0';
	psclog_info("file system was formatted on %s "
	    "(%"PSCPRI_TIMET")", tmbuf, tm);
}

int
mds_send_batch_reclaim(uint64_t batchno)
{
	int i, ri, rc, len, count, nentry, total, nios, didwork = 0,
	    record = 0;
	struct srt_reclaim_entry *entryp;
	struct slashrpc_cservice *csvc;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct pscrpc_request *rq;
	struct sl_resource *res;
	struct srt_stat sstb;
	struct sl_resm *m;
	struct iovec iov;
	uint64_t xid;
	size_t size;
	void *handle, *reclaimbuf;

	OPSTAT_ASSIGN(SLM_OPST_RECLAIM_CURSOR, batchno);

	rc = mds_open_logfile(batchno, 0, 1, &handle);
	if (rc) {
		/*
		 * It is fine that the distill process hasn't written
		 * the next log file after closing the old one.
		 */
		if (rc != ENOENT)
			psc_fatalx("Failed to open reclaim log file, "
			    "batchno=%"PRId64": %s",
			    batchno, slstrerror(rc));
		/*
		 * However, if the log file is missing for some reason,
		 * we skip it so that we can make progress.
		 */
		if (batchno < current_reclaim_batchno) {
			didwork = 1;
			mds_skip_reclaim_batch(batchno);
		}
		return (didwork);
	}
	rc = mdsio_getattr(current_vfsid, 0, handle, &rootcreds, &sstb);
	psc_assert(rc == 0);

	if (sstb.sst_size == 0) {
		mds_release_file(handle);
		psclog_warnx("Zero size reclaim log file, "
		    "batchno=%"PRId64, batchno);
		return (didwork);
	}
	reclaimbuf = PSCALLOC(sstb.sst_size);

	rc = mds_read_file(handle, reclaimbuf, sstb.sst_size, &size, 0);
	psc_assert(rc == 0 && sstb.sst_size == size);
	mds_release_file(handle);

	entryp = reclaimbuf;

	if (size == sizeof(struct srt_reclaim_entry)) {
		PSCFREE(reclaimbuf);
		psclog_warnx("Empty reclaim log file, batchno=%"PRId64,
		    batchno);
		return (didwork);
	}
	/*
	 * XXX XXX XXX XXX hack
	 * We have seen odd file size (> 600MB) without any clue.
	 * To avoid confusing other code on the MDS and sliod, pretend
	 * we have done the job and move on.
	 */
	if ((size > sizeof(struct srt_reclaim_entry) * SLM_RECLAIM_BATCH) ||
	    ((size % sizeof(struct srt_reclaim_entry)) != 0)) {
		psclog_warnx("Reclaim log corrupted! "
		    "batch=%"PRIx64" size=%zd",
		    batchno, size);
		mds_skip_reclaim_batch(batchno);
		PSCFREE(reclaimbuf);
		return (1);
	}
	count = (int)size / sizeof(struct srt_reclaim_entry);

	/* find the xid associated with the last log entry */
	entryp = PSC_AGP(reclaimbuf,
	    (count - 1) * sizeof(struct srt_reclaim_entry));
	xid = entryp->xid;

	len = sizeof(struct srt_reclaim_entry);
	size -= sizeof(struct srt_reclaim_entry);

	/* XXX use random order */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		nios++;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		/*
		 * We won't need this if the IOS is actually down.
		 * But we need to shortcut it for testing purposes.
		 */
		if (iosinfo->si_flags & SIF_DISABLE_GC) {
			RPMI_ULOCK(rpmi);
			continue;
		}
		if (iosinfo->si_batchno < batchno) {
			RPMI_ULOCK(rpmi);
			continue;
		}

		/*
		 * Note that the reclaim xid we can see is not
		 * necessarily contiguous.
		 *
		 * We only check for xid when the log file is not
		 * full to get around some internally corrupted
		 * log file (xid is not increasing all the way).
		 *
		 * Note that this workaround only applies when
		 * the batchno already matches.
		 */
		if (iosinfo->si_batchno > batchno ||
		    (count < SLM_RECLAIM_BATCH && iosinfo->si_xid > xid)) {
			RPMI_ULOCK(rpmi);
			didwork++;
			continue;
		}

		RPMI_ULOCK(rpmi);

		/* Find out which part of the buffer should be sent out */
		i = count - 1;
		total = size;
		entryp = reclaimbuf;
		entryp = PSC_AGP(entryp, len);

		/*
		 * In a perfect world, iosinfo->si_xid <= xid is always true.
		 * This is because batchno and xid are related.  But I was
		 * met with cold reality and couldn't explain why it happened.
		 * Anyway, resending requests is not the end of the world.
		 */
		if (iosinfo->si_xid <= xid) {
			do {
				if (entryp->xid >= iosinfo->si_xid)
					break;
				i--;
				total -= len;
				entryp = PSC_AGP(entryp, len);
			} while (total);
		} else
			psclog_warnx("batch (%"PRId64") versus xids "
			    "(%"PRId64":%"PRId64")",
			    batchno, iosinfo->si_xid, xid);

		psc_assert(total);

		m = psc_dynarray_getpos(&res->res_members, 0);
		csvc = slm_geticsvcf(m, CSVCF_NONBLOCK |
		    CSVCF_NORECON);
		if (csvc == NULL)
			PFL_GOTOERR(fail, rc = SLERR_ION_OFFLINE);
		rc = SL_RSX_NEWREQ(csvc, SRMT_RECLAIM, rq, mq, mp);
		if (rc) {
			sl_csvc_decref(csvc);
			goto fail;
		}

		nentry = i;
		iov.iov_len = total;
		iov.iov_base = entryp;

		mq->batchno = iosinfo->si_batchno;
		mq->xid = xid;
		mq->size = iov.iov_len;
		mq->count = nentry;
		psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

		rsx_bulkclient(rq, BULK_GET_SOURCE, SRIM_BULK_PORTAL,
		    &iov, 1);

		/* XXX use async */
		rc = SL_RSX_WAITREP(csvc, rq, mp);
		if (rc == 0)
			rc = mp->rc;

		pscrpc_req_finished(rq);
		sl_csvc_decref(csvc);

		if (rc == 0) {
			record++;
			didwork++;
			OPSTAT_INCR(SLM_OPST_RECLAIM_RPC_SEND);
			iosinfo->si_xid = xid + 1;
			if (count == SLM_RECLAIM_BATCH)
				iosinfo->si_batchno++;
			psclog_info("Reclaim RPC success: "
			    "batchno=%"PRId64", dst=%s",
			    batchno, res->res_name);
			continue;
		}

 fail:
		OPSTAT_INCR(SLM_OPST_RECLAIM_RPC_FAIL);
		psclog(rc == SLERR_ION_OFFLINE ? PLL_INFO : PLL_WARN,
		    "Reclaim RPC failure: batchno=%"PRId64" dst=%s "
		    "rc=%d", batchno, res->res_name, rc);
	}

	/*
	 * Record the progress first before potentially removing old log
	 * files.
	 */
	if (record)
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
	if (didwork == nios && count == SLM_RECLAIM_BATCH) {
		if (batchno >= 1)
			mds_remove_logfile(batchno - 1, 0, 0);
	}
	PSCFREE(reclaimbuf);
	return (didwork);
}

/**
 * slmjreclaimthr_main - Send garbage collection to I/O servers.
 */
void
slmjreclaimthr_main(struct psc_thread *thr)
{
	uint64_t batchno;
	int didwork, cleanup = 1;

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
			if (!current_reclaim_xid ||
			    mds_reclaim_lwm(0) > current_reclaim_xid) {
				freelock(&mds_distill_lock);
				break;
			}
			freelock(&mds_distill_lock);
			didwork = mds_send_batch_reclaim(batchno);
			batchno++;
		} while (didwork && mds_reclaim_hwm(1) >= batchno);

		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_waitrel_s(&mds_reclaim_waitq,
		    &mds_reclaim_waitqlock, SL_RECLAIM_MAX_AGE);
	}
}

/**
 * slmjnsthr_main - Send local namespace updates to peer MDSes.
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
			if (!current_update_xid ||
			    mds_update_lwm(0) > current_update_xid) {
				freelock(&mds_distill_lock);
				break;
			}
			freelock(&mds_distill_lock);
			didwork = mds_send_batch_update(batchno);
			batchno++;
		} while (didwork && mds_update_hwm(1) >= batchno);

		spinlock(&mds_update_waitqlock);
		psc_waitq_waitrel_s(&mds_update_waitq,
		    &mds_update_waitqlock, SL_UPDATE_MAX_AGE);
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

	sjir = pjournal_get_buf(mdsJournal, sizeof(*sjir));
	mdslogfill_ino_repls(f, sjir);
	pjournal_add_entry(mdsJournal, txg, MDS_LOG_INO_REPLS, 0,
	    sjir, sizeof(*sjir));
	pjournal_put_buf(mdsJournal, sjir);
}

void
mdslogfill_bmap_repls(struct bmapc_memb *b,
    struct slmds_jent_bmap_repls *sjbr)
{
	struct fidc_membh *f = b->bcm_fcmh;

	FCMH_BUSY_ENSURE(f);
	BMAP_BUSY_ENSURE(b);

	sjbr->sjbr_fid = fcmh_2_fid(f);
	sjbr->sjbr_bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &sjbr->sjbr_bgen);

	sjbr->sjbr_nrepls = fcmh_2_nrepls(f);
	sjbr->sjbr_replpol = fcmh_2_replpol(f);

	memcpy(sjbr->sjbr_repls, b->bcm_repls, SL_REPLICA_NBYTES);

	DEBUG_BMAPOD(PLL_DEBUG, b, "filled bmap_repls journal log entry");
}

int
slm_wkcb_wr_brepl(void *p)
{
	struct slm_wkdata_wr_brepl *wk = p;

	slm_repl_upd_write(wk->b);
	return (0);
}

/**
 * mdslog_bmap_repl - Write a recently modified replication table to the
 *	journal.
 * Note:  bmap must be locked to prevent further changes from sneaking
 *	in before the repl table is committed to the journal.
 */
void
mdslog_bmap_repls(void *datap, uint64_t txg, __unusedx int flag)
{
	struct slmds_jent_bmap_repls *sjbr;
	struct slm_wkdata_wr_brepl *wk;
	struct bmapc_memb *b = datap;

	psc_assert(slm_opstate == SLM_OPSTATE_NORMAL);

	sjbr = pjournal_get_buf(mdsJournal, sizeof(*sjbr));
	mdslogfill_bmap_repls(b, sjbr);
	pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_REPLS, 0, sjbr,
	    sizeof(*sjbr));
	pjournal_put_buf(mdsJournal, sjbr);

	/*
	 * Relinquish ownership, which was asserted in
	 * mdslogfill_bmap_repls(), whichever wkthr who gets it.
	 */
	DEBUG_FCMH(PLL_DEBUG, b->bcm_fcmh, "pass BUSY to wkthr");
	b->bcm_fcmh->fcmh_owner = 0;
	(void)BMAP_RLOCK(b);
	b->bcm_owner = 0;
	b->bcm_flags |= BMAP_MDS_REPLMODWR;
	BMAP_ULOCK(b);

	wk = pfl_workq_getitem(slm_wkcb_wr_brepl,
	    struct slm_wkdata_wr_brepl);
	wk->b = b;
	bmap_op_start_type(b, BMAP_OPCNT_WORK);
	pfl_workq_putitem_head(wk);
}

/**
 * mdslog_bmap_crc - Commit bmap CRC changes to the journal.
 * @datap: CRC log structure.
 * @txg: transaction group ID.
 * Notes: bmap_crc_writes from the ION are sent here directly because
 *	this function is responsible for updating the cached bmap after
 *	the CRC has been committed to the journal.  This allows us to
 *	not have to hold the lock while doing journal I/O with the
 *	caveat that we trust the ION to not send multiple CRC updates
 *	for the same region which we may then process out of order.
 */
void
mdslog_bmap_crc(void *datap, uint64_t txg, __unusedx int flag)
{
	struct sl_mds_crc_log *crclog = datap;
	struct bmapc_memb *bmap = crclog->scl_bmap;
	struct srm_bmap_crcup *crcup = crclog->scl_crcup;
	struct slmds_jent_bmap_crc *sjbc;
	uint32_t n, t, distill;

	BMAPOD_READ_DONE(bmap, 0);

	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	/*
	 * See if we need to distill the file enlargement information.
	 */
	distill = crcup->extend;
	for (t = 0, n = 0; t < crcup->nups; t += n) {

		n = MIN(SLJ_MDS_NCRCS, crcup->nups - t);

		sjbc = pjournal_get_buf(mdsJournal, sizeof(*sjbc));
		sjbc->sjbc_fid = fcmh_2_fid(bmap->bcm_fcmh);
		sjbc->sjbc_iosid = crclog->scl_iosid;
		sjbc->sjbc_bmapno = bmap->bcm_bmapno;
		sjbc->sjbc_ncrcs = n;
		sjbc->sjbc_fsize = crcup->fsize;		/* largest known size */
		sjbc->sjbc_repl_nblks = crcup->nblks;
		sjbc->sjbc_aggr_nblks = fcmh_2_nblks(bmap->bcm_fcmh);
		sjbc->sjbc_extend = distill;
		sjbc->sjbc_utimgen = crcup->utimgen;		/* utime generation number */

		memcpy(sjbc->sjbc_crc, &crcup->crcs[t],
		    n * sizeof(struct srt_bmap_crcwire));

		pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_CRC,
		    distill, sjbc, sizeof(*sjbc));

		if (!distill)
			pjournal_put_buf(mdsJournal, sjbc);
		else
			distill = 0;
	}

	psc_assert(t == crcup->nups);

	/* Signify that the update has occurred. */
	BMAP_LOCK(bmap);
	bmap->bcm_flags &= ~BMAP_MDS_CRC_UP;
	BMAP_ULOCK(bmap);
}

void
mds_journal_init(int disable_propagation, uint64_t fsuuid)
{
	uint64_t lwm, hwm, batchno, last_reclaim_xid = 0, last_update_xid = 0, last_distill_xid = 0;
	int i, ri, rc, max, nios, count, stale, total, index, npeers;
	struct srt_reclaim_entry *reclaim_entryp;
	struct srt_update_entry *update_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;
	struct sl_resm *resm;
	struct srt_stat	sstb;
	void *handle, *reclaimbuf = NULL;
	char *jrnldev, fn[PATH_MAX];
	size_t size;

	OPSTAT_INCR(SLM_OPST_RECLAIM_CURSOR);

	psc_assert(_MDS_LOG_LAST_TYPE <= (1 << 15));
	psc_assert(sizeof(struct srt_update_entry) == 512);

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri)
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
	if (!disable_propagation) {
		SL_MDS_WALK(resm, npeers++);
		npeers--;
		if (npeers > MAX_UPDATE_PROG_ENTRY)
			psc_fatalx("number of metadata servers (%d) "
			    "exceeds %d", npeers,
			    MAX_UPDATE_PROG_ENTRY);
	}

	res = nodeResm->resm_res;
	if (res->res_jrnldev[0] == '\0')
		xmkfn(res->res_jrnldev, "%s/%s", sl_datadir,
		    SL_FN_OPJOURNAL);

	mdsJournal = pjournal_open(res->res_jrnldev);
	if (mdsJournal == NULL)
		psc_fatalx("failed to open log file %s", res->res_jrnldev);

#if 0
	/*
	 * We should specify the uuid in the journal when creating it.
	 * Currently, we allow a random number to be used.
	 */
	if (fsuuid && (mdsJournal->pj_hdr->pjh_fsuuid != fsuuid))
		psc_fatalx("UUID mismatch FS=%"PRIx64" JRNL=%"PRIx64".  "
		    "The journal needs to be reinitialized.",
		  fsuuid, mdsJournal->pj_hdr->pjh_fsuuid);
#endif

	jrnldev = res->res_jrnldev;
	mds_open_cursor();

	/*
	 * We need the cursor thread to start any potential log replay.
	 * Also, without this thread, we can't write anything into ZFS.
	 * We can't even read from ZFS because a read changes atime.
	 */
	pscthr_init(SLMTHRT_CURSOR, 0, mds_cursor_thread, NULL, 0,
	    "slmjcursorthr");

	xmkfn(fn, "%s", SL_FN_RECLAIMPROG);
	rc = mds_open_file(fn, O_RDWR, &reclaim_progfile_handle);
	psc_assert(rc == 0);

	/*
	 * Allocate maximum amount of memory to avoid dealing with
	 * the coming and going of IOS as compared with the records
	 * in the progress files.
	 */
	reclaim_prog_buf = PSCALLOC(MAX_RECLAIM_PROG_ENTRY *
	    sizeof(struct reclaim_prog_entry));
	rc = mds_read_file(reclaim_progfile_handle, reclaim_prog_buf,
	    MAX_RECLAIM_PROG_ENTRY * sizeof(struct reclaim_prog_entry),
	    &size, 0);
	psc_assert(rc == 0);

	/* Find out the highest reclaim batchno and xid */

	stale = 0;
	batchno = 0;
	count = index = size / sizeof(struct reclaim_prog_entry);
	for (i = 0; i < count; i++) {

		res = libsl_id2res(reclaim_prog_buf[i].res_id);
		if (res == NULL || !RES_ISFS(res)) {
			psclog_warnx("Bad or non-FS resource ID %u "
			    "found in reclaim progress file",
			    reclaim_prog_buf[i].res_id);

			stale++;
			reclaim_prog_buf[i].res_xid = 0;
			reclaim_prog_buf[i].res_batchno = 0;

			continue;
		}

		iosinfo = res2rpmi(res)->rpmi_info;
		iosinfo->si_xid = reclaim_prog_buf[i].res_xid;
		iosinfo->si_batchno = reclaim_prog_buf[i].res_batchno;
		iosinfo->si_flags &= ~SIF_NEED_JRNL_INIT;
		iosinfo->si_index = i;
		if (iosinfo->si_batchno > batchno)
			batchno = iosinfo->si_batchno;
	}
	if (stale) {
		rc = mds_write_file(reclaim_progfile_handle, reclaim_prog_buf,
		    size, &size, 0);
		psc_assert(rc == 0);
		psc_assert(size == count * sizeof(struct reclaim_prog_entry));
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
		    "batchno=%"PRId64": %s", batchno, slstrerror(rc));

	rc = mdsio_getattr(current_vfsid, 0, handle, &rootcreds, &sstb);
	psc_assert(rc == 0);

	current_reclaim_batchno = batchno;
	OPSTAT_INCR(SLM_OPST_RECLAIM_BATCHNO);
	OPSTAT_ASSIGN(SLM_OPST_RECLAIM_BATCHNO, batchno);

	if (sstb.sst_size) {
		reclaimbuf = PSCALLOC(sstb.sst_size);

		rc = mds_read_file(handle, reclaimbuf, sstb.sst_size,
		    &size, 0);
		psc_assert(rc == 0 && size == sstb.sst_size);

		max = SLM_RECLAIM_BATCH;
		reclaim_entryp = reclaimbuf;

		if (reclaim_entryp->xid != RECLAIM_MAGIC_VER ||
		    reclaim_entryp->fg.fg_gen != RECLAIM_MAGIC_GEN ||
		    reclaim_entryp->fg.fg_fid != RECLAIM_MAGIC_FID)
			psc_fatalx("Reclaim log corrupted, batchno=%"PRId64,
				    current_reclaim_batchno);

		size -= sizeof(struct srt_reclaim_entry);;
		max = SLM_RECLAIM_BATCH - 1;
		reclaim_entryp = PSC_AGP(reclaim_entryp,
		    sizeof(struct srt_reclaim_entry));

		psc_assert((size % sizeof(struct srt_reclaim_entry)) == 0);

		total = size / sizeof(struct srt_reclaim_entry);
		count = 0;
		psclog_info("scanning the last reclaim log, batchno=%"PRId64,
		    current_reclaim_batchno);
		while (count < total) {
			last_reclaim_xid = reclaim_entryp->xid;
			reclaim_entryp = PSC_AGP(reclaim_entryp,
			    sizeof(struct srt_reclaim_entry));
			count++;
		}
		if (total > max) {
			psclog_warnx("the last reclaim log has %d "
			    "entry(s) - more than it should have!",
			    total);
			total = max;
		}
		if (total == max)
			current_reclaim_batchno++;
		PSCFREE(reclaimbuf);
	}

	current_reclaim_xid = last_reclaim_xid;
	OPSTAT_INCR(SLM_OPST_RECLAIM_XID);
	OPSTAT_ASSIGN(SLM_OPST_RECLAIM_XID, current_reclaim_xid);

	last_distill_xid = last_reclaim_xid;

	mds_release_file(handle);

	/* search for newly-added I/O servers */
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		if (iosinfo->si_xid == 0 && iosinfo->si_batchno == 0) {
			iosinfo->si_xid = current_reclaim_xid;
			iosinfo->si_batchno = current_reclaim_batchno;
			psclog_info("Fast forward batchno/xid for "
			    "resource ID %u", res->res_id);
		}
		if (!(iosinfo->si_flags & SIF_NEED_JRNL_INIT))
			continue;

		iosinfo->si_index = index++;
		iosinfo->si_flags &= ~SIF_NEED_JRNL_INIT;
		iosinfo->si_flags |= SIF_NEW_PROG_ENTRY;
	}

	psclog_info("current_reclaim_batchno=%"PRId64" "
	    "current_reclaim_xid=%"PRId64,
	    current_reclaim_batchno, current_reclaim_xid);

	/* We are done if we don't have any peer MDSes */
	if (!npeers)
		goto replay_log;

	xmkfn(fn, "%s", SL_FN_UPDATEPROG);
	rc = mds_open_file(fn, O_RDWR, &update_progfile_handle);
	psc_assert(rc == 0);

	update_prog_buf = PSCALLOC(MAX_UPDATE_PROG_ENTRY *
	    sizeof(struct update_prog_entry));
	rc = mds_read_file(update_progfile_handle, update_prog_buf,
	    MAX_UPDATE_PROG_ENTRY * sizeof(struct update_prog_entry),
	    &size, 0);
	psc_assert(rc == 0);

	/* Find out the highest update batchno and xid */

	batchno = UINT64_MAX;
	count = size / sizeof(struct update_prog_entry);
	for (i = 0; i < count; i++) {
		res = libsl_id2res(update_prog_buf[i].res_id);
		if (res->res_type != SLREST_MDS) {
			psclog_warnx("non-MDS resource ID %u "
			    "in update file", res->res_id);
			continue;
		}
		peerinfo = res2rpmi(res)->rpmi_info;

		peerinfo->sp_flags &= ~SPF_NEED_JRNL_INIT;
		peerinfo->sp_xid = update_prog_buf[i].res_xid;
		peerinfo->sp_batchno = update_prog_buf[i].res_batchno;
		if (peerinfo->sp_batchno < batchno)
			batchno = peerinfo->sp_batchno;
	}
	PSCFREE(update_prog_buf);
	update_prog_buf = PSCALLOC(npeers * sizeof(struct update_prog_entry));

	if (batchno == UINT64_MAX)
		batchno = 0;

	rc = mds_open_logfile(batchno, 1, 1, &handle);
	if (rc) {
		if (batchno) {
			batchno--;
			rc = mds_open_logfile(batchno, 1, 1, &handle);
		}
	}
	if (rc)
		psc_fatalx("Failed to open update log file, "
		    "batchno=%"PRId64": %s", batchno, slstrerror(rc));

	current_update_batchno = batchno;
	updatebuf = PSCALLOC(SLM_UPDATE_BATCH *
	    sizeof(struct srt_update_entry));

	rc = mds_read_file(handle, updatebuf,
	    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry), &size, 0);
	mds_release_file(handle);

	psc_assert(rc == 0);
	psc_assert((size % sizeof(struct srt_update_entry)) == 0);

	total = size / sizeof(struct srt_update_entry);
	count = 0;
	update_entryp = updatebuf;
	while (count < total) {
		last_update_xid = update_entryp->xid;
		update_entryp++;
		count++;
	}
	current_update_xid = last_update_xid;
	if (total == SLM_UPDATE_BATCH)
		current_update_batchno++;

	if (last_distill_xid < last_update_xid)
		last_distill_xid = last_update_xid;

	/* search for newly-added metadata servers */
	SL_MDS_WALK(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;
		if (!(peerinfo->sp_flags & SPF_NEED_JRNL_INIT))
			continue;

		peerinfo->sp_xid = current_update_xid;
		peerinfo->sp_batchno = current_update_batchno;
		peerinfo->sp_flags &= ~SPF_NEED_JRNL_INIT;
	);

	psclog_info("current_update_batchno = %"PRId64", current_update_xid = %"PRId64,
	    current_update_batchno, current_update_xid);

 replay_log:

	mdsJournal->pj_npeers = npeers;
	mdsJournal->pj_distill_xid = last_distill_xid;
	mdsJournal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	mdsJournal->pj_replay_xid = mds_cursor.pjc_replay_xid;

	psclog_info("Journal device is %s", jrnldev);
	psclog_info("Last SLASH FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psclog_info("Last synced ZFS transaction group number is %"PRId64,
	    mdsJournal->pj_commit_txg);
	psclog_info("Last replayed SLASH2 transaction ID is %"PRId64,
	    mdsJournal->pj_replay_xid);

	pjournal_replay(mdsJournal, SLMTHRT_JRNL, "slmjthr",
	    mds_replay_handler, mds_distill_handler);

	psclog_info("Last used SLASH2 transaction ID is %"PRId64,
	   mdsJournal->pj_lastxid);

	psclog_info("The next FID will be %"PRId64, slm_get_curr_slashfid());

	mds_bmap_getcurseq(&hwm, &lwm);
	psclog_info("Last bmap sequence number LWM is %"PRId64, lwm);
	psclog_info("Last bmap sequence number HWM is %"PRId64, hwm);

	psclog_info("Journal UUID=%"PRIx64" MDS UUID=%"PRIx64,
	    mdsJournal->pj_hdr->pjh_fsuuid, fsuuid);

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, 0, slmjreclaimthr_main, NULL, 0,
	    "slmjreclaimthr");
	if (!npeers)
		return;
#if 0
	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	pscthr_init(SLMTHRT_JNAMESPACE, 0, slmjnsthr_main, NULL, 0,
	    "slmjnsthr");
#endif

}

void
mds_reserve_slot(int count)
{
	pjournal_reserve_slot(mdsJournal, count);
}

void
mds_unreserve_slot(int count)
{
	pjournal_unreserve_slot(mdsJournal, count);
}
