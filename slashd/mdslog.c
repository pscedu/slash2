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

#include <sys/param.h>

#include <inttypes.h>
#include <string.h>

#include "pfl/fcntl.h"
#include "pfl/fs.h"
#include "psc_ds/dynarray.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/crc.h"
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
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"

#include "zfs-fuse/zfs_slashlib.h"

#define SLM_CBARG_SLOT_CSVC	0
#define SLM_CBARG_SLOT_RESPROF	1

struct prog_tracker {
	char			 pt_prog_fn[PATH_MAX];
	void			*pt_prog_buf;
	void			*pt_prog_handle;

	uint64_t		 pt_current_batchno;
	uint64_t		 pt_current_xid;
	uint64_t		 pt_sync_xid;
	void			*pt_logfile_handle;
	off_t			 pt_logfile_offset;
	struct psc_waitq	 pt_waitq;
	psc_spinlock_t		 pt_lock;
	struct psclist_head	 pt_buflist;
	void			*pt_buf;
};

struct psc_journal		*mdsJournal;

extern struct bmap_timeo_table	 mdsBmapTimeoTbl;

uint64_t			 current_update_batchno;
uint64_t			 current_reclaim_batchno;

static psc_spinlock_t		 mds_distill_lock = SPINLOCK_INIT;

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
static void			*updatebuf;

/* a buffer used to read on-disk reclaim log file */
static void			*reclaimbuf;

static void			*mds_cursor_handle;
struct psc_journal_cursor	 mds_cursor;

psc_spinlock_t			 mds_txg_lock = SPINLOCK_INIT;

static int
mds_open_file(char *fn, int flags, void **handle)
{
	mdsio_fid_t mf;
	int rc;

	rc = mdsio_lookup(mds_metadir_inum, fn, &mf, &rootcreds, NULL);
	if (rc == ENOENT && (flags & O_CREAT)) {
		rc = mdsio_opencreatef(mds_metadir_inum, &rootcreds,
		    flags, MDSIO_OPENCRF_NOLINK, 0600, fn, NULL, NULL,
		    handle, NULL, NULL, 0);
	} else if (!rc) {
		rc = mdsio_opencreate(mf, &rootcreds, flags, 0, NULL,
		    NULL, NULL, handle, NULL, NULL, 0);
	}
	return (rc);
}

#define mds_read_file(h, buf, size, nb, off)				\
	mdsio_read(&rootcreds, (buf), (size), (nb), (off), (h))

#define mds_write_file(h, buf, size, nb, off)				\
	mdsio_write(&rootcreds, (buf), (size), (nb), (off), 0, (h), NULL, NULL)

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
	int ri, rc, nios = 0;
	size_t size;

	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		iosinfo = res2rpmi(res)->rpmi_info;
		reclaim_prog_buf[nios].res_id = res->res_id;
		reclaim_prog_buf[nios].res_xid = iosinfo->si_xid;
		reclaim_prog_buf[nios].res_batchno = iosinfo->si_batchno;
		nios++;
	}
	rc = mds_write_file(reclaim_progfile_handle, reclaim_prog_buf,
	    nios * sizeof(struct reclaim_prog_entry), &size, 0);
	psc_assert(rc == 0);
	psc_assert(size == (size_t)nios * sizeof(struct reclaim_prog_entry));
}

/**
 * mds_txg_handler - Tie system journal with ZFS transaction groups.
 */
void
mds_txg_handler(__unusedx uint64_t *txgp, __unusedx void *data, int op)
{
	psc_assert(op == PJRNL_TXG_GET || op == PJRNL_TXG_PUT);
}

void
mds_remove_logfile(uint64_t batchno, int update)
{
	char logfn[PATH_MAX];

	if (update)
		xmkfn(logfn, "%s.%d", SL_FN_UPDATELOG, batchno);
	else
		xmkfn(logfn, "%s.%d", SL_FN_RECLAIMLOG, batchno);
	mdsio_unlink(mds_metadir_inum, NULL, logfn, &rootcreds, NULL);
}

int
mds_open_logfile(uint64_t batchno, int update, int readonly,
    void **handle)
{
	char log_fn[PATH_MAX];
	int rc;

	if (update) {
		xmkfn(log_fn, "%s.%d", SL_FN_UPDATELOG, batchno);
	} else {
		xmkfn(log_fn, "%s.%d", SL_FN_RECLAIMLOG, batchno);
	}
	if (readonly) {
		/*
		 * The caller should check the return value.
		 */
		return (mds_open_file(log_fn, O_RDONLY, handle));
	}

	/*
	 * Note we use different file descriptors for read and write.
	 * Luckily, Linux maintains the file offset independently for
	 * each open.
	 *
	 * During replay, we need to read the file first to find out
	 * the right position, so we can't use O_WRONLY.
	 */
	rc = mds_open_file(log_fn, O_RDWR, handle);
	if (rc == 0) {
		/*
		 * During replay, the offset will be determined by the
		 * xid.
		 */
		return (rc);
	}
	rc = mds_open_file(log_fn, O_CREAT | O_TRUNC | O_WRONLY, handle);
	if (rc)
		psc_fatalx("Failed to create log file %s: %s", log_fn,
		    slstrerror(rc));
	return (rc);
}

/**
 * mds_distill_handler - Distill information from the system journal and
 *	write into namespace update or garbage reclaim logs.
 *
 *	Writing the information to secondary logs allows us to recyle
 *	the space in the main system log as quick as possible.  The
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
mds_distill_handler(struct psc_journal_enthdr *pje, uint64_t xid,
    int npeers, int action)
{
	struct srt_reclaim_entry reclaim_entry, *reclaim_entryp;
	struct srt_update_entry update_entry, *update_entryp;
	struct slmds_jent_namespace *sjnm = NULL;
	struct slmds_jent_bmap_crc *sjbc = NULL;
	int rc, count, total;
	uint16_t type;
	size_t size;

	/*
	 * Make sure that the distill log hits the disk now.  This
	 * action can be called by any process that needs log space.
	 */
	if (action == 2) {

		spinlock(&mds_distill_lock);
		if (xid < sync_update_xid) {
			sync_update_xid = current_update_xid;
			freelock(&mds_distill_lock);

			psc_assert(update_logfile_handle);
			mdsio_fsync(&rootcreds, 0, update_logfile_handle);
			spinlock(&mds_distill_lock);
		}
		if (xid < sync_reclaim_xid)  {
			sync_reclaim_xid = current_reclaim_xid;
			freelock(&mds_distill_lock);

			psc_assert(reclaim_logfile_handle);
			mdsio_fsync(&rootcreds, 0, reclaim_logfile_handle);
		} else
			freelock(&mds_distill_lock);

		return (0);
	}

	/*
	 * The following can only be executed by the singleton distill
	 * thread.
	 */

	psc_assert(pje->pje_magic == PJE_MAGIC);

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
	 */

	/*
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

		reclaim_logfile_offset = 0;
		mds_open_logfile(current_reclaim_batchno, 0, 0,
		    &reclaim_logfile_handle);

		if (action == 1) {
			rc = mds_read_file(reclaim_logfile_handle,
			    reclaimbuf, SLM_RECLAIM_BATCH *
			    sizeof(struct srt_reclaim_entry), &size, 0);
			if (rc)
				psc_fatalx("Failed to read reclaim log "
				    "file, batchno=%"PRId64": %s",
				    current_reclaim_batchno,
				    slstrerror(rc));

			total = size / sizeof(struct srt_reclaim_entry);

			count = 0;
			reclaim_entryp = reclaimbuf;
			while (count < total) {
				if (reclaim_entryp->xid == pje->pje_xid) {
					psclog_warnx("Reclaim distill %"PRId64,
					    pje->pje_xid);
					break;
				}
				reclaim_entryp++;
				count++;
				reclaim_logfile_offset +=
				    sizeof(struct srt_reclaim_entry);
			}
		}
	}

	reclaim_entry.xid = pje->pje_xid;
	reclaim_entry.fg.fg_fid = sjnm->sjnm_target_fid;
	reclaim_entry.fg.fg_gen = sjnm->sjnm_target_gen;

	rc = mds_write_file(reclaim_logfile_handle, &reclaim_entry,
	    sizeof(struct srt_reclaim_entry), &size,
	    reclaim_logfile_offset);
	if (size != sizeof(struct srt_reclaim_entry))
		psc_fatal("Failed to write reclaim log file, batchno=%"PRId64,
		    current_reclaim_batchno);

	reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);
	if (reclaim_logfile_offset ==
	    SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry)) {

		mdsio_fsync(&rootcreds, 0, reclaim_logfile_handle);
		mdsio_release(&rootcreds, reclaim_logfile_handle);

		reclaim_logfile_handle = NULL;
		current_reclaim_batchno++;

		spinlock(&mds_distill_lock);
		sync_reclaim_xid = pje->pje_xid;
		freelock(&mds_distill_lock);

		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_wakeall(&mds_reclaim_waitq);
		freelock(&mds_reclaim_waitqlock);
	}

	spinlock(&mds_distill_lock);
	current_reclaim_xid = pje->pje_xid;
	freelock(&mds_distill_lock);

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
				update_logfile_offset += sizeof(struct srt_update_entry);
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
		psc_fatal("Failed to write update log file, batchno=%"PRId64,
		    current_update_batchno);

	/* see if we need to close the current update log file */
	update_logfile_offset += sizeof(struct srt_reclaim_entry);
	if (update_logfile_offset ==
	    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry)) {

		mdsio_fsync(&rootcreds, 0, update_logfile_handle);
		mdsio_release(&rootcreds, update_logfile_handle);

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

/**
 * mdslog_namespace - Log namespace operation before we attempt an
 *	it.  This makes sure that it will be propagated towards
 *	other MDSes and made permanent before we reply to the client.
 */
void
mdslog_namespace(int op, uint64_t txg, uint64_t pfid,
    uint64_t npfid, const struct srt_stat *sstb, int mask,
    const char *name, const char *newname)
{
	struct slmds_jent_namespace *sjnm;
	int distill = 0;

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
		 * zero-lengh file should NOT trigger this code.
		 */
		distill += 100;
		sjnm->sjnm_flag |= SJ_NAMESPACE_RECLAIM;
		sjnm->sjnm_target_gen = sstb->sst_gen;
		if (op == NS_OP_SETSIZE) {
			psc_assert(sstb->sst_gen >= 1);
			sjnm->sjnm_target_gen--;
		}
	}

	if (name) {
		sjnm->sjnm_namelen = strlen(name);
//		psc_assert(sjnm->sjnm_namelen <= sizeof(sjnm->sjnm_name));
		memcpy(sjnm->sjnm_name, name, sjnm->sjnm_namelen);
	}
	if (newname) {
		sjnm->sjnm_namelen2 = strlen(newname);
		psc_assert(sjnm->sjnm_namelen + sjnm->sjnm_namelen2 <=
		    sizeof(sjnm->sjnm_name));
		memcpy(sjnm->sjnm_name + sjnm->sjnm_namelen, newname,
		    sjnm->sjnm_namelen2);
	}

	pjournal_add_entry(mdsJournal, txg,
	    MDS_LOG_NAMESPACE, distill, sjnm,
	    offsetof(struct slmds_jent_namespace, sjnm_name) +
	    sjnm->sjnm_namelen + sjnm->sjnm_namelen2);

	if (!distill)
		pjournal_put_buf(mdsJournal, sjnm);

	psclog_notice("namespace op: optype=%d distill=%d "
	    "fid="SLPRI_FID" name='%s%s%s' mask=%#x size=%"PRId64" "
	    "link=%"PRId64" pfid="SLPRI_FID" npfid="SLPRI_FID" txg=%"PRId64,
	    op, distill,
	    sjnm->sjnm_target_fid, name,
	    newname ? "' newname='" : "", newname ? newname : "",
	    mask, sstb->sst_size, sstb->sst_nlink, pfid, npfid, txg);
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

/**
 * mds_send_batch_update - Send a batch of updates to peer MDSes
 *	that want them.
 */
int
mds_send_batch_update(uint64_t batchno)
{
	int i, rc, npeers, count, total, didwork = 0, record = 0;
	struct srt_update_entry *entryp, *next_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct slashrpc_cservice *csvc;
	struct srm_update_rep *mp;
	struct srm_update_req *mq;
	struct pscrpc_request *rq;
	struct sl_resm *resm;
	struct iovec iov;
	uint64_t xid;
	size_t size;
	void *handle;

	struct sl_resource *_res;
	struct sl_site *_site;
	int _siter;

	rc = mds_open_logfile(batchno, 1, 1, &handle);
	if (rc) {
		/*
		 * It is fine that the distill process hasn't written
		 * the next log file after closing the old one.
		 */
		if (rc != ENOENT)
			psc_fatalx("Failed to open update log file, "
			    "batchno=%"PRId64": %s",
			    batchno, slstrerror(rc));
		return (didwork);
	}
	rc = mds_read_file(handle, updatebuf,
	    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry), &size, 0);
	mdsio_release(&rootcreds, handle);

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
	CONF_FOREACH_SITE(_site)
	    SITE_FOREACH_RES(_site, _res, _siter) {
		if (_res->res_type != SLREST_MDS)
			continue;
		resm = psc_dynarray_getpos(
		    &_res->res_members, 0);
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

		csvc = slm_getmcsvc(resm);
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
			mds_remove_logfile(batchno-1, 1);
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
	struct psc_journal_cursor *cursor = buf;
	uint64_t hwm, lwm;
	int rc;
	static uint64_t start_txg = 0;

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

	rc = mds_bmap_getcurseq(&hwm, &lwm);
	if (rc)
		psc_assert(rc == -EAGAIN);
	else {
		cursor->pjc_seqno_lwm = lwm;
		cursor->pjc_seqno_hwm = hwm;
	}
}

/**
 * mds_cursor_thread - Update the cursor file in the ZFS that records
 *	the current transaction group number and other system log
 *	status.  If there is no activity in system other that this write
 *	to update the cursor, our customized ZFS will extend the
 *	lifetime of the transaction group.
 */
void
mds_cursor_thread(__unusedx struct psc_thread *thr)
{
	int rc;

	while (pscthr_run()) {
		rc = mdsio_write_cursor(&mds_cursor, sizeof(mds_cursor),
			mds_cursor_handle, mds_update_cursor);
		if (rc)
			psclog_warnx("failed to update cursor, rc=%d", rc);
		else
			psclog_notice("cursor updated: txg=%"PRId64", xid=%"PRId64
			    ", fid="SLPRI_FID", seqno=(%"PRIx64", %"PRIx64")",
			    mds_cursor.pjc_commit_txg,
			    mds_cursor.pjc_distill_xid,
			    mds_cursor.pjc_fid,
			    mds_cursor.pjc_seqno_lwm,
			    mds_cursor.pjc_seqno_hwm);
	}
}

void
mds_open_cursor(void)
{
	char *p, tmbuf[26];
	mdsio_fid_t mf;
	size_t nb;
	int rc;

	rc = mdsio_lookup(mds_metadir_inum, SL_FN_CURSOR, &mf,
	    &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(mf, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &mds_cursor_handle, NULL, NULL, 0);
	psc_assert(!rc && mds_cursor_handle);

	rc = mdsio_read(&rootcreds, &mds_cursor,
	    sizeof(struct psc_journal_cursor), &nb, 0, mds_cursor_handle);
	psc_assert(rc == 0 && nb == sizeof(struct psc_journal_cursor));

	psc_assert(mds_cursor.pjc_magic == PJRNL_CURSOR_MAGIC);
	psc_assert(mds_cursor.pjc_version == PJRNL_CURSOR_VERSION);
	psc_assert(mds_cursor.pjc_fid >= SLFID_MIN);

	if (FID_GET_SITEID(mds_cursor.pjc_fid) == 0)
		mds_cursor.pjc_fid |= (uint64_t)nodeSite->site_id <<
		    SLASH_FID_SITE_SHFT;
	if (FID_GET_SITEID(mds_cursor.pjc_fid) != nodeSite->site_id)
		psc_fatal("Mismatched site ID in the FID, expected %d",
		    nodeSite->site_id);

	slm_set_curr_slashfid(mds_cursor.pjc_fid);
	psclog_notice("File system was formatted on %"PRIu64" seconds "
	    "since the Epoch", mds_cursor.pjc_timestamp);
	psclog_notice("SLFID prior to replay="SLPRI_FID, mds_cursor.pjc_fid);
	ctime_r((time_t *)&mds_cursor.pjc_timestamp, tmbuf);
	p = strchr(tmbuf, '\n');
	if (p)
		*p = '\0';
	psclog_notice("File system was formatted on %s", tmbuf);
}

int
mds_send_batch_reclaim(uint64_t batchno)
{
	int i, ri, rc, len, count, nentry, total, nios, didwork = 0, record = 0;
	struct srt_reclaim_entry *entryp, *next_entryp;
	struct slashrpc_cservice *csvc;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct pscrpc_request *rq;
	struct sl_resm *dst_resm;
	struct sl_resource *res;
	struct iovec iov;
	uint64_t xid;
	size_t size;
	void *handle;

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
		return (didwork);
	}
	rc = mds_read_file(handle, reclaimbuf,
	    SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry), &size, 0);
	mdsio_release(&rootcreds, handle);

	if (size == 0)
		return (didwork);

	/*
	 * Short read is Okay, as long as it is a multiple of the basic
	 * data structure.
	 */
	psc_assert((size % sizeof(struct srt_reclaim_entry)) == 0);
	count = (int)size / (int)sizeof(struct srt_reclaim_entry);

	/* find the xid associated with the last log entry */
	entryp = PSC_AGP(reclaimbuf, (count - 1) *
	    sizeof(struct srt_reclaim_entry));
	xid = entryp->xid;

	/*
	 * Trim padding from buffer to reduce RPC traffic.
	 */
	entryp = next_entryp = reclaimbuf;
	len = size = offsetof(struct srt_reclaim_entry, _pad);
	for (i = 1; i < count; i++) {
		entryp++;
		next_entryp = PSC_AGP(next_entryp, len);
		memmove(next_entryp, entryp, len);
		size += len;
	}

	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		nios++;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);

		if (iosinfo->si_batchno < batchno) {
			RPMI_ULOCK(rpmi);
			continue;
		}

		/*
		 * Note that the reclaim xid we can see is not
		 * necessarily contiguous.
		 */
		if (iosinfo->si_batchno > batchno ||
		    iosinfo->si_xid >= xid) {
			RPMI_ULOCK(rpmi);
			didwork++;
			continue;
		}

		RPMI_ULOCK(rpmi);

		/* Find out which part of the buffer should be sent out */
		i = count;
		total = size;
		entryp = reclaimbuf;
		do {
			if (entryp->xid >= iosinfo->si_xid)
				break;
			i--;
			total -= len;
			entryp = PSC_AGP(entryp, len);
		} while (total);

		psc_assert(total);

		nentry = i;
		iov.iov_len = total;
		iov.iov_base = entryp;

		/*
		 * Send RPC to the I/O server and wait for it to
		 * complete.
		 * XXX use random
		 */
		DYNARRAY_FOREACH(dst_resm, i, &res->res_members) {
			csvc = slm_geticsvc_nb(dst_resm, NULL);
			if (csvc == NULL)
				continue;
			rc = SL_RSX_NEWREQ(csvc, SRMT_RECLAIM, rq, mq,
			    mp);
			if (rc) {
				sl_csvc_decref(csvc);
				continue;
			}

			mq->xid = xid;
			mq->size = iov.iov_len;
			mq->count = nentry;
			psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

			rsx_bulkclient(rq, BULK_GET_SOURCE,
			    SRMM_BULK_PORTAL, &iov, 1);

			rc = SL_RSX_WAITREP(csvc, rq, mp);
			if (rc == 0)
				rc = mp->rc;

			pscrpc_req_finished(rq);
			rq = NULL;
			sl_csvc_decref(csvc);

			if (rc == 0) {
				record++;
				didwork++;
				iosinfo->si_xid = xid + 1;
				if (count == SLM_RECLAIM_BATCH)
					iosinfo->si_batchno++;
				break;
			}
		}
	}

	/*
	 * Record the progress first before potentially removing old log
	 * file.
	 */
	if (record)
		mds_record_reclaim_prog();

	/*
	 * If this log file is full and all I/O servers have applied its
	 * contents, remove an old log file (keep the previous one so that
	 * we can figure out the last distill xid upon recovery).
	 */
	if (didwork == nios && count == SLM_RECLAIM_BATCH) {
		if (batchno >= 1)
			mds_remove_logfile(batchno - 1, 0);
	}
	return (didwork);
}

/**
 * slmjreclaimthr_main - Send garbage collection to I/O servers.
 */
void
slmjreclaimthr_main(__unusedx struct psc_thread *thr)
{
	uint64_t batchno;
	int didwork;

	/*
	 * Instead of tracking precisely which reclaim log record has
	 * been sent to an I/O node, we track the batch number.  A
	 * receiving I/O node can safely ignore any resent records.
	 */
	while (pscthr_run()) {
		batchno = mds_reclaim_lwm(1);
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
		} while (didwork && (mds_reclaim_hwm(1) >= batchno));

		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_waitrel_s(&mds_reclaim_waitq,
		    &mds_reclaim_waitqlock, SL_RECLAIM_MAX_AGE);
	}
}

/**
 * slmjnsthr_main - Send local namespace updates to peer MDSes.
 */
void
slmjnsthr_main(__unusedx struct psc_thread *thr)
{
	uint64_t batchno;
	int didwork;

	/*
	 * This thread scans the batches of updates between the low and
	 * high water marks and sends them to peer MDSes.  Although
	 * different MDSes have different paces, we send updates in
	 * order within one MDS.
	 */
	while (pscthr_run()) {
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
		} while (didwork && (mds_update_hwm(1) >= batchno));

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
	sjir->sjir_fid = fcmh_2_fid(f);
	sjir->sjir_nrepls = fcmh_2_nrepls(f);
	sjir->sjir_replpol = fcmh_2_replpol(f);
	memcpy(sjir->sjir_repls, ih->inoh_ino.ino_repls,
	    sizeof(ih->inoh_ino.ino_repls));
	if (fcmh_2_nrepls(f) >= SL_DEF_REPLICAS)
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
	int locked;

	sjbr->sjbr_fid = fcmh_2_fid(f);
	sjbr->sjbr_bmapno = b->bcm_bmapno;
	BHGEN_GET(b, &sjbr->sjbr_bgen);

	locked = FCMH_RLOCK(f);
	sjbr->sjbr_nrepls = fcmh_2_nrepls(f);
	sjbr->sjbr_replpol = fcmh_2_replpol(f);
	FCMH_URLOCK(f, locked);

	memcpy(sjbr->sjbr_repls, b->bcm_repls, SL_REPLICA_NBYTES);

	DEBUG_BMAPOD(PLL_DEBUG, b, "filled bmap_repls journal log");
}

/**
 * mdslog_bmap_repl - Write a modified replication table to the
 *	journal.
 * Note:  bmap must be locked to prevent further changes from sneaking
 *	in before the repl table is committed to the journal.
 */
void
mdslog_bmap_repls(void *datap, uint64_t txg, __unusedx int flag)
{
	struct slmds_jent_bmap_repls *sjbr;
	struct bmapc_memb *b = datap;

	sjbr = pjournal_get_buf(mdsJournal, sizeof(*sjbr));
	mdslogfill_bmap_repls(b, sjbr);
	pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_REPLS, 0, sjbr,
	    sizeof(*sjbr));
	pjournal_put_buf(mdsJournal, sjbr);
}

/**
 * mdslog_bmap_crc - Commit bmap CRC changes to the journal.
 * @bmap: the bmap (not locked).
 * @crcs: array of CRC / slot pairs.
 * @n: the number of CRC / slot pairs.
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
	struct bmap_mds_info *bmi = bmap_2_bmi(bmap);
	struct slmds_jent_bmap_crc *sjbc;
	uint32_t n, t, distill;

	BMAP_LOCK(bmap);
	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);
	BMAP_ULOCK(bmap);

	/*
	 * See if we need to distill the file enlargement information.
	 */
	distill = crcup->extend;
	for (t = 0, n = 0; t < crcup->nups; t += n) {

		n = MIN(SLJ_MDS_NCRCS, crcup->nups - t);

		sjbc = pjournal_get_buf(mdsJournal, sizeof(*sjbc));
		sjbc->sjbc_fid = fcmh_2_fid(bmap->bcm_fcmh);
		sjbc->sjbc_iosid = bmi->bmdsi_wr_ion->rmmi_resm->resm_res_id;
		sjbc->sjbc_bmapno = bmap->bcm_bmapno;
		sjbc->sjbc_ncrcs = n;
		sjbc->sjbc_fsize = crcup->fsize;		/* largest known size */
		sjbc->sjbc_repl_nblks = crcup->nblks;
		sjbc->sjbc_aggr_nblks = fcmh_2_nblks(bmap->bcm_fcmh);
		sjbc->sjbc_extend = distill;
		sjbc->sjbc_utimgen = crcup->utimgen;     /* utime generation number */

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
	/* Signify that the update has occurred.
	 */
	BMAP_LOCK(bmap);
	bmap->bcm_flags &= ~BMAP_MDS_CRC_UP;
	BMAP_ULOCK(bmap);
}

void
mds_journal_init(int disable_propagation, uint64_t fsuuid)
{
	uint64_t batchno, last_reclaim_xid = 0, last_update_xid = 0, last_distill_xid = 0;
	int i, ri, rc, nios, count, total, npeers;
	struct srt_reclaim_entry *reclaim_entryp;
	struct srt_update_entry *update_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_iosinfo *iosinfo;
	struct sl_resource *res;
	struct sl_resm *resm;
	char *jrnldev, fn[PATH_MAX];
	void *handle;
	size_t size;
	struct resprof_mds_info *rpmi;

	psc_assert(_MDS_LOG_LAST_TYPE <= (1 << 15));
	psc_assert(sizeof(struct srt_update_entry) == 512);
	psc_assert(sizeof(struct srt_reclaim_entry) == 512);

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri)
		if (RES_ISFS(res))
			nios++;
	if (!nios)
		psc_fatalx("Missing I/O servers at site %s",
		    nodeSite->site_name);

	if (nios > MAX_RECLAIM_PROG_ENTRY)
		psc_fatalx("Number of I/O servers exceed %d",
		    MAX_RECLAIM_PROG_ENTRY);

	/* Count the number of peer MDSes we have */
	npeers = 0;
	SL_MDS_WALK(resm, npeers++);
	npeers--;
	if (npeers > MAX_UPDATE_PROG_ENTRY)
		psc_fatalx("Number of metadata servers exceed %d",
		    MAX_UPDATE_PROG_ENTRY);

	if (disable_propagation)
		npeers = 0;

	res = nodeResm->resm_res;
	if (res->res_jrnldev[0] == '\0')
		xmkfn(res->res_jrnldev, "%s/%s", sl_datadir,
		    SL_FN_OPJOURNAL);

	mdsJournal = pjournal_open(res->res_jrnldev);
	if (mdsJournal == NULL)
		psc_fatal("Failed to open log file %s", res->res_jrnldev);
	
	if (fsuuid && (mdsJournal->pj_hdr->pjh_fsuuid != fsuuid))
		psc_fatal("UUID mismatch MDS=%"PRIx64" JRNL=%"PRIx64
		  ".  Reinitialize your journal.", 
		  fsuuid, mdsJournal->pj_hdr->pjh_fsuuid);

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

	reclaim_prog_buf = PSCALLOC(MAX_RECLAIM_PROG_ENTRY *
	    sizeof(struct reclaim_prog_entry));
	rc = mds_read_file(reclaim_progfile_handle, reclaim_prog_buf,
	    MAX_RECLAIM_PROG_ENTRY * sizeof(struct reclaim_prog_entry),
	    &size, 0);
	psc_assert(rc == 0);

	/* Find out the highest reclaim batchno and xid */

	batchno = UINT64_MAX;
	count = size / sizeof(struct reclaim_prog_entry);
	for (i = 0; i < count; i++) {
		res = libsl_id2res(reclaim_prog_buf[i].res_id);
		if (!RES_ISFS(res)) {
			psclog_warn("Non-FS resource ID %u "
			    "in reclaim file", res->res_id);
			continue;
		}
		iosinfo = res2rpmi(res)->rpmi_info;
		iosinfo->si_xid = reclaim_prog_buf[i].res_xid;
		iosinfo->si_batchno = reclaim_prog_buf[i].res_batchno;
		iosinfo->si_flags &= ~MDS_IOS_NEED_INIT;
		if (iosinfo->si_batchno < batchno)
			batchno = iosinfo->si_batchno;
	}
	PSCFREE(reclaim_prog_buf);
	reclaim_prog_buf = PSCALLOC(nios * sizeof(struct reclaim_prog_entry));

	if (batchno == UINT64_MAX)
		batchno = 0;

	rc = mds_open_logfile(batchno, 0, 1, &handle);
	if (rc) {
		if (batchno) {
			batchno--;
			rc = mds_open_logfile(batchno, 0, 1, &handle);
		}
	}
	if (rc)
		psc_fatalx("Failed to open reclaim log file, "
		    "batchno=%"PRId64": %s", batchno, slstrerror(rc));

	current_reclaim_batchno = batchno;
	reclaimbuf = PSCALLOC(SLM_RECLAIM_BATCH *
	    sizeof(struct srt_reclaim_entry));

	rc = mds_read_file(handle, reclaimbuf, SLM_RECLAIM_BATCH *
	    sizeof(struct srt_reclaim_entry), &size, 0);
	mdsio_release(&rootcreds, handle);

	psc_assert(rc == 0);
	psc_assert((size % sizeof(struct srt_reclaim_entry)) == 0);

	total = size / sizeof(struct srt_reclaim_entry);
	count = 0;
	reclaim_entryp = reclaimbuf;
	while (count < total) {
		last_reclaim_xid = reclaim_entryp->xid;
		reclaim_entryp++;
		count++;
	}
	current_reclaim_xid = last_reclaim_xid;
	if (total == SLM_RECLAIM_BATCH)
		current_reclaim_batchno++;

	last_distill_xid = last_reclaim_xid;

	/* search for newly-added I/O servers */
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (!RES_ISFS(res))
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		if (!(iosinfo->si_flags & MDS_IOS_NEED_INIT))
			continue;

		iosinfo->si_xid = current_reclaim_xid;
		iosinfo->si_batchno = current_reclaim_batchno;
		iosinfo->si_flags &= ~MDS_IOS_NEED_INIT;
	}

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, 0, slmjreclaimthr_main, NULL, 0,
	    "slmjreclaimthr");

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
			psclog_warnx("Non-MDS resource ID %u "
			    "in update file", res->res_id);
			continue;
		}
		peerinfo = res2rpmi(res)->rpmi_info;

		peerinfo->sp_flags &= ~MDS_PEER_NEED_INIT;
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
	mdsio_release(&rootcreds, handle);

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
		if (!(peerinfo->sp_flags & MDS_PEER_NEED_INIT))
			continue;

		peerinfo->sp_xid = current_update_xid;
		peerinfo->sp_batchno = current_update_batchno;
		peerinfo->sp_flags &= ~MDS_PEER_NEED_INIT;
	);

	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	pscthr_init(SLMTHRT_JNAMESPACE, 0, slmjnsthr_main, NULL,
	    0, "slmjnsthr");

 replay_log:

	mdsJournal->pj_npeers = npeers;
	mdsJournal->pj_distill_xid = last_distill_xid;
	mdsJournal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	mdsJournal->pj_replay_xid = mds_cursor.pjc_replay_xid;

	psclog_warnx("Journal device is %s", jrnldev);
	psclog_warnx("Last SLASH FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psclog_warnx("Last synced ZFS transaction group number is %"PRId64,
	    mdsJournal->pj_commit_txg);
	psclog_warnx("Last replayed SLASH2 transaction ID is %"PRId64,
	    mdsJournal->pj_replay_xid);

	pjournal_replay(mdsJournal, SLMTHRT_JRNL, "slmjthr",
	    mds_replay_handler, mds_distill_handler);

	psclog_warnx("Last used SLASH2 transaction ID is %"PRId64,
	   mdsJournal->pj_lastxid);

	mds_bmap_setcurseq(mds_cursor.pjc_seqno_hwm, mds_cursor.pjc_seqno_lwm);
	psclog_warnx("Last bmap sequence number low water mark is %"PRIx64,
	    mds_cursor.pjc_seqno_lwm);
	psclog_warnx("Last bmap sequence number high water mark is %"PRIx64,
	    mds_cursor.pjc_seqno_hwm);

	psclog_warnx("Journal UUID=%"PRIx64" MDS UUID=%"PRIx64,
	     mdsJournal->pj_hdr->pjh_fsuuid, fsuuid);
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
