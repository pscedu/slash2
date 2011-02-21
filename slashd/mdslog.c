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
#include "mdsio.h"
#include "mdslog.h"
#include "mkfn.h"
#include "pathnames.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "sljournal.h"

#include "zfs-fuse/zfs_slashlib.h"

#define SLM_CBARG_SLOT_CSVC	0
#define SLM_CBARG_SLOT_RESPROF	1

struct psc_journal		*mdsJournal;

extern struct bmap_timeo_table	 mdsBmapTimeoTbl;

uint64_t			 current_update_batchno;
uint64_t			 current_reclaim_batchno;

static psc_spinlock_t		 mds_distill_lock = SPINLOCK_INIT;

uint64_t			 current_update_xid = 0;
uint64_t			 current_reclaim_xid = 0;

uint64_t			 sync_update_xid = 0;
uint64_t			 sync_reclaim_xid = 0;

static void			*update_logfile_handle = NULL;
static void			*reclaim_logfile_handle = NULL;

static off_t			 update_logfile_offset;
static off_t			 reclaim_logfile_offset;

static void			*update_progfile_handle = NULL;
static void			*reclaim_progfile_handle = NULL;

char				 update_progfile_name[PATH_MAX];
char				 reclaim_progfile_name[PATH_MAX];

#define	MAX_UPDATE_PROG_ENTRY	32

/* namespace update progress tracker to peer MDSes */
struct update_prog_entry {
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
	sl_ios_id_t		 res_id;
	int32_t			 _pad;
};

#define	MAX_RECLAIM_PROG_ENTRY	32

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
static struct psc_journal_cursor mds_cursor;

psc_spinlock_t			 mds_txg_lock = SPINLOCK_INIT;

static int
mds_open_file(char *fn, int flags, void **handle)
{
	int rc;
	mdsio_fid_t mf;

	rc = mdsio_lookup(MDSIO_FID_ROOT, fn, &mf, &rootcreds, NULL);
	if (rc == ENOENT && (flags & O_CREAT)) {
		rc = mdsio_opencreate(MDSIO_FID_ROOT, &rootcreds, flags,
		    0600, fn, NULL, NULL, handle, NULL, NULL);
	} else if (!rc) {
		rc = mdsio_opencreate(mf, &rootcreds, flags, 0, NULL,
		    NULL, NULL, handle, NULL, NULL);
	}
	return (rc);
}

static int
mds_read_file(void *handle, char *buf, size_t size, size_t *nb, off_t off)
{
	int rc;

	rc = mdsio_read(&rootcreds, buf, size, nb, off, handle);
	return (rc);
}

static int
mds_write_file(void *handle, char *buf, size_t size, size_t *nb, off_t off)
{
	int rc;

	rc = mdsio_write(&rootcreds, buf, size, nb, off, 0, handle,
	    NULL, NULL);
	return (rc);
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
		update_prog_buf[i].res_id = resm->resm_iosid;
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
		if (res->res_type == SLREST_MDS)
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
 * mds_redo_bmap_repl - Replay a replication update on a bmap.  This has
 *	to be a read-modify-write process because we don't touch the CRC
 *	tables.
 */
static int
mds_redo_bmap_repl(struct psc_journal_enthdr *pje)
{
	struct slmds_jent_repgen *jrpg;
	struct bmap_ondisk bmap_disk;
	void *mdsio_data;
	mdsio_fid_t fid;
	size_t nb;
	int rc;

	jrpg = PJE_DATA(pje);

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
	rc = mdsio_setattr(mf, &sstb, PSCFS_SETATTRF_DATASIZE, &rootcreds,
	    NULL, mdsio_data, NULL);
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
mds_redo_ino_addrepl(struct psc_journal_enthdr *pje)
{
	struct slash_inode_extras_od inoh_extras;
	struct slmds_jent_ino_addrepl *jrir;
	struct slash_inode_od inoh_ino;
	void *mdsio_data;
	mdsio_fid_t fid;
	int pos, j, rc;
	size_t nb;

	memset(&inoh_ino, 0, sizeof(inoh_ino));

	jrir = PJE_DATA(pje);
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

/**
 * mds_txg_handler - Tie system journal with ZFS transaction groups.
 */
void
mds_txg_handler(__unusedx uint64_t *txgp, __unusedx void *data, int op)
{
	psc_assert(op == PJRNL_TXG_GET || op == PJRNL_TXG_PUT);
}

/**
 * mds_replay_handle - Handle journal replay events.
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

void
mds_remove_logfile(uint64_t batchno, int update)
{
	char log_fn[PATH_MAX];

	if (update) {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", sl_datadir,
		    SL_FN_UPDATELOG, batchno, psc_get_hostname(),
		    mds_cursor.pjc_timestamp);
	} else {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", sl_datadir,
		    SL_FN_RECLAIMLOG, batchno, psc_get_hostname(),
		    mds_cursor.pjc_timestamp);
	}
	unlink(log_fn);
}

int
mds_open_logfile(uint64_t batchno, int update, int readonly, void **handle)
{
	int rc;
	char log_fn[PATH_MAX];

	if (update) {
		xmkfn(log_fn, "%s.%d.%s.%lu",
		    SL_FN_UPDATELOG, batchno, psc_get_hostname(),
		    mds_cursor.pjc_timestamp);
	} else {
		xmkfn(log_fn, "%s.%d.%s.%lu",
		    SL_FN_RECLAIMLOG, batchno, psc_get_hostname(),
		    mds_cursor.pjc_timestamp);
	}
	if (readonly) {
		/*
		 * The caller should check the return value.
		 */
		rc = mds_open_file(log_fn, O_RDONLY, handle);
		return (rc);
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
		psc_fatal("Failed to create log file %s", log_fn);
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
mds_distill_handler(struct psc_journal_enthdr *pje, uint64_t xid, int npeers,
    int action)
{
	struct srt_reclaim_entry reclaim_entry, *reclaim_entryp;
	struct srt_update_entry update_entry, *update_entryp;
	struct slmds_jent_namespace *sjnm = NULL;
	struct slmds_jent_crc *jcrc = NULL;
	int rc, count, total;
	uint16_t type;
	size_t size;

	/*
	 * Make sure that the distill log hits the disk now. This action
	 * can be called by any process that needs log space.
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
	 * The following can only be executed by the single distill thread.
	 */

	psc_assert(pje->pje_magic == PJE_MAGIC);

	type = pje->pje_type & ~(_PJE_FLSHFT - 1);
	if (type == MDS_LOG_BMAP_CRC) {
		jcrc = PJE_DATA(pje);
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

	psc_assert(sjnm->sjnm_op == NS_OP_SETATTR ||
	    sjnm->sjnm_op == NS_OP_UNLINK ||
	    sjnm->sjnm_op == NS_OP_SETSIZE);

	if (reclaim_logfile_handle == NULL) {
		reclaim_logfile_offset = 0;
		mds_open_logfile(current_reclaim_batchno, 0, 0,
		    &reclaim_logfile_handle);

		/*
		 * Here we do one-time seek based on the xid stored in
		 * the entry.  Although not necessary contiguous, xids
		 * are in increasing order.
		 */
		if (action == 1) {
			rc = mds_read_file(reclaim_logfile_handle,
			    reclaimbuf, SLM_RECLAIM_BATCH *
			    sizeof(struct srt_reclaim_entry), &size, 0);
			if (rc)
				psc_fatal("Failed to read reclaim log "
				    "file, batchno=%"PRId64,
				    current_reclaim_batchno);

			total = size / sizeof(struct srt_reclaim_entry);

			count = 0;
			reclaim_entryp = reclaimbuf;
			while (count < total) {
				if (reclaim_entryp->xid == pje->pje_xid) {
					psclog_warnx("Reclaim distill %"PRId64, pje->pje_xid);
					break;
				}
				reclaim_entryp++;
				count++;
				reclaim_logfile_offset += sizeof(struct srt_reclaim_entry);
			}
		}
	}

	reclaim_entry.xid = pje->pje_xid;
	reclaim_entry.fg.fg_fid = sjnm->sjnm_target_fid;
	reclaim_entry.fg.fg_gen = sjnm->sjnm_target_gen;

	rc = mds_write_file(reclaim_logfile_handle, (void*)&reclaim_entry,
	    sizeof(struct srt_reclaim_entry), &size, reclaim_logfile_offset);
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

		/*
		 * Here we do one-time seek based on the xid stored in
		 * the entry.  Although not necessary contiguous, xids
		 * are in increasing order.
		 */
		if (action == 1) {
			rc = mds_read_file(update_logfile_handle,
			    updatebuf, SLM_UPDATE_BATCH *
			    sizeof(struct srt_update_entry), &size, 0);
			if (rc)
				psc_fatal("Failed to read update log "
				    "file, batchno=%"PRId64,
				    current_update_batchno);
			total = size / sizeof(struct srt_update_entry);

			count = 0;
			update_entryp = updatebuf;
			while (count < total) {
				if (update_entryp->xid == pje->pje_xid) {
					psclog_warnx("Update distill %"PRId64, pje->pje_xid);
					break;
				}
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
		update_entry.size = jcrc->sjc_fsize;
		update_entry.target_fid = jcrc->sjc_fid;
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
 * mds_namespace_log - Log namespace operation before we attempt an
 *	it.  This makes sure that it will be propagated towards
 *	other MDSes and made permanent before we reply to the client.
 */
void
mds_namespace_log(int op, uint64_t txg, uint64_t parent,
    uint64_t newparent, const struct srt_stat *sstb, int mask,
    const char *name, const char *newname)
{
	struct slmds_jent_namespace *sjnm;
	int distill;

	sjnm = pjournal_get_buf(mdsJournal,
	    sizeof(struct slmds_jent_namespace));
	memset(sjnm, 0, sizeof(*sjnm));
	sjnm->sjnm_magic = SJ_NAMESPACE_MAGIC;
	sjnm->sjnm_op = op;
	sjnm->sjnm_parent_fid = parent;
	sjnm->sjnm_target_fid = sstb->sst_fid;
	sjnm->sjnm_new_parent_fid = newparent;
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
	if ((op == NS_OP_UNLINK && sstb->sst_nlink == 1) ||
	    (op == NS_OP_SETSIZE && sstb->sst_size == 0)) {
		/*
		 * We want to reclaim the space taken by the previous
		 * generation.  Note that changing the attributes of a
		 * zero-lengh file should NOT trigger this code.
		 */
		distill = 1;
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

	psclog_notice("Namespace Op: op = %d, fid = %"PRId64", txg = %"PRId64,
	    op, sjnm->sjnm_target_fid, txg);
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
		if (res->res_type == SLREST_MDS)
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
		if (res->res_type == SLREST_MDS)
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
	uint64_t value = 0;
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
			psc_fatal("Failed to open update log file, "
			    "batchno=%"PRId64, batchno);
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
	 * Record the progress first before potentially remove old log file.
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
mds_update_cursor(void *buf, uint64_t txg)
{
	struct psc_journal_cursor *cursor = buf;
	int rc;

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

	cursor->pjc_fid = slm_get_curr_slashid();

	rc = mds_bmap_getcurseq(&cursor->pjc_seqno_hwm, &cursor->pjc_seqno_lwm);
	if (rc) {
		psc_assert(rc == -EAGAIN);
		cursor->pjc_seqno_lwm = cursor->pjc_seqno_hwm = BMAPSEQ_ANY;
	}
}

void
mds_current_txg(uint64_t *txg)
{
	/*
	 * Take a snapshot of the transaction group number stored in the
	 * cursor.  It maybe the current one being used, or the one that
	 * has already been synced.  And it can change afterwards.  This
	 * is okay, we only need to take a little care at replay time.
	 */
	spinlock(&mds_txg_lock);
	*txg = mds_cursor.pjc_commit_txg;
	freelock(&mds_txg_lock);
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
			psclog_notice("Cursor updated: txg=%"PRId64", xid=%"PRId64
			    ", fid="SLPRI_FID", seqno=(%"PRId64", %"PRId64")",
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
	mdsio_fid_t mf;
	size_t nb;
	int rc;

	rc = mdsio_lookup(MDSIO_FID_ROOT, SL_PATH_CURSOR, &mf,
	    &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(mf, &rootcreds, O_RDWR, 0, NULL, NULL,
	    NULL, &mds_cursor_handle, NULL, NULL);
	psc_assert(!rc && mds_cursor_handle);

	rc = mdsio_read(&rootcreds, &mds_cursor,
	    sizeof(struct psc_journal_cursor), &nb, 0, mds_cursor_handle);
	psc_assert(rc == 0 && nb == sizeof(struct psc_journal_cursor));

	psc_assert(mds_cursor.pjc_magic == PJRNL_CURSOR_MAGIC);
	psc_assert(mds_cursor.pjc_version == PJRNL_CURSOR_VERSION);
	psc_assert(mds_cursor.pjc_fid >= SLFID_MIN);

	slm_set_curr_slashid(mds_cursor.pjc_fid);
	psclog_notice("File system was formatted on %"PRIu64" seconds "
	    "since the Epoch", mds_cursor.pjc_timestamp);
	psclog_notice("File system was formatted on %s",
	    ctime((time_t *)&mds_cursor.pjc_timestamp));
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
			psc_fatal("Failed to open reclaim log file, "
			    "batchno=%"PRId64, batchno);
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
		if (res->res_type == SLREST_MDS)
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
	 * Record the progress first before potentially remove old log file.
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
 * mds_send_reclaim - Send garbage collection to I/O servers.
 */
void
mds_send_reclaim(__unusedx struct psc_thread *thr)
{
	uint64_t batchno;
	int rv, didwork;

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
		rv = psc_waitq_waitrel_s(&mds_reclaim_waitq,
		    &mds_reclaim_waitqlock, SL_RECLAIM_MAX_AGE);
	}
}

/**
 * mds_send_update - Send local namespace updates to peer MDSes.
 */
void
mds_send_update(__unusedx struct psc_thread *thr)
{
	int rv, didwork;
	uint64_t batchno;

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
		rv = psc_waitq_waitrel_s(&mds_update_waitq,
		    &mds_update_waitqlock, SL_UPDATE_MAX_AGE);
	}
}

void
mds_inode_sync(struct slash_inode_handle *inoh)
{
	int rc, tmpx = 0;

	INOH_LOCK(inoh);

	if (inoh->inoh_flags & INOH_INO_DIRTY) {
		psc_crc64_calc(&inoh->inoh_ino.ino_crc, &inoh->inoh_ino,
		    INO_OD_CRCSZ);

		rc = mdsio_inode_write(inoh);
		if (rc)
			DEBUG_INOH(PLL_FATAL, inoh, "rc=%d", rc);

		if (inoh->inoh_flags & INOH_INO_NEW) {
			inoh->inoh_flags &= ~INOH_INO_NEW;
			inoh->inoh_flags |= INOH_EXTRAS_DIRTY;

			if (inoh->inoh_extras == NULL) {
				inoh->inoh_extras = (void *)&null_inox_od;
				tmpx = 1;
			}
		}
	}

	if (inoh->inoh_flags & INOH_EXTRAS_DIRTY) {
		psc_crc64_calc(&inoh->inoh_extras->inox_crc,
		    inoh->inoh_extras, INOX_OD_CRCSZ);
		rc = mdsio_inode_extras_write(inoh);

		if (rc)
			DEBUG_INOH(PLL_FATAL, inoh, "xtras rc=%d sync fail",
			    rc);
		else
			DEBUG_INOH(PLL_TRACE, inoh, "xtras sync ok");

		inoh->inoh_flags &= ~INOH_EXTRAS_DIRTY;
	}

	if (tmpx)
		inoh->inoh_extras = NULL;

	INOH_ULOCK(inoh);
}

/**
 * mds_bmap_sync - Callback function which is called from
 *	mdsfssyncthr_begin().
 * @data: void * which is the bmap.
 * Notes: this call allows SLASH2 to optimize CRC calculation by only
 *	taking them when the bmap is written, not upon each update to
 *	the bmap.  It is important to note that forward changes may be
 *	synced here.
 *
 *	What that means is that changes which are not part of this XID
 *	session may have snuck in here (i.e. a CRC update came in and
 *	was fully processed before mds_bmap_sync() grabbed the lock.
 *	For this reason the CRC updates must be journaled before
 *	manifesting in the bmap cache.  Otherwise, log replays
 *	will look inconsistent.
 */
void
mds_bmap_sync(void *data)
{
	struct bmapc_memb *bmap = data;
	int rc;

	BMAPOD_RDLOCK(bmap_2_bmdsi(bmap));
	psc_crc64_calc(&bmap_2_ondiskcrc(bmap), bmap_2_ondisk(bmap),
	    BMAP_OD_CRCSZ);
	rc = mdsio_bmap_write(bmap);
	if (rc)
		DEBUG_BMAP(PLL_FATAL, bmap, "rc=%d errno=%d sync fail",
			   rc, errno);
	else
		DEBUG_BMAP(PLL_INFO, bmap, "sync ok");

	BMAPOD_ULOCK(bmap_2_bmdsi(bmap));

	if (fcmh_2_inoh(bmap->bcm_fcmh)->inoh_flags & INOH_INO_DIRTY ||
	    fcmh_2_inoh(bmap->bcm_fcmh)->inoh_flags & INOH_EXTRAS_DIRTY)
		mds_inode_sync(fcmh_2_inoh(bmap->bcm_fcmh));

	bmap_op_done_type(bmap, BMAP_OPCNT_MDSLOG);
}

void
mds_inode_addrepl_log(void *datap, uint64_t txg)
{
	struct slmds_jent_ino_addrepl *jrir, *r;

	jrir = pjournal_get_buf(mdsJournal,
	    sizeof(struct slmds_jent_ino_addrepl));

	r = datap;
	jrir->sjir_fid = r->sjir_fid;
	jrir->sjir_ios = r->sjir_ios;
	jrir->sjir_pos = r->sjir_pos;
	jrir->sjir_nrepls = r->sjir_nrepls;

	psclog_trace("jlog fid="SLPRI_FID" ios=%u pos=%u",
	    jrir->sjir_fid, jrir->sjir_ios, jrir->sjir_pos);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_INO_ADDREPL,
	    0, jrir, sizeof(struct slmds_jent_ino_addrepl));

	pjournal_put_buf(mdsJournal, jrir);
}

/**
 * mds_bmap_repl_log - Write a modified replication table to the
 *	journal.
 * Note:  bmap must be locked to prevent further changes from sneaking
 *	in before the repl table is committed to the journal.
 */
void
mds_bmap_repl_log(void *datap, uint64_t txg)
{
	struct bmapc_memb *bmap = datap;
	struct slmds_jent_repgen *jrpg;

	jrpg = pjournal_get_buf(mdsJournal,
	    sizeof(struct slmds_jent_repgen));

	jrpg->sjp_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jrpg->sjp_bmapno = bmap->bcm_bmapno;
	jrpg->sjp_bgen = bmap_2_bgen(bmap);

	memcpy(jrpg->sjp_reptbl, bmap->bcm_repls, SL_REPLICA_NBYTES);

	psclog_trace("jlog fid="SLPRI_FID" bmapno=%u bmapgen=%u",
	    jrpg->sjp_fid, jrpg->sjp_bmapno, jrpg->sjp_bgen);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_REPL,
	    0, jrpg, sizeof(struct slmds_jent_repgen));

	pjournal_put_buf(mdsJournal, jrpg);
}

/**
 * mds_bmap_crc_log - Commit bmap CRC changes to the journal.
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
mds_bmap_crc_log(void *datap, uint64_t txg)
{
	struct sl_mds_crc_log *crclog = datap;
	struct bmapc_memb *bmap = crclog->scl_bmap;
	struct srm_bmap_crcup *crcup = crclog->scl_crcup;
	struct bmap_mds_info *bmdsi = bmap_2_bmdsi(bmap);
	struct slmds_jent_crc *jcrc;
	uint32_t n, t, distill;

	BMAP_LOCK(bmap);
	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);
	BMAP_ULOCK(bmap);

	/*
	 * See if we need to distill the file enlargement information.
	 */
	distill = crcup->extend;
	for (t = 0, n = 0; t < crcup->nups; t += n) {

		n = MIN(SLJ_MDS_NCRCS, (crcup->nups - t));

		jcrc = pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_crc));
		jcrc->sjc_fid = fcmh_2_fid(bmap->bcm_fcmh);
		jcrc->sjc_ion = bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid;
		jcrc->sjc_bmapno = bmap->bcm_bmapno;
		jcrc->sjc_ncrcs = n;
		jcrc->sjc_fsize = crcup->fsize;		/* largest known size */
		jcrc->sjc_extend = distill;
		jcrc->sjc_utimgen = crcup->utimgen;     /* utime generation number */

		memcpy(jcrc->sjc_crc, &crcup->crcs[t],
		    n * sizeof(struct srm_bmap_crcwire));

		pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_CRC,
		    distill, jcrc, sizeof(struct slmds_jent_crc));

		if (!distill)
			pjournal_put_buf(mdsJournal, jcrc);
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
mds_journal_init(int disable_propagation)
{
	uint64_t batchno, last_reclaim_xid = 0, last_update_xid = 0, last_distill_xid = 0;
	int i, ri, rc, nios, count, total, npeers;
	struct srt_reclaim_entry *reclaim_entryp;
	struct srt_update_entry *update_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_iosinfo *iosinfo;
	struct sl_resource *res;
	struct sl_resm *resm;
	size_t size;
	void *handle;

	psc_assert(sizeof(struct srt_update_entry) == 512);
	psc_assert(sizeof(struct srt_reclaim_entry) == 512);

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri)
		if (res->res_type != SLREST_MDS)
			nios++;
	if (!nios)
		psc_fatalx("Missing I/O servers at site %s",
		    nodeSite->site_name);

	/* Count the number of peer MDSes we have */
	npeers = 0;
	SL_MDS_WALK(resm, npeers++);
	npeers--;

	if (disable_propagation)
		npeers = 0;

	mds_open_cursor();

	/*
	 * We need the cursor thread to start any potential log replay.
	 * Also, without this thread, we can't write anything into ZFS.
	 * We can't even read from ZFS because a read changes atime.
	 */
	pscthr_init(SLMTHRT_CURSOR, 0, mds_cursor_thread, NULL, 0,
	    "slmjcursorthr");

	xmkfn(update_progfile_name, "%s.%s.%lu",
	    SL_FN_RECLAIMPROG, psc_get_hostname(),
	    mds_cursor.pjc_timestamp);

	rc = mds_open_file(update_progfile_name, O_RDWR,
	    &reclaim_progfile_handle);
	psc_assert(rc == 0);

	reclaim_prog_buf = PSCALLOC(MAX_RECLAIM_PROG_ENTRY *
	    sizeof(struct reclaim_prog_entry));
	rc = mds_read_file(reclaim_progfile_handle, reclaim_prog_buf,
	    MAX_RECLAIM_PROG_ENTRY * sizeof(struct reclaim_prog_entry),
	    &size, 0);
	psc_assert(rc == 0);

	count = size / sizeof(struct reclaim_prog_entry);
	for (i = 0; i < count; i++) {
		if (reclaim_prog_buf[i].res_id == 0)
			continue;
		res = libsl_id2res(reclaim_prog_buf[i].res_id);
		iosinfo = res2rpmi(res)->rpmi_info;
		if (iosinfo->si_xid < reclaim_prog_buf[i].res_xid)
			iosinfo->si_xid = reclaim_prog_buf[i].res_xid;
		if (iosinfo->si_batchno < reclaim_prog_buf[i].res_batchno)
			iosinfo->si_batchno = reclaim_prog_buf[i].res_batchno;
	}
	PSCFREE(reclaim_prog_buf);
	reclaim_prog_buf = PSCALLOC(nios * sizeof(struct reclaim_prog_entry));

	/* Find out the highest reclaim batchno and xid */
	batchno = mds_reclaim_lwm(1);
	rc = mds_open_logfile(batchno, 0, 1, &handle);
	if (rc) {
		if (batchno) {
			batchno--;
			rc = mds_open_logfile(batchno, 0, 1, &handle);
		}
	}
	if (rc)
		psc_fatal("Failed to open reclaim log file, batchno=%"PRId64, batchno);

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

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, 0, mds_send_reclaim, NULL, 0,
	    "slmjreclaimthr");

	/* We are done if we don't have any peer MDSes */
	if (!npeers)
		goto replay_log;

	xmkfn(reclaim_progfile_name, "%s.%s.%lu",
	    SL_FN_RECLAIMPROG, psc_get_hostname(),
	    mds_cursor.pjc_timestamp);

	rc = mds_open_file(reclaim_progfile_name, O_RDWR,
	    &update_progfile_handle);
	psc_assert(rc == 0);

	update_prog_buf = PSCALLOC(MAX_UPDATE_PROG_ENTRY * sizeof(struct
	    update_prog_entry));
	rc = mds_read_file(update_progfile_handle, update_prog_buf,
	    MAX_UPDATE_PROG_ENTRY * sizeof(struct update_prog_entry),
	    &size, 0);
	psc_assert(rc == 0);

	count = size / sizeof(struct update_prog_entry);
	for (i = 0; i < count; i++) {
		if (update_prog_buf[i].res_id == 0)
			continue;
		res = libsl_id2res(update_prog_buf[i].res_id);
		peerinfo = res2rpmi(res)->rpmi_info;
		if (peerinfo->sp_xid < update_prog_buf[i].res_xid)
			peerinfo->sp_xid = update_prog_buf[i].res_xid;
		if (peerinfo->sp_batchno < update_prog_buf[i].res_batchno)
			peerinfo->sp_batchno = update_prog_buf[i].res_batchno;
	}
	PSCFREE(update_prog_buf);
	update_prog_buf = PSCALLOC(npeers * sizeof(struct update_prog_entry));

	/* Find out the highest update batchno and xid */
	batchno = mds_update_lwm(1);
	rc = mds_open_logfile(batchno, 1, 1, &handle);
	if (rc) {
		if (batchno) {
			batchno--;
			rc = mds_open_logfile(batchno, 1, 1, &handle);
		}
	}
	if (rc)
		psc_fatal("Failed to open update log file, batchno=%"PRId64, batchno);

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

	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	pscthr_init(SLMTHRT_JNAMESPACE, 0, mds_send_update, NULL,
	    0, "slmjnsthr");

 replay_log:

	res = nodeResm->resm_res;
	if (res->res_jrnldev[0] == '\0')
		xmkfn(res->res_jrnldev, "%s/%s", sl_datadir,
		    SL_FN_OPJOURNAL);

	mdsJournal = pjournal_open(res->res_jrnldev);
	if (mdsJournal == NULL)
		psc_fatal("Failed to open log file %s", res->res_jrnldev);

	mdsJournal->pj_npeers = npeers;
	mdsJournal->pj_distill_xid = last_distill_xid;
	mdsJournal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	mdsJournal->pj_replay_xid = mds_cursor.pjc_replay_xid;

	psclog_notice("Journal device is %s", res->res_jrnldev);
	psclog_notice("Last SLASH FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psclog_notice("Last synced ZFS transaction group number is %"PRId64,
	    mdsJournal->pj_commit_txg);
	psclog_notice("Last replayed SLASH2 transaction number is %"PRId64,
	    mdsJournal->pj_replay_xid);

	pjournal_replay(mdsJournal, SLMTHRT_JRNL, "slmjthr",
	    mds_replay_handler, mds_distill_handler);

	mds_bmap_setcurseq(mds_cursor.pjc_seqno_hwm, mds_cursor.pjc_seqno_lwm);
	psclog_notice("Last bmap sequence number low water mark is %"PRId64,
	    mds_cursor.pjc_seqno_lwm);
	psclog_notice("Last bmap sequence number high water mark is %"PRId64,
	    mds_cursor.pjc_seqno_hwm);
}

void
mds_reserve_slot(void)
{
	pjournal_reserve_slot(mdsJournal);
}

void
mds_unreserve_slot(void)
{
	pjournal_unreserve_slot(mdsJournal);
}

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
