/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

psc_spinlock_t			 mds_update_lock = SPINLOCK_INIT;
psc_spinlock_t			 mds_reclaim_lock = SPINLOCK_INIT;

uint64_t			 current_update_xid = 0;
uint64_t			 current_reclaim_xid = 0;

static int			 current_update_logfile = -1;
static int			 current_reclaim_logfile = -1;

static int			 current_update_progfile[2];
static int			 current_reclaim_progfile[2];

/* namespace update progress tracker to peer MDSes */
struct update_prog_entry {
	sl_ios_id_t		 res_id;
	enum sl_res_type	 res_type;
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
};

/* garbage reclaim progress tracker to IOSes */
struct reclaim_prog_entry {
	sl_ios_id_t		 res_id;
	enum sl_res_type	 res_type;
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
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

static void
mds_record_update_prog(void)
{
	int i;
	ssize_t size;
	struct sl_resm *resm;
	struct resprof_mds_info *rpmi;
	struct sl_mds_peerinfo *peerinfo;
	static int index = 0;

	i = 0;
	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		rpmi = res2rpmi(_res);
		peerinfo = rpmi->rpmi_info;
		update_prog_buf[i].res_id = _res->res_id;
		update_prog_buf[i].res_type = _res->res_type;
		update_prog_buf[i].res_xid = peerinfo->sp_xid;
		update_prog_buf[i].res_batchno = peerinfo->sp_batchno;
		i++;
	);
	if (lseek(current_update_progfile[index], 0, SEEK_SET) == (off_t)-1)
		psc_warn("lseek");
	size = write(current_update_progfile[index], update_prog_buf,
	    i * sizeof(struct update_prog_entry));
	psc_assert(size == i * (int)sizeof(struct update_prog_entry));
	index = (index == 0) ? 1 : 0;
}

static void
mds_record_reclaim_prog(void)
{
	int i, ri;
	ssize_t size;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *iosinfo;
	struct sl_resource *res;
	static int index = 0;

	i = 0;
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (res->res_type == SLREST_MDS)
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;
		reclaim_prog_buf[i].res_id = res->res_id;
		reclaim_prog_buf[i].res_type = res->res_type;
		reclaim_prog_buf[i].res_xid = iosinfo->si_xid;
		reclaim_prog_buf[i].res_batchno = iosinfo->si_batchno;
		i++;
	}
	if (lseek(current_reclaim_progfile[index], 0, SEEK_SET) == (off_t)-1)
		psc_warn("lseek");
	size = write(current_reclaim_progfile[index], reclaim_prog_buf,
	    i * sizeof(struct reclaim_prog_entry));
	psc_assert(size == i * (int)sizeof(struct reclaim_prog_entry));
	index = (index == 0) ? 1 : 0;
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
			psc_warnx("mdsio_lookup_slfid: %s", slstrerror(rc));
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

	psc_info("pje_xid=%"PRIx64" pje_txg=%"PRIx64" fid="SLPRI_FID" "
	    "bmapno=%u ncrcs=%d crc[0]=%"PSCPRIxCRC64,
	    pje->pje_xid, pje->pje_txg, jcrc->sjc_fid, jcrc->sjc_bmapno,
	    jcrc->sjc_ncrcs, jcrc->sjc_crc[0].crc);

	rc = mdsio_lookup_slfid(jcrc->sjc_fid, &rootcreds, NULL, &mf);
	if (rc == ENOENT) {
		psc_warnx("mdsio_lookup_slfid: %s", slstrerror(rc));
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

		rc = mdsio_read(&rootcreds, &inoh_extras, INOX_OD_SZ, &nb,
		    SL_EXTRAS_START_OFF, mdsio_data);
		if (rc)
			goto out;

		j = pos - SL_DEF_REPLICAS;
		inoh_extras.inox_repls[j].bs_id = jrir->sjir_ios;
		psc_crc64_calc(&inoh_extras.inox_crc, &inoh_extras, INOX_OD_CRCSZ);

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
	struct slmds_jent_namespace *jnamespace;
	int rc = 0;

	psc_info("pje=%p pje_xid=%"PRIx64" pje_txg=%"PRIx64,
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
		jnamespace = PJE_DATA(pje);
		psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);
		rc = mds_redo_namespace(jnamespace);
		/*
		 * If we fail above, we still skip these SLASH2 FIDs here
		 * in case a client gets confused.
		 */
		if (jnamespace->sjnm_op == NS_OP_CREATE ||
		    jnamespace->sjnm_op == NS_OP_MKDIR ||
		    jnamespace->sjnm_op == NS_OP_LINK ||
		    jnamespace->sjnm_op == NS_OP_SYMLINK)
			slm_get_next_slashid();
		break;
	    default:
		psc_fatal("invalid log entry type %d", pje->pje_type);
	}
	return (rc);
}

void
mds_remove_logfile(uint64_t batchno, int update)
{
	char log_fn[PATH_MAX];

	if (update) {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_UPDATELOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	} else {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_RECLAIMLOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	}
	unlink(log_fn);
}

int
mds_open_logfile(uint64_t batchno, int update, int readonly)
{
	char log_fn[PATH_MAX];
	int logfile;

	if (update) {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_UPDATELOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	} else {
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_RECLAIMLOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	}
	if (readonly) {
		/*
		 * The caller should check the return value.
		 */
		logfile = open(log_fn, O_RDONLY);
		return logfile;
	}

	/*
	 * Note we use different file descriptors for read and write.
	 * Luckily, Linux maintains the file offset independently for
	 * each open.
	 *
	 * During replay, we need to read the file first to find out
	 * the right position, so we can't use O_WRONLY.
	 */
	logfile = open(log_fn, O_RDWR | O_SYNC);
	if (logfile > 0) {
		/*
		 * During replay, the offset will be determined by the
		 * xid.
		 */
		if (lseek(logfile, 0, SEEK_END) == (off_t)-1)
			psc_warn("lseek");
		return (logfile);
	}
	logfile = open(log_fn, O_CREAT | O_TRUNC | O_WRONLY | O_SYNC, 0600);
	if (logfile == -1)
		psc_fatal("Failed to create log file %s", log_fn);
	return (logfile);
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
mds_distill_handler(struct psc_journal_enthdr *pje, int npeers,
    int replay)
{
	int size, count, total;
	struct slmds_jent_namespace *jnamespace;
	struct srt_update_entry update_entry, *update_entryp;
	struct srt_reclaim_entry reclaim_entry, *reclaim_entryp;
	off_t off;

	psc_assert(pje->pje_magic == PJE_MAGIC);
	if (!(pje->pje_type & MDS_LOG_NAMESPACE))
		return (0);

	jnamespace = PJE_DATA(pje);
	psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);

	/*
	 * Note that distill reclaim before update.  This is the same
	 * order we use in recovery.
	 */

	/*
	 * If the namespace operation needs to reclaim disk space on I/O
	 * servers, write the information into the reclaim log.
	 */
	if (!(jnamespace->sjnm_flag & SJ_NAMESPACE_RECLAIM))
		goto check_update;

	psc_assert(jnamespace->sjnm_op == NS_OP_SETATTR ||
	    jnamespace->sjnm_op == NS_OP_UNLINK ||
	    jnamespace->sjnm_op == NS_OP_SETSIZE);

	if (current_reclaim_logfile == -1) {
		current_reclaim_logfile =
		    mds_open_logfile(current_reclaim_batchno, 0, 0);

		/*
		 * Here we do one-time seek based on the xid stored in
		 * the entry.  Although not necessary contiguous, xids
		 * are in increasing order.
		 */
		if (replay) {
			size = read(current_reclaim_logfile, reclaimbuf,
			    SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry));
			if (size == -1)
			    psc_fatal("Fail to read reclaim log file, batchno = %"PRId64,
				current_reclaim_batchno);
			total = size / sizeof(struct srt_reclaim_entry);

			count = 0;
			reclaim_entryp = reclaimbuf;
			while (count < total) {
				if (reclaim_entryp->xid == pje->pje_xid) {
					psc_warnx("Reclaim distill %"PRId64, pje->pje_xid);
					break;
				}
				reclaim_entryp++;
				count++;
			}
			/*
			 * If we didn't find the entry, this is
			 * seek-to-end.  If we do find it, we will
			 * distill again (overwrite should be fine).
			 */
			if (lseek(current_reclaim_logfile,
			    count * sizeof(struct srt_reclaim_entry),
			    SEEK_CUR) == (off_t)-1)
				psc_warn("lseek");
		}
	}

	spinlock(&mds_reclaim_lock);
	current_reclaim_xid = pje->pje_xid;
	freelock(&mds_reclaim_lock);

	reclaim_entry.xid = pje->pje_xid;
	reclaim_entry.fg.fg_fid = jnamespace->sjnm_target_fid;
	reclaim_entry.fg.fg_gen = jnamespace->sjnm_target_gen;

	size = write(current_reclaim_logfile, &reclaim_entry,
	    sizeof(struct srt_reclaim_entry));
	if (size != sizeof(struct srt_reclaim_entry))
		psc_fatal("Fail to write reclaim log file, batchno = %"PRId64,
		    current_reclaim_batchno);

	/* see if we need to close the current reclaim log file */
	off = lseek(current_reclaim_logfile, 0, SEEK_CUR);
	if (off == (off_t)-1)
		psc_warn("lseek");
	else if (off == SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry)) {
		close(current_reclaim_logfile);
		current_reclaim_logfile = -1;
		current_reclaim_batchno++;

		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_wakeall(&mds_reclaim_waitq);
		freelock(&mds_reclaim_waitqlock);
	}

 check_update:

	if (!npeers)
		return (0);

	if (current_update_logfile == -1) {
		current_update_logfile =
		    mds_open_logfile(current_update_batchno, 1, 0);
		if (replay) {
			size = read(current_update_logfile, updatebuf,
			    SLM_UPDATE_BATCH * sizeof(struct srt_update_entry));
			if (size == -1)
			    psc_fatal("Fail to read update log file, batchno = %"PRId64,
				current_update_batchno);
			total = size / sizeof(struct srt_update_entry);

			count = 0;
			update_entryp = updatebuf;
			while (count < total) {
				if (update_entryp->xid == pje->pje_xid) {
					psc_warnx("Update distill %"PRId64, pje->pje_xid);
					break;
				}
				update_entryp++;
				count++;
			}
			/*
			 * If we didn't find the entry, this is
			 * seek-to-end.  If we do find it, we will
			 * distill again (overwrite should be fine).
			 */
			if (lseek(current_update_logfile,
			    count * sizeof(struct srt_update_entry),
			    SEEK_CUR) == (off_t)-1)
				psc_warn("lseek");
		}
	}

	spinlock(&mds_update_lock);
	current_update_xid = pje->pje_xid;
	freelock(&mds_update_lock);

	update_entry.xid = pje->pje_xid;
	update_entry.op = jnamespace->sjnm_op;
	update_entry.target_gen = jnamespace->sjnm_target_gen;
	update_entry.parent_fid = jnamespace->sjnm_parent_fid;
	update_entry.target_fid = jnamespace->sjnm_target_fid;
	update_entry.new_parent_fid = jnamespace->sjnm_new_parent_fid;

	update_entry.uid = jnamespace->sjnm_uid;
	update_entry.gid = jnamespace->sjnm_gid;
	update_entry.atime = jnamespace->sjnm_atime;
	update_entry.mtime = jnamespace->sjnm_mtime;
	update_entry.ctime = jnamespace->sjnm_ctime;
	update_entry.atime_ns = jnamespace->sjnm_atime_ns;
	update_entry.mtime_ns = jnamespace->sjnm_mtime_ns;
	update_entry.ctime_ns = jnamespace->sjnm_ctime_ns;

	update_entry.namelen = jnamespace->sjnm_namelen;
	memcpy(update_entry.name, jnamespace->sjnm_name, jnamespace->sjnm_namelen);

	size = write(current_update_logfile, &update_entry,
	    sizeof(struct srt_update_entry));
	if (size != sizeof(struct srt_update_entry))
		psc_fatal("Fail to write update log file, batchno = %"PRId64,
		    current_update_batchno);

	/* see if we need to close the current update log file */
	off = lseek(current_update_logfile, 0, SEEK_CUR);
	if (off == (off_t)-1)
		psc_warn("lseek");
	else if (off == SLM_UPDATE_BATCH * sizeof(struct srt_update_entry)) {
		close(current_update_logfile);
		current_update_logfile = -1;
		current_update_batchno++;

		spinlock(&mds_update_waitqlock);
		psc_waitq_wakeall(&mds_update_waitq);
		freelock(&mds_update_waitqlock);
	}
	return (0);
}

/**
 * mds_namespace_log - Log namespace operation before we attempt an
 *	operation.  This makes sure that it will be propagated towards
 *	other MDSes and made permanent before we reply to the client.
 */
void
mds_namespace_log(int op, uint64_t txg, uint64_t parent,
    uint64_t newparent, const struct srt_stat *sstb, int mask,
    const char *name, const char *newname)
{
	struct slmds_jent_namespace *jnamespace;
	size_t rem, len;
	char *ptr;

	jnamespace = pjournal_get_buf(mdsJournal,
	    sizeof(struct slmds_jent_namespace));
	jnamespace->sjnm_magic = SJ_NAMESPACE_MAGIC;
	jnamespace->sjnm_op = op;
	jnamespace->sjnm_parent_fid = parent;
	jnamespace->sjnm_target_fid = sstb->sst_fid;
	jnamespace->sjnm_new_parent_fid = newparent;
	jnamespace->sjnm_mask = mask;

	jnamespace->sjnm_uid = sstb->sst_uid;
	jnamespace->sjnm_gid = sstb->sst_gid;
	jnamespace->sjnm_mode = sstb->sst_mode;
	jnamespace->sjnm_atime = sstb->sst_atime;
	jnamespace->sjnm_atime_ns = sstb->sst_atime_ns;
	jnamespace->sjnm_mtime = sstb->sst_mtime;
	jnamespace->sjnm_mtime_ns = sstb->sst_mtime_ns;
	jnamespace->sjnm_ctime = sstb->sst_ctime;
	jnamespace->sjnm_ctime_ns = sstb->sst_ctime_ns;
	jnamespace->sjnm_size = sstb->sst_size;

	jnamespace->sjnm_flag = 0;
	if ((op == NS_OP_UNLINK && sstb->sst_nlink == 1) ||
	    (op == NS_OP_SETSIZE && sstb->sst_size == 0)) {
		/*
		 * We want to reclaim the space taken by the previous
		 * generation.  Note that changing the attributes of a
		 * zero-lengh file should NOT trigger this code.
		 */
		jnamespace->sjnm_flag |= SJ_NAMESPACE_RECLAIM;
		jnamespace->sjnm_target_gen = sstb->sst_gen;
		if (op == NS_OP_SETSIZE) {
			psc_assert(sstb->sst_gen >= 1);
			jnamespace->sjnm_target_gen--;
		}
	}

	jnamespace->sjnm_namelen = 0;
	ptr = jnamespace->sjnm_name;
	*ptr = '\0';
	rem = sizeof(jnamespace->sjnm_name);
	if (name) {
		psc_assert(rem >= strlen(name) + 1);
		strlcpy(ptr, name, MIN(rem - 1, SL_NAME_MAX + 1));
		len = strlen(ptr) + 1;
		jnamespace->sjnm_namelen += len;
		ptr += len;
		rem -= len;
	}
	if (newname) {
		psc_assert(rem >= strlen(newname) + 1);
		strlcpy(ptr, newname, MIN(rem - 1, SL_NAME_MAX + 1));
		len = strlen(ptr) + 1;
		jnamespace->sjnm_namelen += len;
	}

	pjournal_add_entry_distill(mdsJournal, txg,
	    MDS_LOG_NAMESPACE, jnamespace,
	    offsetof(struct slmds_jent_namespace, sjnm_name) +
	    jnamespace->sjnm_namelen);
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
	uint64_t value = 0;
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
mds_update_lwm(void)
{
	uint64_t batchno = UINT64_MAX;
	struct sl_mds_peerinfo *peerinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;

	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (peerinfo->sp_batchno < batchno)
			batchno = peerinfo->sp_batchno;
		RPMI_ULOCK(rpmi);
	);

	psc_assert(batchno != UINT64_MAX);

	return (batchno);
}

__static uint64_t
mds_update_hwm(void)
{
	uint64_t batchno = 0;
	struct sl_mds_peerinfo *peerinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;

	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (peerinfo->sp_batchno > batchno)
			batchno = peerinfo->sp_batchno;
		RPMI_ULOCK(rpmi);
	);

	psc_assert(batchno != UINT64_MAX);

	return (batchno);
}

/**
 * mds_send_batch_update - Send a batch of updates to peer MDSes
 *	that want them.
 */
int
mds_send_batch_update(uint64_t batchno)
{
	struct srt_update_entry *entryp, *next_entryp;
	struct srm_update_req *mq;
	struct slashrpc_cservice *csvc;
	struct pscrpc_bulk_desc *desc;
	struct sl_mds_peerinfo *peerinfo;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct sl_resm *resm;
	struct iovec iov;
	int i, rc, len, npeers, count, total, didwork=0;
	uint64_t first_xid = 0, last_xid = 0;
	int logfile, record = 0;
	ssize_t size;

	logfile = mds_open_logfile(batchno, 1, 1);
	if (logfile == -1)
		psc_fatal("Fail to open update log file, batch = %"PRId64, batchno);
	size = read(logfile, updatebuf, SLM_UPDATE_BATCH *
	    (sizeof(struct srt_update_entry) + NAME_MAX));
	close(logfile);

	psc_assert((size % sizeof(struct srt_update_entry)) == 0);
	count = (int) size / (int) sizeof(struct srt_update_entry);

	/*
 	 * Compress our buffer to reduce RPC traffic.
 	 */
	entryp = next_entryp = updatebuf;
	for (i = 1; i < count; i++) {

		if (!first_xid)
			first_xid = entryp->xid;
		last_xid = entryp->xid;

		entryp++;
		len = offsetof(struct srt_update_entry, _padding) + next_entryp->namelen;
		next_entryp = PSC_AGP(next_entryp, len);

		len = offsetof(struct srt_update_entry, _padding) + entryp->namelen;
		memmove(next_entryp, entryp, len);
	}

	npeers= 0;
	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		npeers++;
		peerinfo = resm2rpmi(resm)->rpmi_info;

		/*
		 * Skip if the MDS is busy or the current batch is out
		 * of its windows.  Note for each MDS, we send updates
		 * in order.
		 */
		if (peerinfo->sp_flags & SP_FLAG_MIA)
			continue;
		if (peerinfo->sp_flags & SP_FLAG_INFLIGHT)
			continue;

		if (peerinfo->sp_batchno < batchno)
			continue;
		if (peerinfo->sp_batchno > batchno)
			continue;
		if (peerinfo->sp_xid < first_xid || peerinfo->sp_xid > last_xid)
			continue;

		/* Find out which part of the buffer should be send out */
		i = count;
		total = size;
		entryp = updatebuf;
		do {
			if (entryp->xid >= peerinfo->sp_xid)
				break;
			i--;
			len = sizeof(struct srt_update_entry) + entryp->namelen;
			total -= len;
			entryp = PSC_AGP(entryp, len);
		} while (total);

		psc_assert(total);

		iov.iov_len = total;
		iov.iov_base = entryp;

		csvc = slm_getmcsvc(resm);
		if (csvc == NULL) {
			/*
			 * A simplistic way to avoid CPU spinning.
			 * A better way is to let the ping thread handle
			 * this.
			 */
			peerinfo->sp_flags |= SP_FLAG_MIA;
			continue;
		}
		rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMM_VERSION,
		    SRMT_NAMESPACE_UPDATE, rq, mq, mp);
		if (rc) {
			sl_csvc_decref(csvc);
			continue;
		}
		mq->count = i;
		mq->size = iov.iov_len;
		mq->siteid = nodeSite->site_id;
		psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

		rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
		    SRMM_BULK_PORTAL, &iov, 1);

		rc = SL_RSX_WAITREP(rq, mp);
		pscrpc_req_finished(rq);
		rq = NULL;

		if (rc == 0)
			rc = mp->rc;
		if (rc == 0) {
			record++;
			didwork++;
			peerinfo->sp_xid = last_xid + 1;
			if (count == SLM_UPDATE_BATCH)
				peerinfo->sp_batchno++;
		}
	);
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
 * mds_update_cursor - write some system information into our cursor
 *	file.  Note that every field must be protected by a spinlock.
 */
void
mds_update_cursor(void *buf, uint64_t txg)
{
	struct psc_journal_cursor *cursor = buf;
	int rc;

	spinlock(&mds_txg_lock);
	cursor->pjc_commit_txg = txg;
	freelock(&mds_txg_lock);

	/*
	 * Distill happens outside ZFS.  This means if there is no ZFS
	 * activity, the following value will be stale.
	 */
	cursor->pjc_distill_xid = pjournal_next_distill(mdsJournal);
	cursor->pjc_fid = slm_get_curr_slashid();

	/* to be removed */
	cursor->pjc_update_seqno = -1;
	cursor->pjc_reclaim_seqno = -1;

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
			psc_warnx("failed to update cursor, rc = %d", rc);
		else
			psclog_notice("Cursor updated: txg=%"PRId64", xid=%"PRId64
			    ", fid=0x%"PRIx64", seqno=(%"PRId64", %"PRId64")",
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
	psclog_notice("File system was formated on %"PRIu64" seconds "
	    "since the Epoch", mds_cursor.pjc_timestamp);
	psclog_notice("File system was formated on %s",
	    ctime((time_t *)&mds_cursor.pjc_timestamp));
}

int
mds_send_batch_reclaim(uint64_t batchno)
{
	int i, ri, rc, len, count, nentry, total, nios, didwork;
	struct pscrpc_request *rq = NULL;
	struct srt_reclaim_entry *entryp, *next_entryp;
	struct slashrpc_cservice *csvc;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct pscrpc_bulk_desc *desc;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct sl_resm *dst_resm;
	struct sl_resource *res;
	struct iovec iov;
	uint64_t xid;
	int logfile, record = 0;
	ssize_t size;

	didwork = 0;

	logfile = mds_open_logfile(batchno, 0, 1);
	if (logfile == -1)
	    psc_fatal("Fail to open reclaim log file, batch = %"PRId64, batchno);

	size = read(logfile, reclaimbuf, SLM_RECLAIM_BATCH *
	    sizeof(struct srt_reclaim_entry));
	psc_assert(size >= 0);

	close(logfile);
	if (size == 0)
		return (didwork);

	/*
	 * Short read is Okay, as long as it is a multiple of the basic
	 * data structure.
	 */
	psc_assert((size % sizeof(struct srt_reclaim_entry)) == 0);
	count = (int) size / (int) sizeof(struct srt_reclaim_entry);

	/* find the xid associated with the last log entry */
	entryp = PSC_AGP(reclaimbuf, (count - 1) *
	    sizeof(struct srt_reclaim_entry));
	xid = entryp->xid;

	/*
 	 * Compress our buffer to reduce RPC traffic.
 	 */
	entryp = next_entryp = reclaimbuf;
	len = offsetof(struct srt_reclaim_entry, _padding);
	for (i = 1; i < count; i++) {
		entryp++;
		next_entryp = PSC_AGP(next_entryp, len);
		memmove(next_entryp, entryp, len);
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
		if (iosinfo->si_batchno > batchno || iosinfo->si_xid >= xid) {
			RPMI_ULOCK(rpmi);
			didwork++;
			continue;
		}

		RPMI_ULOCK(rpmi);

		/* Find out which part of the buffer should be send out */
		i = count;
		total = size;
		entryp = reclaimbuf;
		do {
			if (entryp->xid >= iosinfo->si_xid)
				break;
			i--;
			len = sizeof(struct srt_reclaim_entry);
			total -= len;
			entryp = PSC_AGP(entryp, len);
		} while (total);

		psc_assert(total);

		nentry = i;
		iov.iov_len = total;
		iov.iov_base = entryp;

		/*
		 * Send RPC to the I/O server and wait for it to complete.
		 */
		DYNARRAY_FOREACH(dst_resm, i, &res->res_members) {
			csvc = slm_geticsvc_nb(dst_resm, NULL);
			if (csvc == NULL)
				continue;
			rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMM_VERSION,
				SRMT_RECLAIM, rq, mq, mp);
			if (rc) {
				sl_csvc_decref(csvc);
				continue;
			}

			mq->xid = xid;
			mq->size = iov.iov_len;
			mq->count = nentry;
			psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

			rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
			    SRMM_BULK_PORTAL, &iov, 1);

			rc = SL_RSX_WAITREP(rq, mp);

			pscrpc_req_finished(rq);
			rq = NULL;

			sl_csvc_decref(csvc);
			if (rc == 0)
				rc = mp->rc;
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
		if (batchno >= 1 )
			mds_remove_logfile(batchno-1, 0);
	}
	return (didwork);
}

/**
 * mds_send_reclaim - Send garbage collection to I/O servers.
 */
void
mds_send_reclaim(__unusedx struct psc_thread *thr)
{
	int rv, didwork;
	uint64_t batchno;

	/*
	 * Instead of tracking precisely which reclaim log record has
	 * been sent to an I/O node, we track the batch number.  A
	 * receiving I/O node can safely ignore any resent records.
	 */
	while (pscthr_run()) {

		batchno = mds_reclaim_lwm(1);
		do {
			spinlock(&mds_reclaim_lock);
			if (mds_reclaim_lwm(0) > current_reclaim_xid) {
				freelock(&mds_reclaim_lock);
				break;
			}
			freelock(&mds_reclaim_lock);
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
		batchno = mds_update_lwm();
		do {
			didwork = mds_send_batch_update(batchno);
			batchno++;
		} while (didwork && (mds_update_hwm() >= batchno));

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

	psc_trace("jlog fid=%"PRIx64" ios=%u pos=%u",
	    jrir->sjir_fid, jrir->sjir_ios, jrir->sjir_pos);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_INO_ADDREPL,
	    jrir, sizeof(struct slmds_jent_ino_addrepl));

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

	psc_trace("jlog fid=%"PRIx64" bmapno=%u bmapgen=%u",
	    jrpg->sjp_fid, jrpg->sjp_bmapno, jrpg->sjp_bgen);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_REPL,
	    jrpg, sizeof(struct slmds_jent_repgen));

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
	uint32_t n, t;

	/*
	 * No, I shouldn't need the lock.  Only this instance of this
	 *  call may remove the BMAP_MDS_CRC_UP bit.
	 */
	psc_assert(bmap->bcm_flags & BMAP_MDS_CRC_UP);

	jcrc = pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_crc));
	jcrc->sjc_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jcrc->sjc_ion = bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid;
	jcrc->sjc_bmapno = bmap->bcm_bmapno;
	jcrc->sjc_ncrcs = crcup->nups;
	jcrc->sjc_fsize = crcup->fsize;		/* largest known size */
	jcrc->sjc_utimgen = crcup->utimgen;     /* utime generation number */

	for (t = 0, n = 0; t < crcup->nups; t += n) {
		n = MIN(SLJ_MDS_NCRCS, (crcup->nups - t));

		memcpy(jcrc->sjc_crc, &crcup->crcs[t],
		    n * sizeof(struct srm_bmap_crcwire));

		pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_CRC,
		    jcrc, sizeof(struct slmds_jent_crc));
	}

	psc_assert(t == crcup->nups);
	/* Signify that the update has occurred.
	 */
	BMAP_LOCK(bmap);
	bmap->bcm_flags &= ~BMAP_MDS_CRC_UP;
	BMAP_ULOCK(bmap);

	pjournal_put_buf(mdsJournal, jcrc);
}

void
mds_journal_init(void)
{
	uint64_t batchno, last_reclaim_xid = 0, last_update_xid = 0, last_distill_xid = 0;
	int i, ri, len, nios, count, total, found, npeers, index, logfile;
	struct srt_reclaim_entry *reclaim_entryp;
	struct srt_update_entry *update_entryp;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_iosinfo *iosinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;
	struct sl_resm *resm;
	struct stat sb;
	char fn[PATH_MAX];
	ssize_t size;
	
	psc_assert(sizeof(struct srt_update_entry) == 512);
	psc_assert(sizeof(struct srt_reclaim_entry) == 512);

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri)
		if (res->res_type != SLREST_MDS)
			nios++;
	if (!nios)
		psc_fatalx("Missing I/O servers at site %s", nodeSite->site_name);

	/* Count the number of peer MDSes we have */
	npeers = 0;
	SL_FOREACH_MDS(resm, npeers++)
		;
	npeers--;

	mds_open_cursor();

	for (index = 0; index < 2; index++) {
		xmkfn(fn, "%s/%s.%s.%lu.%d", SL_PATH_DATADIR, SL_FN_RECLAIMPROG,
		    psc_get_hostname(), mds_cursor.pjc_timestamp, index);
		current_reclaim_progfile[index] = open(fn, O_CREAT | O_RDWR | O_SYNC, 0600);
		if (fstat(current_reclaim_progfile[index], &sb) == -1)
			psc_fatal("Fail to stat reclaim log file %s", fn);
		psc_assert((sb.st_size % sizeof(struct reclaim_prog_entry)) == 0);

		i = count = sb.st_size / sizeof(struct reclaim_prog_entry);
		reclaim_prog_buf = PSCALLOC(i * sizeof(struct reclaim_prog_entry));
		if (count) {
			size = read(current_reclaim_progfile[index], reclaim_prog_buf,
			    count * sizeof(struct reclaim_prog_entry));
			psc_assert(size == count * (int)sizeof(struct reclaim_prog_entry));
		}
		found = 0;
		SITE_FOREACH_RES(nodeSite, res, ri) {
			if (res->res_type == SLREST_MDS)
				continue;
			for (i = 0; i < count; i++) {
				if (reclaim_prog_buf[i].res_id != res->res_id)
					continue;
				if (reclaim_prog_buf[i].res_type != res->res_type)
					continue;
				break;
			}
			if (i >= count)
				continue;
			found++;
			rpmi = res2rpmi(res);
			iosinfo = rpmi->rpmi_info;
			if (iosinfo->si_xid < reclaim_prog_buf[i].res_xid)
			    iosinfo->si_xid = reclaim_prog_buf[i].res_xid;
			if (iosinfo->si_batchno < reclaim_prog_buf[i].res_batchno)
			    iosinfo->si_batchno = reclaim_prog_buf[i].res_batchno;
		}
		PSCFREE(reclaim_prog_buf);
	}

	/* Find out the highest reclaim batchno and xid */
	batchno = mds_reclaim_lwm(1);
	logfile = mds_open_logfile(batchno, 0, 1);
	if (logfile == -1) {
		if (batchno) {
			batchno--;
			logfile = mds_open_logfile(batchno, 0, 1);
		}
	}
	if (logfile == -1)
	    psc_fatal("Fail to open reclaim log file, batch = %"PRId64, batchno);

	current_reclaim_batchno = batchno;
	reclaimbuf = PSCALLOC(SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry));

	size = read(logfile, reclaimbuf,
		SLM_RECLAIM_BATCH * sizeof(struct srt_reclaim_entry));
	psc_assert(size >= 0);
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
	close(logfile);

	last_distill_xid = last_reclaim_xid;

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, 0, mds_send_reclaim, NULL,
	    0, "slmjreclaimthr");

	/* We are done if we don't have any peer MDSes */
	if (!npeers)
		goto replay_log;

	for (index = 0; index < 2; index++) {
		xmkfn(fn, "%s/%s.%s.%lu.%d", SL_PATH_DATADIR, SL_FN_UPDATEPROG,
		    psc_get_hostname(), mds_cursor.pjc_timestamp, index);
		current_update_progfile[index] = open(fn, O_CREAT | O_RDWR | O_SYNC, 0600);
		if (fstat(current_update_progfile[index], &sb) == -1)
			psc_fatal("Fail to stat update log file %s", fn);
		psc_assert((sb.st_size % sizeof(struct update_prog_entry)) == 0);

		i = count = sb.st_size / sizeof(struct update_prog_entry);
		update_prog_buf = PSCALLOC(i * sizeof(struct update_prog_entry));
		if (count) {
			size = read(current_update_progfile[index], update_prog_buf,
			    count * sizeof(struct update_prog_entry));
			psc_assert(size == count * (int)sizeof(struct update_prog_entry));
		}
	
		SL_FOREACH_MDS(resm,
			if (resm == nodeResm)
				continue;
			for (i = 0; i < count; i++) {
				if (update_prog_buf[i].res_id != _res->res_id)
					continue;
				if (update_prog_buf[i].res_type != _res->res_type)
					continue;
				break;
			}
			if (i >= count)
				continue;
			found++;
			rpmi = res2rpmi(_res);
			peerinfo = rpmi->rpmi_info;
			peerinfo->sp_xid = update_prog_buf[i].res_xid;
			peerinfo->sp_batchno = update_prog_buf[i].res_batchno;
		);
		PSCFREE(update_prog_buf);
	}

	/* Find out the highest update batchno and xid */
	batchno = mds_update_lwm();
	logfile = mds_open_logfile(batchno, 1, 1);
	if (logfile == -1) {
		if (batchno) {
			batchno--;
			logfile = mds_open_logfile(batchno, 1, 1);
		}
	}
	if (logfile == -1)
	    psc_fatal("Fail to open update log file, batch = %"PRId64, batchno);

	current_update_batchno = batchno;
	updatebuf = PSCALLOC(SLM_UPDATE_BATCH * (sizeof(struct srt_update_entry) + NAME_MAX));

	size = read(logfile, updatebuf,
	    SLM_UPDATE_BATCH * (sizeof(struct srt_update_entry) + NAME_MAX));
	psc_assert(size >= 0);

	count = 0;
	total = size;
	update_entryp = updatebuf;
	while (total >= 0) {
		last_update_xid = update_entryp->xid;
		count++;
		len = sizeof(struct srt_update_entry) + update_entryp->namelen;
		total -= len;
		update_entryp = PSC_AGP(update_entryp, len);
	}
	psc_assert(!total);
	current_update_xid = last_update_xid;
	if (total == SLM_UPDATE_BATCH)
		current_update_batchno++;
	close(logfile);

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
		psc_fatal("Fail to open log file %s", res->res_jrnldev);

	mdsJournal->pj_npeers = npeers;
	mdsJournal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	mdsJournal->pj_distill_xid = last_distill_xid;

	psclog_notice("Journal device is %s", res->res_jrnldev);
	psclog_notice("Last SLASH FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psclog_notice("Last synced ZFS transaction group number is %"PRId64,
	    mdsJournal->pj_commit_txg);
	psclog_notice("Last distilled SLASH2 transaction number is %"PRId64,
	    mdsJournal->pj_distill_xid);

	/* we need the cursor thread to start any potential log replay */
	pscthr_init(SLMTHRT_CURSOR, 0, mds_cursor_thread, NULL, 0,
	    "slmjcursorthr");

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
mds_redo_namespace(struct slmds_jent_namespace *jnamespace)
{
	int rc, hasname = 1;
	struct srt_stat sstb;
	char *newname;

	memset(&sstb, 0, sizeof(sstb));
	sstb.sst_fid = jnamespace->sjnm_target_fid,
	sstb.sst_uid = jnamespace->sjnm_uid;
	sstb.sst_gid = jnamespace->sjnm_gid;
	sstb.sst_mode = jnamespace->sjnm_mode;
	sstb.sst_atime = jnamespace->sjnm_atime;
	sstb.sst_atime_ns = jnamespace->sjnm_atime_ns;
	sstb.sst_mtime = jnamespace->sjnm_mtime;
	sstb.sst_mtime_ns = jnamespace->sjnm_mtime_ns;
	sstb.sst_ctime = jnamespace->sjnm_ctime;
	sstb.sst_ctime_ns = jnamespace->sjnm_ctime_ns;
	sstb.sst_size = jnamespace->sjnm_size;

	if (!sstb.sst_fid) {
		psc_errorx("Unexpected zero SLASH2 FID.");
		return (EINVAL);
	}

	jnamespace->sjnm_name[sizeof(jnamespace->sjnm_name) - 1] = '\0';
	newname = jnamespace->sjnm_name +
	    strlen(jnamespace->sjnm_name) + 1;
	if (newname > jnamespace->sjnm_name +
	    sizeof(jnamespace->sjnm_name) - 1)
		newname = jnamespace->sjnm_name +
		    sizeof(jnamespace->sjnm_name) - 1;

	switch (jnamespace->sjnm_op) {
	    case NS_OP_CREATE:
		rc = mdsio_redo_create(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_name, &sstb);
		break;
	    case NS_OP_MKDIR:
		rc = mdsio_redo_mkdir(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_name, &sstb);
		break;
	    case NS_OP_LINK:
		rc = mdsio_redo_link(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_target_fid,
			jnamespace->sjnm_name, &sstb);
		break;
	    case NS_OP_SYMLINK:
		rc = mdsio_redo_symlink(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_target_fid,
			jnamespace->sjnm_name, newname, &sstb);
		break;
	    case NS_OP_RENAME:
		rc = mdsio_redo_rename(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_name,
			jnamespace->sjnm_new_parent_fid,
			newname, &sstb);
		break;
	    case NS_OP_UNLINK:
		rc = mdsio_redo_unlink(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_target_fid,
			jnamespace->sjnm_name, &sstb);
		break;
	    case NS_OP_RMDIR:
		rc = mdsio_redo_rmdir(
			jnamespace->sjnm_parent_fid,
			jnamespace->sjnm_target_fid,
			jnamespace->sjnm_name, &sstb);
		break;
	    case NS_OP_SETSIZE:
	    case NS_OP_SETATTR:
		rc = mdsio_redo_setattr(
			jnamespace->sjnm_target_fid,
			jnamespace->sjnm_mask, &sstb);
		hasname = 0;
		break;
	    default:
		psc_errorx("Unexpected opcode %d", jnamespace->sjnm_op);
		hasname = 0;
		rc = EINVAL;
		break;
	}
	if (hasname) {
		psclog_info("Redo namespace log: op=%d name=%s "
		    "id=%"PRIx64" rc=%d",
		    jnamespace->sjnm_op, jnamespace->sjnm_name,
		    jnamespace->sjnm_target_fid, rc);
	} else {
		psclog_info("Redo namespace log: op=%d "
		    "id=%"PRIx64" rc=%d",
		    jnamespace->sjnm_op, jnamespace->sjnm_target_fid,
		    rc);
	}
	return (rc);
}
