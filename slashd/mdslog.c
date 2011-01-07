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
static struct pscrpc_nbreqset	*logPndgReqs;

static int			 logentrysize;

extern struct bmap_timeo_table	 mdsBmapTimeoTbl;

/*
 * Eventually, we are going to retrieve the namespace update and reclaim
 * sequence number from the system journal.
 */
uint64_t			 next_update_seqno;
uint64_t			 next_reclaim_seqno;

uint64_t			 next_update_batchno;
uint64_t			 next_reclaim_batchno;

/*
 * Low and high water marks of update sequence numbers that need to be
 * propagated.  Note that the pace of each MDS is different.
 */
static uint64_t			 update_seqno_lwm;
static uint64_t			 update_seqno_hwm;

static int			 current_update_logfile = -1;
static int			 current_update_progfile = -1;

static int			 current_reclaim_logfile = -1;
static int			 current_reclaim_progfile = -1;

struct update_prog_entry {
	sl_ios_id_t		 res_id;
	enum sl_res_type	 res_type;
	uint64_t		 res_xid;
	uint64_t		 res_batchno;
};

struct reclaim_prog_entry {
	int			 res_dir;		/* sending or receiving */
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

#define SL_RECLAIM_MAX_AGE	 10

/* a buffer used to read on-disk update log file */
static char			*updatebuf;

/* a buffer used to read on-disk reclaim log file */
static char			*reclaimbuf;

static void			*mds_cursor_handle;
static struct psc_journal_cursor mds_cursor;

psc_spinlock_t			 mds_txg_lock = SPINLOCK_INIT;

static psc_spinlock_t		update_seqno_lock = SPINLOCK_INIT;
static psc_spinlock_t		reclaim_seqno_lock = SPINLOCK_INIT;

uint64_t
mds_next_update_seqno(void)
{
	uint64_t seqno;

	spinlock(&update_seqno_lock);
	seqno = next_update_seqno++;
	freelock(&update_seqno_lock);
	return (seqno);
}

uint64_t
mds_get_next_update_seqno(void)
{
	uint64_t seqno;

	spinlock(&update_seqno_lock);
	seqno = next_update_seqno;
	freelock(&update_seqno_lock);
	return (seqno);
}

uint64_t
mds_next_reclaim_seqno(void)
{
	uint64_t seqno;

	spinlock(&reclaim_seqno_lock);
	seqno = next_reclaim_seqno++;
	freelock(&reclaim_seqno_lock);
	return (seqno);
}

uint64_t
mds_get_next_reclaim_seqno(void)
{
	uint64_t seqno;

	spinlock(&reclaim_seqno_lock);
	seqno = next_reclaim_seqno;
	freelock(&reclaim_seqno_lock);
	return (seqno);
}

static void
mds_update_reclaim_prog(void)
{
	int i, ri;
	ssize_t size;
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *iosinfo;
	struct sl_resource *res;

	i = 0;
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (res->res_type == SLREST_MDS)
			continue;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;
		reclaim_prog_buf[i].res_id = res->res_id;
		reclaim_prog_buf[i].res_type = res->res_type;
		reclaim_prog_buf[i].res_xid = iosinfo->si_batchno;
		reclaim_prog_buf[i].res_batchno = iosinfo->si_batchno;
		i++;
	}
	lseek(current_reclaim_progfile, 0, SEEK_SET);
	size = write(current_reclaim_progfile, reclaim_prog_buf,
	    i * sizeof(struct reclaim_prog_entry));
	psc_assert(size == i * (int)sizeof(struct reclaim_prog_entry));
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

		rc = mdsio_write(&rootcreds, &inoh_extras, INOX_OD_SZ, &nb,
		    SL_EXTRAS_START_OFF, 0, mdsio_data, NULL, NULL);

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
	int logfile, direct;

	if (update) {
		direct = O_DIRECT;
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_UPDATELOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	} else {
		direct = 0;
		xmkfn(log_fn, "%s/%s.%d.%s.%lu", SL_PATH_DATADIR,
		    SL_FN_RECLAIMLOG, batchno,
		    psc_get_hostname(), mds_cursor.pjc_timestamp);
	}
	if (readonly) {
		logfile = open(log_fn, O_RDONLY);
		if (logfile < 0)
			psc_fatal("Failed to open log file %s to read", log_fn);
		return logfile;
	}

	/*
	 * Note we use different file descriptors for read and write.  Luckily,
	 * Linux maintains the file offset independently for each open.
	 */
	logfile = open(log_fn, O_WRONLY | O_SYNC | direct);
	if (logfile > 0) {
		/*
		 * During replay, the offset will be determined by the xid.
		 * Otherwise, we should always append at the end. This seek
		 * is needed in case the log file already exists.
		 */
		lseek(logfile, 0, SEEK_END);
		return logfile;
	}
	logfile = open(log_fn, O_CREAT | O_TRUNC | O_WRONLY | O_SYNC |
	    direct, 0600);
	if (logfile < 0)
		psc_fatal("Failed to create log file %s", log_fn);
	return (logfile);
}

/**
 * mds_distill_handler - Distill information from the system journal and
 *	write into namespace update or garbage reclaim logs.
 *
 *	Writing the information to secondary logs allows us to recyle the
 *	space in the main system log as quick as possible.  The distill
 *	process is continuous in order to make room for system logs.
 *	Once in a secondary log, we can process them as we see fit.
 *	Sometimes these secondary log files can hang over a long time
 *	because a peer MDS or an IO server is down or slow.
 *
 *	We encode the cursor creation time and hostname into the log file
 *	names to minimize collisions.  If undetected, these collisions
 *	can lead to insidious bugs, especially when on-disk format changes.
 */
int
mds_distill_handler(struct psc_journal_enthdr *pje, int npeers, int replay)
{
	struct slmds_jent_namespace *jnamespace;
	static char update_fn[PATH_MAX];
	static char reclaim_fn[PATH_MAX];
	unsigned long off;
	uint64_t seqno;
	int size, count, total;
	struct srm_reclaim_entry entry;
	struct srm_reclaim_entry *entryp;

	psc_assert(pje->pje_magic == PJE_MAGIC);
	if (!(pje->pje_type & MDS_LOG_NAMESPACE))
		return (0);

	jnamespace = PJE_DATA(pje);
	psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);

	if (!npeers)
		goto check_reclaim;

	if (current_update_logfile == -1)
		current_update_logfile = mds_open_logfile(next_update_batchno, 1, 0);

	if (replay) {
		next_update_seqno = seqno + 1;
		lseek(current_update_logfile, (seqno %
		    SLM_UPDATE_BATCH) * logentrysize, SEEK_SET);
	} else {
		/* make sure we write sequentially - no holes in our log */
		off = lseek(current_update_logfile, 0, SEEK_CUR);
		psc_assert(off == (seqno % SLM_UPDATE_BATCH) * logentrysize);
	}
	size = write(current_update_logfile, pje, logentrysize);
	if (size != logentrysize)
		psc_fatal("Fail to write update log file %s", update_fn);

	if (update_seqno_hwm < seqno + 1)
		update_seqno_hwm = seqno + 1;

	/* see if we need to close the current update log file */
	if (((seqno + 1) % SLM_UPDATE_BATCH) == 0) {
		close(current_update_logfile);
		current_update_logfile = -1;

		/* wake up the namespace log propagator */
		spinlock(&mds_update_waitqlock);
		psc_waitq_wakeall(&mds_update_waitq);
		freelock(&mds_update_waitqlock);
	}

check_reclaim:

	/*
	 * If the namespace operation needs to reclaim disk space on I/O
	 * servers, write the information into the reclaim log.
	 */
	if (!(jnamespace->sjnm_flag & SJ_NAMESPACE_RECLAIM))
		return (0);

	psc_assert(jnamespace->sjnm_op == NS_OP_SETATTR ||
	    jnamespace->sjnm_op == NS_OP_UNLINK ||
	    jnamespace->sjnm_op == NS_OP_SETSIZE);

	if (current_reclaim_logfile == -1) {
		current_reclaim_logfile = mds_open_logfile(next_reclaim_batchno, 0, 0);
		/*
 		 * Here we do one-time seek based on the xid stored in the entry.
 		 * Although not necessary contiguous, xids are in increasing order.
 		 */
		if (replay) {
			size = read(current_reclaim_logfile, reclaimbuf, 
			    SLM_RECLAIM_BATCH * sizeof(struct srm_reclaim_entry));
			total = size / sizeof(struct srm_reclaim_entry);

			count = 0;
			entryp = (struct srm_reclaim_entry *)reclaimbuf;
			while (count < total) {
				if (entryp->xid == pje->pje_xid)
					break;
				entryp++;
				count++;
			}
			/*
			 * If we didn't find the entry, this is seek-to-end. If we
			 * do find it, we will distill again (overwrite should be
			 * fine).
			 */
			lseek(current_reclaim_logfile, 
			    count * sizeof(struct srm_reclaim_entry), SEEK_CUR);
		}
	}

	entry.xid = pje->pje_xid;
	entry.fid = jnamespace->sjnm_target_fid;
	entry.gen = jnamespace->sjnm_target_gen;

	size = write(current_reclaim_logfile, &entry, sizeof(struct srm_reclaim_entry));
	if (size != sizeof(struct srm_reclaim_entry))
		psc_fatal("Fail to write reclaim log file %s", reclaim_fn);

	/* see if we need to close the current reclaim log file */
	off = lseek(current_reclaim_logfile, 0, SEEK_CUR);
	if (off == SLM_RECLAIM_BATCH * sizeof(struct srm_reclaim_entry)) {
		close(current_reclaim_logfile);
		current_reclaim_logfile = -1;
		next_reclaim_batchno++;

		/* wake up the namespace log propagator */
		spinlock(&mds_reclaim_waitqlock);
		psc_waitq_wakeall(&mds_reclaim_waitq);
		freelock(&mds_reclaim_waitqlock);
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
		 * We want to reclaim the space taken by the previous generation.
		 * Note that changing the attributes of a zero-lengh file should
		 * NOT trigger this code.
		 */
		jnamespace->sjnm_flag |= SJ_NAMESPACE_RECLAIM;
		jnamespace->sjnm_target_gen = sstb->sst_gen;
		if (op == NS_OP_SETSIZE) {
			psc_assert(sstb->sst_gen >= 1);
			jnamespace->sjnm_target_gen--;
		}
	}

	jnamespace->sjnm_reclen = offsetof(struct slmds_jent_namespace,
	    sjnm_name);
	ptr = jnamespace->sjnm_name;
	*ptr = '\0';
	rem = sizeof(jnamespace->sjnm_name);
	if (name) {
		psc_assert(sizeof(jnamespace->sjnm_name) > NAME_MAX);
		strlcpy(ptr, name, NAME_MAX + 1);
		len = strlen(ptr) + 1;
		jnamespace->sjnm_reclen += len;
		ptr += len;
		rem -= len;
	}
	if (newname) {
		strlcpy(ptr, newname, MIN(rem, NAME_MAX + 1));
		len = strlen(ptr) + 1;
		jnamespace->sjnm_reclen += len;
		ptr += len;
		rem -= len;
	}
	ptr[rem - 1] = '\0';
	psc_assert(logentrysize >= jnamespace->sjnm_reclen +
	    (int)sizeof(struct psc_journal_enthdr) - 1);

	pjournal_add_entry_distill(mdsJournal, txg,
	    MDS_LOG_NAMESPACE, jnamespace, jnamespace->sjnm_reclen);
}

__static int
mds_namespace_rpc_cb(struct pscrpc_request *req,
    struct pscrpc_async_args *args)
{
	struct srm_namespace_entry *jnamespace;
	struct sl_mds_peerinfo *peerinfo;
	struct slashrpc_cservice *csvc;
	struct sl_mds_logbuf *logbuf;
	struct sl_resource *res;
	void *buf;
	int i, j;

	res = args->pointer_arg[SLM_CBARG_SLOT_RESPROF];
	peerinfo = res2rpmi(res)->rpmi_info;
	csvc = args->pointer_arg[SLM_CBARG_SLOT_CSVC];
	RPMI_LOCK(res2rpmi(res));
	logbuf = peerinfo->sp_logbuf;
	if (req->rq_status)
		goto rpc_error;

	/*
	 * Scan the buffer for the entries we have attempted to send to update
	 * our statistics before dropping our reference to the buffer.
	 */
	i = logbuf->slb_count;
	buf = logbuf->slb_buf;
	do {
		jnamespace = buf;
		if (jnamespace->sjnm_xid == peerinfo->sp_send_seqno)
			break;
		buf = PSC_AGP(buf, jnamespace->sjnm_reclen);
		i--;
	} while (i);
	psc_assert(i > 0);
	j = i;
	do {
		jnamespace = buf;
		if (jnamespace->sjnm_xid >=
		    peerinfo->sp_send_seqno + peerinfo->sp_send_count)
			break;
		SLM_NSSTATS_INCR(peerinfo, NS_DIR_SEND,
		    jnamespace->sjnm_op, NS_SUM_PEND);
		buf = PSC_AGP(buf, jnamespace->sjnm_reclen);
		j--;
	} while (j);
	psc_assert(i - j == peerinfo->sp_send_count);

	peerinfo->sp_send_seqno += peerinfo->sp_send_count;

 rpc_error:
	peerinfo->sp_send_count = 0;				/* defensive */
	peerinfo->sp_flags &= ~SP_FLAG_INFLIGHT;

	atomic_dec(&logbuf->slb_refcnt);
	RPMI_ULOCK(res2rpmi(res));

	sl_csvc_decref(csvc);
	return (0);
}

/**
 * mds_reclaim_lwm - Find the lowest garbage reclamation water
 *	mark of all IOSes.
 */
__static uint64_t
mds_reclaim_lwm(void)
{
	uint64_t batchno = UINT64_MAX;
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
		if (iosinfo->si_batchno < batchno)
			batchno = iosinfo->si_batchno;
		RPMI_ULOCK(rpmi);
	}
	psc_assert(batchno != UINT64_MAX);

	return (batchno);
}

__static uint64_t
mds_reclaim_hwm(void)
{
	uint64_t batchno = 0;
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
		if (iosinfo->si_batchno > batchno)
			batchno = iosinfo->si_batchno;
		RPMI_ULOCK(rpmi);
	}
	return (batchno);
}

/**
 * mds_update_lwm - Find the lowest namespace update water
 *	mark of all peer MDSes.
 */
__static uint64_t
mds_update_lwm(void)
{
	uint64_t seqno = UINT64_MAX;
	struct sl_mds_peerinfo *peerinfo;
	struct resprof_mds_info *rpmi;
	struct sl_resm *resm;

	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		rpmi = resm2rpmi(resm);
		peerinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (peerinfo->sp_send_seqno < seqno)
			seqno = peerinfo->sp_send_seqno;
		RPMI_ULOCK(rpmi);
	);

	psc_assert(seqno != UINT64_MAX);

	/* XXX purge old update log files here before bumping lwm */
	update_seqno_lwm = seqno;
	return (seqno);
}

/**
 * mds_read_batch_update - Read a batch of updates from the corresponding
 *	log file and packed them for RPC later.
 */
struct sl_mds_logbuf *
mds_read_batch_update(uint64_t seqno)
{
	int i, newbuf, nitems, logfile;
	struct slmds_jent_namespace *jnamespace;
	struct sl_mds_logbuf *buf, *victim;
	struct psc_thread *thr;
	void *ptr, *logptr;
	ssize_t size;

	/*
	 * Currently, there is only one thread manipulating the list.
	 * Make sure this is the case.
	 */
	thr = pscthr_get();
	slmjnsthr(thr);

 restart:

	i = 0;
	buf = NULL;
	newbuf = 0;
	victim = NULL;
	psclist_for_each_entry(buf, &mds_update_buflist, slb_link) {
		i++;
		if (buf->slb_seqno == seqno)
			break;
		/* I am the only thread that can add a reference to a buf */
		if (!victim && atomic_read(&buf->slb_refcnt) == 0)
			victim = buf;
	}
	if (buf) {
		if (buf->slb_count == SLM_UPDATE_BATCH)
			return buf;
		goto readit;
	}
	if (i < SL_UPDATE_MAX_BUF) {
		newbuf = 1;
		buf = PSCALLOC(sizeof(struct sl_mds_logbuf) +
		    SLM_UPDATE_BATCH * logentrysize);
		buf->slb_size = 0;
		buf->slb_count = 0;
		buf->slb_seqno = seqno;
		atomic_set(&buf->slb_refcnt, 0);
		INIT_PSC_LISTENTRY(&buf->slb_link);
		buf->slb_buf = PSC_AGP(buf, sizeof(struct sl_mds_logbuf));
		goto readit;
	}
	/*
	 * If we are over the limit and we don't have a victim,
	 * wait until an RPC returns or times out.
	 */
	if (!victim) {
		spinlock(&mds_update_waitqlock);
		psc_waitq_wait(&mds_update_waitq, &mds_update_waitqlock);
		goto restart;
	}
	newbuf = 1;
	buf = victim;
	buf->slb_size = 0;
	buf->slb_count = 0;
	buf->slb_seqno = seqno;
	atomic_set(&buf->slb_refcnt, 0);
	psclist_del(&buf->slb_link, psc_lentry_hd(&buf->slb_link));

 readit:

	logfile = mds_open_logfile(seqno, 1, 1);
	lseek(logfile, buf->slb_count * logentrysize, SEEK_SET);
	size = read(logfile, updatebuf,
	    (SLM_UPDATE_BATCH - buf->slb_count) * logentrysize);
	close(logfile);
	/*
	 * A short read is allowed, but the returned size must be a
	 * multiple of the log entry size (should be 512 bytes).
	 */
	psc_assert((size % logentrysize) == 0);

	nitems = size / logentrysize;
	psc_assert(nitems + buf->slb_count <= SLM_UPDATE_BATCH);

	ptr = PSC_AGP(buf->slb_buf, buf->slb_size);
	logptr = updatebuf;
	for (i = 0; i < nitems; i++) {
		struct psc_journal_enthdr *pje;

		pje = logptr;
		psc_assert(pje->pje_magic == PJE_MAGIC);

		jnamespace = PSC_AGP(logptr,
		    offsetof(struct psc_journal_enthdr, pje_data));
		psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);
		psc_assert(jnamespace->sjnm_reclen <= logentrysize);
		memcpy(ptr, jnamespace, jnamespace->sjnm_reclen);
		ptr = PSC_AGP(ptr, jnamespace->sjnm_reclen);
		buf->slb_size += jnamespace->sjnm_reclen;
		logptr = PSC_AGP(logptr, logentrysize);
	}
	buf->slb_count += nitems;

	if (newbuf)
		psclist_add_tail(&buf->slb_link, &mds_update_buflist);
	/*
	 * Return the loaded buffer without taking a reference.  This is
	 * only possible because we are the only thread involved.
	 */
	return (buf);
}

/**
 * mds_send_batch_update - Send a batch of updates to peer MDSes
 *	that want them.
 */
int
mds_send_batch_update(struct sl_mds_logbuf *logbuf)
{
	struct srm_namespace_entry *jnamespace;
	struct srm_send_namespace_req *mq;
	struct slashrpc_cservice *csvc;
	struct pscrpc_bulk_desc *desc;
	struct sl_mds_peerinfo *peerinfo;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	struct sl_resm *resm;
	struct iovec iov;
	int rc, j, didwork=0;
	void *buf;

	SL_FOREACH_MDS(resm,
		if (resm == nodeResm)
			continue;
		peerinfo = resm2rpmi(resm)->rpmi_info;

		/*
		 * Skip if the MDS is busy or the current batch is out of
		 * its windows.  Note for each MDS, we send updates in order.
		 */
		if (peerinfo->sp_flags & SP_FLAG_MIA)
			continue;
		if (peerinfo->sp_flags & SP_FLAG_INFLIGHT)
			continue;
		if (peerinfo->sp_send_seqno < logbuf->slb_seqno ||
		    peerinfo->sp_send_seqno >= logbuf->slb_seqno + logbuf->slb_count)
			continue;

		/* Find out which part of the buffer should be send out */
		j = logbuf->slb_count;
		buf = logbuf->slb_buf;
		do {
			jnamespace = buf;
			if (jnamespace->sjnm_xid == peerinfo->sp_send_seqno)
				break;
			buf = PSC_AGP(buf, jnamespace->sjnm_reclen);
			j--;
		} while (j);
		psc_assert(j);

		iov.iov_base = buf;
		iov.iov_len = logbuf->slb_size -
		    ((char *)buf - (char *)logbuf->slb_buf);

		csvc = slm_getmcsvc(resm);
		if (csvc == NULL) {
			/*
			 * A simplistic way to avoid CPU spinning.  A better
			 * way is to let the ping thread handle this.
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
		mq->seqno = peerinfo->sp_send_seqno;
		mq->size = iov.iov_len;
		mq->count = j;
		mq->siteid = nodeSite->site_id;
		psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

		peerinfo->sp_send_count = j;
		peerinfo->sp_logbuf = logbuf;
		peerinfo->sp_flags |= SP_FLAG_INFLIGHT;
		atomic_inc(&logbuf->slb_refcnt);

		/*
		 * Be careful, we use the value of j and buf from the
		 * previous while loop.
		 */
		while (j) {
			j--;
			jnamespace = buf;
			SLM_NSSTATS_INCR(peerinfo, NS_DIR_SEND,
			    jnamespace->sjnm_op, NS_SUM_PEND);
			buf = PSC_AGP(buf, jnamespace->sjnm_reclen);
		}
		rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
		    SRMM_BULK_PORTAL, &iov, 1);

		authbuf_sign(rq, PSCRPC_MSG_REQUEST);
		rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_RESPROF] = resm->resm_res;
		rq->rq_async_args.pointer_arg[SLM_CBARG_SLOT_CSVC] = peerinfo;
		psc_assert(pscrpc_nbreqset_add(logPndgReqs, rq) == 0);
		didwork = 1;
	);
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
 	 * Distill happens outside ZFS. This means if there is no ZFS
 	 * activity, the following value will be stale.
 	 */
	cursor->pjc_distill_xid = pjournal_next_distill(mdsJournal);
	cursor->pjc_fid = slm_get_curr_slashid();
	cursor->pjc_update_seqno = mds_get_next_update_seqno();
	cursor->pjc_reclaim_seqno = mds_get_next_reclaim_seqno();

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
	 * has already been synced.  And it can change afterwards. This
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
			psc_notify("Cursor updated: txg=%"PRId64", xid=%"PRId64
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
	int rc;
	size_t nb;
	mdsio_fid_t mf;

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
	psc_notify("File system was formated on %"PRIu64" seconds "
	    "since the Epoch", mds_cursor.pjc_timestamp);
	psc_notify("File system was formated on %s",
	    ctime((time_t *)&mds_cursor.pjc_timestamp));
}

int
mds_send_batch_reclaim(uint64_t batchno)
{
	uint64_t xid;
	ssize_t size;
	struct resprof_mds_info *rpmi;
	struct sl_resource *res;
	struct sl_resm *dst_resm;
	struct iovec iov;
	struct sl_mds_iosinfo *iosinfo;
	struct srm_reclaim_entry *entry;
	int i, ri, rc, count, nios, logfile, didwork;
	struct slashrpc_cservice *csvc;
	struct srm_reclaim_req *mq;
	struct srm_reclaim_rep *mp;
	struct pscrpc_request *rq = NULL;
	struct pscrpc_bulk_desc *desc;

	didwork = 0;

	logfile = mds_open_logfile(batchno, 0, 1);
	size = read(logfile, reclaimbuf, SLM_RECLAIM_BATCH *
	    sizeof(struct srm_reclaim_entry));
	close(logfile);
	if (size == 0)
		return (didwork);

	/*
	 * Short read is Okay, as long as it is a multiple of the basic
	 * data structure.
	 */
	psc_assert((size % sizeof(struct srm_reclaim_entry)) == 0);
	count = (int) size / (int) sizeof(struct srm_reclaim_entry);

	iov.iov_len = size;
	iov.iov_base = reclaimbuf;

	/* find the xid associated with the last log entry */
	entry = (struct srm_reclaim_entry *) (reclaimbuf + (count - 1) * sizeof(struct srm_reclaim_entry));
	xid = entry->xid;

	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri) {
		if (res->res_type == SLREST_MDS)
			continue;
		nios++;
		rpmi = res2rpmi(res);
		iosinfo = rpmi->rpmi_info;

		RPMI_LOCK(rpmi);
		if (iosinfo->si_batchno > batchno) {
			RPMI_ULOCK(rpmi);
			didwork++;
			continue;
		}

		if (iosinfo->si_batchno < batchno) 
			continue;
		if (iosinfo->si_xid >= xid) 
			continue;

		RPMI_ULOCK(rpmi);
		/*
		 * Send RPC to the IO server and wait for it to complete.
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
			mq->count = count;
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
				didwork++;
				iosinfo->si_batchno = xid;;
				if (count == SLM_RECLAIM_BATCH)
					iosinfo->si_batchno++;
				break;
			}
		}
	}
	/*
	 * If this log file is full and all I/O servers have applied its
	 * contents, update our progress on the disk first and then remove
	 * the log file.
	 */
	if (didwork == nios && count == SLM_RECLAIM_BATCH) {
		mds_update_reclaim_prog();
		mds_remove_logfile(batchno, 0);
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
 	 * Instead of tracking precisely which reclaim log record has been sent
 	 * to an I/O node, we track the batch number.  A receiving I/O node can
 	 * safely ignore any resent records.
 	 */
	while (pscthr_run()) {

		batchno = mds_reclaim_lwm();
		do {
			didwork = mds_send_batch_reclaim(batchno);
			batchno++;
		} while (didwork && (mds_reclaim_hwm() >= batchno));

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
	struct sl_mds_logbuf *buf;
	int rv, didwork;
	uint64_t seqno;

	/*
	 * This thread scans the batches of updates between the low and
	 * high water marks and sends them to peer MDSes.  Although
	 * different MDSes have different paces, we send updates in
	 * order within one MDS.
	 */
	while (pscthr_run()) {
		pscrpc_nbreqset_reap(logPndgReqs);
		seqno = mds_update_lwm();
		/*
		 * If update_seqno_hwm is zero, then there are no
		 * local updates.
		 */
		if (update_seqno_hwm && seqno < update_seqno_hwm) {
			buf = mds_read_batch_update(seqno);
			didwork = mds_send_batch_update(buf);
			seqno += SLM_UPDATE_BATCH;
			if (didwork)
				continue;
		}
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
 *   mdsfssyncthr_begin().
 * @data: void * which is the bmap.
 * Notes: this call allows slash2 to optimize crc calculation by only
 *   taking them when the bmap is written, not upon each update to the
 *   bmap.  It is important to note that forward changes may be synced
 *   here.  What that means is that changes which are not part of this
 *   XID session may have snuck in here (ie a crc update came in and
 *   was fully processed before mds_bmap_sync() grabbed the lock.  For
 *   this reason the crc updates must be journaled before manifesting
 *   in the bmap cache.  Otherwise, log replays will look inconsistent.
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
 * mds_bmap_repl_log - Write a modified replication table to the journal.
 * Note:  bmap must be locked to prevent further changes from sneaking in
 *	before the repl table is committed to the journal.
 */
void
mds_bmap_repl_log(void *datap, uint64_t txg)
{
	struct bmapc_memb *bmap = datap;
	struct slmds_jent_repgen *jrpg;

	jrpg = pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_repgen));

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
 * @crcs: array of crc / slot pairs.
 * @n: the number of crc / slot pairs.
 * Notes: bmap_crc_writes from the ION are sent here directly because this
 *    function is responsible for updating the cached bmap after the crc
 *    has been committed to the journal.  This allows us to not have to
 *    hold the lock while doing journal I/O with the caveat that we trust
 *    the ION to not send multiple crc updates for the same region which
 *    me may then process out of order.
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
	int i, ri, rc, nios, count, found, npeers;
	static char fn[PATH_MAX];
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *iosinfo;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_resource *res;
	struct sl_resm *resm;
	struct stat sb;
	ssize_t size;

	/* Make sure we have some I/O servers to work with */
	nios = 0;
	SITE_FOREACH_RES(nodeSite, res, ri)
		if (res->res_type != SLREST_MDS)
			nios++;
	if (!nios)
		psc_fatal("Missing I/O servers at site %s", nodeSite->site_name);

	/* Count the number of peer MDSes we have */
	npeers = 0;
	SL_FOREACH_MDS(resm, npeers++);
	npeers--;

	res = nodeResm->resm_res;
	if (res->res_jrnldev[0] == '\0')
		xmkfn(res->res_jrnldev, "%s/%s", sl_datadir, SL_FN_OPJOURNAL);

	mds_open_cursor();

	mdsJournal = pjournal_open(res->res_jrnldev);
	if (mdsJournal == NULL)
		psc_fatal("Fail to open log file %s", res->res_jrnldev);

	logentrysize = mdsJournal->pj_hdr->pjh_entsz;

	mdsJournal->pj_npeers = npeers;
	mdsJournal->pj_commit_txg = mds_cursor.pjc_commit_txg;
	mdsJournal->pj_distill_xid = mds_cursor.pjc_distill_xid;

	next_update_seqno = mds_cursor.pjc_update_seqno;
	next_reclaim_seqno = mds_cursor.pjc_reclaim_seqno;

	psc_notify("Journal device is %s", res->res_jrnldev);
	psc_notify("Last SLASH FID is "SLPRI_FID, mds_cursor.pjc_fid);
	psc_notify("Last synced ZFS transaction group number is %"PRId64,
	    mdsJournal->pj_commit_txg);
	psc_notify("Last distilled SLASH2 transaction number is %"PRId64,
	    mdsJournal->pj_distill_xid);

	/* we need the cursor thread to start any potential log replay */
	pscthr_init(SLMTHRT_CURSOR, 0, mds_cursor_thread, NULL, 0,
	    "slmjcursorthr");

	psc_notify("Next update sequence number before log replay is %"PRId64,
	    next_reclaim_seqno);
	psc_notify("Next reclaim sequence number before log replay is %"PRId64,
	    next_update_seqno);

	reclaimbuf = PSCALLOC(SLM_UPDATE_BATCH * logentrysize);

	pjournal_replay(mdsJournal, SLMTHRT_JRNL, "slmjthr",
	    mds_replay_handler, mds_distill_handler);

	psc_notify("The next update sequence number after log replay is %"PRId64,
	    next_update_seqno);
	psc_notify("The next reclaim sequence number after log replay is %"PRId64,
	    next_reclaim_seqno);

	mds_bmap_setcurseq(mds_cursor.pjc_seqno_hwm, mds_cursor.pjc_seqno_lwm);
	psc_notify("Last bmap sequence number low water mark is %"PRId64,
	    mds_cursor.pjc_seqno_lwm);
	psc_notify("Last bmap sequence number high water mark is %"PRId64,
	    mds_cursor.pjc_seqno_hwm);

	xmkfn(fn, "%s/%s.%s.%lu", SL_PATH_DATADIR, SL_FN_RECLAIMPROG,
	    psc_get_hostname(), mds_cursor.pjc_timestamp);

	current_reclaim_progfile = open(fn, O_CREAT | O_RDWR | O_SYNC, 0600);
	rc = fstat(current_reclaim_progfile, &sb);
	if (rc < 0)
		psc_fatal("Fail to stat reclaim log file %s", fn);
	psc_assert((sb.st_size % sizeof(struct reclaim_prog_entry)) == 0);

	i = count = sb.st_size / sizeof(struct reclaim_prog_entry);
	if (i < nios)
		i = nios;

	reclaim_prog_buf = PSCALLOC(i * sizeof(struct reclaim_prog_entry));
	if (count) {
		size = read(current_reclaim_progfile, reclaim_prog_buf,
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
		iosinfo->si_xid = reclaim_prog_buf[i].res_xid;
		iosinfo->si_batchno = reclaim_prog_buf[i].res_batchno;
	}
	if (found != nios)
		mds_update_reclaim_prog();

	/* Always start a thread to send reclaim updates. */
	pscthr_init(SLMTHRT_JRECLAIM, 0, mds_send_reclaim, NULL,
	    0, "slmjreclaimthr");

	/* We are done if we don't have any peer MDSes */
	if (!npeers)
		return;

	xmkfn(fn, "%s/%s.%s.%lu", SL_PATH_DATADIR, SL_FN_UPDATEPROG, psc_get_hostname(), mds_cursor.pjc_timestamp);

	current_update_progfile = open(fn, O_CREAT | O_RDWR | O_SYNC, 0600);
	rc = fstat(current_update_progfile, &sb);
	if (rc < 0)
		psc_fatal("Fail to stat update log file %s", fn);
	psc_assert((sb.st_size % sizeof(struct update_prog_entry)) == 0);

	i = count = sb.st_size / sizeof(struct update_prog_entry);
	if (i < npeers)
		i = npeers;

	update_prog_buf = PSCALLOC(i * sizeof(struct update_prog_entry));
	if (count) {
		size = read(current_update_progfile, update_prog_buf,
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
	updatebuf = PSCALLOC(SLM_UPDATE_BATCH * logentrysize);
	logPndgReqs = pscrpc_nbreqset_init(NULL, mds_namespace_rpc_cb);

	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	pscthr_init(SLMTHRT_JNAMESPACE, 0, mds_send_update, NULL,
	    0, "slmjnsthr");
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
		psc_notify("Redo namespace log: op = %d, name = %s, id = %"PRIx64", rc = %d",
			   jnamespace->sjnm_op, jnamespace->sjnm_name,
			   jnamespace->sjnm_target_fid, rc);
	} else {
		psc_notify("Redo namespace log: op = %d, id = %"PRIx64", rc = %d",
			   jnamespace->sjnm_op, jnamespace->sjnm_target_fid, rc);
	}
	return (rc);
}
