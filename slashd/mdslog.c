/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_ds/dynarray.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/crc.h"
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
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "sljournal.h"

#include "zfs-fuse/zfs_slashlib.h"

struct psc_journal		*mdsJournal;
static struct pscrpc_nbreqset	*logPndgReqs;

static int			 logentrysize;

extern struct bmap_timeo_table	 mdsBmapTimeoTbl;

/*
 * Eventually, we are going to retrieve the namespace update sequence number
 * from the system journal.
 */
uint64_t			 next_update_seqno;

/*
 * Low and high water marks of update sequence numbers that need to be propagated.
 * Note that the pace of each MDS is different.
 */
static uint64_t			 propagate_seqno_lwm;
static uint64_t			 propagate_seqno_hwm;

static int			 current_logfile = -1;

struct psc_waitq		 mds_namespace_waitq = PSC_WAITQ_INIT;
psc_spinlock_t			 mds_namespace_waitqlock = LOCK_INITIALIZER;

/* max # of buffers used to decrease I/O */
#define	SL_NAMESPACE_MAX_BUF	 8

/* max # of seconds before an update is propagated */
#define SL_NAMESPACE_MAX_AGE	 30

/* a buffer used to read on-disk log file */
static char			*stagebuf;

static struct psc_thread	*namespaceThr;


/* we only have a few buffers, so a list is fine */
__static PSCLIST_HEAD(mds_namespace_buflist);

struct sl_mds_peerinfo		*localinfo = NULL;

/* list of peer MDSes and its lock */
struct psc_dynarray		 mds_namespace_peerlist = DYNARRAY_INIT;
psc_spinlock_t			 mds_namespace_peerlist_lock = LOCK_INITIALIZER;

int
mds_peerinfo_cmp(const void *a, const void *b)
{
	const struct sl_mds_peerinfo *x = a;
	const struct sl_mds_peerinfo *y = b;

	return (CMP(x->sp_siteid, y->sp_siteid));
}

uint64_t
mds_get_next_seqno(void)
{
	static psc_spinlock_t lock = LOCK_INITIALIZER;
	uint64_t seqno;

	spinlock(&lock);
	seqno = next_update_seqno++;
	freelock(&lock);
	return (seqno);
}

static int
mds_redo_bmap_repl(__unusedx struct psc_journal_enthdr *pje)
{
	return (0);
}

/*
 * mds_redo_bmap_crc - replay a CRC update.  Because we only log
 *     CRCs that have been changed in the bmap, this has to be a
 *     read-modify-write process.
 */
static int
mds_redo_bmap_crc(__unusedx struct psc_journal_enthdr *pje)
{
	int i, rc;
	size_t nb;
	void *mdsio_data;
	struct slmds_jent_crc *jcrc;
	struct srt_bmap_wire bmap_disk;
	struct srm_bmap_crcwire *bmap_wire;
	jcrc = (struct slmds_jent_crc *)pje->pje_data;

	zfsslash2_opencreate(jcrc->sjc_fid, &rootcreds, O_RDWR, 0, NULL,
	    NULL, NULL, NULL, &mdsio_data, NULL, NULL);

	rc = zfsslash2_read(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
		(off_t)((BMAP_OD_SZ * jcrc->sjc_bmapno) + SL_BMAP_START_OFF),
		mdsio_data);
	if (rc || nb != BMAP_OD_SZ)
		goto out;

	for (i = 0 ; i < jcrc->sjc_ncrcs; i++) {
		bmap_wire = &jcrc->sjc_crc[i];
		bmap_disk.bh_crcs[bmap_wire->slot].gc_crc = bmap_wire->crc;
	}

	rc = zfsslash2_write(&rootcreds, &bmap_disk, BMAP_OD_SZ, &nb,
		(off_t)((BMAP_OD_SZ * jcrc->sjc_bmapno) + SL_BMAP_START_OFF),
		mdsio_data, NULL, NULL);
	if (rc || nb != BMAP_OD_SZ)
		goto out;

out:
	zfsslash2_release(&rootcreds, &mdsio_data);
	return (rc);
}

static int
mds_redo_bmap_seq(__unusedx struct psc_journal_enthdr *pje)
{
	return (0);
}

static int
mds_redo_ino_addrepl(__unusedx struct psc_journal_enthdr *pje)
{
	return (0);
}

static int
mds_redo_namespace(__unusedx struct psc_journal_enthdr *pje)
{
	return (0);
}

/**
 * mds_replay_handle - Handle journal replay events.
 */
void
mds_replay_handler(struct psc_journal_enthdr *pje, int *rcp)
{
	int rc = 0;

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
		rc = mds_redo_namespace(pje);
		break;
	    default:
		psc_fatal("invalid log entry type %d", pje->pje_type);
	}
	*rcp = rc;
}

/**
 * mds_distill_handler - Distill information from the system journal and
 *	write into change log files.
 */
void
mds_distill_handler(struct psc_journal_enthdr *pje, __unusedx int size)
{
	int sz;
	uint64_t seqno;
	char fn[PATH_MAX+1];
	struct slmds_jent_namespace *jnamespace;

	psc_assert(pje->pje_magic == PJE_MAGIC);
	if (!(pje->pje_type & MDS_LOG_NAMESPACE))
		return;

	jnamespace = (struct slmds_jent_namespace *)&pje->pje_data[0];
	psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);

	/* see if we can open a new change log file */
	seqno = jnamespace->sjnm_seqno;
	if ((seqno % SLM_NAMESPACE_BATCH) == 0) {
		psc_assert(current_logfile == -1);
		xmkfn(fn, "%s/%s.%d", SL_PATH_DATADIR,
		    SL_FN_NAMESPACELOG, seqno/SLM_NAMESPACE_BATCH);
		/*
		 * Truncate the file if it already exists. Otherwise, it
		 * can lead to an insidious * bug especially when the
		 * on-disk format of the log file changes.
		 */
		current_logfile = open(fn, O_CREAT | O_TRUNC | O_RDWR |
		    O_SYNC | O_DIRECT | O_APPEND, 0600);
		if (current_logfile == -1)
			psc_fatal("Fail to create change log file %s", fn);
	} else
		psc_assert(current_logfile != -1);

	/*
	 * Write to the disk now so that we can reclaim the log space in the
	 * system log. In other words, we can't accumulate one batch worth
	 * of namespace updates and move them into the buffer directly. We
	 * also can't compete with the update propagator for limited number
	 * of buffers either.
	 */
	sz = write(current_logfile, pje, logentrysize);
	if (sz != logentrysize)
		psc_fatal("Fail to write change log file %s", fn);

	propagate_seqno_hwm = seqno + 1;

	/* see if we need to close the current change log file */
	if (((seqno + 1) % SLM_NAMESPACE_BATCH) == 0) {
		close(current_logfile);
		current_logfile = -1;

		/* wait up the namespace log propagator */
		spinlock(&mds_namespace_waitqlock);
		psc_waitq_wakeall(&mds_namespace_waitq);
		freelock(&mds_namespace_waitqlock);
	}
}

/*
 * Log namespace operation before we attempt the operation.  This makes sure
 * that it will be propagated towards other MDSes and made permanent before
 * we reply to the client.
 */
void
mds_namespace_log(int op, uint64_t txg, uint64_t parent, uint64_t newparent, uint64_t target,
	const struct srt_stat *stat, const char *name, const char *newname)
{
	char *ptr;
	struct slmds_jent_namespace *jnamespace;

	psc_assert(target);

	jnamespace = (struct slmds_jent_namespace *) 
	    pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_namespace));
	jnamespace->sjnm_magic = SJ_NAMESPACE_MAGIC;
	jnamespace->sjnm_op = op;
	jnamespace->sjnm_seqno = mds_get_next_seqno();
	jnamespace->sjnm_parent_s2id = parent;
	jnamespace->sjnm_target_s2id = target;
	jnamespace->sjnm_new_parent_s2id = newparent;

	if (stat) {
		jnamespace->sjnm_mask = stat->sst_mask;
		jnamespace->sjnm_uid = stat->sst_uid;
		jnamespace->sjnm_gid = stat->sst_gid;
		jnamespace->sjnm_mode = stat->sst_mode;

		jnamespace->sjnm_atime = stat->sst_atime;
		jnamespace->sjnm_mtime = stat->sst_mtime;
		jnamespace->sjnm_ctime = stat->sst_ctime;
	}
	jnamespace->sjnm_reclen = offsetof(struct slmds_jent_namespace, sjnm_name);
	ptr = &jnamespace->sjnm_name[0];
	if (name) {
		strncpy(ptr, name, NAME_MAX);
		jnamespace->sjnm_reclen += strlen(name) + 1;
		ptr += strlen(name) + 1;
	}
	if (newname) {
		strncpy(ptr, newname, NAME_MAX);
		jnamespace->sjnm_reclen += strlen(newname) + 1;
	}
	psc_assert(logentrysize >= jnamespace->sjnm_reclen +
	    (int)sizeof(struct psc_journal_enthdr) - 1);

	pjournal_add_entry_distill(mdsJournal, txg, MDS_LOG_NAMESPACE,
	    jnamespace, jnamespace->sjnm_reclen);

	pjournal_put_buf(mdsJournal, (void *)jnamespace);
}

__static int
mds_namespace_rpc_cb(__unusedx struct pscrpc_request *req,
		  struct pscrpc_async_args *args)
{
	int i, j;
	struct sl_mds_peerinfo *peerinfo;
	struct sl_mds_logbuf *logbuf;
	char *buf;
	struct slmds_jent_namespace *jnamespace;

	peerinfo = args->pointer_arg[0];
	spinlock(&peerinfo->sp_lock);
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
		jnamespace = (struct slmds_jent_namespace *)buf;
		if (jnamespace->sjnm_seqno == peerinfo->sp_send_seqno)
			break;
		buf = buf + jnamespace->sjnm_reclen;
		i--;
	} while (i);
	psc_assert(i > 0);
	j = i;
	do {
		jnamespace = (struct slmds_jent_namespace *)buf;
		if (jnamespace->sjnm_seqno >=
		    peerinfo->sp_send_seqno + peerinfo->sp_send_count)
			break;
		SLM_NSSTATS_INCR(peerinfo, NS_DIR_SEND,
		    jnamespace->sjnm_op, NS_SUM_PEND);
		buf = buf + jnamespace->sjnm_reclen;
		j--;
	} while (j);
	psc_assert(i - j == peerinfo->sp_send_count);

	peerinfo->sp_send_seqno += peerinfo->sp_send_count;

rpc_error:

	peerinfo->sp_send_count = 0;				/* defensive */
	peerinfo->sp_flags &= ~SP_FLAG_INFLIGHT;

	atomic_dec(&logbuf->slb_refcnt);
	freelock(&peerinfo->sp_lock);

	/* drop the reference taken by slm_getmcsvc() */
	sl_csvc_decref(peerinfo->sp_resm->resm_csvc);
	return (0);
}

/*
 * mds_namespace_update_lwm - Find the lowest water mark of all peer MDSes.
 */
__static uint64_t
mds_namespace_update_lwm(void)
{
	int i;
	int first = 1;
	uint64_t seqno;
	struct sl_mds_peerinfo *peerinfo;

	spinlock(&mds_namespace_peerlist_lock);
	for (i = 0; i < psc_dynarray_len(&mds_namespace_peerlist); i++) {
		peerinfo = psc_dynarray_getpos(&mds_namespace_peerlist, i);
		if (peerinfo->sp_resm == nodeResm)
			continue;
		spinlock(&peerinfo->sp_lock);
		if (first) {
			first = 0;
			seqno = peerinfo->sp_send_seqno;
			freelock(&peerinfo->sp_lock);
			continue;
		}
		if (seqno > peerinfo->sp_send_seqno)
			seqno = peerinfo->sp_send_seqno;
		freelock(&peerinfo->sp_lock);
	}
	freelock(&mds_namespace_peerlist_lock);
	/* XXX purge old log files here before bumping lwm */
	propagate_seqno_lwm = seqno;
	return (seqno);
}

/*
 * mds_namespace_read - read a batch of updates from the corresponding log file
 *	and packed them for RPC later.
 *
 */
struct sl_mds_logbuf *
mds_namespace_read_batch(uint64_t seqno)
{
	struct slmds_jent_namespace *jnamespace;
	struct sl_mds_logbuf *buf, *victim;
	char fn[PATH_MAX], *ptr, *logptr;
	int i, newbuf, nitems, logfile;
	ssize_t size;
	struct psc_thread *thr;

	/*
	 * Currently, there is only one thread manipulating the list.
	 * Make sure this is the case.
	 */
	thr = pscthr_get();
	psc_assert(thr == namespaceThr);

 restart:

	i = 0;
	buf = 0;
	newbuf = 0;
	victim = NULL;
	psclist_for_each_entry(buf, &mds_namespace_buflist, slb_link) {
		i++;
		if (buf->slb_seqno == seqno)
			break;
		/* I am the only thread that can add a reference to a buf */
		if (!victim && atomic_read(&buf->slb_refcnt) == 0)
			victim = buf;
	}
	if (buf) {
		if (buf->slb_count == SLM_NAMESPACE_BATCH)
			return buf;
		goto readit;
	}
	if (i < SL_NAMESPACE_MAX_BUF) {
		newbuf = 1;
		buf = PSCALLOC(sizeof(struct sl_mds_logbuf) + SLM_NAMESPACE_BATCH * logentrysize);
		buf->slb_size = 0;
		buf->slb_count = 0;
		buf->slb_seqno = seqno;
		atomic_set(&buf->slb_refcnt, 0);
		INIT_PSCLIST_ENTRY(&buf->slb_link);
		buf->slb_buf = (char *)buf + sizeof(struct sl_mds_logbuf);
		goto readit;
	}
	/*
	 * If we are over the limit and we don't have a victim,
	 * wait until an RPC returns or times out.
	 */
	if (!victim) {
		spinlock(&mds_namespace_waitqlock);
		psc_waitq_wait(&mds_namespace_waitq, &mds_namespace_waitqlock);
		goto restart;
	}
	newbuf = 1;
	buf = victim;
	buf->slb_size = 0;
	buf->slb_count = 0;
	buf->slb_seqno = seqno;
	atomic_set(&buf->slb_refcnt, 0);
	psclist_del(&buf->slb_link);

 readit:

	/*
	 * A short read is allowed, but the returned size must be a
	 * multiple of the log entry size (should be 512 bytes).
	 */
	xmkfn(fn, "%s/%s.%d", SL_PATH_DATADIR, SL_FN_NAMESPACELOG,
	    seqno / SLM_NAMESPACE_BATCH);
	logfile = open(fn, O_RDONLY);
	if (logfile == -1)
		psc_fatal("Fail to open change log file %s", fn);
	lseek(logfile, buf->slb_count * logentrysize, SEEK_SET);
	size = read(logfile, stagebuf,
		   (SLM_NAMESPACE_BATCH - buf->slb_count) * logentrysize);
	close(logfile);

	nitems = size / logentrysize;
	psc_assert((size % logentrysize) == 0);
	psc_assert(nitems + buf->slb_count <= SLM_NAMESPACE_BATCH);

	ptr = buf->slb_buf + buf->slb_size;
	logptr = stagebuf;
	for (i = 0; i < nitems; i++) {

		struct psc_journal_enthdr *pje;
		pje = (struct psc_journal_enthdr *) logptr;
		psc_assert(pje->pje_magic == PJE_MAGIC);

		jnamespace = (struct slmds_jent_namespace *)
			(logptr + offsetof(struct psc_journal_enthdr, pje_data));
		psc_assert(jnamespace->sjnm_magic == SJ_NAMESPACE_MAGIC);
		psc_assert(jnamespace->sjnm_reclen <= logentrysize);
		memcpy(ptr, jnamespace, jnamespace->sjnm_reclen);
		ptr += jnamespace->sjnm_reclen;
		buf->slb_size += jnamespace->sjnm_reclen;
		logptr += logentrysize;
	}
	buf->slb_count += nitems;

	if (newbuf)
		psclist_xadd_tail(&buf->slb_link, &mds_namespace_buflist);
	/*
	 * Return the loaded buffer without taking a reference.  This is
	 * only possible because we are the only thread involved.
	 */
	return buf;
}

/**
 * mds_namespace_propagate_batch - Send a batch of updates to peer MDSes that want them.
 */
void
mds_namespace_propagate_batch(struct sl_mds_logbuf *logbuf)
{
	struct slmds_jent_namespace *jnamespace;
	struct srm_send_namespace_req *mq;
	struct slashrpc_cservice *csvc;
	struct pscrpc_bulk_desc *desc;
	struct sl_mds_peerinfo *peerinfo;
	struct srm_generic_rep *mp;
	struct pscrpc_request *req;
	struct iovec iov;
	int rc, i, j;
	char *buf;

	spinlock(&mds_namespace_peerlist_lock);
	for (i = 0; i < psc_dynarray_len(&mds_namespace_peerlist); i++) {
		peerinfo = psc_dynarray_getpos(&mds_namespace_peerlist, i);
		if (peerinfo->sp_resm == nodeResm)
			continue;
		/*
		 * Skip if the MDS is busy or the current batch is out of
		 * its windows.  Note for each MDS, we send updates in order.
		 */
		if (peerinfo->sp_flags & SP_FLAG_INFLIGHT)
			continue;
		if (peerinfo->sp_send_seqno < logbuf->slb_seqno ||
		    peerinfo->sp_send_seqno >= logbuf->slb_seqno + logbuf->slb_count)
			continue;

		/* Find out which part of the buffer should be send out */
		j = logbuf->slb_count;
		buf = logbuf->slb_buf;
		do {
			jnamespace = (struct slmds_jent_namespace *)buf;
			if (jnamespace->sjnm_seqno == peerinfo->sp_send_seqno)
				break;
			buf = buf + jnamespace->sjnm_reclen;
			j--;
		} while (j);
		psc_assert(j);

		iov.iov_base = buf;
		iov.iov_len = logbuf->slb_size - (buf - logbuf->slb_buf);

		csvc = slm_getmcsvc(peerinfo->sp_resm);
		if (csvc == NULL)
			continue;
		rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMM_VERSION,
		    SRMT_NAMESPACE_UPDATE, req, mq, mp);
		if (rc) {
			sl_csvc_decref(csvc);
			continue;
		}
		mq->seqno = peerinfo->sp_send_seqno;
		mq->size = iov.iov_len;
		mq->count = j;
		mq->siteid = localinfo->sp_siteid;
		psc_crc64_calc(&mq->crc, iov.iov_base, iov.iov_len);

		peerinfo->sp_send_count = j;
		peerinfo->sp_logbuf = logbuf;
		peerinfo->sp_flags |= SP_FLAG_INFLIGHT;
		atomic_inc(&logbuf->slb_refcnt);

		/*
		 * Be careful, we use the value of i and buf from the
		 * previous while loop.
		 */
		while (j) {
			j--;
			jnamespace = (struct slmds_jent_namespace *)buf;
			SLM_NSSTATS_INCR(peerinfo, NS_DIR_SEND,
			    jnamespace->sjnm_op, NS_SUM_PEND);
			buf = buf + jnamespace->sjnm_reclen;
		}
		rsx_bulkclient(req, &desc, BULK_GET_SOURCE,
		    SRMM_BULK_PORTAL, &iov, 1);

		req->rq_async_args.pointer_arg[0] = peerinfo;
		pscrpc_nbreqset_add(logPndgReqs, req);
	}
	freelock(&mds_namespace_peerlist_lock);
}

/*
 * mds_namespace_propagate - Send local namespace updates to peer MDSes.
 */
void
mds_namespace_propagate(__unusedx struct psc_thread *thr)
{
	int rv;
	uint64_t seqno;
	struct sl_mds_logbuf *buf;

	/*
	 * This thread scans the batches of changes between the low and high
	 * water marks and sends them to peer MDSes.  Although different MDSes
	 * have different paces, we send updates in order within one MDS.
	 */
	while (pscthr_run()) {
		pscrpc_nbreqset_reap(logPndgReqs);
		seqno = mds_namespace_update_lwm();
		/*
		 * If propagate_seqno_hwm is zero, then there are no local updates.
		 */
		if (propagate_seqno_hwm && seqno < propagate_seqno_hwm) {
			buf = mds_namespace_read_batch(seqno);
			mds_namespace_propagate_batch(buf);
			seqno += SLM_NAMESPACE_BATCH;
			continue;
		}
		spinlock(&mds_namespace_waitqlock);
		rv = psc_waitq_waitrel_s(&mds_namespace_waitq,
		    &mds_namespace_waitqlock, SL_NAMESPACE_MAX_AGE);
	}
}

void
mds_inode_sync(void *data)
{
	int rc, tmpx = 0;
	struct slash_inode_handle *inoh = data;

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
		psc_crc64_calc(&inoh->inoh_extras->inox_crc, inoh->inoh_extras,
			     INOX_OD_CRCSZ);
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


void
mds_bmap_jfiprep(void *data)
{
	struct bmapc_memb *bmap=data;

	bmap_op_start_type(bmap, BMAP_OPCNT_MDSLOG);
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
	struct slash_bmap_od *bmapod = bmap->bcm_od;
	int rc;

	BMAPOD_RDLOCK(bmap_2_bmdsi(bmap));
	psc_crc64_calc(&bmapod->bh_bhcrc, bmapod, BMAP_OD_CRCSZ);
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
	struct slmds_jent_ino_addrepl *jrir;

	jrir = (struct slmds_jent_ino_addrepl *) 
	    pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_ino_addrepl));

	jrir->sjir_fid = ((struct slmds_jent_ino_addrepl *)datap)->sjir_fid; 
	jrir->sjir_ios = ((struct slmds_jent_ino_addrepl *)datap)->sjir_ios; 
	jrir->sjir_pos = ((struct slmds_jent_ino_addrepl *)datap)->sjir_pos; 

	psc_trace("jlog fid=%"PRIx64" ios=%u pos=%u",
		  jrir->sjir_fid, jrir->sjir_ios, jrir->sjir_pos);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_INO_ADDREPL,
	    jrir, sizeof(struct slmds_jent_ino_addrepl));

	pjournal_put_buf(mdsJournal, (char *)jrir);
}

/**
 * mds_bmap_repl_log - Write a modified replication table to the journal.
 * Note:  bmap must be locked to prevent further changes from sneaking in
 *	before the repl table is committed to the journal.
 */
void
mds_bmap_repl_log(void *datap, uint64_t txg)
{
	struct bmapc_memb *bmap = (struct bmapc_memb *)datap;
	struct slmds_jent_repgen *jrpg;

	jrpg = (struct slmds_jent_repgen *) 
	    pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_repgen));

	jrpg->sjp_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jrpg->sjp_bmapno = bmap->bcm_blkno;
	jrpg->sjp_bgen = bmap_2_bgen(bmap);

	memcpy(jrpg->sjp_reptbl, bmap->bcm_od->bh_repls,
	       SL_REPLICA_NBYTES);

	psc_trace("jlog fid=%"PRIx64" bmapno=%u bmapgen=%u",
		  jrpg->sjp_fid, jrpg->sjp_bmapno, jrpg->sjp_bgen);

	pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_REPL,
	    (char *)jrpg, sizeof(struct slmds_jent_repgen));

	pjournal_put_buf(mdsJournal, (void *)jrpg);
}

/**
 * mds_bmap_crc_log - commit bmap crc changes to the journal.
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
	struct sl_mds_crc_log *crclog = (struct sl_mds_crc_log *)datap;
	struct bmapc_memb *bmap = crclog->scl_bmap;
	struct srm_bmap_crcup *crcup = crclog->scl_crcup;
	struct slmds_jent_crc *jcrc;
	struct bmap_mds_info *bmdsi = bmap->bcm_pri;
	struct slash_bmap_od *bmapod = bmap->bcm_od;
	int i, n=crcup->nups;
	uint32_t t=0, j=0;

	/* No, I shouldn't need the lock.  Only this instance of this
	 *  call may remove the BMAP_MDS_CRC_UP bit.
	 */
	psc_assert(bmap->bcm_mode & BMAP_MDS_CRC_UP);

	jcrc = (struct slmds_jent_crc *) 
	    pjournal_get_buf(mdsJournal, sizeof(struct slmds_jent_crc));
	jcrc->sjc_fid = fcmh_2_mdsio_fid(bmap->bcm_fcmh);
	jcrc->sjc_ion = bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid;
	jcrc->sjc_bmapno = bmap->bcm_blkno;
	jcrc->sjc_ncrcs = n;
	jcrc->sjc_fsize = crcup->fsize;		/* largest known size */

	while (n) {
		i = MIN(SLJ_MDS_NCRCS, n);

		memcpy(jcrc->sjc_crc, &crcup->crcs[t],
		       i * sizeof(struct srm_bmap_crcwire));

		pjournal_add_entry(mdsJournal, txg, MDS_LOG_BMAP_CRC,
		    jcrc, sizeof(struct slmds_jent_crc));

		/* Apply the CRC update into memory AFTER recording them
		 *  in the journal. The lock should not be needed since the
		 *  BMAP_MDS_CRC_UP is protecting the crc table from other
		 *  threads who may like to update.  Besides at this moment,
		 *  on the ION updating us has the real story on this bmap's
		 *  CRCs and all I/O for this bmap is being directed to it.
		 */
		BMAPOD_WRLOCK(bmdsi);
		for (t+=i; j < t; j++) {
			bmapod->bh_crcs[(crcup->crcs[j].slot)].gc_crc =
				crcup->crcs[j].crc;

			bmapod->bh_crcstates[(crcup->crcs[j].slot)] =
				(BMAP_SLVR_DATA | BMAP_SLVR_CRC);

			DEBUG_BMAP(PLL_DEBUG, bmap, "slot(%d) crc(%"PRIx64")",
				   crcup->crcs[j].slot, crcup->crcs[j].crc);
		}
		BMAPOD_ULOCK(bmdsi);
		n -= i;
		psc_assert(n >= 0);
	}
	psc_assert(t == crcup->nups);
	/* Signify that the update has occurred.
	 */
	BMAP_LOCK(bmap);
	bmap->bcm_mode &= ~BMAP_MDS_CRC_UP;
	BMAP_ULOCK(bmap);

	pjournal_put_buf(mdsJournal, (void *)jcrc);
}

void
mds_journal_init(void)
{
	struct sl_mds_peerinfo *peerinfo;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	uint64_t txg;
	int n;

	txg = mdsio_first_txg();

	r = nodeResm->resm_res;

	if (r->res_jrnldev[0] == '\0')	
		xmkfn(r->res_jrnldev, "%s/%s", sl_datadir, SL_FN_OPJOURNAL);
              
	mdsJournal = pjournal_init(r->res_jrnldev, txg, SLMTHRT_JRNL_DISTILL, 
			   "slmjdistthr", mds_replay_handler, mds_distill_handler);

	if (mdsJournal == NULL)
		psc_fatal("Fail to load/replay log file %s", r->res_jrnldev);

	logentrysize = mdsJournal->pj_hdr->pjh_entsz;
	psc_assert(logentrysize >= (int)sizeof(struct slmds_jent_namespace));

	logPndgReqs = pscrpc_nbreqset_init(NULL, mds_namespace_rpc_cb);

	stagebuf = PSCALLOC(SLM_NAMESPACE_BATCH * logentrysize);

	/*
	 * Construct a list of MDSes from the global configuration file
	 * to save some run time.  It also allows us to dynamically add
	 * or remove MDSes to/from our private list in the future.
	 */
	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, n, &s->site_resources) {
			if (r->res_type != SLREST_MDS)
				continue;

			/* MDS cannot have more than one member */
			resm = psc_dynarray_getpos(&r->res_members, 0);

			peerinfo = res2rpmi(r)->rpmi_peerinfo;
			peerinfo->sp_resm = resm;
			peerinfo->sp_siteid = s->site_id;

			psc_dynarray_add(&mds_namespace_peerlist, peerinfo);
			if (resm == nodeResm) {
				localinfo = peerinfo;
				psc_info("Added  local MDS: addr = %s, site ID = %d, "
				    "resource ID = %"PSCPRIxLNID,
				    resm->resm_addrbuf, s->site_id, resm->resm_nid);
			} else
				psc_info("Added remote MDS: addr = %s, site ID = %d, "
				    "resource ID = %"PSCPRIxLNID,
				    resm->resm_addrbuf, s->site_id, resm->resm_nid);
		}

	PLL_ULOCK(&globalConfig.gconf_sites);
	if (localinfo == NULL)
		psc_fatal("missing local MDS information");
	psc_dynarray_sort(&mds_namespace_peerlist, qsort, mds_peerinfo_cmp);

	/*
	 * Start a thread to propagate local namespace updates to peers
	 * after our MDS peer list has been all setup.
	 */
	namespaceThr = pscthr_init(SLMTHRT_JRNL_SEND, 0, 
			   mds_namespace_propagate, NULL, 0, "slmjsendthr");
	/*
	 * Eventually we have to read this from a on-disk log.
	 */
	next_update_seqno = 0;
}
