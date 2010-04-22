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
#include "psc_util/crc.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"
#include "psc_rpc/rsx.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "jflush.h"
#include "mdsio.h"
#include "mdslog.h"
#include "mkfn.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "sljournal.h"

struct psc_journal		 *mdsJournal;
static struct pscrpc_nbreqset	 *logPndgReqs;

static int			  logentrysize;

/*
 * Eventually, we are going to retrieve the namespace update sequence number
 * from the system journal.
 */
uint64_t			  next_update_seqno;
 
uint64_t			  next_propagate_seqno;

/*
 * Low and high water marks of update sequence numbers that need to be propagated.
 * Note that the pace of each MDS is different.
 */
static uint64_t			  propagate_seqno_lwm;
static uint64_t			  propagate_seqno_hwm;

static int			  current_logfile = -1;

struct psc_waitq		  mds_namespace_waitq = PSC_WAITQ_INIT;
psc_spinlock_t			  mds_namespace_waitqlock = LOCK_INITIALIZER;

/* max # of buffers used to decrease I/O */
#define	MDS_NAMESPACE_MAX_BUF	  8

/* max # of seconds before an update is propagated */
#define MDS_NAMESPACE_MAX_AGE	 30

/* a buffer used to read on-disk log file */
static char			*stagebuf;

/* we only have a few buffers, so a list is fine */
static struct psclist_head	 mds_namespace_buflist = PSCLIST_HEAD_INIT(mds_namespace_buflist);

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

/**
 * mds_replay_handle - Handle journal replay events.
 */
void
mds_replay_handler(__unusedx struct psc_dynarray *logentrys, __unusedx int *rc)
{
}

/**
 * mds_shadow_handler - Distill information from the system journal and
 *	write into change log files.
 */
void
mds_shadow_handler(struct psc_journal_enthdr *pje, int size)
{
	int sz;
	uint64_t seqno;
	char fn[PATH_MAX];
	struct slmds_jent_namespace *jnamespace;

	jnamespace = (struct slmds_jent_namespace *)&pje->pje_data[0];
	seqno = jnamespace->sjnm_seqno;

	/* see if we can open a new change log file */
	if ((seqno % SLM_NAMESPACE_BATCH) == 0) {
		psc_assert(current_logfile == -1);
		xmkfn(fn, "%s/%s.%d", SL_PATH_DATADIR, SL_FN_NAMESPACELOG, seqno);
		current_logfile = open(fn, O_RDWR | O_SYNC | O_DIRECT | O_APPEND);
		if (current_logfile == -1)
			psc_fatal("Fail to open change log file %s", fn);
	} else
		psc_assert(current_logfile != -1);

	sz = write(current_logfile, pje, size);
	if (sz != size)
		psc_fatal("Fail to write change log file %s", fn);

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
mds_namespace_log(int op, int type, int perm, uint64_t parent,
    uint64_t target, const char *name)
{
	int rc;
	struct slmds_jent_namespace *jnamespace;

	jnamespace = PSCALLOC(sizeof(struct slmds_jent_namespace));
	jnamespace->sjnm_op = op;
	jnamespace->sjnm_type = type;
	jnamespace->sjnm_perm = perm;
	jnamespace->sjnm_parent_s2id = parent;
	jnamespace->sjnm_target_s2id = target;
	jnamespace->sjnm_seqno = mds_get_next_seqno();
	strcpy(jnamespace->sjnm_name, name);

	rc = pjournal_xadd_sngl(mdsJournal, MDS_LOG_NAMESPACE, jnamespace,
		sizeof(struct slmds_jent_namespace));
	if (rc)
		psc_fatalx("jlog fid=%"PRIx64", name=%s, rc=%d", target, name, rc);

	PSCFREE(jnamespace);
}


__static int
mds_namespace_rpc_cb(__unusedx struct pscrpc_request *req,
		  struct pscrpc_async_args *args)
{
	struct slashrpc_cservice *csvc;
	struct sl_mds_loginfo *loginfo;

	loginfo = args->pointer_arg[0];
	sl_csvc_decref(csvc);
	return (0);
}

/*
 * mds_namespace_read - read a batch of updates from the corresponding log file
 *	and packed them for RPC later.
 *
 */
struct sl_mds_logbuf *
mds_namespace_read_batch(uint64_t seqno)
{
	int i;
	char *ptr;
	int nitems;
	int logfile;
	ssize_t size;
	char fn[PATH_MAX];
	struct psclist_head *tmp;
	struct sl_mds_logbuf *buf;
	struct sl_mds_logbuf *victim;
	struct slmds_jent_namespace *jnamespace;

restart:
	/*
	 * Currently, there is only one thread manipulating the list.
	 * But we do have to lock each individual buffer.
	 */
	i = 0;
	victim = NULL;
	psclist_for_each(tmp, &mds_namespace_buflist) {
		buf = psclist_entry(tmp, struct sl_mds_logbuf, slb_link);
		i++;
		if (buf->slb_seqno == seqno)
			break;
		if (!victim && buf->slb_refcnt == 0)
			victim = buf;
	}
	if (buf) {
		buf->slb_refcnt++;
		if (buf->slb_count == SLM_NAMESPACE_BATCH)
			return buf;
		goto readit;
	}
	if (i < MDS_NAMESPACE_MAX_BUF) {
		buf = PSCALLOC(sizeof(struct sl_mds_logbuf) + SLM_NAMESPACE_BATCH * logentrysize);
		buf->slb_refcnt = 1;
		buf->slb_count = 0;
		buf->slb_seqno = seqno;
		buf->slb_buf = (char *)buf + sizeof(struct sl_mds_logbuf);
		goto readit;
	}
	/* 
	 * If we are over the limit and we don't have a victim,
	 * wait until an RPC returns or times out.
	 */
	if (!victim) {
		goto restart;
	}
	buf = victim;
	buf->slb_refcnt = 1;
	buf->slb_count = 0;
	buf->slb_size = 0;
	buf->slb_seqno = seqno;
	psclist_del(&buf->slb_link);

readit:

	/*
	 * A short read is allowed, but the returned size must be a 
	 * multiple of the log entry size (should be 512 bytes).
	 */
	xmkfn(fn, "%s/%s.%d", SL_PATH_DATADIR, SL_FN_NAMESPACELOG, seqno);
	logfile = open(fn, O_RDONLY);
	lseek(logfile, buf->slb_count * logentrysize, SEEK_SET);
	size = read(logfile, stagebuf, 
		   (SLM_NAMESPACE_BATCH - buf->slb_count) * logentrysize);
	close(logfile);

	nitems = size / logentrysize;
	psc_assert((size % logentrysize) == 0);
	psc_assert(nitems + buf->slb_count <= SLM_NAMESPACE_BATCH);

	ptr = buf->slb_buf + buf->slb_size;
	jnamespace = (struct slmds_jent_namespace *)stagebuf + buf->slb_count * logentrysize;
	for (i = 0; i < nitems; i++) {
		memcpy((void *)ptr, (void *)jnamespace, jnamespace->sjnm_reclen);
		ptr += jnamespace->sjnm_reclen;
		buf->slb_size += jnamespace->sjnm_reclen;
		/* sizeof(struct slmds_jent_namespace) is less than logentrysize */
		jnamespace = (struct slmds_jent_namespace *)((char *)jnamespace + logentrysize);
	}

	psclist_xadd_head(&buf->slb_link, &mds_namespace_buflist);
	return buf;
}

/**
 * mds_namespace_propagate_batch - Send the newest batch of changes to
 *	peer MDSes that seem to be active.
 */
void
mds_namespace_propagate_batch(struct sl_mds_logbuf *buf)
{
	struct srm_send_namespace_req *mq;
	struct srm_generic_rep *mp;
	struct slashrpc_cservice *csvc;
	struct pscrpc_request *req;
	struct pscrpc_bulk_desc *desc;
	struct iovec iov;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	int rc, n;
	struct sl_mds_loginfo *loginfo;

	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, n, &s->site_resources) {
			if (r->res_type != SLREST_MDS)
				continue;

			/* MDS cannot have more than one member */
			resm = psc_dynarray_getpos(&r->res_members, 0);

			if (resm == nodeResm)
				continue;

			loginfo = ((struct resprof_mds_info *)r->res_pri)->rpmi_loginfo;
			/*
			 * Skip if the MDS is busy or the current batch is out of
			 * its windows.  Note for each MDS, we send updates in order.
			 */
			if (loginfo->sml_flags & SML_FLAG_INFLIGHT)
				continue;
			if (loginfo->sml_next_seqno < buf->slb_seqno ||
			    loginfo->sml_next_seqno >= buf->slb_seqno + buf->slb_count)
				continue;

			csvc = slm_getmcsvc(resm);
			if (csvc == NULL)
				continue;
			rc = SL_RSX_NEWREQ(csvc->csvc_import, SRMM_VERSION,
			    SRMT_SEND_NAMESPACE, req, mq, mp);
			if (rc) {
				sl_csvc_decref(csvc);
				continue;
			}

			rc = rsx_bulkclient(req, &desc, BULK_GET_SOURCE,
			    SRMM_BULK_PORTAL, &iov, 1);

			req->rq_async_args.pointer_arg[0] = loginfo;
			pscrpc_nbreqset_add(logPndgReqs, req);
		}
	PLL_ULOCK(&globalConfig.gconf_sites);
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
	 * The thread scans the batches of changes between the low and high
	 * water marks and sends them to peer MDSes.  Although different MDSes
	 * have different paces, we send updates in order within one MDS.
	 */
	seqno = propagate_seqno_lwm;
	while (pscthr_run()) {
		if (seqno < propagate_seqno_hwm) {
			buf = mds_namespace_read_batch(seqno);
			mds_namespace_propagate_batch(buf);
			seqno += SLM_NAMESPACE_BATCH;
			continue;
		}
		spinlock(&mds_namespace_waitqlock);
		rv = psc_waitq_waitrel_s(&mds_namespace_waitq,
		    &mds_namespace_waitqlock, MDS_NAMESPACE_MAX_AGE);
		seqno = propagate_seqno_lwm;
	}
}

void
mds_inode_sync(void *data)
{
	int locked, rc, tmpx = 0;
	struct slash_inode_handle *inoh = data;

	locked = reqlock(&inoh->inoh_lock);

	psc_assert((inoh->inoh_flags & INOH_INO_DIRTY) ||
		   (inoh->inoh_flags & INOH_EXTRAS_DIRTY));

	if (inoh->inoh_flags & INOH_INO_DIRTY) {
		psc_crc64_calc(&inoh->inoh_ino.ino_crc,
			     &inoh->inoh_ino, INO_OD_CRCSZ);
		rc = mdsio_inode_write(inoh);

		if (rc)
			DEBUG_INOH(PLL_FATAL, inoh, "rc=%d sync fail", rc);

		inoh->inoh_flags &= ~INOH_INO_DIRTY;
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

	ureqlock(&inoh->inoh_lock, locked);
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

	/* XXX At some point this lock should really be changed to
	 *  a pthread_rwlock.
	 */
	BMAP_LOCK(bmap);
	psc_crc64_calc(&bmapod->bh_bhcrc, bmapod, BMAP_OD_CRCSZ);
	rc = mdsio_bmap_write(bmap);
	if (rc)
		DEBUG_BMAP(PLL_FATAL, bmap, "rc=%d errno=%d sync fail",
			   rc, errno);
	else
		DEBUG_BMAP(PLL_INFO, bmap, "sync ok");

	BMAP_ULOCK(bmap);

	bmap_op_done_type(bmap, BMAP_OPCNT_MDSLOG);
}

void
mds_inode_addrepl_log(struct slash_inode_handle *inoh, sl_ios_id_t ios,
		      uint32_t pos)
{
	int rc;
	struct slmds_jent_ino_addrepl jrir = { fcmh_2_fid(inoh->inoh_fcmh),
					       ios, pos };

	INOH_LOCK_ENSURE(inoh);
	psc_assert((inoh->inoh_flags & INOH_INO_DIRTY) ||
		   (inoh->inoh_flags & INOH_EXTRAS_DIRTY));

	psc_trace("jlog fid=%"PRIx64" ios=%u pos=%u",
		  jrir.sjir_fid, jrir.sjir_ios, jrir.sjir_pos);

	jfi_prep(&inoh->inoh_jfi, mdsJournal);
	psc_assert(inoh->inoh_jfi.jfi_handler == mds_inode_sync);
	psc_assert(inoh->inoh_jfi.jfi_data == inoh);

	rc = pjournal_xadd(inoh->inoh_jfi.jfi_xh, MDS_LOG_INO_ADDREPL, &jrir,
			   sizeof(struct slmds_jent_ino_addrepl));
	if (rc)
		psc_trace("jlog fid=%"PRIx64" ios=%x pos=%u rc=%d",
			  jrir.sjir_fid, jrir.sjir_ios, jrir.sjir_pos, rc);

	jfi_schedule(&inoh->inoh_jfi, &dirtyMdsData);
}

/**
 * mds_bmap_repl_log - Write a modified replication table to the journal.
 * Note:  bmap must be locked to prevent further changes from sneaking in
 *	before the repl table is committed to the journal.
 * XXX Another case for a rwlock, currently this code holds the lock while
 *     doing I/O to the journal.
 */
void
mds_bmap_repl_log(struct bmapc_memb *bmap)
{
	struct slmds_jent_repgen jrpg;
	struct bmap_mds_info *bmdsi = bmap->bcm_pri;
	int rc;

	BMAP_LOCK_ENSURE(bmap);

	DEBUG_BMAPOD(PLL_INFO, bmap, "");

	jrpg.sjp_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jrpg.sjp_bmapno = bmap->bcm_blkno;
	jrpg.sjp_bgen = bmap_2_bgen(bmap);
	memcpy(jrpg.sjp_reptbl, bmap->bcm_od->bh_repls,
	       SL_REPLICA_NBYTES);

	psc_trace("jlog fid=%"PRIx64" bmapno=%u bmapgen=%u",
		  jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_bgen);

	jfi_prep(&bmdsi->bmdsi_jfi, mdsJournal);

	psc_assert(bmdsi->bmdsi_jfi.jfi_handler == mds_bmap_sync);
	psc_assert(bmdsi->bmdsi_jfi.jfi_data == bmap);

	rc = pjournal_xadd(bmdsi->bmdsi_jfi.jfi_xh, MDS_LOG_BMAP_REPL, &jrpg,
			   sizeof(struct slmds_jent_repgen));
	if (rc)
		psc_fatalx("jlog fid=%"PRIx64" bmapno=%u bmapgen=%u rc=%d",
			   jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_bgen,
			   rc);
	jfi_schedule(&bmdsi->bmdsi_jfi, &dirtyMdsData);
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
mds_bmap_crc_log(struct bmapc_memb *bmap, struct srm_bmap_crcup *crcup)
{
	struct slmds_jent_crc *jcrc = PSCALLOC(sizeof(struct slmds_jent_crc));
	struct bmap_mds_info *bmdsi = bmap->bcm_pri;
	struct slash_bmap_od *bmapod = bmap->bcm_od;
	int i, rc=0;
	int n=crcup->nups;
	uint32_t t=0, j=0;

	mdsio_apply_fcmh_size(bmap->bcm_fcmh, crcup->fsize);

	jfi_prep(&bmdsi->bmdsi_jfi, mdsJournal);

	psc_assert(bmdsi->bmdsi_jfi.jfi_handler == mds_bmap_sync);
	psc_assert(bmdsi->bmdsi_jfi.jfi_data == bmap);
	/* No I shouldn't need the lock.  Only this instance of this
	 *  call may remove the BMAP_MDS_CRC_UP bit.
	 */
	psc_assert(bmap->bcm_mode & BMAP_MDS_CRC_UP);

	jcrc->sjc_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jcrc->sjc_ion = bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid;
	jcrc->sjc_bmapno = bmap->bcm_blkno;
	jcrc->sjc_ncrcs = n;

	while (n) {
		i = MIN(SLJ_MDS_NCRCS, n);

		memcpy(jcrc->sjc_crc, &crcup->crcs[t],
		       (i * sizeof(struct srm_bmap_crcwire)));

		rc = pjournal_xadd(bmdsi->bmdsi_jfi.jfi_xh, MDS_LOG_BMAP_CRC,
				   jcrc, sizeof(struct slmds_jent_crc));
		if (rc)
			psc_fatalx("jlog fid=%"PRIx64" bmapno=%u rc=%d",
				   jcrc->sjc_fid, jcrc->sjc_bmapno, rc);
		/* Apply the CRC update into memory AFTER recording them
		 *  in the journal. The lock should not be needed since the
		 *  BMAP_MDS_CRC_UP is protecting the crc table from other
		 *  threads who may like to update.  Besides at this moment,
		 *  on the ION updating us has the real story on this bmap's
		 *  CRCs and all I/O for this bmap is being directed to it.
		 */
		BMAP_LOCK(bmap);
		for (t+=i; j < t; j++) {
			bmapod->bh_crcs[(crcup->crcs[j].slot)].gc_crc =
				crcup->crcs[j].crc;

			bmapod->bh_crcstates[(crcup->crcs[j].slot)] =
				(BMAP_SLVR_DATA | BMAP_SLVR_CRC);

			DEBUG_BMAP(PLL_INFO, bmap, "slot(%d) crc(%"PRIx64")",
				   crcup->crcs[j].slot, crcup->crcs[j].crc);
		}
		BMAP_ULOCK(bmap);
		n -= i;
		psc_assert(n >= 0);
	}
	psc_assert(t == crcup->nups);
	/* Signify that the update has occurred.
	 */
	BMAP_LOCK(bmap);
	bmap->bcm_mode &= ~BMAP_MDS_CRC_UP;
	BMAP_ULOCK(bmap);
	/* Tell the 'syncer' thread to flush this bmap.
	 */
	jfi_schedule(&bmdsi->bmdsi_jfi, &dirtyMdsData);

	PSCFREE(jcrc);
}


void
mds_journal_init(void)
{
	char fn[PATH_MAX];
	struct psc_thread *thr;

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_OPJOURNAL);
	mdsJournal = pjournal_init(fn, SLMTHRT_JRNL_SHDW, "slmjshdwthr",
	    mds_replay_handler, mds_shadow_handler);
	if (mdsJournal == NULL)
		psc_fatal("Fail to load/replay log file %s", fn);

	logentrysize = mdsJournal->pj_hdr->pjh_entsz;

	/* start a thread to propagate local namespace changes */
	thr = pscthr_init(SLMTHRT_JRNL_SEND, 0, mds_namespace_propagate,
	    NULL, 0, "slmjsendthr");

	logPndgReqs = pscrpc_nbreqset_init(NULL, mds_namespace_rpc_cb);
			
	stagebuf = PSCALLOC(SLM_NAMESPACE_BATCH * logentrysize);
}
