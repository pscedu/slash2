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

#include "psc_util/crc.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "bmap.h"
#include "bmap_mds.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "inode.h"
#include "jflush.h"
#include "mdsio.h"
#include "mdslog.h"
#include "mkfn.h"
#include "slashd.h"
#include "sljournal.h"

struct psc_journal	*mdsJournal;

/*
 * Eventually, we are going to retrieve the namespace update sequence number
 * from the system journal.
 */
uint64_t		 next_update_seqno;

int			 current_logfile = -1;

/*
 * The number of namespace operations that are recorded in the same change log.
 */
#define MDS_NAMESPACE_BATCH	4096

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

/* master journal log replay function */
void
mds_journal_replay(__unusedx struct psc_dynarray *logentrys, __unusedx int *rc)
{
}

/* Distill information from the system journal and write into change log files */
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
	if ((seqno % MDS_NAMESPACE_BATCH) == 0) {
		psc_assert(current_logfile == -1);
        	xmkfn(fn, "%s/%s", SL_PATH_DATADIR, SL_FN_NAMESPACELOG);
		current_logfile = open(fn, O_RDWR | O_SYNC | O_DIRECT | O_APPEND);
		if (current_logfile == -1)
			psc_fatal("Fail to open change log file %s", fn);
	} else 
		psc_assert(current_logfile != -1);

	sz = write(current_logfile, pje, size);
	if (sz != size)
		psc_fatal("Fail to write change log file %s", fn);
 
	/* see if we need to close the current change log file */
	if (((seqno + 1) % MDS_NAMESPACE_BATCH) == 0) {
		close(current_logfile);
		current_logfile = -1;
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
 * mds_bmap_sync - callback function which is called from
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
 * mds_bmap_repl_log - write a modified replication table to the journal.
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

/*
 * Log namespace operation before we attempt the operation.  This makes sure
 * that it will be propagated towards other MDSes and made permanent before
 * we reply to the client.
 */
void
mds_namespace_log(int op, int type, int perm, uint64_t s2id, const char *name)
{
	int rc;
	struct slmds_jent_namespace *jnamespace;

	jnamespace = PSCALLOC(sizeof(struct slmds_jent_namespace));
	jnamespace->sjnm_op = op;
	jnamespace->sjnm_type = type;
	jnamespace->sjnm_perm = perm;
	jnamespace->sjnm_target_s2id = s2id;
	jnamespace->sjnm_seqno = mds_get_next_seqno();
	strcpy(jnamespace->sjnm_name, name);

	rc = pjournal_xadd_sngl(mdsJournal, MDS_LOG_NAMESPACE, jnamespace,
		sizeof(struct slmds_jent_namespace));
	if (rc)
		psc_fatalx("jlog fid=%"PRIx64", name=%s, rc=%d", s2id, name, rc);

	PSCFREE(jnamespace);
}

void
mds_journal_init(void)
{
	char fn[PATH_MAX];

	xmkfn(fn, "%s/%s", sl_datadir, SL_FN_OPJOURNAL);
	mdsJournal = pjournal_replay(fn, mds_journal_replay, mds_shadow_handler);
	if (mdsJournal == NULL)
		psc_fatal("Fail to load/replay log file %s", fn);
}
