/* $Id$ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/param.h>

#include <string.h>
#include <inttypes.h>

#include "psc_util/assert.h"
#include "psc_util/crc.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "sljournal.h"
#include "inode.h"
#include "fidcache.h"
#include "jflush.h"
#include "mdsexpc.h"


#ifdef INUM_SELF_MANAGE
#include "sb.h"
#endif

#include "slashdthr.h"

extern list_cache_t dirtyMdsData;
struct psc_journal mdsJournal;

enum mds_log_types {
#ifdef INUM_SELF_MANAGE
	MDS_LOG_SB        = (1<<0),
#endif
	MDS_LOG_BMAP_REPL = (1<<1),
	MDS_LOG_BMAP_CRC  = (1<<2),
	MDS_LOG_INODE     = (1<<3)
};


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
	struct bmapc_memb *bmap=data;
	struct slash_bmap_od *bmapod=bmap_2_bmdsiod(bmap);
	int rc;

	/* XXX At some point this lock should really be changed to
	 *  a pthread_rwlock.
	 */
	BMAP_LOCK(bmap);
	psc_crc_calc(&bmapod->bh_bhcrc, bmapod, BMAP_OD_CRCSZ);
	rc = mdsio_zfs_bmap_write(bmap);
	if (rc != BMAP_OD_SZ)
		DEBUG_BMAP(PLL_FATAL, bmap, "rc=%d errno=%d sync fail", rc, errno);
	else
		DEBUG_BMAP(PLL_TRACE, bmap, "sync ok");
	BMAP_ULOCK(bmap);
}

/**
 * mds_bmap_repl_log - write a modified replication table to the journal.
 * Note:  bmap must be locked to prevent further changes from sneaking in
 *	before the repl table is committed to the journal.
 */
void
mds_bmap_repl_log(struct bmapc_memb *bmap)
{
	struct slmds_jent_repgen jrpg;
	struct bmap_mds_info *bmdsi = bmap->bcm_pri;

	int rc;

	BMAP_LOCK_ENSURE(bmap);

	jrpg.sjp_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jrpg.sjp_bmapno = bmap->bcm_blkno;
	jrpg.sjp_gen.bl_gen = bmap_2_bmdsiod(bmap)->bh_gen.bl_gen;
	memcpy(jrpg.sjp_reptbl, bmap_2_bmdsiod(bmap)->bh_repls,
	       SL_REPLICA_NBYTES);

	psc_trace("jlog fid=%"_P_U64"x bmapno=%u bmapgen=%u",
		  jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_gen.bl_gen);

	jfi_prep(&bmdsi->bmdsi_jfi, &mdsJournal);

	psc_assert(bmdsi->bmdsi_jfi.jfi_handler == mds_bmap_sync);
	psc_assert(bmdsi->bmdsi_jfi.jfi_data == bmap);

	rc = pjournal_xadd(bmdsi->bmdsi_jfi.jfi_xh, MDS_LOG_BMAP_REPL, &jrpg);
	if (rc)
		psc_fatalx("jlog fid=%"_P_U64"x bmapno=%u bmapgen=%u rc=%d",
			   jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_gen.bl_gen, rc);

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
	struct slmds_jent_crc jcrc;
	struct bmap_mds_info *bmdsi = bmap->bcm_pri;
	struct slash_bmap_od *bmapod = bmdsi->bmdsi_od;
	int i, rc=0;
	int n=crcup->nups;
	u32 t=0, j=0;

	jfi_prep(&bmdsi->bmdsi_jfi, &mdsJournal);

	psc_assert(bmdsi->bmdsi_jfi.jfi_handler == mds_bmap_sync);
	psc_assert(bmdsi->bmdsi_jfi.jfi_data == bmap);
	/* No I shouldn't need the lock.  Only this instance of this
	 *  call may remove the BMAP_MDS_CRC_UP bit.
	 */
	psc_assert(bmap->bcm_mode & BMAP_MDS_CRC_UP);

	jcrc.sjc_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jcrc.sjc_ion = bmdsi->bmdsi_wr_ion->mi_resm->resm_nid;
        jcrc.sjc_bmapno = bmap->bcm_blkno;

	while (n) {
		i = MIN(SLJ_MDS_NCRCS, n);

		memcpy(jcrc.sjc_crc, &crcup->crcs[t],
		       (i * sizeof(struct srm_bmap_crcwire)));

		rc = pjournal_xadd(bmdsi->bmdsi_jfi.jfi_xh,
				   MDS_LOG_BMAP_CRC, &jcrc);
		if (rc)
			psc_fatalx("jlog fid=%"_P_U64"x bmapno=%u rc=%d",
				   jcrc.sjc_fid, jcrc.sjc_bmapno, rc);
		/* Apply the CRC update into memory AFTER recording them
		 *  in the journal.  The lock should not be needed since the
		 *  BMAP_MDS_CRC_UP is protecting the crc table from other
		 *  threads who may like to update.  Besides at this moment,
		 *  on the ION updating us has the real story on this bmap's
		 *  CRCs and all I/O for this bmap is being directed to it.
		 */
		//BMAP_LOCK(bmap);
		for (t+=i; j < t; j++) {
			bmapod->bh_crcs[(crcup->crcs[j].slot)].gc_crc =
				crcup->crcs[j].crc;
			DEBUG_BMAP(PLL_INFO, bmap, "slot(%d) crc(%"PRIx64")",
				   crcup->crcs[j].slot, crcup->crcs[j].crc);
		}
		//BMAP_ULOCK(bmap);
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
}

void
mds_journal_init(void)
{
	char fn[PATH_MAX];
	int rc;

	rc = snprintf(fn, sizeof(fn), "%s/%s",
                      nodeInfo.node_res->res_fsroot, _PATH_SLJOURNAL);
        if (rc == -1)
                psc_fatal("snprintf");

	pjournal_init(&mdsJournal, fn, 0,
		      SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE, SLJ_MDS_RA);
}

#ifdef INUM_SELF_MANAGE
void
mds_sb_sync(void *data)
{
	struct slash_sb_mem *sb=data;
	int rc;

	spinlock(&sb->sbm_lock);

	psc_crc_calc(&sb->sbm_sbs->sbs_crc, sb->sbm_sbs, SBS_OD_CRCSZ);
	rc = msync(sb->sbm_sbs, SBS_OD_SZ, MS_SYNC);
	if (rc < 0)
		psc_fatal("msync() of sb failed");

	freelock(&sb->sbm_lock);

	psc_dbg("sb=%p inum=%"_P_U64"x crc="_P_CRC" sync ok",
		sb, sb->sbm_sbs->sbs_inum, sb->sbm_sbs->sbs_crc);
}

/**
 * mds_sb_getinum - get the next inode number from the sb structure.
 * Notes:  the inode is composed of a counter of size SL_SUPER_INODE_BITS
 *    which is prepended by the mds id bits.
 * Notes1: mds_sb_getinum() does not hold the sb lock while performing
 *    journal I/O.  This is because the highest sji_inum number is taken
 *    from the journal (upon restart) not the value at the latest position.
 *    So in other words, out of order jentries for inum updates don't matter.
 */
sl_inum_t
mds_sb_getinum(void)
{
	struct slmds_jent_inum sji;
	sl_inum_t ino=0;

	spinlock(&sbm.sbm_lock);

	if (!((ino = ++sbm.sbm_inum_minor) % SLMDS_INUM_ALLOC_SZ)) {
		struct jflush_item *jfi = &sbm.sbm_jfi;

		sji.sji_inum = ++sbm.sbm_sbs->sbs_inum_major;
		freelock(&sbm.sbm_lock);

		psc_dbg("bumped inum_maj=%"_P_U64, sji.sji_inum);

		jfi_prep(jfi, &mdsJournal);
		psc_assert(jfi->jfi_handler == mds_sb_sync);
		psc_assert(jfi->jfi_data == &sbm);

		if (pjournal_xadd(jfi->jfi_xh, MDS_LOG_SB, &sji))
			psc_fatalx("jlog sb pjournal_xadd() failed");

		jfi_schedule(jfi, &dirtyMdsData);

	} else {
		sji.sji_inum = sbm.sbm_sbs.sbs_inum_major;
		freelock(&sbm.sbm_lock);
	}
	/* Add the major bits to the ones already stored in ino.
	 */
	ino += (sji.sji_inum * SLMDS_INUM_ALLOC_SZ);
	/* Prepend the mds_id bits to the ino.
	 */
	ino |= (sbm.sbm_sbs->sbs_mds_id << SL_SUPER_INODE_BITS);

	psc_trace("ino=%"_P_U64" inum_maj=%"_P_U64" mds_id=%u",
		  ino, sji.sji_inum, sbm.sbm_sbs->sbs_mds_id);

	return (ino);
}
#endif
