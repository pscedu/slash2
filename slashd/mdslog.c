#include <sys/mman.h>

#include "psc_util/journal.h"

#include "sljournal.h"
#include "inode.h"
#include "fidcache.h"
#include "jflush.h"
#include "sb.h"
#include "slashd.h"

extern list_cache_t dirtyMdsData;
struct psc_journal mdsJournal;

enum mds_log_types {	
	MDS_LOG_BMAP_REPL = (1<<0),
	MDS_LOG_BMAP_CRC  = (1<<1),    
	MDS_LOG_INODE     = (1<<2),
	MDS_LOG_SB        = (1<<3)
};

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
	
	psc_dbg("sb=%p inum=%"_P_U64"x crc=%"_P_U64"x sync ok", 
		sb, sb->sbm_sbs->sbs_inum, sb->sbm_sbs->sbs_crc);
}

/**
 * mds_sb_getinum - get the next inode number from the sb structure.
 * Notes:  the inode is composed of a counter of size SL_SUPER_INODE_BITS which is prepended by the mds id bits.
 * Notes1: mds_sb_getinum() does not hold the sb lock while performing journal I/O.  This is because the highest sji_inum number is taken from the journal (upon restart) not the value at the latest position.  So in other words, out of order jentries for inum updates don't matter.
 */
sl_inum_t 
mds_sb_getinum(void)
{
	struct slmds_jent_inum sji;
	sl_inum_t ino=0;

	spinlock(&slSuperBlk.sbm_lock);
	
	if (!((ino = ++slSuperBlk.sbm_inum_minor) % SLASH_INUM_ALLOC_SZ)) {
		struct jflush_item *jfi = &slSuperBlk.sbm_jfi;
		
		sji.sji_inum = ++slSuperBlk.sbm_sbs->sbs_inum_major;
		freelock(&slSuperBlk.sbm_lock);		

		psc_dbg("bumped inum_maj=%"_P_U64, sji.sji_inum);
		
		jfi_prep(jfi, &mdsJournal);
		psc_assert(jfi->jfi_handler == mds_sb_sync);
		psc_assert(jfi->jfi_data == &slSuperBlk);
		
		if (pjournal_xadd(jfi->jfi_xh, MDS_LOG_SB, &sji))
			psc_fatalx("jlog sb pjournal_xadd() failed");
		
		jfi_schedule(jfi, &dirtyMdsData);

	} else {
		sji.sji_inum = slSuperBlk.sbm_sbs.sbs_inum_major;
		freelock(&slSuperBlk.sbm_lock);
	}
	/* Add the major bits to the ones already stored in ino.
	 */
	ino += (sji.sji_inum * SLASH_INUM_ALLOC_SZ);
	/* Prepend the mds_id bits to the ino.
	 */
	ino |= (slSuperBlk.sbm_sbs->sbs_mds_id << SL_SUPER_INODE_BITS);

	psc_trace("ino=%"_P_U64" inum_maj=%"_P_U64" mds_id=%u", 
		  ino, sji.sji_inum, slSuperBlk.sbm_sbs->sbs_mds_id);

	return (ino);
}

/**
 * mds_bmap_sync - callback function which is called from mdsfssyncthr_begin().
 * @data: void * which is the bmap.
 * Notes: this call allows slash2 to optimize crc calculation by only taking them when the bmap is written, not upon each update to the bmap.  It is important to note that forward changes may be synced here.  What that means is that changes which are not part of this XID session may have snuck in here (ie a crc update came in and was fully processed before mds_bmap_sync() grabbed the lock.  For this reason the crc updates must be journaled before manifesting in the bmap cache.
 */
void 
mds_bmap_sync(void *data)
{
	struct bmapc_memb *bmap=data;
	sl_blkh_t *bmapod=bmap->bcm_bmapih.bmapi_data;
	int rc;
	       
	BMAP_LOCK(bmap);

	psc_crc_calc(&bmapod->bh_bhcrc, bmapod, BMAP_OD_CRCSZ);
	rc = pwrite(bmap->bcm_fcmh->fcmh_fd, bmapod, 
		    BMAP_OD_SZ, (off_t)(BMAP_OD_SZ * bmap->bcm_blkno));

	if (rc != BMAP_OD_SZ)
		DEBUG_BMAP(PLL_FATAL, "rc=%d errno=%d sync fail", rc, errno);
	else
		DEBUG_BMAP(PLL_TRACE, "sync ok");
	
	BMAP_ULOCK(bmap);
}

/**
 * mds_bmap_repl_log - write a modified replication table to the journal.
 * Note:  bmap must be locked to prevent further changes from sneaking in before the repl table is committed to the journal.
 */
void
mds_bmap_repl_log(struct bmapc_memb *bmap) 
{
	struct slmds_jent_repgen jrpg;
	int rc;

	BMAP_LOCK_ENSURE(bmap);
	
	jrpg.sjp_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jrpg.sjp_bmapno = bmap->bcm_blkno;
	jrpg.sjp_gen = bmap->bcm_bmapih.bmapi_data->bh_gen.bl_gen;
	memcpy(jrpg.sjp_reptbl, bmap->bcm_bmapih.bmapi_data->bh_repls, 
	       SL_REPLICA_NBYTES);

	psc_trace("jlog fid=%"_P_U64" bmapno=%u bmapgen=%u", 
		  jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_gen);
	
	jfi_prep(&bmap->bcm_jfi, &mdsJournal);

	psc_assert(bmap->bcm_jfi->jfi_handler == mds_bmap_sync);
	psc_assert(bmap->bcm_jfi->jfi_data == bmap);

	rc = pjournal_xadd(bmap->bcm_jfi->jfi_xh, MDS_LOG_BMAP_REPL, &rg);
	if (rc) 
		psc_fatalx("jlog fid=%"_P_U64" bmapno=%u bmapgen=%u rc=%d",
			   jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_gen, rc);
	
	jfi_schedule(&bmap->bcm_jfi, &dirtyMdsData);
}

/**
 * mds_bmap_crc_log - commit bmap crc changes to the journal.  
 * @bmap: the bmap (not locked).
 * @crcs: array of crc / slot pairs.
 * @n: the number of crc / slot pairs.
 * Notes: bmap_crc_writes from the ION are sent here directly because this function is responsible for updating the cached bmap after the crc has been committed to the journal.  This allows us to not have to hold the lock while doing journal I/O with the caveat that we trust the ION to not send multiple crc updates for the same region which me may then process out of order.
 */
void
mds_bmap_crc_log(struct bmapc_memb *bmap, struct slmds_bmap_crc *crcs, int n)
{
	struct slmds_jent_crc jcrc;
	sl_blkh_t *bmapod=bmap->bcm_bmapih.bmapi_data;
	int i, j=0, t=0, rc=0;

	jfi_prep(&bmap->bcm_jfi, &mdsJoural);

	psc_assert(bmap->bcm_jfi->jfi_handler == mds_bmap_sync);
	psc_assert(bmap->bcm_jfi->jfi_data == bmap);

	jcrc.sjc_fid = fcmh_2_fid(bmap->bcm_fcmh);
	jcrc.sjc_ion = bmap->bcm_bmapih.bmapi_ion;
        jcrc.sjc_bmapno = bmap->bcm_blkno;
	
	while (n) {
		i = MIN(SLJ_MDS_NCRCS, n);
		memcpy(jcrc.sjc_crc, crcs[i],
		       (i * sizeof(struct slmds_bmap_crc)));

		rc = pjournal_xadd(bmap->bcm_jfi->jfi_xh, 
				   MDS_LOG_BMAP_CRC, &jcrc);
		if (rc)
			psc_fatalx("jlog fid=%"_P_U64" bmapno=%u rc=%d", 
				   jcrc.sjp_fid, jcrc.sjp_bmapno, rc);
		/* Apply the CRC update into memory AFTER recording them
		 *  in the journal.
		 */
		BMAP_LOCK(bmap);
		for (t+=i; j < t; j++) {
			bmapod->bh_crcs[(crcs[j].slot)] = crcs[j].crc;
			DEBUG_BMAP(PLL_INFO, bmap, "slot(%d) crc(%"_P_U64"x)", 
				   crcs[j].slot, crcs[j].crc);
		}
		BMAP_ULOCK(bmap);

		n -= i;
	}
	jfi_schedule(&bmap->bcm_jfi, &dirtyMdsData);
}

void
mds_journal_init(void)
{
	char fn[PATH_MAX];
	
	rc = snprintf(fn, sizeof(fn), "%s/%s",
                      nodeInfo.node_res->res_fsroot, _PATH_SLJOURNAL);
        if (rc == -1)
                psc_fatal("snprintf");

	pjournal_init(&mdsJournal, fn, 0, 
		      SLJ_MDS_JNENTS, SLJ_MDS_ENTSIZE, SLJ_MDS_RA);
}
