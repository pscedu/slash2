#include "psc_util/journal.h"

#include "sljournal.h"
#include "inode.h"
#include "fidcache.h"
#include "jflush.h"

extern list_cache_t dirtyMdsData;
struct psc_journal mdsJournal;

enum mds_log_types {	
	MDS_LOG_BMAP_REPL = (1<<0),
	MDS_LOG_BMAP_CRC  = (1<<1),    
	MDS_LOG_INODE     = (1<<2),
};

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
	
	jfi_prep(&bmap->bcm_jfi);

	psc_assert(bmap->bcm_jfi->jfi_handler == mds_bmap_sync);
	psc_assert(bmap->bcm_jfi->jfi_data == bmap);

	rc = pjournal_xadd(bmap->bcm_jfi->jfi_xh, MDS_LOG_BMAP_REPL, &rg);
	if (rc) 
		psc_fatalx("jlog fid=%"_P_U64" bmapno=%u bmapgen=%u rc=%d",
			   jrpg.sjp_fid, jrpg.sjp_bmapno, jrpg.sjp_gen, rc);
	
	jfi_schedule(&bmap->bcm_jfi, &dirtyMdsData);
}

void
mds_bmap_crc_log(struct bmapc_memb *bmap, struct slmds_bmap_crc *crcs, int n)
{
	struct slmds_jent_crc jcrc;
	sl_blkh_t *bmapod=bmap->bcm_bmapih.bmapi_data;
	int i, j=0, t=0, rc=0;

	jfi_prep(&bmap->bcm_jfi);

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
