/* $Id$ */

#ifndef __SL_JOURNAL__
#define __SL_JOURNAL__ 1

#include "psc_types.h"

#include "inode.h"
#include "slconfig.h"
#include "pathnames.h"
#include "slashrpc.h" /* struct srm_bmap_crcup */

#define SLJ_MDS_JNENTS		131072
#define SLJ_MDS_RA              1024 /* SLJ_MDS_JNENTS % SLJ_MDS_RA == 0 */
#define SLJ_MDS_NCRCS           28

#define SLJ_MDS_PJET_VOID	  0
#define SLJ_MDS_PJET_INUM	  1
#define SLJ_MDS_PJET_BMAP         2
#define SLJ_MDS_PJET_INODE        3


#ifdef INUM_SELF_MANAGE
#define SLMDS_INUM_ALLOC_SZ	1024	/* allocate 1024 inums at a time */

struct slmds_jent_inum {
	sl_inum_t			sji_inum;
} __attribute__ ((packed));
#endif

/* 
 * slmds_jent_crc - is used to log crc updates which come from the ION's.  
 * @sjc_ion: the ion who sent the request.  Upon reboot this is used to 
 *    rebuild the mds's bmap <-> ion associations so that retried crc 
 *    updates (ion -> mds) may succeed.
 * @sjc_fid: what file.
 * @sjc_bmapno: which bmap region.
 * @sjc_crc: array of slots and crcs.
 * Notes: I presume that this will be the most common operation into the 
 *    journal.   
 */
struct slmds_jent_crc {
	sl_ios_id_t             sjc_ion; /* Track the ion which did the I/O */
	slfid_t                 sjc_fid;
	sl_blkno_t              sjc_bmapno;
	uint64_t                sjc_fsize;
	uint32_t                sjc_ncrcs;
	struct srm_bmap_crcwire sjc_crc[SLJ_MDS_NCRCS];
} __attribute__ ((packed));

#define slion_jent_crc slmds_jent_crc

/* 
 * slmds_jent_repgen - log changes to the replication state of a bmap which
 *    occur upon processing a new write for a replicated bmap.
 * @sjp_fid: what file.
 * @sjp_bmapno: which bmap region.
 * @sjp_gen: the new bmap generation.
 * @sjp_reptbl: the replica table.
 */
struct slmds_jent_repgen {
	slfid_t               sjp_fid;
	sl_blkno_t            sjp_bmapno;
	sl_blkgen_t           sjp_gen;
	uint8_t               sjp_reptbl[SL_REPLICA_NBYTES];
} __attribute__ ((packed));


/*
 * slmds_jent_ino_addrepl - add a new replica IOS to the inode or the inode 
 *    extras.
 * @sjir_fid: what file.
 * @sjir_ios: the IOS being added.
 * @sjir_pos: the slot or position the replica IOS is to be added to.
 */
struct slmds_jent_ino_addrepl {
	slfid_t       sjir_fid;
	sl_ios_id_t   sjir_ios;
	uint32_t      sjir_pos;
} __attribute__ ((packed));


/* List all of the journaling structures here so that the maximum
 *  size can be obtained.
 */
struct slmds_jents {
	union {
		struct slmds_jent_repgen sjr;
		struct slmds_jent_crc sjc;
		struct slmds_jent_ino_addrepl sjia;	
	} slmds_jent_types;
};

//#define SLJ_MDS_ENTSIZE (sizeof(struct slmds_jents))
/* A bit of a hack.  Make SLJ_MDS_ENTSIZE + sizeof(struct psc_journal_enthdr)
 *  == to 512, so long as 512 > than the size of any the structs above.
 */
#define SLJ_MDS_ENTSIZE 480

#endif
