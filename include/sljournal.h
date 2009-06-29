/* $Id$ */

#ifndef __SL_JOURNAL__
#define __SL_JOURNAL__ 1

#include "psc_types.h"

#include "inode.h"
#include "slconfig.h"
#include "pathnames.h"
#include "slashrpc.h" /* struct srm_bmap_crcup */

#define SLJ_MDS_JNENTS		10485760
#define SLJ_MDS_RA              200

#define SLMDS_INUM_ALLOC_SZ	1024	/* allocate 1024 inums at a time */

#define SLJ_MDS_PJET_VOID	  0
#define SLJ_MDS_PJET_INUM	  1
#define SLJ_MDS_PJET_BMAP         2
#define SLJ_MDS_PJET_INODE        3

struct slmds_jent_inum {
	sl_inum_t			sji_inum;
} __attribute__ ((packed));


#define SLJ_MDS_NCRCS 64

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
	u32                     sjc_ncrcs;
	struct srm_bmap_crcwire sjc_crc[SLJ_MDS_NCRCS];
} __attribute__ ((packed));

/* Track replication table changes.
 */
struct slmds_jent_repgen {
	slfid_t               sjp_fid;
	sl_blkno_t            sjp_bmapno;
	sl_blkgen_t           sjp_gen;
	u8                    sjp_reptbl[SL_REPLICA_NBYTES];
} __attribute__ ((packed));


#define SLJ_MDS_ENTSIZE MAX(MAX((sizeof(struct slmds_jent_inum)),	\
				(sizeof(struct slmds_jent_crc))),	\
			    sizeof(struct slmds_jent_repgen))

#define slion_jent_crc slmds_jent_crc

#endif
