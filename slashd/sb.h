/* $Id$ */

#ifndef __SLASH_SB_H__
#define __SLASH_SB_H__ 1

#include <sys/types.h>

#include "psc_util/journal.h"

#include "jflush.h"
#include "inode.h"

#define SL_SUPER_MAGIC 0x66ffee0110eeff66ULL
#define SL_SUPER_INODE_BITS ((sizeof(sl_inum_t) * NBBY) - SL_MDS_ID_BITS)


struct slash_sb_store {
	u64                      sbs_magic;
	sl_mds_id_t              sbs_mds_id;
	sl_inum_t		 sbs_inum_major; 
	psc_crc_t		 sbs_crc;
};

struct slash_sb_mem {	
	u32                      sbm_inum_minor;
	struct slash_sb_store	*sbm_sbs;	
	struct jflush_item       sbm_jfi;
	psc_spinlock_t		 sbm_lock;
};

#define SBS_OD_SZ (sizeof(struct slash_sb_store))
#define SBS_OD_CRCSZ (SBS_OD_SZ - sizeof(psc_crc_t))

void slash_superblock_init(void);

#endif
