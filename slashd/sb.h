/* $Id$ */

#include "psc_util/journal.h"

struct slash_sb_store {
	int			 sbs_inum;	/* next inum to assign */
	psc_crc_t		 sbs_crc;
};

struct slash_sb_mem {
	struct slash_sb_store	*sbm_sbs;
	struct psc_journal	 sbm_pj;
	psc_spinlock_t		 sbm_lock;
};

void slash_superblock_init(void);
