/* $Id$ */

#ifndef _SLASHD_MDSLOG_H_
#define _SLASHD_MDSLOG_H_

#include "inode.h"

enum mds_log_types {
	MDS_LOG_BMAP_REPL     = (1 << (1 + PJE_LASTBIT)),
	MDS_LOG_BMAP_CRC      = (1 << (2 + PJE_LASTBIT)),
	MDS_LOG_INO_ADDREPL   = (1 << (3 + PJE_LASTBIT))
};

struct bmapc_memb;
struct fidc_membh;
struct slash_inode_handle;
struct srm_bmap_crcup;

void mds_bmap_crc_log(struct bmapc_memb *, struct srm_bmap_crcup *);
void mds_bmap_repl_log(struct bmapc_memb *);
void mds_bmap_sync(void *);
void mds_inode_addrepl_log(struct slash_inode_handle *, sl_ios_id_t, uint32_t);
void mds_inode_sync(void *);
void mds_journal_init(void);

extern struct psc_journal *mdsJournal;

#endif /* _SLASHD_MDSLOG_H_ */
