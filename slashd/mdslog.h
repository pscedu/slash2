/* $Id$ */

#ifndef _SLASHD_MDSLOG_H_
#define _SLASHD_MDSLOG_H_

#include "inode.h"

enum mds_log_types {
#ifdef INUM_SELF_MANAGE
	MDS_LOG_SB            = (1 << (0 + PJET_RESERVED)),
#endif
	MDS_LOG_BMAP_REPL     = (1 << (1 + PJET_RESERVED)),
	MDS_LOG_BMAP_CRC      = (1 << (2 + PJET_RESERVED)),
	MDS_LOG_INO_ADDREPL   = (1 << (3 + PJET_RESERVED))
};

struct bmapc_memb;
struct fidc_membh;
struct slash_inode_handle;
struct srm_bmap_crcup;

int  mds_inode_release(struct fidc_membh *);
void mds_bmap_crc_log(struct bmapc_memb *, struct srm_bmap_crcup *);
void mds_bmap_repl_log(struct bmapc_memb *);
void mds_bmap_sync(void *);
void mds_inode_addrepl_log(struct slash_inode_handle *, sl_ios_id_t, uint32_t);
void mds_inode_sync(void *);

extern struct psc_journal *mdsJournal;

#endif /* _SLASHD_MDSLOG_H_ */
