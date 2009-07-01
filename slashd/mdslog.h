/* $Id$ */

#ifndef _SLASHD_MDSLOG_H_
#define _SLASHD_MDSLOG_H_

#include "psc_types.h"
#include "inode.h"
#include "inodeh.h"
#include "bmap.h"
#include "slashrpc.h"

extern void
mds_inode_addrepl_log(struct slash_inode_handle *, sl_ios_id_t, uint32_t);

extern void
mds_bmap_repl_log(struct bmapc_memb *);

extern void
mds_bmap_crc_log(struct bmapc_memb *, struct srm_bmap_crcup *);

#endif
