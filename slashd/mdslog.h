/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

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
void mds_bmap_jfiprep(void *);
void mds_inode_addrepl_log(struct slash_inode_handle *, sl_ios_id_t, uint32_t);
void mds_inode_sync(void *);
void mds_journal_init(void);

extern struct psc_journal *mdsJournal;

#endif /* _SLASHD_MDSLOG_H_ */
