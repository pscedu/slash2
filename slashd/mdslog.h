/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_util/journal.h"

struct bmapc_memb;
struct fidc_membh;
struct slash_inode_handle;
struct slmds_jent_namespace;
struct srm_bmap_crcup;
struct srt_stat;

/*
 * Keep track of the bmap associated with a CRC update
 * to save fid and bmap lookups.
 */
struct sl_mds_crc_log {
	struct bmapc_memb	*scl_bmap;
	struct srm_bmap_crcup	*scl_crcup;
};

#define MDS_LOG_BMAP_REPL	(_PJE_FLSHFT << 0)
#define MDS_LOG_BMAP_CRC	(_PJE_FLSHFT << 1)
#define MDS_LOG_BMAP_SEQ	(_PJE_FLSHFT << 2)
#define MDS_LOG_BMAP_ASSIGN	(_PJE_FLSHFT << 3)
#define MDS_LOG_INO_ADDREPL	(_PJE_FLSHFT << 4)
#define MDS_LOG_NAMESPACE	(_PJE_FLSHFT << 5)

#define MDS_LOG_LAST		MDS_LOG_NAMESPACE

/*
 * A structure used to describe the log application progress on each site.
 */
struct site_progress {
	int			sp_siteid;
	uint64_t		sp_seqno;
};

/*
 * If something is wrong with logging, we take a crash.
 * Our MDS should be able to recover after being restarted.
 */

void	mds_bmap_crc_log(void *, uint64_t);
void	mds_bmap_repl_log(void *, uint64_t);
void	mds_inode_addrepl_log(void *, uint64_t);
void	mds_namespace_log(int, uint64_t, uint64_t, uint64_t,
	    const struct srt_stat *, int, const char *, const char *);

void	mds_bmap_sync(void *);
void	mds_inode_sync(struct slash_inode_handle *);
void	mds_journal_init(int);

int	mds_bmap_repl_update(struct bmapc_memb *);
int	mds_bmap_crc_update(struct bmapc_memb *, struct srm_bmap_crcup *);
int	mds_inode_addrepl_update(struct slash_inode_handle *, sl_ios_id_t, uint32_t);

void	mds_current_txg(uint64_t *);

void	mds_reserve_slot(void);
void	mds_unreserve_slot(void);

int	mds_redo_namespace(struct slmds_jent_namespace *, int);

int	mds_replay_handler(struct psc_journal_enthdr *);

extern struct psc_journal		*mdsJournal;
extern struct psc_journal_cursor	 mds_cursor;

#endif /* _SLASHD_MDSLOG_H_ */
