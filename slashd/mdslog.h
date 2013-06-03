/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2011, Pittsburgh Supercomputing Center (PSC).
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
struct slmds_jent_bmap_repls;
struct slmds_jent_ino_repls;
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

#define MDS_LOG_BMAP_REPLS	(_PJE_FLSHFT << 0)
#define MDS_LOG_BMAP_CRC	(_PJE_FLSHFT << 1)
#define MDS_LOG_BMAP_SEQ	(_PJE_FLSHFT << 2)
#define MDS_LOG_BMAP_ASSIGN	(_PJE_FLSHFT << 3)
#define MDS_LOG_INO_REPLS	(_PJE_FLSHFT << 4)
#define MDS_LOG_NAMESPACE	(_PJE_FLSHFT << 5)
#define _MDS_LOG_LAST_TYPE	(_PJE_FLSHFT << 5)

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

void	mdslog_bmap_crc(void *, uint64_t, int);
void	mdslog_bmap_repls(void *, uint64_t, int);
void	mdslog_ino_repls(void *, uint64_t, int);
void	mdslog_namespace(int, uint64_t, uint64_t, uint64_t,
	    const struct srt_stat *, int, const char *, const char *, void *);

void	mdslogfill_bmap_repls(struct bmapc_memb *, struct slmds_jent_bmap_repls *);
void	mdslogfill_ino_repls(struct fidc_membh *, struct slmds_jent_ino_repls *);

void	mds_journal_init(int, uint64_t);

int	mds_bmap_crc_update(struct bmapc_memb *, struct srm_bmap_crcup *);

void	mds_reserve_slot(int);
void	mds_unreserve_slot(int);

int	mds_replay_namespace(struct slmds_jent_namespace *, int);
int	mds_replay_handler(struct psc_journal_enthdr *);

extern struct psc_journal		*mdsJournal;
extern struct psc_journal_cursor	 mds_cursor;

extern uint64_t				 current_update_batchno;
extern uint64_t				 current_reclaim_batchno;

#endif /* _SLASHD_MDSLOG_H_ */
