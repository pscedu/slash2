/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#ifndef _SLASHD_MDSLOG_H_
#define _SLASHD_MDSLOG_H_

#include "pfl/journal.h"

#include "sltypes.h"

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
 * to save FID and bmap lookups.
 */
struct sl_mds_crc_log {
	struct bmapc_memb	*scl_bmap;
	struct srm_bmap_crcup	*scl_crcup;
	sl_ios_id_t		 scl_iosid;
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

int	mds_bmap_crc_update(struct bmapc_memb *, sl_ios_id_t, struct srm_bmap_crcup *);

void	mds_reserve_slot(int);
void	mds_unreserve_slot(int);

int	mds_replay_namespace(struct slmds_jent_namespace *, int);
int	mds_replay_handler(struct psc_journal_enthdr *);

extern struct psc_journal		*slm_journal;
extern struct psc_journal_cursor	 mds_cursor;

extern uint64_t				 current_update_batchno;
extern uint64_t				 current_reclaim_batchno;

#endif /* _SLASHD_MDSLOG_H_ */
