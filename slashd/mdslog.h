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

#include <sys/stat.h>

#include "psc_util/journal.h"

#include "inode.h"

struct bmapc_memb;
struct fidc_membh;
struct slash_inode_handle;
struct srm_bmap_crcup;

enum {
	MDS_LOG_BMAP_REPL	= _PJE_FLSHFT << 0,
	MDS_LOG_BMAP_CRC	= _PJE_FLSHFT << 1,
	MDS_LOG_BMAP_SEQ	= _PJE_FLSHFT << 2,
	MDS_LOG_INO_ADDREPL	= _PJE_FLSHFT << 3,
	MDS_LOG_NAMESPACE	= _PJE_FLSHFT << 4
};

/*
 * If all MDSes are perfectly in sync, then we can use one buffer to read
 * log entries from the log file and send it to all MDSes.  Otherwise, we
 * need to maintain a list of buffers to avoid reading the same log file
 * repeatedly.
 */
#define	MDS_LOG_MAX_LOG_BATCH	8
struct sl_mds_logbuf {
	atomic_t		 slb_refcnt;
	int			 slb_count;			/* total # of entries */
	int			 slb_size;			/* total size in bytes */
	uint64_t		 slb_seqno;			/* starting sequence number */
	struct psclist_head	 slb_link;
	/*
	 * A buffer that is ready for RPC (i.e., packed). In order to deal
	 * with timeouts of log entries, we allow a buffer to be filled
	 * gradually.
	 */
	char			*slb_buf;
};

/*
 * A structure used to describe the log application progress on each site.
 */
struct site_progress {
	int			sp_siteid;
	uint64_t		sp_seqno;
};

/* if something is wrong with logging, we take a crash.
 * Our MDS should be able to recover after being restarted */
void mds_bmap_crc_log(struct bmapc_memb *, struct srm_bmap_crcup *);
void mds_bmap_jfiprep(void *);
void mds_bmap_repl_log(struct bmapc_memb *);
void mds_bmap_sync(void *);
void mds_inode_addrepl_log(struct slash_inode_handle *, sl_ios_id_t, uint32_t);
void mds_namespace_log(int, uint64_t, uint64_t, uint64_t, const struct srt_stat *, const char *);

void mds_inode_sync(void *);
void mds_journal_init(void);

extern struct psc_journal *mdsJournal;

#endif /* _SLASHD_MDSLOG_H_ */
