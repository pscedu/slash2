/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "pfl/cdefs.h"
#include "pfl/opstats.h"
#include "pfl/service.h"
#include "pfl/thread.h"

#include "fid.h"
#include "slconfig.h"
#include "sltypes.h"
#include "bmap_iod.h"

struct bmapc_memb;
struct fidc_membh;

/* sliod thread types */
enum {
	SLITHRT_AIO = _PFL_NTHRT,	/* asynchronous I/O handlers */
	SLITHRT_BMAPRLS,		/* notifier to MDS of completed write bmaps */
	SLITHRT_BMAPLEASE_PROC,		/* bmap lease relinquish processor */
	SLITHRT_BREAP,			/* bmap reaper */
	SLITHRT_BATCHRPC,		/* batch RPC sender */
	SLITHRT_CONN,			/* connection monitor */
	SLITHRT_CTL,			/* control processor */
	SLITHRT_CTLAC,			/* control acceptor */
	SLITHRT_FREAP,			/* file reaper */
	SLITHRT_HEALTH,			/* underlying file system health checker */
	SLITHRT_LNETAC,			/* Lustre net accept thr */
	SLITHRT_NBRQ,			/* non blocking RPC request processor */
	SLITHRT_OPSTIMER,		/* iostats updater */
	SLITHRT_REPLPND,		/* process enqueued replication work */
	SLITHRT_RIC,			/* service RPC requests from CLI */
	SLITHRT_RII,			/* service RPC requests from ION */
	SLITHRT_RIM,			/* service RPC requests from MDS */
	SLITHRT_SEQNO,			/* update min seqno */
	SLITHRT_UPDATE,			/* update file status */
	SLITHRT_SLVR_SYNC,		/* sliver SYNC to reduce fsync spikes */
	SLITHRT_READAHEAD,		/* sliver read-ahead */
	SLITHRT_STATFS,			/* statvfs(2) updater */
	SLITHRT_USKLNDPL,		/* userland socket Lustre net dev poll thr */
	SLITHRT_WORKER			/* generic worker thread */
};

#define NSLVR_READAHEAD_THRS	8

#define NSLVRCRC_THRS		4	/* perhaps default to ncores + configurable? */

#define NSLVRSYNC_THRS		2	/* perhaps default to ncores + configurable? */

#define NBMAPRLS_THRS		4	/* perhaps default to ncores + configurable? */

struct sliric_thread {
	struct pscrpc_thread	 sirct_prt;
};

struct slirim_thread {
	struct pscrpc_thread	 sirmt_prt;
};

struct slirii_thread {
	struct pscrpc_thread	 sirit_prt;
	int			 sirit_st_nread;
};

PSCTHR_MKCAST(sliricthr, sliric_thread, SLITHRT_RIC)
PSCTHR_MKCAST(slirimthr, slirim_thread, SLITHRT_RIM)
PSCTHR_MKCAST(sliriithr, slirii_thread, SLITHRT_RII)

struct resm_iod_info {
};

static __inline struct resm_iod_info *
resm2rmii(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

void	slictlthr_main(const char *);

int	sli_has_enough_space(struct fidc_membh *, uint32_t, uint32_t,
	    uint32_t);

#define SLI_NWORKER_THREADS	4

extern struct pfl_opstats_grad	 sli_iorpc_iostats_rd;
extern struct pfl_opstats_grad	 sli_iorpc_iostats_wr;
extern struct pfl_iostats_rw	 sli_backingstore_iostats;
extern int			 sli_selftest_enable;
extern int			 sli_selftest_result;
extern int			 sli_disable_write;
extern struct srt_bwqueued	 sli_bwqueued;
extern psc_spinlock_t		 sli_bwqueued_lock;
extern struct srt_statfs	 sli_ssfb;
extern psc_spinlock_t		 sli_ssfb_lock;
extern struct timespec		 sli_ssfb_send;
extern struct psc_listcache	 sli_fcmh_dirty;
extern struct psc_listcache	 sli_fcmh_update;
extern int			 sli_sync_max_writes;
extern int			 sli_min_space_reserve_gb;
extern int			 sli_min_space_reserve_pct;
extern int			 sli_predio_max_slivers;
extern struct psc_thread	*sliconnthr;

extern uint64_t			 sli_current_reclaim_xid;
extern uint64_t			 sli_current_reclaim_batchno;

extern struct psc_listcache	 sli_bmaplease_releaseq;
extern struct statvfs		 sli_statvfs_buf;

/*
 * List of fault point that will be auto-registered on startup.
 */
#define RIC_HANDLE_FAULT        "sliod/ric_handle"

void	slictlthr_spawn(const char *);

void	sliupdthr_main(struct psc_thread *);
void	slisyncthr_main(struct psc_thread *);
void	sliseqnothr_main(struct psc_thread *);

void	sli_enqueue_update(struct fidc_membh *);

#endif /* _SLIOD_H_ */
