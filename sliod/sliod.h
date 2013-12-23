/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "pfl/cdefs.h"
#include "pfl/service.h"
#include "pfl/thread.h"

#include "fid.h"
#include "slconfig.h"
#include "sltypes.h"

struct bmapc_memb;
struct fidc_membh;

/* sliod thread types */
enum {
	SLITHRT_ASYNC_IO,	/* asynchronous I/O handlers */
	SLITHRT_BMAPRLS,	/* notify MDS of completed write bmaps */
	SLITHRT_BREAP,
	SLITHRT_CONN,		/* connection monitor */
	SLITHRT_CTL,		/* control processor */
	SLITHRT_CTLAC,		/* control acceptor */
	SLITHRT_HEALTH,
	SLITHRT_LNETAC,		/* Lustre net accept thr */
	SLITHRT_NBRQ,		/* non blocking RPC request processor */
	SLITHRT_REPLPND,	/* process enqueued replication work */
	SLITHRT_RIC,		/* service RPC requests from CLI */
	SLITHRT_RII,		/* service RPC requests from ION */
	SLITHRT_RIM,		/* service RPC requests from MDS */
	SLITHRT_SLVR_CRC,	/* sliver CRC updaters */
	SLITHRT_STATFS,		/* statvfs(2) updater */
	SLITHRT_TIOS,		/* iostats updater */
	SLITHRT_USKLNDPL	/* userland socket Lustre net dev poll thr */
};

#define NSLVRCRC_THRS		4	/* perhaps default to ncores + configurable? */

enum {
	SLI_OPST_AIO_INSERT,
	SLI_OPST_CLOSE_FAIL,
	SLI_OPST_CLOSE_SUCCEED,
	SLI_OPST_CRC_UPDATE,
	SLI_OPST_CRC_UPDATE_BACKLOG,
	SLI_OPST_CRC_UPDATE_BACKLOG_CLEAR,
	SLI_OPST_CRC_UPDATE_CB,
	SLI_OPST_CRC_UPDATE_CB_FAILURE,
	SLI_OPST_FSIO_READ,
	SLI_OPST_FSIO_READ_FAIL,
	SLI_OPST_FSIO_READ_CRC_BAD,
	SLI_OPST_FSIO_READ_CRC_GOOD,
	SLI_OPST_FSIO_WRITE,
	SLI_OPST_FSIO_WRITE_FAIL,

	SLI_OPST_GET_CUR_SEQ,
	SLI_OPST_GET_CUR_SEQ_RPC,

	SLI_OPST_HANDLE_IO,
	SLI_OPST_HANDLE_PRECLAIM,
	SLI_OPST_HANDLE_REPLREAD,
	SLI_OPST_HANDLE_REPLREAD_AIO,
	SLI_OPST_HANDLE_REPLREAD_REMOVE,
	SLI_OPST_HANDLE_REPL_SCHED,
	SLI_OPST_IOCB_FREE,
	SLI_OPST_IOCB_GET,
	SLI_OPST_IO_PREP_RMW,
	SLI_OPST_ISSUE_REPLREAD,
	SLI_OPST_ISSUE_REPLREAD_CB,
	SLI_OPST_ISSUE_REPLREAD_CB_AIO,
	SLI_OPST_ISSUE_REPLREAD_ERROR,

	SLI_OPST_MIN_SEQNO,

	SLI_OPST_OPEN_FAIL,
	SLI_OPST_OPEN_SUCCEED,
	SLI_OPST_RECLAIM,
	SLI_OPST_RECLAIM_FILE,
	SLI_OPST_RELEASE_BMAP,
	SLI_OPST_REOPEN,
	SLI_OPST_REPL_READAIO,
	SLI_OPST_SEQNO_REDUCE,
	SLI_OPST_SEQNO_INVALID,
	SLI_OPST_SLVR_AIO_REPLY,
	SLI_OPST_SRMT_RELEASE
};

enum {
	SLI_FAULT_AIO_FAIL,
	SLI_FAULT_CRCUP_FAIL,
	SLI_FAULT_FSIO_READ_FAIL
};

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

void		slictlthr_main(const char *);

int		iod_inode_getinfo(struct slash_fidgen *, uint64_t *, uint64_t *, uint32_t *);

int				 sli_selftest_rc;
extern struct srt_statfs	 sli_ssfb;
extern psc_spinlock_t		 sli_ssfb_lock;
extern struct psc_thread	*sliconnthr;

extern uint64_t			 current_reclaim_xid;
extern uint64_t			 current_reclaim_batchno;

#endif /* _SLIOD_H_ */
