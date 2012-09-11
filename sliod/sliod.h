/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "pfl/cdefs.h"
#include "psc_rpc/service.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "slconfig.h"
#include "sltypes.h"

/* sliod thread types */
enum {
	SLITHRT_ASYNC_IO,	/* asynchronous I/O handlers */
	SLITHRT_BMAPRLS,	/* notify MDS of completed write bmaps */
	SLITHRT_CONN,		/* connection monitor */
	SLITHRT_CTL,		/* control processor */
	SLITHRT_CTLAC,		/* control acceptor */
	SLITHRT_LNETAC,		/* Lustre net accept thr */
	SLITHRT_NBRQ,		/* non blocking RPC request processor */
	SLITHRT_REPLPND,	/* process enqueued replication work */
	SLITHRT_REPLREAP,	/* reap in-flight replication work */
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
	OPSTAT_SRMT_RELEASE,
	OPSTAT_FSIO_READ,
	OPSTAT_FSIO_READ_FAIL,
	OPSTAT_FSIO_WRITE,
	OPSTAT_FSIO_WRITE_FAIL,
	OPSTAT_HANDLE_IO,
	OPSTAT_OPEN,
	OPSTAT_OPEN_FAIL,
	OPSTAT_OPEN_SUCCEED,
	OPSTAT_REOPEN,
	OPSTAT_RELEASE_BMAP,
	OPSTAT_RECLAIM,
	OPSTAT_SLVR_AIO_REPLY
};

struct bmapc_memb;
struct fidc_membh;

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

__dead void	slictlthr_main(const char *);

int		iod_inode_getinfo(struct slash_fidgen *, uint64_t *, uint64_t *, uint32_t *);

extern struct srt_statfs	 sli_ssfb;
extern psc_spinlock_t		 sli_ssfb_lock;
extern struct psc_thread	*sliconnthr;

#endif /* _SLIOD_H_ */
