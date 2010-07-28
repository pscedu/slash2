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

#ifndef _SLIOD_H_
#define _SLIOD_H_

#include "pfl/cdefs.h"
#include "psc_rpc/service.h"
#include "psc_util/thread.h"

#include "fid.h"
#include "sltypes.h"

/* sliod thread types */
#define SLITHRT_CTL		0	/* control processor */
#define SLITHRT_CTLAC		1	/* control acceptor */
#define SLITHRT_LNETAC		2	/* lustre net accept thr */
#define SLITHRT_USKLNDPL	3	/* userland socket lustre net dev poll thr */
#define SLITHRT_RIC		4	/* service RPC requests from CLIENT */
#define SLITHRT_RIM		5	/* service RPC requests from MDS */
#define SLITHRT_RII		6	/* service RPC requests from ION */
#define SLITHRT_TINTV		7	/* timer interval */
#define SLITHRT_TIOS		8	/* iostats updater */
#define SLITHRT_SLVR_CRC	9	/* sliver crc updaters */
#define SLITHRT_REPLFIN		10	/* process finished replication work */
#define SLITHRT_REPLREAP	11	/* reap in-flight replication work */
#define SLITHRT_REPLPND		12	/* process enqueued replication work */
#define SLITHRT_BMAPRLS		13	/* notify mds of completed write bmaps */
#define SLITHRT_CONN		14	/* connection monitor */

#define NSLVRCRC_THRS		4	/* perhaps default to ncores + configurable? */

struct bmapc_memb;
struct fidc_membh;
struct srt_bmapdesc_buf;

struct sliric_thread {
	struct pscrpc_thread	 sirct_prt;
};

struct slirim_thread {
	struct pscrpc_thread	 sirmt_prt;
};

struct slirii_thread {
	struct pscrpc_thread	 sirit_prt;
};

PSCTHR_MKCAST(sliricthr, sliric_thread, SLITHRT_RIC)
PSCTHR_MKCAST(slirimthr, slirim_thread, SLITHRT_RIM)
PSCTHR_MKCAST(sliriithr, slirii_thread, SLITHRT_RII)

struct resm_iod_info {
	psc_spinlock_t		  rmii_lock;
	struct psc_waitq	  rmii_waitq;
};

#define resm2rmii(resm)		((struct resm_iod_info *)(resm)->resm_pri)

void		slitimerthr_spawn(void);
__dead void	slictlthr_main(const char *);

int		iod_inode_getsize(struct slash_fidgen *, uint64_t *);

extern struct slash_creds	 rootcreds;

#endif /* _SLIOD_H_ */
