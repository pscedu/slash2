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

#ifndef _SLASH_CLI_BMAP_H_
#define _SLASH_CLI_BMAP_H_

#include "psc_rpc/rpc.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "bmpc.h"
#include "inode.h"
#include "slashrpc.h"

/*
 * msbmap_crcrepl_states - must be the same as bh_crcstates and bh_repls
 *  in slash_bmap_od.
 */
struct msbmap_crcrepl_states {
	uint8_t	msbcr_crcstates[SL_CRCS_PER_BMAP];	/* crc descriptor bits  */
	uint8_t	msbcr_repls[SL_REPLICA_NBYTES];		/* replica bit map        */
};

/*
 * bmap_cli_data - assigned to bmap_get_pri(bmap) for mount slash client.
 */
struct bmap_cli_info {
	struct bmap_pagecache		 msbd_bmpc;
	struct bmapc_memb		*msbd_bmap;
	struct msbmap_crcrepl_states	 msbd_msbcr;
	struct srt_bmapdesc		 msbd_sbd;   /* open bmap descriptor */
	struct psclist_head		 msbd_lentry;
	struct timespec			 msbd_xtime; /* max time */
	struct timespec			 msbd_etime; /* current expire time */
};

#define BMAP_CLI_MAX_LEASE		60 /* seconds */
#define BMAP_CLI_TIMEO_INC		5
#define BMAP_CLI_DIOWAIT_SECS           1

#define bmap_2_msbd(b)			((struct bmap_cli_info *)bmap_get_pri(b))
#define bmap_2_bci(b)			((struct bmap_cli_info *)bmap_get_pri(b))
#define bmap_2_bmpc(b)			(&bmap_2_msbd(b)->msbd_bmpc)
#define bmap_2_sbd(b)			(&bmap_2_msbd(b)->msbd_sbd)
#define bmap_2_ion(b)			bmap_2_sbd(b)->sbd_ion_nid

static __inline int
bmap_cli_timeo_cmp(const void *x, const void *y)
{
	const struct bmap_cli_info * const *pa = x, *a = *pa;
	const struct bmap_cli_info * const *pb = y, *b = *pb;

	if (timespeccmp(&a->msbd_etime, &b->msbd_etime, <))
		return (-1);

	if (timespeccmp(&a->msbd_etime, &b->msbd_etime, >))
		return (1);

	return (0);
}

#define BMAP_CLI_BUMP_TIMEO(b)						\
	do {								\
		struct timespec _ctime;					\
									\
		clock_gettime(CLOCK_REALTIME, &_ctime);			\
		BMAP_LOCK(b);						\
		timespecadd(&_ctime, &msl_bmap_timeo_inc,		\
		    &bmap_2_msbd(b)->msbd_etime);			\
		if (timespeccmp(&bmap_2_msbd(b)->msbd_etime,		\
		    &bmap_2_msbd(b)->msbd_xtime, >))			\
			memcpy(&bmap_2_msbd(b)->msbd_etime,		\
			    &bmap_2_msbd(b)->msbd_xtime,		\
			    sizeof(struct timespec));			\
		BMAP_ULOCK(b);						\
	} while (0)

#define BMAP_CLI_FLUSHPROC (_BMAP_FLSHFT << 0)	/* proc'd by flush thr */

extern struct timespec msl_bmap_max_lease;
extern struct timespec msl_bmap_timeo_inc;

void     msl_bmap_cache_rls(struct bmapc_memb *);

#endif /* _SLASH_CLI_BMAP_H_ */
