/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2014, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASH_BMAP_CLI_H_
#define _SLASH_BMAP_CLI_H_

#include "pfl/lock.h"
#include "pfl/rpc.h"

#include "bmap.h"
#include "pgcache.h"
#include "slashrpc.h"

/* number of bmaps to allow before reaper kicks into gear */
#define	BMAP_CACHE_MAX			1024

/* number of bmap flush threads */
#define NUM_BMAP_FLUSH_THREADS		2

#define NUM_ATTR_FLUSH_THREADS		1

#define NUM_READAHEAD_THREADS		2

/**
 * bmap_cli_data - private data associated with a bmap used by a SLASH2 client
 */
struct bmap_cli_info {
	struct bmap_pagecache	 bci_bmpc;		/* must be first */
	struct srt_bmapdesc	 bci_sbd;		/* open bmap descriptor */
	struct timespec		 bci_etime;		/* current expire time */
	int			 bci_error;		/* lease request error */
	int			 bci_flush_rc;		/* flush error */
	int			 bci_nreassigns;	/* number of reassigns */
	sl_ios_id_t		 bci_prev_sliods[SL_MAX_IOSREASSIGN];
	struct psc_listentry	 bci_lentry;		/* bmap flushq */
	uint8_t			 bci_repls[SL_REPLICA_NBYTES];
};

/* mount_slash specific bcm_flags */
#define BMAP_CLI_LEASEEXTREQ	(_BMAP_FLSHFT << 0)	/* requesting a lease ext */
#define BMAP_CLI_REASSIGNREQ	(_BMAP_FLSHFT << 1)
#define BMAP_CLI_LEASEFAILED	(_BMAP_FLSHFT << 2)	/* lease request has failed */
#define BMAP_CLI_LEASEEXPIRED	(_BMAP_FLSHFT << 3)	/* lease has expired, new one is needed */
#define BMAP_CLI_SCHED		(_BMAP_FLSHFT << 4)	/* bmap flush in progress */
#define BMAP_CLI_BENCH		(_BMAP_FLSHFT << 5)

/* XXX change horribly named flags */
#define BMAP_CLI_MAX_LEASE	60 /* seconds */
#define BMAP_CLI_EXTREQSECS	20
#define BMAP_CLI_TIMEO_INC	1

static __inline struct bmap_cli_info *
bmap_2_bci(struct bmap *b)
{
	return (bmap_get_pri(b));
}

#define bmap_2_bci_const(b)	((const struct bmap_cli_info *)bmap_get_pri_const(b))

#define bmap_2_bmpc(b)		(&bmap_2_bci(b)->bci_bmpc)
#define bmap_2_restbl(b)	bmap_2_bci(b)->bci_repls
#define bmap_2_sbd(b)		(&bmap_2_bci(b)->bci_sbd)
#define bmap_2_ios(b)		bmap_2_sbd(b)->sbd_ios

void	 msl_bmap_cache_rls(struct bmap *);
int	 msl_bmap_lease_secs_remaining(struct bmap *);
int	 msl_bmap_lease_tryext(struct bmap *, int);
void	 msl_bmap_lease_tryreassign(struct bmap *);
int	 msl_bmap_lease_secs_remaining(struct bmap *);
int	 msl_bmap_release_cb(struct pscrpc_request *, struct pscrpc_async_args *);

void	 bmap_biorq_expire(struct bmap *);

void	 msbmaprlsthr_main(struct psc_thread *);

extern struct timespec msl_bmap_max_lease;
extern struct timespec msl_bmap_timeo_inc;

static __inline struct bmap *
bci_2_bmap(struct bmap_cli_info *bci)
{
	struct bmap *b;

	psc_assert(bci);
	b = (void *)bci;
	return (b - 1);
}

#endif /* _SLASH_BMAP_CLI_H_ */
