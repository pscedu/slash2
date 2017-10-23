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

#ifndef _SLIOD_BMAP_H_
#define _SLIOD_BMAP_H_

#include <sys/time.h>

#include "pfl/bitflag.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"
#include "pfl/time.h"

#include "bmap.h"
#include "slashrpc.h"

struct bmap_iod_info;
struct slvr;

extern psc_spinlock_t            sli_release_bmap_lock;
extern struct psc_waitq          sli_release_bmap_waitq;

#define bcr_2_bmap(bcr)		bii_2_bmap((bcr)->bcr_bii)

struct bmap_iod_minseq {
	psc_spinlock_t		 bim_lock;
	struct timespec		 bim_age;
	struct psc_waitq	 bim_waitq;
	uint64_t		 bim_minseq;
	int			 bim_flags;
};

struct bmap_iod_rls {
	struct srt_bmapdesc	 bir_sbd;
	struct psc_listentry	 bir_lentry;
};

struct sli_update {
	int			 sli_count;
	struct psc_listentry     sli_lentry;
	struct srt_update_rec	 sli_recs[MAX_FILE_UPDATES];
};

#define BIM_RETRIEVE_SEQ	1

#define BIM_MINAGE		5	/* seconds */

/* time to wait for more bmap releases from the client */
#define SLIOD_BMAP_RLS_WAIT_SECS 1	/* seconds */

#define DEBUG_BCR(level, bcr, fmt, ...)				\
	psclogs((level), SLSS_BMAP,				\
	    "bcr@%p fid="SLPRI_FG" nups=%d "			\
	    "age=%"PSCPRI_TIMET" "				\
	    "bmap@%p:%u"					\
	    " :: " fmt,						\
	    (bcr), SLPRI_FG_ARGS(&(bcr)->bcr_crcup.fg),		\
	    (bcr)->bcr_crcup.nups, (bcr)->bcr_age.tv_sec,	\
	    bcr_2_bmap(bcr), bcr_2_bmap(bcr)->bcm_bmapno,	\
	    ## __VA_ARGS__)

SPLAY_HEAD(biod_slvrtree, slvr);

/*
 * bmap_get_pri() data specific to the I/O server.
 */
struct bmap_iod_info {
	uint8_t			 bii_crcstates[SLASH_SLVRS_PER_BMAP];
	uint64_t		 bii_crcs[SLASH_SLVRS_PER_BMAP];
	struct biod_slvrtree	 bii_slvrs;
	struct psc_listentry	 bii_lentry;
	struct psc_lockedlist	 bii_rls;	/* leases */
};

/* sliod-specific bcm_flags */
#define BMAPF_CRUD_INFLIGHT	(_BMAPF_SHIFT << 0)	/* CRC update RPC inflight */
#define BMAPF_RELEASEQ		(_BMAPF_SHIFT << 1)	/* on releaseq */

#define bii_2_flags(b)		bii_2_bmap(b)->bcm_flags

#undef bmap_2_crcs

#define bmap_2_bii(b)		((struct bmap_iod_info *)bmap_get_pri(b))
#define bmap_2_bii_slvrs(b)	(&bmap_2_bii(b)->bii_slvrs)
#define bmap_2_crcstates(b)	bmap_2_bii(b)->bii_crcstates
#define bmap_2_crcs(b)		bmap_2_bii(b)->bii_crcs

#define BMAP_SLVR_WANTREPL	_BMAP_SLVR_FLSHFT	/* Queued for replication */

#define BII_LOCK(bii)		BMAP_LOCK(bii_2_bmap(bii))
#define BII_ULOCK(bii)		BMAP_ULOCK(bii_2_bmap(bii))
#define BII_RLOCK(bii)		BMAP_RLOCK(bii_2_bmap(bii))
#define BII_URLOCK(bii, lk)	BMAP_URLOCK(bii_2_bmap(bii), (lk))
#define BII_TRYLOCK(bii)	BMAP_TRYLOCK(bii_2_bmap(bii))
#define BII_LOCK_ENSURE(bii)	BMAP_LOCK_ENSURE(bii_2_bmap(bii))

#define CRC_QUEUE_AGE		2	/* time wait on CRC queue in seconds */
#define BCR_BATCH_AGE		12	/* time wait for BCR batching in seconds */

uint64_t	bim_getcurseq(void);
void		bim_init(void);
int		bim_updateseq(uint64_t);

void		slibmaprlsthr_spawn(void);

extern struct bmap_iod_minseq	 sli_bminseq;

extern struct psc_poolmaster	 bmap_rls_poolmaster;
extern struct psc_poolmgr	*bmap_rls_pool;

extern struct psc_poolmaster     sli_upd_poolmaster;
extern struct psc_poolmgr       *sli_upd_pool;

static __inline struct bmap *
bii_2_bmap(struct bmap_iod_info *bii)
{
	struct bmap *b;

	psc_assert(bii);
	b = (void *)bii;
	return (b - 1);
}

#endif /* _SLIOD_BMAP_H_ */
