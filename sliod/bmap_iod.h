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

#ifndef _SLIOD_BMAP_H_
#define _SLIOD_BMAP_H_

#include <sys/time.h>

#include "pfl/time.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/rpc.h"
#include "pfl/bitflag.h"
#include "pfl/lock.h"

#include "bmap.h"
#include "slashrpc.h"

struct bmap_iod_info;
struct slvr;

struct bcrcupd {
	struct timespec		 bcr_age;
	struct bmap_iod_info	*bcr_bii;
	struct psclist_head	 bcr_lentry;
	struct srm_bmap_crcup	 bcr_crcup;
};

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
	struct psclist_head	 bir_lentry;
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

/**
 * bmap_iod_info - the bmap_get_pri() data structure for the I/O server.
 */
struct bmap_iod_info {
	uint8_t			 bii_crcstates[SLASH_CRCS_PER_BMAP];
	uint64_t                 bii_crcs[SLASH_CRCS_PER_BMAP];

	/*
	 * Accumulate CRC updates here until its associated bcrcupd
	 * structure is full, at which point it is set to NULL then
	 * moved to a ready/hold list for transmission, and a new
	 * bcrcupd structure must be allocated for future CRC updates.
	 */
	struct bcrcupd		*bii_bcr;

	struct biod_slvrtree	 bii_slvrs;
	struct psclist_head	 bii_lentry;
	struct psc_lockedlist	 bii_rls;
};

/* sliod-specific bcm_flags */
#define	BMAP_IOD_INFLIGHT	(_BMAP_FLSHFT << 0)

#define bii_2_flags(b)		bii_2_bmap(b)->bcm_flags

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

#define BCR_MIN_AGE		3			/* in seconds */
#define BCR_MAX_AGE		6			/* in seconds */

uint64_t	bim_getcurseq(void);
void		bim_init(void);
int		bim_updateseq(uint64_t);

void		bcr_ready_add(struct bcrcupd *);
void		bcr_ready_remove(struct bcrcupd *);

void		slibmaprlsthr_spawn(void);

extern struct psc_listcache	 bmapRlsQ;
extern struct psc_poolmaster	 bmap_rls_poolmaster;
extern struct psc_poolmgr	*bmap_rls_pool;

extern struct psc_poolmaster	 bmap_crcupd_poolmaster;
extern struct psc_poolmgr	*bmap_crcupd_pool;

extern struct psc_listcache	 bcr_ready;

static __inline struct bmapc_memb *
bii_2_bmap(struct bmap_iod_info *bii)
{
	struct bmapc_memb *b;

	psc_assert(bii);
	b = (void *)bii;
	return (b - 1);
}

#endif /* _SLIOD_BMAP_H_ */
