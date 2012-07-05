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

#ifndef _SLIOD_BMAP_H_
#define _SLIOD_BMAP_H_

#include <sys/time.h>

#include "pfl/time.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "slashrpc.h"

struct bmap_iod_info;
struct slvr_ref;

/*
 * For now only one of these structures is needed.  In the future
 *   we'll need one per MDS.
 */
struct biod_infl_crcs {
	psc_spinlock_t		 binfcrcs_lock;
	atomic_t		 binfcrcs_nbcrs;
	struct psc_lockedlist	 binfcrcs_hold;
	struct psc_lockedlist	 binfcrcs_ready;
};

struct biod_crcup_ref {
	uint64_t		 bcr_xid;
	uint16_t		 bcr_flags;
	struct timespec		 bcr_age;
	struct bmap_iod_info	*bcr_biodi;
	struct psclist_head	 bcr_lentry;
	struct srm_bmap_crcup	 bcr_crcup;
};

/* bcr_flags */
#define	BCR_NONE		0x00
#define BCR_SCHEDULED		0x01

#define bcr_2_bmap(bcr)		bii_2_bmap((bcr)->bcr_biodi)

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

#define BIM_MINAGE		5	/* Seconds */

#define SLIOD_BMAP_RLS_WAIT_SECS 1 /* Number of seconds to wait for more
				    *  bmap releases from the client
				    */

#define DEBUG_BCR(level, bcr, fmt, ...)					\
	psclogs((level), SLSS_BMAP,					\
	    "bcr@%p fid="SLPRI_FG" xid=%"PRIu64" nups=%d fl=%d "	\
	    "age=%"PSCPRI_TIMET" "					\
	    "bmap@%p:%u biod_bcr_xid=%"PRId64" "			\
	    "biod_bcr_xid_last=%"PRId64" :: " fmt,			\
	    (bcr), SLPRI_FG_ARGS(&(bcr)->bcr_crcup.fg), (bcr)->bcr_xid,	\
	    (bcr)->bcr_crcup.nups, (bcr)->bcr_flags,			\
	    (bcr)->bcr_age.tv_sec,					\
	    bcr_2_bmap(bcr), bcr_2_bmap(bcr)->bcm_bmapno,		\
	    (bcr)->bcr_biodi->biod_bcr_xid,				\
	    (bcr)->bcr_biodi->biod_bcr_xid_last, ## __VA_ARGS__)

SPLAY_HEAD(biod_slvrtree, slvr_ref);

/**
 * bmap_iod_info - the bmap_get_pri() data structure for the I/O server.
 */
struct bmap_iod_info {
	/*
	 * This structure must start with the continuation of
	 * bmap_ondisk from where bmapc_memb left off so an entire
	 * bmap_ondisk will be laid contiguously in memory for I/O over
	 * the network and with ZFS.
	 */
	struct bmap_extra_state	 biod_extrastate;

	/*
	 * Accumulate CRC updates until its associated biod_crcup_ref
	 * structure is full, at which point it is set to NULL and a
	 * new biod_crcup_ref structure must be allocated for future
	 * CRC updates.
	 */
	struct biod_crcup_ref	*biod_bcr;
	struct biod_slvrtree	 biod_slvrs;
	struct psclist_head	 biod_lentry;
	struct timespec		 biod_age;
	struct psc_lockedlist	 biod_bklog_bcrs;	/* at most one CRC update RPC per bmap */
	struct psc_lockedlist	 biod_rls;
	uint64_t		 biod_bcr_xid;
	uint64_t		 biod_bcr_xid_last;
	psc_atomic32_t		 biod_crcdrty_slvrs;
};

/* sliod-specific bcm_flags */
#define	BMAP_IOD_INFLIGHT	(_BMAP_FLSHFT << 0)
#define	BMAP_IOD_RLSSEQ		(_BMAP_FLSHFT << 1)
#define	BMAP_IOD_BCRSCHED	(_BMAP_FLSHFT << 2)
#define	BMAP_IOD_RLSSCHED	(_BMAP_FLSHFT << 3)

#define bii_2_flags(b)		bii_2_bmap(b)->bcm_flags

#define bmap_2_biodi(b)		((struct bmap_iod_info *)bmap_get_pri(b))
#define bmap_2_bii(b)		((struct bmap_iod_info *)bmap_get_pri(b))
#define bmap_2_biodi_age(b)	bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b)	bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_slvrs(b)	(&bmap_2_biodi(b)->biod_slvrs)
#define bmap_2_ondisk(b)	((struct bmap_ondisk *)&(b)->bcm_corestate)

#define BMAP_SLVR_WANTREPL	(_BMAP_SLVR_FLSHFT)	/* Queued for replication */

#define BIOD_LOCK(bii)		BMAP_LOCK(bii_2_bmap(bii))
#define BIOD_ULOCK(bii)		BMAP_ULOCK(bii_2_bmap(bii))
#define BIOD_RLOCK(bii)		BMAP_RLOCK(bii_2_bmap(bii))
#define BIOD_URLOCK(bii, lk)	BMAP_URLOCK(bii_2_bmap(bii), (lk))
#define BIOD_TRYLOCK(bii)	BMAP_TRYLOCK(bii_2_bmap(bii))
#define BIOD_LOCK_ENSURE(bii)	BMAP_LOCK_ENSURE(bii_2_bmap(bii))

#define BIOD_CRCUP_MAX_AGE	1			/* in seconds */

uint64_t	bim_getcurseq(void);
void		bim_init(void);
int		bim_updateseq(uint64_t);

void bcr_finalize(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_hold_2_ready(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_hold_add(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_hold_requeue(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_ready_add(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_ready_remove(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_xid_check(struct biod_crcup_ref *);

void biod_rlssched_locked(struct bmap_iod_info *);

void slibmaprlsthr_spawn(void);

extern struct psc_listcache	 bmapRlsQ;
extern struct psc_listcache	 bmapReapQ;
extern struct psc_poolmaster	 bmap_rls_poolmaster;
extern struct psc_poolmgr	*bmap_rls_pool;

static __inline struct bmapc_memb *
bii_2_bmap(struct bmap_iod_info *bii)
{
	struct bmapc_memb *bcm;

	psc_assert(bii);
	bcm = (void *)bii;
	return (bcm - 1);
}

#endif /* _SLIOD_BMAP_H_ */
