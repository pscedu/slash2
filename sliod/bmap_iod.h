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

#ifndef _SLIOD_BMAP_H_
#define _SLIOD_BMAP_H_

#include <sys/time.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_rpc/rpc.h"
#include "psc_util/bitflag.h"
#include "psc_util/lock.h"

#include "bmap.h"
#include "inode.h"
#include "slashrpc.h"

struct bmap_iod_info;
struct slvr_ref;

/* For now only one of these structures is needed.  In the future
 *   we'll need one per MDS.
 */
struct biod_infl_crcs {
	psc_spinlock_t		 binfcrcs_lock;
	atomic_t		 binfcrcs_nbcrs;
	struct psc_lockedlist	 binfcrcs_hold;
	struct psc_lockedlist	 binfcrcs_ready;
	struct psc_lockedlist	 binfcrcs_infl;
};

struct biod_crcup_ref {
	uint64_t		 bcr_xid;
	uint16_t		 bcr_flags;
	struct timespec		 bcr_age;
	struct bmap_iod_info	*bcr_biodi;
	struct psclist_head	 bcr_lentry;
	struct srm_bmap_crcup	 bcr_crcup;
};

struct bmap_iod_minseq {
	psc_spinlock_t		 bim_lock;
	struct timespec		 bim_age;
	struct psc_waitq	 bim_waitq;
	uint64_t		 bim_minseq;
	int			 bim_flags;
};

#define BIM_RETRIEVE_SEQ	1

#define BIM_MINAGE		10	/* Seconds */

/* bcr_flags */
#define	BCR_NONE		0x00
#define BCR_SCHEDULED		0x01

#define DEBUG_BCR(level, b, fmt, ...)					\
	psc_logs((level), PSS_GEN,					\
	    "bcr@%p fid="FIDFMT" xid=%"PRIu64" nups=%d fl=%d age=%lu"	\
	    " rls=%u seq=%"PRId64" key=%"PRId64" bmap@%p:%u :: "fmt,	\
	    (b), FIDFMTARGS(&(b)->bcr_crcup.fg), (b)->bcr_xid,		\
	    (b)->bcr_crcup.nups, (b)->bcr_flags, (b)->bcr_age.tv_sec,	\
	    (b)->bcr_crcup.rls, (b)->bcr_crcup.seq,			\
	    (b)->bcr_crcup.key, (b)->bcr_biodi->biod_bmap,		\
	    (b)->bcr_biodi->biod_bmap->bcm_blkno,			\
	    ## __VA_ARGS__)

SPLAY_HEAD(biod_slvrtree, slvr_ref);

struct bmap_iod_info {
	psc_spinlock_t		 biod_lock;
	struct bmapc_memb	*biod_bmap;
	struct biod_crcup_ref	*biod_bcr;
	struct biod_slvrtree	 biod_slvrs;
	struct slash_bmap_wire	*biod_bmap_wire;
	struct psclist_head	 biod_lentry;
	struct timespec		 biod_age;
	uint64_t		 biod_bcr_xid;
	uint64_t		 biod_bcr_xid_last;
	uint64_t		 biod_cur_seqkey[2];
	uint64_t		 biod_rls_seqkey[2];
	uint32_t		 biod_crcdrty_slvrs:30;
	uint32_t		 biod_inflight:1;
	uint32_t		 biod_rlsseq:1;
};

#define biodi_2_wire(bi)	(bi)->biod_bmap_wire
#define biodi_2_crcbits(bi, sl)	biodi_2_wire(bi)->bh_crcstates[sl]

#define bmap_2_biodi(b)		((struct bmap_iod_info *)(b)->bcm_pri)
#define bmap_2_biodi_age(b)	bmap_2_biodi(b)->biod_age
#define bmap_2_biodi_lentry(b)	bmap_2_biodi(b)->biod_lentry
#define bmap_2_biodi_slvrs(b)	(&bmap_2_biodi(b)->biod_slvrs)
#define bmap_2_biodi_wire(b)	bmap_2_biodi(b)->biod_bmap_wire

#define bmap_2_crcbits(b, sl)	biodi_2_crcbits(bmap_2_biodi(b), (sl))

#define BIOD_CRCUP_MAX_AGE	 2		/* in seconds */

uint64_t	bim_getcurseq(void);
void		bim_init(void);
void		bim_updateseq(uint64_t);


void bcr_hold_2_ready(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_hold_add(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_hold_requeue(struct biod_infl_crcs *, struct biod_crcup_ref *);
void bcr_xid_check(struct biod_crcup_ref *);
void bcr_xid_last_bump(struct biod_crcup_ref *);
void bcr_ready_remove(struct biod_infl_crcs *, struct biod_crcup_ref *);

extern struct psc_listcache iodBmapLru;

#endif /* _SLIOD_BMAP_H_ */
