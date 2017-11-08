/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015, Google, Inc.
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

#ifndef _SLIOD_SLVR_H_
#define _SLIOD_SLVR_H_

#include "pfl/aio.h"
#include "pfl/atomic.h"
#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/tree.h"

#include "bmap.h"
#include "bmap_iod.h"
#include "slab.h"
#include "slashrpc.h"
#include "subsys_iod.h"

struct bmap_iod_info;

/*
 * Bmap "sliver" used for scheduling dirty slivers to be CRC'd
 *	and sent to the MDS.
 * Note: slivers are locked through their bmap_iod_info lock.  A slab is
 *	the in-memory data represented by the sliver.
 */
struct slvr {
	uint16_t		 slvr_num;	/* bmap slvr offset */
	uint16_t		 slvr_flags;	/* see SLVR_* flags */
	uint32_t		 slvr_refcnt;
	/*
	 * KISS: A sliver marked with an error must be freed.
	 * Otherwise, we run the risk of turning a transient error into
	 * a longer-time error.
	 */
	 int32_t		 slvr_err;
	psc_spinlock_t		 slvr_lock;
	struct bmap_iod_info	*slvr_bii;
	struct timespec		 slvr_ts;
	struct sli_iocb		*slvr_iocb;
	/*
	 * Used for both read caching and write aggregation.
	 */
	void			*slvr_slab;
	struct sli_aiocb_reply  *slvr_aioreply;
	struct psclist_head	 slvr_lentry;	/* dirty queue */
	SPLAY_ENTRY(slvr)	 slvr_tentry;	/* bmap tree entry */
};

/* slvr_flags */
#define SLVRF_FAULTING		(1 <<  0)	/* contents loading from disk or net */
#define SLVRF_DATARDY		(1 <<  1)	/* ready for read / write activity */
#define SLVRF_DATAERR		(1 <<  2)	/* a sliver can't be reused if marked error */
#define SLVRF_LRU		(1 <<  3)	/* cached but not dirty */
/*
 * This flag acts like an extra reference count to the sliver.  I would
 * like to get rid of this special case.  However, maybe someday, we
 * will remove the CRC logic entirely.
 */
#define SLVRF_FREEING		(1 <<  4)	/* sliver is being reaped */
#define SLVRF_ACCESSED		(1 <<  5)	/* actually used by a client */
#define SLVRF_READAHEAD		(1 <<  6)	/* loaded via readahead logic */

#define SLVR_LOCK(s)		spinlock(&(s)->slvr_lock)
#define SLVR_ULOCK(s)		freelock(&(s)->slvr_lock)
/*
 * Think twice if you ever want to use recursive lock.
 */
#define SLVR_LOCK_ENSURE(s)	LOCK_ENSURE(&(s)->slvr_lock)
#define SLVR_TRYLOCK(s)		trylock(&(s)->slvr_lock)
#define SLVR_TRYRLOCK(s, lk)	tryreqlock(&(s)->slvr_lock, (lk))

#define SLVR_WAKEUP(s)							\
	do {								\
		SLVR_LOCK_ENSURE(s);					\
		psc_waitq_wakeall(&slvr_2_fcmh(s)->fcmh_waitq);		\
	} while (0)

#define SLVR_WAIT(s, cond)						\
	do {								\
		SLVR_LOCK_ENSURE(s);					\
		DEBUG_SLVR(PLL_DIAG, (s), "SLVR_WAIT");			\
		while (cond) {						\
			psc_waitq_wait(&slvr_2_fcmh(s)->fcmh_waitq,	\
			    &(s)->slvr_lock);				\
			SLVR_LOCK(s);					\
		}							\
	} while (0)

#define slvr_2_bii(s)		((s)->slvr_bii)
#define slvr_2_bmap(s)		bii_2_bmap(slvr_2_bii(s))
#define slvr_2_fcmh(s)		slvr_2_bmap(s)->bcm_fcmh
#define slvr_2_fii(s)		fcmh_2_fii(slvr_2_fcmh(s))
#define slvr_2_fd(s)		slvr_2_fii(s)->fii_fd

#define slvr_2_buf(s, blk)						\
	((void *)((s)->slvr_slab + ((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_fileoff(s, blk)						\
	((off_t)((slvr_2_bmap(s)->bcm_bmapno * SLASH_BMAP_SIZE) +	\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_crcbits(s)						\
	slvr_2_bii(s)->bii_crcstates[(s)->slvr_num]

#define slvr_2_crc(s)							\
	slvr_2_bii(s)->bii_crcs[(s)->slvr_num]

#define DEBUG_SLVR(level, s, fmt, ...)					\
	psclogs((level), SLISS_SLVR, "slvr@%p num=%hu ref=%u "		\
	    "ts="PSCPRI_TIMESPEC" "					\
	    "bii=%p slab=%p bmap=%p fid="SLPRI_FID" iocb=%p flgs="	\
	    "%s%s%s%s%s%s :: " fmt,					\
	    (s), (s)->slvr_num, (s)->slvr_refcnt,			\
	    PSCPRI_TIMESPEC_ARGS(&(s)->slvr_ts),			\
	    (s)->slvr_bii, (s)->slvr_slab,				\
	    (s)->slvr_bii ? slvr_2_bmap(s) : NULL,			\
	    (s)->slvr_bii ?						\
	      fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh) : FID_ANY,		\
	    (s)->slvr_iocb,						\
	    (s)->slvr_flags & SLVRF_FAULTING	? "f" : "-",		\
	    (s)->slvr_flags & SLVRF_DATARDY	? "d" : "-",		\
	    (s)->slvr_flags & SLVRF_DATAERR	? "E" : "-",		\
	    (s)->slvr_flags & SLVRF_LRU		? "l" : "-",		\
	    (s)->slvr_flags & SLVRF_FREEING	? "F" : "-",		\
	    (s)->slvr_flags & SLVRF_ACCESSED	? "a" : "-",		\
	    ##__VA_ARGS__)

#define RIC_MAX_SLVRS_PER_IO	2

struct sli_aiocb_reply {
	struct psc_listentry	  aiocbr_lentry;
	struct iovec		  aiocbr_iovs[RIC_MAX_SLVRS_PER_IO];
	struct slvr		 *aiocbr_slvrs[RIC_MAX_SLVRS_PER_IO];
	int			  aiocbr_flags;
	int			  aiocbr_nslvrs;
	int			  aiocbr_niov;
	struct slrpc_cservice *aiocbr_csvc;
	struct srt_bmapdesc	  aiocbr_sbd;
	uint64_t		  aiocbr_id;
	enum rw			  aiocbr_rw;
	uint32_t		  aiocbr_len;
	uint32_t		  aiocbr_off;
};

/* aiocbr_flags */
#define SLI_AIOCBSF_NONE	(0)
#define SLI_AIOCBSF_REPL	(1 << 0)
#define SLI_AIOCBSF_DIO		(1 << 1)

struct sli_iocb {
	struct psc_listentry	  iocb_lentry;
	struct slvr		 *iocb_slvr;
	struct aiocb		  iocb_aiocb;
	void			(*iocb_cbf)(struct sli_iocb *);
	int			  iocb_rc;
};

struct slvr *
	slvr_lookup(uint32_t, struct bmap_iod_info *);
void	slvr_cache_init(void);
int	slvr_do_crc(struct slvr *, uint64_t *);
ssize_t	slvr_fsbytes_wio(struct slvr *, uint32_t, uint32_t);
ssize_t	slvr_io_prep(struct slvr *, uint32_t, uint32_t, enum rw, int);

void	slvr_io_done(struct slvr *, int);
void	slvr_rio_done(struct slvr *);
void	slvr_wio_done(struct slvr *);

void	slvr_remove(struct slvr *);
void	slvr_remove_all(struct fidc_membh *);

void	slvr_schedule_crc(struct slvr *);

struct sli_aiocb_reply *
	sli_aio_reply_setup(struct pscrpc_request *, uint32_t, uint32_t,
	    struct slvr **, int, struct iovec *, int, enum rw);

struct sli_aiocb_reply *
	sli_aio_replreply_setup(struct pscrpc_request *, struct slvr *,
	    struct iovec *);

void	sli_aio_aiocbr_release(struct sli_aiocb_reply *);


struct sli_readaheadrq {
	struct sl_fidgen	rarq_fg;
	off_t			rarq_off;
	off_t			rarq_size;
	struct psc_listentry	rarq_lentry;
};

extern struct psc_poolmgr	*sli_readaheadrq_pool;
extern struct psc_listcache	 sli_lruslvrs;
extern struct psc_listcache	 sli_crcqslvrs;
extern struct psc_listcache	 sli_readaheadq;


static __inline int
slvr_cmp(const void *x, const void *y)
{
	const struct slvr *a = x, *b = y;

	return (CMP(a->slvr_num, b->slvr_num));
}

SPLAY_PROTOTYPE(biod_slvrtree, slvr, slvr_tentry, slvr_cmp)

#endif /* _SLIOD_SLVR_H_ */
