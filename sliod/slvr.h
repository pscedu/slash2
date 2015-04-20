/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLIOD_SLVR_H_
#define _SLIOD_SLVR_H_

#include "pfl/aio.h"
#include "pfl/atomic.h"
#include "pfl/dynarray.h"
#include "pfl/listcache.h"
#include "pfl/log.h"
#include "pfl/rpc.h"
#include "pfl/tree.h"
#include "pfl/vbitmap.h"

#include "bmap.h"
#include "bmap_iod.h"
#include "slab.h"
#include "slashrpc.h"
#include "subsys_iod.h"

struct bmap_iod_info;

/**
 * Bmap "sliver" used for scheduling dirty slivers to be CRC'd
 *	and sent to the MDS.
 * Note: slivers are locked through their bmap_iod_info lock.  A slab is
 *	the in-memory data represented by the sliver.
 */
struct slvr {
	uint16_t		 slvr_num;	/* bmap slvr offset */
	uint16_t		 slvr_flags;	/* see SLVR_* flags */
	uint16_t		 slvr_pndgwrts;	/* # writes in progess */
	uint32_t		 slvr_pndgreads;/* # reads in progress */
	 int32_t		 slvr_err;
	psc_spinlock_t		 slvr_lock;
	struct bmap_iod_info	*slvr_bii;
	struct timespec		 slvr_ts;
	struct sli_iocb		*slvr_iocb;
	struct sl_buffer	*slvr_slab;
	struct sli_aiocb_reply  *slvr_aioreply;
	struct psclist_head	 slvr_lentry;	/* dirty queue */
	SPLAY_ENTRY(slvr)	 slvr_tentry;	/* bmap tree entry */
};

/* slvr_flags */
#define SLVR_FAULTING		(1 <<  0)	/* contents loading from disk or net */
#define SLVR_PINNED		(1 <<  1)	/* active references or CRC dirty */
#define SLVR_DATARDY		(1 <<  2)	/* ready for read / write activity */
#define SLVR_DATAERR		(1 <<  3)
#define SLVR_LRU		(1 <<  4)	/* cached but not dirty */
#define SLVR_CRCDIRTY		(1 <<  5)	/* CRC does not match cached buffer */
#define SLVR_FREEING		(1 <<  6)	/* sliver is being reaped */
#define SLVR_AIOWAIT		(1 <<  7)	/* early return for AIO in repldst */
#define SLVR_REPLWIRE		(1 <<  8)	/* prevent AIO race in repldst */
#define SLVRF_READAHEAD		(1 <<  9)	/* loaded via readahead prediction */
#define SLVRF_ACCESSED		(1 << 10)	/* actually used by a client */

#define SLVR_LOCK(s)		spinlock(&(s)->slvr_lock)
#define SLVR_ULOCK(s)		freelock(&(s)->slvr_lock)
#define SLVR_RLOCK(s)		reqlock(&(s)->slvr_lock)
#define SLVR_URLOCK(s, lk)	ureqlock(&(s)->slvr_lock, (lk))
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
	((void *)(((s)->slvr_slab->slb_base) + ((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_fileoff(s, blk)						\
	((off_t)((slvr_2_bmap(s)->bcm_bmapno * SLASH_BMAP_SIZE) +	\
		((s)->slvr_num * SLASH_SLVR_SIZE) +			\
		((blk) * SLASH_SLVR_BLKSZ)))

#define slvr_2_crcbits(s)						\
	slvr_2_bii(s)->bii_crcstates[(s)->slvr_num]

#define slvr_2_crc(s)							\
	slvr_2_bii(s)->bii_crcs[(s)->slvr_num]

#define DEBUG_SLVR(level, s, fmt, ...)					\
	psclogs((level), SLISS_SLVR, "slvr@%p num=%hu pw=%u "		\
	    "pr=%u "							\
	    "ts="PSCPRI_TIMESPEC" "					\
	    "bii=%p slab=%p bmap=%p fid="SLPRI_FID" iocb=%p flgs="	\
	    "%s%s%s%s%s%s%s%s%s :: " fmt,				\
	    (s), (s)->slvr_num, (s)->slvr_pndgwrts,			\
	    (s)->slvr_pndgreads,					\
	    PSCPRI_TIMESPEC_ARGS(&(s)->slvr_ts),			\
	    (s)->slvr_bii, (s)->slvr_slab,				\
	    (s)->slvr_bii ? slvr_2_bmap(s) : NULL,			\
	    (s)->slvr_bii ?						\
	      fcmh_2_fid(slvr_2_bmap(s)->bcm_fcmh) : FID_ANY,		\
	    (s)->slvr_iocb,						\
	    (s)->slvr_flags & SLVR_FAULTING	? "f" : "-",		\
	    (s)->slvr_flags & SLVR_PINNED	? "p" : "-",		\
	    (s)->slvr_flags & SLVR_DATARDY	? "d" : "-",		\
	    (s)->slvr_flags & SLVR_DATAERR	? "E" : "-",		\
	    (s)->slvr_flags & SLVR_LRU		? "l" : "-",		\
	    (s)->slvr_flags & SLVR_CRCDIRTY	? "D" : "-",		\
	    (s)->slvr_flags & SLVR_FREEING	? "F" : "-",		\
	    (s)->slvr_flags & SLVR_REPLWIRE	? "w" : "-",		\
	    (s)->slvr_flags & SLVR_AIOWAIT	? "a" : "-",		\
	    ##__VA_ARGS__)

#define RIC_MAX_SLVRS_PER_IO	2

struct sli_aiocb_reply {
	struct psc_listentry	  aiocbr_lentry;
	struct iovec		  aiocbr_iovs[RIC_MAX_SLVRS_PER_IO];
	struct slvr		 *aiocbr_slvrs[RIC_MAX_SLVRS_PER_IO];
	int			  aiocbr_flags;
	int			  aiocbr_nslvrs;
	int			  aiocbr_niov;
	struct slashrpc_cservice *aiocbr_csvc;
	struct srt_bmapdesc	  aiocbr_sbd;
	uint64_t		  aiocbr_id;
	enum rw			  aiocbr_rw;
	uint32_t		  aiocbr_len;
	uint32_t		  aiocbr_off;
};

#define SLI_AIOCBSF_NONE	(0 << 0)
#define SLI_AIOCBSF_REPL	(1 << 0)
#define SLI_AIOCBSF_DIO		(1 << 1)

struct sli_iocb {
	struct psc_listentry	  iocb_lentry;
	struct slvr		 *iocb_slvr;
	struct aiocb		  iocb_aiocb;
	void			(*iocb_cbf)(struct sli_iocb *);
	int			  iocb_rc;
};

#define slvr_lookup(n, bii, rw)						\
	_slvr_lookup(PFL_CALLERINFO(), (n), (bii), (rw))

struct slvr *
	_slvr_lookup(const struct pfl_callerinfo *pci, uint32_t,
	    struct bmap_iod_info *, enum rw);
void	slvr_cache_init(void);
int	slvr_do_crc(struct slvr *, uint64_t *);
ssize_t	slvr_fsbytes_wio(struct slvr *, uint32_t, uint32_t);
ssize_t	slvr_io_prep(struct slvr *, uint32_t, uint32_t, enum rw, int);
int	slvr_lru_tryunpin_locked(struct slvr *);
void	slvr_repl_prep(struct slvr *);
void	slvr_rio_done(struct slvr *);
void	slvr_schedule_crc(struct slvr *);
void	slvr_wio_done(struct slvr *, int);
void	slvr_worker_init(void);

struct sli_aiocb_reply *
	sli_aio_reply_setup(struct pscrpc_request *, uint32_t, uint32_t,
	    struct slvr **, int, struct iovec *, int, enum rw);

struct sli_aiocb_reply *
	sli_aio_replreply_setup(struct pscrpc_request *, struct slvr *,
	    struct iovec *);

void	sli_aio_aiocbr_release(struct sli_aiocb_reply *);

int	slvr_buffer_reap(struct psc_poolmgr *);

struct sli_readaheadrq {
	struct sl_fidgen	rarq_fg;
	sl_bmapno_t		rarq_bno;
	int32_t			rarq_off;
	int32_t			rarq_size;
	struct psc_listentry	rarq_lentry;
};

extern struct psc_poolmgr	*sli_readaheadrq_pool;
extern struct psc_listcache	 sli_lruslvrs;
extern struct psc_listcache	 sli_crcqslvrs;
extern struct psc_listcache	 sli_readaheadq;
extern struct psc_waitq		 sli_slvr_waitq;

static __inline int
slvr_cmp(const void *x, const void *y)
{
	const struct slvr *a = x, *b = y;

	return (CMP(a->slvr_num, b->slvr_num));
}

SPLAY_PROTOTYPE(biod_slvrtree, slvr, slvr_tentry, slvr_cmp)

#endif /* _SLIOD_SLVR_H_ */
