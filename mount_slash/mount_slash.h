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

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>

#include "pfl/fs.h"
#include "psc_rpc/service.h"
#include "psc_util/atomic.h"
#include "psc_util/multiwait.h"

#include "bmap.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "slconfig.h"

struct pscfs_req;
struct pscrpc_request;

struct bmap_pagecache_entry;
struct bmpc_ioreq;

/* mount_slash thread types */
enum {
	MSTHRT_ATTRFLSH,		/* attr write data flush thread */
	MSTHRT_BMAPFLSH,		/* bmap write data flush thread */
	MSTHRT_BMAPFLSHRLS,		/* bmap lease releaser */
	MSTHRT_BMAPFLSHRPC,		/* async buffer thread for RPC reaping */
	MSTHRT_BMAPLSWATCHER,		/* bmap lease watcher */
	MSTHRT_BMAPREADAHEAD,		/* async thread for read ahead */
	MSTHRT_CONN,			/* connection monitor */
	MSTHRT_CTL,			/* control processor */
	MSTHRT_CTLAC,			/* control acceptor */
	MSTHRT_EQPOLL,			/* LNET event queue polling */
	MSTHRT_FS,			/* file system syscall handler workers */
	MSTHRT_FSMGR,			/* pscfs manager */
	MSTHRT_LNETAC,			/* lustre net accept thr */
	MSTHRT_NBRQ,			/* non-blocking RPC reply handler */
	MSTHRT_RCI,			/* service RPC reqs for CLI from ION */
	MSTHRT_RCM,			/* service RPC reqs for CLI from MDS */
	MSTHRT_TIOS,			/* timer iostat updater */
	MSTHRT_USKLNDPL			/* userland socket lustre net dev poll thr */
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
};

struct msrci_thread {
	struct pscrpc_thread		 mrci_prt;
	struct psc_multiwait		 mrci_mw;
};

struct msfs_thread {
	int				 mft_failcnt;
	size_t				 mft_uniqid;
	struct psc_multiwait		 mft_mw;
};

struct msbmfl_thread {
	int				 mbft_failcnt;
	struct psc_multiwait		 mbft_mw;
};

struct msbmflrls_thread {
	int				 mbfrlst_failcnt;
	struct psc_multiwait		 mbfrlst_mw;
};

struct msbmflrpc_thread {
	int				 mbflrpc_failcnt;
	struct psc_multiwait		 mbflrpc_mw;
};

struct msattrfl_thread {
	int				 maft_failcnt;
	struct psc_multiwait		 maft_mw;
};

struct msbmflra_thread {
	int				 mbfra_failcnt;
	struct psc_multiwait		 mbfra_mw;
};

struct msbmflwatcher_thread {
	int				 mbfwa_failcnt;
	struct psc_multiwait		 mbfwa_mw;
};

PSCTHR_MKCAST(msbmflrlsthr, msbmflrls_thread, MSTHRT_BMAPFLSHRLS);
PSCTHR_MKCAST(msbmflwthr, msbmflwatcher_thread, MSTHRT_BMAPLSWATCHER);
PSCTHR_MKCAST(msbmflthr, msbmfl_thread, MSTHRT_BMAPFLSH);
PSCTHR_MKCAST(msbmflrpc, msbmflrpc_thread, MSTHRT_BMAPFLSHRPC);
PSCTHR_MKCAST(msbmfrathr, msbmflra_thread, MSTHRT_BMAPREADAHEAD);
PSCTHR_MKCAST(msattrflthr, msattrfl_thread, MSTHRT_ATTRFLSH);
PSCTHR_MKCAST(msfsthr, msfs_thread, MSTHRT_FS);
PSCTHR_MKCAST(msrcithr, msrci_thread, MSTHRT_RCI);

#define MS_READAHEAD_MINSEQ		2
#define MS_READAHEAD_MAXPGS		256
#define MS_READAHEAD_DIRUNK		(-1)

#define MSL_RA_RESET(ra) do {						\
		(ra)->mra_nseq  = -(MS_READAHEAD_MINSEQ);		\
		(ra)->mra_bkwd = MS_READAHEAD_DIRUNK;			\
		(ra)->mra_raoff = 0;					\
	} while (0)

struct msl_ra {
	off_t				 mra_loff;	/* last offset */
	off_t				 mra_raoff;	/* current read ahead offset */
	off_t				 mra_lsz;	/* last size */
	int				 mra_nseq;	/* num sequential io's */
	int				 mra_nrios;	/* num read io's */
	int				 mra_bkwd;	/* reverse access io */
};

#define MAX_BMAPS_REQ			4

struct slc_async_req {
	struct psc_listentry		  car_lentry;
	struct pscrpc_async_args	  car_argv;
	int				(*car_cbf)(struct pscrpc_request *, int,
						struct pscrpc_async_args *);
	uint64_t			  car_id;
	size_t				  car_len;
	struct msl_fsrqinfo		 *car_fsrqinfo;
};

struct msl_fhent {			 /* XXX rename */
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct psclist_head		 mfh_lentry;
	int				 mfh_flags;

	int				 mfh_oflags;	/* open(2) flags */
	int				 mfh_flush_rc;	/* fsync(2) status */
	struct psc_lockedlist		 mfh_biorqs;	/* track biorqs (flush) */
	struct psc_lockedlist		 mfh_ra_bmpces;	/* read ahead bmpce's */
	struct msl_ra			 mfh_ra;	/* readahead tracking */

	/* stats */
	struct timespec			 mfh_open_time;	/* clock_gettime(2) at open(2) time */
	struct sl_timespec		 mfh_open_atime;/* st_atime at open(2) time */
	off_t				 mfh_nbytes_rd;
	off_t				 mfh_nbytes_wr;
};

#define MSL_FHENT_RASCHED		(1 << 0)
#define MSL_FHENT_CLOSING		(1 << 1)

#define MFH_LOCK(m)			spinlock(&(m)->mfh_lock)
#define MFH_ULOCK(m)			freelock(&(m)->mfh_lock)
#define MFH_RLOCK(m)			reqlock(&(m)->mfh_lock)
#define MFH_URLOCK(m, locked)		ureqlock(&(m)->mfh_lock, (locked))

struct msl_fsrqinfo {
	struct bmpc_ioreq		*mfsrq_biorq[MAX_BMAPS_REQ];
	struct bmap_pagecache_entry	*mfsrq_bmpceatt;
	struct msl_fhent		*mfsrq_fh;
	char				*mfsrq_buf;
	size_t				 mfsrq_size;
	off_t				 mfsrq_off;
	int				 mfsrq_flags;
	int				 mfsrq_err;
	int				 mfsrq_ref;	/* # car's needed to satisfy this req */
	enum rw				 mfsrq_rw;
	struct pscfs_req		*mfsrq_pfr;
	struct psclist_head		 mfsrq_lentry;	/* pending AIOs in struct bmap_pagecache_entry  */
};

#define MFSRQ_AIOWAIT			(1 << 0)
#define MFSRQ_READY			(1 << 1)
#define MFSRQ_BMPCEATT			(1 << 2)
#define MFSRQ_DIO			(1 << 3)

void	msl_fsrqinfo_write(struct msl_fsrqinfo *);
int	msl_fsrqinfo_state(struct msl_fsrqinfo *, int, int, int);

#define msl_fsrqinfo_isset(q, f)	msl_fsrqinfo_state((q), (f), 0, 0)
#define msl_fsrqinfo_aioisset(q)	msl_fsrqinfo_state((q), MFSRQ_AIOWAIT, 0, 0)
#define msl_fsrqinfo_aioset(q)		msl_fsrqinfo_state((q), MFSRQ_AIOWAIT, 1, 0)
#define msl_fsrqinfo_readywait(q)	msl_fsrqinfo_state((q), MFSRQ_READY, 0, 1)
#define msl_fsrqinfo_readyset(q)	msl_fsrqinfo_state((q), MFSRQ_READY, 1, 1)

struct resprof_cli_info {
	struct psc_dynarray		 rpci_pinned_bmaps;
};

static __inline struct resprof_cli_info *
res2rpci(struct sl_resource *res)
{
	return (resprof_get_pri(res));
}

#define MAX_PENDING_RPCS 16

/* CLI-specific data for struct sl_resm */
struct resm_cli_info {
	struct pfl_mutex		 rmci_mutex;
	struct psc_multiwaitcond	 rmci_mwc;
	struct srm_bmap_release_req	 rmci_bmaprls;
	struct psc_listcache		 rmci_async_reqs;
	psc_atomic32_t                   rmci_pndg_rpcs;
};

static __inline struct resm_cli_info *
resm2rmci(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

#define msl_read(pfr, fh, buf, size, off)	msl_io((pfr), (fh), (buf), (size), (off), SL_READ)
#define msl_write(pfr, fh, buf, size, off)	msl_io((pfr), (fh), (char *)(buf), (size), (off), SL_WRITE)

#define msl_biorq_destroy(r) _msl_biorq_destroy(PFL_CALLERINFOSS(SLSS_BMAP), (r))

struct slashrpc_cservice *
	 msl_bmap_to_csvc(struct bmapc_memb *, int);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
void     msl_bmpces_fail(struct bmpc_ioreq *);
void	_msl_biorq_destroy(const struct pfl_callerinfo *, struct bmpc_ioreq *);
void     msl_mfh_seterr(struct msl_fhent *);
int	 msl_dio_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
ssize_t	 msl_io(struct pscfs_req *, struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_read_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
void	 msl_reada_rpc_launch(struct bmap_pagecache_entry **, int);
int	 msl_readahead_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
int	 msl_stat(struct fidc_membh *, void *);
int	 msl_write_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_write_rpcset_cb(struct pscrpc_request_set *, void *, int);

size_t	 msl_pages_copyout(struct bmpc_ioreq *, char *);

struct slashrpc_cservice *
	 msl_try_get_replica_res(struct bmapc_memb *, int);
struct msl_fhent *
	 msl_fhent_new(struct fidc_membh *);

void	 msbmapflushthr_spawn(void);
void	 msctlthr_begin(struct psc_thread *);
void	 msctlthr_spawn(void);
void	 mstimerthr_spawn(void);

#define bmap_flushq_wake(mode, t)					\
	_bmap_flushq_wake(PFL_CALLERINFOSS(SLSS_BMAP), (mode), (t))

void	 _bmap_flushq_wake(const struct pfl_callerinfo *, int, struct timespec *);
void	 bmap_flush_resched(struct bmpc_ioreq *);

/* bmap flush modes (bmap_flushq_wake) */
#define BMAPFLSH_TIMEOA		(1 << 0)
#define BMAPFLSH_WAKE		(1 << 1)
#define BMAPFLSH_RPCWAIT	(1 << 2)
#define BMAPFLSH_EXPIRE		(1 << 3)

extern char			 ctlsockfn[];
extern sl_ios_id_t		 prefIOS;
extern struct psc_listcache	 bmapFlushQ;
extern struct sl_resm		*slc_rmc_resm;
extern char			 mountpoint[];

extern struct psc_waitq		 msl_fhent_flush_waitq;

extern struct psc_iostats	 msl_diord_stat;
extern struct psc_iostats	 msl_diowr_stat;
extern struct psc_iostats	 msl_rdcache_stat;
extern struct psc_iostats	 msl_racache_stat;

extern struct psc_iostats	 msl_io_1b_stat;
extern struct psc_iostats	 msl_io_1k_stat;
extern struct psc_iostats	 msl_io_4k_stat;
extern struct psc_iostats	 msl_io_16k_stat;
extern struct psc_iostats	 msl_io_64k_stat;
extern struct psc_iostats	 msl_io_128k_stat;
extern struct psc_iostats	 msl_io_512k_stat;
extern struct psc_iostats	 msl_io_1m_stat;

extern struct psc_listcache	 bmapTimeoutQ;
extern struct psc_waitq		 bmapFlushWaitq;

extern struct psc_listcache	 bmapReadAheadQ;
extern struct pscrpc_nbreqset	*pndgReadaReqs;
extern struct pscrpc_nbreqset	*pndgBmaplsReqs;

extern struct psc_poolmgr	*slc_async_req_pool;
extern struct psc_poolmgr	*slc_biorq_pool;
extern psc_atomic32_t		 offline_nretries;

#endif /* _MOUNT_SLASH_H_ */
