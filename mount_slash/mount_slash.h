/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>

#include "pfl/fs.h"
#include "pfl/service.h"
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
	MSTHRT_NBRQ,			/* non-blocking RPC reply handler */
	MSTHRT_RCI,			/* service RPC reqs for CLI from ION */
	MSTHRT_RCM,			/* service RPC reqs for CLI from MDS */
	MSTHRT_TIOS,			/* timer iostat updater */
	MSTHRT_USKLNDPL			/* userland socket lustre net dev poll thr */
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
	struct psc_multiwait		 mrcm_mw;
};

struct msrci_thread {
	struct pscrpc_thread		 mrci_prt;
	struct psc_multiwait		 mrci_mw;
};

struct msfs_thread {
	size_t				 mft_uniqid;
	struct psc_multiwait		 mft_mw;
};

struct msbmfl_thread {
	int				 mbft_failcnt;
	struct psc_multiwait		 mbft_mw;
};

struct msbmflrls_thread {
	struct psc_multiwait		 mbfrlst_mw;
};

struct msbmflrpc_thread {
	struct psc_multiwait		 mbflrpc_mw;
};

struct msattrfl_thread {
	struct psc_multiwait		 maft_mw;
};

struct msbmflra_thread {
	struct psc_multiwait		 mbfra_mw;
};

struct msbmflwatcher_thread {
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
PSCTHR_MKCAST(msrcmthr, msrcm_thread, MSTHRT_RCM);

#define MS_READAHEAD_MINSEQ		2
#define MS_READAHEAD_MAXPGS		256
#define MS_READAHEAD_DIRUNK		(-1)

#define MSL_RA_RESET(ra)						\
	do {								\
		(ra)->mra_nseq = -MS_READAHEAD_MINSEQ;			\
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

#define MAX_BMAPS_REQ			2

struct slc_async_req {
	struct psc_listentry		  car_lentry;
	struct pscrpc_async_args	  car_argv;
	int				(*car_cbf)(struct pscrpc_request *, int,
					    struct pscrpc_async_args *);
	uint64_t			  car_id;
	struct msl_fsrqinfo		 *car_fsrqinfo;
};

struct msl_fhent {
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct psclist_head		 mfh_lentry;
	int				 mfh_flags;
	int				 mfh_refcnt;

	int				 mfh_retries;
	int				 mfh_oflags;	/* open(2) flags */
	int				 mfh_flush_rc;	/* fsync(2) status */
	struct psc_lockedlist		 mfh_biorqs;	/* track biorqs (flush) */
	struct psc_lockedlist		 mfh_ra_bmpces;	/* read ahead bmpce's */
	struct msl_ra			 mfh_ra;	/* readahead tracking */

	/* stats */
	struct timespec			 mfh_open_time;	/* clock_gettime(2) at open(2) time */
	struct pfl_timespec		 mfh_open_atime;/* st_atime at open(2) time */
	off_t				 mfh_nbytes_rd;
	off_t				 mfh_nbytes_wr;
};

#define MSL_FHENT_RASCHED		(1 << 0)
#define MSL_FHENT_CLOSING		(1 << 1)

#define MFH_LOCK(m)			spinlock(&(m)->mfh_lock)
#define MFH_ULOCK(m)			freelock(&(m)->mfh_lock)
#define MFH_RLOCK(m)			reqlock(&(m)->mfh_lock)
#define MFH_URLOCK(m, lk)		ureqlock(&(m)->mfh_lock, (lk))

struct msl_fsrqinfo {
	struct bmpc_ioreq		*mfsrq_biorq[MAX_BMAPS_REQ];
	struct msl_fhent		*mfsrq_mfh;
	char				*mfsrq_buf;
	size_t				 mfsrq_size;	/* incoming request size */
	size_t				 mfsrq_len;	/* outgoing I/O result, must be accurate if no error */
	off_t				 mfsrq_off;
	int				 mfsrq_flags;
	int				 mfsrq_err;
	int				 mfsrq_ref;	/* taken by biorq and the thread that does the I/O */
	enum rw				 mfsrq_rw;
};

#define MFSRQ_AIOWAIT			(1 << 0)

void	msl_fsrqinfo_biorq_add(struct msl_fsrqinfo *, struct bmpc_ioreq *,int);

struct resprof_cli_info {
	struct psc_dynarray		 rpci_pinned_bmaps;
	int				 rpci_flags;
};

#define RPCIF_AVOID			(1 << 0)

static __inline struct resprof_cli_info *
res2rpci(struct sl_resource *res)
{
	return (resprof_get_pri(res));
}

/* CLI-specific data for struct sl_resm */
struct resm_cli_info {
	struct srm_bmap_release_req	 rmci_bmaprls;
	struct psc_listcache		 rmci_async_reqs;
	psc_atomic32_t			 rmci_infl_rpcs;
};

static __inline struct resm_cli_info *
resm2rmci(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

#define msl_read(pfr, fh, buf, size, off)	msl_io((pfr), (fh), (buf), (size), (off), SL_READ)
#define msl_write(pfr, fh, buf, size, off)	msl_io((pfr), (fh), (char *)(buf), (size), (off), SL_WRITE)

#define msl_biorq_destroy(r)	_msl_biorq_destroy(PFL_CALLERINFOSS(SLSS_BMAP), (r))

struct slashrpc_cservice *
	 msl_bmap_to_csvc(struct bmapc_memb *, int);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
void	 msl_bmpces_fail(struct bmpc_ioreq *, int);
void	_msl_biorq_destroy(const struct pfl_callerinfo *, struct bmpc_ioreq *);

void	 mfh_decref(struct msl_fhent *);
void	 mfh_incref(struct msl_fhent *);
void	 mfh_seterr(struct msl_fhent *, int);

int	 msl_dio_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
ssize_t	 msl_io(struct pscfs_req *, struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_read_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
void	 msl_reada_rpc_launch(struct bmap_pagecache_entry **, int);
int	 msl_readahead_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
int	 msl_stat(struct fidc_membh *, void *);
int	 msl_write_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);

size_t	 msl_pages_copyout(struct bmpc_ioreq *);
int	 msl_fd_should_retry(struct msl_fhent *, int);

struct slashrpc_cservice *
	 msl_try_get_replica_res(struct bmapc_memb *, int);
struct msl_fhent *
	 msl_fhent_new(struct fidc_membh *);

void	 msbmapflushthr_spawn(void);
void	 msctlthr_spawn(void);
void	 mstimerthr_spawn(void);

#define bmap_flushq_wake(reason)						\
	_bmap_flushq_wake(PFL_CALLERINFOSS(SLSS_BMAP), (reason))

void	 _bmap_flushq_wake(const struct pfl_callerinfo *, int);
void	  bmap_flush_resched(struct bmpc_ioreq *, int);

/* bmap flush modes (bmap_flushq_wake) */
#define BMAPFLSH_RPCWAIT	(1 << 0)
#define BMAPFLSH_EXPIRE		(1 << 1)
#define BMAPFLSH_TIMEOA		(1 << 2)

enum {
	SLC_OPST_AIO_PLACED,
	SLC_OPST_BIORQ_ALLOC,
	SLC_OPST_BIORQ_DESTROY,
	SLC_OPST_BIORQ_MAX,
	SLC_OPST_BIORQ_RESTART,
	SLC_OPST_BMAP_DIO,
	SLC_OPST_BMAP_FLUSH,

	SLC_OPST_BMAP_LEASE_EXT_ABRT,
	SLC_OPST_BMAP_LEASE_EXT_DONE,
	SLC_OPST_BMAP_LEASE_EXT_FAIL,
	SLC_OPST_BMAP_LEASE_EXT_SEND,

	SLC_OPST_BMAP_REASSIGN_ABRT,
	SLC_OPST_BMAP_REASSIGN_BAIL,
	SLC_OPST_BMAP_REASSIGN_DONE,
	SLC_OPST_BMAP_REASSIGN_FAIL,
	SLC_OPST_BMAP_REASSIGN_SEND,

	SLC_OPST_BMAP_RELEASE,
	SLC_OPST_BMAP_RETRIEVE,

	SLC_OPST_BMAP_FLUSH_RESCHED,

	SLC_OPST_BMPCE_EIO,
	SLC_OPST_BMPCE_GET,
	SLC_OPST_BMPCE_HIT,
	SLC_OPST_BMPCE_INSERT,
	SLC_OPST_BMPCE_PUT,
	SLC_OPST_BMPCE_REAP,
	SLC_OPST_BMPCE_BMAP_REAP,
	SLC_OPST_CLOSE,
	SLC_OPST_CLOSE_DONE,
	SLC_OPST_CREAT,
	SLC_OPST_CREAT_DONE,
	SLC_OPST_DIO_CB0,
	SLC_OPST_DIO_CB_ADD,
	SLC_OPST_DIO_CB_READ,
	SLC_OPST_DIO_CB_WRITE,

	SLC_OPST_DIRCACHE_HIT,
	SLC_OPST_DIRCACHE_HIT_EOF,
	SLC_OPST_DIRCACHE_ISSUE,
	SLC_OPST_DIRCACHE_LOOKUP_HIT,
	SLC_OPST_DIRCACHE_RACE,
	SLC_OPST_DIRCACHE_RAISSUE,
	SLC_OPST_DIRCACHE_REG_ENTRY,
	SLC_OPST_DIRCACHE_REL_ENTRY,
	SLC_OPST_DIRCACHE_WAIT,

	SLC_OPST_FLUSH,
	SLC_OPST_FLUSH_ATTR,
	SLC_OPST_FLUSH_ATTR_WAIT,
	SLC_OPST_FLUSH_DONE,
	SLC_OPST_FSRQ_READ,
	SLC_OPST_FSRQ_READ_FREE,
	SLC_OPST_FSRQ_WRITE,
	SLC_OPST_FSRQ_WRITE_FREE,
	SLC_OPST_FSYNC,
	SLC_OPST_FSYNC_DONE,
	SLC_OPST_GETATTR,
	SLC_OPST_GETXATTR,
	SLC_OPST_GETXATTR_NOSYS,
	SLC_OPST_LINK,
	SLC_OPST_LISTXATTR,
	SLC_OPST_LEASE_REFRESH,
	SLC_OPST_MKDIR,
	SLC_OPST_MKNOD,
	SLC_OPST_OFFLINE_NO_RETRY,
	SLC_OPST_OFFLINE_RETRY,
	SLC_OPST_OFFLINE_RETRY_CLEAR_ERR,
	SLC_OPST_PREFETCH,
	SLC_OPST_READ,
	SLC_OPST_READDIR,
	SLC_OPST_READ_AHEAD,
	SLC_OPST_READ_AHEAD_CB,
	SLC_OPST_READ_AHEAD_CB_ADD,
	SLC_OPST_READ_AHEAD_CB_PAGES,
	SLC_OPST_READ_AIO_NOT_FOUND,
	SLC_OPST_READ_AIO_WAIT,
	SLC_OPST_READ_AIO_WAIT_MAX,
	SLC_OPST_READ_CB,
	SLC_OPST_READ_CB_ADD,
	SLC_OPST_READ_DONE,
	SLC_OPST_READ_RPC_LAUNCH,
	SLC_OPST_REMOVEXATTR,
	SLC_OPST_RENAME,
	SLC_OPST_RENAME_DONE,
	SLC_OPST_RMDIR,
	SLC_OPST_RMDIR_DONE,
	SLC_OPST_RPC_PUSH_REQ_FAIL,
	SLC_OPST_SETATTR,
	SLC_OPST_SETATTR_DONE,
	SLC_OPST_SETXATTR,
	SLC_OPST_SLC_FCMH_CTOR,
	SLC_OPST_SLC_FCMH_DTOR,
	SLC_OPST_SRMT_WRITE,
	SLC_OPST_SRMT_WRITE_CALLBACK,
	SLC_OPST_SYMLINK,
	SLC_OPST_TRUNCATE_FULL,
	SLC_OPST_TRUNCATE_PART,
	SLC_OPST_UNLINK,
	SLC_OPST_UNLINK_DONE,
	SLC_OPST_VERSION,
	SLC_OPST_WRITE,
	SLC_OPST_WRITE_COALESCE,
	SLC_OPST_WRITE_COALESCE_MAX,
	SLC_OPST_WRITE_DONE
};

enum {
	SLC_FAULT_READAHEAD_CB_EIO,
	SLC_FAULT_READRPC_OFFLINE,
	SLC_FAULT_READ_CB_EIO,
	SLC_FAULT_REQUEST_TIMEOUT
};

extern const char		*ctlsockfn;
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
extern struct psc_poolmgr	*mfh_pool;

extern psc_atomic32_t		 max_nretries;

#endif /* _MOUNT_SLASH_H_ */
