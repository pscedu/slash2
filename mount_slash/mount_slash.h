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

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>

#include "pfl/atomic.h"
#include "pfl/fs.h"
#include "pfl/multiwait.h"
#include "pfl/service.h"

#include "bmap.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "slconfig.h"
#include "slconn.h"

struct pscfs_req;
struct pscrpc_request;

struct bmap_pagecache_entry;
struct bmpc_ioreq;
struct dircache_page;

/* mount_slash thread types */
enum {
	MSTHRT_ATTR_FLUSH,		/* attr write data flush thread */
	MSTHRT_BENCH,			/* I/O benchmarking thread */
	MSTHRT_BRELEASE,		/* bmap lease releaser */
	MSTHRT_BWATCH,			/* bmap lease watcher */
	MSTHRT_CONN,			/* connection monitor */
	MSTHRT_CTL,			/* control processor */
	MSTHRT_CTLAC,			/* control acceptor */
	MSTHRT_EQPOLL,			/* LNET event queue polling */
	MSTHRT_FREAP,			/* fcmh reap thread */
	MSTHRT_FLUSH,			/* bmap write data flush thread */
	MSTHRT_FS,			/* file system syscall handler workers */
	MSTHRT_FSMGR,			/* pscfs manager */
	MSTHRT_NBRQ,			/* non-blocking RPC reply handler */
	MSTHRT_RCI,			/* service RPC reqs for CLI from ION */
	MSTHRT_RCM,			/* service RPC reqs for CLI from MDS */
	MSTHRT_READAHEAD,		/* readahead thread */
	MSTHRT_TIOS,			/* timer iostat updater */
	MSTHRT_USKLNDPL,		/* userland socket lustre net dev poll thr */
	MSTHRT_WORKER			/* generic worker */
};

struct msattrflush_thread {
	struct psc_multiwait		 maft_mw;
};

struct msbrelease_thread {
	struct psc_multiwait		 mbrt_mw;
};

struct msbwatch_thread {
	struct psc_multiwait		 mbwt_mw;
};

struct msflush_thread {
	int				 mflt_failcnt;
	struct psc_multiwait		 mflt_mw;
};

struct msfs_thread {
	size_t				 mft_uniqid;
	struct psc_multiwait		 mft_mw;
	char				 mft_uprog[256];
	struct pscfs_req		*mft_pfr;
};

struct msrci_thread {
	struct pscrpc_thread		 mrci_prt;
	struct psc_multiwait		 mrci_mw;
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
	struct psc_multiwait		 mrcm_mw;
};

struct msreadahead_thread {
	struct psc_multiwait		 mrat_mw;
};

PSCTHR_MKCAST(msattrflushthr, msattrflush_thread, MSTHRT_ATTR_FLUSH);
PSCTHR_MKCAST(msflushthr, msflush_thread, MSTHRT_FLUSH);
PSCTHR_MKCAST(msbreleasethr, msbrelease_thread, MSTHRT_BRELEASE);
PSCTHR_MKCAST(msbwatchthr, msbwatch_thread, MSTHRT_BWATCH);
PSCTHR_MKCAST(msfsthr, msfs_thread, MSTHRT_FS);
PSCTHR_MKCAST(msrcithr, msrci_thread, MSTHRT_RCI);
PSCTHR_MKCAST(msrcmthr, msrcm_thread, MSTHRT_RCM);
PSCTHR_MKCAST(msreadaheadthr, msreadahead_thread, MSTHRT_READAHEAD);

#define MS_READAHEAD_MAXPGS		32

struct msl_ra {
	off_t				 mra_loff;	/* last offset */
	off_t				 mra_lsz;	/* last size */
	int				 mra_nseq;	/* num sequential io's */
	off_t				 mra_raoff;
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

struct slc_wkdata_readdir {
	struct fidc_membh		*d;
	struct dircache_page		*pg;
	off_t				 off;
	size_t				 size;
};

/* file handle in struct fuse_file_info */
struct msl_fhent {
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct psclist_head		 mfh_lentry;
	int				 mfh_flags;
	int				 mfh_refcnt;
	pid_t				 mfh_pid;
	pid_t				 mfh_sid;

	int				 mfh_retries;
	int				 mfh_oflags;	/* open(2) flags */
	struct msl_ra			 mfh_ra;	/* readahead tracking */

	/* stats */
	struct timespec			 mfh_open_time;	/* clock_gettime(2) at open(2) time */
	struct pfl_timespec		 mfh_open_atime;/* st_atime at open(2) time */
	off_t				 mfh_nbytes_rd;
	off_t				 mfh_nbytes_wr;
	char				 mfh_uprog[256];
};

#define MSL_FHENT_CLOSING		(1 << 0)

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
};

#define MFSRQ_NONE			(0 << 0)
#define MFSRQ_READ			(1 << 0)
#define MFSRQ_AIOWAIT			(1 << 1)

#define mfsrq2pfr(q)			((struct pscfs_req *)(q) - 1)

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
#define msl_write(pfr, fh, buf, size, off)	msl_io((pfr), (fh), (buf), (size), (off), SL_WRITE)

#define msl_biorq_destroy(r)	_msl_biorq_destroy(PFL_CALLERINFOSS(SLSS_BMAP), (r))

int	 msl_bmap_to_csvc(struct bmapc_memb *, int, struct slashrpc_cservice **);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
void	 msl_bmpces_fail(struct bmpc_ioreq *, int);
void	_msl_biorq_destroy(const struct pfl_callerinfo *, struct bmpc_ioreq *);

void	 mfh_decref(struct msl_fhent *);
void	 mfh_incref(struct msl_fhent *);

int	 msl_dio_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
ssize_t	 msl_io(struct pscfs_req *, struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_read_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
void	 msl_reada_rpc_launch(struct psc_dynarray *, int, int, struct bmap *);
int	 msl_readahead_cb(struct pscrpc_request *, int, struct pscrpc_async_args *);
int	 msl_stat(struct fidc_membh *, void *);

ssize_t	slc_getxattr(const struct pscfs_clientctx *,
	    const struct pscfs_creds *, const char *, void *, size_t,
	    struct fidc_membh *, size_t *);

void	 msl_readdir_error(struct fidc_membh *, struct dircache_page *, int);
void	 msl_readdir_finish(struct fidc_membh *, struct dircache_page *, int,
	    int, int, struct iovec *);

size_t	 msl_pages_copyout(struct bmpc_ioreq *);
int	 msl_fd_should_retry(struct msl_fhent *, struct pscfs_req *, int);

int	 msl_try_get_replica_res(struct bmapc_memb *, int, int,
	    struct slashrpc_cservice **);
struct msl_fhent *
	 msl_fhent_new(struct pscfs_req *, struct fidc_membh *);

void	 msbmapthr_spawn(void);
void	 msctlthr_spawn(void);
void	 mstimerthr_spawn(void);

#define bmap_flushq_wake(reason)						\
	_bmap_flushq_wake(PFL_CALLERINFOSS(SLSS_BMAP), (reason))

void	 _bmap_flushq_wake(const struct pfl_callerinfo *, int);
void	  bmap_flush_resched(struct bmpc_ioreq *, int);

void	 slc_getprog(pid_t, char *, size_t);
void	 slc_setprefios(sl_ios_id_t);
int	 msl_pages_fetch(struct bmpc_ioreq *);

/* bmap flush modes (bmap_flushq_wake) */
#define BMAPFLSH_RPCWAIT	(1 << 0)
#define BMAPFLSH_EXPIRE		(1 << 1)
#define BMAPFLSH_TIMEOA		(1 << 2)
#define BMAPFLSH_TRUNCATE	(1 << 3)
#define BMAPFLSH_RPCDONE	(1 << 4)

enum {
	SLC_OPST_ACCESS,

	SLC_OPST_AIO_PLACED,
	SLC_OPST_BIORQ_ALLOC,
	SLC_OPST_BIORQ_DESTROY,
	SLC_OPST_BIORQ_DESTROY_BATCH,
	SLC_OPST_BIORQ_MAX,
	SLC_OPST_BIORQ_RESTART,

	SLC_OPST_BMAP_ALLOC_STALL,
	SLC_OPST_BMAP_DIO,
	SLC_OPST_BMAP_FLUSH,
	SLC_OPST_BMAP_WAIT_EMPTY,

	SLC_OPST_BMAP_LEASE_EXT_HIT,
	SLC_OPST_BMAP_LEASE_EXT_ABRT,
	SLC_OPST_BMAP_LEASE_EXT_DONE,
	SLC_OPST_BMAP_LEASE_EXT_FAIL,
	SLC_OPST_BMAP_LEASE_EXT_SEND,
	SLC_OPST_BMAP_LEASE_EXT_WAIT,

	SLC_OPST_BMAP_REASSIGN_ABRT,
	SLC_OPST_BMAP_REASSIGN_BAIL,
	SLC_OPST_BMAP_REASSIGN_DONE,
	SLC_OPST_BMAP_REASSIGN_FAIL,
	SLC_OPST_BMAP_REASSIGN_SEND,

	SLC_OPST_BMAP_RELEASE,
	SLC_OPST_BMAP_RELEASE_READ,
	SLC_OPST_BMAP_RELEASE_WRITE,
	SLC_OPST_BMAP_RETRIEVE,

	SLC_OPST_BMAP_FLUSH_RESCHED,
	SLC_OPST_BMAP_FLUSH_RPCWAIT,
	SLC_OPST_BMAP_FLUSH_COALESCE_CONTIG,
	SLC_OPST_BMAP_FLUSH_COALESCE_EXPIRE,
	SLC_OPST_BMAP_FLUSH_COALESCE_RESTART,

	SLC_OPST_BMPCE_EIO,
	SLC_OPST_BMPCE_GET,
	SLC_OPST_BMPCE_HIT,
	SLC_OPST_BMPCE_INSERT,
	SLC_OPST_BMPCE_PUT,
	SLC_OPST_BMPCE_REAP,
	SLC_OPST_BMPCE_BMAP_REAP,
	SLC_OPST_CREAT,

	SLC_OPST_DELETE_MARKED,
	SLC_OPST_DELETE_SKIPPED,

	SLC_OPST_DIO_CB0,
	SLC_OPST_DIO_CB_ADD,
	SLC_OPST_DIO_CB_READ,
	SLC_OPST_DIO_CB_WRITE,
	SLC_OPST_DIO_READ,
	SLC_OPST_DIO_WRITE,
	SLC_OPST_DIO_ADD_REQ_FAIL,

	SLC_OPST_DIRCACHE_HIT,
	SLC_OPST_DIRCACHE_HIT_EOF,
	SLC_OPST_DIRCACHE_ISSUE,
	SLC_OPST_DIRCACHE_LOOKUP_HIT,
	SLC_OPST_DIRCACHE_LOOKUP_MISS,
	SLC_OPST_DIRCACHE_REG_ENTRY,
	SLC_OPST_DIRCACHE_UNUSED,
	SLC_OPST_DIRCACHE_WAIT,

	SLC_OPST_FLUSH,
	SLC_OPST_FLUSH_ATTR,
	SLC_OPST_FLUSH_ATTR_WAIT,

	SLC_OPST_FLUSH_RPC_EXPIRE,
	SLC_OPST_FLUSH_SKIP_EXPIRE,

	SLC_OPST_FSRQ_READ,
	SLC_OPST_FSRQ_READ_OK,
	SLC_OPST_FSRQ_READ_ERR,
	SLC_OPST_FSRQ_READ_NOREG,
	SLC_OPST_FSRQ_WRITE,
	SLC_OPST_FSRQ_WRITE_OK,
	SLC_OPST_FSRQ_WRITE_ERR,
	SLC_OPST_FSYNC,
	SLC_OPST_FSYNCDIR,
	SLC_OPST_GETATTR,
	SLC_OPST_GETXATTR,
	SLC_OPST_GETXATTR_RPC,
	SLC_OPST_LINK,
	SLC_OPST_LISTXATTR,
	SLC_OPST_LISTXATTR_RPC,
	SLC_OPST_LEASE_REFRESH,
	SLC_OPST_LOOKUP,
	SLC_OPST_MKDIR,
	SLC_OPST_MKNOD,

	SLC_OPST_OFFLINE_NO_RETRY,
	SLC_OPST_OFFLINE_RETRY,
	SLC_OPST_OFFLINE_RETRY_CLEAR_ERR,

	SLC_OPST_OPEN,

	SLC_OPST_READ,
	SLC_OPST_READ_PART_VALID,
	SLC_OPST_READ_ADD_REQ_FAIL,

	SLC_OPST_READDIR,
	SLC_OPST_READDIR_STALE,
	SLC_OPST_READDIR_DROP,

	SLC_OPST_READ_AHEAD,
	SLC_OPST_READ_AHEAD_CB_ADD,
	SLC_OPST_READ_AHEAD_CB_PAGE,
	SLC_OPST_READ_AHEAD_OVERRUN,

	SLC_OPST_READ_AIO_NOT_FOUND,
	SLC_OPST_READ_AIO_WAIT,
	SLC_OPST_READ_AIO_WAIT_MAX,

	SLC_OPST_READ_CB,
	SLC_OPST_READ_CB_ADD,
	SLC_OPST_READ_RPC_LAUNCH,

	SLC_OPST_READAHEAD_CB,
	SLC_OPST_READAHEAD_FETCH,
	SLC_OPST_READAHEAD_RPC_LAUNCH,

	SLC_OPST_READLINK,
	SLC_OPST_RELEASE,
	SLC_OPST_RELEASEDIR,
	SLC_OPST_REMOVEXATTR,
	SLC_OPST_RENAME,
	SLC_OPST_RMDIR,
	SLC_OPST_SETATTR,
	SLC_OPST_SETXATTR,
	SLC_OPST_SLC_FCMH_CTOR,
	SLC_OPST_SLC_FCMH_DTOR,
	SLC_OPST_STATFS,
	SLC_OPST_SYMLINK,
	SLC_OPST_TRUNCATE_FULL,
	SLC_OPST_TRUNCATE_PART,
	SLC_OPST_UNLINK,
	SLC_OPST_VERSION,
	SLC_OPST_WRITE,
	SLC_OPST_WRITE_CALLBACK,
	SLC_OPST_WRITE_COALESCE,
	SLC_OPST_WRITE_COALESCE_MAX,
	SLC_OPST_WRITE_RPC
};

enum {
	SLC_FAULT_READAHEAD_CB_EIO,
	SLC_FAULT_READRPC_OFFLINE,
	SLC_FAULT_READ_CB_EIO,
	SLC_FAULT_REQUEST_TIMEOUT
};

extern const char		*ctlsockfn;
extern sl_ios_id_t		 prefIOS;
extern struct sl_resm		*slc_rmc_resm;
extern char			 mountpoint[];

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

extern struct psc_listcache	 slc_attrtimeoutq;
extern struct psc_listcache	 slc_bmapflushq;
extern struct psc_listcache	 slc_bmaptimeoutq;
extern struct psc_listcache	 slc_readaheadq;

extern struct psc_poolmgr	*slc_async_req_pool;
extern struct psc_poolmgr	*slc_biorq_pool;
extern struct psc_poolmgr	*slc_mfh_pool;

extern psc_atomic32_t		 slc_max_nretries;
extern psc_atomic32_t		 slc_max_readahead;

#endif /* _MOUNT_SLASH_H_ */
