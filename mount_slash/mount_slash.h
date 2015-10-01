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

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>
#include <sys/statvfs.h>

#include "pfl/atomic.h"
#include "pfl/fs.h"
#include "pfl/multiwait.h"
#include "pfl/opstats.h"
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
	MSTHRT_OPSTIMER,		/* opstats updater */
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

#define NUM_BMAP_FLUSH_THREADS		16
#define NUM_ATTR_FLUSH_THREADS		4
#define NUM_READAHEAD_THREADS		4

#define MS_READAHEAD_MAXPGS		64
#define MS_READAHEAD_PIPESZ		128

#define MSL_FIDNS_RPATH			".slfidns"

/*
 * Maximum number of bmaps that may span an I/O request.  We currently
 * limit FUSE to 128MB I/Os and bmaps are by default 128MB, meaning any
 * I/O can never span more than two bmap regions.
 *
 * XXX This value should be calculated dynamically.
 */
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

	/* XXX these should be 32-bit as they can relative to bmap */
	off_t				 mfh_predio_lastoff;	/* last offset */
	off_t				 mfh_predio_lastsz;	/* last size */
	off_t				 mfh_predio_off;	/* next offset */
	int				 mfh_predio_nseq;	/* num sequential IOs */

	/* stats */
	struct timespec			 mfh_open_time;	/* clock_gettime(2) at open(2) time */
	struct pfl_timespec		 mfh_open_atime;/* st_atime at open(2) time */
	off_t				 mfh_nbytes_rd;
	off_t				 mfh_nbytes_wr;
	char				 mfh_uprog[256];
};

#define MFHF_CLOSING			(1 << 0)	/* close(2) has been issued */
#define MFHF_TRACKING_RA		(1 << 1)	/* tracking for readahead */
#define MFHF_TRACKING_WA		(1 << 2)	/* tracking for writeahead */

#define MFH_LOCK(m)			spinlock(&(m)->mfh_lock)
#define MFH_ULOCK(m)			freelock(&(m)->mfh_lock)
#define MFH_RLOCK(m)			reqlock(&(m)->mfh_lock)
#define MFH_URLOCK(m, lk)		ureqlock(&(m)->mfh_lock, (lk))
#define MFH_LOCK_ENSURE(m)		LOCK_ENSURE(&(m)->mfh_lock)

/*
 * This is attached to each pscfs_req structure.  It is only used for
 * I/O requests (read/write).
 */
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
	int				 mfsrq_niov;
	struct iovec			*mfsrq_iovs;
};

#define MFSRQ_NONE			0
#define MFSRQ_READ			(1 << 0)
#define MFSRQ_AIOWAIT			(1 << 1)
#define MFSRQ_FSREPLIED			(1 << 2)	/* replied to pscfs, as a sanity check */
#define MFSRQ_COPIED			(1 << 3)	/* data has been copied in/out from user to our buffers */

#define mfsrq_2_pfr(q)			((struct pscfs_req *)(q) - 1)

#define DPRINTFS_MFSRQ(level, ss, q, fmt, ...)				\
	psclogs((level), (ss), "mfsrq@%p ref=%d flags=%d len=%zd "	\
	    "error=%d pfr=%p mfh=%p " fmt,				\
	    (q), (q)->mfsrq_ref, (q)->mfsrq_flags, (q)->mfsrq_len,	\
	    (q)->mfsrq_err, mfsrq_2_pfr(q), (q)->mfsrq_mfh, ## __VA_ARGS__)

#define DPRINTF_MFSRQ(level, q, fmt, ...)				\
	DPRINTFS_MFSRQ((level), SLSS_FCMH, (q), fmt, ## __VA_ARGS__)

/*
 * Client-specific private data for sl_resource, shared for both MDS and IOS
 * types.
 */
struct resprof_cli_info {
	struct psc_spinlock		 rpci_lock;
	struct psc_dynarray		 rpci_pinned_bmaps;
	struct statvfs			 rpci_sfb;
	struct timespec			 rpci_sfb_time;
	struct psc_waitq		 rpci_waitq;
	int				 rpci_flags;
};

#define RPCIF_AVOID			(1 << 0)	/* IOS self-advertised degradation */
#define RPCIF_STATFS_FETCHING		(1 << 1)	/* RPC for STATFS in flight */

#define RPCI_LOCK(rpci)			spinlock(&(rpci)->rpci_lock)
#define RPCI_ULOCK(rpci)		freelock(&(rpci)->rpci_lock)
#define RPCI_WAIT(rpci)			psc_waitq_wait(&(rpci)->rpci_waitq, \
					    &(rpci)->rpci_lock)
#define RPCI_WAKE(rpci)			psc_waitq_wakeall(&(rpci)->rpci_waitq)

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

struct readaheadrq {
	struct psc_listentry		rarq_lentry;
	struct sl_fidgen		rarq_fg;
	sl_bmapno_t			rarq_bno;
	uint32_t			rarq_off;
	int				rarq_npages;
};

struct uid_mapping {
	/* these are 64-bit as limitation of hash API */
	uint64_t			um_key;
	uint64_t			um_val;
	struct pfl_hashentry		um_hentry;
};

struct gid_mapping {
	uint64_t			gm_key;
	gid_t				gm_gid;
	int				gm_ngid;
	gid_t				gm_gidv[NGROUPS_MAX];
	struct pfl_hashentry		gm_hentry;
};

#define msl_read(pfr, fh, p, sz, off)	msl_io((pfr), (fh), (p), (sz), (off), SL_READ)
#define msl_write(pfr, fh, p, sz, off)	msl_io((pfr), (fh), (p), (sz), (off), SL_WRITE)

#define msl_biorq_release(r)		_msl_biorq_release(PFL_CALLERINFOSS(SLSS_FCMH), (r))

int	 msl_bmap_to_csvc(struct bmap *, int, struct slashrpc_cservice **);
void	 msl_bmap_reap_init(struct bmap *, const struct srt_bmapdesc *);
void	 msl_bmpces_fail(struct bmpc_ioreq *, int);
void	_msl_biorq_release(const struct pfl_callerinfo *, struct bmpc_ioreq *);

void	 mfh_decref(struct msl_fhent *);
void	 mfh_incref(struct msl_fhent *);

ssize_t	 msl_io(struct pscfs_req *, struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_stat(struct fidc_membh *, void *);

int	 msl_read_cleanup(struct pscrpc_request *, int, struct pscrpc_async_args *);
int	 msl_dio_cleanup(struct pscrpc_request *, int, struct pscrpc_async_args *);

ssize_t	 slc_getxattr(const struct pscfs_clientctx *,
	    const struct pscfs_creds *, const char *, void *, size_t,
	    struct fidc_membh *, size_t *);

size_t	 msl_pages_copyout(struct bmpc_ioreq *, struct msl_fsrqinfo *);
int	 msl_fd_should_retry(struct msl_fhent *, struct pscfs_req *, int);

void	 msl_update_iocounters(struct pfl_iostats_grad *, enum rw, int);

int	 msl_try_get_replica_res(struct bmap *, int, int,
	    struct slashrpc_cservice **);
struct msl_fhent *
	 msl_fhent_new(struct pscfs_req *, struct fidc_membh *);

void	 msbmapthr_spawn(void);
void	 msctlthr_spawn(void);
void	 msreadaheadthr_spawn(void);

void	 slc_getuprog(pid_t, char *, size_t);
void	 slc_setprefios(sl_ios_id_t);
int	 msl_pages_fetch(struct bmpc_ioreq *);

int	 uidmap_ext_cred(struct srt_creds *);
int	 gidmap_int_cred(struct pscfs_creds *);
int	 uidmap_ext_stat(struct srt_stat *);
int	 uidmap_int_stat(struct srt_stat *);
void	 parse_mapfile(void);

#define bmap_flushq_wake(reason)					\
	_bmap_flushq_wake(PFL_CALLERINFOSS(SLSS_BMAP), (reason))

void	 _bmap_flushq_wake(const struct pfl_callerinfo *, int);
void	  bmap_flush_resched(struct bmpc_ioreq *, int);

/* bmap flush modes (bmap_flushq_wake) */
#define BMAPFLSH_RPCWAIT	(1 << 0)
#define BMAPFLSH_EXPIRE		(1 << 1)
#define BMAPFLSH_TIMEOA		(1 << 2)
#define BMAPFLSH_TRUNCATE	(1 << 3)
#define BMAPFLSH_RPCDONE	(1 << 4)
#define BMAPFLSH_REAP		(1 << 5)

enum {
	SLC_FAULT_READAHEAD_CB_EIO,
	SLC_FAULT_READRPC_OFFLINE,
	SLC_FAULT_READ_CB_EIO,
	SLC_FAULT_REQUEST_TIMEOUT
};

extern const char		*ctlsockfn;
extern sl_ios_id_t		 msl_mds;
extern sl_ios_id_t		 msl_pref_ios;
extern struct sl_resm		*slc_rmc_resm;
extern char			 mountpoint[];
extern int			 slc_use_mapfile;

extern struct psc_hashtbl	 slc_uidmap_ext;
extern struct psc_hashtbl	 slc_uidmap_int;
extern struct psc_hashtbl	 slc_gidmap_int;

extern struct pfl_iostats_rw	 slc_dio_iostats;
extern struct pfl_opstat	*slc_rdcache_iostats;

extern struct pfl_iostats_grad	 slc_iosyscall_iostats[];
extern struct pfl_iostats_grad	 slc_iorpc_iostats[];

extern struct psc_listcache	 slc_attrtimeoutq;
extern struct psc_listcache	 slc_bmapflushq;
extern struct psc_listcache	 slc_bmaptimeoutq;
extern struct psc_listcache	 msl_readaheadq;

extern struct psc_poolmgr	*slc_async_req_pool;
extern struct psc_poolmgr	*slc_biorq_pool;
extern struct psc_poolmgr	*slc_mfh_pool;

extern psc_atomic32_t		 slc_direct_io;
extern psc_atomic32_t		 slc_max_nretries;
extern psc_atomic32_t		 slc_max_readahead;
extern psc_atomic32_t		 slc_readahead_pipesz;

extern int			 bmap_max_cache;

#endif /* _MOUNT_SLASH_H_ */
