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

extern struct psc_thread *slcconnthr;

/* mount_slash thread types */
enum {
	MSTHRT_ATTR_FLUSH = _PFL_NTHRT,	/* attr write data flush thread */
	MSTHRT_BENCH,			/* I/O benchmarking thread */
	MSTHRT_BRELEASE,		/* bmap lease releaser */
	MSTHRT_BWATCH,			/* bmap lease watcher */
	MSTHRT_CTL,			/* control processor */
	MSTHRT_CTLAC,			/* control acceptor */
	MSTHRT_REAP,			/* pool reap thread */
	MSTHRT_FREAP,			/* fcmh reap thread */
	MSTHRT_FLUSH,			/* bmap write data flush thread */
	MSTHRT_FSMGR,			/* pscfs manager */
	MSTHRT_NBRQ,			/* non-blocking RPC reply handler */
	MSTHRT_CONN,			/* monitor connection to peers */
	MSTHRT_RCI,			/* service RPC reqs for CLI from ION */
	MSTHRT_RCM,			/* service RPC reqs for CLI from MDS */
	MSTHRT_READAHEAD,		/* readahead thread */
	MSTHRT_OPSTIMER,		/* opstats updater */
	MSTHRT_USKLNDPL,		/* userland socket lustre net dev poll thr */
	MSTHRT_WORKER			/* generic worker */
};

struct msfs_thread {
	struct pfl_multiwait		 mft_mw;
};

#define msfsthr(thr)	((struct msfs_thread *)pfl_fsthr_getpri(thr))

struct msattrflush_thread {
	struct pfl_multiwait		 maft_mw;
};

struct msbrelease_thread {
	struct pfl_multiwait		 mbrt_mw;
};

struct msbwatch_thread {
	struct pfl_multiwait		 mbwt_mw;
};

struct msflush_thread {
	int				 mflt_failcnt;
	int				 mflt_credits;
	struct pfl_multiwait		 mflt_mw;
};

struct msrci_thread {
	struct pscrpc_thread		 mrci_prt;
	struct pfl_multiwait		 mrci_mw;
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
	struct pfl_multiwait		 mrcm_mw;
};

struct msreadahead_thread {
	struct pfl_multiwait		 mrat_mw;
};

PSCTHR_MKCAST(msattrflushthr, msattrflush_thread, MSTHRT_ATTR_FLUSH);
PSCTHR_MKCAST(msflushthr, msflush_thread, MSTHRT_FLUSH);
PSCTHR_MKCAST(msbreleasethr, msbrelease_thread, MSTHRT_BRELEASE);
PSCTHR_MKCAST(msbwatchthr, msbwatch_thread, MSTHRT_BWATCH);
PSCTHR_MKCAST(msrcithr, msrci_thread, MSTHRT_RCI);
PSCTHR_MKCAST(msrcmthr, msrcm_thread, MSTHRT_RCM);
PSCTHR_MKCAST(msreadaheadthr, msreadahead_thread, MSTHRT_READAHEAD);

#define NUM_NBRQ_THREADS		16
#define NUM_BMAP_FLUSH_THREADS		16
#define NUM_ATTR_FLUSH_THREADS		4
#define NUM_READAHEAD_THREADS		4

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
	struct psc_listentry		 mfh_lentry;
	int				 mfh_flags;
	int				 mfh_refcnt;
	pid_t				 mfh_pid;
	pid_t				 mfh_sid;
	uid_t				 mfh_accessing_euid;
	uid_t				 mfh_accessing_uid;
	gid_t				 mfh_accessing_gid;

	int				 mfh_oflags;	/* open(2) flags */

	/* offsets are file-wise */
	off_t				 mfh_predio_lastoff;	/* last I/O offset */
	off_t				 mfh_predio_lastsize;	/* last I/O size */
	off_t				 mfh_predio_off;	/* next predio I/O offset */
	int				 mfh_predio_nseq;	/* num sequential IOs */

	/* stats */
	struct timespec			 mfh_open_time;	/* clock_gettime(2) at open(2) time */
	struct pfl_timespec		 mfh_open_atime;/* st_atime at open(2) time */
	off_t				 mfh_nbytes_rd;
	off_t				 mfh_nbytes_wr;
};

#define MFHF_TRACKING_RA		(1 << 0)	/* tracking for readahead */
#define MFHF_TRACKING_WA		(1 << 1)	/* tracking for writeahead */

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
	struct psc_listentry		 mfsrq_lentry;
	struct bmpc_ioreq		*mfsrq_biorq[MAX_BMAPS_REQ];
	struct pscfs_req		*mfsrq_pfr;
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

#define mfsrq_2_pfr(q)			(q)->mfsrq_pfr

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
	struct statvfs			 rpci_sfb;
	struct timespec			 rpci_sfb_time;
	struct psc_waitq		 rpci_waitq;
	int				 rpci_flags;
	int				 rpci_timeouts;
	int				 rpci_saw_error;
	int				 rpci_infl_rpcs;
	int				 rpci_total_rpcs;
	int				 rpci_infl_credits;
	int				 rpci_max_infl_rpcs;
};

#define RPCIF_AVOID			(1 << 0)	/* IOS self-advertised degradation */
#define RPCIF_STATFS_FETCHING		(1 << 1)	/* RPC for STATFS in flight */

#define RPCI_LOCK(rpci)			spinlock(&(rpci)->rpci_lock)
#define RPCI_ULOCK(rpci)		freelock(&(rpci)->rpci_lock)
#define RPCI_WAIT(rpci)			psc_waitq_wait(&(rpci)->rpci_waitq, \
					    &(rpci)->rpci_lock)
#define RPCI_WAITABS(rpci, ts)		psc_waitq_waitabs(&(rpci)->rpci_waitq, \
					    &(rpci)->rpci_lock, &(ts))
#define RPCI_WAKE(rpci)			psc_waitq_wakeall(&(rpci)->rpci_waitq)

static __inline struct resprof_cli_info *
res2rpci(struct sl_resource *res)
{
	return (resprof_get_pri(res));
}

void	slc_init_rpci(struct resprof_cli_info *);
void	slc_destroy_rpci(struct resprof_cli_info *);

/* CLI-specific data for struct sl_resm */
struct resm_cli_info {
	struct srm_bmap_release_req	 rmci_bmaprls;
	struct psc_listcache		 rmci_async_reqs;
};

static __inline struct resm_cli_info *
resm2rmci(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

struct readaheadrq {
	struct psc_listentry		rarq_lentry;
	enum rw				rarq_rw;
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
	uint64_t			gm_val;
	struct pfl_hashentry		gm_hentry;
};

#define msl_read(pfr, fh, p, sz, off)	msl_io((pfr), (fh), (p), (sz), (off), SL_READ)
#define msl_write(pfr, fh, p, sz, off)	msl_io((pfr), (fh), (p), (sz), (off), SL_WRITE)

void	 msl_biorq_release(struct bmpc_ioreq *);

void	 msl_bmap_stash_lease(struct bmap *, const struct srt_bmapdesc *,  const char *);
int	 msl_bmap_to_csvc(struct bmap *, int, struct sl_resm **, struct slrpc_cservice **);
void	 msl_bmap_reap_init(struct bmap *);
void	 msl_bmpces_fail(struct bmpc_ioreq *, int);
void	_msl_biorq_release(const struct pfl_callerinfo *, struct bmpc_ioreq *);

void	 mfh_decref(struct msl_fhent *);
void	 mfh_incref(struct msl_fhent *);

void	 msl_io(struct pscfs_req *, struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_stat(struct fidc_membh *, void *);

int	 msl_read_cleanup(struct pscrpc_request *, int, struct pscrpc_async_args *);
int	 msl_dio_cleanup(struct pscrpc_request *, int, struct pscrpc_async_args *);

int	 slc_getxattr(struct pscfs_req *, const char *, void *, size_t, 
	     struct fidc_membh *, size_t *);
int	 slc_setxattr(struct pscfs_req *, const char *, const void *, size_t, 
	     struct fidc_membh *);
int	 slc_removexattr(struct pscfs_req *, const char *, struct fidc_membh *);


size_t	 msl_pages_copyout(struct bmpc_ioreq *, struct msl_fsrqinfo *);

int	 msl_try_get_replica_res(struct bmap *, int, int,
	    struct sl_resm **, struct slrpc_cservice **);
struct msl_fhent *
	 msl_fhent_new(struct pscfs_req *, struct fidc_membh *);

void	 msl_resm_throttle_wake(struct sl_resm *, int);
void	 msl_resm_throttle_wait(struct sl_resm *);
int	 msl_resm_throttle_yield(struct sl_resm *);

int	 _msl_resm_throttle(struct sl_resm *, int);

void	 msbmapthr_spawn(void);
void	 msctlthr_spawn(void);
void	 msreadaheadthr_spawn(void);
void	 msl_readahead_svc_destroy(void);

void	 slc_setprefios(sl_ios_id_t);
int	 msl_pages_fetch(struct bmpc_ioreq *);

struct pscfs_creds *
	 slc_getfscreds(struct pscfs_req *, struct pscfs_creds *, int);

void	 uidmap_ext_cred(struct pscfs_creds *);
void	 gidmap_ext_cred(struct pscfs_creds *);

void	 uidmap_ext_stat(struct stat *);
void	 gidmap_ext_stat(struct stat *);

void	 uidmap_int_stat(struct srt_stat *, uint32_t *);
void	 gidmap_int_stat(struct srt_stat *, uint32_t *);

void	 parse_mapfile(void);

void	 bmap_flushq_wake(int);
void	 bmap_flush_resched(struct bmpc_ioreq *, int);

/* bmap flush modes (bmap_flushq_wake) */
#define BMAPFLSH_RPCWAIT	(1 << 0)
#define BMAPFLSH_EXPIRE		(1 << 1)
#define BMAPFLSH_TIMEOADD	(1 << 2)

extern const char		*msl_ctlsockfn;
extern sl_ios_id_t		 msl_pref_ios;
extern struct sl_resm		*msl_rmc_resm;
extern char			 mountpoint[];
extern int			 msl_has_mapfile;

extern struct psc_hashtbl	 msl_uidmap_ext;
extern struct psc_hashtbl	 msl_uidmap_int;
extern struct psc_hashtbl	 msl_gidmap_ext;
extern struct psc_hashtbl	 msl_gidmap_int;

extern struct pfl_opstats_grad	 slc_iosyscall_iostats_rd;
extern struct pfl_opstats_grad	 slc_iosyscall_iostats_wr;
extern struct pfl_opstats_grad	 slc_iorpc_iostats_rd;
extern struct pfl_opstats_grad	 slc_iorpc_iostats_wr;

extern struct psc_listcache	 msl_attrtimeoutq;
extern struct psc_listcache	 msl_bmapflushq;
extern struct psc_listcache	 msl_bmaptimeoutq;
extern struct psc_listcache	 msl_readaheadq;

extern struct psc_poolmgr	*msl_iorq_pool;
extern struct psc_poolmgr	*msl_async_req_pool;
extern struct psc_poolmgr	*msl_retry_req_pool;
extern struct psc_poolmgr	*msl_biorq_pool;
extern struct psc_poolmgr	*msl_mfh_pool;

extern int			 msl_acl;
extern int			 msl_enable_namecache;
extern int			 msl_enable_sillyrename;
extern int			 msl_force_dio;
extern int			 msl_map_enable;
extern int			 msl_bmap_reassign;
extern int			 msl_fuse_direct_io;
extern int			 msl_ios_max_inflight_rpcs;
extern int			 msl_mds_max_inflight_rpcs;
extern int			 msl_max_nretries;

extern int			 msl_predio_max_pages;
extern int			 msl_predio_pipe_size;

extern int			 msl_max_retries;
extern int			 msl_root_squash;
extern int			 msl_repl_enable;
extern int			 msl_statfs_pref_ios_only;
extern uint64_t			 msl_pagecache_maxsize;
extern int			 msl_max_namecache_per_directory; 
extern int			 msl_attributes_timeout;

void				 msl_pgcache_init(void);
void				 msl_pgcache_reap(void);

extern struct psc_waitq		 sl_freap_waitq;
int				 bmpce_reaper(struct psc_poolmgr *);

#endif /* _MOUNT_SLASH_H_ */
