/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_rpc/service.h"
#include "psc_util/multiwait.h"

#include "bmap.h"
#include "fidcache.h"
#include "slashrpc.h"
#include "slconfig.h"

struct pscfs_req;
struct pscrpc_request;

struct bmap_pagecache_entry;

/* mount_slash thread types */
enum {
	MSTHRT_BMAPFLSH,		/* bmap write data flush thread */
	MSTHRT_BMAPFLSHRLS,		/* bmap lease releaser */
	MSTHRT_BMAPFLSHRPC,		/* async buffer thread for RPC reaping */
	MSTHRT_BMAPREADAHEAD,		/* async thread for read ahead */
	MSTHRT_CONN,			/* connection monitor */
	MSTHRT_CTL,			/* control processor */
	MSTHRT_CTLAC,			/* control acceptor */
	MSTHRT_EQPOLL,			/* LNET event queue polling */
	MSTHRT_FS,			/* file system syscall handler workers */
	MSTHRT_FSMGR,			/* pscfs manager */
	MSTHRT_LNETAC,			/* lustre net accept thr */
	MSTHRT_NBRQ,			/* non-blocking RPC reply handler */
	MSTHRT_RCM,			/* service RPC reqs for client from MDS */
	MSTHRT_TIOS,			/* timer iostat updater */
	MSTHRT_USKLNDPL			/* userland socket lustre net dev poll thr */
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
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

struct msbmflra_thread {
	int				 mbfra_failcnt;
	struct psc_multiwait		 mbfra_mw;
};

PSCTHR_MKCAST(msbmflrlsthr, msbmflrls_thread, MSTHRT_BMAPFLSHRLS);
PSCTHR_MKCAST(msbmflthr, msbmfl_thread, MSTHRT_BMAPFLSH);
PSCTHR_MKCAST(msbmfrathr, msbmflra_thread, MSTHRT_BMAPREADAHEAD);
PSCTHR_MKCAST(msfsthr, msfs_thread, MSTHRT_FS);

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

struct msl_fhent {			 /* XXX rename */
	int				 mfh_oflags;	/* open(2) flags */
	int				 mfh_flush_rc;	/* fsync(2) status */
	int                              mfh_flags;
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct psc_lockedlist		 mfh_biorqs;	/* track biorqs (flush) */
	struct psc_lockedlist            mfh_ra_bmpces; /* read ahead bmpce's */
	struct psclist_head              mfh_lentry;
	struct msl_ra			 mfh_ra;
};

#define MSL_FHENT_RASCHED (1 << 0)
#define MSL_FHENT_CLOSING (1 << 1)

struct resprof_cli_info {
	struct psc_dynarray		 rpci_pinned_bmaps;
};

static __inline struct resprof_cli_info *
res2rpci(struct sl_resource *res)
{
	return (resprof_get_pri(res));
}

/* CLI-specific data for struct sl_resm */
struct resm_cli_info {
	struct pfl_mutex		 rmci_mutex;
	struct psc_multiwaitcond	 rmci_mwc;
	struct srm_bmap_release_req	 rmci_bmaprls;
};

static __inline struct resm_cli_info *
resm2rmci(struct sl_resm *resm)
{
	return (resm_get_pri(resm));
}

#define msl_read(fh, buf, size, off)	msl_io((fh), (buf), (size), (off), SL_READ)
#define msl_write(fh, buf, size, off)	msl_io((fh), (char *)(buf), (size), (off), SL_WRITE)

void	 msl_bmpce_getbuf(struct bmap_pagecache_entry *);
void     msl_reada_rpc_launch(struct bmap_pagecache_entry **, int);

struct slashrpc_cservice *
	 msl_bmap_to_csvc(struct bmapc_memb *, int);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
int	 msl_dio_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_io(struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_write_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_write_rpcset_cb(struct pscrpc_request_set *, void *, int);
int	 msl_stat(struct fidc_membh *, void *);
int	 msl_getrqstatus(struct slashrpc_cservice *csvc, struct pscrpc_request *);

struct msl_fhent * msl_fhent_new(struct fidc_membh *);

int	 msl_readahead_cb(struct pscrpc_request *, struct pscrpc_async_args *);

void	 msctlthr_spawn(void);
void	 mstimerthr_spawn(void);
void	 msbmapflushthr_spawn(void);
void	 msctlthr_begin(struct psc_thread *);

int	 translate_pathname(const char *, char []);
int	 lookup_pathname_fg(struct pscfs_req *, const char *,
	    struct slash_creds *, struct slash_fidgen *, struct srt_stat *);

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

extern struct psc_listcache	 bmapTimeoutQ;
extern struct psc_waitq		 bmapflushwaitq;

extern struct psc_listcache	 bmapReadAheadQ;
extern struct pscrpc_nbreqset	*ra_nbreqset;
extern struct pscrpc_nbreqset	*pndgBmaplsReqs;

#endif /* _MOUNT_SLASH_H_ */
