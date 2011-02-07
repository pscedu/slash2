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

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>

#include <stdarg.h>

#include "psc_rpc/service.h"

#include "bmap.h"
#include "fidcache.h"
#include "inode.h"
#include "slashrpc.h"
#include "slconfig.h"

struct pscrpc_request;

/* mount_slash thread types */
enum {
	MSTHRT_BMAPFLSH,		/* bmap write data flush thread */
	MSTHRT_BMAPFLSHRLS,		/* bmap lease releaser */
	MSTHRT_BMAPFLSHRPC,		/* async buffer thread for RPC reaping */
	MSTHRT_CONN,			/* connection monitor */
	MSTHRT_CTL,			/* control processor */
	MSTHRT_CTLAC,			/* control acceptor */
	MSTHRT_EQPOLL,			/* LNET event queue polling */
	MSTHRT_FS,			/* file system syscall handler workers */
	MSTHRT_FSMGR,			/* pscfs manager */
	MSTHRT_LNETAC,			/* lustre net accept thr */
	MSTHRT_RCM,			/* service RPC reqs for client from MDS */
	MSTHRT_TIOS,			/* timer iostat updater */
	MSTHRT_USKLNDPL			/* userland socket lustre net dev poll thr */
};

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
};

struct msfs_thread {
	size_t				 mft_uniqid;
};

struct msl_fhent {			 /* XXX rename */
	int				 mfh_oflags;
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct psc_lockedlist		 mfh_biorqs; /* track biorqs (flush) */
};

/*
 * CLIENT-specific private data for struct sl_resm.
 */
struct resm_cli_info {
	psc_spinlock_t			 rmci_lock;
	struct psc_waitq		 rmci_waitq;
	struct srm_bmap_release_req	 rmci_bmaprls;
};

#define resm2rmci(resm)			((struct resm_cli_info *)(resm)->resm_pri)

#define msl_read(fh, buf, size, off)	msl_io((fh), (buf), (size), (off), SL_READ)
#define msl_write(fh, buf, size, off)	msl_io((fh), (char *)(buf), (size), (off), SL_WRITE)

struct slashrpc_cservice *
	 msl_bmap_to_csvc(struct bmapc_memb *, int);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
int	 msl_dio_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_io(struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_io_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_io_rpcset_cb(struct pscrpc_request_set *, void *, int);
int	 msl_stat(struct fidc_membh *);

struct msl_fhent *
	 msl_fhent_new(struct fidc_membh *);

void	 msctlthr_spawn(void);
void	 mstimerthr_spawn(void);
void	 msbmapflushthr_spawn(void);
void	 msctlthr_begin(struct psc_thread *);

int	 checkcreds(const struct srt_stat *, const struct slash_creds *, int);
int	 translate_pathname(const char *, char []);
int	 lookup_pathname_fg(const char *, struct slash_creds *,
	    struct slash_fidgen *, struct srt_stat *);

extern char			 ctlsockfn[];
extern sl_ios_id_t		 prefIOS;
extern struct psc_listcache	 bmapFlushQ;
extern struct sl_resm		*slc_rmc_resm;
extern struct slash_creds	 rootcreds;
extern char			 mountpoint[];

extern struct psc_waitq		 msl_fhent_flush_waitq;
extern struct timespec		 msl_bmap_max_lease;
extern struct timespec		 msl_bmap_timeo_inc;

extern struct psc_listcache	 bmapTimeoutQ;

#endif /* _MOUNT_SLASH_H_ */
