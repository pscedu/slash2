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

#include "psc_ds/tree.h"
#include "psc_rpc/service.h"

#include "bmap.h"
#include "fidcache.h"
#include "fuse_listener.h"
#include "inode.h"
#include "slashrpc.h"
#include "slconfig.h"

struct pscrpc_request;

/* mount_slash thread types */
#define MSTHRT_CTL			0	/* control processor */
#define MSTHRT_CTLAC			1	/* control acceptor */
#define MSTHRT_FS			2	/* fuse filesystem syscall handlers */
#define MSTHRT_RCM			3	/* service RPC reqs for client from MDS */
#define MSTHRT_LNETAC			4	/* lustre net accept thr */
#define MSTHRT_USKLNDPL			5	/* userland socket lustre net dev poll thr */
#define MSTHRT_EQPOLL			6	/* LNET event queue polling */
#define MSTHRT_TINTV			7	/* timer interval thread */
#define MSTHRT_TIOS			8	/* timer iostat updater */
#define MSTHRT_FUSE			9	/* fuse internal manager */
#define MSTHRT_BMAPFLSH			10	/* bmap write data flush thread */
#define MSTHRT_BMAPFLSHRPC		11	/* async buffer thread for RPC reaping */
#define MSTHRT_BMAPFLSHRLS		12	/* bmap lease releaser */
#define MSTHRT_CONN			13	/* connection monitor */

/* async RPC pointers */
#define MSL_IO_CB_POINTER_SLOT		1
#define MSL_WRITE_CB_POINTER_SLOT	2
#define MSL_OFTRQ_CB_POINTER_SLOT	3

struct msrcm_thread {
	struct pscrpc_thread		 mrcm_prt;
};

struct msfs_thread {
	size_t				 mft_uniqid;
};

struct msl_fhent {			 /* XXX rename */
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
#define msl_write(fh, buf, size, off)	msl_io((fh), (buf), (size), (off), SL_WRITE)

struct slashrpc_cservice *
	 msl_bmap_to_csvc(struct bmapc_memb *, int);
void	 msl_bmap_reap_init(struct bmapc_memb *, const struct srt_bmapdesc *);
int	 msl_dio_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_io(struct msl_fhent *, char *, size_t, off_t, enum rw);
int	 msl_io_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	 msl_io_rpcset_cb(struct pscrpc_request_set *, void *, int);

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

int	 slash2fuse_stat(struct fidc_membh *, const struct slash_creds *);

extern char			 ctlsockfn[];
extern sl_ios_id_t		 prefIOS;
extern struct psc_listcache	 bmapFlushQ;
extern struct sl_resm		*slc_rmc_resm;
extern struct slash_creds	 rootcreds;

extern struct psc_waitq		 msl_fhent_flush_waitq;
extern struct timespec		 msl_bmap_max_lease;
extern struct timespec		 msl_bmap_timeo_inc;

extern struct psc_listcache	 bmapTimeoutQ;

#endif /* _MOUNT_SLASH_H_ */
