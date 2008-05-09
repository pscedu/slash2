/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_types.h"
#include "psc_rpc/rpc.h"

#define MSTHRT_CTL	0
#define MSTHRT_FS	1
#define MSTHRT_LNET	2

struct msctl_thread {
	u32	mc_st_nclients;
	u32	mc_st_nsent;
	u32	mc_st_nrecv;
};

struct slashrpc_service {
	struct pscrpc_import	 *svc_import;
	psc_spinlock_t		  svc_lock;
	struct psclist_head	  svc_old_imports;
	int			  svc_failed;
	int			  svc_initialized;
};

void rpc_svc_init(void);

int slash_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int slash_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int slash_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);

extern struct slashrpc_service *mds_svc;
extern struct slashrpc_service *io_svc;

#define mds_import	(mds_svc->svc_import)
#define io_import	(io_svc->svc_import)

#define msctlthr(thr)	((struct msctl_thread *)(thr)->pscthr_private)
