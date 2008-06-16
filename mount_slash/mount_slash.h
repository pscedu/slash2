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

struct slashrpc_cservice {
	struct pscrpc_import		*csvc_import;
	psc_spinlock_t			 csvc_lock;
	struct psclist_head		 csvc_old_imports;
	int				 csvc_failed;
	int				 csvc_initialized;
};

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

void rpc_svc_init(void);

void *msctlthr_begin(void *);

int slash_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int slash_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int slash_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);

struct slashrpc_cservice *ion_get(void);

extern struct slashrpc_cservice *mds_csvc;

#define mds_import	(mds_csvc->csvc_import)

#define msctlthr(thr)	((struct msctl_thread *)(thr)->pscthr_private)
