/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_types.h"
#include "psc_rpc/rpc.h"

struct fhent;

#define MSTHRT_CTL	0
#define MSTHRT_FS	1
#define MSTHRT_LNET	2

#define MSL_IO_CB_POINTER_SLOT 1
#define MSL_WRITE_CB_POINTER_SLOT 2

#define MSL_READ 0
#define MSL_WRITE 1

struct msctl_thread {
	u32	mc_st_nclients;
	u32	mc_st_nsent;
	u32	mc_st_nrecv;
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

void msl_fdreg_cb(struct fhent *, int, void *[]);

extern struct slashrpc_cservice *mds_csvc;

#define mds_import	(mds_csvc->csvc_import)

#define msctlthr(thr)	((struct msctl_thread *)(thr)->pscthr_private)

#define msl_read(fh, buf, size, off) msl_io(fh, buf, size, off, MSL_READ)
#define msl_write(fh, buf, size, off) msl_io(fh, buf, size, off, MSL_WRITE)

int
msl_io(struct fhent *fh, char *buf, size_t size, off_t off, int op);

int
msl_io_cb(struct pscrpc_request *rq, void *arg, int status);
