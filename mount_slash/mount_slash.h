/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_rpc/rpc.h"
#include "psc_util/waitq.h"

struct pscrpc_request;
struct pscrpc_export;

/* RPC services. */
#define RPCSVC_MDS		0
#define RPCSVC_IO		1
#define NRPCSVC			2

struct slashrpc_service {
	struct pscrpc_import	 *svc_import;
	psc_spinlock_t		  svc_lock;
	struct psclist_head	  svc_old_imports;
	int			  svc_failed;
	int			  svc_initialized;
};

struct slashrpc_export {
};

int rpc_svc_init(void);
int rpc_newreq(int, int, int, int, int, struct pscrpc_request **, void *);
int rpc_getrep(struct pscrpc_request *, int, void *);
int rpc_sendmsg(int, ...);
int rpc_connect(lnet_nid_t, int, u64, u32);

int slash_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int slash_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int slash_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);

#define slashrpc_mds_connect(svr) rpc_connect(svr, RPCSVC_MDS, SMDS_MAGIC, SMDS_VERSION)
#define slashrpc_io_connect(svr)  rpc_connect(svr, RPCSVC_IO, SIO_MAGIC, SIO_VERSION)
