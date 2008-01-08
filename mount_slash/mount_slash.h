/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_types.h"

struct pscrpc_request;

/* RPC services. */
#define RPCSVC_MDS		0
#define RPCSVC_IO		1
#define NRPCSVC			2

/* RPC portals. */
#define RPCMDS_REQ_PORTAL	20
#define RPCMDS_REP_PORTAL	21
#define RPCIO_REQ_PORTAL	22
#define RPCIO_REP_PORTAL	23

#define SMDS_VERSION		1
#define SMDS_CONNECT_MAGIC	0xaabbccddeeff0011ULL

#define SIO_VERSION		1
#define SIO_CONNECT_MAGIC	0xaabbccddeeff0011ULL

struct slashrpc_export {
	uid_t	uid;
	gid_t	gid;
};

int rpc_svc_init(void);
int rpc_newreq(int, int, int, int, int, struct pscrpc_request **, void *);
int rpc_getrep(struct pscrpc_request *, int, void *);
int rpc_sendmsg(int, ...);

int slash_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int slash_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int slash_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);
