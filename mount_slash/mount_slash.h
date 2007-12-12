/* $Id$ */

#include <sys/param.h>

#include <stdarg.h>

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

/* Slash RPC message types. */
#define SRMT_CONNECT	0
#define SRMT_ACCESS	1
#define SRMT_CHMOD	2
#define SRMT_CHOWN	3
#define SRMT_GETATTR	4
#define SRMT_LINK	5
#define SRMT_MKDIR	6
#define SRMT_RENAME	7
#define SRMT_RMDIR	8
#define SRMT_SYMLINK	9
#define SRMT_TRUNCATE	10
#define SRMT_UNLINK	11
#define SRMT_UTIMES	12

struct slashrpc_connect_req {
	u64	magic;
	u32	version;
};

struct slashrpc_connect_rep {
};

struct slashrpc_access_req {
	char	path[PATH_MAX];
	u32	mask;
};

struct slashrpc_chmod_req {
	char	path[PATH_MAX];
	u32	mode;
};

struct slashrpc_chown_req {
	char	path[PATH_MAX];
	u32	uid;
	u32	gid;
};

struct slashrpc_getattr_req {
	char path[PATH_MAX];
};

struct slashrpc_getattr_rep {
	u32	mode;
	u32	nlink;
	u32	uid;
	u32	gid;
	u64	size;
	u64	atime;
	u64	mtime;
	u64	ctime;
};

struct slashrpc_link_req {
	char	from[PATH_MAX];
	char	to[PATH_MAX];
};

struct slashrpc_mkdir_req {
	char	path[PATH_MAX];
	u32	mode;
};

struct slashrpc_readlink_req {
	char	path[PATH_MAX];
	u32	size;
};

struct slashrpc_readlink_rep {
	char	buf[0];			/* determined by request size */
};

struct slashrpc_rename_req {
	char	from[PATH_MAX];
	char	to[PATH_MAX];
};

struct slashrpc_rmdir_req {
	char	path[PATH_MAX];
};

struct slashrpc_statfs_req {
	char	path[PATH_MAX];
};

struct slashrpc_statfs_rep {
	u32	f_type;
	u32	f_bsize;
	u32	f_blocks;
	u32	f_bfree;
	u32	f_bavail;
	u64	f_files;
	u64	f_ffree;
	u32	f_namelen;
};

struct slashrpc_symlink_req {
	char	from[PATH_MAX];
	char	to[PATH_MAX];
};

struct slashrpc_truncate_req {
	char	path[PATH_MAX];
	u64	size;
};

struct slashrpc_unlink_req {
	char	path[PATH_MAX];
};

struct slashrpc_utimes_req {
	char	path[PATH_MAX];
	struct timeval times[2];
};

int rpc_svc_init(void);
int rpc_newreq(int, int, int, int, int, struct pscrpc_request **, void *);
int rpc_getrep(struct pscrpc_request *, int, void *);
int rpc_sendmsg(int, ...);
