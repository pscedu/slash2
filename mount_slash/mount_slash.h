/* $Id$ */

#include <sys/param.h>

#include <stdarg.h>

#include "psc_types.h"

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
#define SRMT_ACCESS	0
#define SRMT_CHMOD	1
#define SRMT_CHOWN	2
#define SRMT_LINK	3
#define SRMT_MKDIR	4
#define SRMT_RENAME	5
#define SRMT_RMDIR	6
#define SRMT_SYMLINK	7
#define SRMT_TRUNCATE	8
#define SRMT_UNLINK	9
#define SRMT_UTIMES	10
#define SRMT_CONNECT	11

struct slashrpc_connect_req {
	u64	magic;
	u32	version;
};

struct slashrpc_connect_rep {
};

struct slashrpc_access_req {
	char path[PATH_MAX];
	int mask;
};

struct slashrpc_chmod_req {
	char path[PATH_MAX];
	mode_t mode;
};

struct slashrpc_chown_req {
	char path[PATH_MAX];
	uid_t uid;
	gid_t gid;
};

struct slashrpc_link_req {
	char from[PATH_MAX];
	char to[PATH_MAX];
};

struct slashrpc_mkdir_req {
	char path[PATH_MAX];
	mode_t mode;
};

struct slashrpc_rename_req {
	char from[PATH_MAX];
	char to[PATH_MAX];
};

struct slashrpc_rmdir_req {
	char path[PATH_MAX];
};

struct slashrpc_symlink_req {
	char from[PATH_MAX];
	char to[PATH_MAX];
};

struct slashrpc_truncate_req {
	char path[PATH_MAX];
	size_t size;
};

struct slashrpc_unlink_req {
	char path[PATH_MAX];
};

struct slashrpc_utimes_req {
	char path[PATH_MAX];
	struct timeval times[2];
};

int rpc_sendmsg(int, ...);
int rpc_svc_init(void);
