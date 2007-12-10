/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

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

/* Slash RPC message types. */
#define SRM_ACCESS	0
#define SRM_CHMOD	1
#define SRM_CHOWN	2
#define SRM_LINK	3
#define SRM_MKDIR	4
#define SRM_RENAME	5
#define SRM_RMDIR	6
#define SRM_SYMLINK	7
#define SRM_TRUNCATE	8
#define SRM_UNLINK	9

struct slashrpc_access_req {
	const char *path;
	int mask;
};

struct slashrpc_chmod_req {
	const char *path;
	mode_t mode;
};

struct slashrpc_chown_req {
	const char *path;
	uid_t uid;
	gid_t gid;
};

struct slashrpc_link_req {
	const char *from;
	const char *to;
};

struct slashrpc_mkdir_req {
	const char *path;
	mode_t mode;
};

struct slashrpc_rename_req {
	const char *from;
	const char *to;
};

struct slashrpc_rmdir_req {
	const char *path;
};

struct slashrpc_symlink_req {
	const char *from;
	const char *to;
};

struct slashrpc_truncate_req {
	const char *path;
	size_t size;
};

struct slashrpc_unlink_req {
	const char *path;
};

int rpc_sendmsg(int, ...);
int rpc_svc_init(void);
