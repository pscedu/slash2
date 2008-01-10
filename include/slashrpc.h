/* $Id$ */

#include <sys/param.h>

#include "psc_rpc/rpc.h"
#include "psc_types.h"
#include "psc_util/cdefs.h"

/* Asynchronous I/O operations. */
#define SLASH_IOP_READDIR	0

/* Slash RPC message types. */
#define SRMT_ACCESS	0
#define SRMT_CHMOD	1
#define SRMT_CHOWN	2
#define SRMT_CONNECT	3
#define SRMT_CREATE	4
#define SRMT_DESTROY	5
#define SRMT_GETATTR	6
#define SRMT_FGETATTR	7
#define SRMT_FTRUNCATE	8
#define SRMT_LINK	9
#define SRMT_LOCK	10
#define SRMT_MKDIR	11
#define SRMT_MKNOD	12
#define SRMT_OPEN	13
#define SRMT_OPENDIR	14
#define SRMT_READ	15
#define SRMT_READDIR	16
#define SRMT_READLINK	17
#define SRMT_RELEASE	18
#define SRMT_RELEASEDIR	19
#define SRMT_RENAME	20
#define SRMT_RMDIR	21
#define SRMT_STATFS	22
#define SRMT_SYMLINK	23
#define SRMT_TRUNCATE	24
#define SRMT_UNLINK	25
#define SRMT_UTIMES	26
#define SRMT_WRITE	27

struct slashrpc_connect_req {
	u64	magic;
	u32	version;
	u32	uid;
	u32	gid;
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

struct slashrpc_create_req {
	char	path[PATH_MAX];
	u32	mode;
};

struct slashrpc_create_rep {
	u64	cfd;
};

struct slashrpc_destroy_req {
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

struct slashrpc_fgetattr_req {
	u64	cfd;
};

#define slashrpc_fgetattr_rep slashrpc_getattr_rep

struct slashrpc_ftruncate_req {
	u64	cfd;
	u64	size;
};

struct slashrpc_link_req {
	char	from[PATH_MAX];
	char	to[PATH_MAX];
};

struct slashrpc_mkdir_req {
	char	path[PATH_MAX];
	u32	mode;
};

struct slashrpc_mknod_req {
	char	path[PATH_MAX];
	u32	mode;
	u32	dev_t;
};

struct slashrpc_open_req {
	char	path[PATH_MAX];
	u32	flags;
};

struct slashrpc_open_rep {
	u64	cfd;
};

struct slashrpc_opendir_req {
	char	path[PATH_MAX];
};

struct slashrpc_opendir_rep {
	u64	cfd;
};

struct slashrpc_read_req {
	u64	cfd;
	u32	size;
	u32	offset;
};

struct slashrpc_read_rep {
	u32	size;
	unsigned char buf[0];
};

struct slashrpc_readdir_req {
	u64	cfd;
	u64	offset;
};

struct slashrpc_readdir_rep {
};

struct slashrpc_readdir_res_req {
	u64	cfd;
	u64	offset;
	u32	nents;
	u32	flags;
};

/* Slash RPC READDIR operation flags. */
#define SRORF_END	(1<<0)

/* A directory entry in the bulk readdir data. */
struct slashrpc_readdir_ent {
	char	name[NAME_MAX + 1];
	u32	ino;
	u32	mode;
};

/* Reply bulk data. */
struct slashrpc_readdir_bulk {
	struct slashrpc_readdir_ent ents[0];
};

/* Acknowledgment of reception of bulk data. */
struct slashrpc_readdir_res_rep {
	u32	flags;
};

/* Slash RPC READDIR operation bulk transfer flags. */
#define SRORBF_STOP	(1<<0)

struct slashrpc_readlink_req {
	char	path[PATH_MAX];
	u32	size;
};

struct slashrpc_readlink_rep {
	char	buf[0];			/* determined by request size */
};

struct slashrpc_release_req {
	u64	cfd;
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

struct slashrpc_write_req {
	u64		cfd;
	u32		size;
	u32		offset;
	unsigned char	buf[0];
};

struct slashrpc_write_rep {
	u32	size;
};

struct slashrpc_generic_rep {
	u32 rc;
};

/*
 * slashrpc_export_get - access our application-specific variables associated
 *	with an LNET connection.
 * @exp: RPC export of peer.
 */
static __inline struct slashrpc_export *
slashrpc_export_get(struct pscrpc_export *exp)
{
	spinlock(&exp->exp_lock);
	if (exp->exp_private == NULL)
		exp->exp_private = PSCALLOC(sizeof(struct slashrpc_export));
	freelock(&exp->exp_lock);
	return (exp->exp_private);
}
