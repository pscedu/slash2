/* $Id$ */

#include <sys/param.h>

#include "psc_rpc/rpc.h"
#include "psc_types.h"
#include "psc_util/cdefs.h"

#define SLASH_SVR_PID		54321

/* Slash RPC MDS defines. */
#define SRM_REQ_PORTAL		20
#define SRM_REP_PORTAL		21
#define SRM_BULK_PORTAL		22

#define SRM_VERSION		1
#define SRM_MAGIC		0xaabbccddeeff0011ULL

/* Slash RPC I/O defines. */
#define SRI_REQ_PORTAL		30
#define SRI_REP_PORTAL		31
#define SRI_BULK_PORTAL		32

#define SRI_VERSION		1
#define SRI_MAGIC		0xaabbccddeeff0011ULL

/* Slash RPC backend defines. */
#define SRB_REQ_PORTAL		40
#define SRB_REP_PORTAL		41

#define SRB_VERSION		1
#define SRB_MAGIC		0xaabbccddeeff0011ULL

/* Slash RPC message types. */
enum {
	SRMT_ACCESS,
	SRMT_CHMOD,
	SRMT_CHOWN,
	SRMT_CONNECT,
	SRMT_CREATE,
	SRMT_DESTROY,
	SRMT_FGETATTR,
	SRMT_FTRUNCATE,
	SRMT_GETATTR,
	SRMT_LINK,
	SRMT_LOCK,
	SRMT_MKDIR,
	SRMT_MKNOD,
	SRMT_OPEN,
	SRMT_OPENDIR,
	SRMT_READDIR,
	SRMT_READLINK,
	SRMT_RELEASE,
	SRMT_RELEASEDIR,
	SRMT_RENAME,
	SRMT_RMDIR,
	SRMT_STATFS,
	SRMT_SYMLINK,
	SRMT_TRUNCATE,
	SRMT_UNLINK,
	SRMT_UTIMES,
	SRMT_READ,
	SRMT_WRITE,
	SRMT_GETFID,
	SNRT
};

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
	s32	rc;
	u32	_pad;
	u64	cfd;
};

struct slashrpc_destroy_req {
};

struct slashrpc_getattr_req {
	char path[PATH_MAX];
};

struct slashrpc_getattr_rep {
	s32	rc;
	u32	mode;
	u32	nlink;
	u32	uid;
	u32	gid;
	u32	_pad;
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
	u32	dev;
};

struct slashrpc_open_req {
	char	path[PATH_MAX];
	u32	flags;
};

struct slashrpc_open_rep {
	s32	rc;
	u32	_pad;
	u64	cfd;
};

struct slashrpc_opendir_req {
	char	path[PATH_MAX];
};

struct slashrpc_opendir_rep {
	s32	rc;
	u32	_pad;
	u64	cfd;
};

struct slashrpc_read_req {
	u64	cfd;
	u32	size;
	u32	offset;
};

struct slashrpc_read_rep {
	s32	rc;
	u32	size;
	unsigned char buf[0];
};

struct slashrpc_readdir_req {
	u64	cfd;
	u64	offset;
};

struct slashrpc_readdir_rep {
	s32	rc;
	u32	size;
	/* accompanied by bulk data in pure getdents(2) format */
};

struct slashrpc_readlink_req {
	char	path[PATH_MAX];
	u32	size;
};

struct slashrpc_readlink_rep {
	s32	rc;
	u32	_pad;
	char	buf[0];			/* determined by request size */
};

struct slashrpc_release_req {
	u64	cfd;
};

struct slashrpc_releasedir_req {
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
	s32	rc;
	u32	f_bsize;
	u32	f_blocks;
	u32	f_bfree;
	u32	f_bavail;
	u32	_pad;
	u64	f_files;
	u64	f_ffree;
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
	s32	rc;
	u32	size;
};

struct slashrpc_generic_rep {
	s32	rc;
};

struct slashrpc_getfid_req {
	u64	nid;
	u64	pid;
	u64	cfd;
};

struct slashrpc_getfid_rep {
	s32	rc;
	u32	_pad;
	u64	fid;
};

void slashrpc_export_destroy(void *);
