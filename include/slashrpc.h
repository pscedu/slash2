/* $Id$ */

/*
 * Slash RPC subsystem definitions including messages definitions.
 * Note: many of the messages will take on different sizes between
 * 32-bit and 64-bit machine architectures.
 */

#include "psc_types.h"

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

struct slash_creds {
	u32	uid;
	u32	gid;
};

struct srm_connect_req {
	u64	magic;
	u32	version;
};

struct srm_create_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	mode;
};

struct srm_chmod_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	mode;
};

struct srm_chown_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	uid;
	u32	gid;
};

struct srm_destroy_req {
};

struct srm_getattr_req {
	struct slash_creds creds;
	u32	fnlen;
};

struct srm_getattr_rep {
	u64	size;
	u64	atime;
	u64	mtime;
	u64	ctime;
	s32	rc;
	u32	mode;
	u32	nlink;
	u32	uid;
	u32	gid;
};

struct srm_fgetattr_req {
	u64	cfd;
};

#define srm_fgetattr_rep srm_getattr_rep

struct srm_ftruncate_req {
	u64	cfd;
	u64	size;
};

struct srm_link_req {
	struct slash_creds creds;
	u32	fromlen;
	u32	tolen;
};

struct srm_mkdir_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	mode;
};

struct srm_mknod_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	mode;
	u32	dev;
};

struct srm_open_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	flags;
};

struct srm_open_rep {
	u64	cfd;
	s32	rc;
};

struct srm_opendir_req {
	struct slash_creds creds;
	u32	fnlen;
};

struct srm_opendir_rep {
	u64	cfd;
	s32	rc;
};

struct srm_read_req {
	u64	cfd;
	u32	size;
	u32	offset;
};

struct srm_read_rep {
	s32	rc;
	u32	size;
	unsigned char buf[0];
};

struct srm_readdir_req {
	u64	cfd;
	u64	offset;
};

struct srm_readdir_rep {
	s32	rc;
	u32	size;
	/* accompanied by bulk data in pure getdents(2) format */
};

struct srm_readlink_req {
	struct slash_creds creds;
	u32	fnlen;
	u32	size;
};

struct srm_readlink_rep {
	s32	rc;
};

struct srm_release_req {
	u64	cfd;
};

struct srm_releasedir_req {
	u64	cfd;
};

struct srm_rename_req {
	struct slash_creds creds;
	u32	fromlen;
	u32	tolen;
};

struct srm_rmdir_req {
	struct slash_creds creds;
	u32	fnlen;
};

struct srm_statfs_req {
	struct slash_creds creds;
	u32	fnlen;
};

struct srm_statfs_rep {
	u64	f_files;
	u64	f_ffree;
	s32	rc;
	u32	f_bsize;
	u32	f_blocks;
	u32	f_bfree;
	u32	f_bavail;
};

struct srm_symlink_req {
	struct slash_creds creds;
	u32	fromlen;
	u32	tolen;
};

struct srm_truncate_req {
	struct slash_creds creds;
	u32	fnlen;
	u64	size;
};

struct srm_unlink_req {
	struct slash_creds creds;
	u32	fnlen;
};

struct srm_utimes_req {
	struct slash_creds creds;
	u32	fnlen;
	struct timeval times[2];
};

struct srm_write_req {
	u64		cfd;
	u32		size;
	u32		offset;
	unsigned char	buf[0];
};

struct srm_write_rep {
	s32	rc;
	u32	size;
};

struct srm_generic_rep {
	s32	rc;
};

struct srm_getfid_req {
	u64	nid;
	u64	pid;
	u64	cfd;
};

struct srm_getfid_rep {
	u64	fid;
	s32	rc;
};

void slashrpc_export_destroy(void *);
