/* $Id$ */

#include <sys/param.h>

#include "psc_rpc/rpc.h"
#include "psc_types.h"
#include "psc_util/cdefs.h"

#define SLASH_SVR_PID		54321

/* RPC portals. */
#define SR_MDS_REQ_PORTAL	20
#define SR_MDS_REP_PORTAL	21
#define SR_MDS_BULK_PORTAL	22

#define SR_IO_REQ_PORTAL	30
#define SR_IO_REP_PORTAL	31
#define SR_IO_BULK_PORTAL	32

#define SR_BE_REQ_PORTAL	40
#define SR_BE_REP_PORTAL	41

#define SR_MDS_VERSION		1
#define SR_MDS_MAGIC		0xaabbccddeeff0011ULL

#define SR_IO_VERSION		1
#define SR_IO_MAGIC		0xaabbccddeeff0011ULL

#define SR_BE_VERSION		1
#define SR_BE_MAGIC		0xaabbccddeeff0011ULL

#define GENERIC_REPLY(rq, prc)								\
	do {										\
		struct slashrpc_generic_rep *_mp;					\
		int _rc, _size;								\
											\
		_size = sizeof(*(_mp));							\
		if (_size > (rq)->rq_rqbd->rqbd_service->srv_max_reply_size)		\
			psc_fatalx("reply size greater than max");			\
		_rc = psc_pack_reply((rq), 1, &_size, NULL);				\
		if (_rc) {								\
			psc_assert(_rc == -ENOMEM);					\
			psc_errorx("psc_pack_reply failed: %s", strerror(_rc));		\
			return (_rc);							\
		}									\
		(_mp) = psc_msg_buf((rq)->rq_repmsg, 0, _size);				\
		if ((_mp) == NULL) {							\
			psc_errorx("connect repbody is null");				\
			return (-ENOMEM);						\
		}									\
		(_mp)->rc = (prc);							\
		return (0);								\
	} while (0)

#define GET_CUSTOM_REPLY_SZ(rq, mp, sz)							\
	do {										\
		int _rc, _size;								\
											\
		_size = sz;								\
		if (_size > (rq)->rq_rqbd->rqbd_service->srv_max_reply_size)		\
			psc_fatalx("reply size greater than max");			\
		_rc = psc_pack_reply((rq), 1, &_size, NULL);				\
		if (_rc) {								\
			psc_assert(_rc == -ENOMEM);					\
			psc_errorx("psc_pack_reply failed: %s", strerror(_rc));		\
			return (_rc);							\
		}									\
		(mp) = psc_msg_buf((rq)->rq_repmsg, 0, _size);				\
		if ((mp) == NULL) {							\
			psc_errorx("connect repbody is null");				\
			return (-ENOMEM);						\
		}									\
	} while (0)

#define GET_CUSTOM_REPLY(rq, mp) GET_CUSTOM_REPLY_SZ(rq, mp, sizeof(*(mp)))

#define GET_GEN_REQ(rq, mq)								\
	do {										\
		(mq) = psc_msg_buf((rq)->rq_reqmsg, 0, sizeof(*(mq)));			\
		if ((mq) == NULL) {							\
			psc_warnx("reqbody is null");					\
			GENERIC_REPLY((rq), -ENOMSG);					\
		}									\
	} while (0)

/* Slash RPC message types. */
#define SRMT_ACCESS	0
#define SRMT_CHMOD	1
#define SRMT_CHOWN	2
#define SRMT_CONNECT	3
#define SRMT_CREATE	4
#define SRMT_DESTROY	5
#define SRMT_FGETATTR	6
#define SRMT_FTRUNCATE	7
#define SRMT_GETATTR	8
#define SRMT_LINK	9
#define SRMT_LOCK	10
#define SRMT_MKDIR	11
#define SRMT_MKNOD	12
#define SRMT_OPEN	13
#define SRMT_OPENDIR	14
#define SRMT_READDIR	15
#define SRMT_READLINK	16
#define SRMT_RELEASE	17
#define SRMT_RELEASEDIR	18
#define SRMT_RENAME	19
#define SRMT_RMDIR	20
#define SRMT_STATFS	21
#define SRMT_SYMLINK	22
#define SRMT_TRUNCATE	23
#define SRMT_UNLINK	24
#define SRMT_UTIMES	25
#define SRMT_READ	26
#define SRMT_WRITE	27
#define SRMT_GETFID	28
#define SNRT		29

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
