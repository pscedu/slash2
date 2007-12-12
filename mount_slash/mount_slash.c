/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26

#include <fuse.h>

#include "psc_util/log.h"
#include "psc_rpc/rpc.h"

#include "mount_slash.h"

int
slash_access(const char *path, int mask)
{
	if (rpc_sendmsg(SRMT_ACCESS, path, mask) == -1)
		return (-errno);
	return (0);
}

int
slash_chmod(const char *path, mode_t mode)
{
	if (rpc_sendmsg(SRMT_CHMOD, path, mode) == -1)
		return (-errno);
	return (0);
}

int
slash_chown(const char *path, uid_t uid, gid_t gid)
{
	if (rpc_sendmsg(SRMT_CHOWN, path, uid, gid) == -1)
		return (-errno);
	return (0);
}

int
slash_getattr(const char *path, struct stat *stb)
{
	struct slashrpc_getattr_req *mq;
	struct slashrpc_getattr_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_GETATTR,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0) {
		memset(stb, 0, sizeof(*stb));
		stb->st_mode = mp->mode;
		stb->st_nlink = mp->nlink;
		stb->st_uid = mp->uid;
		stb->st_gid = mp->gid;
		stb->st_size = mp->size;
		stb->st_atime = mp->atime;
		stb->st_mtime = mp->mtime;
		stb->st_ctime = mp->ctime;
	}
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_link(const char *from, const char *to)
{
	if (rpc_sendmsg(SRMT_LINK, from, to) == -1)
		return (-errno);
	return (0);
}

int
slash_mkdir(const char *path, mode_t mode)
{
	if (rpc_sendmsg(SRMT_MKDIR, path, mode) == -1)
		return (-errno);
	return (0);
}

int
slash_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path, "/hello") != 0)
		return -ENOENT;
	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;
	return (0);
}

int
slash_read(const char *path, char *buf, size_t size, off_t offset,
    __unusedx struct fuse_file_info *fi)
{
	size_t len;

	if (strcmp(path, "/hello") != 0)
		return -ENOENT;

	len = strlen("hi");
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, "hi" + offset, size);
	} else
		size = 0;

	return size;
}

int
slash_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
    __unusedx off_t offset, __unusedx struct fuse_file_info *fi)
{
	struct stat st;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	memset(&st, 0, sizeof(st));
	st.st_ino = 2;
	st.st_mode = 0755;
	filler(buf, ".", &st, 0);

	memset(&st, 0, sizeof(st));
	st.st_ino = 1;
	st.st_mode = 0755;
	filler(buf, "..", &st, 0);

	memset(&st, 0, sizeof(st));
	st.st_ino = 3;
	st.st_mode = 0644;
	filler(buf, "/hello" + 1, &st, 0);

	return 0;
}

int
slash_readlink(const char *path, char *buf, size_t size)
{
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_UTIMES,
	    sizeof(*mq), size, &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	mq->size = size;
	if ((rc = rpc_getrep(rq, size, &mp)) == 0)
		rc = snprintf(buf, size, "%s", mp->buf);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_rename(const char *from, const char *to)
{
	if (rpc_sendmsg(SRMT_RENAME, from, to) == -1)
		return (-errno);
	return (0);
}

int
slash_rmdir(const char *path)
{
	if (rpc_sendmsg(SRMT_RMDIR, path) == -1)
		return (-errno);
	return (0);
}

int
slash_statfs(const char *path, struct statvfs *stbuf)
{
	struct slashrpc_statfs_req *mq;
	struct slashrpc_statfs_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_UTIMES,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0) {
		/* XXX copy statfs */
	}
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_symlink(const char *from, const char *to)
{
	if (rpc_sendmsg(SRMT_SYMLINK, from, to) == -1)
		return (-errno);
	return (0);
}

int
slash_truncate(const char *path, off_t size)
{
	if (rpc_sendmsg(SRMT_TRUNCATE, path, size) == -1)
		return (-errno);
	return (0);
}

int
slash_unlink(const char *path)
{
	if (rpc_sendmsg(SRMT_UNLINK, path) == -1)
		return (-errno);
	return (0);
}

int
slash_utimens(const char *path, const struct timespec ts[2])
{
	if (rpc_sendmsg(SRMT_UTIMES, path, ts) == -1)
		return (-errno);
	return (0);
}

int
slash_write(const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	int fd;
	int rc;

	fd = open(path, O_WRONLY);
	if (fd == -1)
		return -errno;

	rc = pwrite(fd, buf, size, offset);
	if (rc == -1)
		rc = -errno;

	close(fd);
	return rc;
}

struct fuse_operations slashops = {
	.access		= slash_access,
	.chmod		= slash_chmod,
	.chown		= slash_chown,
	.getattr	= slash_getattr,
	.link		= slash_link,
	.mkdir		= slash_mkdir,
	.open		= slash_open,
	.read		= slash_read,
	.readdir	= slash_readdir,
	.readlink	= slash_readlink,
	.rename		= slash_rename,
	.rmdir		= slash_rmdir,
	.statfs		= slash_statfs,
	.symlink	= slash_symlink,
	.truncate	= slash_truncate,
	.unlink		= slash_unlink,
	.utimens	= slash_utimens,
	.write		= slash_write,
};

int
main(int argc, char *argv[])
{
	if (rpc_svc_init())
		psc_fatalx("rpc_init");
	return (fuse_main(argc, argv, &slashops, NULL));
}
