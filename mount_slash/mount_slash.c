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

#include "psc_types.h"
#include "psc_util/log.h"
#include "psc_rpc/rpc.h"

#include "mount_slash.h"
#include "rpc.h"

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
slash_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	struct slashrpc_create_req *mq;
	struct slashrpc_create_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_CREATE,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	mq->mode = mode;
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0)
		fi->fh = mp->cfd;
	pscrpc_req_finished(rq);
	return (rc);
}

void
slash_destroy(__unusedx void *arg)
{
	rpc_sendmsg(SRMT_DESTROY);
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
slash_fgetattr(__unusedx const char *path, struct stat *stb,
    struct fuse_file_info *fi)
{
	struct slashrpc_fgetattr_req *mq;
	struct slashrpc_fgetattr_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_FGETATTR,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
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
slash_ftruncate(__unusedx const char *path, off_t size,
    struct fuse_file_info *fi)
{
	if (rpc_sendmsg(SRMT_FTRUNCATE, fi->fh, size) == -1)
		return (-errno);
	return (0);
}

int
slash_link(const char *from, const char *to)
{
	if (rpc_sendmsg(SRMT_LINK, from, to) == -1)
		return (-errno);
	return (0);
}

int
slash_lock(__unusedx const char *path, struct fuse_file_info *fi,
    int cmd, struct flock *fl)
{
	if (rpc_sendmsg(SRMT_LOCK, fi->fh, cmd, fl) == -1)
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
slash_mknod(const char *path, mode_t mode, dev_t dev)
{
	if (rpc_sendmsg(SRMT_MKNOD, path, mode, dev) == -1)
		return (-errno);
	return (0);
}

int
slash_open(const char *path, struct fuse_file_info *fi)
{
	struct slashrpc_open_req *mq;
	struct slashrpc_open_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_OPEN,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	mq->flags = fi->flags;
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0)
		fi->fh = mp->cfd;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_opendir(const char *path, struct fuse_file_info *fi)
{
	struct slashrpc_opendir_req *mq;
	struct slashrpc_opendir_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_OPENDIR,
	    sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
	if ((rc = rpc_getrep(rq, sizeof(*mp), &mp)) == 0)
		fi->fh = mp->cfd;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_readdir(__unusedx const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_readdir_req *mq;
	struct pscrpc_request *rq;
	struct readdir_cache_ent rce;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_READDIR,
	    sizeof(*mq), 0, &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->offset = offset;

	memset(&rce, 0, sizeof(rce));
	rce.buf = buf;
	rce.filler = filler;
	rce.cfd = fi->fh;
	rce.offset = offset;
	rc_add(&rce, rq->rq_export);
	rc = rpc_getrep(rq, sizeof(*mp), &mp);
	pscrpc_req_finished(rq);
	rc_remove(&rce, rq->rq_export);

	return (rc);
}

int
slash_readlink(const char *path, char *buf, size_t size)
{
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_READLINK,
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
slash_release(__unusedx const char *path, struct fuse_file_info *fi)
{
	if (rpc_sendmsg(SRMT_RELEASE, fi->fh) == -1)
		return (-errno);
	return (0);
}

int
slash_releasedir(__unusedx const char *path, struct fuse_file_info *fi)
{
	if (rpc_sendmsg(SRMT_RELEASEDIR, fi->fh) == -1)
		return (-errno);
	return (0);
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

	if ((rc = rpc_newreq(RPCSVC_MDS, SMDS_VERSION, SRMT_STATFS,
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

void *
slash_init(struct fuse_conn_info *conn)
{
	return (NULL);
}

struct fuse_operations slashops = {
	.access		= slash_access,
	.chmod		= slash_chmod,
	.chown		= slash_chown,
	.create		= slash_create,
	.destroy	= slash_destroy,
	.fgetattr	= slash_fgetattr,
	.ftruncate	= slash_ftruncate,
	.getattr	= slash_getattr,
	.init		= slash_init,
	.link		= slash_link,
	.lock		= slash_lock,
	.mkdir		= slash_mkdir,
	.mknod		= slash_mknod,
	.open		= slash_open,
	.opendir	= slash_opendir,
	.read		= slash_read,
	.readdir	= slash_readdir,
	.readlink	= slash_readlink,
	.release	= slash_release,
	.releasedir	= slash_releasedir,
	.rename		= slash_rename,
	.rmdir		= slash_rmdir,
	.statfs		= slash_statfs,
	.symlink	= slash_symlink,
	.truncate	= slash_truncate,
	.unlink		= slash_unlink,
	.utimens	= slash_utimens,
	.write		= slash_write
};

int
main(int argc, char *argv[])
{
	if (rpc_svc_init())
		psc_fatalx("rpc_init");
	return (fuse_main(argc, argv, &slashops, NULL));
}
