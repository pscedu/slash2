/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26

#include <fuse.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/log.h"

#include "mount_slash.h"
#include "slashrpc.h"

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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_CREATE, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	mq->mode = mode;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			fi->fh = mp->cfd;
	}
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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_GETATTR, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			memset(stb, 0, sizeof(*stb));
			stb->st_mode = mp->mode;
			stb->st_nlink = mp->nlink;
			stb->st_uid = mp->uid;
			stb->st_gid = mp->gid;
			stb->st_size = mp->size; /* XXX */
			stb->st_atime = mp->atime;
			stb->st_mtime = mp->mtime;
			stb->st_ctime = mp->ctime;
		}
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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_FGETATTR, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_OPEN, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	mq->flags = fi->flags;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			fi->fh = mp->cfd;
	}
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

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_OPENDIR, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			fi->fh = mp->cfd;
	}
	pscrpc_req_finished(rq);
	return (rc);
}

#define OBD_TIMEOUT 15

int
slashrpc_timeout(__unusedx void *arg)
{
	return (0);
}

int
slash_readdir(__unusedx const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
	struct slashrpc_readdir_req *mq;
	struct slashrpc_readdir_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct dirent *d;
	struct iovec iov;
	struct stat stb;
	char *mb;
	u64 off;
	int rc;

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_READDIR, sizeof(*mq), 0, &rq, &mq)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->offset = offset;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) != 0 || (rc = mp->rc)) {
		pscrpc_req_finished(rq);
		return (rc);
	}
	mb = PSCALLOC(mp->size);
	iov.iov_base = mb;
	iov.iov_len = mp->size;
	rc = rsx_bulkgetsink(rq, &desc, SRM_BULK_PORTAL, &iov, 1);
	if (rc == 0) {
		/* Pull down readdir contents slashd posted. */
		for (off = 0; off < mp->size; off = d->d_off - offset) {
			d = (void *)(off + mb);
			memset(&stb, 0, sizeof(stb));
			stb.st_ino = d->d_ino;
			if (filler(buf, d->d_name, &stb, 1))
				break;
		}
	}
	free(mb);
	if (desc)
		pscrpc_free_bulk(desc);
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_readlink(const char *path, char *buf, size_t size)
{
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_READLINK, sizeof(*mq), size, &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	mq->size = size;
	if ((rc = rsx_waitrep(rq, size, &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else
			if (snprintf(buf, size, "%s", mp->buf) == -1)
				rc = -errno;
	}
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
slash_statfs(const char *path, struct statvfs *sfb)
{
	struct slashrpc_statfs_req *mq;
	struct slashrpc_statfs_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	if ((rc = rsx_newreq(rpcsvcs[RPCSVC_MDS]->svc_import, SRM_VERSION,
	    SRMT_STATFS, sizeof(*mq), sizeof(*mp), &rq, &mq)) != 0)
		return (rc);
	snprintf(mq->path, sizeof(mq->path), "%s", path);
//	if (rc == -1)
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			sfb->f_bsize = mp->f_bsize;
			sfb->f_blocks = mp->f_blocks;
			sfb->f_bfree = mp->f_bfree;
			sfb->f_bavail = mp->f_bavail;
			sfb->f_files = mp->f_files;
			sfb->f_ffree = mp->f_ffree;
		}
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
slash_init(__unusedx struct fuse_conn_info *conn)
{
	rpc_svc_init();
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

void
spawn_lnet_thr(pthread_t *t, void *(*startf)(void *), void *arg)
{
	int rc;

	rc = pthread_create(t, NULL, startf, arg);
	if (rc)
		psc_fatalx("pthread_create: %s", strerror(rc));
}

int
main(int argc, char *argv[])
{
	if (getenv("LNET_NETWORKS") == NULL)
		errx(1, "please export LNET_NETWORKS");
	if (getenv("SLASH_SERVER_NID") == NULL)
		errx(1, "please export SLASH_SERVER_NID");
	pfl_init(7);
	lnet_thrspawnf = spawn_lnet_thr;
	return (fuse_main(argc, argv, &slashops, NULL));
}
