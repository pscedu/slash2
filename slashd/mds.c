/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#include <unistd.h>
#include <errno.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"

#include "cfd.h"
#include "dircache.h"
#include "fid.h"
#include "rpc.h"
#include "slashd.h"
#include "slashrpc.h"

#define SRM_NTHREADS	8
#define SRM_NBUFS	1024
#define SRM_BUFSZ	(4096 + 256)
#define SRM_REPSZ	128
#define SRM_SVCNAME	"slrpcmdsthr"

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

int
slmds_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRM_MAGIC || mq->version != SRM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slmds_chmod(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_chmod_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (chmod(fn, mq->mode) == -1)
		mp->rc = -errno;
	return (0);
}

#if 0
int
slmds_fchmod(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_fchmod_req *mq;
	char fn[PATH_MAX];
	struct slash_fid fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd))
		mp->rc = -errno;
	else {
		fid_makepath(&fid, fn);
		if (chmod(fn, mq->mode) == -1)
			mp->rc = -errno;
	}
	return (0);
}
#endif

int
slmds_chown(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_chown_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (chown(fn, mq->uid, mq->gid) == -1)
		mp->rc = -errno;
	return (0);
}

#if 0
int
slmds_fchown(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_fchown_req *mq;
	struct slash_fid fid;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd))
		mp->rc = -errno;
	else {
		fid_makepath(&fid, fn);
		if (chown(fn, mq->uid, mq->gid) == -1)
			mp->rc = -errno;
	}
	return (0);
}
#endif

int
slmds_create(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_create_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];
	int fd;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 0) == -1)
		mp->rc = -errno;
	else if ((fd = creat(fn, mq->mode)) == -1)
		mp->rc = -errno;
	else
		close(fd);
	return (0);
}

int
slmds_getattr(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct iovec iov;
	struct stat stb;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1) {
		mp->rc = -errno;
		return (0);
	}
	if (stat(fn, &stb) == -1) {
		mp->rc = -errno;
		return (0);
	}
	mp->mode = stb.st_mode;
	mp->nlink = stb.st_nlink;
	mp->uid = stb.st_uid;
	mp->gid = stb.st_gid;
	mp->size = stb.st_size;	/* XXX */
	mp->atime = stb.st_atime;
	mp->mtime = stb.st_mtime;
	mp->ctime = stb.st_ctime;
	return (0);
}

int
slmds_fgetattr(struct pscrpc_request *rq)
{
	struct srm_fgetattr_req *mq;
	struct srm_fgetattr_rep *mp;
	struct slash_fid fid;
	struct stat stb;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd)) {
		mp->rc = -errno;
		return (0);
	}
	fid_makepath(&fid, fn);
	if (stat(fn, &stb) == -1) {
		mp->rc = -errno;
		return (0);
	}
	mp->mode = stb.st_mode;
	mp->nlink = stb.st_nlink;
	mp->uid = stb.st_uid;
	mp->gid = stb.st_gid;
	mp->size = stb.st_size;	/* XXX */
	mp->atime = stb.st_atime;
	mp->mtime = stb.st_mtime;
	mp->ctime = stb.st_ctime;
	return (0);
}

int
slmds_ftruncate(struct pscrpc_request *rq)
{
	struct srm_ftruncate_req *mq;
	struct srm_generic_rep *mp;
	struct slash_fid fid;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd))
		mp->rc = -errno;
	else {
		fid_makepath(&fid, fn);
		if (truncate(fn, mq->size) == -1)
			mp->rc = -errno;
	}
	return (0);
}

int
slmds_link(struct pscrpc_request *rq)
{
	char from[PATH_MAX], to[PATH_MAX];
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_link_req *mq;
	struct iovec iov[2];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen == 0 || mq->fromlen >= PATH_MAX ||
	    mq->tolen == 0 || mq->tolen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, iov, 2)) != 0)
		return (0);
	from[mq->fromlen] = '\0';
	to[mq->tolen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(to, 0) == -1)
		mp->rc = -errno;
	else if (link(from, to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_mkdir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_mkdir_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 0) == -1)
		mp->rc = -errno;
	else if (mkdir(fn, mq->mode) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_mknod(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_mknod_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 0) == -1)
		mp->rc = -errno;
	else if (mknod(fn, mq->mode, mq->dev) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_open(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_open_req *mq;
	struct srm_open_rep *mp;
	struct slash_fid fid;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, fn, 1) == -1)
		mp->rc = -errno;
	else
		cfdnew(&mp->cfd, rq->rq_export, &fid);
	/* XXX check access permissions */
	return (0);
}

int
slmds_opendir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_opendir_req *mq;
	struct srm_opendir_rep *mp;
	struct slash_fid fid;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, fn, 1) == -1)
		mp->rc = -errno;
	else
		cfdnew(&mp->cfd, rq->rq_export, &fid);
	/* XXX check access permissions */
	return (0);
}

#define READDIR_BUFSZ (512 * 1024)

int
slmds_readdir(struct pscrpc_request *rq)
{
	struct dirent ents[READDIR_BUFSZ];
	struct pscrpc_bulk_desc *desc;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct slash_fid fid;
	struct dircache *dc;
	struct iovec iov;
	int rc;

	RSX_ALLOCREP(rq, rq, mp);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd)) {
		mp->rc = -errno;
		return (0);
	}
	dc = dircache_get(&fid);
	if (dc == NULL) {
		mp->rc = -errno;
		return (0);
	}
	rc = dircache_read(dc, mq->offset, ents, READDIR_BUFSZ);
	dircache_rel(dc);
	if (rc == -1) {
		mp->rc = -errno;
		return (0);
	}
	if (rc == 0) {
		mp->size = 0;
		return (0);
	}
	mp->size = rc;
	iov.iov_base = ents;
	iov.iov_len = mp->size;
	mp->rc = rsx_bulkgetsource(rq, &desc, SRM_BULK_PORTAL, &iov, 1);
// rc / pscPageSize
	if (desc)
		pscrpc_free_bulk(desc);
	return (0);
}

int
slmds_readlink(struct pscrpc_request *rq)
{
	char fn[PATH_MAX], rfn[PATH_MAX];
	struct pscrpc_bulk_desc *desc;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct iovec iov;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->size >= PATH_MAX || mq->size == 0)
		return (-EINVAL);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (readlink(fn, rfn, mq->size) == -1)
		mp->rc = -errno;
	else if (untranslate_pathname(rfn) == -1)
		mp->rc = -errno;
	else {
		iov.iov_base = rfn;
		iov.iov_len = mq->size;
		mp->rc = rsx_bulkgetsource(rq, &desc, SRM_BULK_PORTAL,
		    &iov, 1);
		if (desc)
			pscrpc_free_bulk(desc);
	}
	return (0);
}

int
slmds_release(struct pscrpc_request *rq)
{
	struct srm_release_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_releasedir(struct pscrpc_request *rq)
{
	struct srm_releasedir_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_rename(struct pscrpc_request *rq)
{
	char from[PATH_MAX], to[PATH_MAX];
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct iovec iov[2];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen == 0 || mq->fromlen >= PATH_MAX ||
	    mq->tolen == 0 || mq->tolen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, iov, 2)) != 0)
		return (0);
	from[mq->fromlen] = '\0';
	to[mq->tolen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(to, 0) == -1)
		mp->rc = -errno;
	else if (rename(from, to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_rmdir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_rmdir_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (rmdir(fn) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_statfs(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct statfs sfb;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (statfs(fn, &sfb) == -1)
		mp->rc = -errno;
	else {
		mp->f_bsize	= sfb.f_bsize;
		mp->f_blocks	= sfb.f_blocks;
		mp->f_bfree	= sfb.f_bfree;
		mp->f_bavail	= sfb.f_bavail;
		mp->f_files	= sfb.f_files;
		mp->f_ffree	= sfb.f_ffree;
	}
	return (0);
}

int
slmds_symlink(struct pscrpc_request *rq)
{
	char from[PATH_MAX], to[PATH_MAX];
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov[2];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen == 0 || mq->fromlen >= PATH_MAX ||
	    mq->tolen == 0 || mq->tolen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, iov, 2)) != 0)
		return (0);
	from[mq->fromlen] = '\0';
	to[mq->tolen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(to, 0) == -1)
		mp->rc = -errno;
	else if (symlink(from, to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_truncate(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_truncate_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (truncate(fn, mq->size) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_unlink(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_unlink_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (unlink(fn) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_utimes(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_generic_rep *mp;
	struct srm_utimes_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkgetsink(rq, &desc,
	    SRM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (utimes(fn, mq->times) == -1)
		mp->rc = -errno;
	return (0);
}

int
setcred(uid_t uid, gid_t gid, uid_t *myuid, gid_t *mygid)
{
	uid_t tuid;
	gid_t tgid;

	/* Set fs credentials */
	spinlock(&fsidlock);
	*myuid = getuid();
	if ((tuid = setfsuid(uid)) != *myuid)
		psc_fatal("invalid fsuid %u", tuid);
	if (setfsuid(uid) != (int)uid) {
		psc_error("setfsuid %u", uid);
		return (-1);
	}

	*mygid = getgid();
	if ((tgid = setfsgid(gid)) != *mygid)
		psc_fatal("invalid fsgid %u", tgid);
	if (setfsgid(gid) != (int)gid) {
		psc_error("setfsgid %u", gid);
		return (-1);
	}
	return (0);
}

void
revokecred(uid_t uid, gid_t gid)
{
	setfsuid(uid);
	if (setfsuid(uid) != (int)uid)
		psc_fatal("setfsuid %d", uid);
	setfsgid(gid);
	if (setfsgid(gid) != (int)gid)
		psc_fatal("setfsgid %d", gid);
	freelock(&fsidlock);
}

int
slmds_svc_handler(struct pscrpc_request *rq)
{
	struct slashrpc_export *sexp;
	uid_t myuid;
	gid_t mygid;
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slmds_connect(rq);
		target_send_reply_msg(rq, rc, 0);
		return (rc);
	}

	sexp = slashrpc_export_get(rq->rq_export);
	if (setcred(sexp->uid, sexp->gid, &myuid, &mygid) == -1)
		goto done;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CHMOD:
		rc = slmds_chmod(rq);
		break;
	case SRMT_CHOWN:
		rc = slmds_chown(rq);
		break;
	case SRMT_CREATE:
		rc = slmds_create(rq);
		break;
	case SRMT_DESTROY:	/* client has unmounted */
		break;
	case SRMT_GETATTR:
		rc = slmds_getattr(rq);
		break;
	case SRMT_FGETATTR:
		rc = slmds_fgetattr(rq);
		break;
	case SRMT_FTRUNCATE:
		rc = slmds_ftruncate(rq);
		break;
//	case SRMT_FCHMOD:
//		rc = slmds_fchmod(rq);
//		break;
//	case SRMT_FCHOWN:
//		rc = slmds_fchown(rq);
//		break;
	case SRMT_LINK:
		rc = slmds_link(rq);
		break;
	case SRMT_LOCK:
		break;
	case SRMT_MKDIR:
		rc = slmds_mkdir(rq);
		break;
	case SRMT_MKNOD:
		rc = slmds_mknod(rq);
		break;
	case SRMT_OPEN:
		rc = slmds_open(rq);
		break;
	case SRMT_OPENDIR:
		rc = slmds_opendir(rq);
		break;
	case SRMT_READDIR:
		rc = slmds_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slmds_readlink(rq);
		break;
	case SRMT_RELEASE:
		rc = slmds_release(rq);
		break;
	case SRMT_RELEASEDIR:
		rc = slmds_releasedir(rq);
		break;
	case SRMT_RENAME:
		rc = slmds_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slmds_rmdir(rq);
		break;
	case SRMT_STATFS:
		rc = slmds_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slmds_symlink(rq);
		break;
	case SRMT_TRUNCATE:
		rc = slmds_truncate(rq);
		break;
	case SRMT_UNLINK:
		rc = slmds_unlink(rq);
		break;
	case SRMT_UTIMES:
		rc = slmds_utimes(rq);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		rc = pscrpc_error(rq);
		goto done;
	}
	psc_info("rq->rq_status == %d", rq->rq_status);
	target_send_reply_msg(rq, rc, 0);

 done:
	revokecred(myuid, mygid);
	return (rc);
}

/**
 * slmds_init - start up the MDS threads via pscrpc_thread_spawn()
 */
void
slmds_init(void)
{
	pscrpc_svc_handle_t *svh;

	svh = PSCALLOC(sizeof(*svh));
	svh->svh_nbufs = SRM_NBUFS;
	svh->svh_bufsz = SRM_BUFSZ;
	svh->svh_reqsz = SRM_BUFSZ;
	svh->svh_repsz = SRM_REPSZ;
	svh->svh_req_portal = SRM_REQ_PORTAL;
	svh->svh_rep_portal = SRM_REP_PORTAL;
	svh->svh_type = SLTHRT_RPCMDS;
	svh->svh_nthreads = SRM_NTHREADS;
	svh->svh_handler = slmds_svc_handler;

	if (snprintf(svh->svh_svc_name, sizeof(svh->svh_svc_name), "%s",
	    SRM_SVCNAME) == -1)
		psc_fatal("snprintf");

	pscrpc_thread_spawn(svh, struct slash_rpcmdsthr);
}
