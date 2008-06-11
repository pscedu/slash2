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
#include "psc_util/lock.h"
#include "psc_util/strlcpy.h"

#include "cfd.h"
#include "dircache.h"
#include "fid.h"
#include "rpc.h"
#include "slashd.h"
#include "slashrpc.h"

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

int
slrmc_connect(struct pscrpc_request *rq)
{
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRCM_MAGIC || mq->version != SRCM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slrmc_chmod(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_fchmod(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_fchmod_req *mq;
	char fn[PATH_MAX];
	struct slash_fid fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(rq->rq_export, mq->cfd, &fid))
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
slrmc_chown(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_fchown(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_fchown_req *mq;
	struct slash_fid fid;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(rq->rq_export, mq->cfd, &fid))
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
slrmc_create(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_create_rep *mp;
	struct srm_create_req *mq;
	struct iovec iov;
	char fn[PATH_MAX];
	slfid_t fid;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 0) == -1)
		mp->rc = -errno;
	else if ((fd = creat(fn, mq->mode)) == -1)
		mp->rc = -errno;
	else {
		close(fd);
		if (fid_get(&fid, fn, 1) == -1)
			mp->rc = -errno;
		else
			cfdnew(&mp->cfd, rq->rq_export, fid);
	}
	return (0);
}

int
slrmc_getattr(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_fgetattr(struct pscrpc_request *rq)
{
	struct srm_fgetattr_req *mq;
	struct srm_fgetattr_rep *mp;
	struct stat stb;
	char fn[PATH_MAX];
	slfid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(rq->rq_export, mq->cfd, &fid)) {
		mp->rc = -errno;
		return (0);
	}
	fid_makepath(fid, fn);
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
slrmc_ftruncate(struct pscrpc_request *rq)
{
	struct srm_ftruncate_req *mq;
	struct srm_generic_rep *mp;
	char fn[PATH_MAX];
	slfid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfd2fid(rq->rq_export, mq->cfd, &fid))
		mp->rc = -errno;
	else {
		fid_makepath(fid, fn);
		if (truncate(fn, mq->size) == -1)
			mp->rc = -errno;
	}
	return (0);
}

int
slrmc_link(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, iov, 2)) != 0)
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
slrmc_mkdir(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_mknod(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_open(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_open_req *mq;
	struct srm_open_rep *mp;
	struct iovec iov;
	char fn[PATH_MAX];
	slfid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, fn, 1) == -1)
		mp->rc = -errno;
	else
		cfdnew(&mp->cfd, rq->rq_export, fid);
	/* XXX check access permissions */
	return (0);
}

int
slrmc_opendir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_opendir_req *mq;
	struct srm_opendir_rep *mp;
	struct iovec iov;
	char fn[PATH_MAX];
	slfid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fnlen == 0 || mq->fnlen >= PATH_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}
	iov.iov_base = fn;
	iov.iov_len = mq->fnlen;
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
		return (0);
	fn[mq->fnlen] = '\0';
	pscrpc_free_bulk(desc);
	if (translate_pathname(fn, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, fn, 1) == -1)
		mp->rc = -errno;
	else
		cfdnew(&mp->cfd, rq->rq_export, fid);
	/* XXX check access permissions */
	return (0);
}

#define READDIR_BUFSZ (512 * 1024)

int
slrmc_readdir(struct pscrpc_request *rq)
{
	struct dirent ents[READDIR_BUFSZ];
	struct pscrpc_bulk_desc *desc;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct dircache *dc;
	struct iovec iov;
	slfid_t fid;
	int rc;

	RSX_ALLOCREP(rq, rq, mp);
	if (cfd2fid(rq->rq_export, mq->cfd, &fid)) {
		mp->rc = -errno;
		return (0);
	}
	dc = dircache_get(fid);
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
	mp->rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRCM_BULK_PORTAL, &iov, 1);
// rc / pscPageSize
	if (desc)
		pscrpc_free_bulk(desc);
	return (0);
}

int
slrmc_readlink(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
		mp->rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
		    SRCM_BULK_PORTAL, &iov, 1);
		if (desc)
			pscrpc_free_bulk(desc);
	}
	return (0);
}

int
slrmc_release(struct pscrpc_request *rq)
{
	struct srm_release_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slrmc_releasedir(struct pscrpc_request *rq)
{
	struct srm_releasedir_req *mq;
	struct srm_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slrmc_rename(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, iov, 2)) != 0)
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
slrmc_rmdir(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_statfs(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_symlink(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, iov, 2)) != 0)
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
slrmc_truncate(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_unlink(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_utimes(struct pscrpc_request *rq)
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
	if ((mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRCM_BULK_PORTAL, &iov, 1)) != 0)
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
slrmc_handler(struct pscrpc_request *rq)
{
	struct slashrpc_export *sexp;
	uid_t myuid;
	gid_t mygid;
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slrmc_connect(rq);
		target_send_reply_msg(rq, rc, 0);
		return (rc);
	}

	sexp = slashrpc_export_get(rq->rq_export);
	if (setcred(sexp->uid, sexp->gid, &myuid, &mygid) == -1)
		goto done;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_CHMOD:
		rc = slrmc_chmod(rq);
		break;
	case SRMT_CHOWN:
		rc = slrmc_chown(rq);
		break;
	case SRMT_CREATE:
		rc = slrmc_create(rq);
		break;
	case SRMT_DESTROY:	/* client has unmounted */
		break;
	case SRMT_GETATTR:
		rc = slrmc_getattr(rq);
		break;
	case SRMT_FGETATTR:
		rc = slrmc_fgetattr(rq);
		break;
	case SRMT_FTRUNCATE:
		rc = slrmc_ftruncate(rq);
		break;
//	case SRMT_FCHMOD:
//		rc = slrmc_fchmod(rq);
//		break;
//	case SRMT_FCHOWN:
//		rc = slrmc_fchown(rq);
//		break;
	case SRMT_LINK:
		rc = slrmc_link(rq);
		break;
	case SRMT_LOCK:
		break;
	case SRMT_MKDIR:
		rc = slrmc_mkdir(rq);
		break;
	case SRMT_MKNOD:
		rc = slrmc_mknod(rq);
		break;
	case SRMT_OPEN:
		rc = slrmc_open(rq);
		break;
	case SRMT_OPENDIR:
		rc = slrmc_opendir(rq);
		break;
	case SRMT_READDIR:
		rc = slrmc_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slrmc_readlink(rq);
		break;
	case SRMT_RELEASE:
		rc = slrmc_release(rq);
		break;
	case SRMT_RELEASEDIR:
		rc = slrmc_releasedir(rq);
		break;
	case SRMT_RENAME:
		rc = slrmc_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slrmc_rmdir(rq);
		break;
	case SRMT_STATFS:
		rc = slrmc_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slrmc_symlink(rq);
		break;
	case SRMT_TRUNCATE:
		rc = slrmc_truncate(rq);
		break;
	case SRMT_UNLINK:
		rc = slrmc_unlink(rq);
		break;
	case SRMT_UTIMES:
		rc = slrmc_utimes(rq);
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
