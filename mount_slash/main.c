/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26

#include <fuse.h>

#include "pfl.h"
#include "psc_types.h"
#include "psc_mount/dhfh.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_util/cdefs.h"
#include "psc_util/ctlsvr.h"
#include "psc_util/log.h"
#include "psc_util/threadtable.h"

#include "fidcache.h"
#include "mount_slash.h"
#include "slashrpc.h"

sl_ios_id_t prefIOS = IOS_ID_ANY;

const char *progname;

void
msfsthr_teardown(void *arg)
{
	struct psc_thread *thr = arg;

	free(thr->pscthr_private);
	pscthr_destroy(thr);
	free(thr);
}

void
msfsthr_ensure(void)
{
	static psc_spinlock_t lock = LOCK_INITIALIZER;
	static int thrid; /* XXX maintain bitmap for transiency */
	struct psc_thread *thr;

	thr = psc_threadtbl_get_canfail();
	if (thr == NULL) {
		spinlock(&lock);
		thr = psc_threadtbl_get_canfail();
		if (thr == NULL) {
			thr = PSCALLOC(sizeof(*thr));
			pscthr_init(thr, MSTHRT_FS, NULL,
			    PSCALLOC(sizeof(struct msfs_thread)),
			    "msfsthr%d", thrid++);
			pthread_cleanup_push(msfsthr_teardown, thr);
			pthread_cleanup_pop(0);
		}
		freelock(&lock);
	}
	psc_assert(thr->pscthr_type == MSTHRT_FS);
}

int
slash_access(__unusedx const char *path, __unusedx int mask)
{
	msfsthr_ensure();

	// fidcache op
	return (0);
}

int
slash_chmod(const char *path, mode_t mode)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_chmod_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_CHMOD, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->mode = mode;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_chown(const char *path, uid_t uid, gid_t gid)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_chown_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_CHOWN, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->uid = uid;
	mq->gid = gid;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_create_req *mq;
	struct srm_create_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_CREATE, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->mode = mode;
	mq->flags = fi->flags;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			/* Return the cfd to Fuse and register
			 *   it in the fd cache.
			 */
			fi->fh = mp->cfd;
			fh_register(mp->cfd, FHENT_WRITE, msl_fdreg_cb, NULL);
		}
	}
	pscrpc_req_finished(rq);
	return (rc);
}

void
slash_destroy(__unusedx void *arg)
{
	struct srm_generic_rep *mp;
	struct srm_chmod_req *mq;
	struct pscrpc_request *rq;

	msfsthr_ensure();

	if (RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_DESTROY, rq, mq, mp) == 0) {
		rsx_waitrep(rq, sizeof(*mp), &mp);
		pscrpc_req_finished(rq);
	}
}

int
slash_getattr(const char *path, struct stat *stb)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_GETATTR, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
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
	struct srm_fgetattr_req *mq;
	struct srm_fgetattr_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_FGETATTR, rq, mq, mp)) != 0)
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
	struct srm_ftruncate_req *mq;
	struct srm_generic_rep *mp;
	struct pscrpc_request *rq;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_FTRUNCATE, rq, mq, mp)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->size = size;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_link(const char *from, const char *to)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_link_req *mq;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_LINK, rq, mq, mp)) != 0)
		return (rc);
	mq->fromlen = strlen(from);
	mq->tolen = strlen(to);
	iov[0].iov_base = (void *)from;
	iov[0].iov_len = strlen(from);
	iov[1].iov_base = (void *)to;
	iov[1].iov_len = strlen(to);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    iov, 2);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_mkdir(const char *path, mode_t mode)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_mkdir_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_MKDIR, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->mode = mode;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_mknod(const char *path, mode_t mode, dev_t dev)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_mknod_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_MKNOD, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->mode = mode;
	mq->dev = dev;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_open(const char *path, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_open_req *mq;
	struct srm_open_rep *mp;
	struct iovec iov;
	int rc, oflag;

	msfsthr_ensure();

	oflag = msl_fuse_2_oflags(fi->flags);

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_OPEN, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->flags = fi->flags;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			fi->fh = mp->cfd;
			fh_register(mp->cfd, oflag, msl_fdreg_cb, NULL);
		}
	}
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_opendir(const char *path, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_opendir_req *mq;
	struct srm_opendir_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_OPENDIR, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
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
slash_read(__unusedx const char *path, char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_io_rep *mp;
	struct srm_io_req *mq;
	struct iovec iov;
	struct fhent *fh;
	int rc;

	msfsthr_ensure();

	/* First get the fhentry from the cache.
	 */
	fh = fh_lookup(fi->fh);
	if (!fh)
		return (-EBADF);

//	rc = msl_read(fh, buf, size, off);

	if ((rc = RSX_NEWREQ(ion_get()->csvc_import,
	    SRIC_VERSION, SRMT_READ, rq, mq, mp)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	mq->op = SRMIO_WR;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			iov.iov_base = buf;
			iov.iov_len = size;
			rc = rsx_bulkclient(rq, &desc, BULK_GET_SOURCE,
			    SRIC_BULK_PORTAL, &iov, 1);
			if (desc)
				pscrpc_free_bulk(desc);
			if (rc == 0)
				rc = mp->size;
		}
	}
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_readdir(__unusedx const char *path, void *buf, fuse_fill_dir_t filler,
    off_t offset, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct dirent *d;
	struct iovec iov;
	struct stat stb;
	char *mb;
	u64 off;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_READDIR, rq, mq, mp)) != 0)
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
	rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK, SRMC_BULK_PORTAL,
	    &iov, 1);
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
	struct pscrpc_bulk_desc *de, *di;
	struct pscrpc_request *rq;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_READLINK, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->size = size;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &de, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, size, &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			iov.iov_base = buf;
			iov.iov_len = size;
			rc = rsx_bulkserver(rq, &di, BULK_GET_SINK,
			    SRMC_BULK_PORTAL, &iov, 1);
			if (di)
				pscrpc_free_bulk(di);
		}
	}
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_release(__unusedx const char *path, struct fuse_file_info *fi)
{
	struct srm_generic_rep *mp;
	struct srm_release_req *mq;
	struct pscrpc_request *rq;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RELEASE, rq, mq, mp)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_releasedir(__unusedx const char *path, struct fuse_file_info *fi)
{
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_release_req *mq;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RELEASEDIR, rq, mq, mp)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_rename(const char *from, const char *to)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RENAME, rq, mq, mp)) != 0)
		return (rc);
	mq->fromlen = strlen(from);
	mq->tolen = strlen(to);
	iov[0].iov_base = (void *)from;
	iov[0].iov_len = strlen(from);
	iov[1].iov_base = (void *)to;
	iov[1].iov_len = strlen(to);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    iov, 2);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_rmdir(const char *path)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_rmdir_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_RMDIR, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_statfs(const char *path, struct statvfs *sfb)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_STATFS, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0) {
		if (mp->rc)
			rc = mp->rc;
		else {
			memset(sfb, 0, sizeof(*sfb));
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
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_symlink_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov[2];
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_SYMLINK, rq, mq, mp)) != 0)
		return (rc);
	mq->fromlen = strlen(from);
	mq->tolen = strlen(to);
	iov[0].iov_base = (void *)from;
	iov[0].iov_len = strlen(from);
	iov[1].iov_base = (void *)to;
	iov[1].iov_len = strlen(to);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    iov, 2);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_truncate(const char *path, off_t size)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_truncate_req *mq;
	struct srm_generic_rep *mp;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_TRUNCATE, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	mq->size = size;
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_unlink(const char *path)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_unlink_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_UNLINK, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_utimens(const char *path, const struct timespec ts[2])
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_generic_rep *mp;
	struct srm_utimes_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(mds_import, SRMC_VERSION,
	    SRMT_UTIMES, rq, mq, mp)) != 0)
		return (rc);
	mq->fnlen = strlen(path);
	memcpy(mq->times, ts, sizeof(ts));
	iov.iov_base = (void *)path;
	iov.iov_len = strlen(path);
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRMC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc;
	pscrpc_req_finished(rq);
	return (rc);
}

int
slash_write(__unusedx const char *path, const char *buf, size_t size,
    off_t offset, struct fuse_file_info *fi)
{
	struct pscrpc_bulk_desc *desc;
	struct pscrpc_request *rq;
	struct srm_io_rep *mp;
	struct srm_io_req *mq;
	struct iovec iov;
	int rc;

	msfsthr_ensure();

	if ((rc = RSX_NEWREQ(ion_get()->csvc_import,
	    SRIC_VERSION, SRMT_WRITE, rq, mq, mp)) != 0)
		return (rc);
	mq->cfd = fi->fh;
	mq->size = size;
	mq->offset = offset;
	mq->op = SRMIO_WR;
	iov.iov_base = (void *)buf;
	iov.iov_len = size;
	rsx_bulkclient(rq, &desc, BULK_GET_SOURCE, SRIC_BULK_PORTAL,
	    &iov, 1);
	if ((rc = rsx_waitrep(rq, sizeof(*mp), &mp)) == 0)
		rc = mp->rc ? mp->rc : (int)mp->size;
	pscrpc_req_finished(rq);
	return (rc);
}

void *
slash_init(__unusedx struct fuse_conn_info *conn)
{
	char *name;

	msfsthr_ensure();

	if (getenv("LNET_NETWORKS") == NULL)
		psc_fatalx("please export LNET_NETWORKS");
	if ((name = getenv("SLASH_MDS_NID")) == NULL)
		psc_fatalx("please export SLASH_MDS_NID");

	fidcache_init();
	rpc_initsvc();

	/* Start up service threads. */
	mseqpollthr_spawn();
	msctlthr_spawn();
	mstimerthr_spawn();

	if (msrmc_connect(name))
		psc_fatal("unable to connect to MDS");
	if ((name = getenv("SLASH2_PIOS_ID")) != NULL) {
		if ((prefIOS = libsl_str2id(name)) == IOS_ID_ANY)
			psc_warnx("SLASH2_PIOS_ID (%s) does not resolve to "
				  "a valid IOS, defaulting to IOS_ID_ANY", name);
	}
	return (NULL);
}

struct fuse_operations slashops = {
//	.access		= slash_access,
	.chmod		= slash_chmod,
	.chown		= slash_chown,
	.create		= slash_create,
	.destroy	= slash_destroy,
	.fgetattr	= slash_fgetattr,
	.ftruncate	= slash_ftruncate,
	.getattr	= slash_getattr,
	.init		= slash_init,
	.link		= slash_link,
//	.lock		= slash_lock,
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

void *nal_thread(void *);

void *
lndthr_begin(void *arg)
{
	struct psc_thread *thr;

	thr = arg;
	return (nal_thread(thr->pscthr_private));
}

void
mslndthr_spawn(pthread_t *t, void *(*startf)(void *), void *arg)
{
	extern int tcpnal_instances;
	struct psc_thread *pt;

	if (startf != nal_thread)
		psc_fatalx("unknown LNET start routine");

	pt = PSCALLOC(sizeof(*pt));
	pscthr_init(pt, MSTHRT_LND, lndthr_begin, arg, "mslndthr%d",
	    tcpnal_instances - 1);
	*t = pt->pscthr_pthread;
	pt->pscthr_private = arg;
}

enum {
	MS_OPT_CTLSOCK
};

struct fuse_opt msopts[] = {
	FUSE_OPT_KEY("-S ", MS_OPT_CTLSOCK),
	FUSE_OPT_END
};

__dead void
usage(void)
{
	char *argv[] = { (char *)progname, "-ho", NULL };

	fprintf(stderr,
	    "usage: %s [options] node\n"
	    "\n"
	    "Slash options:\n"
	    "    -S ctlsock             specify alternate control socket\n\n",
	    progname);
	fuse_main(2, argv, &slashops, NULL);
	exit(1);
}

int
proc_opt(__unusedx void *data, const char *arg, int c,
    __unusedx struct fuse_args *outargs)
{
	switch (c) {
	case FUSE_OPT_KEY_OPT:
	case FUSE_OPT_KEY_NONOPT:
		return (1);
	case MS_OPT_CTLSOCK:
		ctlsockfn = arg + 2;
		break;
	default:
		usage();
	}
	return (0);
}

int
main(int argc, char *argv[])
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	progname = argv[0];
	if (fuse_opt_parse(&args, NULL, msopts, proc_opt))
		usage();

#define THRTAB_SZ 13
	pfl_init(THRTAB_SZ);
	lnet_thrspawnf = mslndthr_spawn;

	msfsthr_ensure();

	return (fuse_main(args.argc, args.argv, &slashops, NULL));
}
