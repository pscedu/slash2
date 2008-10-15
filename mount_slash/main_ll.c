/* $Id: main.c 4180 2008-09-16 21:50:48Z yanovich $ */

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


static int
slash2fuse_access(fuse_req_t req, fuse_ino_t ino, int mask)
{
	msfsthr_ensure();
	// XXX Send Access RPC
	return (0);
}

static void 
slash2fuse_access_helper(fuse_req_t req, fuse_ino_t ino, int mask)
{
	int error = slash2fuse_access(req, real_ino, mask);
	fuse_reply_err(req, error);
}

//XXX convert me
static int
slash2fuse_opencreate(fuse_req_t req, fuse_ino_t ino, 
		      struct fuse_file_info *fi, int fflags, 
		      mode_t createmode, const char *name)
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

static void
slash2fuse_create_helper(fuse_req_t req, fuse_ino_t parent, const char *name, 
			 mode_t mode, struct fuse_file_info *fi)
{
        int error = slash2fuse_opencreate(req, parent, fi, 
					  fi->flags | O_CREAT, mode, name);
        if(error)
                fuse_reply_err(req, error);
}

static void 
slash2fuse_open_helper(fuse_req_t req, fuse_ino_t ino, 
		       struct fuse_file_info *fi)
{
        int error = slash2fuse_opencreate(req, ino, fi, fi->flags, 0, NULL);
        if(error)
                fuse_reply_err(req, error);
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

//XXX convert me to use ino rather than pathname
static int
slash2fuse_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
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

static void 
slash2fuse_getattr_helper(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
        int error = slash2fuse_getattr(req, ino, fi);
        if(error)
                fuse_reply_err(req, error);
}


static int
slash2fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, 
		const char *newname)
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

static void slash2fuse_link_helper(fuse_req_t req, fuse_ino_t ino, 
				fuse_ino_t newparent, const char *newname)
{
        int error = slash2fuse_link(req, ino, newparent, newname);
        if(error)
                fuse_reply_err(req, error);
}

// XXX convert me
static int
slash2fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
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

static void 
slash2fuse_mkdir_helper(fuse_req_t req, fuse_ino_t parent, 
				 const char *name, mode_t mode)
{
        int error = slash2fuse_mkdir(req, parent, name, mode);
        if(error)
                fuse_reply_err(req, error);
}


//XXX convert me
static int 
slash2fuse_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
        if(strlen(name) >= MAXNAMELEN)
                return ENAMETOOLONG;

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

static void 
slash2fuse_rmdir_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
        int error = slash2fuse_rmdir(req, parent, name);
        /* rmdir events always reply_err */
        fuse_reply_err(req, error);
}

// XXX convert me
static int
slash2fuse_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, 
		 mode_t mode, dev_t rdev)
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

static void 
slash2fuse_mknod_helper(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
        int error = slash2fuse_mknod(req, parent, name, mode, rdev);
        if(error)
                fuse_reply_err(req, error);
}

// XXX convert
static int
slash_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
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

static void 
slash2fuse_opendir_helper(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
        int error = slash2fuse_opendir(req, ino, fi);
        if(error)
                fuse_reply_err(req, error);
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

//XXX convert me
static int 
slash2fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, 
		struct fuse_file_info *fi)
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

static void 
slash2fuse_readdir_helper(fuse_req_t req, fuse_ino_t ino, size_t size, 
		       off_t off, struct fuse_file_info *fi)
{
        fuse_ino_t real_ino = ino == 1 ? 3 : ino;

        int error = slash2fuse_readdir(req, real_ino, size, off, fi);
        if(error)
                fuse_reply_err(req, error);
}


// XXX convert
static int 
slash2fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	// XXX fill me in
        struct fuse_entry_param e = { 0 };

        e.attr_timeout = 0.0;
        e.entry_timeout = 0.0;

        if(vp == NULL)
                goto out;

        e.ino = VTOZ(vp)->z_id;
        if(e.ino == 3)
                e.ino = 1;

        e.generation = VTOZ(vp)->z_phys->zp_gen;

        error = slash2fuse_stat(vp, &e.attr, &cred);

}

static void 
slash2fuse_lookup_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
        int error = slash2fuse_lookup(req, parent, name);
        if(error)
                fuse_reply_err(req, error);
}

// XXX convert me
static int
slash2fuse_readlink(fuse_req_t req, fuse_ino_t ino)
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

static void 
slash2fuse_readlink_helper(fuse_req_t req, fuse_ino_t ino)
{
        fuse_ino_t real_ino = ino == 1 ? 3 : ino;

        int error = slash2fuse_readlink(req, real_ino);
        if(error)
                fuse_reply_err(req, error);
}



// XXX convert me
int
slash2fuse_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
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

static void 
slash2fuse_release_helper(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
        int error = slash2fuse_release(req, ino, fi);
        /* Release events always reply_err */
        fuse_reply_err(req, error);
}


// XXX convert
static int
slash2fuse_rename(fuse_req_t req, fuse_ino_t parent, const char *name, 
	     fuse_ino_t newparent, const char *newname)
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


static void 
slash2fuse_rename_helper(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname)
{
        int error = slash2fuse_rename(req, parent, name, newparent, newname);

        /* rename events always reply_err */
        fuse_reply_err(req, error);
}

// XXX convert
static int
slash2fuse_statfs(fuse_req_t req)
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
	fuse_reply_statfs(req, &stat);
}

static int
slash2fuse_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, 
		   const char *name)
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

static void 
zfsfuse_symlink_helper(fuse_req_t req, const char *link, fuse_ino_t parent, 
		       const char *name)
{
        int error = zfsfuse_symlink(req, link, parent, name);
        if(error)
                fuse_reply_err(req, error);
}

// XXX convert me
static int 
slash2fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
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

static void 
slash2fuse_unlink_helper(fuse_req_t req, fuse_ino_t parent, const char *name)
{
        int error = slash2fuse_unlink(req, parent, name);
        /* unlink events always reply_err */
        fuse_reply_err(req, error);
}

// XXX convert me
static int 
slash2fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, 
		int to_set, struct fuse_file_info *fi)
{
	if(to_set & FUSE_SET_ATTR_MODE) {
                vattr.va_mask |= AT_MODE;
                vattr.va_mode = attr->st_mode;
        }
        if(to_set & FUSE_SET_ATTR_UID) {
                vattr.va_mask |= AT_UID;
                vattr.va_uid = attr->st_uid;
        }
        if(to_set & FUSE_SET_ATTR_GID) {
                vattr.va_mask |= AT_GID;
                vattr.va_gid = attr->st_gid;
        }
        if(to_set & FUSE_SET_ATTR_SIZE) {
                vattr.va_mask |= AT_SIZE;
                vattr.va_size = attr->st_size;
        }
        if(to_set & FUSE_SET_ATTR_ATIME) {
                vattr.va_mask |= AT_ATIME;
                TIME_TO_TIMESTRUC(attr->st_atime, &vattr.va_atime);
        }
        if(to_set & FUSE_SET_ATTR_MTIME) {
                vattr.va_mask |= AT_MTIME;
                TIME_TO_TIMESTRUC(attr->st_mtime, &vattr.va_mtime);
        }

 out: ;
        struct stat stat_reply;

	if(!error)
                fuse_reply_attr(req, &stat_reply, 0.0);

	return (error);
}

static void 
slash2fuse_setattr_helper(fuse_req_t req, fuse_ino_t ino, struct stat *attr, 
		       int to_set, struct fuse_file_info *fi)
{
        int error = slash2fuse_setattr(req, ino, attr, to_set, fi);
        if(error)
                fuse_reply_err(req, error);
}


//XXX convert me
static int slash2fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{

}

static void 
slash2fuse_fsync_helper(fuse_req_t req, fuse_ino_t ino, int datasync, 
		     struct fuse_file_info *fi)
{
        int error = slash2fuse_fsync(req, ino, datasync, fi);

        /* fsync events always reply_err */
        fuse_reply_err(req, error);
}

static void
slash2fuse_destroy(void *userdata)
{
	//do an unmount of slash2
}

// XXX convertme
static int 
slash2fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf, 
	      size_t size, off_t off, struct fuse_file_info *fi)
{

}


static void 
slash2fuse_write_helper(fuse_req_t req, fuse_ino_t ino, const char *buf, 
		     size_t size, off_t off, struct fuse_file_info *fi)
{
        int error = slash2fuse_write(req, ino, buf, size, off, fi);
        if(error)
                fuse_reply_err(req, error);
}


// XXX convert me
static int 
slash2fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, 
	     [struct fuse_file_info *fi) 
{


}

static void 
slash2fuse_read_helper(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, 
		    struct fuse_file_info *fi)
{
        int error = slash2fuse_read(req, ino, size, off, fi);
        if(error)
                fuse_reply_err(req, error);
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


struct fuse_lowlevel_ops zfs_operations =
{
	.open       = slash2fuse_open_helper,    // 
	.read       = slash2fuse_read_helper,
	.write      = slash2fuse_write_helper,
	.release    = slash2fuse_release_helper, //
	.opendir    = slash2fuse_opendir_helper, //
	.readdir    = slash2fuse_readdir_helper, //
	.releasedir = slash2fuse_release_helper, //
	.lookup     = slash2fuse_lookup_helper,  //
	.getattr    = slash2fuse_getattr_helper, //
	.readlink   = slash2fuse_readlink_helper,//
	.mkdir      = slash2fuse_mkdir_helper,   //
	.rmdir      = slash2fuse_rmdir_helper,   //
	.create     = slash2fuse_create_helper,  //
	.unlink     = slash2fuse_unlink_helper,  //
	.mknod      = slash2fuse_mknod_helper,   //
	.symlink    = slash2fuse_symlink_helper, //
	.link       = slash2fuse_link_helper,    //
	.rename     = slash2fuse_rename_helper,  //
	.setattr    = slash2fuse_setattr_helper, //  
	.fsync      = slash2fuse_fsync_helper, //
	.fsyncdir   = slash2fuse_fsync_helper, //
	.access     = slash2fuse_access_helper,  //
	.statfs     = slash2fuse_statfs, //
	.destroy    = slash2fuse_destroy, //
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
