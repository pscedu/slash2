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
	struct slashrpc_connect_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRM_MAGIC || mq->version != SRM_VERSION)
		mp->rc = -EINVAL;
	return (0);
}

int
slmds_access(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_access_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (access(mq->path, mq->mask) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_chmod(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_chmod_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (chmod(mq->path, mq->mode) == -1)
		mp->rc = -errno;
	return (0);
}

#if 0
int
slmds_fchmod(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_fchmod_req *mq;
	char fn[PATH_MAX];
	slash_fid_t fid;

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
	struct slashrpc_generic_rep *mp;
	struct slashrpc_chown_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (chown(mq->path, mq->uid, mq->gid) == -1)
		mp->rc = -errno;
	return (0);
}

/**
 *
 * Notes:  fchown of a directory cannot be supported in this version since
 *          there is no immutable namespace for directories.
 */
#if 0
int
slmds_fchown(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_fchown_req *mq;
	char fn[PATH_MAX];
	slash_fid_t fid;

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
	struct slashrpc_generic_rep *mp;
	struct slashrpc_create_req *mq;
	int fd;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 0) == -1)
		mp->rc = -errno;
	else if ((fd = creat(mq->path, mq->mode)) == -1)
		mp->rc = -errno;
	else
		close(fd);
	return (0);
}

int
slmds_getattr(struct pscrpc_request *rq)
{
	struct slashrpc_getattr_req *mq;
	struct slashrpc_getattr_rep *mp;
	struct stat stb;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1) {
		mp->rc = -errno;
		return (0);
	}
	if (stat(mq->path, &stb) == -1) {
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
	struct slashrpc_fgetattr_req *mq;
	struct slashrpc_fgetattr_rep *mp;
	char fn[PATH_MAX];
	slash_fid_t fid;
	struct stat stb;

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
	struct slashrpc_ftruncate_req *mq;
	struct slashrpc_generic_rep *mp;
	char fn[PATH_MAX];
	slash_fid_t fid;

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
	struct slashrpc_generic_rep *mp;
	struct slashrpc_link_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(mq->to, 0) == -1)
		mp->rc = -errno;
	else if (link(mq->from, mq->to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_mkdir(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_mkdir_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 0) == -1)
		mp->rc = -errno;
	else if (mkdir(mq->path, mq->mode) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_mknod(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_mknod_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 0) == -1)
		mp->rc = -errno;
	else if (mknod(mq->path, mq->mode, mq->dev) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_open(struct pscrpc_request *rq)
{
	struct slashrpc_open_req *mq;
	struct slashrpc_open_rep *mp;
	slash_fid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, mq->path, 1) == -1)
		mp->rc = -errno;
	else
		cfdnew(&mp->cfd, rq->rq_export, &fid);
	/* XXX check access permissions */
	return (0);
}

int
slmds_opendir(struct pscrpc_request *rq)
{
	struct slashrpc_opendir_req *mq;
	struct slashrpc_opendir_rep *mp;
	slash_fid_t fid;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (fid_get(&fid, mq->path, 1) == -1)
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
	struct slashrpc_readdir_req *mq;
	struct slashrpc_readdir_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct dircache *dc;
	struct iovec iov;
	slash_fid_t fid;
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
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;
	struct pscrpc_bulk_desc *desc;
	struct iovec iov;
	char fn[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->size >= PATH_MAX || mq->size == 0)
		return (-EINVAL);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (readlink(mq->path, fn, mq->size) == -1)
		mp->rc = -errno;
	else if (untranslate_pathname(fn) == -1)
		mp->rc = -errno;
	else {
		iov.iov_base = fn;
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
	struct slashrpc_release_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_releasedir(struct pscrpc_request *rq)
{
	struct slashrpc_releasedir_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_rename(struct pscrpc_request *rq)
{
	struct slashrpc_rename_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(mq->to, 0) == -1)
		mp->rc = -errno;
	else if (rename(mq->from, mq->to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_rmdir(struct pscrpc_request *rq)
{
	struct slashrpc_generic_rep *mp;
	struct slashrpc_rmdir_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (rmdir(mq->path) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_statfs(struct pscrpc_request *rq)
{
	struct slashrpc_statfs_req *mq;
	struct slashrpc_statfs_rep *mp;
	struct statfs sfb;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (statfs(mq->path, &sfb) == -1)
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
	struct slashrpc_symlink_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->from, 1) == -1)
		mp->rc = -errno;
	else if (translate_pathname(mq->to, 0) == -1)
		mp->rc = -errno;
	else if (symlink(mq->from, mq->to) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_truncate(struct pscrpc_request *rq)
{
	struct slashrpc_truncate_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (truncate(mq->path, mq->size) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_unlink(struct pscrpc_request *rq)
{
	struct slashrpc_unlink_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (unlink(mq->path) == -1)
		mp->rc = -errno;
	return (0);
}

int
slmds_utimes(struct pscrpc_request *rq)
{
	struct slashrpc_utimes_req *mq;
	struct slashrpc_generic_rep *mp;

	RSX_ALLOCREP(rq, mq, mp);
	if (translate_pathname(mq->path, 1) == -1)
		mp->rc = -errno;
	else if (utimes(mq->path, mq->times) == -1)
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
	*mygid = getgid();

	if ((tuid = setfsuid(uid)) != *myuid)
		psc_fatal("invalid fsuid %u", tuid);
	if (setfsuid(uid) != (int)uid) {
		psc_error("setfsuid %u", uid);
		return (-1);
	}

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
slmds_svc_handler(struct pscrpc_request *req)
{
	struct slashrpc_export *sexp;
	uid_t myuid;
	gid_t mygid;
	int rc = 0;

	ENTRY;
	DEBUG_REQ(PLL_TRACE, req, "new req");

	switch (req->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slmds_connect(req);
		target_send_reply_msg(req, rc, 0);
		RETURN(rc);
	}

	sexp = slashrpc_export_get(req->rq_export);
	if (setcred(sexp->uid, sexp->gid, &myuid, &mygid) == -1)
		goto done;

	switch (req->rq_reqmsg->opc) {
	case SRMT_ACCESS:
		rc = slmds_access(req);
		break;
	case SRMT_CHMOD:
		rc = slmds_chmod(req);
		break;
	case SRMT_CHOWN:
		rc = slmds_chown(req);
		break;
	case SRMT_CREATE:
		rc = slmds_create(req);
		break;
	case SRMT_DESTROY:	/* client has unmounted */
		break;
	case SRMT_GETATTR:
		rc = slmds_getattr(req);
		break;
	case SRMT_FGETATTR:
		rc = slmds_fgetattr(req);
		break;
	case SRMT_FTRUNCATE:
		rc = slmds_ftruncate(req);
		break;
//	case SRMT_FCHMOD:
//		rc = slmds_fchmod(req);
//		break;
//	case SRMT_FCHOWN:
//		rc = slmds_fchown(req);
//		break;
	case SRMT_LINK:
		rc = slmds_link(req);
		break;
	case SRMT_LOCK:
		break;
	case SRMT_MKDIR:
		rc = slmds_mkdir(req);
		break;
	case SRMT_MKNOD:
		rc = slmds_mknod(req);
		break;
	case SRMT_OPEN:
		rc = slmds_open(req);
		break;
	case SRMT_OPENDIR:
		rc = slmds_opendir(req);
		break;
	case SRMT_READDIR:
		rc = slmds_readdir(req);
		break;
	case SRMT_READLINK:
		rc = slmds_readlink(req);
		break;
	case SRMT_RELEASE:
		rc = slmds_release(req);
		break;
	case SRMT_RELEASEDIR:
		rc = slmds_releasedir(req);
		break;
	case SRMT_RENAME:
		rc = slmds_rename(req);
		break;
	case SRMT_RMDIR:
		rc = slmds_rmdir(req);
		break;
	case SRMT_STATFS:
		rc = slmds_statfs(req);
		break;
	case SRMT_SYMLINK:
		rc = slmds_symlink(req);
		break;
	case SRMT_TRUNCATE:
		rc = slmds_truncate(req);
		break;
	case SRMT_UNLINK:
		rc = slmds_unlink(req);
		break;
	case SRMT_UTIMES:
		rc = slmds_utimes(req);
		break;
	default:
		psc_errorx("Unexpected opcode %d", req->rq_reqmsg->opc);
		req->rq_status = -ENOSYS;
		rc = pscrpc_error(req);
		goto done;
	}
	psc_info("req->rq_status == %d", req->rq_status);
	target_send_reply_msg (req, rc, 0);

 done:
	revokecred(myuid, mygid);
	RETURN(rc);
}

/**
 * slmds_init - start up the mds threads via pscrpc_thread_spawn()
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
