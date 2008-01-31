/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/vfs.h>

#include <unistd.h>
#include <errno.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"

#include "cfd.h"
#include "dircache.h"
#include "fid.h"
#include "rpc.h"
#include "slash.h"
#include "slashrpc.h"

#define MDS_NTHREADS  8
#define MDS_NBUFS     1024
#define MDS_BUFSZ     (4096+256)
#define MDS_REPSZ     128
#define MDS_REQPORTAL RPCMDS_REQ_PORTAL
#define MDS_REPPORTAL RPCMDS_REP_PORTAL
#define MDS_SVCNAME   "slrpcmdsthr"

#define GENERIC_REPLY(rq, prc)								\
	do {										\
		struct slashrpc_generic_rep *_mp;					\
		int _rc, _size;								\
											\
		_size = sizeof(*(_mp));							\
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

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

/*
 * translate_pathname - rewrite a pathname from a client to the location
 *	it actually correponds with as known to slash in the server file system.
 * @path: client-issued path which will contain the server path on successful return.
 * Returns 0 on success or -1 on error.
 */
int
translate_pathname(char *path)
{
	char buf[PATH_MAX];
	int rc;

	if (realpath(path, buf) == NULL)
		return (-1);
//	rc = snprintf(path, PATH_MAX, "%s/%s", nodeProfile->slnprof_fsroot, buf);
	rc = snprintf(path, PATH_MAX, "%s/%s", "/slashfs", buf);
	if (rc == -1)
		return (-1);
	if (rc >= (int)sizeof(buf)) {
		errno = ENAMETOOLONG;
		return (-1);
	}
	return (0);
}

int
slmds_connect(struct pscrpc_request *rq)
{
	struct slashrpc_connect_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (mq->magic != SMDS_MAGIC || mq->version != SMDS_VERSION)
		rc = -EINVAL;
	GENERIC_REPLY(rq, rc);
}

int
slmds_access(struct pscrpc_request *rq)
{
	struct slashrpc_access_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (access(mq->path, mq->mask) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_chmod(struct pscrpc_request *rq)
{
	struct slashrpc_chmod_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (chmod(mq->path, mq->mode) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

#if 0
int
slmds_fchmod(struct pscrpc_request *rq)
{
	struct slashrpc_fchmod_req *mq;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn))
		rc = -errno;
	else if (chmod(fn, mq->mode) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}
#endif

int
slmds_chown(struct pscrpc_request *rq)
{
	struct slashrpc_chown_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (chown(mq->path, mq->uid, mq->gid) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
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
	struct slashrpc_fchown_req *mq;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn))
		rc = -errno;
	else if (chown(fn, mq->uid, mq->gid) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}
#endif

int
slmds_create(struct pscrpc_request *rq)
{
	struct slashrpc_create_req *mq;
	int fd, rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if ((fd = creat(mq->path, mq->mode)) == -1)
		rc = -errno;
	else
		close(fd);
	GENERIC_REPLY(rq, rc);
}

int
slmds_getattr(struct pscrpc_request *rq)
{
	struct slashrpc_getattr_req *mq;
	struct slashrpc_getattr_rep *mp;
	struct stat stb;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
	if (translate_pathname(mq->path) == -1) {
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

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
	if (cfd2fid(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn)) {
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
slmds_ftruncate(struct pscrpc_request *rq)
{
	struct slashrpc_ftruncate_req *mq;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (cfd2fid(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn))
		rc = -errno;
	else if (truncate(fn, mq->size) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_link(struct pscrpc_request *rq)
{
	struct slashrpc_link_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->from) == -1)
		rc = -errno;
	else if (translate_pathname(mq->to) == -1)
		rc = -errno;
	else if (link(mq->from, mq->to) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_mkdir(struct pscrpc_request *rq)
{
	struct slashrpc_mkdir_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (mkdir(mq->path, mq->mode) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_mknod(struct pscrpc_request *rq)
{
	struct slashrpc_mknod_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (mknod(mq->path, mq->mode, mq->dev) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_open(struct pscrpc_request *rq)
{
	struct slashrpc_open_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (open(mq->path, mq->flags) == -1)	/* XXX dead wrong */
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_opendir(struct pscrpc_request *rq)
{
	struct slashrpc_opendir_req *mq;
	struct slashrpc_opendir_rep *mp;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
	if (translate_pathname(mq->path) == -1)
		mp->rc = -errno;
	else if (cfdnew(&mp->cfd, rq->rq_export, mq->path))
		mp->rc = -errno;
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
	struct l_wait_info lwi;
	struct dircache *dc;
	int comms_error, rc;
	slash_fid_t fid;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
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

	desc = pscrpc_prep_bulk_exp(rq, rc / pscPageSize,
	    BULK_PUT_SOURCE, RPCMDS_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		mp->rc = -ENOMEM;
		return (0);
	}
	desc->bd_iov[0].iov_base = ents;
	desc->bd_iov[0].iov_len = mp->size;
	desc->bd_iov_count = 1;
	desc->bd_nob = mp->size;

	if (desc->bd_export->exp_failed)
		rc = -ENOTCONN;
	else
		rc = pscrpc_start_bulk_transfer(desc);

	if (rc == 0) {
		lwi = LWI_TIMEOUT_INTERVAL(20 * HZ / 2, HZ, NULL, desc);

		rc = psc_svr_wait_event(&desc->bd_waitq,
		    !pscrpc_bulk_active(desc) || desc->bd_export->exp_failed,
		    &lwi, NULL);
		LASSERT(rc == 0 || rc == -ETIMEDOUT);
		if (rc == -ETIMEDOUT) {
			psc_info("timeout on bulk PUT");
			pscrpc_abort_bulk(desc);
		} else if (desc->bd_export->exp_failed) {
			psc_info("eviction on bulk PUT");
			rc = -ENOTCONN;
			pscrpc_abort_bulk(desc);
		} else if (!desc->bd_success ||
		    desc->bd_nob_transferred != desc->bd_nob) {
			psc_info("%s bulk PUT %d(%d)",
			    desc->bd_success ? "truncated" : "network err",
			    desc->bd_nob_transferred, desc->bd_nob);
			/* XXX should this be a different errno? */
			rc = -ETIMEDOUT;
		}
	} else
		psc_info("pscrpc bulk put failed: rc %d", rc);
	comms_error = (rc != 0);
	if (rc == 0)
		psc_info("put readdir contents successfully");
	else if (!comms_error) {
		/* Only reply if there was no comms problem with bulk */
		rq->rq_status = rc;
		pscrpc_error(rq);
	}
	pscrpc_free_bulk(desc);
	mp->rc = rc;
	return (0);
}

int
slmds_readlink(struct pscrpc_request *rq)
{
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-EPROTO);
	if (mq->size > PATH_MAX || mq->size == 0)
		return (-EINVAL);
	GET_CUSTOM_REPLY_SZ(rq, mp, mq->size);
	mp->rc = 0;
	if (translate_pathname(mq->path) == -1)
		mp->rc = -errno;
	else if (readlink(mq->path, mp->buf, mq->size) == -1)
		mp->rc = -errno;
	else
		/* XXX untranslate pathname */;
	return (0);
}

int
slmds_release(struct pscrpc_request *rq)
{
	struct slashrpc_release_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_releasedir(struct pscrpc_request *rq)
{
	struct slashrpc_releasedir_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (cfdfree(rq->rq_export, mq->cfd) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_rename(struct pscrpc_request *rq)
{
	struct slashrpc_rename_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->from) == -1)
		rc = -errno;
	else if (translate_pathname(mq->to) == -1)
		rc = -errno;
	else if (rename(mq->from, mq->to) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_rmdir(struct pscrpc_request *rq)
{
	struct slashrpc_rmdir_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (rmdir(mq->path) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_statfs(struct pscrpc_request *rq)
{
	struct slashrpc_statfs_req *mq;
	struct slashrpc_statfs_rep *mp;
	struct statfs sfb;

	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		mp->rc = -EPROTO;
	else if (translate_pathname(mq->path) == -1)
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
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->from) == -1)
		rc = -errno;
	else if (translate_pathname(mq->to) == -1)
		rc = -errno;
	else if (symlink(mq->from, mq->to) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_truncate(struct pscrpc_request *rq)
{
	struct slashrpc_truncate_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (truncate(mq->path, mq->size) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_unlink(struct pscrpc_request *rq)
{
	struct slashrpc_unlink_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (unlink(mq->path) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_utimes(struct pscrpc_request *rq)
{
	struct slashrpc_utimes_req *mq;
	int rc;

	rc = 0;
	GET_GEN_REQ(rq, mq);
	if (translate_pathname(mq->path) == -1)
		rc = -errno;
	else if (utimes(mq->path, mq->times) == -1)
		rc = -errno;
	GENERIC_REPLY(rq, rc);
}

int
slmds_svc_handler(struct pscrpc_request *req)
{
	struct slashrpc_export *sexp;
	uid_t myuid, tuid;
	gid_t mygid, tgid;
	int rc = 0;

	ENTRY;
	DEBUG_REQ(PLL_TRACE, req, "new req");

	/* Set fs credentials */
	myuid = getuid();
	mygid = getgid();
	spinlock(&fsidlock);
	sexp = slashrpc_export_get(req->rq_export);

	if ((tuid = setfsuid(sexp->uid)) != myuid)
		psc_fatal("invalid fsuid %u", tuid);
	if (setfsuid(sexp->uid) != (int)sexp->uid) {
		psc_error("setfsuid %u", sexp->uid);
		rc = -1;
		goto done;
	}

	if ((tgid = setfsgid(sexp->gid)) != mygid)
		psc_fatal("invalid fsgid %u", tgid);
	if (setfsgid(sexp->gid) != (int)sexp->gid) {
		psc_error("setfsgid %u", sexp->gid);
		rc = -1;
		goto done;
	}

	switch (req->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slmds_connect(req);
		break;
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
	case SRMT_DESTROY: /* client has unmounted */
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
	case SRMT_READ:
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
	case SRMT_WRITE:
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
	setfsuid(myuid);
	if (setfsuid(myuid) != (int)myuid)
		psc_fatal("setfsuid %d", myuid);
	setfsgid(mygid);
	if (setfsgid(mygid) != (int)mygid)
		psc_fatal("setfsgid %d", mygid);
	freelock(&fsidlock);
	RETURN(rc);
}

/**
 * slmds_init - start up the mds threads via pscrpc_thread_spawn()
 */
void
slmds_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = MDS_NBUFS;
	svh->svh_bufsz      = MDS_BUFSZ;
	svh->svh_reqsz      = MDS_BUFSZ;
	svh->svh_repsz      = MDS_REPSZ;
	svh->svh_req_portal = MDS_REQPORTAL;
	svh->svh_rep_portal = MDS_REPPORTAL;
	svh->svh_type       = SLTHRT_RPCMDS;
	svh->svh_nthreads   = MDS_NTHREADS;
	svh->svh_handler    = slmds_svc_handler;

	strncpy(svh->svh_svc_name, MDS_SVCNAME, PSCRPC_SVCNAME_MAX);

	pscrpc_thread_spawn(svh);
}
