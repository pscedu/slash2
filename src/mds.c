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
#define MDS_BUFSZ     256
#define MDS_REPSZ     128
#define MDS_REQPORTAL RPCMDS_REQ_PORTAL
#define MDS_REPPORTAL RPCMDS_REP_PORTAL
#define MDS_SVCNAME   "slash_mds_svc"

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

int
slmds_connect(struct pscrpc_request *req)
{
	struct slashrpc_connect_req *body;
	int rc, size;

	body = psc_msg_buf(req->rq_reqmsg, 0, size);
	if (body == NULL) {
		psc_warnx("connect_body is null");
		rc = -ENOMSG;
		goto fail;
	}
	psc_notify("magic %"_P_LP64"x version %u",
		   body->magic, body->version);

	if (body->magic   != SMDS_MAGIC ||
	    body->version != SMDS_VERSION) {
		rc = -EINVAL;
		goto fail;
	}
	size = 0;
	rc = psc_pack_reply(req, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		goto fail;
	}
	psc_notify("Connect request from %"_P_LP64"x:%u",
		   req->rq_peer.nid, req->rq_peer.pid);

	return (0);
 fail:
	psc_notify("Failed connect request from %"_P_LP64"x:%u",
		   req->rq_peer.nid, req->rq_peer.pid);
	return (rc);
}

int
slmds_access(struct pscrpc_request *req)
{
	int rc;
	struct slashrpc_access_req *body;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = access(body->path, body->mask);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_chmod(struct pscrpc_request *req)
{
	int rc;
	struct slashrpc_chmod_req *body;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = chmod(body->path, body->mode);
	if (rc)
		return (-errno);

	return (0);
}

#if 0
int
slmds_fchmod(struct pscrpc_request *rq)
{
	struct slashrpc_fchmod_req *body;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);
	if (cfd2fid(&fid, rq->rq_export, body->cfd) || fid_makepath(&fid, fn))
		return (-errno);
	rc = chmod(fn, body->mode);
	if (rc)
		return (-errno);
	return (0);
}
#endif

int
slmds_chown(struct pscrpc_request *req)
{
	struct slashrpc_chown_req *body;
	int rc;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = chown(body->path, body->uid, body->gid);
	if (rc)
		return (-errno);

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
	struct slashrpc_fchown_req *body;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);
	if (cfd2fid(&fid, rq->rq_export, body->cfd) || fid_makepath(&fid, fn))
		return (-errno);
	rc = chown(fn, body->uid, body->gid);
	if (rc)
		return (-errno);
	return (0);
}
#endif

int
slmds_create(struct pscrpc_request *req)
{
	int rc;
	struct slashrpc_create_req *body;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = creat(body->path, body->mode);
	if (rc)
		return (-errno);

	return (0);
}

int
slmds_getattr(struct pscrpc_request *rq)
{
	struct slashrpc_getattr_req *mq;
	struct slashrpc_getattr_rep *mp;
	struct stat stb;
	int rc, size;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	if (!mq)
		return (-EPROTO);

	rc = stat(mq->path, &stb);
	if (rc)
		return (-errno);

	size = sizeof(*mp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	psc_assert(mp);
	mp->mode = stb.st_mode;
	mp->nlink = stb.st_nlink;
	mp->uid = stb.st_uid;
	mp->gid = stb.st_gid;
	mp->size = stb.st_size; /* XXX */
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
	int rc, size;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	if (!mq)
		return (-EPROTO);

	if (cfd2fid(&fid, rq->rq_export, mq->cfd) || fid_makepath(&fid, fn))
		return (-errno);
	rc = stat(fn, &stb);
	if (rc)
		return (-errno);

	size = sizeof(*mp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	psc_assert(mp);
	mp->mode = stb.st_mode;
	mp->nlink = stb.st_nlink;
	mp->uid = stb.st_uid;
	mp->gid = stb.st_gid;
	mp->size = stb.st_size; /* XXX */
	mp->atime = stb.st_atime;
	mp->mtime = stb.st_mtime;
	mp->ctime = stb.st_ctime;
	return (0);
}

int
slmds_ftruncate(struct pscrpc_request *rq)
{
	struct slashrpc_ftruncate_req *body;
	char fn[PATH_MAX];
	slash_fid_t fid;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	if (cfd2fid(&fid, rq->rq_export, body->cfd) || fid_makepath(&fid, fn))
		return (-errno);
	rc = truncate(fn, body->size);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_link(struct pscrpc_request *rq)
{
	struct slashrpc_link_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = link(body->from, body->to);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_mkdir(struct pscrpc_request *rq)
{
	struct slashrpc_mkdir_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = mkdir(body->path, body->mode);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_mknod(struct pscrpc_request *rq)
{
	struct slashrpc_mknod_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = mknod(body->path, body->mode, body->dev);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_open(struct pscrpc_request *req)
{
	int rc;
	struct slashrpc_open_req *body;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = open(body->path, body->flags);
	if (rc)
		return (-errno);

	return (0);
}

int
slmds_opendir(struct pscrpc_request *rq)
{
	struct slashrpc_opendir_req *mq;
	struct slashrpc_opendir_rep *mp;
	int rc, size;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	if (!mq)
		return (-EPROTO);
	size = sizeof(*mp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	psc_assert(mp);
	if (cfdnew(&mp->cfd, rq->rq_export, mq->path))
		return (-errno);
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
	slash_fid_t fid;
	int comms_error, size, rc;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	if (!mq)
		return (-EPROTO);

	size = sizeof(*mp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	if (!mp)
		return (-EPROTO);

	if (cfd2fid(&fid, rq->rq_export, mq->cfd))
		return (-errno);
	dc = dircache_get(&fid);
	if (dc == NULL)
		return (-errno);
	rc = dircache_read(dc, mq->offset, ents, READDIR_BUFSZ);
	dircache_rel(dc);
	if (rc == -1)
		return (-errno);
	if (rc == 0)
		return (0);
	mp->size = rc;

	desc = pscrpc_prep_bulk_exp(rq, rc / pscPageSize,
	    BULK_PUT_SOURCE, RPCMDS_BULK_PORTAL);
	if (desc == NULL) {
		psc_warnx("pscrpc_prep_bulk_exp returned a null desc");
		return (-ENOMEM);
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
	return (rc);
}

int
slmds_readlink(struct pscrpc_request *rq)
{
	struct slashrpc_readlink_req *mq;
	struct slashrpc_readlink_rep *mp;
	int size, rc;

	mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq));
	if (!mq)
		return (-EPROTO);
	if (mq->size > PATH_MAX || mq->size == 0)
		return (-EINVAL);

	size = mq->size;
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	if (!mp)
		return (-EPROTO);

	rc = readlink(mq->path, mp->buf, mq->size);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_release(struct pscrpc_request *rq)
{
	struct slashrpc_release_req *body;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);
	if (cfdfree(rq->rq_export, body->cfd))
		return (-errno);
	return (0);
}

int
slmds_releasedir(struct pscrpc_request *rq)
{
	struct slashrpc_releasedir_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = cfdfree(rq->rq_export, body->cfd);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_rename(struct pscrpc_request *rq)
{
	struct slashrpc_rename_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = rename(body->from, body->to);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_rmdir(struct pscrpc_request *rq)
{
	struct slashrpc_rmdir_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = rmdir(body->path);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_statfs(struct pscrpc_request *rq)
{
	struct slashrpc_statfs_req *mq;
	struct slashrpc_statfs_rep *mp;
	struct statfs sfb;
	int rc, size;

	mp = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mp));
	if (!mp)
		return (-EPROTO);

	size = sizeof(*mp);
	rc = psc_pack_reply(rq, 1, &size, NULL);
	if (rc) {
		psc_assert(rc == -ENOMEM);
		psc_error("psc_pack_reply failed");
		return (rc);
	}
	mp = psc_msg_buf(rq->rq_repmsg, 0, size);
	psc_assert(mp);

	rc = statfs(mq->path, &sfb);
	if (rc)
		return (-errno);
	mp->f_bsize	= sfb.f_bsize;
	mp->f_blocks	= sfb.f_blocks;
	mp->f_bfree	= sfb.f_bfree;
	mp->f_bavail	= sfb.f_bavail;
	mp->f_files	= sfb.f_files;
	mp->f_ffree	= sfb.f_ffree;
	return (0);
}

int
slmds_symlink(struct pscrpc_request *rq)
{
	struct slashrpc_symlink_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = symlink(body->from, body->to);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_truncate(struct pscrpc_request *rq)
{
	struct slashrpc_truncate_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = truncate(body->path, body->size);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_unlink(struct pscrpc_request *rq)
{
	struct slashrpc_unlink_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = unlink(body->path);
	if (rc)
		return (-errno);
	return (0);
}

int
slmds_utimes(struct pscrpc_request *rq)
{
	struct slashrpc_utimes_req *body;
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

	rc = utimes(body->path, body->times);
	if (rc)
		return (-errno);
	return (0);
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
