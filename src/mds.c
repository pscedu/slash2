/* $Id$ */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>

#include <unistd.h>
#include <errno.h>

#include "psc_rpc/rpc.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"

#include "rpc.h"
#include "slashrpc.h"
#include "slash.h"
#include "fid.h"

#define MDS_NTHREADS  8
#define MDS_NBUFS     1024
#define MDS_BUFSZ     256
#define MDS_REPSZ     128
#define MDS_REQPORTAL RPCMDS_REQ_PORTAL
#define MDS_REPPORTAL RPCMDS_REP_PORTAL
#define MDS_SVCNAME   "slash_mds_svc"

psc_spinlock_t fsidlock = LOCK_INITIALIZER;

/*
 * cfd2fid - look up a client file descriptor in the export cfdtree
 *	for the associated file ID.
 * @rq: RPC request containing RPC export peer info.
 * @cfd: client file descriptor.
 */
slash_fid_t *
cfd2fid(struct pscrpc_request *rq, u64 cfd)
{
	return (0);
}

int
slmds_connect(struct pscrpc_request *req)
{
	int rc;
	int size = sizeof(struct slashrpc_connect_req);
        struct slashrpc_connect_req *body, *repbody;

	body = psc_msg_buf(req->rq_reqmsg, 0, size);
        if (body == NULL) {
                psc_warnx("connect_body is null");
                rc = -ENOMSG;
                goto fail;
        }
        psc_notify("magic %"ZLPX64" version %u",
		   body->magic, body->version);

        if (body->magic   != SMDS_MAGIC ||
            body->version != SMDS_VERSION) {
		rc = -EINVAL;
		goto fail;
	}
        rc = psc_pack_reply(req, 1, &size, NULL);
        if (rc) {
                psc_assert(rc == -ENOMEM);
                psc_error("psc_pack_reply failed");
                goto fail;
        }
        repbody = psc_msg_buf(req->rq_repmsg, 0, size);
	/* Malloc was done in psc_pack_reply() */
        psc_assert(repbody);

	repbody->magic  = SMDS_MAGIC;
	repbody->version = SMDS_VERSION;

        psc_notify("Connect request from %"ZLPX64":%u",
		   req->rq_peer.nid, req->rq_peer.pid);

        return (0);
 fail:
        psc_notify("Failed connect request from %"ZLPX64":%u",
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
        int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);
	if (fid_makepath(cfd2fid(rq, body->cfd), fn))
		return (-EINVAL);
	rc = chmod(fn, body->mode);
	if (rc)
		return (-errno);
	return (0);
}
#endif

int
slmds_chown(struct pscrpc_request *req)
{
        int rc;
        struct slashrpc_chown_req *body;

	body = psc_msg_buf(req->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);

//	if (which_chown == SYS_fchown)
//		if (fid_makepath(cfd2fid(req, body->cfd), body->path))
//			return (-EINVAL);

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
        int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
	if (!body)
		return (-EPROTO);
	if (fid_makepath(cfd2fid(rq, body->cfd), fn))
		return (-EINVAL);
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
slmds_ftruncate(struct pscrpc_request *rq)
{
	struct slashrpc_ftruncate_req *body;
	char fn[PATH_MAX];
	int rc;

	body = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*body));
        if (!body)
                return (-EPROTO);

	if (fid_makepath(cfd2fid(rq, body->cfd), fn))
		return (-EINVAL);
	rc = truncate(fn, body->size);
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
//	case SRMT_FCHMOD:
//		rc = slmds_fchmod(req);
//		break;
	case SRMT_CHOWN:
		rc = slmds_chown(req);
		break;
//	case SRMT_FCHOWN:
//		rc = slmds_fchown(req);
//		break;
	case SRMT_CREATE:
		rc = slmds_create(req);
		break;
	case SRMT_OPEN:
		rc = slmds_open(req);
		break;
	case SRMT_GETATTR:
		rc = slmds_getattr(req);
		break;
	case SRMT_LINK:
		rc = slmds_link(req);
		break;
	case SRMT_MKDIR:
		rc = slmds_mkdir(req);
		break;
	case SRMT_RELEASE:
		rc = -EOPNOTSUPP;
		break;
	case SRMT_RENAME:
		rc = slmds_rename(req);
		break;
	case SRMT_RMDIR:
		rc = slmds_rmdir(req);
		break;
	case SRMT_SYMLINK:
		rc = slmds_symlink(req);
		break;
	case SRMT_TRUNCATE:
		rc = slmds_truncate(req);
		break;
	case SRMT_FTRUNCATE:
		rc = slmds_ftruncate(req);
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
