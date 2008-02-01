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

#define SLIO_NTHREADS  8
#define SLIO_NBUFS     1024
#define SLIO_BUFSZ     (4096+256)
#define SLIO_REPSZ     128
#define SLIO_REQPORTAL RPCIO_REQ_PORTAL
#define SLIO_REPPORTAL RPCIO_REP_PORTAL
#define SLIO_SVCNAME   "slrpciothr"

#define GENERIC_REPLY(rq, prc)								\
	do {										\
		struct slashrpc_generic_rep *_mp;					\
		int _rc, _size;								\
											\
		_size = sizeof(*(_mp));							\
		if (_size > SLIO_REPSZ)							\
			psc_fatalx("reply size greater than max");			\
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
		if (_size > SLIO_REPSZ)							\
			psc_fatalx("reply size greater than max");			\
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
 * @must_exist: whether this path must exist or not (e.g. if being created).
 * Returns 0 on success or -1 on error.
 */
int
translate_pathname(char *path, int must_exist)
{
	char *lastsep, buf[PATH_MAX];
	int rc;

//	rc = snprintf(path, PATH_MAX, "%s/%s", nodeProfile->slnprof_fsroot, buf);
	rc = snprintf(buf, PATH_MAX, "%s/%s", "/slashfs", path);
	if (rc == -1)
		return (-1);
	if (rc >= (int)sizeof(buf)) {
		errno = ENAMETOOLONG;
		return (-1);
	}
	/*
	 * As realpath(3) requires that the resolved pathname must exist,
	 * if we are creating a new pathname, it obviously won't exist,
	 * so trim the last component and append it later on.
	 */
	if (must_exist == 0 && (lastsep = strrchr(buf, '/')) != NULL) {
		if (strncmp(lastsep, "/..", strlen("/..")) == 0) {
			errno = -EINVAL;
			return (-1);
		}
		*lastsep = '\0';
	}
	if (realpath(buf, path) == NULL)
		return (-1);
	if (strncmp(path, "/slashfs", strlen("/slashfs"))) {
		/*
		 * If they found some way around
		 * realpath(3), try to catch it...
		 */
		errno = EINVAL;
		return (-1);
	}
	if (lastsep) {
		*lastsep = '/';
		strncat(path, lastsep, PATH_MAX - 1 - strlen(path));
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
slmds_read(struct pscrpc_request *rq)
{
	struct slashrpc_getattr_req *mq;
	struct slashrpc_getattr_rep *mp;
	struct stat stb;

	if ((mq = psc_msg_buf(rq->rq_reqmsg, 0, sizeof(*mq))) == NULL)
		return (-ENOMSG);
	GET_CUSTOM_REPLY(rq, mp);
	mp->rc = 0;
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
slmds_write(struct pscrpc_request *rq)
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
setcred(uid_t uid, gid_t gid, uid_t *myuid, gid_t *mygid)
{
	uid_t tuid;
	gid_t guid;

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
fidcache_opencb(struct fidcache_ent *fc, const char *fn, int flags, int mode)
{
	uid_t myuid;
	gid_t mygid;
	int fd;

	sexp = slashrpc_export_get(fc->fc_rq->rq_export);
	if (setcred(sexp->uid, sexp->gid, &myuid, &mygid) == -1)
		return (-1);
	fd = open(fn, flags, mode);
	revokecred(&myuid, &mygid);
	return (fd);
}

int
slio_svc_handler(struct pscrpc_request *rq)
{
	struct slashrpc_export *sexp;
	int rc = 0;

	ENTRY;
	DEBUG_REQ(PLL_TRACE, rq, "new req");
	switch (rq->rq_reqmsg->opc) {
	case SRMT_CONNECT:
		rc = slio_connect(rq);
		break;
	case SRMT_READ:
		rc = slio_read(rq);
		break;
	case SRMT_WRITE:
		rc = slio_write(rq);
		break;
	case SRMT_WRITE:
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
	RETURN(rc);
}

/**
 * slio_init - start up the I/O threads via pscrpc_thread_spawn()
 */
void
slio_init(void)
{
	pscrpc_svc_handle_t *svh = PSCALLOC(sizeof(*svh));

	svh->svh_nbufs      = RPCIO_NBUFS;
	svh->svh_bufsz      = RPCIO_BUFSZ;
	svh->svh_reqsz      = RPCIO_BUFSZ;
	svh->svh_repsz      = RPCIO_REPSZ;
	svh->svh_req_portal = RPCIO_REQPORTAL;
	svh->svh_rep_portal = RPCIO_REPPORTAL;
	svh->svh_type       = SLTHRT_RPCIO;
	svh->svh_nthreads   = RPCIO_NTHREADS;
	svh->svh_handler    = slio_svc_handler;

	strncpy(svh->svh_svc_name, RPCIO_SVCNAME, PSCRPC_SVCNAME_MAX);

	pscrpc_thread_spawn(svh);
}
